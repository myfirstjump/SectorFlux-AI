from loguru import logger
import gc

import numpy as np
import pandas as pd
import torch
from tsfm_public import TinyTimeMixerForPrediction


class TSFIntegrator:
    """
    IBM Granite-TTM 推論核心（Flux-based, v3）。

    輸入  : Feature_DailyNodeFlux 的 12 channel 每日淨流序列（net_flux_weight）
    模型  : granite-timeseries-ttm-r2 (512-192-r2)，CPU、channel-independent、zero-shot
    輸出  : 未來 H 天的 12ch 每日淨流預測 → 累加回 now allocation → 未來 M/Q 板塊佔比
    """

    def __init__(self, config, db, context_window=512, horizon=192):
        self.config = config
        self.db = db
        self.context_window = context_window
        self.horizon = horizon
        self.device = "cpu"                      # 8GB RAM：CPU 序列推論
        self.model_id = "ibm-granite/granite-timeseries-ttm-r2"
        self.revision = "512-192-r2"
        self.nodes = config.L0_SECTORS + ['HEDGE']   # 固定 12 channel 順序
        self.n_channels = len(self.nodes)

    # ──────────────────────────────────────────────────────────────
    def create_input_tensor(self, latest_date=None):
        """
        讀 Feature_DailyNodeFlux 最近 context_window 天 → [1, context_window, 12] 張量。

        回傳 (tensor, used_dates)；資料不足時回傳 (None, None)。
        channel 順序嚴格對齊 self.nodes。
        """
        date_filter = "AND [Date] <= :d" if latest_date else ""
        params = {"d": latest_date} if latest_date else {}
        df = self.db.execute_query(f"""
            SELECT CONVERT(VARCHAR,[Date],23) AS Date, Node_ID, Net_Flux_Weight
            FROM Feature_DailyNodeFlux WITH (NOLOCK)
            WHERE 1=1 {date_filter}
            ORDER BY [Date]
        """, params=params)

        if df.empty:
            logger.error("💥 [TSF] Feature_DailyNodeFlux 無資料，請先執行 build_flux_features。")
            return None, None

        pivot = (df.pivot(index='Date', columns='Node_ID', values='Net_Flux_Weight')
                   .sort_index())
        # 對齊 channel 順序，缺漏補 0
        for n in self.nodes:
            if n not in pivot.columns:
                pivot[n] = 0.0
        pivot = pivot[self.nodes].fillna(0.0)

        if len(pivot) < self.context_window:
            logger.error(f"💥 [TSF] 序列長度 {len(pivot)} < context_window {self.context_window}。")
            return None, None

        window = pivot.iloc[-self.context_window:]
        used_dates = (window.index[0], window.index[-1])
        arr = window.values.astype(np.float32)            # [context, 12]

        # ── per-channel 標準化（關鍵）────────────────────────────────
        # net_flux_weight 各 channel 量級極小（std 0.01%~0.3%），直接餵 TTM 會觸發
        # tiny-variance 數值失穩（小方差 channel 預測值暴衝 ~100×）。z-score 成單位變異
        # 後再推論、輸出反標準化，可徹底穩定 zero-shot。
        mu = arr.mean(axis=0)
        sd = arr.std(axis=0) + 1e-9
        z  = (arr - mu) / sd
        tensor = torch.from_numpy(z.astype(np.float32)).unsqueeze(0)   # [1, context, 12]
        scaler = (mu, sd)
        logger.info(f"📦 [TSF] 輸入張量 {tuple(tensor.shape)}（{used_dates[0]} ~ {used_dates[1]}，已 z-score）")
        return tensor, used_dates, scaler

    # ──────────────────────────────────────────────────────────────
    def run_ttm_inference(self, input_tensor):
        """
        載入 TTM-r2 執行推論，回傳 forecast numpy [H, 12]（H=模型 horizon）。
        推論後立即釋放模型（8GB 保護）。
        """
        try:
            logger.info(f"🧠 [TSF] 載入 TTM-r2（revision={self.revision}, channels={self.n_channels}）...")
            model = TinyTimeMixerForPrediction.from_pretrained(
                self.model_id,
                revision=self.revision,
                num_input_channels=self.n_channels,
                ignore_mismatched_sizes=True,
            ).to(self.device)
            model.eval()

            with torch.no_grad():
                outputs = model(past_values=input_tensor.to(self.device))
                forecast = outputs.prediction_outputs.detach().cpu().numpy()  # [1, H, 12]

            del model
            gc.collect()
            logger.info(f"🧹 [TSF] 推論完成，模型已釋放。forecast shape={forecast.shape}")
            return forecast[0]   # [H, 12]

        except Exception as e:
            logger.error(f"💥 [TSF] TTM 推論失敗: {e}")
            return None

    def run_ttm_inference_batch(self, batch_tensor):
        """
        批次推論：input [B, context, 12]（已 per-window z-score）→ forecast [B, H, 12]。
        回測用——一次載入模型跑完所有 anchor，避免反覆 load/free。
        """
        try:
            logger.info(f"🧠 [TSF] 批次推論 {tuple(batch_tensor.shape)}（載入 TTM-r2 一次）...")
            model = TinyTimeMixerForPrediction.from_pretrained(
                self.model_id, revision=self.revision,
                num_input_channels=self.n_channels, ignore_mismatched_sizes=True,
            ).to(self.device)
            model.eval()
            with torch.no_grad():
                out = model(past_values=batch_tensor.to(self.device))
                forecast = out.prediction_outputs.detach().cpu().numpy()   # [B, H, 12]
            del model
            gc.collect()
            logger.info(f"🧹 [TSF] 批次推論完成 forecast={forecast.shape}")
            return forecast
        except Exception as e:
            logger.error(f"💥 [TSF] 批次推論失敗: {e}")
            return None

    # ──────────────────────────────────────────────────────────────
    def reconstruct_future_allocation(self, forecast, now_alloc, scaler=None,
                                      horizons=(('M', 21), ('Q', 63))):
        """
        把每日淨流預測累加回 now allocation，得未來各 horizon 的 12 節點佔比。

        forecast   : [H, 12] z-score 空間的每日預測（channel 對齊 self.nodes）
        scaler     : (mu, sd) 來自 create_input_tensor，用於反標準化回 net_flux 單位
        now_alloc  : dict {node: weight}（Fact_NodeAllocation LW=0 當日快照）
        回傳 {'M': {node: weight}, 'Q': {...}}，每組已 clip≥0 並 renormalize 加總=1。
        """
        if forecast is None:
            return None

        # 反標準化：z-score → 真實 net_flux_weight
        if scaler is not None:
            mu, sd = scaler
            forecast = forecast * sd + mu

        now_vec = np.array([float(now_alloc.get(n, 0.0)) for n in self.nodes])
        cum = np.cumsum(forecast, axis=0)          # [H, 12] 累積淨流

        results = {}
        for key, h in horizons:
            step = min(h, cum.shape[0]) - 1
            future = now_vec + cum[step]           # 加性重建
            future = np.clip(future, 0.0, None)    # 佔比不可為負
            s = future.sum()
            future = future / s if s > 0 else now_vec
            results[key] = {self.nodes[i]: float(future[i]) for i in range(self.n_channels)}
            drift = float(np.abs(future - now_vec).sum() / 2)   # 總配置位移（0~1）
            logger.info(f"🔮 [TSF] {key} (h={h}) 重建完成，配置位移 {drift*100:.1f}%")
        return results
