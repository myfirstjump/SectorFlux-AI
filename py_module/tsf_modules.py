from loguru import logger
import gc

import numpy as np
import pandas as pd
import torch
from tsfm_public import TinyTimeMixerForPrediction

class TSFIntegrator:
    """
    模型藝術家的推論核心：
    負責將 19 組 Z-Score 序列打包成張量，並執行 IBM Granite-TTM 63 天預測。
    """
    def __init__(self, config, db, context_window=512, horizon=63):
        self.config = config
        self.db = db
        self.context_window = context_window
        self.horizon = horizon
        self.device = "cpu" # 8GB RAM 限制下使用 CPU 確保穩定
        self.model_id = "ibm-granite/granite-timeseries-ttm-r2"
        self.revision = "512-192-r2"

    def create_input_tensor(self, db_manager, latest_date, context_window=512):
        """
        從現有 Fact_DailyPrice 提取 Z-Score 並封裝為 19 頻道張量。
        """
        # 定義 11 Sectors + 7 Macro 原始標的
        L0_SECTORS = self.config.L0_SECTORS # ['XLK', 'XLF', 'XLV', 'XLE', 'XLI', 'XLY', 'XLP', 'XLU', 'XLB', 'XLRE', 'XLC']
        MACRO_RAW = self.config.MACRO_UNIVERSE # ["GLD", "USO", "UUP", "^FVX", "^TNX", "^VIX", "DX-Y.NYB"]
        target_symbols = L0_SECTORS + MACRO_RAW
        
        # 最終張量頻道順序 (對齊模型輸入規範)
        FINAL_CHANNELS = L0_SECTORS + MACRO_RAW + ["SPREAD_10Y5Y"]

        try:
            query = """
            SELECT Date, Symbol, ZScore_20D 
            FROM Fact_DailyPrice 
            WHERE Symbol IN :symbols 
            AND Date <= :latest_date
            ORDER BY Date DESC
            """
            params = {
            "symbols": tuple(target_symbols),
            "latest_date": latest_date
            }
            # 利用數據維運官優化的 SQLAlchemy 引擎執行
            raw_df = db_manager.execute_query(query, params=params)
            
            if raw_df.empty:
                logger.error(f"TSF | {latest_date} 前無數據，請檢查爬蟲與 NCCI 運算狀態。")
                return None

            pivot_df = raw_df.pivot(index='Date', columns='Symbol', values='ZScore_20D')
            pivot_df = pivot_df.ffill().bfill() 
            pivot_df = pivot_df.sort_index().tail(self.context_window)
            
            # 動態計算 10Y-5Y 利差 (SPREAD)
            if '^TNX' in pivot_df.columns and '^FVX' in pivot_df.columns:
                pivot_df['SPREAD_10Y5Y'] = pivot_df['^TNX'] - pivot_df['^FVX']
            else:
                pivot_df['SPREAD_10Y5Y'] = 0.0

            # 截取最後 512 天並轉換為模型所需的 [1, 512, 19] 張量
            final_df = pivot_df[FINAL_CHANNELS].tail(context_window)
            input_tensor = torch.tensor(final_df.values, dtype=torch.float32).unsqueeze(0)
            
            logger.info(f"TSF | 張量封裝成功！通道數: {len(FINAL_CHANNELS)}, Shape: {list(input_tensor.shape)}")
            return input_tensor

        except Exception as e:
            logger.error(f"TSF | 張量產製異常: {e}")
            return None

    def run_ttm_inference(self, input_tensor):
        """
        執行 IBM Granite-TTM 官方版推論
        """
        try:
            logger.info(f"🧠 [TSF] 加載官方 TTM-r2 模型 (Revision: {self.revision})...")
            
            # 1. 序列化加載：確保記憶體不被重複佔用
            model = TinyTimeMixerForPrediction.from_pretrained(
                self.model_id, 
                revision=self.revision,
                num_input_channels=19, # 對齊我們的 19 頻道
                prediction_filter_length=self.horizon, # 只輸出我們需要的長度
                ignore_mismatched_sizes=True
            ).to(self.device)
            
            model.eval()
            logger.info("✅ [TSF] 官方模型加載成功，執行預測中...")

            # 2. 執行推論
            with torch.no_grad():
                # TTM-r2 預期輸入為 [Batch, Context, Channels]
                outputs = model(past_values=input_tensor)
                # 取得預測結果
                forecast_values = outputs.prediction_outputs.detach().cpu().numpy()

            # 3. 極致資源回收：保護 8GB RAM
            del model
            gc.collect()
            logger.info("🧹 [TSF] 推論結束，模型資源已釋放。")

            return forecast_values

        except Exception as e:
            logger.error(f"💥 [TSF] 官方模型推論失敗: {e}")
            return None

    def calculate_future_flux(self, forecast_data, current_allocations):
        """
        將 Z-Score 預測轉化為 12x12 Flux Matrix。
        current_allocations: 來自 Fact_NodeAllocation 的當前權重 (Dict)
        """
        if forecast_data is None: return None
        
        # 標籤清單 (對應你的 11 Sectors + 1 Hedge)
        nodes = self.config.L0_SECTORS + ['HEDGE']
        results = {}

        for horizon_key, step_idx in [('M', 20), ('Q', 62)]:
            # 1. 取得該 Horizon 的預測 Z-Score
            preds = forecast_data[0, step_idx, :len(nodes)]
            
            # 2. 轉化為預期權重 (使用 Softmax 或 權重增量法)
            # 這裡假設 Z-Score 直接影響權重偏離度
            target_weights = self._predict_weight_shift(current_allocations, preds)
            
            # 3. 構建 12x12 矩陣 (Index: Source, Column: Target)
            # 核心邏輯：將權重減少的 Node 作為 Source，增加的作為 Target
            flux_matrix = self._generate_transfer_matrix(current_allocations, target_weights, nodes)
            results[horizon_key] = flux_matrix
            
        return results

    def _generate_transfer_matrix(self, now, future, nodes):
        """
        計算 N-to-N 的資金轉移路徑，確保總量守恆。
        """
        import pandas as pd
        matrix = pd.DataFrame(0.0, index=nodes, columns=nodes)
        
        # 計算各節點的變動量 delta
        deltas = {node: future.get(node, 0) - now.get(node, 0) for node in nodes}
        
        sources = {n: -d for n, d in deltas.items() if d < 0} # 權重減少
        targets = {n: d for n, d in deltas.items() if d > 0}  # 權重增加
        
        # 按照比例分配流向 (Proportional Allocation)
        total_out = sum(sources.values())
        if total_out > 0:
            for s_name, s_val in sources.items():
                for t_name, t_val in targets.items():
                    # 流向權重 = (流出比例 * 流入比例)
                    matrix.loc[s_name, t_name] = s_val * (t_val / sum(targets.values()))
                    
        return matrix

    def decode_flux_prediction(self, forecast_tensor):
        """
        將預測的 Z-Score 逆推回 RS 勢能，
        並產出 Sankey Plot 所需的 Future Flow 數據。
        """
        pass