import os
import numpy as np
import pandas as pd
import torch
from loguru import logger

from tsfm_public import ForecastDFDataset, TinyTimeMixerForPrediction
from transformers import Trainer, TrainingArguments

MODEL_ID  = "ibm-granite/granite-timeseries-ttm-r2"
REVISION  = "512-192-r2"
CKPT_DIR  = "/workspace/models/ttm_finetuned"


class FineTuner:
    """
    TTM-r2 head-only fine-tune（單一切分：train ≤ train_end，評估留給 backtest 在 2025+）。

    一致性關鍵：以 train 期的 per-channel 全域標準化（mean/std）把 net_flux_weight
    （std 僅 0.01%~0.3%）拉到 O(1)，TTM 內部 RevIN 才能正常運作；scaler 存檔，
    推論/回測載入同一組，確保 train↔inference 尺度一致。
    """

    def __init__(self, config, db, context=512, pred=192,
                 train_end='2024-12-31', ckpt_dir=CKPT_DIR):
        self.config = config
        self.db = db
        self.context = context
        self.pred = pred
        self.train_end = train_end
        self.ckpt_dir = ckpt_dir
        self.nodes = config.L0_SECTORS + ['HEDGE']

    def _load_df(self):
        df = self.db.execute_query("""
            SELECT CONVERT(VARCHAR,[Date],23) AS date, Node_ID, Net_Flux_Weight
            FROM Feature_DailyNodeFlux WITH (NOLOCK) ORDER BY [Date]
        """)
        if df.empty:
            return None
        wide = (df.pivot(index='date', columns='Node_ID', values='Net_Flux_Weight')
                  .sort_index())
        for n in self.nodes:
            if n not in wide.columns:
                wide[n] = 0.0
        return wide[self.nodes].reset_index()           # columns: date + 12 nodes

    def run(self, epochs=40, lr=1e-3, batch=32, weight_decay=0.01):
        df = self._load_df()
        if df is None:
            logger.error("💥 無 Feature_DailyNodeFlux，請先 build_flux_features。")
            return
        train_df = df[df['date'] <= self.train_end].copy()
        logger.info(f"📚 train 期：{train_df['date'].min()} ~ {train_df['date'].max()}（{len(train_df)} 日）")

        # ── 全域 per-channel 標準化（fit on train）──
        mean = train_df[self.nodes].mean().values.astype(np.float32)
        std  = (train_df[self.nodes].std().values + 1e-9).astype(np.float32)
        scaled = train_df.copy()
        scaled[self.nodes] = (train_df[self.nodes].values - mean) / std

        # ── 滑動視窗資料集 ──
        ds = ForecastDFDataset(
            scaled, timestamp_column='date', id_columns=[],
            target_columns=self.nodes,
            context_length=self.context, prediction_length=self.pred,
            enable_padding=False,
        )
        logger.info(f"🪟 訓練視窗數：{len(ds)}（context={self.context}, pred={self.pred}, 12ch）")
        if len(ds) < 50:
            logger.warning("⚠️ 訓練視窗過少，fine-tune 可能不穩。")

        # ── 載入 base 模型，凍結 backbone+decoder，只訓 head ──
        model = TinyTimeMixerForPrediction.from_pretrained(
            MODEL_ID, revision=REVISION,
            num_input_channels=len(self.nodes), ignore_mismatched_sizes=True,
        )
        n_train = 0
        for name, p in model.named_parameters():
            if 'head' in name:
                p.requires_grad = True; n_train += p.numel()
            else:
                p.requires_grad = False
        logger.info(f"🔧 head-only fine-tune，可訓練參數 {n_train:,}（backbone+decoder 凍結）")

        args = TrainingArguments(
            output_dir='/workspace/models/_ttm_train_tmp',
            num_train_epochs=epochs,
            per_device_train_batch_size=batch,
            learning_rate=lr, weight_decay=weight_decay,
            logging_steps=20, save_strategy='no', report_to='none',
            use_cpu=True, dataloader_num_workers=0,
            label_names=['future_values'],
        )
        trainer = Trainer(model=model, args=args, train_dataset=ds)
        logger.info(f"🚂 開始訓練（epochs={epochs}, lr={lr}, batch={batch}, CPU）...")
        trainer.train()

        # ── 存檔模型 + scaler ──
        os.makedirs(self.ckpt_dir, exist_ok=True)
        model.save_pretrained(self.ckpt_dir)
        np.savez(os.path.join(self.ckpt_dir, 'scaler.npz'),
                 mean=mean, std=std, nodes=np.array(self.nodes))
        logger.success(f"✅ fine-tuned 模型已存：{self.ckpt_dir}（含 scaler.npz）")
