import argparse
import sys
import gc
import logging
from datetime import datetime
import pandas as pd
import numpy as np
from loguru import logger

# 修正引用
from py_module.config import Configuration
from py_module.database import DatabaseManipulation
from py_module.crawler import FinancialCrawler
from py_module.tsf_modules import TSFIntegrator

# ==========================================
# 🛡️ 終極日誌系統 (徹底取代內建 logging)
# ==========================================
class InterceptHandler(logging.Handler):
    def emit(self, record):
        try:
            level = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno
        frame, depth = logging.currentframe(), 2
        while frame.f_code.co_filename == logging.__file__:
            frame = frame.f_back
            depth += 1
        logger.opt(depth=depth, exception=record.exc_info).log(level, record.getMessage())

def setup_logging():
    logger.remove() # 移除預設輸出
    fmt = "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>"
    logger.add(sys.stdout, format=fmt, level="INFO")
    
    # 強制攔截所有第三方套件
    logging.basicConfig(handlers=[InterceptHandler()], level=0, force=True)
    for name in ("transformers", "sqlalchemy", "tsfm", "pandas", "pyodbc"):
        logging.getLogger(name).handlers = [InterceptHandler()]
        logging.getLogger(name).propagate = False

setup_logging()

# ==========================================
# 🚀 業務邏輯
# ==========================================

def run_prediction_pipeline(tsf, args):
    """序列化執行預測流水線，以符合 8GB RAM 限制"""
    logger.info("啟動 L0 階層預測 (Ensemble Mode)...")
    tsf.run_l0_ensemble(market=args.market, horizon=args.horizon)
    gc.collect()

    logger.info("啟動 L1 階層預測 (Thematic Cascading)...")
    tsf.run_l1_cascading(horizon=args.horizon)
    gc.collect()

    logger.info("啟動 L2 階層預測 (TTM Multivariate)...")
    tsf.run_l2_ttm(horizon=args.horizon)
    gc.collect()

def main():
    parser = argparse.ArgumentParser(description="SectorFlux-AI 量化調度入口")
    parser.add_argument('--item', type=str, required=True, 
                        choices=['crawler', 'seed_history', 'process', 'process_history', 'calculate_flux', 'predict', 'backtest', 'cluster', 'test-tensor', 'holdings'],
                        help='執行項目')
    parser.add_argument('--horizon', type=str, default='M', choices=['M', 'Q', 'Y'], help='預測長度')
    parser.add_argument('--market', type=str, default='us', choices=['us', 'tw'], help='市場目標')

    args = parser.parse_args()
    config = Configuration()
    db = DatabaseManipulation(config)

    try:
        if args.item == 'crawler':
            logger.info("執行每日增量爬蟲 (抓取近 30 天數據)...")
            crawler = FinancialCrawler(config)
            crawler.fetch_all_data(market=args.market, history_days=30)
            
        elif args.item == 'seed_history':
            logger.warning("⚠️ 啟動歷史資料大灌注 (Seed History) - 預計抓取 30 年數據...")
            crawler = FinancialCrawler(config)
            crawler.fetch_all_data(market=args.market, history_days=10950)

        elif args.item == 'process':
            logger.info("開始數據預處理：計算 RS 序列與資金佔比...")
            db.prepare_tsf_features(benchmark='SPY')

        elif args.item == 'process_history':
            logger.warning("⚠️ 啟動歷史特徵大灌注 (全量計算 30 年 RS 序列)...")
            db.prepare_tsf_features(benchmark='SPY', days_to_process=None)
        
        elif args.item == 'holdings':
            logger.info("執行 ETF Holdings 撈取任務...")
            crawler = FinancialCrawler(config)
            crawler.fetch_all_holdings()

        elif args.item == 'calculate_flux':
            # 1. 取得最新交易日作為 Now
            res = db.execute_query("SELECT MAX(Date) FROM Fact_DailyPrice")
            if res.empty or pd.isna(res.iloc[0, 0]):
                logger.error("💥 資料庫無數據，無法計算流量。")
                return
                
            now_date = res.iloc[0, 0]
            now_str = now_date.strftime('%Y-%m-%d')
            
            # 2. 定義計算窗口 (M=21, Q=63)
            window_size = 21 if args.horizon == 'M' else 63
            
            # 3. 取得 N 個交易日前作為 Past
            # 透過 row_num = window + 1 取得起點日期
            past_query = """
                SELECT Date FROM (
                    SELECT Date, ROW_NUMBER() OVER (ORDER BY Date DESC) as row_num
                    FROM (SELECT DISTINCT Date FROM Fact_DailyPrice WHERE Date <= :now) t
                ) r WHERE row_num = :rn
            """
            past_res = db.execute_query(past_query, params={"now": now_str, "rn": window_size + 1})
            
            if not past_res.empty:
                past_date = past_res.iloc[0, 0]
                past_str = past_date.strftime('%Y-%m-%d')
                logger.info(f"🌊 正在計算過去事實流向: {past_str} -> {now_str} (Window: {window_size}D)")
                
                # A. 產出流量矩陣
                flux_matrix = db.generate_net_flux_matrix(past_date=past_str, now_date=now_str)
                
                if flux_matrix is not None:
                    # B. 持久化事實流向 (Fact_NodeFlux)
                    db.upsert_net_flux(flux_matrix, now_str, lookback_window=window_size)
                    
                    # C. 🔥 同步持久化事實水位 (Fact_NodeAllocation)
                    # 這能讓前端知道這筆流量發生時，各板塊原本的資金比例
                    db.upsert_node_allocation(now_str, lookback_window=window_size)
                    
                    logger.success(f"✅ {now_str} 的流量真相與水位配置已完整同步至資料庫。")
            else:
                logger.error(f"無法找到足夠的歷史日期 (需要往回找 {window_size} 筆) 來計算流量事實。")

        elif args.item == 'predict':
            logger.info("🔮 [TSF] 啟動 AI 趨勢預測任務：IBM Granite-TTM (r2)...")
            
            try:
                # 1. 獲取基準日期
                latest_date_query = "SELECT MAX(Date) FROM Fact_DailyPrice"
                latest_date = db.execute_query(latest_date_query).iloc[0, 0]
                
                # 2. 抓取當前分配權重 (用於計算 Flux)
                alloc_query = "SELECT Node_ID, Weight FROM Fact_NodeAllocation WHERE Date = :dt AND Lookback_Window = :lw"
                alloc_df = db.execute_query(alloc_query, params={"dt": latest_date, "lw": 512})
                current_alloc = dict(zip(alloc_df['Node_ID'], alloc_df['Weight']))

                # 3. 張量建構與推論
                tsf = TSFIntegrator(config, db)
                input_tensor = tsf.create_input_tensor(db, latest_date)
                forecast_values = tsf.run_ttm_inference(input_tensor)
                
                if forecast_values is not None:
                    # 4. 計算 12x12 未來流向矩陣
                    flux_results = tsf.calculate_future_flux(forecast_values, current_alloc)
                    
                    # 5. 持久化存入新表 Forecast_NodeFlux
                    db.save_predictions(latest_date, flux_results, context_window=512)
                    logger.success("✅ [TSF] 預測流向已成功存入 Forecast_NodeFlux 表。")
                    
            except Exception as e:
                logger.error(f"💥 [TSF] 預測失敗: {e}")

    except Exception as e:
        logger.exception(f"💥 任務 [{args.item}] 執行失敗: {e}")
        sys.exit(1)

    logger.info(f"任務 [{args.item}] 執行完畢。")

if __name__ == "__main__":
    main()