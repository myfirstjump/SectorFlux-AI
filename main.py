import argparse
import logging
import sys
import gc
from datetime import datetime

# 導入 V5.0 規劃的模組架構
from py_module.config import Configuration
from py_module.database import DatabaseManipulation
from py_module.crawler import FinancialCrawler
from py_module.tsf_modules import TSFIntegrator
from py_module.backtest import BacktestManager
from py_module.cluster import L2Clusterer

# 日誌配置：確保記錄每個模組的記憶體與執行狀態
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    handlers=[
        # 若在 Docker 內執行，確保 /app/logs 或 /workspace/logs 資料夾存在
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("SectorFlux_Main")

def run_prediction_pipeline(tsf, args):
    """
    序列化執行預測流水線，以符合 8GB RAM 限制
    """
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
    
    # 【核心修改】新增 seed_history 選項
    parser.add_argument('--item', type=str, required=True, 
                        choices=['crawler', 'seed_history', 'process', 'process_history', 'predict', 'backtest', 'cluster'],
                        help='執行項目: crawler(每日更新), seed_history(建庫), process, process_history(大量新資料更新), predict, backtest, cluster')
    
    parser.add_argument('--horizon', type=str, default='M', choices=['M', 'Q', 'Y'], 
                        help='預測長度: M(月), Q(季), Y(年)')
    
    parser.add_argument('--market', type=str, default='us', choices=['us', 'tw'], help='市場目標')

    args = parser.parse_args()
    config = Configuration()
    db = DatabaseManipulation(config)

    try:
        if args.item == 'crawler':
            logger.info("執行每日增量爬蟲 (抓取近 30 天數據以修正拆股)...")
            crawler = FinancialCrawler(config)
            # 每日排程預設抓 30 天
            crawler.fetch_all_data(market=args.market, history_days=30)
            
        elif args.item == 'seed_history':
            logger.info("⚠️ 啟動歷史資料大灌注 (Seed History) - 預計抓取 30 年數據...")
            crawler = FinancialCrawler(config)
            # 15 年約為 5475 天，這裡設定 5500 天確保涵蓋
            crawler.fetch_all_data(market=args.market, history_days=10950)

        elif args.item == 'process':
            logger.info("開始數據預處理：計算 RS 序列與資金佔比...")
            db.prepare_tsf_features(benchmark='SPY') # SPY歷史相較於VOO來得長很多，回測數據可以涵蓋2000, 2008年等歷史崩盤時刻

        elif args.item == 'process_history':
            logger.info("⚠️ 啟動歷史特徵大灌注 (全量計算 30 年 RS 序列)...")
            # 傳入極大的天數 (或在 SQL 邏輯裡處理 None)，讓它全表運算一次
            db.prepare_tsf_features(benchmark='SPY', days_to_process=None)
        
        elif args.item == 'holdings':
            logger.info("執行ETF Holdings撈取任務...")
            crawler = FinancialCrawler(config)
            crawler.fetch_etf_holdings()

        elif args.item == 'predict':
            # tsf = TSFIntegrator(config, db)
            # run_prediction_pipeline(tsf, args)
            pass
        elif args.item == 'backtest':
            # bt = BacktestManager(config, db)
            # bt.run_walk_forward(horizon=args.horizon)
            pass
        elif args.item == 'cluster':
            # clusterer = L2Clusterer(config, db)
            # clusterer.update_daily_clusters()
            pass
    except Exception as e:
        logger.error(f"任務 [{args.item}] 執行失敗: {str(e)}", exc_info=True)
        sys.exit(1)

    logger.info(f"任務 [{args.item}] 執行完畢。")

if __name__ == "__main__":
    main()