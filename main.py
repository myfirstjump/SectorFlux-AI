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
                        choices=['crawler', 'seed_history', 'process', 'process_history', 'calculate_flux', 'predict', 'backtest', 'cluster', 'test-tensor', 'holdings', 'fix_split_mc', 'sanity_check', 'export_dash', 'build_flux_features', 'finetune'],
                        help='執行項目')
    parser.add_argument('--horizon', type=str, default='M', choices=['M', 'Q', 'Y'], help='預測長度')
    parser.add_argument('--market', type=str, default='us', choices=['us', 'tw'], help='市場目標')
    parser.add_argument('--apply', action='store_true', help='fix_split_mc：實際執行寫入（預設為 dry-run）')
    parser.add_argument('--batch', action='store_true', help='calculate_flux：批次填充 2020 以來全部歷史交易日')

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

        elif args.item in ('process', 'process_history'):
            logger.warning("⚠️ RS/ZScore 特徵計算已停用（Price-based 特徵已移除，待以 Flux-based 特徵重新設計）。")
            sys.exit(1)
        
        elif args.item == 'holdings':
            logger.info("執行 ETF Holdings 撈取任務...")
            crawler = FinancialCrawler(config)
            crawler.fetch_etf_holdings()

        elif args.item == 'fix_split_mc':
            import os
            nasdaq_key = os.getenv("NASDAQ_DL_API_KEY", "")
            if not nasdaq_key:
                logger.error("❌ NASDAQ_DL_API_KEY 未設定，fix_split_mc 需要 SFP 訂閱。")
                sys.exit(1)
            mode = "執行寫入" if args.apply else "DRY-RUN（預覽，不寫入）"
            logger.info(f"🔧 啟動 MC 拆股修正任務 v2（{mode}）...")
            symbols = list(dict.fromkeys(
                config.L0_SECTORS + config.L1_THEMATICS +
                config.L2_UNIVERSE + config.RISK_PROXY
            ))
            db.fix_split_mc_corrections(
                symbols=symbols,
                nasdaq_api_key=nasdaq_key,
                dry_run=not args.apply,
            )

        elif args.item == 'calculate_flux':
            window_size = 21 if args.horizon == 'M' else 63
            l0_str     = "'" + "','".join(config.L0_SECTORS) + "'"

            def _get_past_date(now_str, window):
                """取得 now_str 往前 window 個交易日的起點日期"""
                r = db.execute_query("""
                    SELECT Date FROM (
                        SELECT Date, ROW_NUMBER() OVER (ORDER BY Date DESC) AS rn
                        FROM (SELECT DISTINCT Date FROM Fact_DailyPrice
                              WHERE Date <= :now) t
                    ) r WHERE rn = :rn
                """, params={"now": now_str, "rn": window + 1})
                return r.iloc[0, 0].strftime('%Y-%m-%d') if not r.empty else None

            def _run_one(now_str):
                """計算並持久化單一 now_date 的 Flux + Allocation"""
                past_str = _get_past_date(now_str, window_size)
                if not past_str:
                    return False
                flux_matrix = db.generate_net_flux_matrix(past_str, now_str)
                if flux_matrix is None:
                    return False
                db.upsert_net_flux(flux_matrix, now_str, lookback_window=window_size)
                db.upsert_node_allocation(now_str, now_str,  lookback_window=0)
                db.upsert_node_allocation(now_str, past_str, lookback_window=window_size)
                return True

            if args.batch:
                # ── 歷史批次模式：填充 2020-01-01 以來全部交易日 ────
                logger.info(f"📦 批次模式：填充歷史 Flux（LW={window_size}D）")

                # 取得所有有效 now_date 候選（有 MC 資料的 L0 交易日，從 2020 起）
                all_dates_df = db.execute_query(f"""
                    SELECT DISTINCT [Date]
                    FROM Fact_DailyPrice
                    WHERE Symbol IN ({l0_str})
                      AND COALESCE(Market_Cap_Refined, Market_Cap) > 0
                      AND [Date] >= '2020-01-01'
                    ORDER BY [Date]
                """)
                all_dates = [r.strftime('%Y-%m-%d') for r in all_dates_df.iloc[:, 0]]

                # 已存在的 now_date（跳過，冪等）
                done_df = db.execute_query(
                    "SELECT DISTINCT CONVERT(VARCHAR,Date,23) AS d FROM Fact_NodeFlux "
                    "WHERE Lookback_Window = :lw",
                    params={"lw": window_size}
                )
                done_set = set(done_df['d'].tolist()) if not done_df.empty else set()

                todo = [d for d in all_dates if d not in done_set]
                logger.info(f"  全部 {len(all_dates)} 日，已完成 {len(done_set)}，待計算 {len(todo)} 日")

                ok = skip = err = 0
                for i, now_str in enumerate(todo):
                    try:
                        if _run_one(now_str):
                            ok += 1
                        else:
                            skip += 1
                    except Exception as e:
                        logger.error(f"  ❌ {now_str} 失敗: {e}")
                        err += 1
                    if (i + 1) % 100 == 0 or (i + 1) == len(todo):
                        logger.info(f"  進度 {i+1}/{len(todo)}  ✅{ok} ⏭{skip} ❌{err}")

                logger.success(f"🎉 批次完成：✅{ok} ⏭{skip} ❌{err}")

            else:
                # ── 單日模式：計算最新一筆 ────────────────────────
                res = db.execute_query(f"""
                    SELECT MAX([Date]) FROM Fact_DailyPrice
                    WHERE Symbol IN ({l0_str})
                      AND COALESCE(Market_Cap_Refined, Market_Cap) > 0
                """)
                if res.empty or pd.isna(res.iloc[0, 0]):
                    logger.error("💥 資料庫無有效 MC 數據。")
                    sys.exit(1)
                now_str = res.iloc[0, 0].strftime('%Y-%m-%d')
                past_str = _get_past_date(now_str, window_size)
                logger.info(f"🌊 計算: {past_str} → {now_str} (LW={window_size}D)")
                flux_matrix = db.generate_net_flux_matrix(past_str, now_str)
                if flux_matrix is not None:
                    db.upsert_net_flux(flux_matrix, now_str, lookback_window=window_size)
                    db.upsert_node_allocation(now_str, now_str,  lookback_window=0)
                    db.upsert_node_allocation(now_str, past_str, lookback_window=window_size)
                    logger.success(f"✅ {now_str} Flux + Allocation 已存檔。")

        elif args.item == 'sanity_check':
            """
            能量守恆斷言測試 (Sanity Check)
            ① Fact_NodeFlux：每個 (Date, Lookback_Window) 的節點淨流量總和應 ≈ 0
            ② Fact_NodeAllocation：每個 (Date, Lookback_Window) 的 Weight 總和應 ≈ 1
            違規時 exit(1)，可被 crontab 錯誤通知捕捉。
            """
            logger.info("🔍 執行能量守恆 Sanity Check...")
            violations = 0

            # ── 1. Flux 能量守恆 ──────────────────────────────────
            flux_violations = db.execute_query("""
                SELECT [Date], [Lookback_Window],
                       ABS(SUM(Net_Inflow_Outflow)) AS Leakage
                FROM (
                    SELECT [Date], [Target_Node_ID] AS Node, [Lookback_Window],
                            SUM(Amount) AS Net_Inflow_Outflow
                    FROM Fact_NodeFlux
                    GROUP BY [Date], [Target_Node_ID], [Lookback_Window]
                    UNION ALL
                    SELECT [Date], [Source_Node_ID] AS Node, [Lookback_Window],
                            -SUM(Amount) AS Net_Inflow_Outflow
                    FROM Fact_NodeFlux
                    GROUP BY [Date], [Source_Node_ID], [Lookback_Window]
                ) t
                GROUP BY [Date], [Lookback_Window]
                HAVING ABS(SUM(Net_Inflow_Outflow)) > 0.01
                ORDER BY Leakage DESC
            """)
            if not flux_violations.empty:
                logger.error(f"❌ [Flux 能量守恆] 發現 {len(flux_violations)} 筆違規：")
                for _, r in flux_violations.iterrows():
                    logger.error(f"   Date={r['Date']}  LW={r['Lookback_Window']}  Leakage={r['Leakage']:.4f}")
                violations += len(flux_violations)
            else:
                logger.success("✅ [Flux 能量守恆] 全部通過（總漏能 < 0.01）")

            # ── 2. Allocation Weight 加總 ≈ 1 ────────────────────
            alloc_violations = db.execute_query("""
                SELECT [Date], [Lookback_Window],
                       ABS(SUM(Weight) - 1.0) AS Weight_Error
                FROM Fact_NodeAllocation
                GROUP BY [Date], [Lookback_Window]
                HAVING ABS(SUM(Weight) - 1.0) > 0.001
                ORDER BY Weight_Error DESC
            """)
            if not alloc_violations.empty:
                logger.error(f"❌ [Allocation 加總] 發現 {len(alloc_violations)} 筆違規：")
                for _, r in alloc_violations.iterrows():
                    logger.error(f"   Date={r['Date']}  LW={r['Lookback_Window']}  Error={r['Weight_Error']:.6f}")
                violations += len(alloc_violations)
            else:
                logger.success("✅ [Allocation 加總] 全部通過（Weight Sum 誤差 < 0.001）")

            # ── 統計摘要 ─────────────────────────────────────────
            flux_count  = db.execute_query("SELECT COUNT(DISTINCT CONVERT(VARCHAR,Date,23)+CAST(Lookback_Window AS VARCHAR)) FROM Fact_NodeFlux").iloc[0,0]
            alloc_count = db.execute_query("SELECT COUNT(DISTINCT CONVERT(VARCHAR,Date,23)+CAST(Lookback_Window AS VARCHAR)) FROM Fact_NodeAllocation").iloc[0,0]
            logger.info(f"📊 Flux: {flux_count} (Date×LW) 組合  Allocation: {alloc_count} 組合")

            if violations > 0:
                logger.error(f"💥 Sanity Check 失敗：共 {violations} 筆違規")
                sys.exit(1)
            else:
                logger.success(f"🎉 Sanity Check 全部通過")

        elif args.item == 'build_flux_features':
            """
            建立 TTM 模型輸入：每日 12 節點淨流特徵（Feature_DailyNodeFlux）。
            net_flux_weight = 節點當日淨流金額 / 12 節點總市值（帶正負、可加性）。
            含股數跳變中和（拆股/MC 修正假流量）+ winsorize。
            """
            logger.info("🧬 建立每日 flux 特徵序列（Feature_DailyNodeFlux）...")
            out = db.build_daily_flux_features(dry_run=False)
            if out:
                stats, _ = out
                logger.info(f"📊 {stats['rows']} 交易日 × 12 節點，"
                            f"範圍 {stats['date_min']}~{stats['date_max']}，"
                            f"absmax={stats['absmax']*100:.2f}%（winsorize 前）")

        elif args.item == 'export_dash':
            """
            匯出 Dash 前端所需的靜態 JSON 資料檔（/workspace/app/data/）。
            每日爬蟲 + Flux 計算完成後執行，供 sectorflux_dash 容器讀取。
            輸出：
              alloc_timeseries.json  — LW=0 全部交易日配置（時序圖用）
              spy_prices.json        — SPY 收盤
              sankey_M.json          — LW=21 最新 now_date 的 past/now/flux
              sankey_Q.json          — LW=63 同上
            """
            import json, os
            data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'app', 'data')
            os.makedirs(data_dir, exist_ok=True)

            logger.info(f"📦 匯出 Dash JSON 資料至 {data_dir} ...")

            # 1. 時序圖：LW=0 全部交易日
            df_alloc = db.execute_query("""
                SELECT CONVERT(VARCHAR,Date,23) AS date, Node_ID AS node, Weight AS weight
                FROM Fact_NodeAllocation WITH (NOLOCK)
                WHERE Lookback_Window = 0
                ORDER BY date, node
            """)
            with open(os.path.join(data_dir, 'alloc_timeseries.json'), 'w') as f:
                json.dump(df_alloc.to_dict(orient='records'), f)
            logger.info(f"  alloc_timeseries: {len(df_alloc)} rows")

            # 2. SPY 收盤
            spy = db.execute_query("""
                SELECT CONVERT(VARCHAR,Date,23) AS date, [Close] AS spy_close
                FROM Fact_DailyPrice WITH (NOLOCK)
                WHERE Symbol = 'SPY' AND Date >= '2020-01-01'
                ORDER BY date
            """)
            with open(os.path.join(data_dir, 'spy_prices.json'), 'w') as f:
                json.dump(spy.to_dict(orient='records'), f)
            logger.info(f"  spy_prices: {len(spy)} rows")

            # 3. Sankey 資料（M + Q）
            def _export_sankey(lw, fname):
                r = db.execute_query("""
                    SELECT TOP 1 CONVERT(VARCHAR,Date,23) AS d
                    FROM Fact_NodeAllocation
                    WHERE Lookback_Window = 0
                      AND Date IN (SELECT Date FROM Fact_NodeAllocation WHERE Lookback_Window = :lw)
                    ORDER BY Date DESC
                """, params={"lw": lw})
                if r.empty:
                    logger.warning(f"  LW={lw}: 無資料，跳過")
                    return
                now_date = r.iloc[0, 0]

                now_alloc  = db.execute_query(
                    "SELECT Node_ID, Weight FROM Fact_NodeAllocation WHERE Date=:d AND Lookback_Window=0",
                    params={"d": now_date}
                ).set_index('Node_ID')['Weight'].to_dict()

                past_alloc = db.execute_query(
                    "SELECT Node_ID, Weight FROM Fact_NodeAllocation WHERE Date=:d AND Lookback_Window=:lw",
                    params={"d": now_date, "lw": lw}
                ).set_index('Node_ID')['Weight'].to_dict()

                flux = db.execute_query("""
                    SELECT Source_Node_ID AS src, Target_Node_ID AS tgt,
                           Amount / 1e9   AS amount_b,
                           Flux_Weight    AS flux_weight
                    FROM Fact_NodeFlux
                    WHERE Date = :d AND Lookback_Window = :lw AND Amount > 0
                    ORDER BY Amount DESC
                """, params={"d": now_date, "lw": lw})

                # 12 節點（11 L0 + HEDGE 四檔合計）於 now_date 的總市值（B）
                # 供前端把 allocation transport 的 weight 轉回近似美元金額
                total_mc_b = float(db.execute_query("""
                    SELECT SUM(COALESCE(Market_Cap_Refined, Market_Cap)) / 1e9 AS mc
                    FROM Fact_DailyPrice
                    WHERE [Date] = :d
                      AND Symbol IN ('XLK','XLF','XLV','XLE','XLI','XLY','XLP',
                                     'XLU','XLB','XLRE','XLC','BIL','SHV','TLT','GLD')
                      AND COALESCE(Market_Cap_Refined, Market_Cap) > 0
                """, params={"d": now_date}).iloc[0, 0] or 0.0)

                # 未來佔比：讀 Forecast_NodeFlux 最新一次推論（self-row: Source==Target）
                fut_df = db.execute_query("""
                    SELECT Source_Node_ID AS node, Flux_Weight AS w
                    FROM Forecast_NodeFlux
                    WHERE Lookback_Window = :lw
                      AND Forecast_At = (SELECT MAX(Forecast_At) FROM Forecast_NodeFlux
                                         WHERE Lookback_Window = :lw)
                      AND Source_Node_ID = Target_Node_ID
                """, params={"lw": lw})
                fut_alloc = (fut_df.set_index('node')['w'].to_dict()
                             if not fut_df.empty else None)

                payload = {
                    "now_date":   now_date,
                    "now_alloc":  now_alloc,
                    "past_alloc": past_alloc,
                    "fut_alloc":  fut_alloc,          # None → 前端 fallback 模擬
                    "flux":       flux.to_dict(orient='records'),
                    "total_mc_b": total_mc_b,
                }
                with open(os.path.join(data_dir, fname), 'w') as f:
                    json.dump(payload, f)
                logger.info(f"  {fname}: now={now_date}, flux={len(flux)} rows, "
                            f"forecast={'有' if fut_alloc else '無(模擬)'}")

            _export_sankey(21, 'sankey_M.json')
            _export_sankey(63, 'sankey_Q.json')
            logger.success("✅ Dash JSON 資料匯出完成")

        elif args.item == 'predict':
            logger.info("🔮 [TSF] 啟動 Flux 預測：IBM Granite-TTM-r2（zero-shot, 12ch）...")
            try:
                from py_module.tsf_modules import TSFIntegrator
                tsf = TSFIntegrator(config, db)

                # 1. 建構輸入張量（最近 512 日，12ch，已 z-score）
                input_tensor, used_dates, scaler = tsf.create_input_tensor()
                if input_tensor is None:
                    logger.error("💥 無法建構輸入張量（請先 build_flux_features）。")
                    sys.exit(1)

                # 2. now allocation（Fact_NodeAllocation LW=0 最新快照）作為加法基準
                r = db.execute_query("""
                    SELECT TOP 1 CONVERT(VARCHAR,Date,23) d FROM Fact_NodeAllocation
                    WHERE Lookback_Window=0 ORDER BY Date DESC
                """)
                now_date  = r.iloc[0, 0]
                now_alloc = db.execute_query(
                    "SELECT Node_ID, Weight FROM Fact_NodeAllocation WHERE Date=:d AND Lookback_Window=0",
                    params={"d": now_date}
                ).set_index('Node_ID')['Weight'].to_dict()

                # 3. 12 節點總市值（把佔比換算近似 $）
                total_mc_b = float(db.execute_query("""
                    SELECT SUM(COALESCE(Market_Cap_Refined,Market_Cap))/1e9
                    FROM Fact_DailyPrice
                    WHERE [Date]=:d AND Symbol IN
                      ('XLK','XLF','XLV','XLE','XLI','XLY','XLP','XLU','XLB','XLRE','XLC','BIL','SHV','TLT','GLD')
                      AND COALESCE(Market_Cap_Refined,Market_Cap)>0
                """, params={"d": now_date}).iloc[0, 0] or 0.0)

                # 4. 推論 + 重建未來佔比
                forecast = tsf.run_ttm_inference(input_tensor)
                future   = tsf.reconstruct_future_allocation(forecast, now_alloc, scaler=scaler)

                if future is not None:
                    db.upsert_forecast_allocation(now_date, future, total_mc_b=total_mc_b,
                                                  model_version='TTM-r2-zeroshot')
                    logger.success(f"✅ [TSF] 未來佔比已存入 Forecast_NodeFlux（基準日 {now_date}）。")

            except Exception as e:
                logger.error(f"💥 [TSF] 預測失敗: {e}")

        elif args.item == 'finetune':
            logger.info("🎯 [Finetune] TTM-r2 head-only fine-tune（train ≤2024）...")
            from py_module.finetune import FineTuner
            ft = FineTuner(config, db)
            ft.run(epochs=40, lr=1e-3, batch=32)

        elif args.item == 'backtest':
            logger.info("📏 [Backtest] 預測準度回測（單一切分，每週 anchor）...")
            from py_module.backtest import BacktestManager
            BacktestManager(config, db).run(test_start='2025-01-01', anchor_freq=5)

    except Exception as e:
        logger.exception(f"💥 任務 [{args.item}] 執行失敗: {e}")
        sys.exit(1)

    logger.info(f"任務 [{args.item}] 執行完畢。")

if __name__ == "__main__":
    main()