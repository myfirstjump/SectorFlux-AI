import os
import time
import urllib.parse
import pandas as pd
import uuid
from sqlalchemy import create_engine, text
from loguru import logger
from py_module.config import Configuration 

class DatabaseManipulation:
    def __init__(self, config):
        self.config = config
        self.engine = self._create_engine()

    def _create_engine(self):
        """
        建立 SQL Server 連線引擎 (開啟 fast_executemany 以加速寫入)
        """
        params = urllib.parse.quote_plus(
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={self.config.DB_HOST},{self.config.DB_PORT};"
            f"DATABASE={self.config.DB_NAME};"
            f"UID={self.config.DB_USER};"
            f"PWD={self.config.DB_PASS};"
            f"TrustServerCertificate=yes;"
        )
        return create_engine(f"mssql+pyodbc:///?odbc_connect={params}", fast_executemany=True)

    def upsert_market_data(self, df, table_name="Fact_DailyPrice"):
        """
        將爬蟲資料透過 TempDB + MERGE 寫入 (具備死結重試與暴力覆蓋機制)
        """
        if df is None or df.empty:
            return

        # 確保 DataFrame 欄位與資料庫對齊
        required_cols = ['Date', 'Symbol', 'Open', 'High', 'Low', 'Close', 'Volume', 'Market_Cap', 'Shares_Outstanding']
        for col in required_cols:
            if col not in df.columns:
                df[col] = None 

        unique_suffix = uuid.uuid4().hex[:8]
        staging_table = f"#Staging_{table_name}_{unique_suffix}"
        
        max_retries = 3
        for attempt in range(max_retries):
            trans = None
            try:
                with self.engine.connect() as conn:
                    trans = conn.begin()
                    
                    df[required_cols].to_sql(staging_table, con=conn, if_exists='replace', index=False, 
                                             chunksize=self.config.DB_CHUNK_SIZE)
                    
                    merge_sql = text(f"""
                    MERGE INTO {table_name} AS target
                    USING {staging_table} AS source
                    ON target.Date = source.Date AND target.Symbol = source.Symbol
                    WHEN MATCHED THEN
                        UPDATE SET 
                            target.[Open] = source.[Open],
                            target.High = source.High,
                            target.Low = source.Low,
                            target.[Close] = source.[Close],
                            target.Volume = source.Volume,
                            target.Market_Cap = source.Market_Cap,
                            target.Shares_Outstanding = source.Shares_Outstanding
                    WHEN NOT MATCHED THEN
                        INSERT (Date, Symbol, [Open], High, Low, [Close], Volume, Market_Cap, Shares_Outstanding)
                        VALUES (source.Date, source.Symbol, source.[Open], source.High, source.Low, source.[Close], source.Volume, source.Market_Cap, source.Shares_Outstanding);
                    """)
                    
                    conn.execute(merge_sql)
                    conn.execute(text(f"DROP TABLE {staging_table}"))
                    trans.commit()
                    break 
                    
            except Exception as e:
                if trans: trans.rollback()
                error_msg = str(e)
                if '1205' in error_msg or 'Deadlock' in error_msg:
                    if attempt < max_retries - 1:
                        time.sleep((attempt + 1) * 2)
                        continue
                logger.error(f"❌ Upsert 失敗: {error_msg}")
                raise e
    
    def upsert_etf_holdings(self, df):
        """寫入 ETF 持倉數據 (使用 MERGE)"""
        if df is None or df.empty: return
        
        unique_suffix = uuid.uuid4().hex[:8]
        staging_table = f"#Staging_Holdings_{unique_suffix}"
        
        try:
            with self.engine.connect() as conn:
                trans = conn.begin()
                df.to_sql(staging_table, con=conn, if_exists='replace', index=False)
                
                merge_sql = text(f"""
                MERGE INTO Fact_ETF_Holdings AS target
                USING {staging_table} AS source
                ON target.Date = source.Date AND target.ETF_Symbol = source.ETF_Symbol AND target.Holding_Symbol = source.Holding_Symbol
                WHEN MATCHED THEN
                    UPDATE SET target.[Weight] = source.[Weight], target.Shares = source.Shares, target.Updated_At = GETDATE()
                WHEN NOT MATCHED THEN
                    INSERT (Date, ETF_Symbol, Holding_Symbol, [Weight], Shares, Updated_At)
                    VALUES (source.Date, source.ETF_Symbol, source.Holding_Symbol, source.[Weight], source.Shares, GETDATE());
                """)
                
                conn.execute(merge_sql)
                conn.execute(text(f"DROP TABLE {staging_table}"))
                trans.commit()
        except Exception as e:
            logger.error(f"❌ ETF Holdings Upsert 失敗: {e}")

    def prepare_tsf_features(self, benchmark='SPY', days_to_process=None):


        """
        [高效能] 使用 SQL 原生指令計算 RS、Log_Return_RS 與 20日滾動 Z-Score
        """
        logger.info(f"⚙️ 啟動 RS 特徵工程 (Benchmark: {benchmark})...")
        
        try:
            with self.engine.connect() as conn:
                trans = conn.begin()
                
                # 1. 確保新欄位存在
                conn.execute(text("IF COL_LENGTH('Fact_DailyPrice', 'RS_Ratio') IS NULL ALTER TABLE Fact_DailyPrice ADD RS_Ratio FLOAT"))
                conn.execute(text("IF COL_LENGTH('Fact_DailyPrice', 'Log_Return_RS') IS NULL ALTER TABLE Fact_DailyPrice ADD Log_Return_RS FLOAT"))
                conn.execute(text("IF COL_LENGTH('Fact_DailyPrice', 'ZScore_20D') IS NULL ALTER TABLE Fact_DailyPrice ADD ZScore_20D FLOAT"))
                
                date_filter = ""
                if days_to_process:
                     date_filter = f"AND T1.Date >= DATEADD(day, -{days_to_process}, GETDATE())"

                # 2. 計算 RS_Ratio (相對強度)
                logger.info("🚀 [1/3] 計算 RS_Ratio...")
                sql_rs = text(f"""
                    UPDATE T1
                    SET T1.RS_Ratio = T1.[Close] / NULLIF(T2.[Close], 0)
                    FROM Fact_DailyPrice T1
                    INNER JOIN Fact_DailyPrice T2 ON T1.Date = T2.Date
                    WHERE T2.Symbol = :benchmark
                    {date_filter}
                """)
                conn.execute(sql_rs, {"benchmark": benchmark})

                # 3. 計算 Log_Return_RS (對數報酬率)
                logger.info("🚀 [2/3] 計算 Log_Return_RS (對數報酬率)...")
                sql_log_ret = text(f"""
                    WITH CTE_Prev AS (
                        SELECT Date, Symbol, RS_Ratio,
                               LAG(RS_Ratio) OVER (PARTITION BY Symbol ORDER BY Date) AS Prev_RS
                        FROM Fact_DailyPrice
                    )
                    UPDATE T
                    SET T.Log_Return_RS = LOG(T.RS_Ratio / NULLIF(C.Prev_RS, 0))
                    FROM Fact_DailyPrice T
                    INNER JOIN CTE_Prev C ON T.Date = C.Date AND T.Symbol = C.Symbol
                    WHERE T.RS_Ratio IS NOT NULL AND C.Prev_RS IS NOT NULL
                    {date_filter.replace('T1.', 'T.')}
                """)
                conn.execute(sql_log_ret)

                # 4. 計算 ZScore_20D (20日滾動標準化)
                logger.info("🚀 [3/3] 計算 20日滾動 ZScore_20D...")
                sql_zscore = text(f"""
                    WITH CTE_Stats AS (
                        SELECT Date, Symbol, Log_Return_RS,
                               AVG(Log_Return_RS) OVER (PARTITION BY Symbol ORDER BY Date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS Avg_20D,
                               STDEV(Log_Return_RS) OVER (PARTITION BY Symbol ORDER BY Date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS Std_20D
                        FROM Fact_DailyPrice
                    )
                    UPDATE T
                    SET T.ZScore_20D = (T.Log_Return_RS - C.Avg_20D) / NULLIF(C.Std_20D, 0)
                    FROM Fact_DailyPrice T
                    INNER JOIN CTE_Stats C ON T.Date = C.Date AND T.Symbol = C.Symbol
                    WHERE T.Log_Return_RS IS NOT NULL
                    {date_filter.replace('T1.', 'T.')}
                """)
                conn.execute(sql_zscore)
                
                trans.commit()
                logger.info("✅ 特徵工程全系列 (RS, Log_Return, Z-Score) 批量計算完畢！")
                
        except Exception as e:
            logger.error(f"❌ RS 特徵工程失敗: {e}")
            raise e
        
    def generate_net_flux_matrix(self, past_date, now_date, target_assets=None, hedge_assets=None):
        """
        [歷史事實整理] 計算 ETF Observed Method，動態產出 Sankey 流量矩陣
        支援 L0/L1/L2 擴充，並自動適應標的之動態生命週期 (Inception Dates)
        
        -. input:
            * past_date (str): 觀測起始日期，格式為 'YYYY-MM-DD'。
            * now_date (str): 觀測結束日期，格式為 'YYYY-MM-DD'。
            * target_assets (list[str], optional): 目標分析標的清單。若未提供 (None)，預設載入 L0 之 11 大板塊 ETF。
            * hedge_assets (list[str], optional): 避險緩衝池標的清單。若未提供 (None)，預設載入 ['BIL', 'SHV', 'TLT', 'GLD']。
            
        -. return:
            * flux_matrix (pd.DataFrame | None): 一個 N x N 的資金流向轉移矩陣。
                - Index (Row): 資金流出方 (Source / Outflow)。
                - Column: 資金流入方 (Target / Inflow)。
                - Values: 轉移的實際資金額度 (基於 Proportional Allocation 分配)。
                - 包含一個固定的 'HEDGE' 節點以確保整體流量守恆。若查無資料則回傳 None。
        """
        logger.info(f"🌊 啟動 Net Flux 計算: {past_date} -> {now_date}")
        
        # 1. 處理參數注入 (支援未來 L1, L2 擴充)
        if target_assets is None:
            # 預設 L0 板塊
            target_assets = ['XLK', 'XLF', 'XLV', 'XLE', 'XLI', 'XLY', 'XLP', 'XLU', 'XLB', 'XLRE', 'XLC']
        if hedge_assets is None:
            hedge_assets = ['BIL', 'SHV', 'TLT', 'GLD']
            
        all_tickers = list(set(target_assets + hedge_assets))
        
        # 2. 抓取 Past 與 Now 兩日的資料
        symbols_str = "'" + "','".join(all_tickers) + "'"
        query = f"""
            SELECT Date, Symbol, [Close] AS Price, Market_Cap
            FROM Fact_DailyPrice
            WHERE Date IN ('{past_date}', '{now_date}')
            AND Symbol IN ({symbols_str})
        """
        try:
            df = pd.read_sql(query, self.engine)
        except Exception as e:
            logger.error(f"❌ 讀取資料失敗: {e}")
            return None

        if df.empty:
            logger.warning("⚠️ 查無資料，請確認日期是否為交易日。")
            return None

        df['Date'] = df['Date'].astype(str)
        
        # 3. ⏳ 生命週期過濾 (Dynamic Node Pruning)
        # 找出在 past_date 與 now_date 都「同時存活且有資料」的標的
        available_past = df[df['Date'] == past_date]['Symbol'].tolist()
        available_now = df[df['Date'] == now_date]['Symbol'].tolist()
        alive_tickers = list(set(available_past).intersection(set(available_now)))
        
        # 過濾出本次計算真正活躍的節點
        active_targets = [s for s in target_assets if s in alive_tickers]
        active_hedges = [s for s in hedge_assets if s in alive_tickers]
        
        logger.info(f"📊 活躍節點數: 目標群 {len(active_targets)} 檔, 避險群 {len(active_hedges)} 檔")

        df_past = df[df['Date'] == past_date].set_index('Symbol')
        df_now = df[df['Date'] == now_date].set_index('Symbol')

        # 4. 計算各別活躍 ETF 的淨流量 (F_net)
        f_net_dict = {}
        for sym in alive_tickers:
            mc_past = df_past.loc[sym, 'Market_Cap']
            price_past = df_past.loc[sym, 'Price']
            mc_now = df_now.loc[sym, 'Market_Cap']
            price_now = df_now.loc[sym, 'Price']
            
            if pd.notna(mc_past) and pd.notna(price_past) and price_past != 0:
                r_asset = (price_now / price_past) - 1
                # 公式：F_net = MC_actual,t - (MC_t-n * (1 + r_asset,t))
                f_net = mc_now - (mc_past * (1 + r_asset))
                f_net_dict[sym] = f_net
            else:
                f_net_dict[sym] = 0

        # 5. 整合節點：動態目標群 + 統一的 Hedge Pool
        nodes_f_net = {s: f_net_dict.get(s, 0) for s in active_targets}
        # 即使 active_hedges 為空，HEDGE 節點仍會存在 (值為 0)，做為後續守恆的數學調節池
        nodes_f_net['HEDGE'] = sum(f_net_dict.get(h, 0) for h in active_hedges)

        # 區分資金流出 (Source) 與流入 (Target)
        outflows = {k: abs(v) for k, v in nodes_f_net.items() if v < 0}
        inflows = {k: v for k, v in nodes_f_net.items() if v > 0}
        
        total_out = sum(outflows.values())
        total_in = sum(inflows.values())

        # 6. ⚖️ 流量守恆校正 (Conservation of Flux)
        if total_out > total_in:
            diff = total_out - total_in
            inflows['HEDGE'] = inflows.get('HEDGE', 0) + diff
            total_in = sum(inflows.values())
        elif total_in > total_out:
            diff = total_in - total_out
            outflows['HEDGE'] = outflows.get('HEDGE', 0) + diff
            total_out = sum(outflows.values())

        # 7. 建立動態 N x N 轉移矩陣 (Proportional Allocation)
        matrix_nodes = active_targets + ['HEDGE']
        flux_matrix = pd.DataFrame(0.0, index=matrix_nodes, columns=matrix_nodes)

        # 依比例將流出資金分配至流入節點
        if total_in > 0:
            for source, out_val in outflows.items():
                for target, in_val in inflows.items():
                    flux_matrix.loc[source, target] = out_val * (in_val / total_in)

        logger.success(f"✅ 動態 Flux Matrix 生成完成 (矩陣維度: {len(matrix_nodes)}x{len(matrix_nodes)}, 總轉移資金: ${total_out:,.2f})")
        
        return flux_matrix
