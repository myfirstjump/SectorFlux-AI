import os
import time
from datetime import datetime
import urllib.parse
import pandas as pd
import uuid
from sqlalchemy import create_engine, text, bindparam
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

    def execute_query(self, query: str, params: dict = None) -> pd.DataFrame:
        """[泛用數據提取] 解決 Pandas 2.3+ 不認得 Connection 的問題"""
        try:
            with self.engine.connect() as conn:
                stmt = text(query)
                if params:
                    for key, value in params.items():
                        if isinstance(value, (list, tuple)):
                            stmt = stmt.bindparams(bindparam(key, expanding=True))
                
                result = conn.execute(stmt, params or {})
                # 💡 手動從結果集建立 DataFrame，避開 pd.read_sql 的偵測 bug
                return pd.DataFrame(result.fetchall(), columns=result.keys())
        except Exception as e:
            logger.error(f"❌ 查詢提取失敗: {str(e)}")
            return pd.DataFrame()

    def upsert_market_data(self, df, table_name="Fact_DailyPrice"):
        """
        「建表」、「注入」與「合併」
        """
        if df is None or df.empty:
            return

        # 1. 數據預處理：確保欄位齊全並轉換為原生 Python 格式
        required_cols = ['Date', 'Symbol', 'Open', 'High', 'Low', 'Close', 'Volume', 'Market_Cap', 'Shares_Outstanding']
        for col in required_cols:
            if col not in df.columns:
                df[col] = None 

        # 準備批量資料 (List of Tuples)，並強制將日期轉為字串
        # 使用 .to_dict('records') 是為了配合具名參數 :col 注入
        records = df[required_cols].copy()
        records['Date'] = pd.to_datetime(records['Date']).dt.strftime('%Y-%m-%d')
        data_list = records.to_dict('records')

        unique_suffix = uuid.uuid4().hex[:8]
        staging_table = f"#Staging_{table_name}_{unique_suffix}"
        
        try:
            # 💡 關鍵修正：所有的動作 (建表、注入、MERGE) 都在同一個連線中完成
            with self.engine.connect() as conn:
                trans = conn.begin()

                # 2. 手動建立暫存表 (與 Fact_DailyPrice 結構對齊)
                conn.execute(text(f"""
                CREATE TABLE {staging_table} (
                    Date DATE,
                    Symbol VARCHAR(20),
                    [Open] FLOAT,
                    High FLOAT,
                    Low FLOAT,
                    [Close] FLOAT,
                    Volume BIGINT,
                    Market_Cap FLOAT,
                    Shares_Outstanding FLOAT
                )
                """))

                # 3. 執行原生批量注入 (executemany)
                # 使用 :col 語法讓 SQLAlchemy 自動映射字典內容
                insert_sql = text(f"""
                INSERT INTO {staging_table} (Date, Symbol, [Open], High, Low, [Close], Volume, Market_Cap, Shares_Outstanding)
                VALUES (:Date, :Symbol, :Open, :High, :Low, :Close, :Volume, :Market_Cap, :Shares_Outstanding)
                """)
                conn.execute(insert_sql, data_list)

                # 4. 執行 MERGE 邏輯
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
                
                # 5. 提交並清理
                trans.commit()
                logger.success(f"✅ {table_name} 數據 Upsert 成功 (共 {len(df)} 筆)")

        except Exception as e:
            logger.error(f"❌ {table_name} Upsert 失敗: {str(e)}")
            raise e


    def upsert_node_allocation(self, date_str, lookback_window=512):
        """
        [事實持久化] 計算並儲存特定日期的資金分佈比例 (Fact_NodeAllocation)
        """
        logger.info(f"📊 計算 {date_str} 的資金分配比例 (L0)...")
        
        sectors = self.config.L0_SECTORS
        symbols_str = "'" + "','".join(sectors) + "'"
        
        # 抓取該日所有板塊的市值
        query = f"""
            SELECT Symbol, Market_Cap 
            FROM Fact_DailyPrice 
            WHERE [Date] = :d AND Symbol IN ({symbols_str})
        """
        df = self.execute_query(query, params={"d": date_str})
        
        if df.empty:
            logger.warning(f"⚠️ {date_str} 無法取得市值數據。")
            return

        total_mc = df['Market_Cap'].sum()
        
        # 準備寫入資料
        data_list = []
        for _, row in df.iterrows():
            data_list.append({
                "date": date_str,
                "node": row['Symbol'],
                "window": lookback_window,
                "weight": float(row['Market_Cap'] / total_mc) if total_mc > 0 else 0
            })

        # 同樣採用 Staging + Merge 模式
        table_name = "Fact_NodeAllocation"
        staging_table = f"#Staging_Alloc_{uuid.uuid4().hex[:8]}"
        
        try:
            with self.engine.connect() as conn:
                trans = conn.begin()
                conn.execute(text(f"CREATE TABLE {staging_table} (d DATE, n VARCHAR(20), w INT, weight FLOAT)"))
                
                # 插入暫存
                conn.execute(text(f"INSERT INTO {staging_table} (d, n, w, weight) VALUES (:date, :node, :window, :weight)"), data_list)
                
                # 合併至正式表
                merge_sql = text(f"""
                MERGE INTO {table_name} AS T
                USING {staging_table} AS S
                ON T.[Date] = S.d AND T.[Node_ID] = S.n AND T.[Lookback_Window] = S.w
                WHEN MATCHED THEN
                    UPDATE SET T.[Weight] = S.weight, T.[Updated_At] = GETDATE()
                WHEN NOT MATCHED THEN
                    INSERT ([Date], [Node_ID], [Lookback_Window], [Weight], [Updated_At])
                    VALUES (S.d, S.n, S.w, S.weight, GETDATE());
                """)
                conn.execute(merge_sql)
                trans.commit()
                logger.success(f"✅ {date_str} 的 Fact_NodeAllocation 已存檔。")
        except Exception as e:
            logger.error(f"❌ Fact_NodeAllocation 寫入失敗: {e}")

    def save_predictions(self, latest_date, flux_results, model_version="Granite-TTM-v2", context_window=512):
        """
        [模型預測持久化] 將推論出的未來流向存入 Forecast_NodeFlux。
        """
        if flux_results is None:
            return

        forecast_at = datetime.now() # 記錄精確推論時間
        data_list = []
        
        # 定義 Horizon 與天數的映射
        horizon_map = {
            'M': 21,
            'Q': 63
        }

        # 1. 整理數據格式
        for horizon, matrix in flux_results.items():
            if matrix is None or matrix.empty:
                continue
            
            # 計算預期達成日 (Target_Date)
            days = horizon_map.get(horizon, 21)
            target_date = pd.to_datetime(latest_date) + pd.Timedelta(days=days)
            target_date_str = target_date.strftime('%Y-%m-%d')

            # 將 12x12 矩陣轉為長表格式
            flux_df = matrix.stack().reset_index()
            flux_df.columns = ['Source', 'Target', 'Amount']
            
            total_flux = flux_df['Amount'].sum()

            for _, row in flux_df.iterrows():
                data_list.append({
                    "forecast_at": forecast_at,
                    "target_date": target_date_str,
                    "source": row['Source'],
                    "target": row['Target'],
                    "lookback": context_window,
                    "amount": float(row['Amount']),
                    "weight": float(row['Amount'] / total_flux) if total_flux != 0 else 0,
                    "confidence": 1.0, # 目前預設為 1.0，未來可接入模型機率輸出
                    "version": model_version
                })

        if not data_list:
            logger.warning("⚠️ 無有效預測數據可供存檔。")
            return

        table_name = "Forecast_NodeFlux"
        unique_suffix = uuid.uuid4().hex[:8]
        staging_table = f"#Staging_Forecast_{unique_suffix}"

        try:
            with self.engine.connect() as conn:
                trans = conn.begin()

                # 2. 建立臨時暫存表
                conn.execute(text(f"""
                CREATE TABLE {staging_table} (
                    forecast_at DATETIME, target_date DATE,
                    source VARCHAR(20), target VARCHAR(20),
                    lookback INT, amount FLOAT, weight FLOAT,
                    confidence FLOAT, version VARCHAR(50)
                )
                """))

                # 3. 原生批量寫入
                insert_sql = text(f"""
                INSERT INTO {staging_table} (forecast_at, target_date, source, target, lookback, amount, weight, confidence, version)
                VALUES (:forecast_at, :target_date, :source, :target, :lookback, :amount, :weight, :confidence, :version)
                """)
                conn.execute(insert_sql, data_list)

                # 4. MERGE 進入正式表
                merge_sql = text(f"""
                MERGE INTO {table_name} AS T
                USING {staging_table} AS S
                ON T.Target_Date = S.target_date 
                   AND T.Source_Node_ID = S.source 
                   AND T.Target_Node_ID = S.target
                WHEN MATCHED THEN
                    UPDATE SET 
                        T.Forecast_At = S.forecast_at,
                        T.Amount = S.amount,
                        T.Flux_Weight = S.weight,
                        T.Confidence = S.confidence,
                        T.Updated_At = GETDATE()
                WHEN NOT MATCHED THEN
                    INSERT (Forecast_At, Target_Date, Source_Node_ID, Target_Node_ID, Lookback_Window, Amount, Flux_Weight, Confidence, Model_Version)
                    VALUES (S.forecast_at, S.target_date, S.source, S.target, S.lookback, S.amount, S.weight, S.confidence, S.version);
                """)
                conn.execute(merge_sql)
                
                trans.commit()
                logger.success(f"✅ {len(data_list)} 筆預測流向已更新至 {table_name} (基準日: {latest_date})")

        except Exception as e:
            logger.error(f"❌ 預測流向持久化失敗: {e}")

    def upsert_etf_holdings(self, df):
        """
        寫入 ETF 持倉數據 (完全移除 to_sql，改用原生 SQL 批量注入)。
        確保與 Pandas 2.3+ 相容，並解決 Session 隔離問題。
        """
        if df is None or df.empty:
            return
            
        # 1. 數據預處理：轉換日期格式並封裝為 Dictionary List
        records_df = df.copy()
        # 確保日期欄位為標準字串，避免 SQL 引擎解析錯誤
        records_df['Date'] = pd.to_datetime(records_df['Date']).dt.strftime('%Y-%m-%d')
        data_list = records_df.to_dict('records')

        table_name = "Fact_ETF_Holdings"
        unique_suffix = uuid.uuid4().hex[:8]
        staging_table = f"#Staging_Holdings_{unique_suffix}"
        
        try:
            with self.engine.connect() as conn:
                trans = conn.begin()

                # 2. 手動建立與 Fact_ETF_Holdings 結構一致的暫存表
                conn.execute(text(f"""
                CREATE TABLE {staging_table} (
                    Date DATE,
                    ETF_Symbol VARCHAR(20),
                    Holding_Symbol VARCHAR(20),
                    [Weight] FLOAT,
                    Shares FLOAT
                )
                """))

                # 3. 執行原生批量注入 (executemany)
                # 使用命名參數 :Date, :ETF_Symbol 等與字典 Keys 對應
                insert_sql = text(f"""
                INSERT INTO {staging_table} (Date, ETF_Symbol, Holding_Symbol, [Weight], Shares)
                VALUES (:Date, :ETF_Symbol, :Holding_Symbol, :Weight, :Shares)
                """)
                conn.execute(insert_sql, data_list)

                # 4. 執行 MERGE 邏輯 (基於 Date, ETF, Holding 三位一體作為主鍵)
                merge_sql = text(f"""
                MERGE INTO {table_name} AS target
                USING {staging_table} AS source
                ON target.Date = source.Date 
                   AND target.ETF_Symbol = source.ETF_Symbol 
                   AND target.Holding_Symbol = source.Holding_Symbol
                WHEN MATCHED THEN
                    UPDATE SET 
                        target.[Weight] = source.[Weight], 
                        target.Shares = source.Shares, 
                        target.Updated_At = GETDATE()
                WHEN NOT MATCHED THEN
                    INSERT (Date, ETF_Symbol, Holding_Symbol, [Weight], Shares, Updated_At)
                    VALUES (source.Date, source.ETF_Symbol, source.Holding_Symbol, source.[Weight], source.Shares, GETDATE());
                """)
                conn.execute(merge_sql)
                
                # 5. 提交並清理
                trans.commit()
                logger.success(f"✅ {table_name} 持倉數據 Upsert 成功 (原生 SQL 注入)")

        except Exception as e:
            logger.error(f"❌ ETF Holdings Upsert 失敗: {str(e)}")
            # 發生錯誤時確保回滾
            raise e

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
        [運算引擎] 計算 ETF Observed Method，產出 12x12 資金流轉矩陣
        """
        logger.info(f"🌊 啟動 Net Flux 事實計算: {past_date} -> {now_date}")
        
        # 1. 動態參數處理
        if target_assets is None:
            target_assets = self.config.L0_SECTORS # 優先從 config 讀取
        if hedge_assets is None:
            hedge_assets = ['BIL', 'SHV', 'TLT', 'GLD']
            
        all_tickers = list(set(target_assets + hedge_assets))
        
        # 2. 抓取資料 (使用具名參數與 Tuple 以利 SQL 自動擴展)
        query = """
            SELECT [Date], Symbol, [Close] AS Price, Market_Cap 
            FROM Fact_DailyPrice 
            WHERE [Date] IN :dates AND Symbol IN :symbols
        """
        df = self.execute_query(query, params={
            "dates": (past_date, now_date), 
            "symbols": tuple(all_tickers)
        })

        if df.empty:
            logger.warning(f"⚠️ {past_date} 或 {now_date} 查無資料，請確認是否為交易日。")
            return None

        df['Date'] = df['Date'].astype(str)
        
        # 3. ⏳ 生命週期過濾 (Dynamic Node Pruning)
        available_past = df[df['Date'] == past_date]['Symbol'].tolist()
        available_now = df[df['Date'] == now_date]['Symbol'].tolist()
        alive_tickers = list(set(available_past).intersection(set(available_now)))
        
        active_targets = [s for s in target_assets if s in alive_tickers]
        active_hedges = [s for s in hedge_assets if s in alive_tickers]
        
        logger.info(f"📊 活躍節點: 目標群 {len(active_targets)} 檔, 避險群 {len(active_hedges)} 檔")

        df_past = df[df['Date'] == past_date].set_index('Symbol')
        df_now = df[df['Date'] == now_date].set_index('Symbol')

        # 4. 核心運算：分離價格效應，計算淨流量 (F_net)
        f_net_dict = {}
        for sym in alive_tickers:
            mc_p, pr_p = df_past.loc[sym, 'Market_Cap'], df_past.loc[sym, 'Price']
            mc_n, pr_n = df_now.loc[sym, 'Market_Cap'], df_now.loc[sym, 'Price']
            
            if pd.notna(mc_p) and pr_p > 0:
                # 計算該資產的自然增長率 (Return)
                r_asset = (pr_n / pr_p) - 1
                # 淨流量 = 實際市值 - (舊市值 * (1 + 報酬率))
                f_net = mc_n - (mc_p * (1 + r_asset))
                f_net_dict[sym] = f_net
            else:
                f_net_dict[sym] = 0

        # 5. 整合為 12 個決策節點
        nodes_f_net = {s: f_net_dict.get(s, 0) for s in active_targets}
        # 避險池視為單一資金回收站
        nodes_f_net['HEDGE'] = sum(f_net_dict.get(h, 0) for h in active_hedges)

        # 區分 Source (流出 < 0) 與 Target (流入 > 0)
        outflows = {k: abs(v) for k, v in nodes_f_net.items() if v < 0}
        inflows = {k: v for k, v in nodes_f_net.items() if v > 0}
        
        total_out = sum(outflows.values())
        total_in = sum(inflows.values())

        # 6. ⚖️ 流量守恆校正 (確保 Sankey 圖左右平衡)
        # 若流出 > 流入，差額歸類為 HEDGE 流入；反之亦然
        if total_out > total_in:
            diff = total_out - total_in
            inflows['HEDGE'] = inflows.get('HEDGE', 0) + diff
            total_in = sum(inflows.values())
        elif total_in > total_out:
            diff = total_in - total_out
            outflows['HEDGE'] = outflows.get('HEDGE', 0) + diff
            total_out = sum(outflows.values())

        # 7. 建立 12x12 轉移矩陣 (Proportional Allocation)
        matrix_nodes = active_targets + (['HEDGE'] if 'HEDGE' not in active_targets else [])
        flux_matrix = pd.DataFrame(0.0, index=matrix_nodes, columns=matrix_nodes)

        if total_in > 0:
            for source, out_val in outflows.items():
                for target, in_val in inflows.items():
                    # 比例分配：流出量 * (該流入點佔總流入之比例)
                    flux_matrix.loc[source, target] = out_val * (in_val / total_in)

        logger.success(f"✅ Flux Matrix 生成完成 (總轉移資金: ${total_out:,.2f})")
        return flux_matrix

    def upsert_net_flux(self, flux_matrix, now_date, lookback_window=21):
        """
        [事實持久化] 將 12x12 流量矩陣寫入 Fact_NodeFlux (不使用 to_sql)
        
        -. input:
            * flux_matrix (pd.DataFrame): 12x12 的轉移矩陣 (Index: Source, Column: Target)
            * now_date (str/date): 觀測結束日期
            * lookback_window (int): 觀測窗口天數，預設 21 (月度流量事實)
        """
        if flux_matrix is None or flux_matrix.empty:
            logger.warning("⚠️ 無事實流量數據可供寫入。")
            return

        # 1. 矩陣轉長表並計算權重
        flux_df = flux_matrix.stack().reset_index()
        flux_df.columns = ['Source', 'Target', 'Amount']
        
        total_flux = flux_df['Amount'].sum()
        
        # 準備批量資料 (List of Tuples)
        records = []
        for _, row in flux_df.iterrows():
            records.append({
                "date": now_date,
                "source": row['Source'],
                "target": row['Target'],
                "lookback": lookback_window,
                "amount": float(row['Amount']),
                "weight": float(row['Amount'] / total_flux) if total_flux != 0 else 0
            })

        table_name = "Fact_NodeFlux"
        staging_table = f"#Staging_NodeFlux_{uuid.uuid4().hex[:8]}"

        try:
            with self.engine.connect() as conn:
                trans = conn.begin()

                # 2. 自動建表防呆 (Fact_NodeFlux)
                conn.execute(text(f"""
                IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='{table_name}' AND xtype='U')
                CREATE TABLE {table_name} (
                    [Date] DATE NOT NULL,
                    [Source_Node_ID] VARCHAR(20) NOT NULL,
                    [Target_Node_ID] VARCHAR(20) NOT NULL,
                    [Lookback_Window] INT NOT NULL,
                    [Amount] FLOAT,
                    [Flux_Weight] FLOAT,
                    [Updated_At] DATETIME DEFAULT GETDATE(),
                    CONSTRAINT PK_{table_name} PRIMARY KEY ([Date], [Source_Node_ID], [Target_Node_ID], [Lookback_Window])
                )
                """))

                # 3. 建立臨時暫存表
                conn.execute(text(f"""
                CREATE TABLE {staging_table} (
                    d DATE, s VARCHAR(20), t VARCHAR(20), lw INT, amt FLOAT, w FLOAT
                )
                """))

                # 4. 原生批量注入
                insert_sql = text(f"""
                INSERT INTO {staging_table} (d, s, t, lw, amt, w)
                VALUES (:date, :source, :target, :lookback, :amount, :weight)
                """)
                conn.execute(insert_sql, records)

                # 5. 執行高效 MERGE (UPSERT)
                merge_sql = text(f"""
                MERGE INTO {table_name} AS T
                USING {staging_table} AS S
                ON T.[Date] = S.d 
                   AND T.[Source_Node_ID] = S.s 
                   AND T.[Target_Node_ID] = S.t 
                   AND T.[Lookback_Window] = S.lw
                WHEN MATCHED THEN
                    UPDATE SET 
                        T.[Amount] = S.amt, 
                        T.[Flux_Weight] = S.w, 
                        T.[Updated_At] = GETDATE()
                WHEN NOT MATCHED THEN
                    INSERT ([Date], [Source_Node_ID], [Target_Node_ID], [Lookback_Window], [Amount], [Flux_Weight], [Updated_At])
                    VALUES (S.d, S.s, S.t, S.lw, S.amt, S.w, GETDATE());
                """)
                
                conn.execute(merge_sql)
                trans.commit()
                logger.success(f"✅ {now_date} 之事實流量矩陣 (Fact_NodeFlux) 已存檔。")

        except Exception as e:
            logger.error(f"❌ {table_name} 寫入失敗: {e}")