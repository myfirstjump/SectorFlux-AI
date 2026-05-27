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
    _schema_migrated = False  # 每個 process 只跑一次 schema migration

    def __init__(self, config):
        self.config = config
        self.engine = self._create_engine()
        if not DatabaseManipulation._schema_migrated:
            self._run_schema_migrations()
            DatabaseManipulation._schema_migrated = True

    def _run_schema_migrations(self):
        """
        [一次性] 確保 Fact_DailyPrice schema 與當前設計一致。

        雙軌市值欄位設計：
          Market_Cap         = FMP API 原始值，永不被修正邏輯覆寫（不可變基準線）
          Market_Cap_Refined = 拆股 MC 修正後的工作市值（fix_split_mc 寫入此欄）

        Flux 計算一律使用 COALESCE(Market_Cap_Refined, Market_Cap)，
        確保有修正時用修正值，無修正時 fallback 到 FMP 原始值。

        廢棄欄位（v2 起停用）：
          RS_Ratio, Log_Return_RS, ZScore_20D — 原以 Price/RS 為基礎的特徵，
          改為以 Flux 為基礎重新設計，舊欄位由此 migration 移除。
        """
        try:
            with self.engine.connect() as conn:
                # 新增雙軌市值欄位
                conn.execute(text(
                    "IF COL_LENGTH('Fact_DailyPrice', 'Market_Cap_Refined') IS NULL "
                    "ALTER TABLE Fact_DailyPrice ADD Market_Cap_Refined FLOAT NULL"
                ))
                # 移除舊版 Price-based 特徵欄位
                conn.execute(text(
                    "IF COL_LENGTH('Fact_DailyPrice', 'RS_Ratio') IS NOT NULL "
                    "ALTER TABLE Fact_DailyPrice DROP COLUMN RS_Ratio"
                ))
                conn.execute(text(
                    "IF COL_LENGTH('Fact_DailyPrice', 'Log_Return_RS') IS NOT NULL "
                    "ALTER TABLE Fact_DailyPrice DROP COLUMN Log_Return_RS"
                ))
                conn.execute(text(
                    "IF COL_LENGTH('Fact_DailyPrice', 'ZScore_20D') IS NOT NULL "
                    "ALTER TABLE Fact_DailyPrice DROP COLUMN ZScore_20D"
                ))
                conn.commit()
                logger.debug("Schema migration OK: Market_Cap_Refined added; RS/ZScore columns removed.")
        except Exception as e:
            logger.warning(f"⚠️ Schema migration 執行失敗（Fact_DailyPrice 可能尚未建立）: {e}")

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

        # 準備批量資料，並強制將日期轉為字串
        # SQL Server FLOAT 欄位不接受 Python float('nan')；pyodbc 會將 nan 序列化為字串 'nan'
        # 導致 "Error converting data type varchar to float"，必須替換為 None（→ SQL NULL）
        records = df[required_cols].copy()
        records['Date'] = pd.to_datetime(records['Date']).dt.strftime('%Y-%m-%d')
        data_list = [
            {k: (None if isinstance(v, float) and pd.isna(v) else v) for k, v in row.items()}
            for row in records.to_dict('records')
        ]

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
                        target.Shares_Outstanding = source.Shares_Outstanding,
                        target.Market_Cap_Refined = NULL
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
        
        # 抓取該日所有板塊的市值（優先使用修正值，fallback 到 FMP 原始值）
        query = f"""
            SELECT Symbol, COALESCE(Market_Cap_Refined, Market_Cap) AS Market_Cap
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
        
        # 2. 抓取資料（優先使用修正值，fallback 到 FMP 原始值）
        query = """
            SELECT [Date], Symbol, [Close] AS Price,
                   COALESCE(Market_Cap_Refined, Market_Cap) AS Market_Cap
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

    def fix_split_mc_corrections(self, symbols, fmp_api_key, base_url="https://financialmodelingprep.com", dry_run=True):
        """
        [MC 拆股修正] 偵測並修正 FMP 錯誤回溯調整 Market_Cap 所造成的歷史斷崖。

        背景：FMP 在拆股後會將歷史 Price 與 Market_Cap 同時除以拆股比例。Price 調整是
        業界慣例，但 MC 調整是 FMP 的錯誤行為（MC = P×N，拆股不改變 MC）。此錯誤
        導致 Flux 計算跨越拆股日時看到人造的巨額假流量。

        FMP 的三種調整行為（均需處理）：
        - 乾淨型（如 XLK）：拆股後一次性回溯所有歷史，但 post day-1 shares 尚未創建完畢
          → 直接用緊鄰前後比較會低估（1.18x 而非 2x）
        - 提前型（如 XLU）：拆股日前一天 MC 已跳到正確值，更早的記錄才需修正
          → 以「緊鄰前」為比較基準會把已修正的記錄納入
        - 分段型（如 XLY/XLE）：FMP 分兩批回溯，導致 pre-window 已含二次調整值
          → 緊鄰前比較基準已偏高

        偵測策略（threshold 法，天然幂等）：
        1. 以「拆股日前 15~60 天」的穩定中位 MC 作為 stable_pre（避開過渡帶）
        2. 計算閾值 threshold = stable_pre × (1 + ratio) / 2
           （即「已被 FMP 砍半後」與「正確值」的中點）
        3. UPDATE 條件加入 Market_Cap < threshold：
           - 已被 FMP 砍半的記錄：MC ≈ stable_pre → 低於閾值 → 會被修正
           - 過渡帶/已正確的記錄：MC ≈ stable_pre × ratio → 高於閾值 → 自動跳過
        4. 重複執行安全：修正後記錄 MC 升至 stable_pre × ratio，超出閾值，不會再被修正
        """
        import requests

        all_corrections = []

        for symbol in symbols:
            try:
                resp = requests.get(
                    f"{base_url}/stable/splits?symbol={symbol}&apikey={fmp_api_key}",
                    timeout=10
                )
                if resp.status_code != 200 or not resp.json():
                    continue
                splits = sorted(resp.json(), key=lambda x: x["date"], reverse=True)
            except Exception as e:
                logger.warning(f"⚠️ {symbol} 無法取得拆股資料: {e}")
                continue

            for split in splits:
                split_date = split["date"]
                ratio = split["numerator"] / split["denominator"]

                if abs(ratio - 1.0) < 0.001:
                    continue

                # 跳過反向拆股（ratio < 1）及小幅 NAV 調整（ratio < 1.5）
                # 這些案例的偵測邏輯會將自然歷史成長誤判為 FMP 錯誤調整
                if ratio < 1.5:
                    logger.debug(f"  SKIP {symbol} {split_date} ratio={ratio:.4f} (非正向拆股或幅度過小)")
                    continue

                # 1. 取得穩定前期 MC：拆股日前 15~60 天，取中位數避免離群值
                stable_df = self.execute_query("""
                    SELECT TOP 10 Market_Cap
                    FROM Fact_DailyPrice
                    WHERE Symbol = :sym
                      AND Date < DATEADD(day, -14, :sd)
                      AND Date >= DATEADD(day, -75, :sd)
                      AND Market_Cap IS NOT NULL AND Market_Cap > 0
                    ORDER BY Date DESC
                """, params={"sym": symbol, "sd": split_date})

                if stable_df.empty:
                    continue

                stable_pre_mc = stable_df["Market_Cap"].median()
                if stable_pre_mc <= 0:
                    continue

                # 2. 閾值：「已砍半值」與「正確值」之間的中點
                #    (1 + ratio) / 2：ratio=2 → threshold = 1.5 × stable_pre
                threshold = stable_pre_mc * (1 + ratio) / 2

                # 3. 計算在閾值以下（即被 FMP 砍半）且在拆股日前的記錄數
                count_df = self.execute_query("""
                    SELECT COUNT(*) AS cnt FROM Fact_DailyPrice
                    WHERE Symbol     = :sym
                      AND Date       <= :sd
                      AND Market_Cap <  :thr
                      AND Market_Cap IS NOT NULL
                """, params={"sym": symbol, "sd": split_date, "thr": threshold})

                affected_rows = int(count_df.iloc[0, 0]) if not count_df.empty else 0
                if affected_rows == 0:
                    logger.debug(f"  SKIP {symbol} {split_date} {split['numerator']}:{split['denominator']} (無需修正或已修正)")
                    continue

                all_corrections.append({
                    "symbol":     symbol,
                    "split_date": split_date,
                    "ratio":      ratio,
                    "label":      f"{split['numerator']}:{split['denominator']}",
                    "stable_pre": stable_pre_mc / 1e9,
                    "threshold":  threshold / 1e9,
                    "rows":       affected_rows,
                })

            time.sleep(0.05)

        # ── 報告 ──────────────────────────────────────────────
        if not all_corrections:
            logger.success("✅ 掃描完畢：未發現需要修正的 MC 斷崖。")
            return

        logger.info(f"\n{'='*70}")
        logger.info(f"  MC 拆股修正報告（{'DRY-RUN，不寫入' if dry_run else '即將執行寫入'}）")
        logger.info(f"{'='*70}")
        for c in all_corrections:
            logger.info(
                f"  {c['symbol']:<8} {c['split_date']}  {c['label']:<12}"
                f"  stable_pre={c['stable_pre']:.2f}B  threshold={c['threshold']:.2f}B"
                f"  → 修正 {c['rows']:,} 筆"
            )
        logger.info(f"{'='*70}")
        logger.info(f"  共 {len(all_corrections)} 筆拆股事件需修正")

        if dry_run:
            logger.warning("DRY-RUN 模式，未執行任何寫入。加上 --apply 參數以實際修正。")
            return

        # ── 執行修正 ──────────────────────────────────────────
        logger.info("🔧 開始執行 MC 修正...")
        fixed = 0
        for c in all_corrections:
            try:
                with self.engine.connect() as conn:
                    trans = conn.begin()
                    # Market_Cap × ratio；Shares_Outstanding = 新 MC / Close
                    # SQL Server 中 SET 子句各欄位均引用修改前的原始值，故此寫法正確
                    conn.execute(text("""
                        UPDATE Fact_DailyPrice
                        SET
                            Market_Cap          = Market_Cap * :ratio,
                            Shares_Outstanding  = Market_Cap * :ratio / NULLIF([Close], 0)
                        WHERE Symbol     = :sym
                          AND Date       <= :sd
                          AND Market_Cap IS NOT NULL
                          AND Market_Cap <  :thr
                    """), {
                        "ratio": c["ratio"],
                        "sym":   c["symbol"],
                        "sd":    c["split_date"],
                        "thr":   c["threshold"] * 1e9,
                    })
                    trans.commit()
                logger.success(f"  ✅ {c['symbol']} {c['split_date']} ({c['label']}) 修正完畢（{c['rows']:,} 筆）")
                fixed += 1
            except Exception as e:
                logger.error(f"  ❌ {c['symbol']} {c['split_date']} 修正失敗: {e}")

        logger.success(f"\n🎉 MC 拆股修正完成：{fixed}/{len(all_corrections)} 筆事件已處理。")