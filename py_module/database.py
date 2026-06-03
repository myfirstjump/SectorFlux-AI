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
                # ── Fact_DailyPrice 欄位維護 ─────────────────────────
                conn.execute(text(
                    "IF COL_LENGTH('Fact_DailyPrice', 'Market_Cap_Refined') IS NULL "
                    "ALTER TABLE Fact_DailyPrice ADD Market_Cap_Refined FLOAT NULL"
                ))
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

                # ── Fact_NodeAllocation（12 節點資金分佈比例）────────
                conn.execute(text("""
                    IF NOT EXISTS (
                        SELECT 1 FROM sysobjects
                        WHERE name='Fact_NodeAllocation' AND xtype='U'
                    )
                    CREATE TABLE Fact_NodeAllocation (
                        [Date]            DATE         NOT NULL,
                        [Node_ID]         VARCHAR(20)  NOT NULL,
                        [Lookback_Window] INT          NOT NULL,
                        [Weight]          FLOAT        NOT NULL,
                        [Updated_At]      DATETIME     DEFAULT GETDATE(),
                        CONSTRAINT PK_Fact_NodeAllocation
                            PRIMARY KEY ([Date], [Node_ID], [Lookback_Window])
                    )
                """))

                # ── Fact_NodeFlux（12×12 資金流轉矩陣）───────────────
                conn.execute(text("""
                    IF NOT EXISTS (
                        SELECT 1 FROM sysobjects
                        WHERE name='Fact_NodeFlux' AND xtype='U'
                    )
                    CREATE TABLE Fact_NodeFlux (
                        [Date]             DATE         NOT NULL,
                        [Source_Node_ID]   VARCHAR(20)  NOT NULL,
                        [Target_Node_ID]   VARCHAR(20)  NOT NULL,
                        [Lookback_Window]  INT          NOT NULL,
                        [Amount]           FLOAT,
                        [Flux_Weight]      FLOAT,
                        [Updated_At]       DATETIME     DEFAULT GETDATE(),
                        CONSTRAINT PK_Fact_NodeFlux
                            PRIMARY KEY ([Date], [Source_Node_ID],
                                         [Target_Node_ID], [Lookback_Window])
                    )
                """))

                conn.commit()
                logger.debug("Schema migration OK: 雙軌市值欄位 + Fact_NodeAllocation/Fact_NodeFlux 就緒。")
        except Exception as e:
            logger.warning(f"⚠️ Schema migration 執行失敗: {e}")

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
                        target.Market_Cap_Refined = CASE
                            -- FMP 回溯調整了歷史 MC（拆股後 >20% 變動）→ 清空，待 fix_split_mc 重修正
                            WHEN source.Market_Cap IS NOT NULL
                             AND target.Market_Cap IS NOT NULL
                             AND target.Market_Cap > 0
                             AND ABS(source.Market_Cap - target.Market_Cap)
                                 / target.Market_Cap > 0.20
                            THEN NULL
                            -- 正常每日小幅波動：保留既有修正值，避免 staircase 被覆蓋
                            ELSE target.Market_Cap_Refined
                        END
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


    def upsert_node_allocation(self, now_str, past_str, lookback_window=21):
        """
        [事實持久化] 計算並儲存 12 節點資金分佈比例（Fact_NodeAllocation）

        12 節點 = 11 L0 板塊 + 1 HEDGE（BIL + SHV + TLT + GLD 合計）

        PK 設計（與 Fact_NodeFlux 對齊）：
          Date            = now_str（定錨點，同 Fact_NodeFlux.Date）
          Lookback_Window = 0 / 21 / 63
          Weight          = MC 快照 at past_str / total MC

        語義：
          LW=0  → past_str = now_str → 當日配置（now 側）
          LW=21 → past_str = now_str - 21 交易日 → 一個月前配置（past 側）
          LW=63 → past_str = now_str - 63 交易日 → 一季前配置（past 側）
        → LW=21 vs LW=63 查的是不同 past_str 的 MC，自然不同。
        """
        logger.info(f"📊 計算 {now_str} 的 12 節點配置快照（LW={lookback_window}，MC_at={past_str}）...")

        sectors      = self.config.L0_SECTORS
        hedge_syms   = ['BIL', 'SHV', 'TLT', 'GLD']
        all_symbols  = list(dict.fromkeys(sectors + hedge_syms))
        symbols_str  = "'" + "','".join(all_symbols) + "'"

        df = self.execute_query(f"""
            SELECT Symbol,
                   COALESCE(Market_Cap_Refined, Market_Cap) AS Market_Cap
            FROM Fact_DailyPrice
            WHERE [Date] = :d
              AND Symbol IN ({symbols_str})
              AND COALESCE(Market_Cap_Refined, Market_Cap) > 0
        """, params={"d": past_str})
        date_str = now_str

        if df.empty:
            logger.warning(f"⚠️ {date_str} 無法取得市值數據，跳過。")
            return

        # HEDGE 合計
        hedge_mc = float(df[df['Symbol'].isin(hedge_syms)]['Market_Cap'].sum())

        # 建立 12 節點列表（板塊 × 11 + HEDGE × 1）
        sector_rows = df[df['Symbol'].isin(sectors)][['Symbol', 'Market_Cap']] \
                        .rename(columns={'Symbol': 'Node_ID'}) \
                        .to_dict('records')
        all_rows = sector_rows + [{'Node_ID': 'HEDGE', 'Market_Cap': hedge_mc}]

        total_mc = sum(float(r['Market_Cap']) for r in all_rows)
        if total_mc <= 0:
            logger.warning(f"⚠️ {date_str} 12 節點總市值為 0，跳過。")
            return

        data_list = [
            {
                "date":   date_str,
                "node":   r['Node_ID'],
                "window": lookback_window,
                "weight": float(r['Market_Cap']) / total_mc,
            }
            for r in all_rows
        ]

        table_name    = "Fact_NodeAllocation"
        staging_table = f"#Staging_Alloc_{uuid.uuid4().hex[:8]}"

        try:
            with self.engine.connect() as conn:
                trans = conn.begin()
                conn.execute(text(
                    f"CREATE TABLE {staging_table} "
                    f"(d DATE, n VARCHAR(20), w INT, weight FLOAT)"
                ))
                conn.execute(
                    text(f"INSERT INTO {staging_table} (d, n, w, weight) "
                         f"VALUES (:date, :node, :window, :weight)"),
                    data_list
                )
                conn.execute(text(f"""
                    MERGE INTO {table_name} AS T
                    USING {staging_table} AS S
                    ON  T.[Date]            = S.d
                    AND T.[Node_ID]         = S.n
                    AND T.[Lookback_Window] = S.w
                    WHEN MATCHED THEN
                        UPDATE SET T.[Weight]     = S.weight,
                                   T.[Updated_At] = GETDATE()
                    WHEN NOT MATCHED THEN
                        INSERT ([Date], [Node_ID], [Lookback_Window], [Weight], [Updated_At])
                        VALUES (S.d, S.n, S.w, S.weight, GETDATE());
                """))
                trans.commit()
            logger.success(f"✅ {date_str} Fact_NodeAllocation（12 節點，LW={lookback_window}）已存檔。")
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
        [運算引擎] ETF Observed Method — 產出 N×N 資金流轉矩陣（N ≤ 12）

        核心公式（代數簡化）：
          F_net = Price_now × (Shares_now − Shares_past)
          其中 Shares = COALESCE(Market_Cap_Refined, Market_Cap) / Close

        物理意義：
          F_net > 0 → 資金流入（市值增幅超過價格漲幅，表示有淨申購）
          F_net < 0 → 資金流出（市值增幅低於價格漲幅，表示有淨贖回）
          F_net = 0 → Shares 未變動（精確 0.0，無浮點誤差）

        生命週期過濾（集合交集）：
          XLRE（2015+）、XLC（2018+）等在 past_date 或 now_date 任一無資料，
          自動剔除出當期矩陣，資金差額由守恆校正導向 HEDGE。
        """
        logger.info(f"🌊 Net Flux 計算: {past_date} → {now_date}")

        if target_assets is None:
            target_assets = self.config.L0_SECTORS
        if hedge_assets is None:
            hedge_assets = ['BIL', 'SHV', 'TLT', 'GLD']

        all_tickers = list(dict.fromkeys(target_assets + hedge_assets))

        # ── 抓取兩個日期的收盤價 + 精鍊市值 ─────────────────────
        df = self.execute_query("""
            SELECT [Date], Symbol, [Close] AS Price,
                   COALESCE(Market_Cap_Refined, Market_Cap) AS Market_Cap
            FROM Fact_DailyPrice
            WHERE [Date] IN :dates AND Symbol IN :symbols
              AND [Close] > 0
              AND COALESCE(Market_Cap_Refined, Market_Cap) > 0
        """, params={"dates": (past_date, now_date), "symbols": tuple(all_tickers)})

        if df.empty:
            logger.warning(f"⚠️ {past_date} 或 {now_date} 查無資料，請確認是否為交易日。")
            return None

        df['Date'] = df['Date'].astype(str)

        # ── 生命週期過濾：兩日都有資料才納入計算 ─────────────────
        avail_past = set(df[df['Date'] == past_date]['Symbol'])
        avail_now  = set(df[df['Date'] == now_date]['Symbol'])
        alive      = avail_past & avail_now  # 集合交集，自動排除上市較晚的 ETF

        active_targets = [s for s in target_assets if s in alive]
        active_hedges  = [s for s in hedge_assets  if s in alive]
        logger.info(f"📊 活躍節點: 板塊 {len(active_targets)} 檔, 避險 {len(active_hedges)} 檔")

        df_past = df[df['Date'] == past_date].set_index('Symbol')
        df_now  = df[df['Date'] == now_date].set_index('Symbol')

        # ── F_net = Price_now × (Shares_now − Shares_past) ───────
        f_net_dict: dict = {}
        for sym in alive:
            mc_p = float(df_past.loc[sym, 'Market_Cap'])
            pr_p = float(df_past.loc[sym, 'Price'])
            mc_n = float(df_now.loc[sym,  'Market_Cap'])
            pr_n = float(df_now.loc[sym,  'Price'])

            shares_past     = mc_p / pr_p
            shares_now      = mc_n / pr_n
            f_net_dict[sym] = pr_n * (shares_now - shares_past)

        # ── 整合 12 決策節點：11 板塊 + 1 HEDGE ──────────────────
        nodes_f_net: dict = {s: f_net_dict[s] for s in active_targets}
        nodes_f_net['HEDGE'] = sum(f_net_dict.get(h, 0.0) for h in active_hedges)

        # ── 分離流出（< 0）/ 流入（> 0）─────────────────────────
        outflows = {k: abs(v) for k, v in nodes_f_net.items() if v < 0}
        inflows  = {k: v     for k, v in nodes_f_net.items() if v > 0}
        total_out = sum(outflows.values())
        total_in  = sum(inflows.values())

        # ── 第一重守恆校正：差額導向 HEDGE ───────────────────────
        if total_out > total_in:
            inflows['HEDGE']  = inflows.get('HEDGE', 0.0)  + (total_out - total_in)
        elif total_in > total_out:
            outflows['HEDGE'] = outflows.get('HEDGE', 0.0) + (total_in - total_out)

        total_flux = sum(outflows.values())  # 校正後 outflows == inflows

        # ── 建立 N×N 矩陣（比例傳導分配）────────────────────────
        matrix_nodes = active_targets + ['HEDGE']
        flux_matrix  = pd.DataFrame(0.0, index=matrix_nodes, columns=matrix_nodes)

        if total_flux > 0:
            for source, out_val in outflows.items():
                for target, in_val in inflows.items():
                    flux_matrix.loc[source, target] = out_val * (in_val / total_flux)

        logger.success(
            f"✅ Flux Matrix {len(matrix_nodes)}×{len(matrix_nodes)} 生成完成，"
            f"總轉移資金: ${total_flux:,.0f}"
        )
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

    def fix_split_mc_corrections(self, symbols, nasdaq_api_key, dry_run=True):
        """
        [MC 拆股修正 v2] 使用 SFP closeunadj 精確還原 FMP 回溯調整造成的 MC 錯誤。

        修正原理：
          FMP 錯誤 = pre-split close 被回溯 ÷ ratio → MC = (close/ratio) × shares = MC_true/ratio
          SFP closeunadj = 真實未調整收盤價
          → Market_Cap_Refined = SFP_closeunadj × stable_pre_split_shares  ✓

        修正範圍：
          ① pre-split 期（Date < split_date）：
               Market_Cap_Refined = closeunadj × stable_shares
          ② post-split 階梯期（split_date ≤ Date < staircase_end）：
               Market_Cap_Refined = closeunadj × (stable_shares × ratio)
          ③ staircase_end 之後：
               Market_Cap_Refined = NULL（FMP 值已恢復正確，COALESCE 自動 fallback）

        特性：
          - 冪等：可重複執行，結果不變
          - 只寫入 Market_Cap_Refined，Market_Cap（FMP 原始）永不覆寫
          - 資料源：SHARADAR/ACTIONS（拆股事件）+ SHARADAR/SFP（closeunadj）
        """
        import requests

        SFP_BASE = "https://data.nasdaq.com/api/v3"

        def sfp_get(endpoint, params):
            """帶 pagination 的 SFP 資料抓取"""
            rows, cursor = [], None
            while True:
                p = {"api_key": nasdaq_api_key, "qopts.per_page": 10000}
                p.update(params)
                if cursor:
                    p["qopts.cursor_id"] = cursor
                r = requests.get(f"{SFP_BASE}/datatables/SHARADAR/{endpoint}.json",
                                 params=p, timeout=60)
                if not r.ok:
                    logger.error(f"SFP {endpoint} HTTP {r.status_code}: {r.text[:120]}")
                    break
                d      = r.json()
                rows.extend(d.get("datatable", {}).get("data", []))
                cursor = d.get("meta", {}).get("next_cursor_id")
                if not cursor:
                    break
            return rows

        # ── Phase 1：從 SHARADAR/ACTIONS 取得拆股事件 ─────────────
        logger.info("📋 Phase 1: 從 SFP ACTIONS 取得拆股事件...")
        action_rows = sfp_get("ACTIONS", {
            "ticker":     ",".join(symbols),
            "action":     "split",
            "date.gte":   "1998-01-01",
        })
        # columns: [date, action, ticker, name, value, contraticker, contraname]

        if not action_rows:
            logger.success("✅ 無拆股事件，無需修正。")
            return

        logger.info(f"  找到 {len(action_rows)} 筆拆股事件")

        plan = []  # 修正計畫清單

        for row in action_rows:
            split_date_str, _, symbol, _, ratio, _, _ = row
            if abs(ratio - 1.0) < 0.001:
                continue

            logger.info(f"  分析 {symbol} {split_date_str} ratio={ratio:.4f}")

            # ── Phase 2：DB 取得 stable_pre_split_shares ──────────
            stable_df = self.execute_query("""
                SELECT TOP 10 ROUND(Market_Cap / NULLIF([Close], 0), 0) AS Shares
                FROM Fact_DailyPrice
                WHERE Symbol = :sym
                  AND [Date] <  DATEADD(day, -14, :sd)
                  AND [Date] >= DATEADD(day, -75, :sd)
                  AND Market_Cap IS NOT NULL AND Market_Cap > 0
                  AND [Close]    IS NOT NULL AND [Close]    > 0
                ORDER BY [Date] DESC
            """, params={"sym": symbol, "sd": split_date_str})

            if stable_df.empty:
                logger.warning(f"  ⚠️ {symbol}: 找不到 pre-split 穩定股數，跳過")
                continue

            stable_shares      = int(stable_df["Shares"].median())
            correct_post_shares = int(stable_shares * ratio)

            # 資料品質檢查：FMP 2020 前 ETF MC 可能是垃圾值（如 XLF 2016 = $16K）
            if stable_shares < 1_000_000:
                logger.warning(
                    f"  ⚠️ {symbol} {split_date_str}: stable_shares={stable_shares:,} "
                    f"過小（FMP MC 歷史品質問題），跳過此事件"
                )
                continue

            # ── Phase 3：SFP 取得 closeunadj 序列 ─────────────────
            sfp_rows = sfp_get("SFP", {
                "ticker":        symbol,
                "date.gte":      "1998-01-01",
                "qopts.columns": "ticker,date,closeunadj",
            })
            if not sfp_rows:
                logger.warning(
                    f"  ⚠️ {symbol}: SFP 無 closeunadj 資料"
                    f"（SFP 僅覆蓋 ETF/CEF，個股請用其他來源）"
                )
                continue

            sfp_map = {str(r[1])[:10]: r[2] for r in sfp_rows if r[2]}

            # ── Phase 4：偵測 staircase_end（FMP shares 穩定日）──
            tol = correct_post_shares * 0.02  # 2% 容差
            sc_df = self.execute_query("""
                SELECT MIN([Date]) AS stable_date
                FROM Fact_DailyPrice
                WHERE Symbol = :sym
                  AND [Date]  > :sd
                  AND [Close] > 0
                  AND ABS(ROUND(Market_Cap / NULLIF([Close], 0), 0) - :exp) <= :tol
            """, params={
                "sym": symbol, "sd": split_date_str,
                "exp": correct_post_shares, "tol": tol,
            })
            staircase_end = (str(sc_df.iloc[0, 0])[:10]
                             if not sc_df.empty and sc_df.iloc[0, 0] is not None
                             else None)

            pre_count_df = self.execute_query(
                "SELECT COUNT(*) AS n FROM Fact_DailyPrice "
                "WHERE Symbol=:s AND [Date] < :d",
                params={"s": symbol, "d": split_date_str}
            )
            pre_rows = int(pre_count_df.iloc[0, 0]) if not pre_count_df.empty else 0

            plan.append({
                "symbol":        symbol,
                "split_date":    split_date_str,
                "ratio":         ratio,
                "stable_shares": stable_shares,
                "post_shares":   correct_post_shares,
                "staircase_end": staircase_end,
                "sfp_map":       sfp_map,
                "pre_rows":      pre_rows,
                "sfp_coverage":  sum(1 for d in sfp_map if d < split_date_str),
            })

        # ── 報告 ──────────────────────────────────────────────────
        if not plan:
            logger.success("✅ 掃描完畢：無需修正。")
            return

        logger.info(f"\n{'='*72}")
        logger.info(f"  MC 拆股修正報告 v2（{'DRY-RUN' if dry_run else '即將執行寫入 Market_Cap_Refined'}）")
        logger.info(f"{'='*72}")
        for c in plan:
            logger.info(
                f"  {c['symbol']:<6} {c['split_date']}  ratio={c['ratio']:.4f}"
                f"  stable={c['stable_shares']:,}  post={c['post_shares']:,}"
                f"  staircase_end={c['staircase_end'] or 'N/A':<12}"
                f"  pre_rows={c['pre_rows']:,}  sfp_cov={c['sfp_coverage']:,}"
            )
        logger.info(f"{'='*72}")

        if dry_run:
            logger.warning("DRY-RUN 模式，未執行寫入。加上 --apply 參數以實際修正。")
            return

        # ── 執行修正（寫入 Market_Cap_Refined）────────────────────
        logger.info("🔧 執行 MC 修正（寫入 Market_Cap_Refined，Market_Cap 不動）...")
        fixed = 0

        for c in plan:
            symbol        = c["symbol"]
            split_date    = c["split_date"]
            sfp_map       = c["sfp_map"]
            stable        = c["stable_shares"]
            post          = c["post_shares"]
            sc_end        = c["staircase_end"]

            try:
                # cutoff 邏輯：
                #   staircase_end 存在 → 修正至 staircase_end（pre-split + 階梯期）
                #   staircase_end 不存在 → 只修正 pre-split（post-split 可能已正確）
                cutoff = sc_end if sc_end else split_date

                # 取得修正範圍內的 Date / Close / Market_Cap
                rows_df = self.execute_query("""
                    SELECT [Date], [Close], Market_Cap
                    FROM Fact_DailyPrice
                    WHERE Symbol = :sym AND [Date] < :cut
                      AND [Close] > 0 AND Market_Cap > 0
                    ORDER BY [Date]
                """, params={"sym": symbol, "cut": cutoff})

                if rows_df.empty:
                    logger.warning(f"  ⚠️ {symbol}: 修正範圍內無資料")
                    continue

                # 組裝批次更新列表
                # pre-split：Market_Cap_Refined = Market_Cap × (closeunadj/Close)
                #   等價於真實 MC，無論 shares 是否有 organic growth
                # staircase 期：Market_Cap_Refined = closeunadj × post_shares
                records = []
                for _, row in rows_df.iterrows():
                    dstr       = str(row["Date"])[:10]
                    closeunadj = sfp_map.get(dstr)
                    if not closeunadj or closeunadj <= 0:
                        continue
                    if dstr < split_date:
                        # pre-split：用比例修正（自動處理 organic share growth）
                        mc_refined = float(row["Market_Cap"]) * (closeunadj / float(row["Close"]))
                    else:
                        # staircase 期（只有 sc_end 存在時才到達此分支）
                        mc_refined = closeunadj * post
                    records.append({"mc": float(mc_refined), "sym": symbol, "d": dstr})

                if not records:
                    logger.warning(f"  ⚠️ {symbol}: sfp_map 無法覆蓋修正範圍")
                    continue

                # 批次寫入 Market_Cap_Refined（fast_executemany=True）
                with self.engine.connect() as conn:
                    conn.execute(
                        text("UPDATE Fact_DailyPrice "
                             "SET Market_Cap_Refined = :mc "
                             "WHERE Symbol = :sym AND [Date] = :d"),
                        records
                    )
                    # staircase_end 之後清空（FMP 已正確，COALESCE fallback 生效）
                    if sc_end:
                        conn.execute(
                            text("UPDATE Fact_DailyPrice "
                                 "SET Market_Cap_Refined = NULL "
                                 "WHERE Symbol = :sym AND [Date] >= :sc"),
                            {"sym": symbol, "sc": sc_end}
                        )
                    conn.commit()

                logger.success(
                    f"  ✅ {symbol} {split_date} ratio={c['ratio']:.4f}"
                    f"  → {len(records):,} 筆寫入 Market_Cap_Refined"
                    + (f"  staircase_end={sc_end}" if sc_end else "")
                )
                fixed += 1

            except Exception as e:
                logger.error(f"  ❌ {symbol} 修正失敗: {e}")

        logger.success(f"\n🎉 MC 拆股修正完成：{fixed}/{len(plan)} 筆事件已處理。")