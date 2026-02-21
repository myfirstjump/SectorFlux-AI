import pandas as pd
import logging
import time
from sqlalchemy import create_engine, text
import uuid  # 🌟 新增這行：用來產生隨機字串

logger = logging.getLogger("SectorFlux_Database")

class DatabaseManipulation:
    def __init__(self, config):
        """
        初始化資料庫操作模組
        """
        self.config = config
        try:
            # 建立連線池。fast_executemany=True 是 pyodbc 大量寫入的效能關鍵
            self.engine = create_engine(
                self.config.database_url,
                fast_executemany=True, 
                pool_size=5,
                max_overflow=10
            )
            logger.info("✅ DatabaseManipulation 初始化成功，連線池已建立。")
        except Exception as e:
            logger.error(f"❌ 資料庫連線初始化失敗: {str(e)}")
            raise e

    def upsert_market_data(self, df, table_name="Fact_DailyPrice"):
        """
        將爬蟲資料透過 Staging Table + MERGE 寫入
        (實作 TempDB 隔離與 Deadlock 自動重試機制)
        """
        import time # 確保模組內有 import time
        if df is None or df.empty:
            return

        # 🛡️ 防禦 1：加上 '#' 符號，強制在 tempdb 建立區域暫存表，避開主系統目錄鎖定
        unique_suffix = uuid.uuid4().hex[:8]
        staging_table = f"#Staging_{table_name}_{unique_suffix}"
        
        max_retries = 3
        for attempt in range(max_retries):
            try:
                with self.engine.connect() as conn:
                    trans = conn.begin()
                    
                    # 寫入 tempdb，即便 if_exists='replace' 也不會干擾主庫
                    df.to_sql(staging_table, con=conn, if_exists='replace', index=False, 
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
                            target.Volume = source.Volume
                    
                    WHEN NOT MATCHED THEN
                        INSERT (Date, Symbol, [Open], High, Low, [Close], Volume)
                        VALUES (source.Date, source.Symbol, source.[Open], source.High, source.Low, source.[Close], source.Volume);
                    """)
                    
                    conn.execute(merge_sql)
                    
                    # 養成好習慣，用完立刻清理 tempdb 空間
                    conn.execute(text(f"DROP TABLE {staging_table}"))
                    trans.commit()
                    break # 執行成功，跳出重試迴圈
                    
            except Exception as e:
                # 攔截錯誤並 Rollback
                if 'trans' in locals() and trans is not None:
                    trans.rollback()
                    
                error_msg = str(e)
                # 🛡️ 防禦 2：偵測 Error 1205 (Deadlock) 並自動重試
                if '1205' in error_msg or 'Deadlock' in error_msg:
                    if attempt < max_retries - 1:
                        logger.warning(f"⚠️ 遭遇高頻寫入死結 (1205)，系統退避 1 秒後進行第 {attempt + 2} 次重試...")
                        time.sleep(1)
                        continue # 進入下一次迴圈
                
                # 如果不是死結，或是重試次數用盡，則拋出真實錯誤
                logger.error(f"❌ Upsert 失敗: {error_msg}")
                raise e

    def prepare_tsf_features(self, benchmark='VOO', days_to_process=40):
        """
        【資料預處理核心】去貝塔 (De-beta) 邏輯
        在資料庫端直接使用 CTE (Common Table Expression) 進行高效運算，
        計算所有標的相對於 benchmark 的 RS (Relative Strength)，並儲存至 Fact_RS_Features。
        
        :param benchmark: 作為分母的基準標的 (預設 VOO)
        :param days_to_process: 重新計算最近 N 天的資料 (效能優化)。若設為 None 則全量計算 30 年。
        """
        logger.info(f"開始在資料庫端計算相對於 {benchmark} 的 RS (相對強度) 序列...")
        
        # 處理時間濾網邏輯 (針對 30 年歷史大灌注的彈性防呆)
        date_filter_sql = ""
        if days_to_process is not None:
            # 每日排程：只重算最近 40 天
            date_filter_sql = f"AND t1.Date >= DATEADD(day, -{days_to_process}, GETDATE())"
            logger.info(f"啟用時間濾網：僅計算最近 {days_to_process} 天之資料以節省運算資源。")
        else:
            # 歷史大灌注：算到飽
            logger.info("⚠️ 啟動全歷史資料 RS 重新計算 (此動作將執行全表掃描，可能需要一至兩分鐘)...")

        sql_logic = text(f"""
        -- 1. 確保特徵表存在
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Fact_RS_Features' AND xtype='U')
        CREATE TABLE Fact_RS_Features (
            Date DATE NOT NULL,
            Symbol VARCHAR(20) NOT NULL,
            RS_Value FLOAT,
            PRIMARY KEY (Date, Symbol)
        );

        -- 2. 使用 CTE 結合 MERGE 高效計算並 Upsert
        WITH BenchmarkData AS (
            SELECT Date, [Close] AS BenchPrice
            FROM Fact_DailyPrice
            WHERE Symbol = '{benchmark}'
        )
        MERGE INTO Fact_RS_Features AS target
        USING (
            -- 計算 RS_t = Price(target) / Price(VOO)
            SELECT t1.Date, t1.Symbol, (t1.[Close] / t2.BenchPrice) AS RS_Value
            FROM Fact_DailyPrice t1
            JOIN BenchmarkData t2 ON t1.Date = t2.Date
            WHERE t1.Symbol != '{benchmark}' AND t2.BenchPrice > 0
            {date_filter_sql}  -- <== 動態注入時間濾網
        ) AS source
        ON target.Date = source.Date AND target.Symbol = source.Symbol
        
        WHEN MATCHED THEN
            UPDATE SET target.RS_Value = source.RS_Value
        
        WHEN NOT MATCHED THEN
            INSERT (Date, Symbol, RS_Value)
            VALUES (source.Date, source.Symbol, source.RS_Value);
        """)
        
        with self.engine.connect() as conn:
            trans = conn.begin()
            try:
                conn.execute(sql_logic)
                trans.commit()
                logger.info(f"✅ RS 特徵工程 (De-beta) 運算完畢，已結構化儲存至 Fact_RS_Features。")
            except Exception as e:
                trans.rollback()
                logger.error(f"❌ RS 特徵運算失敗: {str(e)}")
                raise e

    def get_rs_series(self, target_ticker):
        """
        供 tsf_modules.py (預測模組) 提取單一標的之 RS 序列
        """
        query = f"SELECT Date, RS_Value FROM Fact_RS_Features WHERE Symbol = '{target_ticker}' ORDER BY Date ASC"
        return pd.read_sql(query, self.engine)

    def save_predictions(self, layer, horizon, df_pred):
        """
        儲存模型預測結果 (M/Q/Y)。
        df_pred 必須包含: Date, Symbol, Prediction_Value
        """
        if df_pred is None or df_pred.empty:
            return
            
        table_name = f"Fact_Predictions_{layer}_{horizon}" # 例如 Fact_Predictions_L0_M
        
        # 為了保持範例精簡，這裡採用最單純的 pandas to_sql (實務上同樣可改寫為 MERGE)
        try:
            with self.engine.connect() as conn:
                df_pred.to_sql(table_name, con=conn, if_exists='append', index=False, chunksize=self.config.DB_CHUNK_SIZE)
            logger.info(f"✅ {layer} 層 {horizon} 尺度預測結果已寫入 {table_name}")
        except Exception as e:
            logger.error(f"❌ 寫入預測結果失敗: {str(e)}")
            raise e