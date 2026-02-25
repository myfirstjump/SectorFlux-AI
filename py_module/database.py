import os
import time
import urllib.parse
import pandas as pd
import uuid
from sqlalchemy import create_engine, text
from loguru import logger

# 修正 1：改回您原本的絕對路徑引用方式
from py_module.config import Configuration 

# 修正 2：類別名稱改回 DatabaseManipulation，確保 main.py 不會報錯
class DatabaseManipulation:
    def __init__(self, config):
        # 修正 3：實例化 Configuration
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
        # fast_executemany=True 對於大量寫入至關重要
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
                df[col] = None # 補齊缺失欄位，避免報錯

        # 🛡️ 防禦 1：TempDB 隔離 (使用 # 前綴)
        unique_suffix = uuid.uuid4().hex[:8]
        staging_table = f"#Staging_{table_name}_{unique_suffix}"
        
        max_retries = 3
        for attempt in range(max_retries):
            trans = None
            try:
                with self.engine.connect() as conn:
                    trans = conn.begin()
                    
                    # 寫入暫存表
                    df[required_cols].to_sql(staging_table, con=conn, if_exists='replace', index=False, 
                                             chunksize=self.config.DB_CHUNK_SIZE)
                    
                    # 🛡️ 核心邏輯：MERGE (UPSERT) 暴力覆蓋
                    # 無論是價格修正還是股數更新，只要 (Date, Symbol) 吻合就強制 Update
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
                    
                    if attempt > 0:
                        logger.info(f"✅ 第 {attempt + 1} 次重試寫入成功！")
                    break 
                    
            except Exception as e:
                if trans:
                    trans.rollback()
                
                error_msg = str(e)
                # 🛡️ 防禦 2：死結 (1205) 自動退避重試
                if '1205' in error_msg or 'Deadlock' in error_msg:
                    if attempt < max_retries - 1:
                        wait_time = (attempt + 1) * 2 # 指數退避
                        logger.warning(f"⚠️ 遭遇死結 (1205)，系統退避 {wait_time} 秒後重試...")
                        time.sleep(wait_time)
                        continue
                
                logger.error(f"❌ Upsert 失敗: {error_msg}")
                raise e
    
    def upsert_etf_holdings(self, df):
        """寫入 ETF 持倉數據 (使用 MERGE)"""
        if df is None or df.empty: return
        
        # 確保資料庫有這張表 (若尚未建立，請先執行 SQL)
        table_name = "Fact_ETF_Holdings"
        unique_suffix = uuid.uuid4().hex[:8]
        staging_table = f"#Staging_Holdings_{unique_suffix}"
        
        try:
            with self.engine.connect() as conn:
                trans = conn.begin()
                df.to_sql(staging_table, con=conn, if_exists='replace', index=False)
                
                merge_sql = text(f"""
                MERGE INTO {table_name} AS target
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