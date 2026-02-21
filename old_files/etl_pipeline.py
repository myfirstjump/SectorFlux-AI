import pandas as pd
from datetime import datetime, timedelta
import requests
import os
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# --- 1. 設定與連線 ---
load_dotenv()
API_KEY = os.environ.get("FMP_API_KEY")

# 資料庫連線設定 (請確認密碼與 docker-compose.yml 一致)
DB_USER = "sa"
DB_PASS = "SectorFlux_DB_2026!"
DB_HOST = "localhost"
DB_PORT = "1433"
DB_NAME = "SectorFluxDB"

# 建立 SQLAlchemy Engine
connection_url = f"mssql+pyodbc://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes"
engine = create_engine(connection_url)

# 快取目錄
CACHE_DIR = Path("./fmp_cache")
CACHE_DIR.mkdir(exist_ok=True)

# --- 2. 爬蟲函數 (FMP API) ---
def fetch_historical_prices(ticker, api_key, start_date="2023-01-01", end_date="2023-12-31"):
    """
    抓取 FMP 歷史股價 (Stable Endpoint)
    """
    # 檢查快取 (為了測試方便，您可以隨時刪除 cache 資料夾來強制重抓)
    cache_file = CACHE_DIR / f"{ticker}_{start_date}_{end_date}.csv"
    if cache_file.exists():
        print(f"[Cache Hit] 讀取本地快取: {ticker}")
        return pd.read_csv(cache_file)

    url = f"https://financialmodelingprep.com/stable/historical-price-eod/full?symbol={ticker}&from={start_date}&to={end_date}&apikey={api_key}"
    print(f"[API Call] 下載數據中: {ticker}...")
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        
        # 處理 FMP 回傳格式
        if isinstance(data, list):
            df = pd.DataFrame(data)
        elif isinstance(data, dict) and "historical" in data:
            df = pd.DataFrame(data["historical"])
        else:
            print(f"❌ 無法解析 {ticker} 的回傳格式")
            return None

        if not df.empty:
            # 確保有 symbol 欄位 (有些端點回傳不帶 symbol)
            if 'symbol' not in df.columns:
                df['symbol'] = ticker
                
            df.to_csv(cache_file, index=False)
            return df
            
    except Exception as e:
        print(f"❌ API 請求失敗: {e}")
    return None

# --- 3. 資料清洗與轉換 (Transform) ---
def transform_data(df):
    """
    將 DataFrame 欄位名稱轉換為符合 SQL Server Schema 的格式
    """
    # 選擇需要的欄位並重新命名
    # FMP 回傳通常是 lowercase, 我們 DB 設計是 PascalCase
    rename_map = {
        'date': 'Date',
        'symbol': 'Symbol',
        'open': 'Open',
        'high': 'High',
        'low': 'Low',
        'close': 'Close',
        'volume': 'Volume'
    }
    
    # 過濾並重新命名
    df_clean = df[rename_map.keys()].rename(columns=rename_map).copy()
    
    # 確保 Date 是日期格式
    df_clean['Date'] = pd.to_datetime(df_clean['Date'])
    
    return df_clean

# --- 4. 核心 Upsert 邏輯 (Load) ---
def upsert_to_sql(df, table_name="Fact_DailyPrice"):
    """
    使用 Staging Table + MERGE 語法進行高效 Upsert
    """
    if df is None or df.empty:
        return

    # 產生一個隨機或固定的暫存表名稱
    staging_table = f"Staging_{table_name}"
    
    with engine.connect() as conn:
        trans = conn.begin() # 開啟交易
        try:
            # A. 將資料寫入暫存表 (如果存在則取代)
            print(f"⏳ 正在寫入暫存表 {staging_table} ({len(df)} 筆)...")
            df.to_sql(staging_table, con=conn, if_exists='replace', index=False)
            
            # B. 執行 MERGE SQL 指令
            # 這段 SQL 是 Upsert 的靈魂：
            # 當 Date 與 Symbol 相同時 -> 更新價格 (覆蓋舊資料，解決拆股修正)
            # 當找不到時 -> 插入新資料
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
            
            # C. 刪除暫存表
            conn.execute(text(f"DROP TABLE {staging_table}"))
            
            trans.commit()
            print(f"✅ Upsert 成功！已同步 {len(df)} 筆資料至 {table_name}。")
            
        except Exception as e:
            trans.rollback()
            print(f"❌ 資料庫寫入失敗: {e}")
            raise e

# --- 5. 主程式 ---
if __name__ == "__main__":
    tickers = ["AAPL", "MSFT"]
    
    # 動態計算日期視窗
    today = datetime.now()
    end_date_str = today.strftime("%Y-%m-%d")
    
    # 往前推 30 天作為 start_date (自動修正近期可能的拆股與除息)
    start_date_str = (today - timedelta(days=30)).strftime("%Y-%m-%d")
    
    for ticker in tickers:
        print(f"\n🚀 開始處理: {ticker} (擷取區間: {start_date_str} 至 {end_date_str})")
        
        # 1. Extract (傳入動態日期)
        raw_df = fetch_historical_prices(ticker, API_KEY, start_date=start_date_str, end_date=end_date_str)
        
        if raw_df is not None:
            # 2. Transform
            clean_df = transform_data(raw_df)
            
            # 3. Load (Upsert)
            upsert_to_sql(clean_df)
            
    print("\n🎉 所有作業完成！您的 SectorFlux 資料庫已更新完畢。")