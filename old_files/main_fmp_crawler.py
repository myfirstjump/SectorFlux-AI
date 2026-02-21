import requests
import pandas as pd
import os
from pathlib import Path
from dotenv import load_dotenv

# 1. 載入 .env 檔案中的環境變數
load_dotenv()
API_KEY = os.environ.get("FMP_API_KEY")

# 2. 建立快取資料夾
CACHE_DIR = Path("./fmp_cache")
CACHE_DIR.mkdir(exist_ok=True)

def fetch_historical_prices_safe(ticker, api_key, start_date="2023-01-01", end_date="2023-12-31"):
    if not api_key:
        print("錯誤：找不到 API Key，請確認 .env 設定！")
        return None
        
    cache_file = CACHE_DIR / f"{ticker}_{start_date}_{end_date}.csv"
    
    if cache_file.exists():
        print(f"[Cache Hit] 從本地讀取 {ticker} 的歷史資料...")
        return pd.read_csv(cache_file)
    
    # 【核心修正】: 改用 FMP 官方最新的 Stable API 端點
    url = f"https://financialmodelingprep.com/stable/historical-price-eod/full?symbol={ticker}&from={start_date}&to={end_date}&apikey={api_key}"
    print(f"[API Call] 正在向 FMP 請求 {ticker} 的歷史資料 (消耗流量中)...")
    
    response = requests.get(url)
    
    if response.status_code == 200:
        data = response.json()
        
        # 新版 API 回傳格式可能直接是 list，也可能包在字典裡，這裡做動態判定
        df = None
        if isinstance(data, list):
            df = pd.DataFrame(data)
        elif isinstance(data, dict) and "historical" in data:
            df = pd.DataFrame(data["historical"])
        elif isinstance(data, dict) and "data" in data:
            df = pd.DataFrame(data["data"])
            
        if df is not None and not df.empty:
            # 為了保險起見，先保留所有欄位存入本地快取
            df.to_csv(cache_file, index=False)
            print(f"✅ 資料抓取成功並已建立快取！")
            return df
        else:
            print(f"找不到 {ticker} 的資料，或回傳格式無法解析。回傳內容: {data}")
    else:
        print(f"API 請求失敗，狀態碼: {response.status_code}")
        try:
            print("詳細錯誤訊息:", response.json())
        except:
            print("無法解析錯誤訊息")
            
    return None

if __name__ == "__main__":
    print("開始測試抓取股價 (使用最新 Stable 端點)...")
    # 測試抓取微軟 2023 年整年的資料
    price_data = fetch_historical_prices_safe("MSFT", API_KEY, start_date="2023-01-01", end_date="2023-12-31")
    
    if price_data is not None:
        print("\n--- 資料預覽 (前 5 筆) ---")
        # 印出最新的欄位名稱與前五筆資料
        print(price_data.head())