import os
import requests
import pandas as pd
from dotenv import load_dotenv

# 1. 載入環境變數
load_dotenv()
API_KEY = os.getenv("FMP_API_KEY")
BASE_URL = "https://financialmodelingprep.com"

# 2. 定義我們要測試的宏觀標的清單
# 格式: (代碼, 描述)
MACRO_TARGETS = [
    ("^TNX", "10-Year Treasury Yield (CBOE)"), # 關鍵：殖利率
    ("DX-Y.NYB", "US Dollar Index (DXY)"),     # 關鍵：美元
    ("^VIX", "CBOE Volatility Index"),         # 關鍵：恐慌指數
    ("GLD", "SPDR Gold Trust"),                # 關鍵：黃金
    ("USO", "United States Oil Fund"),         # 關鍵：原油
    ("UUP", "Invesco DB US Dollar Index (DXY Alternative)") # 備用：若 DXY 抓不到用這個
]

def run_macro_poc():
    print("🚀 啟動 FMP Stable API 宏觀因子 PoC 測試 (修正版)\n" + "="*60)
    
    if not API_KEY:
        print("❌ 找不到 FMP_API_KEY，請確認 .env 檔案！")
        return

    # ✅ 使用您驗證成功的黃金路徑
    target_url = f"{BASE_URL}/stable/historical-price-eod/full"

    for symbol, description in MACRO_TARGETS:
        print(f"\n🔎 正在測試: {description} ({symbol})")
        
        try:
            # 使用 params 讓 requests 自動處理 ? 和 & 以及 URL 編碼 (如 ^ 轉 %5E)
            payload = {
                "symbol": symbol,
                "apikey": API_KEY
            }
            
            resp = requests.get(target_url, params=payload, timeout=15)
            
            if resp.status_code == 200:
                data = resp.json()
                if data and isinstance(data, list) and len(data) > 0:
                    df = pd.DataFrame(data[:3])
                    print(f"✅ [SUCCESS] 成功抓取！")
                    print(f"   最新日期: {df.iloc[0]['date']}")
                    print(f"   收盤價: {df.iloc[0]['close']}")
                else:
                    print(f"⚠️ [EMPTY] 回傳 200 但無資料 (可能是代碼不支援此端點)")
            elif resp.status_code == 403:
                print(f"❌ [403] 權限不足 (Forbidden)")
            elif resp.status_code == 404:
                print(f"❌ [404] 找不到資料 (Not Found)")
            else:
                print(f"❌ [FAIL] Status: {resp.status_code}")
                
        except Exception as e:
            print(f"🔥 [ERROR] 連線錯誤: {str(e)}")

if __name__ == "__main__":
    run_macro_poc()