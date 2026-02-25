import os
import requests
import pandas as pd
from dotenv import load_dotenv

def run_market_cap_poc():
    # 1. 載入環境變數中的 API Key
    load_dotenv()
    api_key = os.getenv("FMP_API_KEY")
    
    if not api_key:
        print("❌ 找不到 FMP_API_KEY，請確認 .env 檔案！")
        return

    # 2. 定義測試標的：AAPL (對照組/股票) vs XLK (實驗組/ETF)
    test_symbols = ["AAPL", "XLK", "SPY"]
    base_url = "https://financialmodelingprep.com"
    
    print("🚀 啟動 FMP Stable API 歷史市值 / 流通股數 PoC 測試\n" + "="*50)

    for symbol in test_symbols:
        print(f"\n🎯 正在測試標的: {symbol}")
        
        # 採用 FMP 最新 Stable 端點進行歷史市值測試
        url = f"{base_url}/stable/historical-market-capitalization?symbol={symbol}&apikey={api_key}"
        
        try:
            response = requests.get(url, timeout=10)
            print(f"📡 HTTP 狀態碼: {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                
                # 檢查回傳資料是否為空
                if not data:
                    print(f"⚠️ 警告: FMP 回傳了 200 OK，但 {symbol} 的資料是空的 (Empty Array)！")
                    continue
                
                # 將前 3 筆資料轉為 DataFrame 以利觀察欄位
                df = pd.DataFrame(data[:3])
                print(f"✅ 成功獲取資料！以下為最新 3 筆紀錄之欄位與數值：")
                print(df.to_string(index=False))
                
                # 關鍵防呆檢查：確認我們需要的欄位是否存在
                expected_columns = ['date', 'marketCap'] # 某些端點可能叫 sharesOutstanding 或 equivalent
                missing_cols = [col for col in expected_columns if col not in df.columns]
                if missing_cols:
                    print(f"❌ 嚴重警告: 回傳資料中缺少計算 Flux 必須的欄位: {missing_cols}")
                else:
                    print(f"🎉 欄位驗證通過！該標的具備計算 ETF Observed Method 的底層數據。")
                    
            elif response.status_code == 403:
                print(f"⛔ 權限遭拒 (403): 您的方案可能不支援此 Stable 端點，或端點名稱有誤。")
                print(f"FMP 回應: {response.text}")
            else:
                print(f"❌ 呼叫失敗: {response.text}")
                
        except Exception as e:
            print(f"🔥 發生例外錯誤: {str(e)}")

if __name__ == "__main__":
    run_market_cap_poc()