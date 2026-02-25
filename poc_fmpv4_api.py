import os
import requests
import pandas as pd
from dotenv import load_dotenv

def run_fmp_v4_stable_poc():
    load_dotenv()
    api_key = os.getenv("FMP_API_KEY")
    
    if not api_key:
        print("❌ 找不到 FMP_API_KEY，請確認 .env 檔案！")
        return

    # 定義不同的測試目標與對應的最優端點路徑
    # 1. v4/etf-fund-flow: 驗證流量真值
    # 2. v4/economic: 驗證宏觀環境因子
    # 3. stable/etf-sector-weightings: 驗證權重動態映射
    test_tasks = [
        {
            "name": "ETF Fund Flow (v4)",
            "url": "https://financialmodelingprep.com/api/v4/etf-fund-flow/SPY",
            "expected_cols": ["date", "fundFlow"]
        },
        {
            "name": "Economic Data - Interest Rate (v4)",
            "url": "https://financialmodelingprep.com/api/v4/economic?name=federalFundsRate",
            "expected_cols": ["date", "value"]
        },
        {
            "name": "ETF Sector Weights (Stable)",
            "url": f"https://financialmodelingprep.com/api/stable/etf-sector-weightings?symbol=XLK",
            "expected_cols": ["sector", "weightPercentage"]
        }
    ]
    
    print(f"🚀 啟動 SectorFlux-AI 數據源偵察 (v4 & Stable)\n" + "="*60)

    for task in test_tasks:
        print(f"\n📡 正在測試端點: {task['name']}")
        
        # 構造完整的 URL (處理 api_key 拼接)
        sep = "&" if "?" in task['url'] else "?"
        full_url = f"{task['url']}{sep}apikey={api_key}"
        
        try:
            response = requests.get(full_url, timeout=12)
            print(f"🔗 URL: {task['url']}")
            print(f"📊 HTTP Status: {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                if not data:
                    print(f"⚠️ 警告: 回傳資料為空，可能該標的在該端點無紀錄。")
                    continue
                
                # 轉為 DataFrame 觀察
                df = pd.DataFrame(data)
                print(f"✅ 成功獲取資料！前 2 筆紀錄：")
                print(df.head(2).to_string(index=False))
                
                # 欄位驗證
                missing_cols = [col for col in task['expected_cols'] if col not in df.columns]
                if missing_cols:
                    print(f"❌ 警告: 缺少預期欄位 {missing_cols}")
                else:
                    print(f"🎉 驗證通過！可用於實作 V12.0 邏輯。")
            
            elif response.status_code == 403:
                print(f"⛔ 權限不足: 您的方案可能未包含 v4 或該 Stable 端點。")
            else:
                print(f"❌ 錯誤原因: {response.text[:100]}")
                
        except Exception as e:
            print(f"🔥 例外錯誤: {str(e)}")

if __name__ == "__main__":
    run_fmp_v4_stable_poc()