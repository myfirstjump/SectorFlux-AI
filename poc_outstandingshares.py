import requests
import pandas as pd
from loguru import logger
import os
import time
from dotenv import load_dotenv

# 配置區
API_KEY = os.getenv("FMP_API_KEY")
SYMBOLS = ['XLK']#, 'XLF', 'XLV', 'XLE', 'XLI', 'XLY', 'XLP', 'XLU', 'XLB', 'XLRE', 'XLC']



def fetch_stable_key_metrics_v2(symbol, limit=40, period='quarter'):
    """
    依照官方最新 Parameters 修正的提取函數
    Endpoint: https://financialmodelingprep.com/stable/key-metrics
    
    Parameters:
    - period: Q1, Q2, Q3, Q4, FY, annual, quarter (建議使用 quarter 以獲得最高頻率)
    """
    logger.info(f"🔍 [Stable API] 提取 {symbol} 財務指標 (Period: {period}, Limit: {limit})")
    
    # 組合 URL 參數
    url = f"https://financialmodelingprep.com/stable/key-metrics"
    params = {
        "symbol": symbol,
        "limit": limit,
        "period": period,
        "apikey": API_KEY
    }
    
    try:
        response = requests.get(url, params=params)
        if response.status_code != 200:
            logger.error(f"❌ 請求失敗: {response.status_code}")
            return pd.DataFrame()
            
        data = response.json()
        if not data:
            return pd.DataFrame()

        df = pd.DataFrame(data)
        
        # 依照您提供的 Response 結構選取欄位
        # 雖然範例沒寫，但我們會嘗試抓取 averageSharesOut 欄位
        target_cols = ['symbol', 'date', 'period', 'marketCap', 'averageSharesOut', 'enterpriseValue']
        
        # 動態檢查欄位是否存在
        existing_cols = [c for c in target_cols if c in df.columns]
        df_cleaned = df[existing_cols].copy()
        
        # 如果缺少 averageSharesOut，我們至少保留 marketCap 用於後續計算
        if 'averageSharesOut' not in df_cleaned.columns:
            logger.warning(f"⚠️ {symbol} 的回傳中未包含 averageSharesOut，將使用 marketCap 作為基準。")
            
        return df_cleaned

    except Exception as e:
        logger.error(f"❌ {symbol} 處理異常: {e}")
        return pd.DataFrame()

if __name__ == "__main__":
    logger.info("📊 啟動 11 支 ETF 財務指標深度提取任務...")
    
    results = []
    for sym in SYMBOLS:
        # 使用 quarter 模式以獲取比 FY (年報) 更密集的數據
        df_sym = fetch_stable_key_metrics_v2(sym, limit=20, period='quarter')
        if not df_sym.empty:
            results.append(df_sym)
        time.sleep(0.1) # 禮貌性延遲
        
    if results:
        df_final = pd.concat(results, ignore_index=True)
        # 日期排序
        df_final['date'] = pd.to_datetime(df_final['date'])
        df_final = df_final.sort_values(['symbol', 'date'], ascending=[True, False])
        
        print("\n--- 修正版 Key-Metrics 提取結果 ---")
        print(df_final.head(20).to_string(index=False))
        
        # 數據維運提醒：如果需要計算股數
        if 'marketCap' in df_final.columns:
            logger.success("✅ 成功取得市場價值數據，隨時可以結合 Price 欄位反推歷史股數。")
    else:
        logger.error("未能取得任何數據，請檢查 API Key 或參數設定。")