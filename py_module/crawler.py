import time
import requests
import pandas as pd
from datetime import datetime, timedelta
from loguru import logger
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from py_module.config import Configuration
from py_module.database import DatabaseManipulation

class FinancialCrawler:

    def __init__(self, config):

        self.config = config
        self.api_key = config.FMP_API_KEY
        self.base_url = "https://financialmodelingprep.com"
        self.db = DatabaseManipulation(config)
        self.session = self._create_retry_session()

    def _create_retry_session(self):
        """
        🛡️ 建立具備指數回退 (Exponential Backoff) 的 Requests Session
        專門處理 HTTP 429 (Too Many Requests) 與網路波動
        """

        session = requests.Session()
        retries = Retry(
            total=5,
            backoff_factor=1,  # 等待時間: 1s, 2s, 4s, 8s, 16s...
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"]
        )

        adapter = HTTPAdapter(max_retries=retries)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session



    def fetch_all_data(self, market='us', history_days=30):
        """主入口：執行全市場爬取任務 (含拆股偵測與自動修復)"""
        logger.info(f"🚀 開始執行 {market.upper()} 市場數據任務...")

        if market == 'us':
            macro_universe = getattr(self.config, 'MACRO_UNIVERSE', []) # 加入宏觀因子
            target_universe = self.config.get_all_tickers()
            target_universe = target_universe + macro_universe
            # target_universe = macro_universe  ### 只爬 MACRO 的項目時打開


            # === 🛡️ 階段一：拆股雷達 (Split Radar) ===
            split_tickers = self._detect_splits(target_universe)

            # === 🚀 階段二：執行抓取 ===
            # 將拆股標的 (需重抓 30 年) 與 正常標的 (增量) 分流
            normal_tickers = [t for t in target_universe if t not in split_tickers]

            if split_tickers:
                logger.warning(f"🚨 發現 {len(split_tickers)} 檔拆股標的，啟動 30 年修復灌注...")
                self._fetch_and_store_prices(split_tickers, history_days=10950) # 30年

            if normal_tickers:
                logger.info(f"📋 開始執行 {len(normal_tickers)} 檔標的之常規增量更新 ({history_days} 天)...")
                self._fetch_and_store_prices(normal_tickers, history_days)

    def _detect_splits(self, universe):

        """偵測過去 7 天是否有拆股事件"""
        try:
            end_date = datetime.now().strftime("%Y-%m-%d")
            start_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
            url = f"{self.base_url}/stable/splits-calendar?from={start_date}&to={end_date}&apikey={self.api_key}"
            resp = self.session.get(url, timeout=45)

            if resp.status_code == 200:
                splits = resp.json()
                split_symbols = [item.get('symbol') for item in splits if item.get('symbol')]

                # 回傳交集 (Intersection)
                return list(set(split_symbols).intersection(set(universe)))
            return []

        except Exception as e:
            logger.error(f"❌ 拆股偵測失敗: {e}")
            return []

    def _fetch_and_store_prices(self, tickers, history_days):

        """
        [最終整合版] 雙軌抓取 + 5年分塊機制 + Stable API
        解決 30 年一次請求導致 FMP 回傳空值或 Timeout 的問題
        """

        import pandas as pd
        from datetime import datetime, timedelta
        import gc

        # FMP 限制建議：每次請求不超過 5 年 (約 1825 天)

        CHUNK_SIZE_DAYS = 1825

        for idx, symbol in enumerate(tickers):
            try:
                logger.info(f"[{idx+1}/{len(tickers)}] 處理 {symbol} (分塊抓取 {history_days} 天)...")
                # 準備大容器
                all_prices = []
                all_mcaps = []

                # 設定時間游標
                end_date = datetime.now()
                start_date_limit = end_date - timedelta(days=history_days)

                cursor_end = end_date

                # === 🔄 5年分塊迴圈 (Chunk Loop) ===
                while cursor_end > start_date_limit:
                    cursor_start = cursor_end - timedelta(days=CHUNK_SIZE_DAYS)
                    if cursor_start < start_date_limit:
                        cursor_start = start_date_limit

                    # 轉字串
                    str_start = cursor_start.strftime("%Y-%m-%d")
                    str_end = cursor_end.strftime("%Y-%m-%d")

                    # 1. 抓取股價 (分塊)
                    price_url = f"{self.base_url}/stable/historical-price-eod/full"
                    payload = {
                        "symbol": symbol,
                        "from": str_start,
                        "to": str_end,
                        "apikey": self.api_key
                    }
                    try:
                        p_resp = self.session.get(price_url, params=payload, timeout=30).json()
                        if isinstance(p_resp, dict) and 'historical' in p_resp:
                            all_prices.extend(p_resp['historical'])
                        elif isinstance(p_resp, list): # 有些端點直接回 list
                            all_prices.extend(p_resp)
                    except Exception as e:
                        logger.warning(f"⚠️ {symbol} 股價分塊 {str_start}~{str_end} 失敗: {e}")

                    # 2. 抓取市值 (分塊) - 雖然市值 API 參數叫 limit，但我們嘗試帶入日期區間以求對齊
                    # 若 FMP 市值 API 不支援 from/to，則 fallback 到 limit 模式
                    # 但根據經驗，分塊抓取較安全
                    mcap_url = f"{self.base_url}/stable/historical-market-capitalization?symbol={symbol}&from={str_start}&to={str_end}&apikey={self.api_key}"
                   
                    try:
                        m_resp = self.session.get(mcap_url, timeout=40).json()
                        if isinstance(m_resp, list):
                            all_mcaps.extend(m_resp)
                    except Exception as e:
                        pass # 市值失敗不中斷

                    # 游標往前推 (避免重疊，減 1 天)
                    cursor_end = cursor_start - timedelta(days=1)
                    
                    # 禮貌性延遲
                    time.sleep(0.05)

                # === 🧩 數據組裝與寫入 ===
                if not all_prices:
                    logger.warning(f"⚠️ {symbol} 全部分塊皆無股價資料，跳過。")
                    continue

                df_price = pd.DataFrame(all_prices)
                df_mcap = pd.DataFrame(all_mcaps) if all_mcaps else pd.DataFrame()

                # 資料融合
                if not df_price.empty:
                    # 確保日期格式
                    df_price['date'] = pd.to_datetime(df_price['date'])
                   
                    # 去重 (分塊邊界可能會重複)
                    df_price.drop_duplicates(subset=['date'], inplace=True)
                    df_final = df_price.copy()

                    if not df_mcap.empty and 'date' in df_mcap.columns and 'marketCap' in df_mcap.columns:
                        df_mcap['date'] = pd.to_datetime(df_mcap['date'])
                        df_mcap.drop_duplicates(subset=['date'], inplace=True)
                        # Merge
                        df_final = pd.merge(df_price, df_mcap[['date', 'marketCap']], on='date', how='left')
                    else:
                        df_final['marketCap'] = None

                    # 欄位對映
                    df_final.rename(columns={
                        'date': 'Date', 'open': 'Open', 'high': 'High', 'low': 'Low',
                        'close': 'Close', 'volume': 'Volume', 'marketCap': 'Market_Cap'
                    }, inplace=True)
                
                    df_final['Symbol'] = symbol

                    # 計算 Shares
                    def validate_and_fix(row):
                        price = row['Close']
                        mcap = row['Market_Cap']
                        if pd.notna(price) and pd.notna(mcap) and price != 0:
                            return mcap / price
                        return None

                    df_final['Shares_Outstanding'] = df_final.apply(validate_and_fix, axis=1)

                    # 寫入資料庫
                    self.db.upsert_market_data(df_final)                   

                    # 垃圾回收
                    del df_price, df_mcap, df_final, all_prices, all_mcaps
                    gc.collect()

            except Exception as e:
                logger.error(f"❌ 處理 {symbol} 時發生嚴重錯誤: {str(e)}")

    def fetch_etf_holdings(self, etf_list=None):

        """
        [獨立任務] 抓取 ETF 持倉權重
        建議頻率：每週或每月執行一次
        """
        logger.info("📦 開始執行 ETF 持倉抓取任務...")
      
        # 如果沒指定名單，就抓所有監控中的標的 (API 會自動過濾非 ETF)
        # 但為了效率，建議最好傳入明確的 ETF 清單 (如 XLK, SPY...)
        if etf_list is None:
            etf_list = self.config.get_all_tickers()

        for symbol in etf_list:
            try:
                # 使用 Stable 端點
                url = f"{self.base_url}/stable/etf-holdings?symbol={symbol}&apikey={self.api_key}"
                resp = self.session.get(url, timeout=10)               

                if resp.status_code == 200:
                    data = resp.json()
                    if not data: continue # 不是 ETF 或沒資料

                    # 整理資料
                    holdings_data = []
                    fetch_date = datetime.now().strftime("%Y-%m-%d")                   

                    for item in data:
                        holdings_data.append({
                            'Date': item.get('date', fetch_date), # 若 API 沒給日期就用當天
                            'ETF_Symbol': symbol,
                            'Holding_Symbol': item.get('asset'),
                            'Weight': item.get('weightPercentage'),
                            'Shares': item.get('sharesNumber')
                        })
                
                    if holdings_data:
                        df = pd.DataFrame(holdings_data)
                        logger.info(f"✅ {symbol} 抓取到 {len(df)} 檔持倉")
                        # 呼叫專用的 DB 寫入方法 (需在 database.py 新增對應方法)
                        self.db.upsert_etf_holdings(df)
                       
                time.sleep(0.1) # 禮貌性延遲
              
            except Exception as e:
                logger.error(f"❌ 抓取 ETF {symbol} 持倉失敗: {e}")