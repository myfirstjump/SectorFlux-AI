import logging
import requests
import pandas as pd
import time
import gc
import json
import gzip
from datetime import datetime, timedelta
from pathlib import Path

logger = logging.getLogger("SectorFlux_Crawler")

class FinancialCrawler:
    def __init__(self, config):
        """
        初始化爬蟲模組
        :param config: 來自 config.py 的 Configuration 物件，包含 API Key 與宇宙定義
        """
        self.config = config
        self.api_key = config.FMP_API_KEY
        self.base_url = "https://financialmodelingprep.com"
        
        # 標的宇宙 (這些應該定義在 config.py 中)
        self.l0_tickers = config.L0_SECTORS       # e.g., ['XLK', 'XLF', ...]
        self.l1_tickers = config.L1_THEMATICS     # e.g., ['SMH', 'ITA', ...]
        self.risk_tickers = config.RISK_PROXY     # e.g., ['BIL', 'SHV']
        self.benchmark = config.BASE_BENCHMARK    # e.g., ['VOO']
        
        # 為了 L2，可以在 config 定義要追蹤的大型股清單，例如 SP500 成分股
        self.l2_tickers = config.L2_UNIVERSE      

    def fetch_all_data(self, market='us', history_days=30):
        """
        供 main.py 呼叫的主入口
        """
        logger.info(f"開始執行 {market.upper()} 市場數據抓取任務...")
        
        if market == 'us':
            # 組合所有需要抓取的 Ticker
            target_universe = self.config.get_all_tickers()
            logger.info(f"本次任務共需抓取 {len(target_universe)} 檔標的")

            # 1. 抓取股價資料
            self._fetch_and_store_prices(target_universe, history_days)
            
            # 2. 抓取 ETF 持倉 (供 L2 或後續權重分析使用)
            # self._fetch_etf_holdings(self.l0_tickers + self.l1_tickers)
            
            # 3. 抓取 MMF 總體經濟指標 (避險資金判斷)
            # self._fetch_macro_data()
            
        elif market == 'tw':
            logger.warning("台股爬蟲模組尚未實作 (將於後續版本接軌)")
            pass

    def _fetch_and_store_prices(self, tickers, history_days):
        """
        抓取歷史股價並直接寫入 Database，嚴格控管記憶體 (支援 30 年無縫分塊抓取)
        """
        from py_module.database import DatabaseManipulation
        db = DatabaseManipulation(self.config)

        success_count = 0
        
        for i, ticker in enumerate(tickers):
            try:
                # ---------------------------------------------------------
                # 🌟 新增：具備「防腐敗」檢查的斷點續傳機制
                # ---------------------------------------------------------
                backup_dir = Path("/workspace/raw_backup")
                backup_file = backup_dir / f"{ticker}_30yr_backup.json.gz"
                
                if history_days > 5000 and backup_file.exists():
                    import os
                    # 取得檔案最後修改時間
                    file_mtime = backup_file.stat().st_mtime
                    file_age_days = (time.time() - file_mtime) / (24 * 3600)
                    
                    # 嚴格限制：只有 7 天內的備份才被視為安全 (無拆股/除息風險)
                    if file_age_days <= 7:
                        logger.info(f"[{i+1}/{len(tickers)}] ♻️ {ticker} 備份有效 (距今 {file_age_days:.1f} 天)，從本地載入...")
                        with gzip.open(backup_file, 'rt', encoding='utf-8') as f:
                            cached_data = json.load(f)
                            
                        df = self._parse_fmp_json(cached_data, ticker)
                        if df is not None and not df.empty:
                            db.upsert_market_data(df)
                            success_count += 1
                        continue # 跳過 API 抓取
                    else:
                        logger.warning(f"[{i+1}/{len(tickers)}] ⚠️ {ticker} 備份已過期 ({file_age_days:.1f} 天 > 7 天)，可能存在未調整之拆股資料，強制重新向 FMP 抓取最新數據！")
                # ---------------------------------------------------------

                logger.info(f"[{i+1}/{len(tickers)}] 🌐 正在從 FMP API 抓取: {ticker}")
                
                # 準備承接所有時間區塊資料的容器
                aggregated_data = []
                
                # 🌟 關鍵優化：實作 FMP 官方規定的「5 年分塊迴圈」
                current_end_date = datetime.now()
                final_start_date = current_end_date - timedelta(days=history_days)
                
                # FMP 規定每次區間不能超過 5 年 (約 1825 天)
                chunk_days = 1825 
                
                while current_end_date > final_start_date:
                    current_start_date = current_end_date - timedelta(days=chunk_days)
                    # 確保不會超抓超過使用者設定的歷史天數
                    if current_start_date < final_start_date:
                        current_start_date = final_start_date
                        
                    str_start = current_start_date.strftime("%Y-%m-%d")
                    str_end = current_end_date.strftime("%Y-%m-%d")
                    
                    # 統一使用 stable 端點，帶入迴圈計算好的 from 與 to
                    url = f"{self.base_url}/stable/historical-price-eod/full?symbol={ticker}&from={str_start}&to={str_end}&apikey={self.api_key}"
                    
                    response = requests.get(url, timeout=10)
                    
                    if response.status_code == 200:
                        chunk_data = response.json()
                        # FMP 回傳的資料可能是 list 或 dict，將其無縫接合到大容器中
                        if isinstance(chunk_data, list):
                            aggregated_data.extend(chunk_data)
                        elif isinstance(chunk_data, dict) and "historical" in chunk_data:
                            aggregated_data.extend(chunk_data["historical"])
                        elif isinstance(chunk_data, dict) and "data" in chunk_data:
                            aggregated_data.extend(chunk_data["data"])
                            
                    elif response.status_code == 429:
                        logger.warning("達到 API 速率限制，暫停 5 秒...")
                        time.sleep(5)
                        continue # 迴圈不推進，重試此區間
                    else:
                        logger.error(f"抓取 {ticker} ({str_start} 至 {str_end}) 失敗，狀態碼: {response.status_code}")
                        
                    # 推進時間軸，準備抓取上一個 5 年 (往前推 1 天避免日期重疊)
                    current_end_date = current_start_date - timedelta(days=1)
                
                # --- 迴圈分塊抓取完畢，開始處理與落地 ---
                if aggregated_data:
                    # 🌟 歷史大灌注 (>5000天)，實體備份 30 年的完整 JSON
                    if history_days > 5000:
                        backup_dir = Path("/workspace/raw_backup")
                        backup_dir.mkdir(exist_ok=True)
                        backup_file = backup_dir / f"{ticker}_30yr_backup.json.gz"
                        with gzip.open(backup_file, 'wt', encoding='utf-8') as f:
                            json.dump(aggregated_data, f)
                        logger.info(f"💾 已將 {ticker} 的 30 年原始 JSON 壓縮備份至磁碟 (共 {len(aggregated_data)} 筆)。")

                    # 將合併後的 List 交給原有的 parser 處理
                    df = self._parse_fmp_json(aggregated_data, ticker)
                    
                    if df is not None and not df.empty:
                        db.upsert_market_data(df)
                        success_count += 1
                        
            except Exception as e:
                logger.error(f"抓取 {ticker} 時發生例外錯誤: {str(e)}")
            
            finally:
                gc.collect()
                time.sleep(0.1) # 您的 Ultimate 方案有 3000 次/分額度，0.1 秒延遲非常安全

        logger.info(f"股價資料抓取完畢！成功: {success_count}/{len(tickers)}")

    def _parse_fmp_json(self, data, ticker):
        """
        解析 FMP 回傳的 JSON 並標準化欄位
        """
        df = None
        if isinstance(data, list):
            df = pd.DataFrame(data)
        elif isinstance(data, dict) and "historical" in data:
            df = pd.DataFrame(data["historical"])
        elif isinstance(data, dict) and "data" in data:
            df = pd.DataFrame(data["data"])
            
        if df is not None and not df.empty:
            if 'symbol' not in df.columns:
                df['symbol'] = ticker
                
            # 欄位重新命名以符合 SQL Server Schema
            rename_map = {
                'date': 'Date', 'symbol': 'Symbol', 'open': 'Open',
                'high': 'High', 'low': 'Low', 'close': 'Close', 'volume': 'Volume'
            }
            # 只保留需要的欄位
            df_clean = df[list(set(rename_map.keys()).intersection(df.columns))].rename(columns=rename_map)
            df_clean['Date'] = pd.to_datetime(df_clean['Date'])
            return df_clean
        return None