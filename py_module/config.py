import os
from dotenv import load_dotenv
import logging
import requests

logger = logging.getLogger("SectorFlux_Config")

class Configuration:
    def __init__(self):
        """
        初始化 SectorFlux-AI 的全域設定
        """
        # 1. 載入環境變數
        load_dotenv()
        
        # --- API 金鑰設定 ---
        self.FMP_API_KEY = os.getenv("FMP_API_KEY")
        self.FRED_API_KEY = os.getenv("FRED_API_KEY", "YOUR_FRED_KEY_HERE") 
        
        if not self.FMP_API_KEY:
            raise ValueError("❌ 找不到 FMP_API_KEY，請確認 .env 檔案設定！")

        # --- 資料庫連線設定 ---
        self.DB_USER = os.getenv("DB_USER")
        self.DB_PASS = os.getenv("DB_PASS")
        self.DB_HOST = os.getenv("DB_HOST")
        self.DB_PORT = os.getenv("DB_PORT")
        self.DB_NAME = os.getenv("DB_NAME")
        self.DB_CHUNK_SIZE = 5000 
        
        # --- 標的宇宙定義 (The Asset Universe) ---
        self.BASE_BENCHMARK = ['SPY']
        self.RISK_PROXY = ['BIL', 'SHV', 'TLT', 'GLD']
        
        self.L0_SECTORS = [
            'XLK', 'XLF', 'XLV', 'XLE', 'XLI', 'XLY', 
            'XLP', 'XLU', 'XLB', 'XLRE', 'XLC' 
        ]
        
        self.L1_THEMATICS = [
            'SMH', 'ITA', 'XBI',  
            'KRE', 'XRT', 'XOP', 'XME', 'ARKK', 'EWT', 'PAVE', 'COPX'
        ]
        
        self.L2_UNIVERSE = [
            'AAPL', 'MSFT', 'NVDA', 'AMZN', 'META', 'GOOGL', 'TSLA', 'GEV', 'GLW'
        ]

        self.AUTHORITATIVE_ETFS = [
            'QQQ',  
            'DIA',  
            'IWM',
            'SPY',
            'VOO',
        ]

        self.MACRO_UNIVERSE = [
            "GLD", "USO", "UUP",      # 有市值的 ETF
            "^FVX", "^TNX", "^VIX", "DX-Y.NYB" # 無市值的指數
        ]

    @property
    def database_url(self):
        return f"mssql+pyodbc://{self.DB_USER}:{self.DB_PASS}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes"
    
    def get_sp500_tickers(self):
        """動態抓取 S&P 500 成分股"""
        try:
            url = f"https://financialmodelingprep.com/api/v3/sp500_constituent?apikey={self.FMP_API_KEY}"
            resp = requests.get(url, timeout=10)
            if resp.status_code == 200:
                return [item['symbol'] for item in resp.json()]
        except Exception as e:
            logger.error(f"抓取 S&P 500 清單失敗: {e}")
        return []
    
    def get_etf_constituents(self, target_etfs):
        """🌟 核心擴充：使用 Ultimate 方案的 Stable 端點動態展開 ETF 成分股"""
        expanded_tickers = []
        for etf in target_etfs:
            try:
                # 採用 FMP 最新的 Stable ETF Holdings 端點
                url = f"https://financialmodelingprep.com/stable/etf/holdings?symbol={etf}&apikey={self.FMP_API_KEY}"
                resp = requests.get(url, timeout=10)
                
                if resp.status_code == 200:
                    data = resp.json()
                    # 從回傳陣列中提取 'asset' 欄位 (即股票代碼)
                    constituents = [item.get('asset') for item in data if item.get('asset')]
                    expanded_tickers.extend(constituents)
                    logger.info(f"✅ 成功展開 {etf} 成分股: 共 {len(constituents)} 檔標的")
                elif resp.status_code == 403:
                    logger.error(f"❌ 抓取 {etf} 失敗：權限不足 (HTTP 403)。請確認您的 API Key 已正確綁定 Ultimate 方案。")
                else:
                    logger.error(f"⚠️ 抓取 {etf} 回應異常：HTTP {resp.status_code} - {resp.text}")
            except Exception as e:
                logger.error(f"❌ 抓取 {etf} 成分股發生網路例外: {e}")
                
        return expanded_tickers
    
    def get_all_tickers(self):
        """終極宇宙聚合：合併所有靜態清單與動態成分股"""
        logger.info("啟動標的宇宙擴充程序，正在連接 FMP API...")
        
        # 1. 抓取標普 500
        dynamic_sp500 = self.get_sp500_tickers()
        
        # 2. 抓取所有 L0, L1 與權威 ETF 的底層成分股
        etfs_to_expand = self.L0_SECTORS + self.L1_THEMATICS + self.AUTHORITATIVE_ETFS
        dynamic_etf_holdings = self.get_etf_constituents(etfs_to_expand)
        
        # 3. 聚合
        all_tickers = (
            self.BASE_BENCHMARK + 
            self.RISK_PROXY + 
            self.L0_SECTORS + 
            self.L1_THEMATICS + 
            self.L2_UNIVERSE +
            dynamic_sp500 +
            dynamic_etf_holdings
        )
        
        # 去重複並排除可能抓到的空值或錯誤格式
        final_universe = list(set([t for t in all_tickers if isinstance(t, str) and t.strip() != '']))
        logger.info(f"🌍 標的宇宙擴充完畢！本次任務共需監控 {len(final_universe)} 檔標的。")
        return final_universe