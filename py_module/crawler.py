import os
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
        self.nasdaq_api_key = os.getenv("NASDAQ_DL_API_KEY", "")
        self.base_url = "https://financialmodelingprep.com"
        self.sfp_base = "https://data.nasdaq.com/api/v3"
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
        """
        主入口：執行核心標的宇宙的每日增量爬取。

        資料源分工（v3，2026-06）：
          OHLCV   ← SFP（SHARADAR/SFP），單次批量請求，無 FMP call 消耗
          MC      ← FMP BASIC（historical-market-capitalization），每 symbol 1 call
          拆股修正 ← fix_split_mc（SFP closeunadj × stable_shares），獨立執行

        ⚠️ 拆股不再觸發 30 年重爬：拆股事件由 fix_split_mc --apply 處理，
           爬蟲只做每日增量，不依賴 FMP ULTRA。
        """
        logger.info(f"🚀 開始執行 {market.upper()} 市場數據任務（核心 41 symbols）...")

        if market == 'us':
            target_universe = self.config.get_core_tickers()
            logger.info(f"📋 目標清單：{len(target_universe)} symbols（FMP BASIC 安全範圍）")

            if self.nasdaq_api_key:
                # 優先路徑：SFP OHLCV + FMP BASIC MC
                logger.info("🟢 使用 SFP（SHARADAR/SFP）抓取 OHLCV，FMP BASIC 抓取 MC")
                self._fetch_sfp_prices(target_universe, history_days)
            else:
                # 降級路徑：純 FMP（NASDAQ_DL_API_KEY 未設定時）
                logger.warning("⚠️ NASDAQ_DL_API_KEY 未設定，降級為純 FMP BASIC 爬蟲")
                self._fetch_and_store_prices(target_universe, history_days)

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

    def _fetch_sfp_prices(self, tickers, history_days):
        """
        [SFP 路徑] 使用 SHARADAR/SFP 抓取 OHLCV + FMP BASIC 抓取 MC，寫入 DB。

        優點：
          - SFP 單次批量請求所有 symbols（無 FMP call 消耗）
          - closeunadj 欄位保留真實未調整價（fix_split_mc 修正依據）
          - FMP BASIC MC：每 symbol 1 call，41 symbols = 41 calls << 250/day 上限
        """
        import gc

        end_date   = datetime.now()
        start_date = end_date - timedelta(days=history_days)
        str_start  = start_date.strftime("%Y-%m-%d")
        str_end    = end_date.strftime("%Y-%m-%d")

        # ── 1. SFP 批量抓取 OHLCV ────────────────────────────────
        logger.info(f"[SFP] 批量抓取 {len(tickers)} symbols OHLCV ({str_start} ~ {str_end})...")
        sfp_rows = []
        cursor   = None
        tickers_str = ",".join(tickers)

        while True:
            params = {
                "ticker":         tickers_str,
                "date.gte":       str_start,
                "date.lte":       str_end,
                "qopts.columns":  "ticker,date,open,high,low,close,volume,closeunadj",
                "qopts.per_page": 10000,
                "api_key":        self.nasdaq_api_key,
            }
            if cursor:
                params["qopts.cursor_id"] = cursor
            try:
                resp = self.session.get(
                    f"{self.sfp_base}/datatables/SHARADAR/SFP.json",
                    params=params, timeout=60
                )
                d      = resp.json()
                page   = d.get("datatable", {}).get("data", [])
                sfp_rows.extend(page)
                cursor = d.get("meta", {}).get("next_cursor_id")
                if not cursor:
                    break
            except Exception as e:
                logger.error(f"❌ SFP 批量抓取失敗: {e}")
                break

        if not sfp_rows:
            logger.warning("⚠️ SFP 回傳 0 筆資料，跳過本次更新。")
            return

        df_sfp = pd.DataFrame(sfp_rows,
                              columns=["Symbol", "Date", "Open", "High", "Low", "Close",
                                       "Volume", "closeunadj"])
        df_sfp["Date"] = pd.to_datetime(df_sfp["Date"])
        df_sfp.drop_duplicates(subset=["Symbol", "Date"], inplace=True)
        logger.info(f"[SFP] 取得 {len(df_sfp)} 筆 OHLCV（{df_sfp['Symbol'].nunique()} symbols）")

        # ── 2. FMP BASIC 逐一抓取 MC ─────────────────────────────
        logger.info(f"[FMP] 逐一抓取 {len(tickers)} symbols 市值...")
        all_mc = []
        for idx, symbol in enumerate(tickers):
            try:
                mc_url = (f"{self.base_url}/stable/historical-market-capitalization"
                          f"?symbol={symbol}&from={str_start}&to={str_end}&apikey={self.api_key}")
                mc_resp = self.session.get(mc_url, timeout=30).json()
                if isinstance(mc_resp, list):
                    all_mc.extend(mc_resp)
            except Exception as e:
                logger.warning(f"⚠️ [{idx+1}/{len(tickers)}] {symbol} MC 抓取失敗: {e}")
            time.sleep(0.05)

        df_mc = pd.DataFrame(all_mc) if all_mc else pd.DataFrame(columns=["symbol", "date", "marketCap"])
        if not df_mc.empty:
            df_mc.rename(columns={"symbol": "Symbol", "date": "Date", "marketCap": "Market_Cap"}, inplace=True)
            df_mc["Date"] = pd.to_datetime(df_mc["Date"])
            df_mc.drop_duplicates(subset=["Symbol", "Date"], inplace=True)

        # ── 3. 合併並寫入 DB ──────────────────────────────────────
        if not df_mc.empty:
            df_final = pd.merge(df_sfp, df_mc[["Symbol", "Date", "Market_Cap"]],
                                on=["Symbol", "Date"], how="left")
        else:
            df_final = df_sfp.copy()
            df_final["Market_Cap"] = None

        df_final["Shares_Outstanding"] = df_final.apply(
            lambda r: r["Market_Cap"] / r["Close"]
            if pd.notna(r.get("Market_Cap")) and pd.notna(r["Close"]) and r["Close"] != 0
            else None,
            axis=1
        )

        # upsert 前先保留帶 closeunadj 的副本，供拆股 patch 使用
        df_for_patch = df_final.copy()

        df_final.drop(columns=["closeunadj"], inplace=True)
        self.db.upsert_market_data(df_final)
        logger.info(f"✅ SFP 爬蟲完成：{len(df_final)} 筆寫入 DB")

        # Change 2 & 3：upsert 後補寫拆股修正 + 偵測新拆股
        self._patch_split_corrections(df_for_patch)

        del df_sfp, df_mc, df_final, df_for_patch, sfp_rows, all_mc
        gc.collect()

    def _patch_split_corrections(self, df_merged):
        """
        Change 2 & 3：每日爬蟲結束後自動執行的 MC 修正補丁。

        Change 2 — 活躍拆股修正：
          對本次爬取的日期中，屬於 pre-split 或 staircase 期的記錄，
          補寫正確的 Market_Cap_Refined（upsert 已重置，需重補）。

        Change 3 — 新拆股偵測：
          若 7 天內偵測到新拆股，記錄警告並提示執行 fix_split_mc --apply
          以修正完整的歷史 MC 資料。
        """
        from datetime import date, timedelta
        from sqlalchemy import text

        if not self.nasdaq_api_key:
            return

        today        = date.today()
        window_start = (today - timedelta(days=90)).strftime("%Y-%m-%d")
        new_split_threshold = (today - timedelta(days=7)).strftime("%Y-%m-%d")

        tickers = df_merged["Symbol"].dropna().unique().tolist()
        if not tickers:
            return

        # ── 1. 查詢近 90 天的拆股事件 ────────────────────────────
        try:
            r = self.session.get(
                f"{self.sfp_base}/datatables/SHARADAR/ACTIONS.json",
                params={
                    "ticker":   ",".join(tickers),
                    "action":   "split",
                    "date.gte": window_start,
                    "api_key":  self.nasdaq_api_key,
                },
                timeout=20,
            )
            action_rows = r.json().get("datatable", {}).get("data", []) if r.ok else []
        except Exception as e:
            logger.warning(f"⚠️ [patch] ACTIONS 查詢失敗: {e}")
            return

        if not action_rows:
            return

        # ── 2. 逐一處理每個活躍拆股 ──────────────────────────────
        for row in action_rows:
            split_date_str, _, symbol, _, ratio, _, _ = row
            if abs(ratio - 1.0) < 0.001:
                continue

            # Change 3：新拆股警告（7 天內）
            if split_date_str >= new_split_threshold:
                logger.warning(
                    f"🚨 [新拆股] {symbol} {split_date_str} ratio={ratio:.4f} — "
                    f"FMP 歷史 MC 已被回溯污染！"
                    f"請儘速執行：docker exec sectorflux_worker "
                    f"python main.py --item fix_split_mc --apply"
                )

            # 取得 stable_shares
            stable_df = self.db.execute_query("""
                SELECT TOP 10 ROUND(Market_Cap / NULLIF([Close], 0), 0) AS Shares
                FROM Fact_DailyPrice
                WHERE Symbol = :sym
                  AND [Date] <  DATEADD(day, -14, :sd)
                  AND [Date] >= DATEADD(day, -75, :sd)
                  AND Market_Cap IS NOT NULL AND Market_Cap > 0
                  AND [Close]    IS NOT NULL AND [Close]    > 0
                ORDER BY [Date] DESC
            """, params={"sym": symbol, "sd": split_date_str})

            if stable_df.empty:
                continue
            stable_shares = int(stable_df["Shares"].median())
            if stable_shares < 1_000_000:
                continue
            post_shares = int(stable_shares * ratio)

            # 取得 staircase_end（如有）
            tol = post_shares * 0.02
            sc_df = self.db.execute_query("""
                SELECT MIN([Date]) AS stable_date
                FROM Fact_DailyPrice
                WHERE Symbol = :sym AND [Date] > :sd AND [Close] > 0
                  AND ABS(ROUND(Market_Cap / NULLIF([Close], 0), 0) - :exp) <= :tol
            """, params={"sym": symbol, "sd": split_date_str,
                         "exp": post_shares, "tol": tol})

            staircase_end = (str(sc_df.iloc[0, 0])[:10]
                             if not sc_df.empty and sc_df.iloc[0, 0] is not None
                             else None)

            cutoff = staircase_end if staircase_end else split_date_str

            # 篩選本次爬取資料中需要修正的行
            sym_df = df_merged[df_merged["Symbol"] == symbol].copy()
            if sym_df.empty:
                continue

            sym_df["_dstr"] = sym_df["Date"].dt.strftime("%Y-%m-%d")
            to_correct = sym_df[sym_df["_dstr"] < cutoff]
            if to_correct.empty:
                continue

            # 組裝批次更新記錄
            records = []
            for _, r2 in to_correct.iterrows():
                dstr       = r2["_dstr"]
                closeunadj = r2.get("closeunadj")
                fmp_close  = r2["Close"]
                fmp_mc     = r2.get("Market_Cap")

                if not closeunadj or closeunadj <= 0:
                    continue

                if dstr < split_date_str:
                    # pre-split：MC_refined = MC × (closeunadj / close)
                    if not fmp_mc or not fmp_close or fmp_close == 0:
                        continue
                    mc_refined = float(fmp_mc) * (closeunadj / fmp_close)
                else:
                    # staircase 期
                    mc_refined = float(closeunadj) * post_shares

                records.append({"mc": mc_refined, "sym": symbol, "d": dstr})

            if not records:
                continue

            try:
                with self.db.engine.connect() as conn:
                    conn.execute(
                        text("UPDATE Fact_DailyPrice "
                             "SET Market_Cap_Refined = :mc "
                             "WHERE Symbol = :sym AND [Date] = :d"),
                        records,
                    )
                    conn.commit()
                logger.info(
                    f"🔧 [patch] {symbol} {split_date_str}: "
                    f"{len(records)} 筆 Market_Cap_Refined 補寫完成"
                )
            except Exception as e:
                logger.error(f"❌ [patch] {symbol} 補寫失敗: {e}")

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