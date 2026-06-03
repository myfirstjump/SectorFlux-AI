"""
NASDAQ Data Link (Quandl) API PoC
測試項目：
1. OHLCV 歷史數據
2. Shares Outstanding / Market Cap / AUM
3. Split / Corporate Actions 資訊
"""
import os
import requests
from datetime import date, timedelta
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.environ["NASDAQ_DL_API_KEY"]
BASE = "https://data.nasdaq.com/api/v3"
L0 = ['XLK', 'XLF', 'XLV', 'XLE', 'XLI', 'XLY', 'XLP', 'XLU', 'XLB', 'XLRE', 'XLC']
TODAY = str(date.today())

def get(url, params=None):
    p = {"api_key": API_KEY}
    if params:
        p.update(params)
    r = requests.get(url, params=p, timeout=15)
    return r.status_code, r.json() if r.headers.get("content-type","").startswith("application/json") else r.text

def tbl(url, params=None):
    p = {"api_key": API_KEY, "qopts.per_page": 5}
    if params:
        p.update(params)
    r = requests.get(url, params=p, timeout=15)
    try:
        return r.status_code, r.json()
    except Exception:
        return r.status_code, r.text

def sep(title):
    print(f"\n{'='*65}")
    print(f"  {title}")
    print('='*65)

# ─────────────────────────────────────────────────────────────
# 0. 確認可用的 database / datatable
# ─────────────────────────────────────────────────────────────
sep("0. 帳戶權限確認")
status, data = get(f"{BASE}/users/me.json")
print(f"HTTP {status}")
if isinstance(data, dict):
    print(f"  Plan      : {data.get('user', {}).get('plan_id','?')}")
    print(f"  Databases : {data.get('user', {}).get('database_count','?')}")

# ─────────────────────────────────────────────────────────────
# 1a. SHARADAR/SEP — Equity Prices (OHLCV)
# ─────────────────────────────────────────────────────────────
sep("1a. SHARADAR/SEP — OHLCV (XLF)")
status, data = tbl(f"{BASE}/datatables/SHARADAR/SEP.json",
                   {"ticker": "XLF", "date.gte": "2025-12-01", "date.lte": "2025-12-12"})
print(f"HTTP {status}")
if isinstance(data, dict) and "datatable" in data:
    cols = data["datatable"]["columns"]
    rows = data["datatable"]["data"]
    print(f"  Columns: {[c['name'] for c in cols]}")
    for row in rows[:6]:
        print(f"  {row}")
else:
    print(f"  {str(data)[:200]}")

# ─────────────────────────────────────────────────────────────
# 1b. 歷史深度 — XLF 1999 年數據
# ─────────────────────────────────────────────────────────────
sep("1b. 歷史深度 — XLF 1999 (SHARADAR/SEP)")
status, data = tbl(f"{BASE}/datatables/SHARADAR/SEP.json",
                   {"ticker": "XLF", "date.gte": "1999-01-01", "date.lte": "1999-01-15"})
print(f"HTTP {status}")
if isinstance(data, dict) and "datatable" in data:
    rows = data["datatable"]["data"]
    print(f"  {len(rows)} rows returned")
    for row in rows:
        print(f"  {row}")
else:
    print(f"  {str(data)[:200]}")

# ─────────────────────────────────────────────────────────────
# 2. SHARADAR/SF1 — Fundamentals (Shares, MC)
# ─────────────────────────────────────────────────────────────
sep("2. SHARADAR/SF1 — Fundamentals (XLF, dimension=ARQ)")
status, data = tbl(f"{BASE}/datatables/SHARADAR/SF1.json",
                   {"ticker": "XLF", "dimension": "ARQ",
                    "qopts.columns": "ticker,caldate,reportperiod,shareswa,marketcap,assetsavg"})
print(f"HTTP {status}")
if isinstance(data, dict) and "datatable" in data:
    cols = data["datatable"]["columns"]
    rows = data["datatable"]["data"]
    print(f"  Columns: {[c['name'] for c in cols]}")
    print(f"  Last 5 rows:")
    for row in rows[-5:]:
        print(f"  {row}")
else:
    print(f"  {str(data)[:300]}")

# ─────────────────────────────────────────────────────────────
# 3. SHARADAR/DAILY — Daily Metrics (MC, PE, sharefactor)
# ─────────────────────────────────────────────────────────────
sep("3. SHARADAR/DAILY — Daily MC & Shares (XLF around split)")
status, data = tbl(f"{BASE}/datatables/SHARADAR/DAILY.json",
                   {"ticker": "XLF",
                    "date.gte": "2025-12-01", "date.lte": "2025-12-15",
                    "qopts.columns": "ticker,date,marketcap,ev,pe,sharebas",
                    "qopts.per_page": 15})
print(f"HTTP {status}")
if isinstance(data, dict) and "datatable" in data:
    cols = data["datatable"]["columns"]
    rows = data["datatable"]["data"]
    print(f"  Columns: {[c['name'] for c in cols]}")
    for row in rows:
        print(f"  {row}")
else:
    print(f"  {str(data)[:300]}")

# XLK の 拆股 前後
sep("3b. SHARADAR/DAILY — XLK split window (marketcap,sharebas)")
status, data = tbl(f"{BASE}/datatables/SHARADAR/DAILY.json",
                   {"ticker": "XLK",
                    "date.gte": "2025-12-01", "date.lte": "2026-01-20",
                    "qopts.columns": "ticker,date,marketcap,sharebas",
                    "qopts.per_page": 50})
print(f"HTTP {status}")
if isinstance(data, dict) and "datatable" in data:
    rows = data["datatable"]["data"]
    print(f"  {len(rows)} rows")
    for row in rows:
        print(f"  {row}")
else:
    print(f"  {str(data)[:300]}")

# ─────────────────────────────────────────────────────────────
# 4. SHARADAR/ACTIONS — Corporate Actions (splits)
# ─────────────────────────────────────────────────────────────
sep("4. SHARADAR/ACTIONS — Split Events (L0 Sectors, 2025-12)")
status, data = tbl(f"{BASE}/datatables/SHARADAR/ACTIONS.json",
                   {"action": "split",
                    "date.gte": "2025-11-01", "date.lte": "2026-01-31",
                    "qopts.per_page": 20})
print(f"HTTP {status}")
if isinstance(data, dict) and "datatable" in data:
    cols = data["datatable"]["columns"]
    rows = data["datatable"]["data"]
    print(f"  Columns: {[c['name'] for c in cols]}")
    for row in rows:
        print(f"  {row}")
else:
    print(f"  {str(data)[:300]}")

# ─────────────────────────────────────────────────────────────
# 5. SHARADAR/DAILY — 全部 L0 Sectors 最新 MC 對比
# ─────────────────────────────────────────────────────────────
sep("5. 全部 L0 Sectors 最新 MC (SHARADAR/DAILY)")
tickers_str = ",".join(L0)
status, data = tbl(f"{BASE}/datatables/SHARADAR/DAILY.json",
                   {"ticker": tickers_str,
                    "date.gte": "2026-05-25",
                    "qopts.columns": "ticker,date,marketcap,sharebas",
                    "qopts.per_page": 30})
print(f"HTTP {status}")
if isinstance(data, dict) and "datatable" in data:
    rows = data["datatable"]["data"]
    print(f"  {len(rows)} rows")
    print(f"  {'Symbol':<6} {'Date':<12} {'MC_B':>10} {'Shares_M':>12}")
    for row in rows:
        ticker, dt, mc, sh = row
        mc_b = f"{mc/1e9:.2f}B" if mc else "N/A"
        sh_m = f"{sh/1e6:.1f}M" if sh else "N/A"
        print(f"  {ticker:<6} {str(dt):<12} {mc_b:>10} {sh_m:>12}")
else:
    print(f"  {str(data)[:300]}")

# ─────────────────────────────────────────────────────────────
# 6. SHARADAR/SEP vs DB — XLF close price 交叉驗證
# ─────────────────────────────────────────────────────────────
sep("6. XLF 近期收盤價 Nasdaq DL vs DB 交叉驗證")
status, data = tbl(f"{BASE}/datatables/SHARADAR/SEP.json",
                   {"ticker": "XLF", "date.gte": "2026-05-26",
                    "qopts.columns": "ticker,date,open,high,low,close,volume,closeadj,closeunadj",
                    "qopts.per_page": 10})
print(f"HTTP {status}")
if isinstance(data, dict) and "datatable" in data:
    cols = data["datatable"]["columns"]
    rows = data["datatable"]["data"]
    print(f"  Columns: {[c['name'] for c in cols]}")
    for row in rows:
        print(f"  {row}")
    print(f"\n  DB ref: 2026-05-29 close=51.58 MC=49.76B")
else:
    print(f"  {str(data)[:300]}")

print("\n✅ NASDAQ Data Link PoC 完成\n")
