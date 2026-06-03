"""
Tiingo API PoC — 三大驗證項目
1. 歷史數據批量爬取（STARTER 方案的歷史深度）
2. 每日數據的 T+1 可用性
3. Market Cap 數值正確性
"""
import os
import requests
import json
from datetime import date, timedelta
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.environ["TIINGO_API_KEY"]
HEADERS = {"Content-Type": "application/json"}

L0_SECTORS = ['XLK', 'XLF', 'XLV', 'XLE', 'XLI', 'XLY', 'XLP', 'XLU', 'XLB', 'XLRE', 'XLC']
TODAY = date.today()
YESTERDAY = TODAY - timedelta(days=1)

def get(url):
    r = requests.get(url, headers=HEADERS)
    return r.status_code, r.json() if r.ok else r.text

def sep(title):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print('='*60)

# ─────────────────────────────────────────────
# 1. 基本連線 + Ticker metadata
# ─────────────────────────────────────────────
sep("1. Ticker Metadata (XLF)")
status, data = get(f"https://api.tiingo.com/tiingo/daily/XLF?token={API_KEY}")
print(f"HTTP {status}")
if isinstance(data, dict):
    print(f"  Name       : {data.get('name')}")
    print(f"  Description: {data.get('description','')[:80]}")
    print(f"  startDate  : {data.get('startDate')}")
    print(f"  endDate    : {data.get('endDate')}")

# ─────────────────────────────────────────────
# 2. 歷史深度測試 — 各 Decade
# ─────────────────────────────────────────────
sep("2. 歷史深度 — XLF 各年代第一筆")
test_dates = {
    "1999": ("1999-01-01", "1999-01-31"),
    "2005": ("2005-01-01", "2005-01-10"),
    "2010": ("2010-01-01", "2010-01-10"),
    "2020": ("2020-01-01", "2020-01-10"),
    "2024": ("2024-01-01", "2024-01-10"),
}
for label, (s, e) in test_dates.items():
    status, data = get(
        f"https://api.tiingo.com/tiingo/daily/XLF/prices"
        f"?startDate={s}&endDate={e}&token={API_KEY}"
    )
    if isinstance(data, list) and data:
        row = data[0]
        print(f"  {label}: HTTP {status} | date={row['date'][:10]} | close={row['close']} | adjClose={row['adjClose']}")
    else:
        print(f"  {label}: HTTP {status} | {str(data)[:80]}")

# ─────────────────────────────────────────────
# 3. T+1 可用性 — 昨日與今日
# ─────────────────────────────────────────────
sep(f"3. T+1 可用性 — 最近 5 個交易日 (XLF)")
status, data = get(
    f"https://api.tiingo.com/tiingo/daily/XLF/prices"
    f"?startDate={TODAY - timedelta(days=10)}&endDate={TODAY}&token={API_KEY}"
)
if isinstance(data, list):
    print(f"  最新 {len(data)} 筆，最後 5 筆：")
    for row in data[-5:]:
        print(f"    {row['date'][:10]} | close={row['close']} | adjClose={row['adjClose']} | vol={row.get('volume','n/a')}")
else:
    print(f"  HTTP {status} | {str(data)[:120]}")

# ─────────────────────────────────────────────
# 4. Market Cap — fundamentals/daily endpoint
# ─────────────────────────────────────────────
sep("4. Market Cap — fundamentals/daily (XLF)")
status, data = get(
    f"https://api.tiingo.com/tiingo/fundamentals/XLF/daily?token={API_KEY}"
)
print(f"HTTP {status}")
if isinstance(data, list) and data:
    print(f"  共 {len(data)} 筆，最後 3 筆：")
    for row in data[-3:]:
        mc = row.get('marketCap')
        mc_b = f"{mc/1e9:.2f}B" if mc else "N/A"
        print(f"    {row.get('date','?')[:10]} | marketCap={mc_b} | peRatio={row.get('peRatio','?')}")
    print(f"\n  最舊 3 筆：")
    for row in data[:3]:
        mc = row.get('marketCap')
        mc_b = f"{mc/1e9:.2f}B" if mc else "N/A"
        print(f"    {row.get('date','?')[:10]} | marketCap={mc_b}")
elif isinstance(data, dict) and 'detail' in data:
    print(f"  ⛔ 無法存取: {data['detail']}")
else:
    print(f"  {str(data)[:200]}")

# ─────────────────────────────────────────────
# 5. Market Cap — 批量 L0 Sectors 最新值
# ─────────────────────────────────────────────
sep("5. Market Cap — 全部 L0 Sectors 最新值對比")
print(f"{'Symbol':<8} {'Tiingo MC':>14} {'Tiingo Close':>14} {'HTTP':>6}")
for sym in L0_SECTORS:
    status, data = get(
        f"https://api.tiingo.com/tiingo/fundamentals/{sym}/daily?token={API_KEY}"
    )
    if isinstance(data, list) and data:
        last = data[-1]
        mc = last.get('marketCap')
        mc_str = f"{mc/1e9:.2f}B" if mc else "N/A"
        # get latest price too
        s2, d2 = get(
            f"https://api.tiingo.com/tiingo/daily/{sym}/prices"
            f"?startDate={TODAY - timedelta(days=5)}&endDate={TODAY}&token={API_KEY}"
        )
        close = d2[-1]['close'] if isinstance(d2, list) and d2 else "?"
        print(f"  {sym:<6} {mc_str:>14} {str(close):>14} {status:>6}")
    else:
        detail = data.get('detail','err') if isinstance(data, dict) else str(data)[:40]
        print(f"  {sym:<6} {'UNAVAIL':>14} {'?':>14} {status:>6}  ← {detail}")

# ─────────────────────────────────────────────
# 6. 對比 DB 值（XLF 今日）
# ─────────────────────────────────────────────
sep("6. XLF 今日 Tiingo vs DB 對比")
status, data = get(
    f"https://api.tiingo.com/tiingo/fundamentals/XLF/daily?token={API_KEY}"
)
if isinstance(data, list) and data:
    last = data[-1]
    mc = last.get('marketCap')
    print(f"  Tiingo  date={last.get('date','?')[:10]} | marketCap={mc/1e9:.2f}B" if mc else "  MC=N/A")
    print(f"  DB      date=2026-05-29 | marketCap=49.76B  (from prev session query)")
    if mc:
        diff_pct = (mc/1e9 - 49.76) / 49.76 * 100
        print(f"  差異    {diff_pct:+.1f}%")

print("\n✅ PoC 完成\n")
