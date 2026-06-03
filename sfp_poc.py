"""
Sharadar Fund Prices (SFP) PoC — 訂閱後完整驗證
1. closeunadj 真實性驗證（對比 Tiingo 已知正確值）
2. ACTIONS 拆股事件覆蓋確認
3. 歷史深度 & T+1 可用性
4. 全 L0 Sectors 批量可行性
5. SFP close vs FMP DB close 交叉驗證
"""
import os
import requests
from datetime import date, timedelta
from dotenv import load_dotenv

load_dotenv()
KEY   = os.environ["NASDAQ_DL_API_KEY"]
BASE  = "https://data.nasdaq.com/api/v3"
L0    = ['XLK','XLF','XLV','XLE','XLI','XLY','XLP','XLU','XLB','XLRE','XLC']
TODAY = str(date.today())

def sfp(params):
    p = {"api_key": KEY}
    p.update(params)
    r = requests.get(f"{BASE}/datatables/SHARADAR/SFP.json", params=p, timeout=20)
    d = r.json()
    cols = [c["name"] for c in d.get("datatable", {}).get("columns", [])]
    rows = d.get("datatable", {}).get("data", [])
    meta = d.get("meta", {})
    return r.status_code, cols, rows, meta

def act(params):
    p = {"api_key": KEY}
    p.update(params)
    r = requests.get(f"{BASE}/datatables/SHARADAR/ACTIONS.json", params=p, timeout=20)
    d = r.json()
    cols = [c["name"] for c in d.get("datatable", {}).get("columns", [])]
    rows = d.get("datatable", {}).get("data", [])
    return r.status_code, cols, rows

def sep(title): print(f"\n{'='*65}\n  {title}\n{'='*65}")

# ─────────────────────────────────────────────────────────────
# 1. closeunadj 核心驗證
#    Tiingo 已確認：XLK Dec 3 = $289.99, Dec 4 = $291.07, Dec 5 = $146.60
# ─────────────────────────────────────────────────────────────
sep("1. closeunadj 驗證 — XLK 拆股前後（Tiingo 對照）")
status, cols, rows, _ = sfp({"ticker": "XLK",
                              "date.gte": "2025-12-01", "date.lte": "2025-12-12",
                              "qopts.columns": "ticker,date,close,closeunadj,closeadj,volume"})
print(f"HTTP {status} | cols: {cols}")
print(f"{'Date':<12} {'close(adj)':>12} {'closeunadj':>12} {'Tiingo_ref':>12} {'Match?':>8}")
tiingo_ref = {"2025-12-03": 289.99, "2025-12-04": 291.07, "2025-12-05": 146.60,
              "2025-12-08": 147.63}
for row in rows:
    _, dt, close, cunadj, cadj, vol = row
    dstr = str(dt)[:10]
    ref  = tiingo_ref.get(dstr, "—")
    match = "✅" if isinstance(ref, float) and abs(cunadj - ref) < 0.05 else ("—" if ref == "—" else "❌")
    print(f"  {dstr:<12} {close:>12.4f} {cunadj:>12.4f} {str(ref):>12} {match:>8}")

# ─────────────────────────────────────────────────────────────
# 2. ACTIONS — L0 Sectors 拆股事件
# ─────────────────────────────────────────────────────────────
sep("2. SHARADAR/ACTIONS — L0 Sectors 拆股事件（全時間）")
status, cols, rows = act({"ticker": ",".join(L0),
                          "action": "split",
                          "date.gte": "1998-01-01"})
print(f"HTTP {status} | cols: {cols}")
print(f"  共 {len(rows)} 筆 split 事件")
for row in rows:
    print(f"  {row}")

# ─────────────────────────────────────────────────────────────
# 3. 歷史深度 — 各年代第一筆
# ─────────────────────────────────────────────────────────────
sep("3. 歷史深度 — XLF 各年代 (1999/2005/2010/2020/2024)")
for year, (s, e) in [("1999", ("1999-01-01","1999-01-10")),
                      ("2005", ("2005-01-03","2005-01-07")),
                      ("2010", ("2010-01-04","2010-01-08")),
                      ("2020", ("2020-01-02","2020-01-06")),
                      ("2024", ("2024-01-02","2024-01-05"))]:
    _, _, rows, _ = sfp({"ticker": "XLF", "date.gte": s, "date.lte": e,
                          "qopts.columns": "ticker,date,close,closeunadj"})
    if rows:
        r = rows[0]
        print(f"  {year}: date={str(r[1])[:10]}  close={r[2]}  closeunadj={r[3]}")
    else:
        print(f"  {year}: 0 rows")

# ─────────────────────────────────────────────────────────────
# 4. T+1 可用性 — 最近 5 個交易日
# ─────────────────────────────────────────────────────────────
sep("4. T+1 可用性 — XLF 最近資料")
status, cols, rows, _ = sfp({"ticker": "XLF",
                              "date.gte": str(date.today() - timedelta(days=10)),
                              "qopts.columns": "ticker,date,close,closeunadj,volume"})
print(f"HTTP {status} | {len(rows)} rows")
for row in rows[-5:]:
    print(f"  {str(row[1])[:10]}  close={row[2]}  closeunadj={row[3]}  vol={row[4]}")

# ─────────────────────────────────────────────────────────────
# 5. 全 L0 Sectors 最新 close (批量單次請求)
# ─────────────────────────────────────────────────────────────
sep("5. 批量請求 — 全部 L0 Sectors 最新 close")
status, cols, rows, meta = sfp({"ticker": ",".join(L0),
                                 "date.gte": str(date.today() - timedelta(days=5)),
                                 "qopts.columns": "ticker,date,close,closeunadj,volume"})
print(f"HTTP {status} | {len(rows)} rows | next_cursor: {meta.get('next_cursor_id')}")
# 每個 symbol 只顯示最新一筆
latest = {}
for row in rows:
    sym = row[0]
    if sym not in latest or row[1] > latest[sym][1]:
        latest[sym] = row
print(f"  {'Symbol':<6} {'Date':<12} {'close':>10} {'closeunadj':>12} {'volume':>14}")
for sym in L0:
    if sym in latest:
        r = latest[sym]
        print(f"  {r[0]:<6} {str(r[1])[:10]:<12} {r[2]:>10.4f} {r[3]:>12.4f} {r[4]:>14,}")
    else:
        print(f"  {sym:<6} NOT FOUND")

# ─────────────────────────────────────────────────────────────
# 6. SFP closeunadj vs DB FMP close — 拆股前後對比
#    驗證 FMP 回溯調整的「污染範圍」
# ─────────────────────────────────────────────────────────────
sep("6. SFP vs FMP DB — XLK/XLE/XLY/XLU/XLB 拆股日前後 closeunadj")
SPLIT_SYMS = ['XLK','XLE','XLY','XLU','XLB']
status, cols, rows, _ = sfp({"ticker": ",".join(SPLIT_SYMS),
                              "date.gte": "2025-11-01", "date.lte": "2025-12-15",
                              "qopts.columns": "ticker,date,close,closeunadj"})
print(f"HTTP {status} | {len(rows)} rows")
# 只顯示 closeunadj / close 比值顯著 ≠ 1 的日期（= FMP 回溯調整了）
print(f"  {'Sym':<5} {'Date':<12} {'SFP_close':>10} {'closeunadj':>12} {'ratio':>8} {'FMP_adjusted?':>15}")
for row in rows:
    sym, dt, cl, cunadj = row
    ratio = cunadj / cl if cl and cl != 0 else 0
    flag  = "YES ← FMP split!" if ratio > 1.8 else ("~ok" if 0.95 < ratio < 1.05 else f"ratio={ratio:.2f}")
    if ratio > 1.5 or not (0.95 < ratio < 1.05):  # 只顯示有問題的日期
        print(f"  {sym:<5} {str(dt)[:10]:<12} {cl:>10.4f} {cunadj:>12.4f} {ratio:>8.4f} {flag:>15}")

# ─────────────────────────────────────────────────────────────
# 7. 計算修正後 MC 樣本驗證
# ─────────────────────────────────────────────────────────────
sep("7. MC 修正效果預覽 — XLK Dec 3 (理論應為 ~$79B)")
_, _, xrows, _ = sfp({"ticker": "XLK", "date.gte": "2025-12-03", "date.lte": "2025-12-05",
                       "qopts.columns": "ticker,date,close,closeunadj"})
XLK_STABLE_SHARES = 272_055_991
for row in xrows:
    sym, dt, cl, cunadj = row
    mc_fmp = cl * XLK_STABLE_SHARES / 1e9
    mc_sfp = cunadj * XLK_STABLE_SHARES / 1e9
    print(f"  {str(dt)[:10]}  FMP_MC={mc_fmp:.2f}B  SFP_corrected={mc_sfp:.2f}B  ratio={mc_sfp/mc_fmp:.2f}x")

print("\n✅ SFP PoC 完成\n")
