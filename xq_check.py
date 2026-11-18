"""
XQ 社群兩個選股腳本 → 台股回測（同一批約 380 支隨機抽樣股票、2019 年起）
=====================================================================
A．技術面「動能・波動・趨勢」三合一（隔天收盤買）
   MACD(12,26,9) 柱狀體今天 > 0、昨天 ≤ 0（加權收盤價 (H+L+2C)/4）
   布林(20, 2) 開口：帶寬比昨天大，且上軌往上、下軌往下
   收盤 > 20 日線，且 20 日線比昨天高；當日成交量 ≥ 500 張
B．存股「股息說明一切」＋營運動能（隔天收盤買）
   20 日均量 ≥ 300 張、股價 ≥ 10；殖利率 ≥ 5%；近四季 EPS > 0；負債比 0～60%
   20 日波動 ≤ 2.5%；距 240 日高點 ≥ −10%；近 3 個月營收年增平均 ≥ 10%；單季 EPS 年增 ≥ 10%
   （作者只用到 2024 年的資料調參 → 2025 年後是「沒看過的資料」，特別看這段）

時間點：月營收 M+1 月 11 日起才知道；財報 Q1～Q3 季底後 45 天、Q4 季底後 90 天才知道。
統計：報酬用還原價、每筆扣 0.5% 成本；和同一天所有合格股票的平均比（超額），逐月 t、前後半、2025 年後。
"""
import bisect
import math
import time
from datetime import datetime, timedelta

import signal_lab as L
import strategy_check as C

log = L.log
_f = L._f
COST = 0.5
HOLDS = [5, 20, 60]
OOS = "2025-01-01"


# ── 指標 ──────────────────────────────────────────────
def ema(vals, n):
    out, k, e = [], 2 / (n + 1), None
    for v in vals:
        e = v if e is None else e + k * (v - e)
        out.append(e)
    return out


def arrays(price_rows, per_rows):
    seq = C.ohlc_rows(price_rows)
    n = len(seq)
    f = [x["lv"] / x["c"] if x["c"] else 1.0 for x in seq]
    a = {"d": [x["d"] for x in seq], "n": n, "V": [x["v"] for x in seq], "raw": [x["c"] for x in seq],
         "C": [x["lv"] for x in seq], "H": [x["h"] * k for x, k in zip(seq, f)], "L": [x["l"] * k for x, k in zip(seq, f)],
         "ret": [x["ret"] for x in seq]}
    wc = [(h + l + 2 * c) / 4 for h, l, c in zip(a["H"], a["L"], a["C"])]
    dif = [x - y for x, y in zip(ema(wc, 12), ema(wc, 26))]
    sig = ema(dif, 9)
    a["osc"] = [x - y for x, y in zip(dif, sig)]
    a["ma20"] = L.rolling_mean(a["C"], 20)
    a["vol20"] = L.rolling_mean(a["V"], 20)
    up, dn, sd20 = [None] * n, [None] * n, [None] * n
    for i in range(19, n):
        w = a["C"][i - 19:i + 1]
        m = sum(w) / 20
        sd = math.sqrt(sum((x - m) ** 2 for x in w) / 20)
        up[i], dn[i] = m + 2 * sd, m - 2 * sd
        r = a["ret"][i - 19:i + 1]
        mr = sum(r) / 20
        sd20[i] = math.sqrt(sum((x - mr) ** 2 for x in r) / 19) * 100
    a["up"], a["dn"], a["vol_pct"] = up, dn, sd20
    per = {r["date"][:10]: (_f(r.get("PER")), _f(r.get("dividend_yield"))) for r in (per_rows or [])}
    a["pe"] = [per.get(d, (None, None))[0] for d in a["d"]]
    a["yld"] = [per.get(d, (None, None))[1] for d in a["d"]]
    hi = []
    for i in range(n):
        hi.append(max(a["H"][max(0, i - 239):i + 1]) if i >= 239 else None)
    a["hi240"] = hi
    return a


def sig_tech(a, i):
    if i < 30 or a["ma20"][i] is None or a["ma20"][i - 1] is None or a["up"][i - 1] is None:
        return False
    bw = (a["up"][i] - a["dn"][i]) / a["ma20"][i]
    bw0 = (a["up"][i - 1] - a["dn"][i - 1]) / a["ma20"][i - 1]
    return (a["osc"][i] > 0 >= a["osc"][i - 1] and bw > bw0 and a["up"][i] > a["up"][i - 1] and a["dn"][i] < a["dn"][i - 1]
            and a["C"][i] > a["ma20"][i] and a["ma20"][i] > a["ma20"][i - 1] and a["V"][i] >= 500)


# ── 財報（不偷看：公布期限後才使用）──────────────────────
def _avail(qend):
    d = datetime.strptime(qend, "%Y-%m-%d")
    return (d + timedelta(days=90 if d.month == 12 else 45)).strftime("%Y-%m-%d")


def eps_series(rows):
    """[(可取得日, 季底, EPS)]"""
    q = sorted((r["date"][:10], _f(r.get("value"))) for r in rows if r.get("type") == "EPS")
    return [(_avail(d), d, v) for d, v in q]


def debt_series(rows):
    """[(可取得日, 負債比%)]：負債總額 ÷ 資產總額"""
    by = {}
    for r in rows:
        t = r.get("type", "")
        if t in ("Liabilities", "TotalLiabilities", "TotalAssets", "Liabilities_per"):
            by.setdefault(r["date"][:10], {})[t] = _f(r.get("value"))
    out = []
    for d, v in sorted(by.items()):
        if "Liabilities_per" in v and v["Liabilities_per"] > 0:
            ratio = v["Liabilities_per"]
        else:
            liab = v.get("Liabilities") or v.get("TotalLiabilities")
            ratio = liab / v["TotalAssets"] * 100 if liab and v.get("TotalAssets") else None
        if ratio is not None:
            out.append((_avail(d), ratio))
    return out


def fund_at(day, eps, debt, rev):
    """day 當天可知的：近四季 EPS、單季 EPS 年增%、負債比、近 3 月營收年增平均"""
    e = [x for x in eps if x[0] <= day]
    ttm = sum(v for _, _, v in e[-4:]) if len(e) >= 4 else None
    yoy = None
    if len(e) >= 5:
        cur, ly = e[-1][2], e[-5][2]
        if ly > 0:
            yoy = (cur / ly - 1) * 100
        elif cur > 0:
            yoy = 999.0                     # 由虧轉盈
    dr = [r for a_, r in debt if a_ <= day]
    rv = [x[1] for x in rev if x[0] <= day][-3:]
    return {"ttm": ttm, "eps_yoy": yoy, "debt": dr[-1] if dr else None, "rev3": sum(rv) / 3 if len(rv) == 3 else None}


def sig_div(a, i, fund):
    if i < 240 or a["vol20"][i] is None or a["vol20"][i] < 300 or a["raw"][i] < 10:
        return False
    y, vp, hi = a["yld"][i], a["vol_pct"][i], a["hi240"][i]
    if not (y and y >= 5 and vp is not None and vp <= 2.5 and hi and a["H"][i] and a["C"][i] / hi - 1 >= -0.10):
        return False
    if fund is None:
        return False
    return ((fund["ttm"] or 0) > 0 and fund["debt"] is not None and 0 <= fund["debt"] <= 60
            and (fund["rev3"] or -99) >= 10 and (fund["eps_yoy"] or -99) >= 10)


def sig_div_pre(a, i):
    """不看財報的部分（先篩，省 FinMind 額度）"""
    if i < 240 or a["vol20"][i] is None or a["vol20"][i] < 300 or a["raw"][i] < 10:
        return False
    y, vp, hi = a["yld"][i], a["vol_pct"][i], a["hi240"][i]
    return bool(y and y >= 5 and vp is not None and vp <= 2.5 and hi and a["C"][i] / hi - 1 >= -0.10)


# ── 回測 ──────────────────────────────────────────────
def run(stocks, funds, master, bench):
    """stocks: {code: a}；funds: {code: (eps, debt, rev)}"""
    n = len(master)
    idx = {d: i for i, d in enumerate(master)}
    start_i = bisect.bisect_left(master, L.START)
    lv = {}
    elig = {}
    for code, a in stocks.items():
        arr, e = [None] * n, [False] * n
        for j, d in enumerate(a["d"]):
            i = idx.get(d)
            if i is not None:
                arr[i] = a["C"][j]
                e[i] = bool(a["vol20"][j] and a["vol20"][j] >= 300 and a["raw"][j] >= 10)
        lv[code], elig[code] = arr, e

    def fwd(code, i, h):
        x = lv[code]
        if i + 1 + h >= n or x[i + 1] is None or x[i + 1 + h] is None:
            return None
        return x[i + 1 + h] / x[i + 1] - 1

    mean = {h: [None] * n for h in HOLDS}
    for i in range(start_i, n - 1):
        live = [c for c in stocks if elig[c][i]]
        for h in HOLDS:
            xs = [r for r in (fwd(c, i, h) for c in live) if r is not None]
            mean[h][i] = sum(xs) / len(xs) if len(xs) >= 30 else None

    def stop8(code, i, days=20):
        x = lv[code]
        if i + 1 + days >= n or x[i + 1] is None:
            return None
        e = x[i + 1]
        for t in range(i + 2, i + 2 + days):
            if x[t] is not None and x[t] <= e * 0.92:
                return x[t] / e - 1
        return x[i + 1 + days] / e - 1 if x[i + 1 + days] else None

    events = {"tech": [], "div": [], "div_nofund": []}
    for code, a in stocks.items():
        last = {k: -99 for k in events}
        eps, debt, rev = funds.get(code, ([], [], []))
        for j in range(len(a["d"])):
            i = idx.get(a["d"][j])
            if i is None or i < start_i:
                continue
            if j - last["tech"] >= 10 and sig_tech(a, j):
                events["tech"].append((code, i))
                last["tech"] = j
            if j - last["div_nofund"] >= 20 and sig_div_pre(a, j):
                events["div_nofund"].append((code, i))
                last["div_nofund"] = j
                fd = fund_at(a["d"][j], eps, debt, rev) if code in funds else None
                if j - last["div"] >= 20 and sig_div(a, j, fd):
                    events["div"].append((code, i))
                    last["div"] = j

    def stats(evs):
        out = {"n": len(evs)}
        for h in HOLDS:
            rs, ex = [], []
            for code, i in evs:
                r = fwd(code, i, h)
                if r is None or mean[h][i] is None:
                    continue
                rs.append(r * 100 - COST)
                ex.append((master[i][:7], (r - mean[h][i]) * 100, master[i]))     # 超額不扣成本（基準也沒扣）
            if not rs:
                continue
            t, months = L.t_monthly([(m, x) for m, x, _ in ex])
            oos = [x for _, x, d in ex if d >= OOS]
            ins = [x for _, x, d in ex if d < OOS]
            b0 = []
            for _, i in evs:
                if i + 1 + h < n and bench.get(master[i + 1]) and bench.get(master[i + 1 + h]):
                    b0.append(bench[master[i + 1 + h]] / bench[master[i + 1]] - 1)
            out[str(h)] = {"n": len(rs), "win": round(sum(1 for r in rs if r > 0) / len(rs) * 100, 1),
                           "avg": round(sum(rs) / len(rs), 2), "med": round(sorted(rs)[len(rs) // 2], 2),
                           "worst": round(min(rs), 1), "loss10": round(sum(1 for r in rs if r <= -10) / len(rs) * 100, 1),
                           "ex": round(sum(x for _, x, _ in ex) / len(ex), 2), "t": t, "months": months,
                           "ex_in": round(sum(ins) / len(ins), 2) if ins else None, "n_in": len(ins),
                           "ex_oos": round(sum(oos) / len(oos), 2) if oos else None, "n_oos": len(oos),
                           "win_oos": round(sum(1 for x in oos if x > 0) / len(oos) * 100, 1) if oos else None,
                           "bench": round(sum(b0) / len(b0) * 100, 2) if b0 else None}
        s8 = [r for r in (stop8(c, i) for c, i in evs) if r is not None]
        if s8:
            out["stop8"] = {"avg": round(sum(s8) / len(s8) * 100 - COST, 2), "worst": round(min(s8) * 100 - COST, 1)}
        out["recent"] = sorted({(master[i], c) for c, i in evs if i >= n - 15}, reverse=True)[:15]
        return out

    base = []
    for i in range(start_i, n - 61, 5):
        base += [(c, i) for c in stocks if elig[c][i]]
    return {k: stats(v) for k, v in events.items()} | {"baseline": stats(base)}


def build():
    t0 = time.time()
    info = L.fm_get("TaiwanStockInfo", None, None)
    universe = C.pick_check_universe(info)
    names = dict(universe)
    bp = L.price_series(L.fm_get("TaiwanStockPrice", "0050"))
    bench = {d: v[0] for d, v in bp.items()}
    master = sorted(bench)
    stocks = {}
    for code, _ in universe:
        pr = L.fm_get("TaiwanStockPrice", code)
        if len(pr) < 300:
            continue
        stocks[code] = arrays(pr, L.fm_get("TaiwanStockPER", code))
    log(f"日K、本益比完成：{len(stocks)} 支（{(time.time() - t0) / 60:.0f} 分）")
    need = [c for c, a in stocks.items() if any(sig_div_pre(a, j) for j in range(len(a["d"])))]
    log(f"存股條件（殖利率、波動、近高點）曾經符合的 {len(need)} 支 → 補抓營收與財報")
    funds = {}
    for k, code in enumerate(need):
        eps = eps_series(L.fm_get("TaiwanStockFinancialStatements", code, "2016-01-01"))
        debt = debt_series(L.fm_get("TaiwanStockBalanceSheet", code, "2016-01-01"))
        rev = [(e[0], e[1]) for e in L.revenue_events(L.fm_get("TaiwanStockMonthRevenue", code, "2016-12-01"))]
        funds[code] = (eps, debt, rev)
        if (k + 1) % 20 == 0:
            log(f"   財報 {k + 1}/{len(need)}（{(time.time() - t0) / 60:.0f} 分）")
    cover = sum(1 for e, d, r in funds.values() if e and d and r)
    log(f"財報資料齊全 {cover}/{len(funds)} 支；開始回測")
    res = run(stocks, funds, master, bench)
    for k in ("tech", "div", "div_nofund"):
        res[k]["recent"] = [(d, c, names.get(c, c)) for d, c in res[k]["recent"]]
    res.update({"generated": (datetime.utcnow() + timedelta(hours=8)).strftime("%Y-%m-%d %H:%M"),
                "n_stocks": len(stocks), "period": [L.START, master[-1]], "oos": OOS})
    return res


def report(res):
    names = {"baseline": "基準：任一合格股票", "tech": "A 技術面：MACD 翻正＋布林開口＋站上揚月線",
             "div": "B 存股＋營運動能（全部條件）", "div_nofund": "B 只用殖利率≥5%＋低波動＋近一年高點（不看財報）"}
    log(f"──── {res['period'][0]} ~ {res['period'][1]}，{res['n_stocks']} 支；每筆扣 {COST}%；2025 年後＝作者沒用來調參的資料")
    for k in ("baseline", "tech", "div", "div_nofund"):
        r = res[k]
        log(f"══ {names[k]}：訊號 {r['n']} 次" + (f"｜停損8%(20天) 平均 {r['stop8']['avg']}% 最差 {r['stop8']['worst']}%" if r.get("stop8") else ""))
        for h in HOLDS:
            s = r.get(str(h))
            if not s:
                continue
            log(f"   抱 {h:>2} 天：n={s['n']} 勝率 {s['win']}% 平均 {s['avg']:+}% 中位 {s['med']:+}% 最差 {s['worst']}% 虧10%↑ {s['loss10']}%"
                f"｜超額 {s['ex']:+}% t={s['t']}｜2024前 {s['ex_in']}%(n={s['n_in']}) 2025後 {s['ex_oos']}%(n={s['n_oos']}, 勝 {s['win_oos']}%)｜同期0050 {s['bench']}%")
        if r.get("recent"):
            log(f"   最近訊號：{r['recent'][:8]}")
