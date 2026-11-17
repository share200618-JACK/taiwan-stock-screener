"""
選股條件體檢：券商「高報酬、遊牧民」條件 → 哪裡容易賠、加什麼條件能避開
=======================================================================
使用者在券商 App 用的條件（訊號日 t，收盤後判斷，隔天買進）：
  近 20 日平均成交量 > 2000 張、今日量 > 前 5 日均量 2 倍、
  KD(9) 黃金交叉（今日 K > D、昨日 K ≤ D）且 20 < K < 50、收盤 > 60 日均線、
  收盤突破上關價（昨低 ＋（昨高 − 昨低）× 1.382）

兩種模式（GitHub Actions 手動執行，結果存 Supabase analysis_store）
  cases    ：逐筆檢查使用者實際買進的股票（訊號當天的價量、籌碼、營收、本益比，買進後的走勢）
  backtest ：約 400 支隨機抽樣上市櫃股票、2019 年起，回測原始條件與各種「過濾條件」的勝率、超額報酬、
             大跌比例；並比較停損規則。統計方法沿用個股勝率實驗室（扣同期平均、逐月 t、前後半）。
"""
import os
import sys
import json
import time
import bisect
from datetime import datetime, timedelta

import signal_lab as L

try:
    import notify
except ImportError:
    notify = None

log = L.log
_f = L._f
SUPABASE_URL, SUPABASE_KEY = L.SUPABASE_URL, L.SUPABASE_KEY

CASES = [  # (代號, 名稱, 買進日, 買進價)
    ("6133", "金橋", "2026-10-22", 26.05),
    ("2355", "敬鵬", "2026-04-22", 55.2),
    ("2355", "敬鵬", "2026-05-14", 54.4),
    ("4720", "德淵", "2026-07-06", 26.05),
    ("4720", "德淵", "2026-07-14", 21.95),
    ("2355", "敬鵬", "2026-07-03", 53.4),
    ("6189", "豐藝", "2026-07-02", 52.3),
    ("5381", "光譜", "2026-07-09", 27.35),
    ("1410", "南染", "2026-07-06", 26.1),
]
def _cases_from_env():
    """CHECK_CASES＝「代號,名稱,買進日,價格;…」（手動執行時填），沒填就用上面的清單"""
    raw = os.environ.get("CHECK_CASES", "").strip()
    out = []
    for part in raw.replace("；", ";").split(";"):
        x = [t.strip() for t in part.replace("，", ",").split(",")]
        if len(x) == 4:
            try:
                out.append((x[0], x[1], x[2], float(x[3])))
            except ValueError:
                pass
    return out or CASES


N_EXTRA = int(os.environ.get("CHECK_EXTRA_N", "180"))     # 在實驗室 200 支之外再抽幾支
HORIZONS = [5, 20, 60]
VOL_STRICT, VOL_LOOSE = 2000, 500


# ── 個股資料（含開高低，指標用原始價、報酬用還原價）──────────────
def ohlc_rows(rows):
    """→ 依日期排序的 [{d,o,h,l,c,v(張),lv(還原價水準),ret(當日漲跌幅)}]"""
    rows = sorted((r for r in rows if _f(r.get("close")) > 0), key=lambda r: r["date"])
    out, lv, prev = [], 100.0, None
    for r in rows:
        c = _f(r["close"])
        ret = 0.0
        if prev is not None:
            sp = r.get("spread")
            ref = c - _f(sp) if sp not in (None, "") else None
            ret = c / ref - 1 if ref and ref > 0 and 0.5 < c / ref < 1.5 else c / prev - 1
            if abs(ret) > 0.3:
                ret = 0.0
            lv *= 1 + ret
        out.append({"d": r["date"][:10], "o": _f(r.get("open")) or c, "h": _f(r.get("max")) or c,
                    "l": _f(r.get("min")) or c, "c": c, "v": _f(r.get("Trading_Volume")) / 1000,
                    "lv": lv, "ret": ret})
        prev = c
    return out


def features(seq, inst, margin, rev_events, per=None):
    """
    seq：ohlc_rows 結果；inst {日期:(外資, 投信)}；margin {日期: 融資餘額}；rev_events 見 signal_lab.revenue_events
    → 每天一個 dict（訊號判斷與過濾用的特徵）；法人、融資用前一個交易日（收盤後才公布）
    """
    n = len(seq)
    c = [x["c"] for x in seq]
    v = [x["v"] for x in seq]
    k = d = 50.0
    out = [None] * n
    rev_dates = [e[0] for e in rev_events]
    for j in range(n):
        lo9 = min(x["l"] for x in seq[max(0, j - 8):j + 1])
        hi9 = max(x["h"] for x in seq[max(0, j - 8):j + 1])
        rsv = 50.0 if hi9 == lo9 else (c[j] - lo9) / (hi9 - lo9) * 100
        pk, pd = k, d
        k = k * 2 / 3 + rsv / 3
        d = d * 2 / 3 + k / 3
        if j < 120:
            continue
        vol20 = sum(v[j - 19:j + 1]) / 20
        vol5p = sum(v[j - 5:j]) / 5
        vol120 = sum(v[j - 119:j + 1]) / 120
        ma60 = sum(c[j - 59:j + 1]) / 60
        upper = seq[j - 1]["l"] + 1.382 * (seq[j - 1]["h"] - seq[j - 1]["l"])
        prev_days = [seq[t]["d"] for t in range(j - 5, j)]
        f5 = sum(inst.get(t, (0, 0))[0] for t in prev_days)
        t5 = sum(inst.get(t, (0, 0))[1] for t in prev_days)
        m1, m6 = margin.get(seq[j - 1]["d"]), margin.get(seq[j - 6]["d"])
        ri = bisect.bisect_right(rev_dates, seq[j]["d"]) - 1
        yoy = rev_events[ri][1] if ri >= 0 else None
        yoy3 = ([e[1] for e in rev_events[max(0, ri - 2):ri + 1]] if ri >= 2 else None)
        out[j] = {
            "k": k, "d": d, "pk": pk, "pd": pd, "vol20": vol20, "vol5p": vol5p, "vol120": vol120,
            "ma60": ma60, "upper": upper, "ret1": seq[j]["ret"],
            "r20": seq[j]["lv"] / seq[j - 20]["lv"] - 1, "r60": seq[j]["lv"] / seq[j - 60]["lv"] - 1,
            "inst5": f5 + t5, "trust5": t5, "for5": f5,
            "mchg5": (m1 / m6 - 1) if m1 and m6 else None,
            "yoy": yoy, "yoy3": sum(yoy3) / 3 if yoy3 else None,
            "rev_hot": bool(ri >= 0 and rev_events[ri][2] and (yoy or 0) >= 20),
            "per": (per or {}).get(seq[j]["d"]),
        }
    return out


def base_signal(seq, f, j, min_vol):
    x = f[j]
    if x is None:
        return False
    return (x["vol20"] > min_vol and seq[j]["v"] > 2 * x["vol5p"] and x["k"] > x["d"] and x["pk"] <= x["pd"]
            and 20 < x["k"] < 50 and seq[j]["c"] > x["ma60"] and seq[j]["c"] > x["upper"])


def conditions(seq, f, j):
    """逐項列出原始條件是否成立（給案例檢查用）"""
    x = f[j]
    return {"20日均量>2000張": x["vol20"] > VOL_STRICT, "量>前5日均量2倍": seq[j]["v"] > 2 * x["vol5p"],
            "KD黃金交叉": x["k"] > x["d"] and x["pk"] <= x["pd"], "20<K<50": 20 < x["k"] < 50,
            "收盤>60日線": seq[j]["c"] > x["ma60"], "突破上關價": seq[j]["c"] > x["upper"]}


FILTERS = {
    "rev_pos":   ("最新月營收年增 > 0", lambda x: x["yoy"] is not None and x["yoy"] > 0),
    "rev3_pos":  ("近 3 個月營收平均年增 > 0", lambda x: x["yoy3"] is not None and x["yoy3"] > 0),
    "rev_hot":   ("月營收創 12 個月新高且年增 ≥ 20%", lambda x: x["rev_hot"]),
    "no_limit":  ("訊號日不是漲停（漲幅 < 9.5%）", lambda x: x["ret1"] < 0.095),
    "rev3_10":   ("近 3 個月營收平均年增 ≥ 10%", lambda x: x["yoy3"] is not None and x["yoy3"] >= 10),
    "not_ext":   ("訊號前 20 日漲幅 < 25%（不追急漲）", lambda x: x["r20"] < 0.25),
    "not_ext60": ("訊號前 60 日漲幅 < 30%（不買在波段末端）", lambda x: x["r60"] < 0.30),
    "pe_ok":     ("本益比 0~25 倍（有獲利、不過貴）", lambda x: x["per"] is not None and 0 < x["per"] <= 25),
    "inst_buy":  ("外資＋投信前 5 日合計買超", lambda x: x["inst5"] > 0),
    "margin_ok": ("融資前 5 日增加 < 5%（散戶沒有追進）", lambda x: x["mchg5"] is None or x["mchg5"] < 0.05),
    "normal_liq": ("平常就有量（20日均量 < 120日均量 3 倍）", lambda x: x["vol20"] < 3 * x["vol120"]),
    "combo":     ("組合：近3月營收年增＋法人買超＋不是漲停", lambda x: (x["yoy3"] or -1) > 0 and x["inst5"] > 0
                  and x["ret1"] < 0.095),
    "combo2":    ("組合：近3月營收年增≥10%＋60日漲幅<30%＋本益比≤25", lambda x: (x["yoy3"] or -1) >= 10
                  and x["r60"] < 0.30 and x["per"] is not None and 0 < x["per"] <= 25),
}


# ── 案例檢查 ──────────────────────────────────────────
def load_stock(code, start="2025-01-01", extra=False):
    pr = L.fm_get("TaiwanStockPrice", code, start)
    ins = L.fm_get("TaiwanStockInstitutionalInvestorsBuySell", code, start)
    mg = L.fm_get("TaiwanStockMarginPurchaseShortSale", code, start)
    rv = L.fm_get("TaiwanStockMonthRevenue", code, str(int(start[:4]) - 2) + start[4:])
    out = {"seq": ohlc_rows(pr), "inst": L.inst_series(ins), "margin": L.margin_series(mg),
           "rev": L.revenue_events(rv), "mg_rows": mg, "ins_rows": ins}
    if extra:
        out["per"] = {r["date"][:10]: (_f(r.get("PER")), _f(r.get("PBR"))) for r in L.fm_get("TaiwanStockPER", code, start)}
        out["hold"] = {r["date"][:10]: _f(r.get("ForeignInvestmentSharesRatio"))
                       for r in L.fm_get("TaiwanStockShareholding", code, start)}
        out["eps"] = sorted((r["date"][:10], _f(r.get("value"))) for r in
                            L.fm_get("TaiwanStockFinancialStatements", code, "2024-01-01") if r.get("type") == "EPS")
    return out


def _near(dct, day, back=True):
    ks = sorted(dct)
    i = bisect.bisect_right(ks, day) - 1 if back else bisect.bisect_left(ks, day)
    return dct[ks[i]] if 0 <= i < len(ks) else None


def case_report(code, name, buy_date, buy_px, S):
    seq = S["seq"]
    f = features(seq, S["inst"], S["margin"], S["rev"], {d: v[0] for d, v in S.get("per", {}).items()})
    dates = [x["d"] for x in seq]
    b = bisect.bisect_left(dates, buy_date)
    if b >= len(dates) or b < 125:
        return {"code": code, "name": name, "buy_date": buy_date, "error": "資料不足"}
    j = b - 1                     # 訊號日＝買進前一個交易日
    x, s = f[j], seq[j]
    # 法人、融資：訊號前 20 日、買進後 20 日
    def inst_sum(a, z):
        fo = sum(S["inst"].get(dates[t], (0, 0))[0] for t in range(max(0, a), min(len(dates), z)))
        tr = sum(S["inst"].get(dates[t], (0, 0))[1] for t in range(max(0, a), min(len(dates), z)))
        return round(fo), round(tr)
    def marg(t):
        return S["margin"].get(dates[min(max(t, 0), len(dates) - 1)])
    lim = {r["date"][:10]: _f(r.get("MarginPurchaseLimit")) for r in S["mg_rows"]}
    m_sig = marg(j)
    # 買進後走勢（原始價，和使用者的成本比較）
    after = seq[b:]
    low = min(after, key=lambda r: r["l"]) if after else None
    rec = next((r["d"] for r in after if r["c"] >= buy_px), None)
    rec_days = (dates.index(rec) - b) if rec else None
    pe = _near(S.get("per", {}), s["d"])
    hold = S.get("hold", {})
    h0, h1 = _near(hold, dates[max(0, j - 20)]), _near(hold, s["d"])
    h2 = _near(hold, dates[min(len(dates) - 1, b + 20)])
    eps = [(dt, v) for dt, v in S.get("eps", []) if dt <= s["d"]][-5:]
    revs = [e for e in S["rev"] if e[0] <= s["d"]][-4:]
    rep = {
        "code": code, "name": name, "buy_date": buy_date, "buy_px": buy_px, "signal_date": s["d"],
        "signal": {"close": s["c"], "chg": round(s["ret"] * 100, 2), "vol": round(s["v"]), "vol20": round(x["vol20"]),
                   "vol120": round(x["vol120"]), "k": round(x["k"], 1), "d": round(x["d"], 1),
                   "ma60": round(x["ma60"], 2), "upper": round(x["upper"], 2),
                   "r20": round(x["r20"] * 100, 1), "r60": round(x["r60"] * 100, 1),
                   "ma60_dist": round((s["c"] / x["ma60"] - 1) * 100, 1)},
        "conditions": conditions(seq, f, j),
        "filters": {k: bool(fn(x)) for k, (_, fn) in FILTERS.items()},
        "inst_before20": inst_sum(j - 19, j + 1), "inst_after20": inst_sum(b, b + 20),
        "margin": {"before20": marg(j - 20), "signal": m_sig, "after20": marg(b + 20),
                   "usage": round(m_sig / lim[s["d"]] * 100, 1) if m_sig and lim.get(s["d"]) else None},
        "foreign_hold": [h0, h1, h2],
        "per": pe, "eps": eps,
        "revenue": [{"avail": e[0], "yoy": e[1], "high": e[2]} for e in revs],
        "revenue_after": [{"avail": e[0], "yoy": e[1], "high": e[2]} for e in S["rev"] if e[0] > s["d"]][:5],
        "after": {"low": low["l"] if low else None, "low_date": low["d"] if low else None,
                  "max_dd": round((low["l"] / buy_px - 1) * 100, 1) if low else None,
                  "recover_date": rec, "recover_days": rec_days,
                  "last": seq[-1]["c"], "last_date": seq[-1]["d"],
                  "r20": round((seq[min(len(seq) - 1, b + 20)]["c"] / buy_px - 1) * 100, 1),
                  "r60": round((seq[min(len(seq) - 1, b + 60)]["c"] / buy_px - 1) * 100, 1)},
        "next_open": seq[b]["o"], "next_close": seq[b]["c"],
    }
    return rep


def run_cases():
    cache, out = {}, []
    for code, name, bd, px in _cases_from_env():
        if code not in cache:
            cache[code] = load_stock(code, extra=True)
        rep = case_report(code, name, bd, px, cache[code])
        out.append(rep)
        if rep.get("error"):
            log(f"{code} {name} {bd}：{rep['error']}")
            continue
        g, c = rep["signal"], rep["conditions"]
        log(f"════ {code} {name}  買進 {bd} @ {px}（訊號日 {rep['signal_date']}，隔天開 {rep['next_open']} 收 {rep['next_close']}）")
        log(f"   訊號日：收 {g['close']}（{g['chg']:+}%）量 {g['vol']} 張｜20日均量 {g['vol20']}、120日均量 {g['vol120']}｜"
            f"K {g['k']} D {g['d']}｜60日線 {g['ma60']}（距 {g['ma60_dist']:+}%）上關價 {g['upper']}")
        log(f"   訊號前漲幅：20日 {g['r20']:+}%、60日 {g['r60']:+}%")
        log(f"   原始條件：" + "、".join(f"{k}{'✓' if v else '✗'}" for k, v in c.items()))
        log(f"   過濾條件：" + "、".join(f"{FILTERS[k][0]}{'✓' if v else '✗'}" for k, v in rep["filters"].items()))
        log(f"   法人（外資,投信 張）：訊號前20日 {rep['inst_before20']}｜買進後20日 {rep['inst_after20']}")
        m = rep["margin"]
        log(f"   融資餘額：訊號前20日 {m['before20']} → 訊號日 {m['signal']}（使用率 {m['usage']}%）→ 買進後20日 {m['after20']}")
        log(f"   外資持股%：訊號前20日 {rep['foreign_hold'][0]} → 訊號日 {rep['foreign_hold'][1]} → 買進後20日 {rep['foreign_hold'][2]}")
        log(f"   本益比/淨值比：{rep['per']}｜近期 EPS：{rep['eps']}")
        log(f"   營收（可取得日, 年增%, 創12月新高）：{[(r['avail'], r['yoy'], r['high']) for r in rep['revenue']]}")
        log(f"   買進後公布的營收：{[(r['avail'], r['yoy'], r['high']) for r in rep['revenue_after']]}")
        a = rep["after"]
        log(f"   買進後：最低 {a['low']}（{a['low_date']}，{a['max_dd']}%）｜20日 {a['r20']:+}%、60日 {a['r60']:+}%｜"
            f"回本 {a['recover_date'] or '尚未'}（{a['recover_days']} 個交易日）｜最新 {a['last']}（{a['last_date']}）")
    return {"cases": out, "generated": (datetime.utcnow() + timedelta(hours=8)).strftime("%Y-%m-%d %H:%M")}


# ── 回測 ──────────────────────────────────────────────
def pick_check_universe(info):
    base = L.pick_universe(info)                       # 實驗室同一批 200 支
    taken = {c for c, _ in base}
    rest = [r for r in info if str(r.get("stock_id")) not in taken]
    extra = L.pick_universe(rest, n=N_EXTRA, seed="strategy-check-v1")
    return base + extra


def run_backtest(stocks, master, names):
    """
    stocks: {code: {"seq","f"}}；master：0050 交易日
    回傳原始條件、寬鬆版（20日均量 > 500 張）與各過濾條件的統計
    """
    n = len(master)
    start_i = bisect.bisect_left(master, L.START)
    idx = {d: i for i, d in enumerate(master)}
    # 對齊還原價（報酬）、合格（同期平均用）
    lv = {}
    elig = {}
    for code, S in stocks.items():
        a = [None] * n
        e = [False] * n
        for j, r in enumerate(S["seq"]):
            i = idx.get(r["d"])
            if i is None:
                continue
            a[i] = r["lv"]
            x = S["f"][j]
            e[i] = bool(x and x["vol20"] >= 300 and r["c"] >= 10)
        lv[code], elig[code] = a, e

    def fwd(code, i, h):
        a = lv[code]
        if i + 1 + h >= n or a[i + 1] is None or a[i + 1 + h] is None:
            return None
        return a[i + 1 + h] / a[i + 1] - 1

    mean = {h: [None] * n for h in HORIZONS}
    for i in range(start_i, n):
        live = [c for c in stocks if elig[c][i]]
        for h in HORIZONS:
            xs = [r for r in (fwd(c, i, h) for c in live) if r is not None]
            mean[h][i] = sum(xs) / len(xs) if len(xs) >= 30 else None

    def path_stats(code, i, entry_open=None):
        """買進後 60 日內最大跌幅、是否跌破 −15%、停損 −8% 的結果（20 日）"""
        a = lv[code]
        if i + 61 >= n or a[i + 1] is None:
            return None
        e = a[i + 1]
        path = [a[t] / e - 1 for t in range(i + 2, i + 62) if a[t] is not None]
        if not path:
            return None
        hold20 = path[19] if len(path) >= 20 else path[-1]
        stop = next((p for p in path[:20] if p <= -0.08), hold20)
        return {"mdd60": min(path), "stop8": stop, "hold20": hold20}

    events = []        # (code, i, 寬鬆/嚴格, 特徵)
    for code, S in stocks.items():
        seq, f = S["seq"], S["f"]
        last = -99
        for j in range(len(seq)):
            if not base_signal(seq, f, j, VOL_LOOSE):
                continue
            i = idx.get(seq[j]["d"])
            if i is None or i < start_i or j - last < 10:
                continue
            last = j
            events.append((code, i, f[j]["vol20"] > VOL_STRICT, f[j]))

    def stats(evs, label):
        obs = {h: [] for h in HORIZONS}
        absr = {h: [] for h in HORIZONS}
        paths = []
        for code, i, _, _ in evs:
            for h in HORIZONS:
                r = fwd(code, i, h)
                if r is not None and mean[h][i] is not None:
                    obs[h].append((master[i][:7], (r - mean[h][i]) * 100, i))
                    absr[h].append(r * 100)
            p = path_stats(code, i)
            if p:
                paths.append(p)
        mid = start_i + (n - start_i) // 2
        st = {}
        for h in HORIZONS:
            xs = absr[h]
            ex = [x for _, x, _ in obs[h]]
            st[str(h)] = {"n": len(xs),
                          "win": round(sum(1 for x in xs if x > 0) / len(xs) * 100, 1) if xs else None,
                          "avg": round(sum(xs) / len(xs), 2) if xs else None,
                          "med": round(sorted(xs)[len(xs) // 2], 2) if xs else None,
                          "ex_avg": round(sum(ex) / len(ex), 2) if ex else None,
                          "ex_win": round(sum(1 for x in ex if x > 0) / len(ex) * 100, 1) if ex else None}
        t20, months = L.t_monthly([(m, x) for m, x, _ in obs[20]])
        halves = []
        for first in (True, False):
            ex = [x for _, x, i in obs[20] if (i < mid) == first]
            halves.append({"n": len(ex), "ex_avg": round(sum(ex) / len(ex), 2) if ex else None})
        crash = round(sum(1 for p in paths if p["mdd60"] <= -0.15) / len(paths) * 100, 1) if paths else None
        stop = {"hold20": round(sum(p["hold20"] for p in paths) / len(paths) * 100, 2) if paths else None,
                "stop8": round(sum(p["stop8"] for p in paths) / len(paths) * 100, 2) if paths else None,
                "hold20_worst": round(min(p["hold20"] for p in paths) * 100, 1) if paths else None,
                "stop8_worst": round(min(p["stop8"] for p in paths) * 100, 1) if paths else None}
        return {"label": label, "stats": st, "t20": t20, "months": months, "halves": halves,
                "crash15": crash, "exit": stop}

    out = {"strict": [], "loose": []}
    for key, evs in (("strict", [e for e in events if e[2]]), ("loose", events)):
        out[key].append({"key": "base", **stats(evs, "原始條件")})
        for fk, (fname, fn) in FILTERS.items():
            keep = [e for e in evs if fn(e[3])]
            drop = [e for e in evs if not fn(e[3])]
            row = {"key": fk, **stats(keep, "＋" + fname)}
            row["dropped"] = stats(drop, "被過濾掉的")
            out[key].append(row)
    # 基準：所有合格股票（每 5 天抽一次），嚴格版只看 20 日均量 > 2000 張
    base_ev = {"strict": [], "loose": []}
    for code, S in stocks.items():
        for j in range(0, len(S["seq"]), 5):
            x = S["f"][j]
            i = idx.get(S["seq"][j]["d"])
            if x is None or i is None or i < start_i or not elig[code][i]:
                continue
            if x["vol20"] > VOL_LOOSE:
                base_ev["loose"].append((code, i, False, x))
            if x["vol20"] > VOL_STRICT:
                base_ev["strict"].append((code, i, True, x))
    out["baseline"] = {k: stats(v, "任一股票（同樣成交量門檻）") for k, v in base_ev.items()}
    out["recent"] = sorted(({"date": master[i], "code": c, "name": names.get(c, c), "strict": s}
                            for c, i, s, _ in events if i >= n - 30), key=lambda r: r["date"], reverse=True)
    out["period"] = [master[start_i], master[-1]]
    out["n_events"] = {"strict": sum(1 for e in events if e[2]), "loose": len(events)}
    return out


def build_backtest():
    t0 = time.time()
    info = L.fm_get("TaiwanStockInfo", None, None)
    universe = pick_check_universe(info)
    names = dict(universe)
    log(f"抽樣 {len(universe)} 支（實驗室 200 支＋另外 {N_EXTRA} 支，固定種子）")
    bp = L.price_series(L.fm_get("TaiwanStockPrice", "0050"))
    master = sorted(bp)
    stocks = {}
    for k, (code, name) in enumerate(universe):
        pr = L.fm_get("TaiwanStockPrice", code)
        if len(pr) < 300:
            continue
        ins = L.fm_get("TaiwanStockInstitutionalInvestorsBuySell", code)
        mg = L.fm_get("TaiwanStockMarginPurchaseShortSale", code)
        rv = L.fm_get("TaiwanStockMonthRevenue", code, "2016-12-01")
        pe = {r["date"][:10]: _f(r.get("PER")) for r in L.fm_get("TaiwanStockPER", code)}
        seq = ohlc_rows(pr)
        stocks[code] = {"seq": seq, "f": features(seq, L.inst_series(ins), L.margin_series(mg), L.revenue_events(rv), pe)}
        if (k + 1) % 25 == 0:
            log(f"   已抓 {k + 1}/{len(universe)} 支（{(time.time() - t0) / 60:.0f} 分）")
    log(f"資料完成：{len(stocks)} 支，開始回測")
    res = run_backtest(stocks, master, names)
    res.update({"generated": (datetime.utcnow() + timedelta(hours=8)).strftime("%Y-%m-%d %H:%M"),
                "n_stocks": len(stocks)})
    return res


def report_backtest(res):
    log(f"──── 期間 {res['period'][0]} ~ {res['period'][1]}，{res['n_stocks']} 支；訊號 嚴格 {res['n_events']['strict']} 次、寬鬆 {res['n_events']['loose']} 次")
    for key, title in (("strict", "原始條件（20日均量 > 2000 張）"), ("loose", "寬鬆版（20日均量 > 500 張，樣本較多）")):
        b = res["baseline"][key]["stats"]
        log(f"══ {title}｜基準：20日勝率 {b['20']['win']}% 平均 {b['20']['avg']}%、60日勝率 {b['60']['win']}% 平均 {b['60']['avg']}%")
        for r in res[key]:
            a, c = r["stats"]["20"], r["stats"]["60"]
            log(f"   {r['label']:<32} n={a['n']:<4} 20日 勝率 {a['win']}% 平均 {a['avg']}% 超額 {a['ex_avg']}%｜60日 勝率 {c['win']}% 平均 {c['avg']}%"
                f"｜60日內跌 15% 以上 {r['crash15']}%｜t={r['t20']} 前後半超額 {[h['ex_avg'] for h in r['halves']]}"
                f"｜持有20日 {r['exit']['hold20']}%（最差 {r['exit']['hold20_worst']}%）vs 停損8% {r['exit']['stop8']}%（最差 {r['exit']['stop8_worst']}%）")
            if "dropped" in r:
                d = r["dropped"]["stats"]["20"]
                log(f"      被過濾掉的 n={d['n']} 20日勝率 {d['win']}% 平均 {d['avg']}%｜60日內跌 15% 以上 {r['dropped']['crash15']}%")


def save(key, payload):
    if not SUPABASE_URL or not SUPABASE_KEY:
        return False
    import requests
    h = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}",
         "Content-Type": "application/json", "Prefer": "return=minimal"}
    url = f"{SUPABASE_URL}/rest/v1/analysis_store"
    today = (datetime.utcnow() + timedelta(hours=8)).strftime("%Y-%m-%d")
    try:
        requests.delete(url, params={"key": f"eq.{key}", "date": f"eq.{today}"}, headers=h, timeout=15)
        r = requests.post(url, json={"key": key, "date": today, "payload": payload}, headers=h, timeout=60)
        log(f"{'✅ 已存入' if r.status_code in (200, 201) else '❌ 存入失敗 ' + str(r.status_code)} analysis_store（{key}）")
        return r.status_code in (200, 201)
    except Exception as e:
        log(f"❌ 存入例外: {e}")
        return False


def main():
    mode = (sys.argv[1] if len(sys.argv) > 1 else os.environ.get("CHECK_MODE", "cases")).strip()
    try:
        if mode == "cases":
            res = run_cases()
            key = "strategy_cases"
        else:
            res = build_backtest()
            report_backtest(res)
            key = "strategy_check"
    except L.TokenBad as e:
        log(f"❌ FinMind token 無效：{e}")
        sys.exit(1)
    with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), f"{key}_result.json"), "w", encoding="utf-8") as f:
        json.dump(res, f, ensure_ascii=False, default=str)
    save(key, res)


if __name__ == "__main__":
    main()
