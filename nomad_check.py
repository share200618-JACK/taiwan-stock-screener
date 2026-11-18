"""
遊牧民（《社畜的財務自由計畫》1、2 作者）短線方法 → 台股回測
==============================================================
把書與作者公開分享的規則，照原意寫成可回測的版本，和使用者在券商設的「高報酬、遊牧民」條件比較：

  0 日（爆量長紅）：漲幅 ≥ 5%、收紅、成交量 ≥ 前 20 日均量 4 倍（壓倒性爆量，不是 2 倍）、
                   成交金額 ≥ 3 億、收盤在 20 日線（生命線）與 200 日線之上、
                   之前是橫盤（前 20 日漲幅 −10%～+15%，不是已經漲一大段）
  買點（原版）    ：爆量後 1～2 日內，出現「量縮（< 0 日量 7 成）的小陰線」，收盤守住 0 日收盤（容許 −3%）、
                   在 5 日線之上、沒有回補 0 日的跳空 → 當天收盤買
  出場（原版）    ：停損設在 0 日長紅的最低點；收盤跌破 5 日線就賣；最多抱 10 天
  對照            ：爆量隔天直接追（使用者的做法）、抱 20 天；以及使用者券商條件搭配不同出場方式

報酬用還原價、扣來回成本 0.5%；並和同期間 0050 比較（超額）。
"""
import bisect
import time
from datetime import datetime, timedelta

import signal_lab as L
import strategy_check as C

log = L.log
_f = L._f
COST = 0.5          # 來回交易成本（%）：手續費＋證交稅
MAX_HOLD = 10


def build_arrays(price_rows):
    seq = C.ohlc_rows(price_rows)
    amt = {r["date"][:10]: _f(r.get("Trading_money")) / 1e8 for r in price_rows}
    n = len(seq)
    a = {"d": [x["d"] for x in seq], "ret": [x["ret"] for x in seq], "V": [x["v"] for x in seq],
         "A": [amt.get(x["d"], 0.0) for x in seq], "raw_c": [x["c"] for x in seq]}
    f = [x["lv"] / x["c"] if x["c"] else 1.0 for x in seq]          # 還原係數
    a["O"] = [x["o"] * k for x, k in zip(seq, f)]
    a["H"] = [x["h"] * k for x, k in zip(seq, f)]
    a["L"] = [x["l"] * k for x, k in zip(seq, f)]
    a["C"] = [x["lv"] for x in seq]
    a["ma5"] = L.rolling_mean(a["C"], 5)
    a["ma20"] = L.rolling_mean(a["C"], 20)
    a["ma200"] = L.rolling_mean(a["C"], 200)
    v20 = L.rolling_mean(a["V"], 20)
    a["v20p"] = [None] + v20[:-1]                                    # 前 20 日均量（不含當天）
    a["n"] = n
    return a, seq


def surge_days(a, cooldown=10):
    out, last = [], -99
    for j in range(201, a["n"] - 1):
        if j - last < cooldown:
            continue
        c, o, v = a["C"][j], a["O"][j], a["V"][j]
        if not (a["ret"][j] >= 0.05 and c > o and a["v20p"][j] and v >= 4 * a["v20p"][j] and a["A"][j] >= 3):
            continue
        if not (a["ma20"][j] and c > a["ma20"][j] and a["ma200"][j] and c > a["ma200"][j]):
            continue
        base = a["C"][j - 1] / a["C"][j - 21] - 1
        if not (-0.10 <= base <= 0.15):
            continue
        out.append(j)
        last = j
    return out


def pullback_entry(a, j):
    """爆量後 1～2 日內的量縮小陰線（原版買點）→ 進場日索引或 None"""
    for d in (j + 1, j + 2):
        if d >= a["n"]:
            return None
        if (a["C"][d] < a["O"][d] and a["V"][d] < 0.7 * a["V"][j] and a["C"][d] >= a["C"][j] * 0.97
                and a["ma5"][d] and a["C"][d] > a["ma5"][d] and a["L"][d] > a["C"][j - 1]):
            return d
    return None


def exit_nomad(a, e, stop):
    """停損＝0 日最低點（盤中觸價，跳空跌破就用開盤價）；收盤跌破 5 日線賣；最多 MAX_HOLD 天"""
    for t in range(e + 1, min(a["n"], e + 1 + MAX_HOLD)):
        if a["L"][t] <= stop:
            return t, min(a["O"][t], stop)
        if a["ma5"][t] and a["C"][t] < a["ma5"][t]:
            return t, a["C"][t]
    t = min(a["n"] - 1, e + MAX_HOLD)
    return (t, a["C"][t]) if t > e else (None, None)


def exit_hold(a, e, days=20):
    t = e + days
    return (t, a["C"][t]) if t < a["n"] else (None, None)


def exit_stop8(a, e, days=20):
    for t in range(e + 1, min(a["n"], e + 1 + days)):
        if a["C"][t] <= a["C"][e] * 0.92:
            return t, a["C"][t]
    return exit_hold(a, e, days)


def trade(a, code, e, exit_fn, bench, **kw):
    t, px = exit_fn(a, e, **kw)
    if t is None:
        return None
    r = (px / a["C"][e] - 1) * 100 - COST
    b0, b1 = bench.get(a["d"][e]), bench.get(a["d"][t])
    br = (b1 / b0 - 1) * 100 if b0 and b1 else 0.0
    return {"c": code, "d": a["d"][e], "x": a["d"][t], "days": t - e, "r": round(r, 2), "ex": round(r - br, 2)}


def summarize(trades, mid_date):
    if not trades:
        return {"n": 0}
    rs = sorted(t["r"] for t in trades)
    n = len(rs)
    t20, months = L.t_monthly([(t["d"][:7], t["ex"]) for t in trades])
    h = [[t["ex"] for t in trades if (t["d"] < mid_date) == first] for first in (True, False)]
    return {"n": n, "win": round(sum(1 for r in rs if r > 0) / n * 100, 1), "avg": round(sum(rs) / n, 2),
            "med": round(rs[n // 2], 2), "worst": round(rs[0], 1),
            "big_loss": round(sum(1 for r in rs if r <= -10) / n * 100, 1),
            "days": round(sum(t["days"] for t in trades) / n, 1),
            "ex": round(sum(t["ex"] for t in trades) / n, 2), "t": t20, "months": months,
            "halves": [round(sum(x) / len(x), 2) if x else None for x in h]}


def run(stocks, bench, inst_map, mid_date):
    """stocks: {code: (a, seq, f)}；bench: {日期: 0050 還原價}；inst_map: {code: {日期:(外資,投信)}}"""
    T = {k: [] for k in ("u_hold", "u_stop8", "u_nomad", "s_chase_hold", "s_chase_nomad", "p_hold", "p_nomad",
                         "p_nomad_inst", "p_nomad_noinst", "p_nomad_lim", "p_nomad_nolim", "p_nomad_amt10")}
    n_surge = n_pull = 0
    for code, (a, seq, f) in stocks.items():
        start = bisect.bisect_left(a["d"], L.START)
        # 使用者的券商條件（嚴格版）：隔天收盤買
        last = -99
        for j in range(max(start, 121), a["n"] - 1):
            if j - last < 10 or not C.base_signal(seq, f, j, C.VOL_STRICT):
                continue
            last = j
            e = j + 1
            for key, fn, kw in (("u_hold", exit_hold, {}), ("u_stop8", exit_stop8, {}),
                                ("u_nomad", exit_nomad, {"stop": a["L"][j]})):
                tr = trade(a, code, e, fn, bench, **kw)
                if tr:
                    T[key].append(tr)
        # 遊牧民爆量長紅
        im = inst_map.get(code, {})
        for j in surge_days(a):
            if j < start:
                continue
            n_surge += 1
            stop = a["L"][j]
            for key, fn, kw in (("s_chase_hold", exit_hold, {}), ("s_chase_nomad", exit_nomad, {"stop": stop})):
                tr = trade(a, code, j + 1, fn, bench, **kw)
                if tr:
                    T[key].append(tr)
            e = pullback_entry(a, j)
            if e is None:
                continue
            n_pull += 1
            th = trade(a, code, e, exit_hold, bench)
            tn = trade(a, code, e, exit_nomad, bench, stop=stop)
            if th:
                T["p_hold"].append(th)
            if not tn:
                continue
            T["p_nomad"].append(tn)
            prev = a["d"][max(0, j - 5):j]
            inst = sum(sum(im.get(d, (0, 0))) for d in prev)
            T["p_nomad_inst" if inst > 0 else "p_nomad_noinst"].append(tn)
            T["p_nomad_lim" if a["ret"][j] >= 0.095 else "p_nomad_nolim"].append(tn)
            if a["A"][j] >= 10:
                T["p_nomad_amt10"].append(tn)
    names = {
        "u_hold": "你的券商條件：隔天買、抱 20 天",
        "u_stop8": "你的券商條件：隔天買、跌 8% 停損（最多 20 天）",
        "u_nomad": "你的券商條件＋遊牧民出場（停損訊號日低點、破 5 日線賣）",
        "s_chase_hold": "遊牧民爆量長紅：隔天追買、抱 20 天",
        "s_chase_nomad": "遊牧民爆量長紅：隔天追買＋遊牧民出場",
        "p_hold": "遊牧民原版買點（量縮小陰線）、抱 20 天",
        "p_nomad": "遊牧民原版：量縮小陰線買＋停損 0 日低點＋破 5 日線賣",
        "p_nomad_inst": "　原版＋爆量前 5 日法人買超",
        "p_nomad_noinst": "　原版＋爆量前 5 日法人沒買",
        "p_nomad_lim": "　原版、0 日是漲停",
        "p_nomad_nolim": "　原版、0 日不是漲停",
        "p_nomad_amt10": "　原版、0 日成交金額 ≥ 10 億（大哥股）",
    }
    return {"rows": [{"key": k, "name": names[k], **summarize(v, mid_date)} for k, v in T.items()],
            "n_surge": n_surge, "n_pullback": n_pull,
            "trades_sample": {k: v[-30:] for k, v in T.items() if k in ("p_nomad", "u_hold")}}


def build():
    t0 = time.time()
    info = L.fm_get("TaiwanStockInfo", None, None)
    universe = C.pick_check_universe(info)
    bp = L.price_series(L.fm_get("TaiwanStockPrice", "0050"))
    bench = {d: v[0] for d, v in bp.items()}
    master = sorted(bench)
    start_i = bisect.bisect_left(master, L.START)
    mid_date = master[start_i + (len(master) - start_i) // 2]
    stocks, inst_map = {}, {}
    for k, (code, name) in enumerate(universe):
        pr = L.fm_get("TaiwanStockPrice", code)
        if len(pr) < 300:
            continue
        ins = L.fm_get("TaiwanStockInstitutionalInvestorsBuySell", code)
        a, seq = build_arrays(pr)
        im = L.inst_series(ins)
        f = C.features(seq, im, {}, [])
        stocks[code] = (a, seq, f)
        inst_map[code] = im
        if (k + 1) % 25 == 0:
            log(f"   已抓 {k + 1}/{len(universe)} 支（{(time.time() - t0) / 60:.0f} 分）")
    log(f"資料完成：{len(stocks)} 支，開始回測")
    res = run(stocks, bench, inst_map, mid_date)
    res.update({"generated": (datetime.utcnow() + timedelta(hours=8)).strftime("%Y-%m-%d %H:%M"),
                "n_stocks": len(stocks), "period": [master[start_i], master[-1]], "split": mid_date})
    return res


def report(res):
    log(f"──── {res['period'][0]} ~ {res['period'][1]}，{res['n_stocks']} 支；爆量長紅 {res['n_surge']} 次、"
        f"其中出現量縮小陰線買點 {res['n_pullback']} 次；前後半以 {res['split']} 為界；報酬已扣成本 {COST}%")
    for r in res["rows"]:
        if not r.get("n"):
            log(f"   {r['name']}：無樣本")
            continue
        log(f"   {r['name']:<34} n={r['n']:<5} 勝率 {r['win']}% 平均 {r['avg']:+}% 中位 {r['med']:+}% 平均抱 {r['days']} 天"
            f"｜最差 {r['worst']}% 虧10%以上 {r['big_loss']}%｜比0050超額 {r['ex']:+}% t={r['t']} 前後半 {r['halves']}")
