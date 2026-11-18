"""
遊牧民（《社畜的財務自由計畫》1、2 作者）短線方法 → 台股回測（第 2 版）
=====================================================================
從書摘、讀書筆記與作者公開分享整理出、可以量化的規則，逐一在台股驗證：

【書 1 第三章：成交量與均線】
  爆量：成交量比前一日多 500%～1000%（≥ 5 倍）是暴漲前兆
  量縮：成交量降到前一日 25% 以下、或「量銳減＋陰線」且離 5 日線不遠，隔天上漲機率較高
  20 日線＝生命線；45 日線第一次碰到、沒跌破 60 日線 → 反彈機率高
【書 1 第四章：買進前三原則】前一天量有沒有縮？縮了是不是陰線？站在短均線上？
【作者操作】前一天選股、收盤前買，隔天中午前賣；賣不掉就 1～5 天內出場
【停損／出場】一天跌超過 5% 減半；虧損時隔天開低就清倉；跌破 5 日線賣；停損設在爆量長紅最低點
【其他】消息股離底部已漲 30% 以上不買；體質差（虧損）避開；盤勢不好時休息

策略
  user          ：使用者券商「高報酬、遊牧民」條件，隔天買
  surge_chase   ：書的爆量長紅（量 ≥ 前日 5 倍、漲 ≥ 5%、站上 20 日線、成交金額 ≥ 3 億），隔天直接買
  pullback      ：爆量後 1～2 天的量縮（≤ 0 日量一半）小陰線、守住 0 日收盤、在 5 日線上、沒回補 → 收盤買
  dryup25/50    ：爆量後 5 天內，量 ≤ 前一日 25%（50%）的陰線、離 5 日線 3% 內、在 20 日線上 → 收盤買
  ma45          ：45 日線第一次被碰到、收盤沒破 60 日線、45 日線往上 → 收盤買（波段）
出場
  next_open / next_close：隔天開盤／收盤賣（作者的基本循環）
  book  ：最多 5 天；虧損中隔天開低 → 開盤賣；單日跌 ≥ 5% → 賣；收盤破 5 日線 → 賣；觸及爆量長紅低點 → 停損
  hold20：抱 20 天（使用者的習慣）
  swing ：ma45 用：碰到前波高點（壓力）停利、收盤破 60 日線停損，最多 20 天
報酬用還原價、每筆扣來回成本 0.5%；和同期間 0050 比（超額）。
"""
import bisect
import time
from datetime import datetime, timedelta

import signal_lab as L
import strategy_check as C

log = L.log
_f = L._f
COST = 0.5
BOOK_HOLD = 5


def build_arrays(price_rows, per_rows=None, inst=None):
    seq = C.ohlc_rows(price_rows)
    amt = {r["date"][:10]: _f(r.get("Trading_money")) / 1e8 for r in price_rows}
    per = {r["date"][:10]: _f(r.get("PER")) for r in (per_rows or [])}
    inst = inst or {}
    a = {"d": [x["d"] for x in seq], "ret": [x["ret"] for x in seq], "V": [x["v"] for x in seq],
         "A": [amt.get(x["d"], 0.0) for x in seq]}
    f = [x["lv"] / x["c"] if x["c"] else 1.0 for x in seq]
    a["O"] = [x["o"] * k for x, k in zip(seq, f)]
    a["H"] = [x["h"] * k for x, k in zip(seq, f)]
    a["L"] = [x["l"] * k for x, k in zip(seq, f)]
    a["C"] = [x["lv"] for x in seq]
    for n in (5, 20, 45, 60):
        a[f"ma{n}"] = L.rolling_mean(a["C"], n)
    v20 = L.rolling_mean(a["V"], 20)
    a["v20p"] = [None] + v20[:-1]
    a["per"] = [per.get(d) for d in a["d"]]
    a["inst"] = [sum(inst.get(d, (0, 0))) for d in a["d"]]
    a["n"] = len(seq)
    return a, seq


# ── 進場 ─────────────────────────────────────────────
def is_surge(a, j):
    """書的爆量長紅：量 ≥ 前日 5 倍、漲 ≥ 5%、收紅、在 20 日線上、成交金額 ≥ 3 億"""
    if j < 1 or a["V"][j - 1] <= 0:
        return False
    return (a["V"][j] >= 5 * a["V"][j - 1] and a["ret"][j] >= 0.05 and a["C"][j] > a["O"][j]
            and a["ma20"][j] is not None and a["C"][j] > a["ma20"][j] and a["A"][j] >= 3)


def surge_days(a, start, cooldown=10):
    out, last = [], -99
    for j in range(max(start, 61), a["n"] - 1):
        if j - last >= cooldown and is_surge(a, j):
            out.append(j)
            last = j
    return out


def pullback_entry(a, j):
    for d in (j + 1, j + 2):
        if d >= a["n"]:
            return None
        if (a["C"][d] < a["O"][d] and a["V"][d] <= 0.5 * a["V"][j] and a["C"][d] >= a["C"][j] * 0.97
                and a["ma5"][d] and a["C"][d] > a["ma5"][d] and a["L"][d] > a["C"][j - 1]):
            return d
    return None


def dryup_entry(a, j, ratio):
    """爆量後 5 天內第一根「量 ≤ 前日 ratio 的陰線、離 5 日線 3% 內、在 20 日線上」"""
    for d in range(j + 1, min(a["n"], j + 6)):
        if (a["V"][d - 1] > 0 and a["V"][d] <= ratio * a["V"][d - 1] and a["C"][d] < a["O"][d]
                and a["ma5"][d] and abs(a["C"][d] / a["ma5"][d] - 1) <= 0.03
                and a["ma20"][d] and a["C"][d] > a["ma20"][d]):
            return d
    return None


def ma45_entries(a, start):
    out, last = [], -99
    for t in range(max(start, 70), a["n"] - 1):
        m45, m60 = a["ma45"][t], a["ma60"][t]
        if not (m45 and m60 and a["ma45"][t - 5]) or t - last < 20:
            continue
        above = all(a["ma45"][k] and a["C"][k] > a["ma45"][k] for k in range(t - 20, t))
        if above and a["L"][t] <= m45 * 1.01 and a["C"][t] >= m60 and m45 > a["ma45"][t - 5]:
            out.append(t)
            last = t
    return out


# ── 出場（回傳 (出場日索引, 出場價)）────────────────────
def x_next_open(a, e, **_):
    return (e + 1, a["O"][e + 1]) if e + 1 < a["n"] else (None, None)


def x_next_close(a, e, **_):
    return (e + 1, a["C"][e + 1]) if e + 1 < a["n"] else (None, None)


def x_book(a, e, stop=None, **_):
    entry = a["C"][e]
    for t in range(e + 1, min(a["n"], e + 1 + BOOK_HOLD)):
        if a["C"][t - 1] < entry and a["O"][t] < a["C"][t - 1]:          # 虧損中、隔天開低 → 開盤清倉
            return t, a["O"][t]
        if stop is not None and a["L"][t] <= stop:                       # 觸及爆量長紅低點
            return t, min(a["O"][t], stop)
        if a["ret"][t] <= -0.05:                                         # 單日跌 ≥ 5%
            return t, a["C"][t]
        if a["ma5"][t] and a["C"][t] < a["ma5"][t]:                      # 收盤破 5 日線
            return t, a["C"][t]
    t = min(a["n"] - 1, e + BOOK_HOLD)
    return (t, a["C"][t]) if t > e else (None, None)


def x_hold20(a, e, **_):
    t = e + 20
    return (t, a["C"][t]) if t < a["n"] else (None, None)


def x_swing(a, e, target=None, **_):
    for t in range(e + 1, min(a["n"], e + 21)):
        if target and a["H"][t] >= target:
            return t, max(a["O"][t], target)
        if a["ma60"][t] and a["C"][t] < a["ma60"][t]:
            return t, a["C"][t]
    t = e + 20
    return (t, a["C"][t]) if t < a["n"] else (None, None)


EXITS = {"next_open": x_next_open, "next_close": x_next_close, "book": x_book, "hold20": x_hold20, "swing": x_swing}


def trade(a, code, e, xname, bench, **kw):
    t, px = EXITS[xname](a, e, **kw)
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
    hw = [[t["r"] for t in trades if (t["d"] < mid_date) == first] for first in (True, False)]
    return {"n": n, "win": round(sum(1 for r in rs if r > 0) / n * 100, 1), "avg": round(sum(rs) / n, 2),
            "med": round(rs[n // 2], 2), "worst": round(rs[0], 1),
            "big_loss": round(sum(1 for r in rs if r <= -10) / n * 100, 1),
            "days": round(sum(t["days"] for t in trades) / n, 1),
            "ex": round(sum(t["ex"] for t in trades) / n, 2), "t": t20, "months": months,
            "halves": [round(sum(x) / len(x), 2) if x else None for x in h],
            "halves_win": [round(sum(1 for r in x if r > 0) / len(x) * 100, 1) if x else None for x in hw]}


# ── 過濾條件（作者的其他原則）────────────────────────
def f_profit(a, e, j):
    return a["per"][e] is not None and a["per"][e] > 0                   # 體質：有獲利


def f_bottom30(a, e, j):
    lo = min(a["L"][max(0, e - 60):e + 1])
    return a["C"][e] / lo - 1 < 0.30                                      # 離底部還沒漲 30%


def f_nolimit(a, e, j):
    return j is None or a["ret"][j] < 0.095                               # 爆量日不是漲停（台股隔日沖）


def f_big(a, e, j):
    return j is not None and a["A"][j] >= 15                              # 爆量日成交金額 ≥ 15 億（大哥股）


def f_inst(a, e, j):
    k = j if j is not None else e
    return sum(a["inst"][max(0, k - 5):k]) > 0                            # 爆量前 5 日外資＋投信買超


FILTERS = {"profit": ("有獲利（本益比 > 0）", f_profit), "bottom30": ("離 60 日低點漲幅 < 30%", f_bottom30),
           "nolimit": ("爆量日不是漲停", f_nolimit), "big": ("爆量日成交 ≥ 15 億（大哥股）", f_big),
           "inst": ("爆量前 5 日法人買超", f_inst), "mkt": ("大盤（0050）在 20 日線上", None)}


def run(stocks, bench, mid_date):
    """stocks: {code: (a, seq, f)}；bench: {日期: 0050 還原價}"""
    bdates = sorted(bench)
    bma = L.rolling_mean([bench[d] for d in bdates], 20)
    mkt_ok = {d: (m is not None and bench[d] > m) for d, m in zip(bdates, bma)}
    T = {}

    def add(key, tr, a=None, e=None, j=None, filt=True):
        if not tr:
            return
        T.setdefault(key, []).append(tr)
        if not filt:
            return
        for fk, (_, fn) in FILTERS.items():
            ok = mkt_ok.get(a["d"][e], False) if fk == "mkt" else fn(a, e, j)
            T.setdefault(f"{key}+{fk}" if ok else f"{key}-{fk}", []).append(tr)

    counts = {"surge": 0, "pullback": 0, "dryup25": 0, "dryup50": 0, "ma45": 0, "user": 0}
    for code, (a, seq, f) in stocks.items():
        start = bisect.bisect_left(a["d"], L.START)
        last = -99
        for j in range(max(start, 121), a["n"] - 2):
            if j - last < 10 or not C.base_signal(seq, f, j, C.VOL_STRICT):
                continue
            last = j
            counts["user"] += 1
            e = j + 1
            add("user|hold20", trade(a, code, e, "hold20", bench), filt=False)
            add("user|book", trade(a, code, e, "book", bench, stop=a["L"][j]), a, e, j)
            add("user|next_open", trade(a, code, e, "next_open", bench), filt=False)
        for j in surge_days(a, start):
            counts["surge"] += 1
            stop = a["L"][j]
            add("surge_chase|hold20", trade(a, code, j + 1, "hold20", bench), filt=False)
            add("surge_chase|book", trade(a, code, j + 1, "book", bench, stop=stop), a, j + 1, j)
            add("surge_chase|next_open", trade(a, code, j + 1, "next_open", bench), filt=False)
            e = pullback_entry(a, j)
            if e is not None:
                counts["pullback"] += 1
                for x in ("next_open", "next_close", "hold20"):
                    add(f"pullback|{x}", trade(a, code, e, x, bench), filt=False)
                add("pullback|book", trade(a, code, e, "book", bench, stop=stop), a, e, j)
            for ratio, key in ((0.25, "dryup25"), (0.5, "dryup50")):
                e = dryup_entry(a, j, ratio)
                if e is None:
                    continue
                counts[key] += 1
                for x in ("next_open", "next_close", "hold20"):
                    add(f"{key}|{x}", trade(a, code, e, x, bench), filt=False)
                add(f"{key}|book", trade(a, code, e, "book", bench, stop=stop), a, e, j)
        for t in ma45_entries(a, start):
            counts["ma45"] += 1
            target = max(a["H"][t - 20:t])
            add("ma45|swing", trade(a, code, t, "swing", bench, target=target), a, t, None)
            add("ma45|hold20", trade(a, code, t, "hold20", bench), filt=False)
    rows = []
    for k in sorted(T):
        rows.append({"key": k, **summarize(T[k], mid_date)})
    return {"rows": rows, "counts": counts}


ENTRY_NAMES = {"user": "你的券商條件（隔天買）", "surge_chase": "書的爆量長紅，隔天直接追",
               "pullback": "爆量後 1～2 天量縮小陰線（原版買點）", "dryup25": "爆量後量 ≤ 前日 25% 的陰線",
               "dryup50": "爆量後量 ≤ 前日 50% 的陰線", "ma45": "45 日線第一次回測（波段）"}
EXIT_NAMES = {"hold20": "抱 20 天", "book": "書的出場（≤5 天、破 5 日線、跌 5%、開低清倉）",
              "next_open": "隔天開盤賣", "next_close": "隔天收盤賣", "swing": "前高停利／破 60 日線停損"}


def label(key):
    if "+" in key:
        base, flt, neg = key.split("+")[0], key.split("+")[1], False
    elif "-" in key:
        base, flt, neg = key.split("-")[0], key.split("-")[1], True
    else:
        base, flt, neg = key, "", False
    ent, _, ex = base.partition("|")
    s = f"{ENTRY_NAMES.get(ent, ent)}｜{EXIT_NAMES.get(ex, ex)}"
    if flt:
        s += ("　✗ " if neg else "　＋") + FILTERS[flt][0]
    return s


def build():
    t0 = time.time()
    info = L.fm_get("TaiwanStockInfo", None, None)
    universe = C.pick_check_universe(info)
    bp = L.price_series(L.fm_get("TaiwanStockPrice", "0050"))
    bench = {d: v[0] for d, v in bp.items()}
    master = sorted(bench)
    start_i = bisect.bisect_left(master, L.START)
    mid_date = master[start_i + (len(master) - start_i) // 2]
    stocks = {}
    for k, (code, name) in enumerate(universe):
        pr = L.fm_get("TaiwanStockPrice", code)
        if len(pr) < 300:
            continue
        ins = L.inst_series(L.fm_get("TaiwanStockInstitutionalInvestorsBuySell", code))
        pe = L.fm_get("TaiwanStockPER", code)
        a, seq = build_arrays(pr, pe, ins)
        stocks[code] = (a, seq, C.features(seq, ins, {}, []))
        if (k + 1) % 25 == 0:
            log(f"   已抓 {k + 1}/{len(universe)} 支（{(time.time() - t0) / 60:.0f} 分）")
    log(f"資料完成：{len(stocks)} 支，開始回測")
    res = run(stocks, bench, mid_date)
    res.update({"generated": (datetime.utcnow() + timedelta(hours=8)).strftime("%Y-%m-%d %H:%M"),
                "n_stocks": len(stocks), "period": [master[start_i], master[-1]], "split": mid_date})
    return res


def report(res):
    log(f"──── {res['period'][0]} ~ {res['period'][1]}，{res['n_stocks']} 支；訊號次數 {res['counts']}；"
        f"前後半以 {res['split']} 為界；每筆已扣成本 {COST}%")
    for r in res["rows"]:
        if not r.get("n"):
            continue
        log(f"   {label(r['key'])}｜n={r['n']} 勝率 {r['win']}% 平均 {r['avg']:+}% 中位 {r['med']:+}% 抱 {r['days']} 天"
            f"｜最差 {r['worst']}% 虧10%↑ {r['big_loss']}%｜超額 {r['ex']:+}% t={r['t']}｜前後半超額 {r['halves']} 勝率 {r['halves_win']}")
