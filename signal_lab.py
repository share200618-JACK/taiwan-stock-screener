"""
個股訊號實驗室：哪些邏輯能提高個股「贏過大盤」的勝率？
=========================================================
用 2019 年起、隨機抽樣約 200 支上市櫃普通股（不是挑現在的熱門股，降低「只看到贏家」的偏差），
驗證下列邏輯在台股是否真的有效：

  營收：月營收創 12 個月新高且年增 ≥ 20%、營收年增率加速
  籌碼：投信連續買超 5 天、法人買＋融資減（散戶退場、法人接手）
  動能：近 12 個月（扣最近 1 個月）漲幅前 20%／後 20%
  過熱：5 日大漲 ≥ 25% 或 5 日內 2 根漲停（預期之後較差）
  趨勢：個股與大盤（0050）都在年線之上
  組合：營收好＋投信買＋站上年線、強勢不過熱＋雙年線

時間點（不偷看未來）
  決策在 t 日收盤後、t+1 日收盤進場；法人、融資用 t−1 日（收盤後才公布）；
  月營收 M 月的數字視為 M+1 月 11 日起才知道（法規 10 日前公布）。

怎麼算「勝」
  報酬一律扣掉「同一天所有合格股票的平均報酬」（扣掉大盤漲跌），超額 > 0 就算贏。
  合格：20 日均量 ≥ 300 張、股價 ≥ 10 元。

判定（刻意嚴格，避免一次測很多種挑到運氣）
  把每個月出現訊號的股票平均起來，逐月做 t 檢定（同一天很多支一起出現不會被重複計算）：
  穩定有效：樣本 ≥ 100、有訊號的月份 ≥ 18、20 日超額報酬逐月 t ≥ 3、勝率比基準高 ≥ 3 個百分點、
           前後兩段期間平均超額都 > 0 且勝率都高於基準、60 日方向一致
  傾向較好：t ≥ 2 且前後兩段都較好；應該避開／傾向較差：反方向
"""
import os
import sys
import json
import math
import time
import random
import bisect
from datetime import datetime, timedelta

import requests

try:
    import notify
except ImportError:
    notify = None

FINMIND_TOKEN = os.environ.get("FINMIND_TOKEN", "").strip()
SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")
FM_URL = "https://api.finmindtrade.com/api/v4/data"
FINMIND_MIN_INTERVAL = float(os.environ.get("FINMIND_MIN_INTERVAL", "7"))
N_STOCKS = int(os.environ.get("SIGNAL_LAB_N", "200"))
SEED = "signal-lab-v1"          # 固定抽樣，每次更新用同一批股票
START = "2019-01-01"
DATA_START = "2017-12-01"       # 年線、12 個月動能需要暖機；營收年增需要前一年
HORIZONS = [20, 60]
COOLDOWN = 20
MIN_VOL = 300                   # 20 日均量（張）
MIN_PRICE = 10
STORE_KEY = "signal_lab"

SIGNALS = {
    "rev_high":       ("月營收創 12 個月新高且年增 ≥ 20%", "營收", "event"),
    "rev_accel":      ("營收年增率加速（近 3 月比前 3 月 +10 個百分點）", "營收", "event"),
    "trust_streak":   ("投信連續買超 5 天", "籌碼", "event"),
    "inst_margin":    ("法人買超＋融資 10 日減 ≥ 5%", "籌碼", "event"),
    "mom_top":        ("中期強勢：近 12 個月漲幅前 20%", "動能", "state"),
    "mom_bottom":     ("中期弱勢：近 12 個月漲幅後 20%", "動能", "state"),
    "overheat":       ("短期過熱：5 日漲 ≥ 25% 或 2 根漲停", "過熱", "event"),
    "trend_both":     ("個股與大盤都在年線之上", "趨勢", "state"),
    "combo_rev_trust": ("營收好＋投信買超＋站上年線", "組合", "event"),
    "combo_mom":      ("中期強勢＋不過熱＋雙年線", "組合", "state"),
}


def log(msg):
    print(f"[{datetime.utcnow() + timedelta(hours=8):%H:%M:%S}] {msg}", flush=True)


def _f(v, d=0.0):
    try:
        return float(v)
    except (TypeError, ValueError):
        return d


# ── 資料 ──────────────────────────────────────────────
class TokenBad(Exception):
    pass


_last = [0.0]


def fm_get(dataset, data_id=None, start=DATA_START):
    wait = FINMIND_MIN_INTERVAL - (time.time() - _last[0])
    if wait > 0:
        time.sleep(wait)
    _last[0] = time.time()
    params = {"dataset": dataset, "start_date": start}
    if data_id:
        params["data_id"] = data_id
    headers = {"Authorization": f"Bearer {FINMIND_TOKEN}"} if FINMIND_TOKEN else {}
    for attempt in range(3):
        try:
            r = requests.get(FM_URL, params=params, headers=headers, timeout=60)
            body = r.json()
        except Exception as e:
            log(f"⚠️ {dataset} {data_id}：{e}")
            time.sleep(10)
            continue
        if r.status_code in (402, 429) or body.get("status") in (402, 429):
            log("⏳ FinMind 額度暫時用完，等 10 分鐘再繼續")
            time.sleep(600)
            continue
        if r.status_code != 200:
            if "token" in str(body.get("msg", "")).lower():
                raise TokenBad(body.get("msg"))
            log(f"⚠️ {dataset} {data_id}：HTTP {r.status_code} {str(body.get('msg', ''))[:80]}")
            return []
        return body.get("data") or []
    return []


def pick_universe(info_rows, n=N_STOCKS, seed=SEED):
    """所有上市櫃普通股（4 碼、非 0 開頭、排除 ETF/受益證券等）隨機抽 n 支，固定種子"""
    skip = ("ETF", "ETN", "受益證券", "存託憑證", "Index", "大盤", "所有證券")
    seen = {}
    for r in info_rows:
        sid = str(r.get("stock_id", ""))
        if (len(sid) == 4 and sid.isdigit() and not sid.startswith("0")
                and r.get("type") in ("twse", "tpex") and not any(k in str(r.get("industry_category", "")) for k in skip)):
            seen.setdefault(sid, r.get("stock_name", sid))
    codes = sorted(seen)
    rnd = random.Random(seed)
    return [(c, seen[c]) for c in sorted(rnd.sample(codes, min(n, len(codes))))]


def price_series(rows):
    """還原價（用交易所參考價算每日報酬，除權息、分割都正確）＋原始收盤、成交量（張）"""
    rows = sorted((r for r in rows if _f(r.get("close")) > 0), key=lambda r: r["date"])
    out, lv, prev = {}, 100.0, None
    for r in rows:
        c = _f(r["close"])
        sp = r.get("spread")
        if prev is not None:
            ref = c - _f(sp) if sp not in (None, "") else None
            ret = c / ref - 1 if ref and ref > 0 and 0.5 < c / ref < 1.5 else c / prev - 1
            if abs(ret) > 0.3:
                ret = 0.0
            lv *= 1 + ret
        out[r["date"][:10]] = (lv, c, _f(r.get("Trading_Volume")) / 1000)
        prev = c
    return out


def inst_series(rows):
    d = {}
    for r in rows:
        k = r["date"][:10]
        f, t = d.get(k, (0.0, 0.0))
        net = (_f(r.get("buy")) - _f(r.get("sell"))) / 1000
        if str(r.get("name", "")).startswith("Foreign"):
            f += net
        elif r.get("name") == "Investment_Trust":
            t += net
        d[k] = (f, t)
    return d


def margin_series(rows):
    return {r["date"][:10]: _f(r.get("MarginPurchaseTodayBalance")) for r in rows}


def revenue_events(rows):
    """
    月營收 → [(可取得日, 年增%, 是否 12 個月新高, 近3月平均年增 − 前3月平均年增)]
    M 月營收視為 M+1 月 11 日起才知道
    """
    rev = {}
    for r in rows:
        y, m = int(_f(r.get("revenue_year"))), int(_f(r.get("revenue_month")))
        if y and 1 <= m <= 12:
            rev[(y, m)] = _f(r.get("revenue"))
    out = []
    keys = sorted(rev)
    yoy = {}
    for (y, m) in keys:
        last = rev.get((y - 1, m))
        if last and last > 0:
            yoy[(y, m)] = (rev[(y, m)] / last - 1) * 100
    def prev_m(y, m, k):
        t = y * 12 + (m - 1) - k
        return (t // 12, t % 12 + 1)
    for (y, m) in keys:
        if (y, m) not in yoy:
            continue
        last12 = [rev.get(prev_m(y, m, k)) for k in range(1, 12)]
        is_high = all(v is not None for v in last12) and rev[(y, m)] > max(last12)
        recent = [yoy.get(prev_m(y, m, k)) for k in range(0, 3)]
        before = [yoy.get(prev_m(y, m, k)) for k in range(3, 6)]
        accel = (sum(recent) / 3 - sum(before) / 3) if None not in recent + before else None
        ay, am = (y + (m == 12), m % 12 + 1)
        out.append((f"{ay}-{am:02d}-11", round(yoy[(y, m)], 1), is_high, accel))
    return sorted(out)


# ── 對齊與指標 ────────────────────────────────────────
def align(master, series_map):
    """把各股資料對到大盤（0050）的交易日；缺的日子為 None"""
    return [series_map.get(d) for d in master]


def build_stock(master, price_rows, inst_rows, margin_rows, rev_rows):
    p = price_series(price_rows)
    al = align(master, p)
    lv = [x[0] if x else None for x in al]
    close = [x[1] if x else None for x in al]
    vol = [x[2] if x else None for x in al]
    inst = align(master, inst_series(inst_rows))
    if inst_rows:   # 有交易但法人沒進出的日子，FinMind 可能不給資料 → 視為 0
        inst = [x if x is not None or c is None else (0.0, 0.0) for x, c in zip(inst, close)]
    mar = align(master, margin_series(margin_rows))
    return {"lv": lv, "close": close, "vol": vol, "inst": inst, "margin": mar, "rev": revenue_events(rev_rows),
            "ma200": rolling_mean(lv, 200), "vol20": rolling_mean(vol, 20)}


def rolling_mean(vals, n):
    """移動平均（視窗內有缺值就是 None），前綴和計算"""
    out = [None] * len(vals)
    s, bad = 0.0, 0
    for i, x in enumerate(vals):
        if x is None:
            bad += 1
        else:
            s += x
        if i >= n:
            y = vals[i - n]
            if y is None:
                bad -= 1
            else:
                s -= y
        if i >= n - 1 and bad == 0:
            out[i] = s / n
    return out


def eligible(s, i):
    v, c = s["vol20"][i], s["close"][i]
    return v is not None and c is not None and v >= MIN_VOL and c >= MIN_PRICE


def fwd(s, i, h):
    """t 日訊號 → t+1 日收盤進場，持有 h 日"""
    a, b = i + 1, i + 1 + h
    if b >= len(s["lv"]) or s["lv"][a] is None or s["lv"][b] is None:
        return None
    return s["lv"][b] / s["lv"][a] - 1


def stock_signals(master, s, bench_above):
    """
    單一股票的事件型訊號 {訊號: [日索引]}（狀態型在 run 裡用每月換股日處理）
    bench_above[i]：大盤（0050）是否在年線之上
    """
    n = len(master)
    out = {k: [] for k, v in SIGNALS.items() if v[2] == "event"}
    last = {k: -10 ** 9 for k in out}
    lv, inst, mar = s["lv"], s["inst"], s["margin"]
    rev_dates = [e[0] for e in s["rev"]]
    def add(k, i):
        if i - last[k] >= COOLDOWN:
            out[k].append(i)
            last[k] = i
    for i in range(250, n):
        if lv[i] is None:
            continue
        d = master[i]
        # 營收：當天剛好可以取得新的一個月
        j = bisect.bisect_right(rev_dates, d) - 1
        rev_new = j >= 0 and (bisect.bisect_right(rev_dates, master[i - 1]) - 1) != j   # 今天剛可取得新月份
        if j >= 0 and rev_new:
            _, yoy, high, accel = s["rev"][j]
            if high and yoy >= 20:
                add("rev_high", i)
            if accel is not None and accel >= 10 and yoy > 0:
                add("rev_accel", i)
        # 法人、融資：用前一個交易日（含）往前的資料
        w = [inst[k] for k in range(i - 10, i)]
        if all(x is not None for x in w):
            trust = [x[1] for x in w]
            if all(t > 0 for t in trust[-5:]) and not trust[-6] > 0:
                add("trust_streak", i)
            nets = [x[0] + x[1] for x in w]
            m0, m1 = mar[i - 11], mar[i - 1]
            if (sum(nets) > 0 and sum(1 for x in nets if x > 0) >= 6 and m0 and m1
                    and m1 / m0 - 1 <= -0.05):
                add("inst_margin", i)
        # 短期過熱
        if lv[i - 5] is not None and all(lv[k] is not None for k in range(i - 5, i + 1)):
            r5 = lv[i] / lv[i - 5] - 1
            limit_ups = sum(1 for k in range(i - 4, i + 1) if lv[k] / lv[k - 1] - 1 >= 0.095)
            if r5 >= 0.25 or limit_ups >= 2:
                add("overheat", i)
        # 組合：最近 30 日內有好營收、投信 10 日買超、站上年線
        ma200 = s["ma200"][i]
        if j >= 0 and ma200 and lv[i] > ma200 and all(x is not None for x in w):
            rd = datetime.strptime(s["rev"][j][0], "%Y-%m-%d")
            fresh = (datetime.strptime(d, "%Y-%m-%d") - rd).days <= 30
            good_rev = s["rev"][j][2] and s["rev"][j][1] >= 20 or (s["rev"][j][3] or -99) >= 10
            if fresh and good_rev and sum(x[1] for x in w) > 0:
                add("combo_rev_trust", i)
    return out


# ── 統計 ──────────────────────────────────────────────
def _mean(xs):
    return sum(xs) / len(xs) if xs else None


def t_monthly(obs):
    """obs: [(月份, 超額)] → 逐月平均後的 t 值與月份數"""
    by = {}
    for m, x in obs:
        by.setdefault(m, []).append(x)
    ms = [sum(v) / len(v) for v in by.values()]
    if len(ms) < 3:
        return 0.0, len(ms)
    mu = sum(ms) / len(ms)
    var = sum((x - mu) ** 2 for x in ms) / (len(ms) - 1)
    return (round(mu / math.sqrt(var / len(ms)), 2) if var > 0 else 0.0), len(ms)


def summarize(obs, base_win):
    """obs: [(月份, 超額%)]"""
    if not obs:
        return {"n": 0}
    xs = sorted(x for _, x in obs)
    return {"n": len(xs), "win": round(sum(1 for x in xs if x > 0) / len(xs) * 100, 1),
            "avg": round(sum(xs) / len(xs), 2), "med": round(xs[len(xs) // 2], 2),
            "edge": round(sum(1 for x in xs if x > 0) / len(xs) * 100 - base_win, 1) if base_win is not None else None}


def verdict(st20, st60, t20, months, halves, base_halves):
    if st20.get("n", 0) < 100 or months < 18 or None in base_halves:
        return "none"
    hw = [(h.get("win"), h.get("avg")) for h in halves]
    if any(w is None for w, _ in hw):
        return "none"
    better = all(w > b and a > 0 for (w, a), b in zip(hw, base_halves))
    worse = all(w < b and a < 0 for (w, a), b in zip(hw, base_halves))
    e20 = st20.get("edge") or 0
    if t20 >= 3 and e20 >= 3 and better and (st60.get("avg") or 0) > 0:
        return "effective"
    if t20 <= -3 and e20 <= -3 and worse and (st60.get("avg") or 0) < 0:
        return "avoid"
    if t20 >= 2 and better:
        return "lean"
    if t20 <= -2 and worse:
        return "lean_avoid"
    return "none"


def run_lab(master, stocks, bench_lv, names):
    """stocks: {code: build_stock 結果} → 完整結果（不連網，方便測試）"""
    n = len(master)
    start_i = bisect.bisect_left(master, START)
    codes = list(stocks)
    bench_ma = rolling_mean(bench_lv, 200)
    bench_above = [bench_ma[i] is not None and bench_lv[i] > bench_ma[i] for i in range(n)]
    # 每天合格股票的平均報酬（同期平均）
    mean = {h: [None] * n for h in HORIZONS}
    elig = {c: [eligible(stocks[c], i) for i in range(n)] for c in codes}
    for i in range(start_i, n):
        live = [c for c in codes if elig[c][i]]
        for h in HORIZONS:
            xs = [r for r in (fwd(stocks[c], i, h) for c in live) if r is not None]
            mean[h][i] = sum(xs) / len(xs) if len(xs) >= 20 else None

    def excess(c, i, h):
        r = fwd(stocks[c], i, h)
        return None if r is None or mean[h][i] is None else (r - mean[h][i]) * 100

    obs = {k: {h: [] for h in HORIZONS} for k in SIGNALS}
    hits_recent = {k: [] for k in SIGNALS}
    # 事件型
    for c in codes:
        ev = stock_signals(master, stocks[c], bench_above)
        for k, days in ev.items():
            for i in days:
                if i < start_i or not elig[c][i]:
                    continue
                if i >= n - 10:
                    hits_recent[k].append((master[i], c))
                for h in HORIZONS:
                    x = excess(c, i, h)
                    if x is not None:
                        obs[k][h].append((master[i][:7], x, i))
    # 狀態型：每 20 個交易日換股一次
    rebal = list(range(start_i, n, 20))
    if rebal[-1] != n - 1:
        rebal.append(n - 1)
    base = {h: [] for h in HORIZONS}
    for i in rebal:
        live = [c for c in codes if elig[c][i]]
        moms = []
        for c in live:
            lv = stocks[c]["lv"]
            if i >= 250 and lv[i - 20] and lv[i - 250]:
                moms.append((lv[i - 20] / lv[i - 250] - 1, c))
        moms.sort()
        q = len(moms) // 5
        bottom = {c for _, c in moms[:q]} if q else set()
        top = {c for _, c in moms[-q:]} if q else set()
        for c in live:
            s = stocks[c]
            lv = s["lv"]
            ma200 = s["ma200"][i]
            above = ma200 is not None and lv[i] > ma200
            r5 = lv[i] / lv[i - 5] - 1 if lv[i - 5] else 0
            states = {"mom_top": c in top, "mom_bottom": c in bottom,
                      "trend_both": above and bench_above[i],
                      "combo_mom": c in top and r5 < 0.15 and above and bench_above[i]}
            for h in HORIZONS:
                x = excess(c, i, h)
                if x is not None:
                    base[h].append((master[i][:7], x, i))
            for k, on in states.items():
                if not on:
                    continue
                if i == rebal[-1]:
                    hits_recent[k].append((master[i], c))
                for h in HORIZONS:
                    x = excess(c, i, h)
                    if x is not None:
                        obs[k][h].append((master[i][:7], x, i))

    mid = start_i + (n - start_i) // 2
    base_win = {h: summarize([(m, x) for m, x, _ in base[h]], None) for h in HORIZONS}
    base_half = [summarize([(m, x) for m, x, i in base[20] if (i < mid) == first], None) for first in (True, False)]
    rows = []
    for k, (name, cat, kind) in SIGNALS.items():
        st = {str(h): summarize([(m, x) for m, x, _ in obs[k][h]], base_win[h].get("win")) for h in HORIZONS}
        t20, months = t_monthly([(m, x) for m, x, _ in obs[k][20]])
        halves = [summarize([(m, x) for m, x, i in obs[k][20] if (i < mid) == first], None) for first in (True, False)]
        v = verdict(st["20"], st["60"], t20, months, halves, [b.get("win") for b in base_half])
        recent = sorted(hits_recent[k], reverse=True)[:20]
        rows.append({"key": k, "name": name, "cat": cat, "kind": kind, "stats": st, "t20": t20, "months": months,
                     "half": [{"win": h.get("win"), "avg": h.get("avg"), "n": h.get("n", 0)} for h in halves],
                     "verdict": v,
                     "recent": [{"date": d, "code": c, "name": names.get(c, c)} for d, c in recent]})
    order = {"effective": 0, "lean": 1, "none": 2, "lean_avoid": 3, "avoid": 4}
    rows.sort(key=lambda r: (order[r["verdict"]], -(r["stats"]["20"].get("win") or 0)))
    return {"signals": rows,
            "baseline": {str(h): base_win[h] for h in HORIZONS},
            "halves": {"split": master[mid], "base_win": [b.get("win") for b in base_half]},
            "period": [master[start_i], master[-1]]}


# ── 主流程 ────────────────────────────────────────────
def build():
    t0 = time.time()
    info = fm_get("TaiwanStockInfo", None, None)
    universe = pick_universe(info)
    names = dict(universe)
    log(f"抽樣 {len(universe)} 支（全市場 {len({r.get('stock_id') for r in info})} 檔中，固定種子）")
    bench_rows = fm_get("TaiwanStockPrice", "0050")
    bp = price_series(bench_rows)
    master = sorted(bp)
    bench_lv = [bp[d][0] for d in master]
    stocks = {}
    for k, (code, name) in enumerate(universe):
        pr = fm_get("TaiwanStockPrice", code)
        if len(pr) < 300:
            log(f"   {code} {name} 資料太少，略過")
            continue
        ins = fm_get("TaiwanStockInstitutionalInvestorsBuySell", code)
        mg = fm_get("TaiwanStockMarginPurchaseShortSale", code)
        rv = fm_get("TaiwanStockMonthRevenue", code, "2016-12-01")
        stocks[code] = build_stock(master, pr, ins, mg, rv)
        if (k + 1) % 20 == 0:
            log(f"   已抓 {k + 1}/{len(universe)} 支（{(time.time() - t0) / 60:.0f} 分）")
    log(f"資料完成：{len(stocks)} 支，開始回測")
    res = run_lab(master, stocks, bench_lv, names)
    res.update({"generated": (datetime.utcnow() + timedelta(hours=8)).strftime("%Y-%m-%d %H:%M"),
                "n_stocks": len(stocks), "universe_total": len({r.get('stock_id') for r in info}),
                "method": {"min_vol": MIN_VOL, "min_price": MIN_PRICE, "cooldown": COOLDOWN, "seed": SEED}})
    return res


def save(payload):
    if not SUPABASE_URL or not SUPABASE_KEY:
        return False
    h = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}",
         "Content-Type": "application/json", "Prefer": "return=minimal"}
    url = f"{SUPABASE_URL}/rest/v1/analysis_store"
    today = (datetime.utcnow() + timedelta(hours=8)).strftime("%Y-%m-%d")
    try:
        requests.delete(url, params={"key": f"eq.{STORE_KEY}", "date": f"eq.{today}"}, headers=h, timeout=15)
        r = requests.post(url, json={"key": STORE_KEY, "date": today, "payload": payload}, headers=h, timeout=60)
        if r.status_code in (200, 201):
            log("✅ 已存入 Supabase analysis_store")
            return True
        log(f"❌ 存入失敗 {r.status_code}（請先建立 analysis_store 表）: {r.text[:150]}")
    except Exception as e:
        log(f"❌ 存入例外: {e}")
    return False


def report(res):
    vname = {"effective": "✅ 穩定有效", "lean": "△ 傾向較好", "avoid": "⛔ 應該避開", "lean_avoid": "▽ 傾向較差", "none": ""}
    b = res["baseline"]
    log(f"──── 期間 {res['period'][0]} ~ {res['period'][1]}，{res['n_stocks']} 支；"
        f"基準（任一合格股票贏過同期平均）20日 {b['20'].get('win')}%、60日 {b['60'].get('win')}%；"
        f"前後半 {res['halves']['base_win']}（以 {res['halves']['split']} 為界）")
    for s in res["signals"]:
        a, c = s["stats"]["20"], s["stats"]["60"]
        log(f"   {s['name']:<30} 20日 勝率 {a.get('win')}% 超額 {a.get('avg')}%｜60日 勝率 {c.get('win')}% 超額 {c.get('avg')}%"
            f"｜n={a.get('n', 0)} 月={s['months']} t={s['t20']}｜前後半 {s['half'][0]['win']}/{s['half'][1]['win']}  {vname[s['verdict']]}")


def main():
    try:
        res = build()
    except TokenBad as e:
        log(f"❌ FinMind token 無效：{e}")
        sys.exit(1)
    report(res)
    with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "signal_lab_result.json"), "w",
              encoding="utf-8") as f:
        json.dump(res, f, ensure_ascii=False)
    saved = save(res)
    if notify and notify.configured():
        good = [s for s in res["signals"] if s["verdict"] in ("effective", "lean")]
        bad = [s for s in res["signals"] if s["verdict"] in ("avoid", "lean_avoid")]
        lines = [f"🔬 <b>個股訊號實驗室</b>（{res['n_stocks']} 支、{res['period'][0][:4]} 起，勝率＝贏過同期平均）"]
        lines += [f"✅ {notify.esc(s['name'])}：20日勝率 {s['stats']['20']['win']}%（基準 {res['baseline']['20']['win']}%）"
                  for s in good] or ["沒有任何邏輯顯著提高勝率"]
        lines += [f"⛔ 避開：{notify.esc(s['name'])}：20日勝率 {s['stats']['20']['win']}%" for s in bad]
        if not saved:
            lines.append("<i>（結果沒存進 Supabase，請建立 analysis_store 表）</i>")
        notify.send("\n".join(lines))


if __name__ == "__main__":
    main()
