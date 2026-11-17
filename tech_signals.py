"""
正2 技術指標訊號回測（MACD、量能、均線、RSI、KD、布林、回檔幅度）
=================================================================
在指數上（0050 含息還原／加權報酬指數，2003 年起）每天收盤計算指標，訊號出現後「隔一個交易日」收盤
買進正2（合成：指數×2 扣成本），統計 20/60/120 個交易日後的勝率與報酬，和「任何一天買進」比較。

一次測很多指標，很容易「剛好」找到看起來很好的——所以判定標準刻意嚴格：
  穩定有效：樣本 ≥ 20、60 日勝率比基準高 ≥ 5 個百分點、z ≥ 3、
            而且前半段（約 2003–2014）與後半段（約 2015–2026）各自都比同期基準高 ≥ 3 個百分點
  傾向較好：z ≥ 2 且前後兩段都較好（參考就好）
  應該避開：反方向同樣標準（勝率明顯比較差）
事件型訊號 10 個交易日內重複出現只算第一次；狀態型（例如站上年線）每 5 天取一次樣本。
顯著性（z）只用「互不重疊」的樣本數計算：同一波下跌裡連續出現的訊號，60 天報酬幾乎相同，
不能當成多次獨立的成功（否則很容易誤判顯著）。
量能：加權報酬指數沒有成交量，兩個群組都用 0050 成交金額（相對 20 日均量）代表市場量能。
"""
import math

HORIZONS = [20, 60, 120]
COOLDOWN = 10
MIN_N = 20
MIN_N_EFF = 8       # 互不重疊（相隔 ≥ 60 個交易日）的樣本至少幾次
MIN_GAP = 5.0
MIN_HALF_GAP = 3.0
MIN_Z = 3.0
LEAN_Z = 2.0


# ── 指標 ──────────────────────────────────────────────
def sma(v, n):
    out, s = [None] * len(v), 0.0
    for i, x in enumerate(v):
        s += x
        if i >= n:
            s -= v[i - n]
        if i >= n - 1:
            out[i] = s / n
    return out


def ema(v, n):
    out, k, e = [None] * len(v), 2 / (n + 1), None
    for i, x in enumerate(v):
        if x is None:
            continue
        if e is None:
            if i >= n - 1 and all(y is not None for y in v[i - n + 1:i + 1]):
                e = sum(v[i - n + 1:i + 1]) / n
                out[i] = e
            continue
        e = x * k + e * (1 - k)
        out[i] = e
    return out


def rsi(v, n=14):
    out = [None] * len(v)
    g = l = 0.0
    for i in range(1, len(v)):
        ch = v[i] - v[i - 1]
        up, dn = max(ch, 0), max(-ch, 0)
        if i <= n:
            g += up / n
            l += dn / n
            if i < n:
                continue
        else:
            g = (g * (n - 1) + up) / n
            l = (l * (n - 1) + dn) / n
        out[i] = 100.0 if l == 0 else 100 - 100 / (1 + g / l)
    return out


def indicators(vals, vol=None):
    n = len(vals)
    ma20, ma60, ma200 = sma(vals, 20), sma(vals, 60), sma(vals, 200)
    e12, e26 = ema(vals, 12), ema(vals, 26)
    macd = [a - b if a is not None and b is not None else None for a, b in zip(e12, e26)]
    sig = ema(macd, 9)
    hist = [a - b if a is not None and b is not None else None for a, b in zip(macd, sig)]
    r = rsi(vals)
    # KD(9,3,3)：指數只有收盤價，用收盤的 9 日高低
    k = [None] * n
    d = [None] * n
    kk = dd_ = 50.0
    for i in range(8, n):
        lo, hi = min(vals[i - 8:i + 1]), max(vals[i - 8:i + 1])
        rsv = 50.0 if hi == lo else (vals[i] - lo) / (hi - lo) * 100
        kk = kk * 2 / 3 + rsv / 3
        dd_ = dd_ * 2 / 3 + kk / 3
        k[i], d[i] = kk, dd_
    # 布林通道 20,2
    bb_lo = [None] * n
    for i in range(19, n):
        w = vals[i - 19:i + 1]
        m = sum(w) / 20
        sd = math.sqrt(sum((x - m) ** 2 for x in w) / 20)
        bb_lo[i] = m - 2 * sd
    # 量比：今天成交金額 ÷ 前 20 天平均（不含今天）
    volr = [None] * n
    if vol:
        for i in range(20, n):
            w = vol[i - 20:i]
            if vol[i] is not None and all(x for x in w):
                volr[i] = vol[i] / (sum(w) / 20)
    # 距一年高點回檔
    dd = [None] * n
    for i in range(n):
        lo_i = max(0, i - 251)
        dd[i] = vals[i] / max(vals[lo_i:i + 1]) - 1 if i >= 60 else None
    ret1 = [None] + [vals[i] / vals[i - 1] - 1 for i in range(1, n)]
    return {"c": vals, "ma20": ma20, "ma60": ma60, "ma200": ma200, "macd": macd, "sig": sig, "hist": hist,
            "rsi": r, "k": k, "d": d, "bb_lo": bb_lo, "volr": volr, "dd": dd, "ret1": ret1}


def _ok(*xs):
    return all(x is not None for x in xs)


def _cross_up(a, b, i):
    return _ok(a[i], b[i], a[i - 1], b[i - 1]) and a[i - 1] <= b[i - 1] and a[i] > b[i]


def _cross_dn(a, b, i):
    return _ok(a[i], b[i], a[i - 1], b[i - 1]) and a[i - 1] >= b[i - 1] and a[i] < b[i]


def _lvl_up(a, lv, i):
    return _ok(a[i], a[i - 1]) and a[i - 1] <= lv < a[i]


def _lvl_dn(a, lv, i):
    return _ok(a[i], a[i - 1]) and a[i - 1] >= lv > a[i]


# (代碼, 名稱, 分類, 種類, 條件)  種類：event＝事件（只算第一次）／state＝狀態
SIGNALS = [
    ("above_ma200", "站上年線（200日均線之上）", "均線", "state", lambda x, i: _ok(x["ma200"][i]) and x["c"][i] > x["ma200"][i]),
    ("below_ma200", "跌破年線（200日均線之下）", "均線", "state", lambda x, i: _ok(x["ma200"][i]) and x["c"][i] < x["ma200"][i]),
    ("cross_ma200_up", "向上突破年線", "均線", "event", lambda x, i: _cross_up(x["c"], x["ma200"], i)),
    ("golden_20_60", "月線上穿季線（黃金交叉）", "均線", "event", lambda x, i: _cross_up(x["ma20"], x["ma60"], i)),
    ("death_20_60", "月線下穿季線（死亡交叉）", "均線", "event", lambda x, i: _cross_dn(x["ma20"], x["ma60"], i)),
    ("macd_cross", "MACD 黃金交叉", "MACD", "event", lambda x, i: _cross_up(x["macd"], x["sig"], i)),
    ("macd_cross_below0", "MACD 零軸下黃金交叉（低檔）", "MACD", "event",
     lambda x, i: _cross_up(x["macd"], x["sig"], i) and x["macd"][i] < 0),
    ("macd_zero_up", "MACD 由負轉正（上穿零軸）", "MACD", "event", lambda x, i: _lvl_up(x["macd"], 0, i)),
    ("macd_hist_turn", "MACD 柱狀體負值收斂（綠柱縮短）", "MACD", "event",
     lambda x, i: i >= 3 and _ok(*x["hist"][i - 3:i + 1]) and x["hist"][i] < 0
     and x["hist"][i] > x["hist"][i - 1] and x["hist"][i - 1] <= x["hist"][i - 2] <= x["hist"][i - 3]),
    ("macd_dead", "MACD 死亡交叉", "MACD", "event", lambda x, i: _cross_dn(x["macd"], x["sig"], i)),
    ("rsi_os_exit", "RSI 從 30 以下回升（超賣反轉）", "RSI/KD", "event", lambda x, i: _lvl_up(x["rsi"], 30, i)),
    ("rsi_50_up", "RSI 上穿 50", "RSI/KD", "event", lambda x, i: _lvl_up(x["rsi"], 50, i)),
    ("rsi_ob", "RSI 超過 70（過熱）", "RSI/KD", "event", lambda x, i: _lvl_up(x["rsi"], 70, i)),
    ("kd_low_cross", "KD 低檔黃金交叉（K<20）", "RSI/KD", "event",
     lambda x, i: _cross_up(x["k"], x["d"], i) and x["k"][i] < 20),
    ("bb_reenter", "跌破布林下軌後收回", "布林", "event",
     lambda x, i: _ok(x["bb_lo"][i], x["bb_lo"][i - 1]) and x["c"][i - 1] < x["bb_lo"][i - 1] and x["c"][i] > x["bb_lo"][i]),
    ("vol_spike_up", "爆量上漲（量 ≥ 2 倍均量）", "量能", "event",
     lambda x, i: _ok(x["volr"][i], x["ret1"][i]) and x["volr"][i] >= 2 and x["ret1"][i] > 0),
    ("vol_spike_down", "爆量重挫（量 ≥ 2 倍、跌 ≥ 2%）", "量能", "event",
     lambda x, i: _ok(x["volr"][i], x["ret1"][i]) and x["volr"][i] >= 2 and x["ret1"][i] <= -0.02),
    ("vol_dry_after_drop", "回檔 10% 後量縮（≤ 0.6 倍均量）", "量能", "event",
     lambda x, i: _ok(x["volr"][i], x["dd"][i]) and x["volr"][i] <= 0.6 and x["dd"][i] <= -0.10),
    ("dd10", "距一年高點回檔達 10%", "回檔", "event", lambda x, i: _lvl_dn(x["dd"], -0.10, i)),
    ("dd20", "距一年高點回檔達 20%", "回檔", "event", lambda x, i: _lvl_dn(x["dd"], -0.20, i)),
    ("dd30", "距一年高點回檔達 30%", "回檔", "event", lambda x, i: _lvl_dn(x["dd"], -0.30, i)),
    ("uptrend_dip", "年線之上 RSI 跌破 40（多頭回檔）", "組合", "event",
     lambda x, i: _ok(x["ma200"][i]) and x["c"][i] > x["ma200"][i] and _lvl_dn(x["rsi"], 40, i)),
    ("uptrend_macd", "年線之上 MACD 黃金交叉", "組合", "event",
     lambda x, i: _ok(x["ma200"][i]) and x["c"][i] > x["ma200"][i] and _cross_up(x["macd"], x["sig"], i)),
    ("dd20_macd", "回檔 20% 以上後 MACD 黃金交叉", "組合", "event",
     lambda x, i: _ok(x["dd"][i]) and x["dd"][i] <= -0.20 and _cross_up(x["macd"], x["sig"], i)),
    ("panic_volume", "回檔 10% 以上又爆量重挫（恐慌量）", "組合", "event",
     lambda x, i: _ok(x["dd"][i], x["volr"][i], x["ret1"][i]) and x["dd"][i] <= -0.10
     and x["volr"][i] >= 2 and x["ret1"][i] <= -0.02),
]


def signal_days(ind, fn, kind, n):
    """訊號成立的日子（事件型 10 日內只算第一次；狀態型每 5 天取一次）"""
    out, last = [], -10 ** 9
    for i in range(1, n):
        try:
            hit = fn(ind, i)
        except (TypeError, IndexError):
            hit = False
        if not hit:
            continue
        if kind == "event":
            if i - last < COOLDOWN:
                continue
            last = i
        elif i - last < 5:
            continue
        else:
            last = i
        out.append(i)
    return out


def _fwd(lev, idxs, h):
    rets = [(lev[i + h] / lev[i] - 1) * 100 for i in idxs if i + h < len(lev) and lev[i] > 0]
    if not rets:
        return {"n": 0}
    s = sorted(rets)
    return {"n": len(rets), "win": round(sum(1 for r in rets if r > 0) / len(rets) * 100, 1),
            "avg": round(sum(rets) / len(rets), 1), "med": round(s[len(s) // 2], 1), "worst": round(s[0], 1)}


def n_eff(entries, h):
    """互不重疊的樣本數：依序挑，下一筆要距上一筆 ≥ h 個交易日"""
    cnt, last = 0, -10 ** 9
    for e in entries:
        if e - last >= h:
            cnt += 1
            last = e
    return cnt


def _z(win, n, base):
    if not n or base is None or not 0 < base < 100:
        return 0.0
    p0 = base / 100
    return round((win / 100 - p0) / math.sqrt(p0 * (1 - p0) / n), 2)


def evaluate(dates, vals, lev, vol=None):
    """回傳 {signals: [...], baseline, halves, active, strategies}"""
    n = len(vals)
    ind = indicators(vals, vol)
    mid = n // 2
    all_days = list(range(200, n, 5))                 # 基準：任何一天買進（扣掉指標暖機期）
    base = {str(h): _fwd(lev, [i + 1 for i in all_days if i + 1 < n], h) for h in HORIZONS}
    base_h = [_fwd(lev, [i + 1 for i in all_days if i < mid], 60),
              _fwd(lev, [i + 1 for i in all_days if i >= mid and i + 1 < n], 60)]
    rows = []
    for key, name, cat, kind, fn in SIGNALS:
        days = signal_days(ind, fn, kind, n)
        entries = [i + 1 for i in days if i + 1 < n]
        st = {str(h): _fwd(lev, entries, h) for h in HORIZONS}
        s60 = st["60"]
        halves = [_fwd(lev, [i + 1 for i in days if i < mid], 60),
                  _fwd(lev, [i + 1 for i in days if i >= mid and i + 1 < n], 60)]
        ne = n_eff([e for e in entries if e + 60 < n], 60)
        z = _z(s60.get("win") or 0, ne, base["60"].get("win"))
        diffs = [(hf.get("win") - bh.get("win")) if hf.get("n", 0) >= 5 and bh.get("win") is not None else None
                 for hf, bh in zip(halves, base_h)]
        gap = (s60.get("win") or 0) - (base["60"].get("win") or 0)
        verdict = "none"
        if s60.get("n", 0) >= MIN_N and ne >= MIN_N_EFF and None not in diffs:
            if gap >= MIN_GAP and z >= MIN_Z and min(diffs) >= MIN_HALF_GAP:
                verdict = "effective"
            elif -gap >= MIN_GAP and z <= -MIN_Z and max(diffs) <= -MIN_HALF_GAP:
                verdict = "avoid"
            elif z >= LEAN_Z and min(diffs) > 0:
                verdict = "lean"
            elif z <= -LEAN_Z and max(diffs) < 0:
                verdict = "lean_avoid"
        recent = [dates[i] for i in days[-3:]]
        rows.append({"key": key, "name": name, "cat": cat, "kind": kind, "stats": st, "z": z, "n_eff": ne,
                     "half": [{"win": hf.get("win"), "n": hf.get("n", 0), "base": bh.get("win")}
                              for hf, bh in zip(halves, base_h)],
                     "verdict": verdict, "recent": recent,
                     "active": bool(days) and (days[-1] >= n - 5 if kind == "event" else _safe(fn, ind, n - 1)),
                     "today": bool(days) and days[-1] == n - 1 if kind == "event" else False})
    order = {"effective": 0, "lean": 1, "none": 2, "lean_avoid": 3, "avoid": 4}
    rows.sort(key=lambda r: (order[r["verdict"]], -(r["stats"]["60"].get("win") or 0)))
    return {"signals": rows, "baseline": base,
            "halves": {"split": dates[mid], "base_win": [b.get("win") for b in base_h]},
            "strategies": strategies(dates, vals, lev, ind["ma200"])}


def _safe(fn, ind, i):
    try:
        return bool(fn(ind, i))
    except (TypeError, IndexError):
        return False


def _curve_stats(eq, days_in, n_days):
    yrs = n_days / 252
    cagr = (eq[-1] / eq[0]) ** (1 / yrs) - 1 if yrs > 0 and eq[0] > 0 and eq[-1] > 0 else None
    peak, mdd = eq[0], 0.0
    for x in eq:
        peak = max(peak, x)
        mdd = min(mdd, x / peak - 1)
    return {"cagr": round(cagr * 100, 1) if cagr is not None else None, "mdd": round(mdd * 100, 1),
            "in_market": round(days_in / max(1, n_days) * 100), "final": round(eq[-1] / eq[0], 2)}


def strategies(dates, vals, lev, ma200, start=200):
    """策略比較：正2 一直抱著 vs 只在指數站上年線時持有（收盤判斷、隔日換手）vs 原型指數"""
    n = len(vals)
    out = {}
    for label, s in (("全期", start), ("近10年", max(start, n - 2520))):
        bh, ft, one = [1.0], [1.0], [1.0]
        inn = 0
        for i in range(s + 1, n):
            r2 = lev[i] / lev[i - 1] - 1
            bh.append(bh[-1] * (1 + r2))
            hold = ma200[i - 1] is not None and vals[i - 1] > ma200[i - 1]
            ft.append(ft[-1] * (1 + r2) if hold else ft[-1])
            inn += hold
            one.append(one[-1] * vals[i] / vals[i - 1])
        nd = n - s - 1
        out[label] = {"from": dates[s], "buy_hold_2x": _curve_stats(bh, nd, nd),
                      "ma200_2x": _curve_stats(ft, inn, nd), "index_1x": _curve_stats(one, nd, nd)}
    return out
