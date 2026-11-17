"""
台股正2 × 波浪理論 歷史回測（每日排程自動更新）
================================================
回答：「用波浪理論看，什麼時候買正2，之後的報酬率與勝率比較高？」

標的
  台灣50 正2：00631L 元大台灣50正2（目前唯一追蹤臺灣50 的正2）
  加權 正2 ：00663L 國泰、00675L 富邦、00685L 群益（追蹤加權指數）
  00631L 2026/3 一拆二十二、00685L 2026/7 一拆二十四 → 用交易所參考價還原，分割日不會變成假暴跌

方法（全部只用「當下拿得到」的資料，不偷看未來）
  1. 波浪畫在「指數」上：臺灣50 用 0050 含息還原、加權用加權報酬指數（2003 年起，約 23 年）
  2. ZigZag 找波段高低點：價格從極值反轉 θ（6/8/10/12%）才「確認」該高/低點；確認當天收盤後
     才知道 → 隔一個交易日收盤進場
  3. 依最近幾個已確認的高低點，用艾略特波浪的基本規則判讀位置：
     第2浪結束（第3浪起漲）、第4浪結束（第5浪起漲）、ABC 修正結束、第3浪/第5浪頂、B 波反彈結束…
  4. 進場後 20/60/120 個交易日的報酬：
     ・合成正2：指數每日報酬 × 2 再扣掉成本（成本用實際 ETF 與合成值的差距校正）→ 可回測到 2003 年
     ・實際 ETF：各檔上市以來的真實價格（樣本較少）
  5. 與「任何一天買進」比較，勝率/報酬明顯較好的情況才有參考價值；θ 換不同值結果仍一致才算穩健

限制：波浪理論本身主觀，這裡是可重複驗證的「規則化近似」；訊號次數少（每種情況通常 10~40 次），
     過去表現不代表未來；正2 有波動耗損，盤整期間會持續虧損。不構成投資建議。
"""
import os
import json
import time
import math
import bisect
from datetime import datetime, timedelta

import requests

try:
    import notify
except ImportError:  # 單獨測試時
    notify = None

FINMIND_TOKEN = os.environ.get("FINMIND_TOKEN", "").strip()
SUPABASE_URL  = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY  = os.environ.get("SUPABASE_KEY", "")
FM_URL = "https://api.finmindtrade.com/api/v4/data"
FINMIND_MIN_INTERVAL = float(os.environ.get("FINMIND_MIN_INTERVAL", "7"))

ETFS = [
    {"code": "00631L", "name": "元大台灣50正2", "group": "tw50"},
    {"code": "00663L", "name": "國泰臺灣加權正2", "group": "taiex"},
    {"code": "00675L", "name": "富邦臺灣加權正2", "group": "taiex"},
    {"code": "00685L", "name": "群益臺灣加權正2", "group": "taiex"},
]
GROUPS = {
    "tw50":  {"name": "臺灣50", "index_name": "臺灣50指數（0050 含息還原）",
              "dataset": "TaiwanStockPrice", "id": "0050"},
    "taiex": {"name": "加權指數", "index_name": "加權報酬指數",
              "dataset": "TaiwanStockTotalReturnIndex", "id": "TAIEX"},
}
THETAS = [0.06, 0.08, 0.10, 0.12]
DEFAULT_THETA = 0.08
HORIZONS = [20, 60, 120]
START = "2003-01-01"
DEFAULT_DRAG = 0.015          # 合成正2 每年成本（費用＋資金成本）預設值；有實際 ETF 時改用校正值

STATES = {   # 代碼 → (名稱, 方向)  方向：buy＝低點確認後買進 / sell＝高點確認後
    "w3_start":   ("第2浪結束 → 第3浪起漲", "buy"),
    "w5_start":   ("第4浪結束 → 第5浪起漲", "buy"),
    "abc_end":    ("ABC 修正結束", "buy"),
    "low_other":  ("其他波段低點", "buy"),
    "w3_top":     ("第3浪頂 → 第4浪回檔", "sell"),
    "w5_top":     ("第5浪頂（可能大頭部）", "sell"),
    "b_top":      ("B波反彈結束 → C波下跌", "sell"),
    "high_other": ("其他波段高點", "sell"),
}


def log(msg):
    print(f"[{datetime.utcnow() + timedelta(hours=8):%H:%M:%S}] {msg}", flush=True)


# ── 資料 ──────────────────────────────────────────────
_fm_last = [0.0]


def fm_get(dataset, data_id, start=START):
    wait = FINMIND_MIN_INTERVAL - (time.time() - _fm_last[0])
    if wait > 0:
        time.sleep(wait)
    _fm_last[0] = time.time()
    headers = {"Authorization": f"Bearer {FINMIND_TOKEN}"} if FINMIND_TOKEN else {}
    try:
        r = requests.get(FM_URL, params={"dataset": dataset, "data_id": data_id, "start_date": start},
                         headers=headers, timeout=60)
        body = r.json()
    except Exception as e:
        log(f"⚠️ FinMind {dataset} {data_id} 失敗：{e}")
        return []
    if r.status_code != 200:
        log(f"⚠️ FinMind {dataset} {data_id}：HTTP {r.status_code} {str(body.get('msg', ''))[:80]}")
        return []
    return body.get("data") or []


def series_from_prices(rows):
    """
    TaiwanStockPrice → 還原後的指數序列（含息、含分割）。
    每日報酬＝收盤 ÷ 交易所參考價 − 1，參考價＝收盤 − 漲跌價差；
    除息、分割當天交易所已把參考價調好，所以 1 拆 22 也不會變成 −95%。
    回傳 (日期 list, 指數 list，從 100 起算)
    """
    rows = sorted((r for r in rows if _f(r.get("close")) > 0), key=lambda r: r["date"])
    dates, vals, level, prev = [], [], 100.0, None
    for r in rows:
        c = _f(r.get("close"))
        sp = r.get("spread")
        if prev is not None:
            ref = c - _f(sp) if sp not in (None, "") else None
            if ref and ref > 0 and 0.5 < c / ref < 1.5:
                ret = c / ref - 1
            else:
                ret = c / prev - 1
                if abs(ret) > 0.3:      # 沒有參考價又跳很大 → 多半是分割，不當成漲跌
                    log(f"   ℹ️ {r['date']} 價格跳動 {ret:+.0%} 無參考價，視為分割")
                    ret = 0.0
            level *= 1 + ret
        dates.append(r["date"][:10])
        vals.append(level)
        prev = c
    return dates, vals


def series_from_index(rows):
    rows = sorted((r for r in rows if _f(r.get("price")) > 0), key=lambda r: r["date"])
    return [r["date"][:10] for r in rows], [_f(r["price"]) for r in rows]


def _f(v, d=0.0):
    try:
        return float(v)
    except (TypeError, ValueError):
        return d


def daily_returns(vals):
    return [0.0] + [vals[i] / vals[i - 1] - 1 for i in range(1, len(vals))]


def synthetic_2x(vals, drag=DEFAULT_DRAG):
    """指數每日報酬 ×2，每天扣 drag/252（費用＋資金成本）"""
    out, lv = [100.0], 100.0
    for i in range(1, len(vals)):
        lv *= max(0.0, 1 + 2 * (vals[i] / vals[i - 1] - 1) - drag / 252)
        out.append(lv)
    return out


def calibrate_drag(u_dates, u_vals, e_dates, e_vals):
    """實際 ETF 與「指數×2、零成本」的年化落差 → 合成正2 的成本"""
    if len(e_dates) < 250:
        return None
    pos = {d: i for i, d in enumerate(u_dates)}
    pairs = [(pos[d], j) for j, d in enumerate(e_dates) if d in pos]
    if len(pairs) < 250:
        return None
    syn = 0.0
    act = 0.0
    for (i0, j0), (i1, j1) in zip(pairs, pairs[1:]):
        ru = u_vals[i1] / u_vals[i0] - 1
        syn += math.log(max(1e-9, 1 + 2 * ru))
        act += math.log(e_vals[j1] / e_vals[j0])
    years = len(pairs) / 252
    return round((syn - act) / years, 4)


# ── ZigZag 與波浪判讀 ─────────────────────────────────
def zigzag(vals, theta):
    """
    逐日往前掃，只用到當天為止的價格：
    從最高點回落 θ → 確認那個高點；從最低點反彈 θ → 確認那個低點。
    回傳 (pivots, 目前方向, 目前極值索引)；pivot: {"i", "t"(H/L), "p", "c"(確認日索引)}
    """
    n = len(vals)
    piv, trend = [], 0
    hi = lo = ext = 0
    for i in range(1, n):
        p = vals[i]
        if trend == 0:
            if p > vals[hi]: hi = i
            if p < vals[lo]: lo = i
            if p >= vals[lo] * (1 + theta) and lo < i:
                piv.append({"i": lo, "t": "L", "p": vals[lo], "c": i}); trend, ext = 1, i
            elif p <= vals[hi] * (1 - theta) and hi < i:
                piv.append({"i": hi, "t": "H", "p": vals[hi], "c": i}); trend, ext = -1, i
        elif trend == 1:
            if p >= vals[ext]:
                ext = i
            elif p <= vals[ext] * (1 - theta):
                piv.append({"i": ext, "t": "H", "p": vals[ext], "c": i}); trend, ext = -1, i
        else:
            if p <= vals[ext]:
                ext = i
            elif p >= vals[ext] * (1 + theta):
                piv.append({"i": ext, "t": "L", "p": vals[ext], "c": i}); trend, ext = 1, i
    return piv, trend, ext


def _retr(a, b, c):
    """b→c 回檔佔 a→b 的比例"""
    return abs(b - c) / abs(b - a) if b != a else 0


def classify(piv, k):
    """用第 k 個（含）以前已確認的高低點，判讀第 k 個轉折的波浪位置"""
    P = [x["p"] for x in piv[:k + 1]]
    t = piv[k]["t"]
    if t == "L":
        if k >= 4:
            L0, H1, L2, H3, L4 = P[k - 4:k + 1]
            if (L2 > L0 and H3 > H1 and L4 > H1 and (H3 - L2) >= 0.9 * (H1 - L0)
                    and 0.236 <= _retr(L2, H3, L4) <= 0.618):
                return "w5_start"
        if k >= 2:
            L0, H1, L2 = P[k - 2:k + 1]
            new_trend = k < 4 or L0 < P[k - 4]          # 第1浪起點是新低（跌勢剛結束）
            if L2 > L0 and new_trend and 0.382 <= _retr(L0, H1, L2) <= 0.786:
                return "w3_start"
        if k >= 3:
            H0, L1, H2, L3 = P[k - 3:k + 1]
            if (H2 < H0 and L3 < L1 and 0.382 <= _retr(H0, L1, H2) <= 0.886
                    and (H2 - L3) >= 0.618 * (H0 - L1)):
                return "abc_end"
        return "low_other"
    if k >= 5:
        L0, H1, L2, H3, L4, H5 = P[k - 5:k + 1]
        if L2 > L0 and H3 > H1 and L4 > H1 and H5 > H3 and (H3 - L2) >= 0.9 * (H1 - L0):
            return "w5_top"
    if k >= 3:
        L0, H1, L2, H3 = P[k - 3:k + 1]
        if L2 > L0 and H3 > H1 and (H3 - L2) >= (H1 - L0):
            return "w3_top"
    if k >= 2:
        H0, L1, H2 = P[k - 2:k + 1]
        if H2 < H0 and 0.382 <= _retr(H0, L1, H2) <= 0.886:
            return "b_top"
    return "high_other"


def signals(dates, vals, theta):
    """每個確認的高低點 → 一筆訊號（確認日的「隔一個交易日」收盤進場）"""
    piv, _, _ = zigzag(vals, theta)
    out = []
    for k, p in enumerate(piv):
        entry = p["c"] + 1
        if entry >= len(dates):
            continue
        out.append({"state": classify(piv, k), "pivot_date": dates[p["i"]], "confirm_date": dates[p["c"]],
                    "entry_date": dates[entry], "entry_i": entry})
    return out


# ── 統計 ──────────────────────────────────────────────
def fwd_stats(lev, idxs, h):
    rets, mdds = [], []
    for i in idxs:
        if i + h < len(lev) and lev[i] > 0:
            rets.append((lev[i + h] / lev[i] - 1) * 100)
            mdds.append((min(lev[i:i + h + 1]) / lev[i] - 1) * 100)
    if not rets:
        return {"n": 0}
    s = sorted(rets)
    return {"n": len(rets), "win": round(sum(1 for r in rets if r > 0) / len(rets) * 100, 1),
            "avg": round(sum(rets) / len(rets), 1), "med": round(s[len(s) // 2], 1),
            "worst": round(s[0], 1), "best": round(s[-1], 1),
            "mdd": round(sum(mdds) / len(mdds), 1)}


def state_table(sigs, lev, idx_of):
    """各波浪位置的後續表現；idx_of：訊號 → 在 lev 序列的索引（None＝不在資料範圍）"""
    table = {}
    for st in STATES:
        idxs = [idx_of(s) for s in sigs if s["state"] == st]
        idxs = [i for i in idxs if i is not None]
        table[st] = {str(h): fwd_stats(lev, idxs, h) for h in HORIZONS}
        table[st]["count"] = len(idxs)
    return table


def baseline(lev, start=0, step=5):
    """任何一天買進（每 5 天取一次）的後續表現，當作比較基準"""
    idxs = list(range(start, len(lev), step))
    return {str(h): fwd_stats(lev, idxs, h) for h in HORIZONS}


def perf(dates, vals, days):
    if len(vals) <= days:
        return None
    return round((vals[-1] / vals[-1 - days] - 1) * 100, 1)


def etf_profile(etf, rows, u_dates, u_vals):
    d, v = series_from_prices(rows)
    if len(d) < 30:
        return None, None, None
    last = sorted(rows, key=lambda r: r["date"])[-20:]
    vol = sum(_f(r.get("Trading_Volume")) for r in last) / len(last) / 1000
    pos = {x: i for i, x in enumerate(u_dates)}
    pairs = [(pos[x], j) for j, x in enumerate(d[-251:], start=len(d) - min(251, len(d))) if x in pos]
    beta = None
    if len(pairs) > 60:
        xs = [u_vals[i1] / u_vals[i0] - 1 for (i0, _), (i1, _) in zip(pairs, pairs[1:])]
        ys = [v[j1] / v[j0] - 1 for (_, j0), (_, j1) in zip(pairs, pairs[1:])]
        mx, my = sum(xs) / len(xs), sum(ys) / len(ys)
        vx = sum((x - mx) ** 2 for x in xs)
        beta = round(sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / vx, 2) if vx else None
    year = d[-1][:4]
    ytd_base = next((i for i, x in enumerate(d) if x[:4] == year), None)
    yr = d[-252:] if len(d) > 252 else d
    vv = v[-len(yr):]
    peak, mdd = vv[0], 0.0
    for x in vv:
        peak = max(peak, x)
        mdd = min(mdd, x / peak - 1)
    prof = {
        "code": etf["code"], "name": etf["name"], "group": etf["group"],
        "listed": d[0], "last_date": d[-1],
        "close": round(_f(sorted(rows, key=lambda r: r["date"])[-1]["close"]), 2),
        "ret_1m": perf(d, v, 21), "ret_3m": perf(d, v, 63), "ret_1y": perf(d, v, 252),
        "ret_ytd": round((v[-1] / v[ytd_base - 1] - 1) * 100, 1) if ytd_base else None,
        "avg_vol": round(vol), "beta": beta, "mdd_1y": round(mdd * 100, 1),
        "drag": calibrate_drag(u_dates, u_vals, d, v),
    }
    return prof, d, v


def current_status(dates, vals, theta, sigs, table):
    piv, trend, ext = zigzag(vals, theta)
    if not piv:
        return None
    last = piv[-1]
    st = classify(piv, len(piv) - 1)
    cur, ext_p = vals[-1], vals[ext]
    out = {
        "theta": theta, "date": dates[-1],
        "last_pivot": {"type": last["t"], "date": dates[last["i"]], "confirm_date": dates[last["c"]],
                       "state": st, "state_name": STATES[st][0]},
        "trend": "up" if trend == 1 else "down",
        "since_pivot_pct": round((cur / last["p"] - 1) * 100, 1),
        "from_extreme_pct": round((cur / ext_p - 1) * 100, 1),
        "days_since_signal": len(dates) - 1 - last["c"],
    }
    if trend == 1:   # 上漲段：跌到這裡會確認高點
        out["next_confirm"] = {"type": "H", "pct_from_now": round((ext_p * (1 - theta) / cur - 1) * 100, 1)}
        swing = (last["p"], ext_p)
    else:
        out["next_confirm"] = {"type": "L", "pct_from_now": round((ext_p * (1 + theta) / cur - 1) * 100, 1)}
        swing = (last["p"], ext_p)
    a, b = swing   # 目前這一段（上一個轉折點 → 目前極值）的費波南希回檔位置（相對目前價格 %）
    out["fib"] = {k: round(((b - (b - a) * r) / cur - 1) * 100, 1)
                  for k, r in (("0.382", 0.382), ("0.5", 0.5), ("0.618", 0.618))}
    out["state_stats"] = table.get(st)
    out["recent_pivots"] = [{"date": dates[p["i"]], "type": p["t"], "state": classify(piv, k),
                             "confirm_date": dates[p["c"]]}
                            for k, p in list(enumerate(piv))[-8:]]
    return out


def analyze_group(gkey, u_dates, u_vals, etf_series, drag):
    """一個指數群組（臺灣50 / 加權）的完整分析"""
    syn = synthetic_2x(u_vals, drag)
    res = {"key": gkey, **{k: GROUPS[gkey][k] for k in ("name", "index_name")},
           "start": u_dates[0], "end": u_dates[-1], "drag": drag, "thetas": {}}
    for th in THETAS:
        sigs = signals(u_dates, u_vals, th)
        tab_syn = state_table(sigs, syn, lambda s: s["entry_i"])
        tab_etf = {}
        for code, (ed, ev) in etf_series.items():
            def idx_of(s, ed=ed):
                j = bisect.bisect_left(ed, s["entry_date"])
                return j if j < len(ed) and ed[j] == s["entry_date"] else None
            tab_etf[code] = state_table(sigs, ev, idx_of)
        res["thetas"][str(th)] = {
            "synthetic": tab_syn,
            "etf": tab_etf,
            "signals": [{k: s[k] for k in ("state", "pivot_date", "entry_date")} for s in sigs],
            "current": current_status(u_dates, u_vals, th, sigs, tab_syn),
        }
    res["baseline_synthetic"] = baseline(syn)
    res["baseline_etf"] = {code: baseline(ev) for code, (ed, ev) in etf_series.items()}
    # 圖表：指數（較早用週線、近一年用日線，控制資料量）
    wk = list(range(0, max(0, len(u_dates) - 250), 5)) + list(range(max(0, len(u_dates) - 250), len(u_dates)))
    res["chart"] = {"dates": [u_dates[i] for i in wk], "values": [round(u_vals[i], 2) for i in wk]}
    return res


def win_z(win, n, base):
    """勝率是否顯著高於基準（二項檢定 z 值）；z ≥ 2.5 約等於只有 0.6% 機率是運氣"""
    if not n or base is None or base <= 0 or base >= 100:
        return 0.0
    p0 = base / 100
    return round((win / 100 - p0) / math.sqrt(p0 * (1 - p0) / n), 2)


def best_states(group_res, theta=DEFAULT_THETA, h="60", min_n=10, min_stable=3, min_z=2.5):
    """
    「較佳買點」要同時：勝率比「任何一天買進」高 ≥5 個百分點、樣本 ≥ min_n、
    統計上顯著（z ≥ min_z）、換不同 θ（6/8/10/12%）至少 min_stable 種也成立。
    （不同 θ 找到的大轉折多半相同，只看 θ 穩定不夠，所以另外要求顯著）
    """
    base = group_res["baseline_synthetic"][h].get("win") or 0
    base_avg = group_res["baseline_synthetic"][h].get("avg") or 0
    tab = group_res["thetas"][str(theta)]["synthetic"]
    out = []
    for st, row in tab.items():
        s = row[h]
        if STATES[st][1] != "buy" or s.get("n", 0) < min_n or s["win"] - base < 5:
            continue
        z = win_z(s["win"], s["n"], base)
        if z < min_z:
            continue
        stable = sum(1 for th in THETAS
                     if group_res["thetas"][str(th)]["synthetic"][st][h].get("n", 0) >= min_n
                     and (group_res["thetas"][str(th)]["synthetic"][st][h].get("win") or 0) - base >= 5)
        if stable >= min_stable:
            out.append({"state": st, "name": STATES[st][0], "win": s["win"], "avg": s["avg"], "n": s["n"],
                        "base_win": base, "base_avg": base_avg, "stable": f"{stable}/{len(THETAS)}", "z": z})
    return sorted(out, key=lambda x: -x["win"])


# ── 主流程 ────────────────────────────────────────────
def build():
    payload = {"generated": (datetime.utcnow() + timedelta(hours=8)).strftime("%Y-%m-%d %H:%M"),
               "thetas": THETAS, "default_theta": DEFAULT_THETA, "horizons": HORIZONS,
               "states": {k: {"name": v[0], "side": v[1]} for k, v in STATES.items()},
               "etfs": [], "groups": {}}
    under = {}
    for gk, g in GROUPS.items():
        rows = fm_get(g["dataset"], g["id"])
        d, v = (series_from_prices(rows) if g["dataset"] == "TaiwanStockPrice" else series_from_index(rows))
        if len(d) < 500 and gk == "taiex":     # 報酬指數拿不到 → 用 0050 代替
            log("⚠️ 加權報酬指數資料不足，改用 0050 代替")
            d, v = under.get("tw50", ([], []))
        under[gk] = (d, v)
        log(f"{g['name']}：{len(d)} 筆（{d[0] if d else '—'} ~ {d[-1] if d else '—'}）")

    etf_series = {"tw50": {}, "taiex": {}}
    for e in ETFS:
        rows = fm_get("TaiwanStockPrice", e["code"], "2014-01-01")
        u_d, u_v = under[e["group"]]
        prof, d, v = etf_profile(e, rows, u_d, u_v) if rows and u_d else (None, None, None)
        if prof:
            payload["etfs"].append(prof)
            etf_series[e["group"]][e["code"]] = (d, v)
            log(f"{e['code']} {e['name']}：{len(d)} 筆，收盤 {prof['close']}，年化成本 {prof['drag']}")
        else:
            log(f"⚠️ {e['code']} 沒有資料")

    for gk in GROUPS:
        d, v = under[gk]
        if len(d) < 500:
            log(f"⚠️ {gk} 資料不足，略過")
            continue
        drags = [p["drag"] for p in payload["etfs"] if p["group"] == gk and p.get("drag") is not None]
        drag = round(sum(drags) / len(drags), 4) if drags else DEFAULT_DRAG
        drag = min(max(drag, 0.0), 0.06)
        payload["groups"][gk] = analyze_group(gk, d, v, etf_series[gk], drag)
        payload["groups"][gk]["best"] = best_states(payload["groups"][gk])
    return payload


def save(payload):
    if not SUPABASE_URL or not SUPABASE_KEY:
        log("未設定 Supabase，只輸出到 etf_wave_result.json")
        return False
    h = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}",
         "Content-Type": "application/json", "Prefer": "return=minimal"}
    url = f"{SUPABASE_URL}/rest/v1/etf_wave_results"
    today = (datetime.utcnow() + timedelta(hours=8)).strftime("%Y-%m-%d")
    try:
        requests.delete(url, params={"date": f"eq.{today}"}, headers=h, timeout=15)
        r = requests.post(url, json={"date": today, "payload": payload}, headers=h, timeout=60)
        if r.status_code in (200, 201):
            log("✅ 已存入 Supabase etf_wave_results")
            return True
        log(f"❌ 存入失敗 {r.status_code}（請先建立 etf_wave_results 表）: {r.text[:150]}")
    except Exception as e:
        log(f"❌ 存入例外: {e}")
    return False


def summary_for_report(payload):
    """給每日 Telegram 報告的精簡狀態"""
    out = {}
    for gk, g in payload.get("groups", {}).items():
        cur = g["thetas"][str(DEFAULT_THETA)].get("current") or {}
        lp = cur.get("last_pivot") or {}
        stats = (cur.get("state_stats") or {}).get("60", {})
        out[gk] = {"name": g["name"], "state": lp.get("state"), "state_name": lp.get("state_name"),
                   "pivot_type": lp.get("type"), "pivot_date": lp.get("date"),
                   "new_today": cur.get("days_since_signal") == 0,
                   "trend": cur.get("trend"), "since_pivot_pct": cur.get("since_pivot_pct"),
                   "next_confirm": cur.get("next_confirm"),
                   "win60": stats.get("win"), "avg60": stats.get("avg"), "n60": stats.get("n"),
                   "base_win60": g["baseline_synthetic"]["60"].get("win")}
    return out


def main():
    payload = build()
    with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "etf_wave_result.json"), "w",
              encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False)
    saved = save(payload)
    if notify:
        notify.record("wave", {"saved": saved, "groups": summary_for_report(payload)})
    for gk, g in payload["groups"].items():
        cur = g["thetas"][str(DEFAULT_THETA)]["current"] or {}
        log(f"──── {g['name']}：最近轉折 {cur.get('last_pivot', {}).get('date')} "
            f"{cur.get('last_pivot', {}).get('state_name')}｜目前{'上漲' if cur.get('trend') == 'up' else '下跌'}段"
            f"｜正2成本 {g['drag']:.2%}/年")
        for b in g.get("best", []):
            log(f"     較佳買點：{b['name']} 60日勝率 {b['win']}%（任意日 {b['base_win']}%）"
                f"平均 {b['avg']:+.1f}% n={b['n']} θ穩定 {b['stable']}")
        if not g.get("best"):
            log("     沒有「顯著且穩定」較佳的波浪買點")
        base = g["baseline_synthetic"]
        log(f"     {'位置':<16}" + "".join(f"{h:>4}日勝率/平均" for h in ("20", "60", "120")) + "   n(60)")
        for st, row in g["thetas"][str(DEFAULT_THETA)]["synthetic"].items():
            cells = "".join(f"{(row[h].get('win') or 0):>8.1f}%/{(row[h].get('avg') or 0):+6.1f}%" if row[h].get("n")
                            else f"{'—':>16}" for h in ("20", "60", "120"))
            log(f"     {STATES[st][0]:<16}{cells}   {row['60'].get('n', 0)}")
        log(f"     {'任何一天買進':<16}" + "".join(f"{base[h]['win']:>8.1f}%/{base[h]['avg']:+6.1f}%" for h in ("20", "60", "120")))


if __name__ == "__main__":
    main()
