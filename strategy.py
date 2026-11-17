"""
AI 選股條件（門檻＋排序）與「每月自動調整」
=============================================
純計算模組，不連網。每日排程（auto_analysis.py）用它決定推薦哪些股票；
績效計算（performance.py）用同一份邏輯回測不同條件，確保「模擬的」＝「實際推薦的」。

可調整的條件（範圍刻意收窄，避免在雜訊裡挑出「剛好最好」的組合）
  min_prob : 漲機率門檻（%）            50 / 55 / 60
  min_edge : 勝率優勢門檻（百分點，需大於） 0 / 3 / 6
  rank     : 排序方式  prob_conf＝漲機率×0.6＋信心度×0.4（原本的） / edge＝勝率優勢
  min_support 固定 5（驗證期至少喊漲 5 次，數字才有意義）

自動調整規則（每月最多一次）
  1. 每天把所有分析過的股票（不只推薦的）存下來，追蹤 20 個交易日（約一個月）後的報酬
  2. 已到期滿 MIN_DAYS 天後，用這些資料模擬每組條件「每天選前 5 名」的平均超額報酬（扣 0050）
  3. 最佳組合必須同時：比現行多 ≥ MIN_GAIN 個百分點、逐日配對 t ≥ MIN_T（扣掉每天大盤共同漲跌）、
     前後兩半期間都贏現行、樣本 ≥ MIN_PICKS 才會換；否則維持現行。距上次調整未滿 COOLDOWN 個交易日不評估。
  4. 使用者可在回測頁「鎖定」條件（不再自動調整）或「恢復預設」。
"""

DEFAULT = {"min_prob": 50, "min_edge": 0, "min_support": 5, "rank": "prob_conf", "locked": False}
GRID = {
    "min_prob": [50, 55, 60],
    "min_edge": [0, 3, 6],
    "rank":     ["prob_conf", "edge"],
}
RANK_NAMES = {"prob_conf": "漲機率＋信心度", "edge": "勝率優勢"}

TOP_PICKS = 5       # 追蹤與評估的是「每天前 5 名」
HOLD      = 20      # 追蹤 20 個交易日（約一個月）
MIN_DAYS  = 20      # 至少 20 個已到期交易日才評估（每組條件約 100 筆樣本）
MIN_PICKS = 60
MIN_GAIN  = 1.0     # 平均超額報酬至少多 1 個百分點才換
MIN_T     = 2.5     # 逐日配對比較的 t 值門檻（18 組一起比，門檻要比一般的 2 高，避免挑到運氣）
COOLDOWN  = 20      # 兩次調整至少隔 20 個交易日（每月最多一次）


def normalize(cfg):
    """補齊缺的欄位、型別；不合法的值退回預設"""
    out = dict(DEFAULT)
    for k, v in (cfg or {}).items():
        if k in ("min_prob", "min_edge", "min_support"):
            try:
                out[k] = float(v)
                if out[k] == int(out[k]):
                    out[k] = int(out[k])
            except (TypeError, ValueError):
                pass
        elif k == "rank" and v in RANK_NAMES:
            out[k] = v
        elif k == "locked":
            out[k] = bool(v)
    return out


def describe(cfg):
    c = normalize(cfg)
    return (f"漲機率 ≥ {c['min_prob']}%、勝率優勢 > {c['min_edge']}、"
            f"依「{RANK_NAMES[c['rank']]}」排序")


def same_rule(a, b):
    a, b = normalize(a), normalize(b)
    return all(a[k] == b[k] for k in ("min_prob", "min_edge", "min_support", "rank"))


def rank_score(s, rank):
    if rank == "edge":
        return (s.get("edge", 0) or 0, s.get("rise_prob", 0) or 0)
    return ((s.get("rise_prob", 0) or 0) * 0.6 + (s.get("confidence", 0) or 0) * 0.4,)


def qualifies(s, cfg):
    return ((s.get("rise_prob", 0) or 0) >= cfg["min_prob"]
            and (s.get("edge", 0) or 0) > cfg["min_edge"]
            and (s.get("support", 0) or 0) >= cfg["min_support"])


def select(cands, cfg, n=None):
    """
    與每日排程完全相同的選股：先排序，再取通過門檻的；
    一支都沒通過時退回不設門檻的排序（排程原本的行為）。
    回傳 (選出的清單, 是否有通過門檻)
    """
    cfg = normalize(cfg)
    ranked = sorted(cands, key=lambda s: rank_score(s, cfg["rank"]), reverse=True)
    qualified = [s for s in ranked if qualifies(s, cfg)]
    picked = qualified if qualified else ranked
    return (picked[:n] if n else picked), bool(qualified)


def all_configs():
    out = []
    for p in GRID["min_prob"]:
        for e in GRID["min_edge"]:
            for r in GRID["rank"]:
                out.append(normalize({"min_prob": p, "min_edge": e, "rank": r}))
    return out


def _mean(xs):
    return sum(xs) / len(xs) if xs else None


def evaluate(days, cfg):
    """
    days: {日期: [候選股 dict（含 excess＝20 日超額報酬，可能為 None）]}，只放已到期的日子
    回傳這組條件每天選前 5 名的成績：平均超額、勝率（贏 0050 比例）、前後半期平均
    """
    dates = sorted(days)
    half = len(dates) // 2
    picks, h1, h2, daily = [], [], [], {}
    for i, d in enumerate(dates):
        sel, _ = select(days[d], cfg, TOP_PICKS)
        vals = [s["excess"] for s in sel if s.get("excess") is not None]
        picks += vals
        (h1 if i < half else h2).extend(vals)
        if vals:
            daily[d] = sum(vals) / len(vals)
    m = _mean(picks)
    return {
        "_daily": daily,
        "min_prob": cfg["min_prob"], "min_edge": cfg["min_edge"], "rank": cfg["rank"],
        "n": len(picks),
        "mean": round(m, 2) if m is not None else None,
        "win": round(sum(1 for v in picks if v > 0) / len(picks) * 100, 1) if picks else None,
        "h1": round(_mean(h1), 2) if h1 else None,
        "h2": round(_mean(h2), 2) if h2 else None,
    }


def paired_t(a, b):
    """兩組條件「同一天」的前 5 名平均超額相減，再看差異是否穩定（扣掉每天共同的大盤漲跌）"""
    diffs = [a["_daily"][d] - b["_daily"][d] for d in a["_daily"] if d in b["_daily"]]
    if len(diffs) < 3:
        return 0.0
    m = sum(diffs) / len(diffs)
    var = sum((x - m) ** 2 for x in diffs) / (len(diffs) - 1)
    if var == 0:
        return 99.0 if m > 0 else 0.0
    return round(m / (var / len(diffs)) ** 0.5, 2)


def _public(t):
    return {k: v for k, v in t.items() if not k.startswith("_")}


def decide(current, days, days_since_change):
    """
    current           : 現行條件
    days              : 已到期的候選股資料（見 evaluate）
    days_since_change : 距上次調整的交易日數（None＝從未調整）
    回傳 {"status", "message", "new"（要換才有）, "table", "current_eval"}
    """
    current = normalize(current)
    n_days = len(days)
    base = {"matured_days": n_days, "need_days": MIN_DAYS, "current": current}
    if current.get("locked"):
        return {**base, "status": "locked", "message": "條件已鎖定，不自動調整", "table": []}
    if n_days < MIN_DAYS:
        return {**base, "status": "collecting", "table": [],
                "message": f"資料累積中：已到期 {n_days}/{MIN_DAYS} 個交易日（每天的前 5 名要追蹤滿 {HOLD} 個交易日）"}

    cur = evaluate(days, current)
    table = [evaluate(days, c) for c in all_configs()]
    table = [t for t in table if t["mean"] is not None]
    table.sort(key=lambda t: t["mean"], reverse=True)
    for t in table:
        t["t"] = paired_t(t, cur) if cur["mean"] is not None else 0.0
    base.update({"current_eval": _public(cur), "table": [_public(t) for t in table[:6]]})

    if days_since_change is not None and days_since_change < COOLDOWN:
        return {**base, "status": "cooldown",
                "message": f"上次調整後才 {days_since_change} 個交易日，滿 {COOLDOWN} 天才會再評估"}
    if not table or cur["mean"] is None:
        return {**base, "status": "kept", "message": "到期資料不足以比較，維持現行條件"}

    best = table[0]
    best_cfg = normalize({**current, "min_prob": best["min_prob"], "min_edge": best["min_edge"],
                          "rank": best["rank"]})
    gain = round(best["mean"] - cur["mean"], 2)
    if same_rule(best_cfg, current):
        return {**base, "status": "kept", "message": f"現行條件就是最好的（平均超額 {cur['mean']:+.2f}%），維持"}
    reasons = []
    if best["n"] < MIN_PICKS:
        reasons.append(f"樣本只有 {best['n']} 筆（需 ≥ {MIN_PICKS}）")
    if gain < MIN_GAIN:
        reasons.append(f"只比現行多 {gain:+.2f} 個百分點（需 ≥ {MIN_GAIN}）")
    if best["t"] < MIN_T:
        reasons.append(f"逐日比較不夠穩定（t={best['t']}，需 ≥ {MIN_T}）")
    if None in (best["h1"], best["h2"], cur["h1"], cur["h2"]) or not (
            best["h1"] > cur["h1"] and best["h2"] > cur["h2"]):
        reasons.append("前後兩段期間沒有都贏現行（可能只是運氣）")
    if reasons:
        return {**base, "status": "kept",
                "message": "最佳組合未達調整標準，維持現行：" + "；".join(reasons)}
    return {**base, "status": "changed", "new": best_cfg, "gain": gain,
            "message": (f"改用「{describe(best_cfg)}」：前 5 名 20 日平均超額 {best['mean']:+.2f}%，"
                        f"比現行 {cur['mean']:+.2f}% 多 {gain:.2f} 個百分點（t={best['t']}），前後兩段都較好")}
