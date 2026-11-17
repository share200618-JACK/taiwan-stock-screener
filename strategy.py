"""
AI 選股條件（門檻＋排序）與「每月自動調整」
=============================================
純計算模組，不連網。每日排程（auto_analysis.py）用它決定推薦哪些股票；
績效計算（performance.py）用同一份邏輯回測不同條件，確保「模擬的」＝「實際推薦的」。

可調整的條件（共 18 組；範圍刻意收窄，避免在雜訊裡挑出「剛好最好」的組合）
  min_prob : 漲機率門檻（%）             50 / 60
  min_edge : 勝率優勢門檻（百分點，需大於） 0 / 3 / 6
  rank     : 排序方式  prob_conf＝漲機率×0.6＋信心度×0.4（原本的） / edge＝勝率優勢 / precision＝歷史命中率
  min_support 固定 5（驗證期至少喊漲 5 次，數字才有意義）

自動調整規則（每月最多一次）——目標：提高「勝率」（前 5 名持有 20 日後賺錢的比例），但不犧牲報酬
  1. 每天把所有分析過的股票（不只推薦的）存下來，追蹤 20 個交易日（約一個月）後的報酬
  2. 已到期滿 MIN_DAYS 天後，用這些資料模擬每組條件「每天選前 5 名」的勝率與超額報酬（扣 0050）
  3. 勝率最高的組合必須同時：勝率比現行高 ≥ MIN_WIN_GAIN 個百分點、逐日配對 t ≥ MIN_T（同一天比，
     扣掉大盤共同漲跌）、前後兩半期間勝率都贏現行、平均超額報酬不比現行少超過 MAX_EXCESS_DROP、
     樣本 ≥ MIN_PICKS 才會換；否則維持現行。距上次調整未滿 COOLDOWN 個交易日不評估。
  4. 使用者可在回測頁「鎖定」條件（不再自動調整）或「恢復預設」。
"""

DEFAULT = {"min_prob": 50, "min_edge": 0, "min_support": 5, "rank": "prob_conf", "locked": False}
GRID = {
    "min_prob": [50, 60],
    "min_edge": [0, 3, 6],
    "rank":     ["prob_conf", "edge", "precision"],
}
RANK_NAMES = {"prob_conf": "漲機率＋信心度", "edge": "勝率優勢", "precision": "歷史命中率"}

TOP_PICKS = 5       # 追蹤與評估的是「每天前 5 名」
HOLD      = 20      # 追蹤 20 個交易日（約一個月）
MIN_DAYS  = 20      # 至少 20 個已到期交易日才評估（每組條件約 100 筆樣本）
MIN_PICKS = 60
MIN_WIN_GAIN    = 5.0   # 勝率至少高 5 個百分點才換
MAX_EXCESS_DROP = 0.5   # 平均超額報酬最多只能比現行少 0.5 個百分點（不為了勝率犧牲報酬）
MIN_T     = 2.2     # 逐日配對 t 值門檻：模擬 18 組一起比時，純雜訊誤換約 5%、強訊號 30 天抓到約 7 成
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
    if rank == "precision":
        return (s.get("precision", 0) or 0, s.get("edge", 0) or 0)
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


def _winrate(rets):
    return round(sum(1 for v in rets if v > 0) / len(rets) * 100, 1) if rets else None


def evaluate(days, cfg):
    """
    days: {日期: [候選股 dict（ret＝20 日報酬、excess＝扣 0050 的超額報酬）]}，只放已到期的日子
    回傳這組條件每天選前 5 名的成績：
      win＝勝率（賺錢比例）、mean＝平均超額報酬、ret＝平均報酬、beat＝贏過 0050 比例、
      h1/h2＝前後半期勝率
    """
    dates = sorted(days)
    half = len(dates) // 2
    rets, excs, h1, h2, daily = [], [], [], [], {}
    for i, d in enumerate(dates):
        sel, _ = select(days[d], cfg, TOP_PICKS)
        sel = [s for s in sel if s.get("ret") is not None]
        if not sel:
            continue
        r = [s["ret"] for s in sel]
        rets += r
        excs += [s["excess"] for s in sel if s.get("excess") is not None]
        (h1 if i < half else h2).extend(r)
        daily[d] = sum(1 for v in r if v > 0) / len(r)       # 當天前 5 名的勝率
    m = _mean(excs)
    return {
        "_daily": daily,
        "min_prob": cfg["min_prob"], "min_edge": cfg["min_edge"], "rank": cfg["rank"],
        "n": len(rets),
        "win": _winrate(rets),
        "mean": round(m, 2) if m is not None else None,
        "ret": round(_mean(rets), 2) if rets else None,
        "beat": _winrate(excs),
        "h1": _winrate(h1),
        "h2": _winrate(h2),
    }


def paired_t(a, b):
    """兩組條件「同一天」的前 5 名勝率相減，再看差異是否穩定（同一天比，扣掉大盤共同漲跌）"""
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
    table = [t for t in table if t["win"] is not None]
    table.sort(key=lambda t: (t["win"], t["mean"] if t["mean"] is not None else -99), reverse=True)
    for t in table:
        t["t"] = paired_t(t, cur) if cur["win"] is not None else 0.0
    base.update({"current_eval": _public(cur), "table": [_public(t) for t in table[:6]]})

    if days_since_change is not None and days_since_change < COOLDOWN:
        return {**base, "status": "cooldown",
                "message": f"上次調整後才 {days_since_change} 個交易日，滿 {COOLDOWN} 天才會再評估"}
    if not table or cur["win"] is None:
        return {**base, "status": "kept", "message": "到期資料不足以比較，維持現行條件"}

    best = table[0]
    best_cfg = normalize({**current, "min_prob": best["min_prob"], "min_edge": best["min_edge"],
                          "rank": best["rank"]})
    gain = round(best["win"] - cur["win"], 1)
    if same_rule(best_cfg, current):
        return {**base, "status": "kept", "message": f"現行條件勝率就是最高的（{cur['win']}%），維持"}
    reasons = []
    if best["n"] < MIN_PICKS:
        reasons.append(f"樣本只有 {best['n']} 筆（需 ≥ {MIN_PICKS}）")
    if gain < MIN_WIN_GAIN:
        reasons.append(f"勝率只高 {gain:+.1f} 個百分點（需 ≥ {MIN_WIN_GAIN:.0f}）")
    if best["t"] < MIN_T:
        reasons.append(f"逐日比較不夠穩定（t={best['t']}，需 ≥ {MIN_T}）")
    if None in (best["h1"], best["h2"], cur["h1"], cur["h2"]) or not (
            best["h1"] > cur["h1"] and best["h2"] > cur["h2"]):
        reasons.append("前後兩段期間勝率沒有都贏現行（可能只是運氣）")
    if None not in (best["mean"], cur["mean"]) and best["mean"] < cur["mean"] - MAX_EXCESS_DROP:
        reasons.append(f"平均超額報酬會從 {cur['mean']:+.2f}% 降到 {best['mean']:+.2f}%（勝率高但賺得少）")
    if reasons:
        return {**base, "status": "kept",
                "message": f"勝率最高的組合（{best['win']}% vs 現行 {cur['win']}%）未達調整標準，維持現行："
                           + "；".join(reasons)}
    return {**base, "status": "changed", "new": best_cfg, "gain": gain,
            "message": (f"改用「{describe(best_cfg)}」：前 5 名持有 20 日勝率 {best['win']}%，"
                        f"比現行 {cur['win']}% 高 {gain:.1f} 個百分點（t={best['t']}），"
                        f"平均超額 {best['mean']:+.2f}%（現行 {cur['mean']:+.2f}%），前後兩段都較好")}
