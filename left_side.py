"""
籌碼左側策略（散戶退場、法人承接）
====================================
純計算模組，不連網、不讀寫資料庫。每日排程（auto_analysis.py）與網站（server.py）
共用同一份規則，確保「驗證的」和「實際推薦的」是同一件事。

進場條件（決策點：當日收盤後）
  1. 近 20 個交易日跌幅 ≥ 10%               → 已經跌一段（左側）
  2. 融資餘額 10 日減少 ≥ 10%               → 散戶退場、籌碼沉澱
  3. 外資＋投信 近 10 日累計買超 > 0，且買超天數 ≥ 5 → 法人在跌勢中承接
  4. 收盤仍在 MA60 之下                       → 還沒轉成右側
  5. 20 日均量 ≥ 300 張、當日跌幅未達跌停     → 避開冷門股與崩跌中的股票

法人、融資資料一律用「前一個交易日」：排程收盤後執行時，當天的法人（約 15~17 點）
與融資（約 21 點）尚未公布，回測與實盤用同一個時間點才不會偷看未來。

出場建議（給交易日誌與賣出提醒用）
  - 停損：進場價 −8%
  - 停利：回到 MA20 先出一半
  - 時間停損：持有滿 20 個交易日仍未反彈
"""
import bisect
from datetime import datetime

RULE = {
    "ret20_max":        -10.0,   # 近20日漲跌幅 ≤ -10%
    "margin_chg10_max": -10.0,   # 融資10日變化 ≤ -10%
    "inst_days_min":    5,       # 近10日法人買超天數 ≥ 5
    "min_avg_vol":      300,     # 20日均量（張）
    "limit_down":       -9.5,    # 當日跌幅低於此視為跌停，排除
}
EXIT = {"stop_pct": 8, "time_stop_days": 20}
WARMUP = 60   # 需要 MA60


def back_adjust(records):
    """
    還原權值（與 auto_analysis.back_adjust 相同邏輯）。
    參考價 = 收盤 − 漲跌價差；若參考價 ≠ 前一日收盤 → 除權息/減資，往前等比調整。
    records 需依日期排序、含 chg（漲跌價差，未知為 None）。
    """
    n = len(records)
    out = [dict(r) for r in records]
    cum = 1.0
    for t in range(n - 1, -1, -1):
        if cum != 1.0:
            for k in ("open", "high", "low", "close"):
                if k in out[t]:
                    out[t][k] = round(out[t][k] * cum, 4)
        if t == 0:
            break
        cur, prev = records[t], records[t - 1]
        chg = cur.get("chg")
        if chg is None or prev["close"] <= 0:
            continue
        try:
            gap = (datetime.strptime(cur["date"], "%Y-%m-%d") -
                   datetime.strptime(prev["date"], "%Y-%m-%d")).days
        except Exception:
            gap = 1
        if gap > 10:
            continue
        ref = cur["close"] - chg
        if ref <= 0:
            continue
        f = ref / prev["close"]
        if abs(f - 1) > 0.001 and 0.2 < f < 5:
            cum *= f
    return out


def _lag_idx(dates, cur_date, max_stale_days=10):
    """cur_date「之前」（不含當天）最近一筆的索引；資料過舊回 None"""
    idx = bisect.bisect_left(dates, cur_date) - 1
    if idx < 0:
        return None
    try:
        gap = (datetime.strptime(cur_date, "%Y-%m-%d") -
               datetime.strptime(dates[idx], "%Y-%m-%d")).days
        if gap > max_stale_days:
            return None
    except Exception:
        pass
    return idx


def compute_series(records, inst_hist=None, margin_hist=None):
    """
    逐日計算左側指標與是否符合進場條件。
      records     : 已還原權值、依日期排序的日K（date, close, vol, ...；vol 單位：張）
      inst_hist   : (日期list, {date: (外資淨買張, 投信淨買張)}) 或 None
      margin_hist : (日期list, {date: (融資餘額, 融資限額)}) 或 None
    回傳與 records 等長的 list；前 WARMUP 天為 None，其餘為 dict。
    """
    n = len(records)
    out = [None] * n
    if n <= WARMUP:
        return out
    closes = [r["close"] for r in records]
    vols = [r.get("vol", 0) or 0 for r in records]

    # Wilder RSI(6)
    rsi6 = [None] * n
    avg_g = avg_l = 0.0
    for i in range(1, n):
        ch = closes[i] - closes[i - 1]
        g, l = max(ch, 0), max(-ch, 0)
        if i <= 6:
            avg_g += g / 6
            avg_l += l / 6
            if i < 6:
                continue
        else:
            avg_g = (avg_g * 5 + g) / 6
            avg_l = (avg_l * 5 + l) / 6
        rsi6[i] = 100.0 if avg_l == 0 else 100 - 100 / (1 + avg_g / avg_l)

    # 前綴和算均線/均量
    pc, pv = [0.0], [0.0]
    for c, v in zip(closes, vols):
        pc.append(pc[-1] + c)
        pv.append(pv[-1] + v)

    idates = inst_hist[0] if inst_hist else None
    idata = inst_hist[1] if inst_hist else None
    mdates = margin_hist[0] if margin_hist else None
    mdata = margin_hist[1] if margin_hist else None

    for i in range(WARMUP, n):
        c = closes[i]
        ma20 = (pc[i + 1] - pc[i - 19]) / 20
        ma60 = (pc[i + 1] - pc[i - 59]) / 60
        avg_vol20 = (pv[i + 1] - pv[i - 19]) / 20
        ret20 = (c / closes[i - 20] - 1) * 100 if closes[i - 20] > 0 else None
        pct1 = (c / closes[i - 1] - 1) * 100 if closes[i - 1] > 0 else 0
        d = records[i]["date"]

        m = {
            "date": d, "close": round(c, 2),
            "ma20": round(ma20, 2), "ma60": round(ma60, 2),
            "ret20": round(ret20, 2) if ret20 is not None else None,
            "bias20": round((c / ma20 - 1) * 100, 2) if ma20 > 0 else None,
            "rsi6": round(rsi6[i], 1) if rsi6[i] is not None else None,
            "avg_vol20": round(avg_vol20),
            "pct1": round(pct1, 2),
            "margin_usage": None, "margin_chg10": None,
            "inst_sum10": None, "inst_days10": None, "inst_pct10": None,
            "foreign5": None,
        }

        if mdates:
            mi = _lag_idx(mdates, d)
            if mi is not None:
                bal, lim = mdata[mdates[mi]]
                m["margin_usage"] = round(bal / lim * 100, 2) if lim > 0 else None
                if mi >= 10:
                    bal10 = mdata[mdates[mi - 10]][0]
                    if bal10 > 0:
                        m["margin_chg10"] = round((bal / bal10 - 1) * 100, 2)

        if idates:
            ii = _lag_idx(idates, d)
            if ii is not None and ii >= 9:
                win = [idata[idates[j]] for j in range(ii - 9, ii + 1)]
                nets = [f + t for f, t in win]
                m["inst_sum10"] = round(sum(nets))
                m["inst_days10"] = sum(1 for x in nets if x > 0)
                if avg_vol20 > 0:
                    m["inst_pct10"] = round(sum(nets) / (avg_vol20 * 10) * 100, 2)
                m["foreign5"] = round(sum(f for f, _ in win[-5:]))

        reasons_fail = []
        if m["ret20"] is None or m["ret20"] > RULE["ret20_max"]:
            reasons_fail.append("ret20")
        if m["margin_chg10"] is None or m["margin_chg10"] > RULE["margin_chg10_max"]:
            reasons_fail.append("margin")
        if (m["inst_sum10"] is None or m["inst_sum10"] <= 0
                or (m["inst_days10"] or 0) < RULE["inst_days_min"]):
            reasons_fail.append("inst")
        if c >= ma60:
            reasons_fail.append("ma60")
        if avg_vol20 < RULE["min_avg_vol"]:
            reasons_fail.append("liquidity")
        if pct1 <= RULE["limit_down"]:
            reasons_fail.append("limit_down")
        m["hit"] = not reasons_fail
        m["fail"] = reasons_fail
        out[i] = m
    return out


def factor_value(metrics, factor):
    """從當日指標取出單一因子值（缺值回 None）"""
    if not metrics:
        return None
    if factor == "left_chip":
        # 規則需要的資料都齊全才算一筆樣本，避免資料缺漏被當成「不符合」
        if metrics["margin_chg10"] is None or metrics["inst_sum10"] is None:
            return None
        return 1.0 if metrics["hit"] else 0.0
    if factor == "inst_in_decline":
        # 只在「近20日跌≥5%」的股票上定義：跌勢中法人買超佔成交量比例
        if metrics["ret20"] is None or metrics["ret20"] > -5:
            return None
        return metrics["inst_pct10"]
    key = {"bias20": "bias20", "rsi6": "rsi6", "margin_chg10": "margin_chg10",
           "margin_usage": "margin_usage", "foreign_net": "foreign5"}.get(factor)
    return metrics.get(key) if key else None


def pick(metrics, code, name, price=None):
    """把符合條件的當日指標整理成推薦輸出（含出場建議）"""
    p = price or metrics["close"]
    return {
        "code": code, "name": name, "price": p, "date": metrics["date"],
        "ret20": metrics["ret20"], "margin_chg10": metrics["margin_chg10"],
        "margin_usage": metrics["margin_usage"],
        "inst_sum10": metrics["inst_sum10"], "inst_days10": metrics["inst_days10"],
        "inst_pct10": metrics["inst_pct10"],
        "bias20": metrics["bias20"], "rsi6": metrics["rsi6"],
        "ma20": metrics["ma20"], "ma60": metrics["ma60"],
        "stop": round(p * (1 - EXIT["stop_pct"] / 100), 2),
        "target": metrics["ma20"],
        "time_stop_days": EXIT["time_stop_days"],
    }


def rank_key(p):
    """排序：法人買超佔成交量比例越高越前面"""
    return p.get("inst_pct10") or 0
