"""
月營收訊號：營收創 12 個月新高且年增 ≥ 20%
==========================================
個股勝率實驗室（signal_lab.py，189 支隨機抽樣、2019~2026）10 種邏輯裡唯一「傾向較好」的：
  出現後 20 日勝率 45.5%（基準 41.3%）、60 日平均超額 +3.8%，前後兩段期間都較好。
AI 選股把它當作「排序優先」的選項（strategy.py 的 rev），由每月自動調整用實際成績決定要不要開。

時間點與實驗室一致：M 月營收視為 M+1 月 11 日起才知道（法規 10 日前公布）。
快取 rev_cache.json（GitHub Actions cache 保存）：每支股票每月只要抓一次 FinMind。
"""
import json
import os
from datetime import datetime

CACHE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "rev_cache.json")
DATASET = "TaiwanStockMonthRevenue"
HOT_YOY = 20.0          # 年增門檻（%）
KEEP_MONTHS = 30        # 快取保留月數（年增要前一年、新高要前 11 個月，再留給回溯判斷）

_cache = {}


def _ym(y, m):
    return f"{y}-{m:02d}"


def _shift(y, m, k):
    t = y * 12 + (m - 1) + k
    return t // 12, t % 12 + 1


def expected_month(asof):
    """asof 當天（YYYY-MM-DD）已經可以知道的最新營收月份 (y, m)"""
    d = datetime.strptime(asof[:10], "%Y-%m-%d")
    return _shift(d.year, d.month, -1 if d.day >= 11 else -2)


def load():
    global _cache
    try:
        with open(CACHE_FILE, encoding="utf-8") as f:
            _cache = json.load(f) or {}
    except Exception:
        _cache = {}
    return len(_cache)


def save():
    try:
        with open(CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(_cache, f)
        return True
    except Exception as e:
        print(f"   ⚠️ 營收快取存檔失敗: {e}", flush=True)
        return False


def months_from_rows(rows):
    out = {}
    for r in rows or []:
        try:
            y, m, v = int(float(r.get("revenue_year"))), int(float(r.get("revenue_month"))), float(r.get("revenue"))
        except (TypeError, ValueError):
            continue
        if y and 1 <= m <= 12 and v > 0:
            out[_ym(y, m)] = v
    return out


def need_fetch(code, today):
    e = _cache.get(code)
    if not e:
        return True
    return _ym(*expected_month(today)) not in e.get("m", {}) and e.get("checked") != today


def ensure(code, today, fetch):
    """
    快取裡沒有今天該知道的最新月份 → 抓一次（同一天同一支只抓一次，公司晚公布就隔天再試）
    fetch(dataset, code, start_date) → list 或 None（失敗/額度用盡）
    """
    if not need_fetch(code, today):
        return False
    y, m = expected_month(today)
    sy, sm = _shift(y, m, -15)
    rows = fetch(DATASET, code, f"{sy}-{sm:02d}-01")
    if rows is None:
        return False
    e = _cache.setdefault(code, {"m": {}})
    e["m"].update(months_from_rows(rows))
    cut = _ym(*_shift(y, m, -KEEP_MONTHS))
    e["m"] = {k: v for k, v in e["m"].items() if k >= cut}
    e["checked"] = today
    return True


def status_from_months(months, asof):
    """
    asof 當天可知的最新月份是否「創 12 個月新高且年增 ≥ 20%」
    回傳 {"rev_month", "rev_yoy", "rev_high", "rev_hot"}；那個月還沒公布 → rev_hot False
    """
    y, m = expected_month(asof)
    cur = months.get(_ym(y, m))
    if not cur:
        return {"rev_month": None, "rev_yoy": None, "rev_high": False, "rev_hot": False}
    last = months.get(_ym(y - 1, m))
    yoy = round((cur / last - 1) * 100, 1) if last else None
    prior = [months.get(_ym(*_shift(y, m, -k))) for k in range(1, 12)]
    high = all(prior) and cur > max(prior)
    return {"rev_month": _ym(y, m), "rev_yoy": yoy, "rev_high": bool(high),
            "rev_hot": bool(high and yoy is not None and yoy >= HOT_YOY)}


def status(code, asof):
    """快取裡沒有這支 → None（不知道）"""
    e = _cache.get(str(code))
    if not e:
        return None
    return status_from_months(e.get("m", {}), asof)
