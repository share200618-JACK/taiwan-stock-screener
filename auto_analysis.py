"""
台股自動選股 + 強化版隨機森林分析
=====================================
v2.0 改進：
  ✅ 歷史資料從 8 個月延長到 2 年
  ✅ 加入外資投信籌碼特徵
  ✅ 加入大盤相對強弱
  ✅ 加入量價型態（連漲天數、紅K比例、價格位置）
  ✅ 特徵從 16 個增加到 28 個
  ✅ 還原權值（保留漲跌停日）、驗證隔離帶、勝率優勢門檻
  ✅ 加入不平衡資料處理
  ✅ 結果存成 JSON，供網頁顯示（不需要 Telegram）

使用方式：
  - 直接執行：python3 auto_analysis.py
  - PythonAnywhere 排程：每天 14:35 執行一次（收盤後）
  - 結果存在 analysis_result.json，由 server.py 讀取

安裝套件（只需一次）：
  pip3 install requests --user
"""

import requests
import urllib3
import json
import os
import random
from datetime import datetime, timedelta
from collections import defaultdict

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ══════════════════════════════════════════════════
# ★ 設定區
# ══════════════════════════════════════════════════

TOP_N          = 20    # 最多推薦幾支
PREDICT_DAYS   = 15    # 預測幾個交易日後（約 3 週）
RISE_THRESHOLD = 3.0   # 漲超過 3% 算「上漲」
HISTORY_MONTHS = 24    # 抓幾個月歷史資料（2 年）

# 結果存檔路徑（server.py 會讀這個檔案）
RESULT_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "analysis_result.json")

# Supabase 設定（從環境變數讀取，GitHub Actions / Render 都適用）
SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")
FINMIND_TOKEN = os.environ.get("FINMIND_TOKEN", "")  # 融資特徵用（GitHub Actions 有傳入）

def _sb_headers():
    return {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal",
    }

def save_to_supabase(data):
    """把 AI 分析結果寫進 Supabase analysis_results（同日 upsert）"""
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("[Supabase] 未設定 SUPABASE_URL / SUPABASE_KEY，跳過")
        return
    try:
        today = (datetime.utcnow() + timedelta(hours=8)).strftime("%Y-%m-%d")
        url   = f"{SUPABASE_URL}/rest/v1/analysis_results"
        # 先刪同日舊資料
        requests.delete(url, params={"date": f"eq.{today}"},
                        headers=_sb_headers(), timeout=10)
        payload = {
            "date":           today,
            "time":           data.get("time", ""),
            "model_ver":      data.get("model_ver", "v2"),
            "total_scanned":  data.get("total_scanned", 0),
            "total_analyzed": data.get("total_analyzed", 0),
            "avg_accuracy":   data.get("avg_accuracy", 0),
            "bullish":        data.get("bullish", 0),
            "bearish":        data.get("bearish", 0),
            "stocks":         json.dumps(data.get("stocks", []), ensure_ascii=False),
        }
        r = requests.post(url, json=payload, headers=_sb_headers(), timeout=10)
        if r.status_code in (200, 201):
            print(f"[Supabase] ✅ analysis_results 已存入 {len(data.get('stocks',[]))} 支推薦")
        else:
            print(f"[Supabase] ❌ 存入失敗 {r.status_code}: {r.text[:200]}")
    except Exception as e:
        print(f"[Supabase] ❌ 例外: {e}")

FILTER = {
    "min_price":   10,
    "max_price":   500,
    "min_vol_张":  500,
    "min_chg_pct": -9.5,
    "max_chg_pct": 9.5,
}

SESSION = requests.Session()
SESSION.verify = False
SESSION.headers.update({"User-Agent": "Mozilla/5.0"})

# ══════════════════════════════════════════════════
# 工具函式
# ══════════════════════════════════════════════════

def safe_float(val, default=0.0):
    try:
        v = str(val).replace(",", "").strip()
        if v in ("--", "", "N/A", "None"): return default
        return float(v)
    except:
        return default

def sma(arr, n):
    if not arr: return 0.0
    sl = arr[-n:] if len(arr) >= n else arr
    return sum(sl) / len(sl)

def ema(arr, n):
    if not arr: return 0.0
    e = arr[0]
    k = 2 / (n + 1)
    for v in arr[1:]:
        e = v * k + e * (1 - k)
    return e

def send_telegram(message):
    """保留此函式避免舊程式碼報錯，但不發送任何訊息"""
    pass

# ══════════════════════════════════════════════════
# 資料抓取
# ══════════════════════════════════════════════════

import time as _time
import bisect as _bisect

# ── FinMind 共用呼叫：節流 + 額度用盡自動改走備援 ──────────────
# 免費帶 token 約 600 次/小時；預設每次間隔 7 秒（≈514 次/小時），留額度給網站本身
FINMIND_MIN_INTERVAL = float(os.environ.get("FINMIND_MIN_INTERVAL", "7"))
_fm_state = {"last": 0.0, "calls": 0, "disabled": False}

def finmind_get(dataset, data_id, start_date, end_date=None):
    """回傳 data list；未設 token / 額度用盡 / 失敗 → None（呼叫端改走備援）"""
    if not FINMIND_TOKEN or _fm_state["disabled"]:
        return None
    wait = FINMIND_MIN_INTERVAL - (_time.time() - _fm_state["last"])
    if wait > 0:
        _time.sleep(wait)
    _fm_state["last"] = _time.time()
    _fm_state["calls"] += 1
    params = {"dataset": dataset, "data_id": data_id, "start_date": start_date}
    if end_date:
        params["end_date"] = end_date
    try:
        r = SESSION.get("https://api.finmindtrade.com/api/v4/data", params=params,
                        headers={"Authorization": f"Bearer {FINMIND_TOKEN}"}, timeout=20)
        body = {}
        try: body = r.json()
        except Exception: pass
        if r.status_code in (402, 429) or body.get("status") in (402, 429):
            _fm_state["disabled"] = True
            print(f"\n   ⚠️ FinMind 額度用盡（{r.status_code}），之後改用 TWSE 備援", end=" ")
            return None
        if r.status_code != 200:
            return None
        return body.get("data", []) or []
    except Exception:
        return None


def _tw_today():
    return (datetime.utcnow() + timedelta(hours=8)).strftime("%Y-%m-%d")


def back_adjust(records):
    """
    還原權值（取代舊版「單日 >9.5% 整天刪除」）。
    舊做法會把漲停/跌停日一起刪掉，最強的訊號日消失、序列也出現斷層。
    新做法：參考價 = 收盤 − 漲跌價差；若參考價 ≠ 前一日收盤，代表除權息/減資，
    把之前所有價格等比例調整，讓報酬率連續正確。
    records 需含 chg（漲跌價差；未知時為 None），依日期排序。
    """
    n = len(records)
    out = [dict(r) for r in records]
    cum = 1.0
    for t in range(n - 1, -1, -1):
        if cum != 1.0:
            for k in ("open", "high", "low", "close"):
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
        if gap > 10:          # 資料缺口，無法判斷，不調整
            continue
        ref = cur["close"] - chg
        if ref <= 0:
            continue
        f = ref / prev["close"]
        if abs(f - 1) > 0.001 and 0.2 < f < 5:
            cum *= f
    return out


def fetch_history(code, months=24):
    """
    個股日線（原始價 + 漲跌價差，尚未還原）。
    優先 FinMind（一次抓完、上市上櫃都有）；失敗才用 TWSE 逐月（只有上市）。
    """
    start = (datetime.utcnow() + timedelta(hours=8) - timedelta(days=months * 31)).strftime("%Y-%m-%d")
    rows = finmind_get("TaiwanStockPrice", code, start)
    records = []
    if rows:
        for r in rows:
            c = safe_float(r.get("close"))
            if c <= 0:
                continue
            records.append({
                "date":  r.get("date", "")[:10],
                "open":  safe_float(r.get("open"), c),
                "high":  safe_float(r.get("max"), c),
                "low":   safe_float(r.get("min"), c),
                "close": c,
                "vol":   round(safe_float(r.get("Trading_Volume")) / 1000),
                "chg":   safe_float(r.get("spread")),
            })
    if len(records) < 80:
        records = _fetch_history_twse(code, months)
    records.sort(key=lambda x: x["date"])
    return records


def _fetch_history_twse(code, months=24):
    """TWSE 逐月備援（僅上市）。除權息日 TWSE 會標 X，價差不可信 → chg=None 不調整"""
    records = []
    today = datetime.today()
    for i in range(months, -1, -1):
        d = today - timedelta(days=i * 31)
        ym = f"{d.year}{d.month:02d}01"
        try:
            url = (f"https://www.twse.com.tw/exchangeReport/STOCK_DAY"
                   f"?response=json&date={ym}&stockNo={code}")
            data = SESSION.get(url, timeout=10).json()
            if data.get("stat") != "OK" or not data.get("data"):
                continue
            for row in data["data"]:
                parts = row[0].split("/")
                if len(parts) != 3:
                    continue
                try:
                    dt = datetime(int(parts[0]) + 1911, int(parts[1]), int(parts[2]))
                except Exception:
                    continue
                c = safe_float(row[6])
                if c <= 0:
                    continue
                raw = str(row[7])
                chg = None if "X" in raw.upper() else safe_float(raw.replace("+", ""), None)
                records.append({
                    "date": dt.strftime("%Y-%m-%d"),
                    "open": safe_float(row[3], c), "high": safe_float(row[4], c),
                    "low":  safe_float(row[5], c), "close": c,
                    "vol":  round(safe_float(row[1]) / 1000), "chg": chg,
                })
        except Exception:
            pass
    seen, uniq = set(), []
    for r in sorted(records, key=lambda x: x["date"]):
        if r["date"] not in seen:
            seen.add(r["date"]); uniq.append(r)
    return uniq


def fetch_inst_history(code, start_date):
    """
    外資/投信每日買賣超（張），整段歷史一次抓，讓「訓練」與「預測」用同一份籌碼資料。
    回傳 (排序日期list, {date: (外資, 投信)})；抓不到回 None。
    """
    rows = finmind_get("TaiwanStockInstitutionalInvestorsBuySell", code, start_date)
    if not rows:
        return None
    daily = {}
    for r in rows:
        d = r.get("date", "")[:10]
        name = r.get("name", "")
        net = (safe_float(r.get("buy")) - safe_float(r.get("sell"))) / 1000
        f, t = daily.get(d, (0.0, 0.0))
        if name.startswith("Foreign"):
            f += net
        elif name == "Investment_Trust":
            t += net
        daily[d] = (f, t)
    if not daily:
        return None
    return sorted(daily), daily


def fetch_margin_history(code, start_date):
    """融資餘額/限額每日歷史。回傳 (排序日期list, {date: (餘額, 限額)})"""
    rows = finmind_get("TaiwanStockMarginPurchaseShortSale", code, start_date)
    if not rows:
        return None
    daily = {}
    for r in rows:
        d = r.get("date", "")[:10]
        daily[d] = (safe_float(r.get("MarginPurchaseTodayBalance")),
                    safe_float(r.get("MarginPurchaseLimit")))
    if not daily:
        return None
    return sorted(daily), daily


def _lagged_index(dates, cur_date, max_stale_days=10):
    """
    回傳 cur_date「之前」最近一筆的索引（嚴格小於，落後一日）。
    原因：排程收盤後就跑，當天的法人(約15~17點)與融資(約21點)資料還沒公布，
    訓練與預測都用「前一個交易日」才一致，避免偷看未來。
    """
    idx = _bisect.bisect_left(dates, cur_date) - 1
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


def fetch_market_index(months=24):
    """加權指數（相對強弱用）：優先 FinMind，失敗用 TWSE"""
    start = (datetime.utcnow() + timedelta(hours=8) - timedelta(days=months * 31)).strftime("%Y-%m-%d")
    rows = finmind_get("TaiwanStockTotalReturnIndex", "TAIEX", start)
    records = []
    if rows:
        for r in rows:
            p = safe_float(r.get("price"))
            if p > 0:
                records.append({"date": r.get("date", "")[:10], "close": p})
    if len(records) >= 60:
        return sorted(records, key=lambda x: x["date"])
    records = []
    today = datetime.today()
    for i in range(months, -1, -1):
        d = today - timedelta(days=i * 31)
        ym = f"{d.year}{d.month:02d}01"
        try:
            url = (f"https://www.twse.com.tw/exchangeReport/STOCK_DAY"
                   f"?response=json&date={ym}&stockNo=Y9999")
            data = SESSION.get(url, timeout=10).json()
            if data.get("stat") != "OK" or not data.get("data"):
                continue
            for row in data["data"]:
                parts = row[0].split("/")
                if len(parts) != 3:
                    continue
                try:
                    dt = datetime(int(parts[0]) + 1911, int(parts[1]), int(parts[2]))
                except Exception:
                    continue
                c = safe_float(row[6])
                if c > 0:
                    records.append({"date": dt.strftime("%Y-%m-%d"), "close": c})
        except Exception:
            pass
    return sorted(records, key=lambda x: x["date"])


_market_cache = None
_market_index = {}   # id(market_records) → (dates, closes)，避免每個樣本都重新排序

def get_market_data():
    global _market_cache
    if _market_cache is None:
        print("   抓取大盤加權指數...")
        _market_cache = fetch_market_index(HISTORY_MONTHS)
        print(f"   大盤資料：{len(_market_cache)} 筆")
    return _market_cache


def _market_lookup(market_records):
    key = id(market_records)
    if key not in _market_index:
        _market_index[key] = ([r["date"] for r in market_records],
                              [r["close"] for r in market_records])
    return _market_index[key]

# ══════════════════════════════════════════════════
# 特徵工程（28 個特徵）
# ══════════════════════════════════════════════════

def calc_features(records, market_records=None, inst_data=None, margin_data=None,
                  inst_hist=None, margin_hist=None):
    """
    計算 28 個技術 + 籌碼 + 大盤相對強弱特徵
    """
    if len(records) < 40:
        return None

    closes = [r["close"] for r in records]
    highs  = [r["high"]  for r in records]
    lows   = [r["low"]   for r in records]
    opens  = [r["open"]  for r in records]
    vols   = [r["vol"]   for r in records]
    i = len(records) - 1
    c = closes[i]

    # ── 均線特徵 ──────────────────────────────────
    ma5   = sma(closes, 5)
    ma10  = sma(closes, 10)
    ma20  = sma(closes, 20)
    ma60  = sma(closes, 60) if len(closes) >= 60 else sma(closes, len(closes))
    ma120 = sma(closes, 120) if len(closes) >= 120 else sma(closes, len(closes))

    f_ma5   = (c - ma5)   / ma5   * 100 if ma5   > 0 else 0
    f_ma20  = (c - ma20)  / ma20  * 100 if ma20  > 0 else 0
    f_ma60  = (c - ma60)  / ma60  * 100 if ma60  > 0 else 0
    f_ma120 = (c - ma120) / ma120 * 100 if ma120 > 0 else 0
    f_ma5_20 = (ma5 - ma20) / ma20 * 100 if ma20 > 0 else 0
    f_ma20_60= (ma20- ma60) / ma60 * 100 if ma60 > 0 else 0

    # ── KD ────────────────────────────────────────
    n = min(9, len(closes))
    rh = max(highs[-n:])
    rl = min(lows[-n:])
    rsv = 0 if rh == rl else (c - rl) / (rh - rl) * 100
    k, d = 50.0, 50.0
    for _ in range(5):
        k = k*2/3 + rsv*1/3
        d = d*2/3 + k*1/3

    # ── RSI ───────────────────────────────────────
    chgs = [closes[j+1]-closes[j] for j in range(len(closes)-1)]
    r14  = chgs[-14:] if len(chgs) >= 14 else chgs
    g14  = sum(x for x in r14 if x > 0) / max(len(r14), 1)
    l14  = sum(-x for x in r14 if x < 0) / max(len(r14), 1)
    rsi  = 100 - 100/(1+g14/l14) if l14 > 0 else 100

    # ── MACD ──────────────────────────────────────
    ema12 = ema(closes[-26:], 12)
    ema26 = ema(closes[-26:], 26)
    macd  = (ema12 - ema26) / c * 100 if c > 0 else 0

    # ── 布林通道位置 ───────────────────────────────
    ma20v = sma(closes[-20:], 20)
    std20 = (sum((x-ma20v)**2 for x in closes[-20:]) / 20) ** 0.5
    boll  = (c - (ma20v - 2*std20)) / (4*std20 + 0.001)

    # ── 成交量特徵 ────────────────────────────────
    avg5v  = sma(vols[:-1], 5)  if len(vols) > 5  else vols[-1]
    avg20v = sma(vols[:-1], 20) if len(vols) > 20 else vols[-1]
    vol_r5  = vols[-1] / avg5v  if avg5v  > 0 else 1
    vol_r20 = vols[-1] / avg20v if avg20v > 0 else 1

    # ── 近期漲幅 ──────────────────────────────────
    ret3  = (c/closes[-4]-1)*100  if len(closes) >= 4  else 0
    ret5  = (c/closes[-6]-1)*100  if len(closes) >= 6  else 0
    ret10 = (c/closes[-11]-1)*100 if len(closes) >= 11 else 0
    ret20 = (c/closes[-21]-1)*100 if len(closes) >= 21 else 0

    # ── ATR 波動率 ────────────────────────────────
    trs = [max(highs[j]-lows[j],
               abs(highs[j]-closes[j-1]),
               abs(lows[j]-closes[j-1]))
           for j in range(max(1, i-13), i+1)]
    atr = (sum(trs)/len(trs) if trs else 0) / c * 100 if c > 0 else 0

    # ── 量價型態特徵（新增）──────────────────────
    # 近5日紅K比例（收盤 > 開盤）
    red_k = sum(1 for j in range(max(0,i-4), i+1)
                if closes[j] >= opens[j]) / 5

    # 連漲天數
    consec_up = 0
    for j in range(i, max(-1, i-10), -1):
        if j == 0: break
        if closes[j] > closes[j-1]: consec_up += 1
        else: break

    # 今日K棒實體大小（相對ATR）
    body = abs(c - opens[i]) / (atr * c / 100 + 0.001)

    # 價格在近60日的相對位置（0=最低 1=最高）
    hi60 = max(highs[-60:])  if len(highs) >= 60  else max(highs)
    lo60 = min(lows[-60:])   if len(lows)  >= 60  else min(lows)
    price_pos = (c - lo60) / (hi60 - lo60 + 0.001)

    # ── 大盤相對強弱 ──────────────────────────────
    f_rel_strength = 0.0
    if market_records and len(market_records) >= 11:
        mdates, mcloses = _market_lookup(market_records)
        mi = _bisect.bisect_right(mdates, records[-1]["date"]) - 1   # 當日（含）以前最近一筆
        if mi >= 10 and mcloses[mi-10] > 0:
            mkt_ret10 = (mcloses[mi]/mcloses[mi-10]-1)*100
            f_rel_strength = ret10 - mkt_ret10

    # ── 籌碼特徵（外資/投信，落後一日以免偷看未來）──
    f_foreign_net = 0.0
    f_trust_net   = 0.0
    f_inst_total  = 0.0
    cur_date = records[-1]["date"]
    if inst_hist:
        idates, idata = inst_hist
        ii = _lagged_index(idates, cur_date)
        if ii is not None:
            fn, tn = idata[idates[ii]]
            f_foreign_net = fn / max(avg5v, 1) * 100
            f_trust_net   = tn / max(avg5v, 1) * 100
            s5 = sum(sum(idata[idates[j]]) for j in range(max(0, ii-4), ii+1))
            f_inst_total  = s5 / max(avg5v, 1) * 100 / 5       # 近5日法人平均買超強度
    elif inst_data:
        f_foreign_net = inst_data.get("foreign_net", 0) / max(avg5v, 1) * 100
        f_trust_net   = inst_data.get("trust_net",   0) / max(avg5v, 1) * 100
        f_inst_total  = f_foreign_net + f_trust_net

    # ── 融資特徵（使用率、近5日增減率；落後一日）──
    f_margin_usage = 0.0
    f_margin_chg   = 0.0
    if margin_hist:
        mdts, mdat = margin_hist
        mi2 = _lagged_index(mdts, cur_date)
        if mi2 is not None:
            bal, lim = mdat[mdts[mi2]]
            f_margin_usage = bal / lim * 100 if lim > 0 else 0
            if mi2 >= 5:
                bal5 = mdat[mdts[mi2-5]][0]
                f_margin_chg = (bal - bal5) / bal5 * 100 if bal5 > 0 else 0
    elif margin_data:
        f_margin_usage = margin_data.get("usage_pct", 0)
        f_margin_chg   = margin_data.get("margin_chg_5_pct", 0)

    # ── 組合成特徵向量（共 30 個）────────────────
    return [
        # 均線（6）
        f_ma5, f_ma20, f_ma60, f_ma120, f_ma5_20, f_ma20_60,
        # KD + RSI + MACD（4）
        k, d, rsi, macd,
        # 布林（1）
        boll,
        # 成交量（2）
        vol_r5, vol_r20,
        # 近期漲幅（4）
        ret3, ret5, ret10, ret20,
        # ATR（1）
        atr,
        # 量價型態（4）
        red_k, consec_up, body, price_pos,
        # 大盤相對強弱（1）
        f_rel_strength,
        # 籌碼（3）
        f_foreign_net, f_trust_net, f_inst_total,
        # 融資（2）
        f_margin_usage, f_margin_chg,
        # K值-D值差、RSI偏離50（2）
        k - d, rsi - 50,
    ]

# ══════════════════════════════════════════════════
# 隨機森林（強化版）
# ══════════════════════════════════════════════════

class DecisionTree:
    def __init__(self, max_depth=7, min_samples=4, n_features=None):
        self.max_depth   = max_depth
        self.min_samples = min_samples
        self.n_features  = n_features
        self.tree        = None

    def _gini(self, labels):
        n = len(labels)
        if n == 0: return 0
        counts = defaultdict(int)
        for l in labels: counts[l] += 1
        return 1 - sum((v/n)**2 for v in counts.values())

    def _best_split(self, X, y, weights):
        best_gain = -1
        best_feat, best_thresh = None, None
        n = len(y)
        gini_p = self._gini(y)

        feats = list(range(len(X[0])))
        if self.n_features and self.n_features < len(feats):
            feats = random.sample(feats, self.n_features)

        for f in feats:
            vals = sorted(set(x[f] for x in X))
            if len(vals) <= 1: continue
            # 只測試幾個分割點（加速）
            thresholds = []
            step = max(1, len(vals) // 10)
            for vi in range(0, len(vals)-1, step):
                thresholds.append((vals[vi] + vals[vi+1]) / 2)

            for thresh in thresholds:
                left_y  = [y[j] for j in range(n) if X[j][f] <= thresh]
                right_y = [y[j] for j in range(n) if X[j][f] >  thresh]
                if not left_y or not right_y: continue
                gain = gini_p - (
                    len(left_y)/n  * self._gini(left_y) +
                    len(right_y)/n * self._gini(right_y)
                )
                if gain > best_gain:
                    best_gain  = gain
                    best_feat  = f
                    best_thresh= thresh
        return best_feat, best_thresh

    def _build(self, X, y, weights, depth):
        counts = defaultdict(int)
        for l in y: counts[l] += 1
        majority = max(counts, key=counts.get)
        prob = counts.get(1, 0) / len(y)

        if depth >= self.max_depth or len(y) <= self.min_samples or len(counts) == 1:
            return {"leaf": True, "label": majority, "prob": prob}

        feat, thresh = self._best_split(X, y, weights)
        if feat is None:
            return {"leaf": True, "label": majority, "prob": prob}

        li = [i for i in range(len(y)) if X[i][feat] <= thresh]
        ri = [i for i in range(len(y)) if X[i][feat] >  thresh]

        return {
            "leaf": False, "feat": feat, "thresh": thresh,
            "left":  self._build([X[i] for i in li], [y[i] for i in li],
                                  [weights[i] for i in li] if weights else None, depth+1),
            "right": self._build([X[i] for i in ri], [y[i] for i in ri],
                                  [weights[i] for i in ri] if weights else None, depth+1),
        }

    def fit(self, X, y, weights=None):
        self.tree = self._build(X, y, weights, 0)

    def _predict_one(self, x, node):
        if node["leaf"]: return node["prob"]
        return self._predict_one(x, node["left"] if x[node["feat"]] <= node["thresh"]
                                 else node["right"])

    def predict_proba(self, X):
        return [self._predict_one(x, self.tree) for x in X]


class RandomForest:
    def __init__(self, n_trees=60, max_depth=7, min_samples=4, n_features=10):
        self.n_trees     = n_trees
        self.max_depth   = max_depth
        self.min_samples = min_samples
        self.n_features  = n_features
        self.trees       = []

    def fit(self, X, y):
        self.trees = []
        n = len(X)

        # 計算類別權重（處理不平衡）
        n_pos = sum(y)
        n_neg = n - n_pos
        w_pos = n / (2 * n_pos) if n_pos > 0 else 1
        w_neg = n / (2 * n_neg) if n_neg > 0 else 1
        weights = [w_pos if yi == 1 else w_neg for yi in y]

        for _ in range(self.n_trees):
            # 加權 Bootstrap 抽樣
            total_w = sum(weights)
            probs   = [w/total_w for w in weights]
            cumprob = []
            cp = 0
            for p in probs:
                cp += p
                cumprob.append(cp)

            idx = []
            for _ in range(n):
                r = random.random()
                for j, cp in enumerate(cumprob):
                    if r <= cp:
                        idx.append(j)
                        break
                else:
                    idx.append(n-1)

            bX = [X[i] for i in idx]
            by = [y[i] for i in idx]
            bw = [weights[i] for i in idx]
            t  = DecisionTree(self.max_depth, self.min_samples, self.n_features)
            t.fit(bX, by, bw)
            self.trees.append(t)

    def predict_proba(self, X):
        all_p = [t.predict_proba(X) for t in self.trees]
        return [sum(all_p[t][i] for t in range(len(self.trees))) / len(self.trees)
                for i in range(len(X))]

# ══════════════════════════════════════════════════
# 訓練資料建立 & 分析
# ══════════════════════════════════════════════════

def build_training_data(records, market_records, predict_days, rise_threshold,
                        inst_hist=None, margin_hist=None):
    X, y = [], []
    closes = [r["close"] for r in records]
    for i in range(40, len(records) - predict_days):
        feats = calc_features(records[:i+1], market_records,
                              inst_hist=inst_hist, margin_hist=margin_hist)
        if feats is None: continue
        future  = closes[i + predict_days]
        current = closes[i]
        label   = 1 if (future - current) / current * 100 >= rise_threshold else 0
        X.append(feats)
        y.append(label)
    return X, y


def _append_today_bar(records, snap):
    """
    收盤後 FinMind 可能還沒更新今日 K 棒 → 用當日快照補上，讓預測用到「今天」。
    快照若仍是昨天的資料（收盤價、漲跌都和最後一筆相同）就不補，避免重複。
    """
    if not snap or not records:
        return records
    today = _tw_today()
    last = records[-1]
    if last["date"] >= today or datetime.strptime(today, "%Y-%m-%d").weekday() >= 5:
        return records
    price, chg = snap.get("price", 0), snap.get("chg", 0)
    if price <= 0 or (abs(price - last["close"]) < 1e-9 and abs(chg - (last.get("chg") or 0)) < 1e-9):
        return records
    return records + [{
        "date": today, "open": snap.get("open") or price, "high": snap.get("high") or price,
        "low": snap.get("low") or price, "close": price,
        "vol": snap.get("vol", 0), "chg": chg,
    }]


def analyze_stock(code, name, market_records, snap=None):
    raw = fetch_history(code, HISTORY_MONTHS)
    raw = _append_today_bar(raw, snap)
    if len(raw) < 80:
        return None
    records = back_adjust(raw)   # 還原權值，保留漲跌停日

    # 籌碼、融資：整段歷史各抓一次，訓練與預測用同一份（以前訓練時全是 0，模型學不到）
    start = records[0]["date"]
    inst_hist   = fetch_inst_history(code, start)
    margin_hist = fetch_margin_history(code, start)

    X, y = build_training_data(records, market_records, PREDICT_DAYS, RISE_THRESHOLD,
                               inst_hist, margin_hist)
    if len(X) < 50 or sum(y) < 8 or sum(1-v for v in y) < 8:
        return None

    # 時間序列 5 折驗證 + 「隔離帶」：
    # 標籤看未來 PREDICT_DAYS 天，若訓練集緊貼驗證集，訓練標籤會用到驗證期股價 → 準確率灌水
    n    = len(X)
    fold = n // 5
    gap  = PREDICT_DAYS
    tot = {"n": 0, "correct": 0, "tp": 0, "fp": 0, "fn": 0, "pos": 0}
    for k in range(5):
        val_start = k * fold
        val_end   = min((k+1)*fold, n) if k < 4 else n
        train_end = val_start - gap
        if train_end < 30: continue
        train_y = y[:train_end]
        if sum(train_y) < 3 or sum(1-v for v in train_y) < 3: continue
        rf = RandomForest(n_trees=40, max_depth=6, n_features=10)
        rf.fit(X[:train_end], train_y)
        probs = rf.predict_proba(X[val_start:val_end])
        val_y = y[val_start:val_end]
        for p, t in zip(probs, val_y):
            pred = 1 if p >= 0.5 else 0
            tot["n"] += 1
            tot["correct"] += (pred == t)
            tot["pos"] += t
            if pred == 1 and t == 1: tot["tp"] += 1
            elif pred == 1 and t == 0: tot["fp"] += 1
            elif pred == 0 and t == 1: tot["fn"] += 1

    if tot["n"] == 0: return None
    accuracy  = tot["correct"] / tot["n"]
    support   = tot["tp"] + tot["fp"]                     # 驗證期「模型說會漲」的次數
    precision = tot["tp"] / support if support else 0     # 說會漲時的實際勝率
    recall_v  = tot["tp"] / (tot["tp"] + tot["fn"]) if (tot["tp"] + tot["fn"]) else 0
    base_rate = tot["pos"] / tot["n"]                     # 不靠模型、隨便買的上漲機率
    edge      = precision - base_rate                     # 模型帶來的勝率優勢

    rf_final = RandomForest(n_trees=60, max_depth=7, n_features=10)
    rf_final.fit(X, y)
    cur_feat = calc_features(records, market_records, inst_hist=inst_hist, margin_hist=margin_hist)
    if cur_feat is None: return None

    rise_prob  = rf_final.predict_proba([cur_feat])[0]
    confidence = accuracy * abs(rise_prob - 0.5) * 2

    # 顯示用：最新一筆已公布的法人買賣超（張）
    inst_f = inst_t = 0
    if inst_hist:
        inst_f, inst_t = (round(v) for v in inst_hist[1][inst_hist[0][-1]])

    return {
        "rise_prob":     round(rise_prob  * 100, 1),
        "accuracy":      round(accuracy   * 100, 1),
        "precision":     round(precision  * 100, 1),
        "recall":        round(recall_v   * 100, 1),
        "base_rate":     round(base_rate  * 100, 1),
        "edge":          round(edge       * 100, 1),
        "support":       support,
        "confidence":    round(confidence * 100, 1),
        "current_price": raw[-1]["close"],
        "recent_chg":    round((raw[-1]["close"]/raw[-2]["close"]-1)*100, 2)
                         if len(raw) >= 2 and raw[-2]["close"] > 0 else 0,
        "inst_foreign":  inst_f,
        "inst_trust":    inst_t,
        "has_inst":      bool(inst_hist),
        "has_margin":    bool(margin_hist),
        "data_years":    round(len(records)/250, 1),
    }

# ══════════════════════════════════════════════════
# 全市場取得 & 主流程
# ══════════════════════════════════════════════════

def get_all_stocks():
    stocks = []
    try:
        url  = "https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL"
        resp = SESSION.get(url, timeout=15)
        resp.raise_for_status()
        for r in resp.json():
            code  = r.get("Code","")
            price = safe_float(r.get("ClosingPrice"))
            chg   = safe_float(r.get("Change"))
            vol   = round(safe_float(r.get("TradeVolume")) / 1000)
            if not (str(code).isdigit() and len(code)==4 and price>0): continue
            prev = price - chg
            pct  = round(chg/prev*100, 2) if prev > 0 else 0
            if price < FILTER["min_price"]:   continue
            if price > FILTER["max_price"]:   continue
            if vol   < FILTER["min_vol_张"]:  continue
            if pct   < FILTER["min_chg_pct"]: continue
            if pct   > FILTER["max_chg_pct"]: continue
            stocks.append({"code":code,"name":r.get("Name",""),
                           "price":price,"chg":chg,"pct":pct,"vol":vol,
                           "open":safe_float(r.get("OpeningPrice"),price),
                           "high":safe_float(r.get("HighestPrice"),price),
                           "low":safe_float(r.get("LowestPrice"),price)})
    except Exception as e:
        print(f"[清單/TWSE] 取得失敗: {e}")

    # ── 上櫃（TPEX）也一併納入 ──
    try:
        r2 = SESSION.get("https://www.tpex.org.tw/openapi/v1/tpex_mainboard_quotes", timeout=15)
        if r2.status_code == 200 and isinstance(r2.json(), list):
            for r in r2.json():
                code  = r.get("SecuritiesCompanyCode","") or r.get("code","")
                price = safe_float(r.get("Close","") or r.get("close",""))
                chg   = safe_float(r.get("Change","") or r.get("change",""))
                vol   = round(safe_float(r.get("TradingShares","") or "0") / 1000)
                if not (str(code).isdigit() and len(code)==4 and price>0): continue
                prev = price - chg
                pct  = round(chg/prev*100, 2) if prev > 0 else 0
                if price < FILTER["min_price"]:   continue
                if price > FILTER["max_price"]:   continue
                if vol   < FILTER["min_vol_张"]:  continue
                if pct   < FILTER["min_chg_pct"]: continue
                if pct   > FILTER["max_chg_pct"]: continue
                stocks.append({"code":code,"name":r.get("CompanyName","") or r.get("name",""),
                               "price":price,"chg":chg,"pct":pct,"vol":vol,
                               "open":safe_float(r.get("Open"),price),
                               "high":safe_float(r.get("High"),price),
                               "low":safe_float(r.get("Low"),price)})
    except Exception as e:
        print(f"[清單/TPEX] 取得失敗: {e}")

    print(f"   股票清單共取得 {len(stocks)} 支（上市+上櫃）")
    return stocks

def save_result(data):
    """把分析結果存成 JSON 檔，供 server.py 讀取"""
    try:
        with open(RESULT_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"  ✅ 結果已存至 {RESULT_FILE}")
    except Exception as e:
        print(f"  ❌ 存檔失敗: {e}")

def run():
    now_str = (datetime.utcnow() + timedelta(hours=8)).strftime("%Y/%m/%d %H:%M")  # 台灣時間
    print(f"\n{'='*55}")
    print(f"  台股自動分析 v2.0  {now_str}")
    print(f"{'='*55}")

    # 取得大盤資料
    market_data = get_market_data()

    # 取得股票清單
    print("\n① 取得市場資料...")
    stocks = get_all_stocks()
    print(f"   篩選後 {len(stocks)} 支")

    if not stocks:
        print("❌ 無法取得任何股票資料（TWSE/TPEX 都失敗），中止本次分析")
        print("   不寫入 Supabase，避免產生「掃描0支」的無效紀錄")
        save_result({"error": "無法取得市場資料", "time": now_str})
        return   # 直接返回，不呼叫 save_to_supabase

    # ── 方案3：智慧粗篩（取代隨機抽樣），兼顧 FinMind 呼叫量與品質 ──
    # 原本 random.shuffle 取 80 支會導致結果飄忽、漏掉強勢股。
    # 改為：先用快照粗篩，再依「今日成交量」排序取前 MAX_ANALYZE 支。
    MAX_ANALYZE = 250   # 上限（控制 FinMind 呼叫量，可視額度調整）

    # 粗篩：今日上漲或平盤（pct >= -1），排除明顯弱勢股
    pre = [s for s in stocks if s.get("pct", 0) >= -1]
    if len(pre) < 50:           # 若太少則放寬回原清單
        pre = stocks
    # 依今日成交量由大到小排序（量大＝市場關注度高），取前 N 支
    pre.sort(key=lambda x: x.get("vol", 0), reverse=True)
    stocks = pre[:MAX_ANALYZE]
    print(f"   智慧粗篩後取量能前 {len(stocks)} 支進行分析（上限 {MAX_ANALYZE}）")

    # 逐支分析
    print(f"\n② 隨機森林分析（{len(stocks)} 支）...")
    results = []
    for i, s in enumerate(stocks):
        print(f"   [{i+1:3d}/{len(stocks)}] {s['code']} {s['name']:<8}", end=" ")
        try:
            r = analyze_stock(s["code"], s["name"], market_data, s)
            if r and r["confidence"] > 5:
                r.update(s)
                results.append(r)
                print(f"✅ 漲:{r['rise_prob']}% 勝率:{r['precision']}%(基準{r['base_rate']}%) 優勢:{r['edge']:+}")
            else:
                print("略過")
        except Exception as e:
            print(f"錯誤:{e}")

    if not results:
        save_result({"error": "本次分析無有效推薦", "time": now_str,
                     "stocks": [], "total_scanned": len(stocks), "total_analyzed": 0})
        return

    # 排序：綜合信心度 + 漲機率
    results.sort(key=lambda x: x["rise_prob"] * 0.6 + x["confidence"] * 0.4, reverse=True)
    # 勝率門檻：只推薦「歷史上說會漲時，勝率確實高於隨便買」的股票
    #   edge > 0：精確率 > 基準上漲率；support ≥ 5：驗證期至少喊漲過 5 次，數字才有意義
    qualified = [r for r in results
                 if r["rise_prob"] >= 50 and r.get("edge", 0) > 0 and r.get("support", 0) >= 5]
    edge_filter = len(qualified) > 0
    if not edge_filter:
        print("\n   ⚠️ 沒有股票通過勝率優勢門檻，退回原本排序")
    top = (qualified if edge_filter else results)[:TOP_N]
    print(f"\n   通過勝率門檻 {len(qualified)}/{len(results)} 支；FinMind 呼叫 {_fm_state['calls']} 次"
          f"{'（額度用盡，部分改用備援）' if _fm_state['disabled'] else ''}")

    # 統計摘要
    avg_acc = round(sum(r["accuracy"] for r in results) / len(results), 1)
    bullish = sum(1 for r in results if r["rise_prob"] >= 50)
    bearish = len(results) - bullish

    print(f"\n③ 儲存結果（{len(top)} 支推薦）...")

    # 整理輸出資料
    output = {
        "time":           now_str,
        "model_ver":      "v2",
        "edge_filter":    edge_filter,
        "total_scanned":  len(stocks),
        "total_analyzed": len(results),
        "avg_accuracy":   avg_acc,
        "bullish":        bullish,
        "bearish":        bearish,
        "predict_days":   PREDICT_DAYS,
        "stocks": [{
            "code":        r["code"],
            "name":        r["name"],
            "rise_prob":   r["rise_prob"],
            "confidence":  r["confidence"],
            "accuracy":    r["accuracy"],
            "precision":   r.get("precision", 0),
            "recall":      r.get("recall", 0),
            "base_rate":   r.get("base_rate", 0),
            "edge":        r.get("edge", 0),
            "support":     r.get("support", 0),
            "price":       r["price"],
            "chg_pct":     r.get("pct", 0),
            "vol":         r.get("vol", 0),
            "data_years":  r.get("data_years", 0),
            "inst_foreign":r.get("inst_foreign", 0),
            "inst_trust":  r.get("inst_trust", 0),
        } for r in top]
    }

    save_result(output)
    save_to_supabase(output)   # ← 同步寫入 Supabase（GitHub Actions 用）

    print(f"\n✅ 完成！前3名：" +
          "、".join(f"{r['code']}{r['name']}" for r in top[:3]))
    print(f"   結果已存至：{RESULT_FILE}")
    print(f"   網頁可在 /analyze 頁面查看最新結果")

# ══════════════════════════════════════════════════

if __name__ == "__main__":
    run()
