"""
台股選股 + 回測系統 - 後端伺服器
====================================
安裝套件（只需執行一次）：
    pip install flask requests

啟動伺服器：
    python server.py

部署到 Render（雲端）：
    pip install gunicorn
    gunicorn server:app
"""

from flask import Flask, jsonify, request, send_from_directory
import requests
import urllib3
from datetime import datetime, timedelta
import threading
import random
import os
import json
from collections import defaultdict

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

app = Flask(__name__, static_folder=".", static_url_path="")

@app.after_request
def after_request(response):
    response.headers["Access-Control-Allow-Origin"]  = "*"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type"
    response.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS, DELETE"
    return response

SESSION = requests.Session()
SESSION.verify = False
SESSION.headers.update({"User-Agent": "Mozilla/5.0"})

# ── 股票市場別對照表（快取）─────────────────────────
# 記錄每支股票是上市(twse)還是上櫃(otc)，避免重複判斷
_market_map = {}   # code -> "twse" | "otc"
_market_map_loaded = False

def _load_market_map():
    """啟動時預載入上市+上櫃股票清單，建立市場別對照表"""
    global _market_map, _market_map_loaded
    if _market_map_loaded:
        return
    try:
        # 上市
        r = SESSION.get("https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL",
                        timeout=15)
        for row in r.json():
            code = row.get("Code","")
            if code: _market_map[code] = "twse"
        print(f"  [市場表] 上市 {sum(1 for v in _market_map.values() if v=='twse')} 支")
    except Exception as e:
        print(f"  [市場表] 上市載入失敗: {e}")
    try:
        # 上櫃
        r2 = SESSION.get("https://www.tpex.org.tw/openapi/v1/tpex_mainboard_quotes",
                         timeout=15)
        for row in r2.json():
            code = str(row.get("SecuritiesCompanyCode","") or row.get("code","")).strip()
            if code: _market_map[code] = "otc"
        print(f"  [市場表] 上櫃 {sum(1 for v in _market_map.values() if v=='otc')} 支")
    except Exception as e:
        print(f"  [市場表] 上櫃載入失敗: {e}")
    _market_map_loaded = True
    print(f"  [市場表] 共 {len(_market_map)} 支股票建立完成")

def get_market(code):
    """取得股票的市場別：'twse'（上市）或 'otc'（上櫃），未知回傳 None"""
    if not _market_map_loaded:
        _load_market_map()
    return _market_map.get(str(code))

# 背景預載市場表
threading.Thread(target=_load_market_map, daemon=True).start()

# ── 靜態頁面 ─────────────────────────────────────
@app.route("/")
def index():
    return send_from_directory(".", "index.html")

@app.route("/backtest")
def backtest_page():
    return send_from_directory(".", "backtest.html")

@app.route("/alert")
def alert_page():
    return send_from_directory(".", "alert.html")

@app.route("/api/health")
def health():
    return jsonify({"ok": True, "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")})

# ══════════════════════════════════════════════════════
# 工具函式
# ══════════════════════════════════════════════════════

def safe_float(val, default=0.0):
    try:
        v = str(val).replace(",", "").strip()
        if v in ("--", "", "N/A", "None"): return default
        return float(v)
    except: return default

def sma(arr, n):
    if not arr: return 0.0
    sl = arr[-n:] if len(arr) >= n else arr
    return sum(sl) / len(sl)

def calc_kd_series(closes, highs, lows, period=9):
    k_series, d_series = [], []
    k, d = 50.0, 50.0
    for i in range(len(closes)):
        n  = min(period, i + 1)
        rh = max(highs[max(0, i-n+1):i+1])
        rl = min(lows[max(0, i-n+1):i+1])
        rsv = 0.0 if rh == rl else (closes[i] - rl) / (rh - rl) * 100
        k = k * 2/3 + rsv * 1/3
        d = d * 2/3 + k   * 1/3
        k_series.append(round(k, 1))
        d_series.append(round(d, 1))
    return k_series, d_series

def calc_rsi_series(closes, period=14):
    rsi_series = [50.0] * len(closes)
    for i in range(period, len(closes)):
        changes = [closes[j+1] - closes[j] for j in range(i-period, i)]
        gains  = sum(x for x in changes if x > 0) / period
        losses = sum(-x for x in changes if x < 0) / period
        if losses == 0:
            rsi_series[i] = 100.0
        else:
            rs = gains / losses
            rsi_series[i] = round(100 - 100 / (1 + rs), 1)
    return rsi_series


# ── 歷史資料快取（同一天同一支股票只抓一次）──────
_history_cache = {}

def fetch_history_range(code, start_date, end_date):
    """抓指定期間的歷史資料（根據市場別直接查詢，含快取）"""
    import time as _time

    cache_key = f"{code}_{start_date[:7]}_{end_date[:7]}"
    if cache_key in _history_cache:
        return _history_cache[cache_key]

    start_dt    = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt      = datetime.strptime(end_date,   "%Y-%m-%d")
    fetch_start = start_dt - timedelta(days=90)
    cur = datetime(fetch_start.year, fetch_start.month, 1)

    # 查市場別（防呆：若查不到就兩個都試）
    market = get_market(code)
    use_twse = market != "otc"   # 預設先試上市
    use_otc  = market != "twse"  # 若確定是上市就不試上櫃

    # ── 上市（TWSE）─────────────────────────────────
    twse_records = []
    if use_twse:
        tmp_cur = cur
        while tmp_cur <= end_dt:
            ym = f"{tmp_cur.year}{tmp_cur.month:02d}01"
            try:
                url  = (f"https://www.twse.com.tw/exchangeReport/STOCK_DAY"
                        f"?response=json&date={ym}&stockNo={code}")
                r    = SESSION.get(url, timeout=10)
                data = r.json()
                if data.get("stat") == "OK" and data.get("data"):
                    for row in data["data"]:
                        parts = row[0].split("/")
                        if len(parts) != 3: continue
                        try:
                            dt = datetime(int(parts[0])+1911, int(parts[1]), int(parts[2]))
                        except: continue
                        c = safe_float(row[6])
                        if c > 0:
                            twse_records.append({
                                "date":   dt.strftime("%Y-%m-%d"),
                                "open":   safe_float(row[3]),
                                "high":   safe_float(row[4]),
                                "low":    safe_float(row[5]),
                                "close":  c,
                                "vol":    round(safe_float(row[1]) / 1000),
                                "change": safe_float(row[7]),
                            })
            except: pass
            tmp_cur = (tmp_cur + timedelta(days=32)).replace(day=1)
            _time.sleep(0.2)

    if twse_records:
        twse_records.sort(key=lambda x: x["date"])
        _market_map[code] = "twse"  # 確認是上市，更新市場別
        _history_cache[cache_key] = twse_records
        return twse_records

    # ── 上櫃（OTC / TPEX）───────────────────────────
    otc_records = []
    if use_otc:
        tmp_cur = cur
        while tmp_cur <= end_dt:
            roc_year = tmp_cur.year - 1911
            ym_otc   = f"{roc_year}/{tmp_cur.month:02d}"
            fetched  = False

            # 方法一：TPEX 舊版 API（最穩定）
            try:
                url  = (f"https://www.tpex.org.tw/web/stock/aftertrading/daily_trading_info/st43_result.php"
                        f"?l=zh-tw&d={ym_otc}&stkno={code}&s=0,asc,0&o=json")
                r    = SESSION.get(url, timeout=12)
                data = r.json()
                rows = data.get("aaData", [])
                if rows:
                    for row in rows:
                        try:
                            date_parts = str(row[0]).strip().split("/")
                            if len(date_parts) != 3: continue
                            dt = datetime(int(date_parts[0])+1911,
                                          int(date_parts[1]), int(date_parts[2]))
                            c = safe_float(str(row[6]).replace(",",""))
                            if c > 0:
                                otc_records.append({
                                    "date":   dt.strftime("%Y-%m-%d"),
                                    "open":   safe_float(str(row[3]).replace(",","")),
                                    "high":   safe_float(str(row[4]).replace(",","")),
                                    "low":    safe_float(str(row[5]).replace(",","")),
                                    "close":  c,
                                    "vol":    round(safe_float(str(row[1]).replace(",","")) / 1000),
                                    "change": safe_float(str(row[7]).replace(",","")),
                                })
                            fetched = True
                        except: continue
            except Exception as e:
                print(f"  [OTC方法1] {code} {ym_otc}: {e}")

            # 方法二：備用 API
            if not fetched:
                try:
                    _time.sleep(1)
                    url2 = (f"https://www.tpex.org.tw/openapi/v1/tpex_mainboard_daily_close_quotes"
                            f"?date={tmp_cur.year}-{tmp_cur.month:02d}-01&stockNo={code}")
                    r2   = SESSION.get(url2, timeout=12)
                    rows2 = r2.json()
                    if isinstance(rows2, list):
                        for row in rows2:
                            try:
                                d_str = row.get("Date","") or row.get("date","")
                                c2    = safe_float(row.get("Close","") or row.get("close",""))
                                if not d_str or c2 <= 0: continue
                                if "/" in d_str:
                                    pts = d_str.split("/")
                                    dt  = datetime(int(pts[0])+1911, int(pts[1]), int(pts[2]))
                                else:
                                    dt  = datetime.strptime(d_str[:10], "%Y-%m-%d")
                                otc_records.append({
                                    "date":   dt.strftime("%Y-%m-%d"),
                                    "open":   safe_float(row.get("Open","") or row.get("open","")),
                                    "high":   safe_float(row.get("High","") or row.get("high","")),
                                    "low":    safe_float(row.get("Low","")  or row.get("low","")),
                                    "close":  c2,
                                    "vol":    round(safe_float(row.get("TradingShares","") or
                                                   row.get("volume","")) / 1000),
                                    "change": safe_float(row.get("Change","") or row.get("change","")),
                                })
                            except: continue
                except Exception as e:
                    print(f"  [OTC方法2] {code} {ym_otc}: {e}")

            tmp_cur = (tmp_cur + timedelta(days=32)).replace(day=1)
            _time.sleep(0.5)

    if otc_records:
        _market_map[code] = "otc"  # 確認是上櫃

    otc_records.sort(key=lambda x: x["date"])
    seen  = set()
    dedup = []
    for r in otc_records:
        if r["date"] not in seen:
            seen.add(r["date"])
            dedup.append(r)

    # ── 若上市上櫃都抓不到 → 嘗試用 FinMind ──────
    if not dedup:
        try:
            token = _get_finmind_token()
            if token:
                print(f"  [歷史] {code} OTC也失敗，嘗試 FinMind...")
                params = {
                    "dataset":    "TaiwanStockPrice",
                    "data_id":    code,
                    "start_date": (datetime.strptime(start_date, "%Y-%m-%d")
                                   - timedelta(days=90)).strftime("%Y-%m-%d"),
                    "end_date":   end_date,
                }
                r = SESSION.get("https://api.finmindtrade.com/api/v4/data",
                                params=params,
                                headers={"Authorization": f"Bearer {token}"},
                                timeout=15)
                data = r.json()
                if data.get("status") == 200:
                    for row in data.get("data", []):
                        c = safe_float(row.get("close", 0))
                        if c > 0:
                            dedup.append({
                                "date":   row.get("date","")[:10],
                                "open":   safe_float(row.get("open",  0)),
                                "high":   safe_float(row.get("max",   0)),
                                "low":    safe_float(row.get("min",   0)),
                                "close":  c,
                                "vol":    round(safe_float(row.get("Trading_Volume", 0)) / 1000),
                                "change": safe_float(row.get("spread", 0)),
                            })
                    dedup.sort(key=lambda x: x["date"])
                    print(f"  [FinMind] {code}: {len(dedup)} 筆")
        except Exception as e:
            print(f"  [FinMind備用] {code}: {e}")

    _history_cache[cache_key] = dedup
    return dedup

def fetch_history_recent(code):
    """抓最近兩個月（給選股用）- 根據市場別直接查詢，含快取"""
    import time as _time

    cache_key = f"recent_{code}_{datetime.today().strftime('%Y%m%d')}"
    if cache_key in _history_cache:
        return _history_cache[cache_key]

    market   = get_market(code)
    records  = []
    today    = datetime.today()
    use_twse = market != "otc"
    use_otc  = market != "twse"

    # ── 上市（TWSE）─────────────────────────────────
    if use_twse:
        for delta in [1, 0]:
            d  = today - timedelta(days=delta * 32)
            ym = f"{d.year}{d.month:02d}01"
            try:
                url  = (f"https://www.twse.com.tw/exchangeReport/STOCK_DAY"
                        f"?response=json&date={ym}&stockNo={code}")
                r    = SESSION.get(url, timeout=8)
                data = r.json()
                if data.get("stat") != "OK" or not data.get("data"): continue
                for row in data["data"]:
                    c = safe_float(row[6])
                    if c > 0:
                        records.append({
                            "close": c,
                            "high":  safe_float(row[4]),
                            "low":   safe_float(row[5]),
                            "vol":   round(safe_float(row[1]) / 1000),
                        })
            except: pass
            _time.sleep(0.1)

    if records:
        _market_map[code] = "twse"
        _history_cache[cache_key] = records
        return records

    # ── 上櫃（OTC）──────────────────────────────────
    if use_otc:
        for delta in [1, 0]:
            d      = today - timedelta(days=delta * 32)
            roc_y  = d.year - 1911
            ym_otc = f"{roc_y}/{d.month:02d}"
            try:
                url  = (f"https://www.tpex.org.tw/web/stock/aftertrading/daily_trading_info/st43_result.php"
                        f"?l=zh-tw&d={ym_otc}&stkno={code}&s=0,asc,0&o=json")
                r    = SESSION.get(url, timeout=10)
                data = r.json()
                rows = data.get("aaData", [])
                for row in rows:
                    try:
                        c = safe_float(str(row[6]).replace(",",""))
                        if c > 0:
                            records.append({
                                "close": c,
                                "high":  safe_float(str(row[4]).replace(",","")),
                                "low":   safe_float(str(row[5]).replace(",","")),
                                "vol":   round(safe_float(str(row[1]).replace(",","")) / 1000),
                            })
                    except: continue
            except: pass
            _time.sleep(0.5)

    if records:
        _market_map[code] = "otc"
    _history_cache[cache_key] = records
    return records

# ══════════════════════════════════════════════════════
# 條件評估
# ══════════════════════════════════════════════════════

def calc_atr(highs, lows, closes, period=14):
    """計算 ATR（平均真實波幅）序列"""
    atr_series = []
    for i in range(len(closes)):
        if i == 0:
            atr_series.append(highs[i] - lows[i])
            continue
        tr = max(highs[i]-lows[i],
                 abs(highs[i]-closes[i-1]),
                 abs(lows[i]-closes[i-1]))
        if i < period:
            atr_series.append(sum(
                max(highs[j]-lows[j], abs(highs[j]-closes[j-1]) if j>0 else highs[j]-lows[j],
                    abs(lows[j]-closes[j-1]) if j>0 else 0)
                for j in range(i+1)) / (i+1))
        else:
            atr_series.append(atr_series[-1]*(period-1)/period + tr/period)
    return [round(v, 4) for v in atr_series]

# 大盤指數快取
_index_cache = {}

def fetch_index_history(start_date, end_date):
    """抓台股加權指數（Y9999）歷史資料"""
    key = f"Y9999_{start_date[:7]}_{end_date[:7]}"
    if key in _index_cache:
        return _index_cache[key]
    records = fetch_history_range("Y9999", start_date, end_date)
    _index_cache[key] = records
    return records

def build_index_map(start_date, end_date):
    """回傳 {date: {close, ma60}} 供回測用"""
    records = fetch_index_history(start_date, end_date)
    if not records:
        return {}
    closes = [r["close"] for r in records]
    result = {}
    for i, r in enumerate(records):
        ma60 = sma(closes[max(0,i-59):i+1], 60)
        ma20 = sma(closes[max(0,i-19):i+1], 20)
        result[r["date"]] = {
            "close": r["close"],
            "ma20":  round(ma20, 2),
            "ma60":  round(ma60, 2),
            "above_ma60": r["close"] > ma60,
            "above_ma20": r["close"] > ma20,
        }
    return result

def eval_cond(s, c):
    if c.get("key") == "kdCross":
        mn = c.get("kdMin", 0)
        mx = c.get("kdMax", 100)
        return (s.get("kVal",50) > s.get("dVal",50) and
                s.get("prevK",50) <= s.get("prevD",50) and
                mn <= s.get("kVal",50) < mx)
    v  = s.get(c.get("key"))
    if v is None: return False
    op = c.get("op", ">")
    vl = c.get("val", 0)
    if op == ">":  return v > vl
    if op == ">=": return v >= vl
    if op == "<":  return v < vl
    if op == "<=": return v <= vl
    if op == "=":  return v == vl
    return False

# ══════════════════════════════════════════════════════
# 回測核心（單股）
# ══════════════════════════════════════════════════════

def build_day_indicators(i, closes, highs, lows, vols, k_ser, d_ser, rsi_ser, dates):
    price = closes[i]
    prev  = closes[i-1] if i > 0 else price
    chg   = round((price-prev)/prev*100, 2) if prev > 0 else 0
    avg5v  = round(sma(vols[max(0,i-5):i],  5))
    avg20v = round(sma(vols[max(0,i-20):i], 20))
    ma5    = round(sma(closes[max(0,i-4):i+1],  5),  2)
    ma20   = round(sma(closes[max(0,i-19):i+1], 20), 2)
    ma60   = round(sma(closes[max(0,i-59):i+1], 60), 2)
    tv = vols[i]
    return {
        "date": dates[i], "price": price,
        "chgPct": chg, "todayVol": tv,
        "avg5Vol": avg5v, "avg20Vol": avg20v,
        "volVsAvg5":  round(tv/avg5v,  2) if avg5v>0  else 0,
        "volVsAvg20": round(tv/avg20v, 2) if avg20v>0 else 0,
        "priceVsMA20": round((price-ma20)/ma20*100,2) if ma20>0 else 0,
        "priceVsMA60": round((price-ma60)/ma60*100,2) if ma60>0 else 0,
        "ma20VsMA60":  round((ma20-ma60)/ma60*100, 2) if ma60>0 else 0,
        "ma5VsMA20":   round((ma5-ma20)/ma20*100,  2) if ma20>0 else 0,
        "kVal":  k_ser[i], "dVal":  d_ser[i],
        "prevK": k_ser[i-1] if i>0 else 50,
        "prevD": d_ser[i-1] if i>0 else 50,
        "rsi14": rsi_ser[i],
    }

def run_single_backtest(records, conditions, start_date, end_date,
                        take_profit, stop_loss, hold_days, trailing_stop,
                        ma_sell_period=0,
                        use_market_filter=False,
                        market_ma=60,
                        use_atr_stop=False,
                        atr_multiplier=2.0,
                        partial_exit_pct=0,
                        partial_exit_ratio=50):
    """
    優化版回測：
    - use_market_filter: 大盤在 market_ma 均線之下不買入
    - use_atr_stop:      用 ATR×atr_multiplier 作動態停損
    - partial_exit_pct:  漲幅達 X% 先出 partial_exit_ratio% 倉位
    """
    if not records: return [], []
    closes = [r["close"] for r in records]
    highs  = [r["high"]  for r in records]
    lows   = [r["low"]   for r in records]
    vols   = [r["vol"]   for r in records]
    dates  = [r["date"]  for r in records]
    k_ser, d_ser = calc_kd_series(closes, highs, lows, 9)
    rsi_ser = calc_rsi_series(closes, 14)
    atr_ser = calc_atr(highs, lows, closes, 14)

    # 大盤資料
    index_map = {}
    if use_market_filter:
        try:
            index_map = build_index_map(start_date, end_date)
            print(f"  [大盤] 載入 {len(index_map)} 筆指數資料")
        except Exception as e:
            print(f"  [大盤] 載入失敗: {e}")

    trades, daily_equity = [], []
    position  = None
    capital   = 100.0

    for i, rec in enumerate(records):
        if rec["date"] < start_date: continue
        if rec["date"] > end_date:   break
        day = build_day_indicators(i, closes, highs, lows, vols,
                                   k_ser, d_ser, rsi_ser, dates)

        if position:
            buy_price    = position["buy_price"]
            held         = i - position["buy_idx"]
            cur_chg      = (closes[i] - buy_price) / buy_price * 100
            position["peak_price"] = max(position["peak_price"], highs[i])
            peak         = position["peak_price"]
            size         = position["size"]          # 目前持倉比例 (0~1)
            atr_stop     = position.get("atr_stop", 0)
            sell_reason  = None
            sell_price   = closes[i]
            partial_done = position.get("partial_done", False)

            # ① 追蹤高點回落
            if trailing_stop > 0 and peak > buy_price:
                drop = (peak - closes[i]) / peak * 100
                if drop >= trailing_stop:
                    sell_price  = round(peak*(1-trailing_stop/100), 2)
                    sell_reason = f"追蹤高點回落{trailing_stop}%"

            # ② 停利（固定）
            if not sell_reason and take_profit > 0 and cur_chg >= take_profit:
                sell_price  = round(buy_price*(1+take_profit/100), 2)
                sell_reason = f"停利+{take_profit}%"

            # ③ ATR 動態停損（優先於固定停損）
            if not sell_reason and use_atr_stop and atr_stop > 0:
                if closes[i] <= atr_stop:
                    sell_reason = f"ATR停損（{round(atr_stop,2)}）"

            # ④ 固定停損
            if not sell_reason and stop_loss > 0 and cur_chg <= -stop_loss:
                sell_price  = round(buy_price*(1-stop_loss/100), 2)
                sell_reason = f"停損-{stop_loss}%"

            # ⑤ 跌破均線
            if not sell_reason and ma_sell_period > 0:
                ma_val = sma(closes[max(0,i-ma_sell_period+1):i+1], ma_sell_period)
                if ma_val > 0 and closes[i] < ma_val:
                    sell_reason = f"跌破MA{ma_sell_period}（{round(ma_val,2)}）"

            # ⑥ 持有天數
            if not sell_reason and hold_days > 0 and held >= hold_days:
                sell_reason = f"持有{hold_days}日到期"

            # ⑦ 分批出場（半倉先出）
            if (not sell_reason and partial_exit_pct > 0
                    and not partial_done and cur_chg >= partial_exit_pct):
                ratio = partial_exit_ratio / 100
                pnl_partial = cur_chg * ratio
                capital *= (1 + pnl_partial / 100)
                position["size"]         = size * (1 - ratio)
                position["partial_done"] = True
                trades.append({
                    "buy_date":    position["buy_date"],
                    "buy_price":   round(buy_price, 2),
                    "peak_price":  round(peak, 2),
                    "sell_date":   rec["date"],
                    "sell_price":  round(closes[i], 2),
                    "sell_reason": f"分批出場{partial_exit_ratio}%（漲{round(cur_chg,1)}%）",
                    "pnl":         round(cur_chg, 2),
                    "held_days":   held,
                    "partial":     True,
                })

            if sell_reason:
                pnl = (sell_price - buy_price) / buy_price * 100
                capital *= (1 + pnl * position["size"] / 100)
                trades.append({
                    "buy_date":    position["buy_date"],
                    "buy_price":   round(buy_price, 2),
                    "peak_price":  round(peak, 2),
                    "sell_date":   rec["date"],
                    "sell_price":  round(sell_price, 2),
                    "sell_reason": sell_reason,
                    "pnl":         round(pnl, 2),
                    "held_days":   held,
                    "partial":     False,
                })
                position = None

        # 買入條件
        if not position:
            cond_ok = all(eval_cond(day, c) for c in conditions if c.get("enabled", True))

            # 大盤過濾
            if cond_ok and use_market_filter and index_map:
                idx = index_map.get(rec["date"])
                if idx is None:
                    # 找最近一個有資料的日期
                    for back in range(1, 6):
                        prev_d = (datetime.strptime(rec["date"],"%Y-%m-%d")
                                  - timedelta(days=back)).strftime("%Y-%m-%d")
                        if prev_d in index_map:
                            idx = index_map[prev_d]; break
                if idx:
                    if market_ma == 20:
                        cond_ok = cond_ok and idx["above_ma20"]
                    else:
                        cond_ok = cond_ok and idx["above_ma60"]

            if cond_ok:
                atr_stop_price = 0
                if use_atr_stop and i < len(atr_ser):
                    atr_stop_price = round(closes[i] - atr_multiplier * atr_ser[i], 2)
                position = {
                    "buy_date":     rec["date"],
                    "buy_price":    closes[i],
                    "buy_idx":      i,
                    "peak_price":   closes[i],
                    "size":         1.0,
                    "atr_stop":     atr_stop_price,
                    "partial_done": False,
                }

        daily_equity.append({
            "date":   rec["date"],
            "equity": round(capital * ((closes[i]/position["buy_price"]
                                        * position.get("size",1.0))
                            if position else 1.0), 4),
        })

    # 強制平倉
    if position and records:
        last = records[-1]
        pnl  = (last["close"] - position["buy_price"]) / position["buy_price"] * 100
        capital *= (1 + pnl * position["size"] / 100)
        trades.append({
            "buy_date":    position["buy_date"],
            "buy_price":   round(position["buy_price"], 2),
            "peak_price":  round(position["peak_price"], 2),
            "sell_date":   last["date"],
            "sell_price":  round(last["close"], 2),
            "sell_reason": "回測結束平倉",
            "pnl":         round(pnl, 2),
            "held_days":   len(records)-1-position["buy_idx"],
            "partial":     False,
        })
    return trades, daily_equity

# ══════════════════════════════════════════════════════
# 回測核心（全市場）
# ══════════════════════════════════════════════════════

_hist_cache = {}

def fetch_cached(code, start_date, end_date):
    key = f"{code}_{start_date[:7]}_{end_date[:7]}"
    if key not in _hist_cache:
        _hist_cache[key] = fetch_history_range(code, start_date, end_date)
    return _hist_cache[key]

def run_market_backtest(codes, conditions, start_date, end_date,
                        take_profit, stop_loss, hold_days, trailing_stop,
                        max_pos, progress_cb=None, ma_sell_period=0):
    total = len(codes)
    all_stock_ind = {}
    all_dates_set = set()
    all_stock_data_closes = {}  # code -> [{date, close}] for MA calculation

    for idx, code in enumerate(codes):
        if progress_cb:
            progress_cb(f"抓取 {code} 歷史資料 ({idx+1}/{total})", idx/total*40)
        try:
            recs = fetch_cached(code, start_date, end_date)
            if len(recs) < 10: continue
            closes = [r["close"] for r in recs]
            highs  = [r["high"]  for r in recs]
            lows   = [r["low"]   for r in recs]
            vols   = [r["vol"]   for r in recs]
            k_ser, d_ser = calc_kd_series(closes, highs, lows, 9)
            rsi_ser = calc_rsi_series(closes, 14)
            ind_map = {}
            for i, r in enumerate(recs):
                ind_map[r["date"]] = build_day_indicators(
                    i, closes, highs, lows, vols, k_ser, d_ser, rsi_ser,
                    [x["date"] for x in recs])
            all_stock_ind[code] = ind_map
            all_dates_set.update(ind_map.keys())
            all_stock_data_closes[code] = [{"date":r["date"],"close":r["close"]} for r in recs]
        except Exception as e:
            print(f"  [市場回測] {code}: {e}")

    trading_days = sorted(d for d in all_dates_set if start_date <= d <= end_date)
    if not trading_days: return [], [], {}

    if progress_cb: progress_cb("模擬交易中...", 55)

    capital     = 100.0
    positions   = {}
    all_trades  = []
    daily_eq    = []
    day_signals = {}

    for di, date in enumerate(trading_days):
        if progress_cb and di % 20 == 0:
            progress_cb(f"模擬 {date} ({di+1}/{len(trading_days)}日)", 55+di/len(trading_days)*40)

        # 處理賣出
        to_sell = []
        for code, pos in positions.items():
            ind = all_stock_ind.get(code, {}).get(date)
            if not ind: continue
            cur  = ind["price"]
            bp   = pos["buy_price"]
            held = di - pos["buy_day_idx"]
            pos["peak_price"] = max(pos["peak_price"], ind["high"])
            peak    = pos["peak_price"]
            cur_chg = (cur - bp) / bp * 100

            sell_reason = None
            sell_price  = cur

            if trailing_stop > 0 and peak > bp:
                drop = (peak - cur) / peak * 100
                if drop >= trailing_stop:
                    sell_price  = round(peak*(1-trailing_stop/100), 2)
                    sell_reason = f"追蹤高點回落{trailing_stop}%"
            if not sell_reason and take_profit > 0 and cur_chg >= take_profit:
                sell_price  = round(bp*(1+take_profit/100), 2)
                sell_reason = f"停利+{take_profit}%"
            if not sell_reason and stop_loss > 0 and cur_chg <= -stop_loss:
                sell_price  = round(bp*(1-stop_loss/100), 2)
                sell_reason = f"停損-{stop_loss}%"
            # 跌破均線
            if not sell_reason and ma_sell_period > 0:
                stock_recs = all_stock_data_closes.get(code, [])
                if stock_recs:
                    day_idx_in_stock = next(
                        (j for j, r in enumerate(stock_recs) if r["date"] == date), None)
                    if day_idx_in_stock is not None:
                        closes_to_now = [r["close"] for r in stock_recs[:day_idx_in_stock+1]]
                        ma_val = sma(closes_to_now[-ma_sell_period:], ma_sell_period)
                        if ma_val > 0 and cur < ma_val:
                            sell_reason = f"跌破MA{ma_sell_period}（{round(ma_val,2)}）"
            if not sell_reason and hold_days > 0 and held >= hold_days:
                sell_reason = f"持有{hold_days}日到期"

            if sell_reason:
                pnl = (sell_price - bp) / bp * 100
                capital *= (1 + (1/max_pos) * pnl/100)
                all_trades.append({
                    "code": code, "buy_date": pos["buy_date"],
                    "buy_price": round(bp, 2), "peak_price": round(peak, 2),
                    "sell_date": date, "sell_price": round(sell_price, 2),
                    "sell_reason": sell_reason, "pnl": round(pnl, 2),
                    "held_days": held,
                })
                to_sell.append(code)
        for c in to_sell: del positions[c]

        # 掃描買入訊號
        signals = [
            code for code, ind_map in all_stock_ind.items()
            if code not in positions
            and (ind := ind_map.get(date))
            and all(eval_cond(ind, c) for c in conditions if c.get("enabled", True))
        ]
        day_signals[date] = signals

        # 買入補位
        slots = max_pos - len(positions)
        for code in signals[:slots]:
            ind = all_stock_ind.get(code, {}).get(date)
            if ind:
                positions[code] = {
                    "buy_date": date, "buy_price": ind["price"],
                    "buy_day_idx": di, "peak_price": ind["price"],
                }

        # 當日資產
        pos_gain = sum(
            (1/max_pos) * ((all_stock_ind.get(c,{}).get(date,{}).get("price", pos["buy_price"])
                            / pos["buy_price"]) - 1) * 100
            for c, pos in positions.items()
        )
        daily_eq.append({
            "date": date,
            "equity": round(capital * (1 + pos_gain/100), 4),
            "n_pos": len(positions),
            "signals": len(signals),
        })

    # 強制平倉剩餘持倉
    last_date = trading_days[-1]
    for code, pos in list(positions.items()):
        ind = all_stock_ind.get(code, {}).get(last_date)
        if not ind: continue
        pnl = (ind["price"] - pos["buy_price"]) / pos["buy_price"] * 100
        capital *= (1 + (1/max_pos) * pnl/100)
        all_trades.append({
            "code": code, "buy_date": pos["buy_date"],
            "buy_price": round(pos["buy_price"], 2),
            "peak_price": round(pos["peak_price"], 2),
            "sell_date": last_date, "sell_price": round(ind["price"], 2),
            "sell_reason": "回測結束平倉",
            "pnl": round(pnl, 2),
            "held_days": len(trading_days)-1-pos["buy_day_idx"],
        })

    return all_trades, daily_eq, day_signals

# ══════════════════════════════════════════════════════
# API 路由 - 選股
# ══════════════════════════════════════════════════════

# ── 選股背景任務 ─────────────────────────────────
_stocks_tasks = {}

@app.route("/api/stocks/start", methods=["POST"])
def stocks_start():
    import uuid
    task_id = str(uuid.uuid4())[:8]
    _stocks_tasks[task_id] = {"pct":0,"msg":"準備中...","done":False,"result":None,"error":None}

    def run_bg():
        try:
            prog = _stocks_tasks[task_id]
            def cb(msg, pct):
                prog["msg"]=msg; prog["pct"]=round(pct,1)

            all_rows = []

            # ── 上市（TWSE）──────────────────────────
            cb("從 TWSE 取得上市股票...", 2)
            try:
                url  = "https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL"
                resp = SESSION.get(url, timeout=15); resp.raise_for_status()
                for r in resp.json():
                    code  = r.get("Code","")
                    price = safe_float(r.get("ClosingPrice"))
                    if not (str(code).isdigit() and len(code)==4 and price>0): continue
                    all_rows.append({**r, "market":"上市"})
                print(f"  上市：{len(all_rows)} 支")
            except Exception as e:
                prog["error"] = f"上市資料取得失敗: {e}"; prog["done"]=True; return

            # ── 上櫃（OTC）───────────────────────────
            cb("從 TPEX 取得上櫃股票...", 5)
            otc_count = 0
            try:
                otc_url = "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_quotes"
                r2 = SESSION.get(otc_url, timeout=15); r2.raise_for_status()
                for r in r2.json():
                    code  = str(r.get("SecuritiesCompanyCode","") or r.get("code","")).strip()
                    price = safe_float(r.get("Close","") or r.get("close",""))
                    if not (str(code).isdigit() and len(code) in [4,5] and price>0): continue
                    all_rows.append({
                        "Code":  code,
                        "Name":  r.get("CompanyName","") or r.get("name",""),
                        "ClosingPrice": r.get("Close","0") or r.get("close","0"),
                        "HighestPrice": r.get("High","0")  or r.get("high","0"),
                        "LowestPrice":  r.get("Low","0")   or r.get("low","0"),
                        "Change":       r.get("Change","0") or r.get("change","0"),
                        "TradeVolume":  str(safe_float(r.get("TradingShares","0") or r.get("volume","0"))),
                        "market": "上櫃",
                    })
                    otc_count += 1
                print(f"  上櫃：{otc_count} 支")
            except Exception as e:
                print(f"  上櫃失敗（只用上市）: {e}")

            total = len(all_rows)
            cb(f"取得 {total} 支（上市+上櫃），計算指標中...", 8)

            stocks = []
            for i, row in enumerate(all_rows):
                code   = row.get("Code","")
                market = row.get("market","上市")
                if i % 100 == 0:
                    pct = 8 + i/total*88
                    cb(f"計算指標 {i+1}/{total}（{market}）...", pct)
                try:
                    hist = fetch_history_recent(code)
                    if len(hist) < 5: continue
                    price   = safe_float(row.get("ClosingPrice"))
                    high_p  = safe_float(row.get("HighestPrice"))
                    low_p   = safe_float(row.get("LowestPrice"))
                    change  = safe_float(row.get("Change"))
                    vol_raw = safe_float(row.get("TradeVolume"))
                    if price <= 0: continue
                    today_vol = round(vol_raw / 1000)
                    closes = [h["close"] for h in hist]
                    highs  = [h["high"]  for h in hist]
                    lows   = [h["low"]   for h in hist]
                    vols   = [h["vol"]   for h in hist]
                    all_c  = closes + [price]
                    all_h  = highs  + [high_p or price]
                    all_l  = lows   + [low_p  or price]
                    all_v  = vols   + [today_vol]
                    prev_c  = closes[-1] if closes else price
                    chg_pct = round((price-prev_c)/prev_c*100,2) if prev_c>0 else 0
                    ma5    = round(sma(all_c,5),2)
                    ma20   = round(sma(all_c,20),2)
                    ma60   = round(sma(all_c,60),2)
                    avg5v  = round(sma(all_v[:-1],5))
                    avg20v = round(sma(all_v[:-1],20))
                    k_s,d_s = calc_kd_series(all_c,all_h,all_l,9)
                    kv,dv   = k_s[-1],d_s[-1]
                    pkv = k_s[-2] if len(k_s)>=2 else 50.0
                    pdv = d_s[-2] if len(d_s)>=2 else 50.0
                    rsi14   = calc_rsi_series(all_c,14)[-1]
                    rsi5_s  = calc_rsi_series(all_c,5)
                    rsi10_s = calc_rsi_series(all_c,10)
                    rsi5    = rsi5_s[-1]
                    rsi10   = rsi10_s[-1]
                    prev_rsi5  = rsi5_s[-2]  if len(rsi5_s)>=2  else rsi5
                    prev_rsi10 = rsi10_s[-2] if len(rsi10_s)>=2 else rsi10
                    stocks.append({
                        "code":code,"name":row.get("Name",""),
                        "sector":market,"price":price,
                        "chgPct":chg_pct,"chgAmt":change,
                        "todayVol":today_vol,"avg5Vol":avg5v,"avg20Vol":avg20v,
                        "volVsAvg5":  round(today_vol/avg5v,2)  if avg5v>0  else 0,
                        "volVsAvg20": round(today_vol/avg20v,2) if avg20v>0 else 0,
                        "priceVsMA20":round((price-ma20)/ma20*100,2) if ma20>0 else 0,
                        "priceVsMA60":round((price-ma60)/ma60*100,2) if ma60>0 else 0,
                        "ma20VsMA60": round((ma20-ma60)/ma60*100,2)  if ma60>0 else 0,
                        "ma5VsMA20":  round((ma5-ma20)/ma20*100,2)   if ma20>0 else 0,
                        "kVal":kv,"dVal":dv,"prevK":pkv,"prevD":pdv,
                        "rsi14":rsi14,
                        "rsi5":round(rsi5,1),"rsi10":round(rsi10,1),
                        "prevRsi5":round(prev_rsi5,1),"prevRsi10":round(prev_rsi10,1),
                        "spark":all_c[-20:],"isLive":True,
                    })
                except Exception as e:
                    pass

            cb("完成！", 100)
            prog.update({
                "pct":100,"msg":f"完成！共 {len(stocks)} 支","done":True,
                "result":{
                    "stocks": stocks,
                    "count":  len(stocks),
                    "time":   datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                }
            })
            print(f"  ✅ 選股完成！上市+上櫃共 {len(stocks)} 支")

        except Exception as e:
            import traceback; traceback.print_exc()
            _stocks_tasks[task_id].update({"done":True,"error":str(e)})

    threading.Thread(target=run_bg, daemon=True).start()
    return jsonify({"task_id": task_id})

@app.route("/api/stocks/progress/<task_id>")
def stocks_progress(task_id):
    prog = _stocks_tasks.get(task_id)
    if not prog: return jsonify({"error":"找不到任務"}), 404
    return jsonify(prog)

@app.route("/api/stocks")
def get_stocks():
    """向後相容，導向新的非同步版本說明"""
    return jsonify({"error": "請使用 /api/stocks/start 非同步版本"}), 400

# ══════════════════════════════════════════════════════
# API 路由 - 單股回測
# ══════════════════════════════════════════════════════

@app.route("/api/backtest", methods=["POST"])
def backtest():
    body          = request.get_json()
    code          = body.get("code","").strip()
    start_date    = body.get("start_date","")
    end_date      = body.get("end_date","")
    conditions    = body.get("conditions",[])
    take_profit   = float(body.get("take_profit",0))
    stop_loss     = float(body.get("stop_loss",0))
    hold_days     = int(body.get("hold_days",0))
    trailing_stop = float(body.get("trailing_stop",0))
    ma_sell_period    = int(body.get("ma_sell_period",0))
    use_market_filter = bool(body.get("use_market_filter", False))
    market_ma         = int(body.get("market_ma", 60))
    use_atr_stop      = bool(body.get("use_atr_stop", False))
    atr_multiplier    = float(body.get("atr_multiplier", 2.0))
    partial_exit_pct  = float(body.get("partial_exit_pct", 0))
    partial_exit_ratio= int(body.get("partial_exit_ratio", 50))
    margin_ratio      = float(body.get("margin_ratio", 1.0))  # 融資倍數，1=不融資，1.6=六成融資

    if not code or not start_date or not end_date:
        return jsonify({"error":"請填入股票代號與日期範圍"}), 400

    print(f"\n[單股回測] {code} {start_date}~{end_date} "
          f"大盤過濾:{use_market_filter} ATR停損:{use_atr_stop} 分批:{partial_exit_pct}%")
    records = fetch_history_range(code, start_date, end_date)
    if not records:
        return jsonify({"error":f"查無 {code} 的歷史資料"}), 404

    trades, daily_equity = run_single_backtest(
        records, conditions, start_date, end_date,
        take_profit, stop_loss, hold_days, trailing_stop, ma_sell_period,
        use_market_filter, market_ma, use_atr_stop, atr_multiplier,
        partial_exit_pct, partial_exit_ratio)

    # 融資倍數套用（放大損益，但不影響交易邏輯）
    # 融資成本：年利率約 6.35%，每日約 0.017%
    daily_cost = 0.00017 * (margin_ratio - 1)  # 只有超過1的部分付利息
    adj_trades = []
    for t in trades:
        adj_pnl = t["pnl"] * margin_ratio - daily_cost * t["held_days"] * 100
        adj_trades.append({**t, "pnl_margin": round(adj_pnl, 2),
                           "pnl_raw": t["pnl"]})

    wins  = [t for t in adj_trades if t["pnl_margin"]>0 and not t.get("partial")]
    loses = [t for t in adj_trades if t["pnl_margin"]<=0 and not t.get("partial")]
    peak_eq = 100.0; max_draw = 0.0
    for d in daily_equity:
        if d["equity"]>peak_eq: peak_eq=d["equity"]
        dd=(peak_eq-d["equity"])/peak_eq*100
        if dd>max_draw: max_draw=dd

    closes = [r["close"] for r in records]
    highs  = [r["high"]  for r in records]
    lows   = [r["low"]   for r in records]
    k_ser, d_ser = calc_kd_series(closes, highs, lows, 9)
    rsi_ser = calc_rsi_series(closes, 14)
    kline = []
    for i, r in enumerate(records):
        if r["date"] < start_date or r["date"] > end_date: continue
        kline.append({
            "date":r["date"],"open":r["open"],"high":r["high"],
            "low":r["low"],"close":r["close"],"vol":r["vol"],
            "ma5":  round(sma(closes[max(0,i-4):i+1],  5),2),
            "ma20": round(sma(closes[max(0,i-19):i+1], 20),2),
            "ma60": round(sma(closes[max(0,i-59):i+1], 60),2),
            "k":k_ser[i],"d":d_ser[i],"rsi":rsi_ser[i],
        })

    full_trades = [t for t in adj_trades if not t.get("partial")]
    total_pnl   = sum(t["pnl_margin"] for t in full_trades)
    return jsonify({
        "code":code,"start_date":start_date,"end_date":end_date,
        "kline":kline,"trades":adj_trades,"daily_equity":daily_equity,
        "margin_ratio": margin_ratio,
        "stats":{
            "total_trades":len(full_trades),
            "win_trades":len(wins),"lose_trades":len(loses),
            "win_rate":round(len(wins)/len(full_trades)*100,1) if full_trades else 0,
            "total_pnl":round(total_pnl,2),
            "total_pnl_raw":round(sum(t["pnl"] for t in full_trades),2),
            "avg_win": round(sum(t["pnl_margin"] for t in wins)/len(wins),2)  if wins  else 0,
            "avg_loss":round(sum(t["pnl_margin"] for t in loses)/len(loses),2) if loses else 0,
            "max_drawdown":round(max_draw,2),
            "margin_ratio": margin_ratio,
        }
    })

# ══════════════════════════════════════════════════════
# API 路由 - 全市場回測（背景執行）
# ══════════════════════════════════════════════════════

import json, os, uuid as _uuid

TASKS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "tasks")
os.makedirs(TASKS_DIR, exist_ok=True)

def task_path(task_id):
    return os.path.join(TASKS_DIR, f"{task_id}.json")

def save_task(task_id, data):
    try:
        # result 可能很大，先存再讀沒問題
        with open(task_path(task_id), "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
    except Exception as e:
        print(f"[task] 儲存失敗: {e}")

def load_task(task_id):
    p = task_path(task_id)
    if not os.path.exists(p):
        return None
    try:
        with open(p, encoding="utf-8") as f:
            return json.load(f)
    except:
        return None

# 記憶體快取（加速輪詢，同時有檔案備份）
_market_tasks = {}

@app.route("/api/market_backtest", methods=["POST"])
def market_backtest():
    body          = request.get_json()
    start_date    = body.get("start_date","")
    end_date      = body.get("end_date","")
    conditions    = body.get("conditions",[])
    take_profit   = float(body.get("take_profit",0))
    stop_loss     = float(body.get("stop_loss",0))
    hold_days     = int(body.get("hold_days",0))
    trailing_stop = float(body.get("trailing_stop",0))
    max_pos       = int(body.get("max_positions",5))
    max_stocks    = int(body.get("max_stocks",300))
    ma_sell_period= int(body.get("ma_sell_period",0))

    if not start_date or not end_date or not conditions:
        return jsonify({"error":"請填入日期範圍與篩選條件"}), 400

    task_id = str(_uuid.uuid4())[:8]
    init_state = {"pct":0,"msg":"準備中...","done":False,"result":None,"error":None}
    _market_tasks[task_id] = init_state
    save_task(task_id, init_state)

    def bg():
        try:
            import random

            def cb(msg, pct):
                state = {"pct":round(pct,1),"msg":msg,"done":False,"result":None,"error":None}
                _market_tasks[task_id] = state
                save_task(task_id, state)
                print(f"  [{pct:.0f}%] {msg}")

            cb("從 TWSE 取得股票清單...", 2)
            url  = "https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL"
            resp = SESSION.get(url, timeout=15)
            resp.raise_for_status()
            all_codes = [
                r["Code"] for r in resp.json()
                if str(r.get("Code","")).isdigit()
                and len(str(r.get("Code",""))) == 4
                and safe_float(r.get("ClosingPrice")) > 0
            ]
            if max_stocks >= len(all_codes):
                codes = all_codes
            else:
                codes = random.sample(all_codes, max_stocks)

            cb(f"掃描 {len(codes)} 支股票（上市共 {len(all_codes)} 支）...", 5)
            trades, daily_eq, day_signals = run_market_backtest(
                codes, conditions, start_date, end_date,
                take_profit, stop_loss, hold_days, trailing_stop, max_pos, cb,
                ma_sell_period=ma_sell_period)

            cb("計算績效統計...", 96)
            wins  = [t for t in trades if t["pnl"]>0]
            loses = [t for t in trades if t["pnl"]<=0]
            peak_eq=100.0; max_draw=0.0
            for d in daily_eq:
                if d["equity"]>peak_eq: peak_eq=d["equity"]
                dd=(peak_eq-d["equity"])/peak_eq*100
                if dd>max_draw: max_draw=dd

            final = {
                "pct": 100, "msg": "完成！", "done": True, "error": None,
                "result": {
                    "start_date":start_date,"end_date":end_date,
                    "stocks_tested":len(codes),
                    "trades":sorted(trades,key=lambda x:x["buy_date"]),
                    "daily_equity":daily_eq,
                    "stats":{
                        "total_trades":len(trades),
                        "win_trades":len(wins),"lose_trades":len(loses),
                        "win_rate":round(len(wins)/len(trades)*100,1) if trades else 0,
                        "total_return":round(daily_eq[-1]["equity"]-100,2) if daily_eq else 0,
                        "avg_win": round(sum(t["pnl"] for t in wins)/len(wins),2)  if wins  else 0,
                        "avg_loss":round(sum(t["pnl"] for t in loses)/len(loses),2) if loses else 0,
                        "max_drawdown":round(max_draw,2),
                        "max_positions":max_pos,
                    }
                }
            }
            _market_tasks[task_id] = final
            save_task(task_id, final)

        except Exception as e:
            import traceback; traceback.print_exc()
            err_state = {"pct":0,"msg":str(e),"done":True,"result":None,"error":str(e)}
            _market_tasks[task_id] = err_state
            save_task(task_id, err_state)

    threading.Thread(target=bg, daemon=True).start()
    return jsonify({"task_id": task_id})

@app.route("/api/market_backtest/progress/<task_id>")
def market_backtest_progress(task_id):
    # 先查記憶體，沒有再讀檔案（Render 重啟後仍可繼續輪詢）
    prog = _market_tasks.get(task_id) or load_task(task_id)
    if not prog:
        return jsonify({"error":"找不到任務，可能因伺服器重啟而遺失，請重新開始回測"}), 404
    return jsonify(prog)

# ══════════════════════════════════════════════════════
# API 路由 - 隨機森林預測（升級版）
# ══════════════════════════════════════════════════════

# FinMind 資料快取
_finmind_cache = {}
FINMIND_URL    = "https://api.finmindtrade.com/api/v4/data"

FINMIND_TOKEN_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "finmind_token.json")

def _get_finmind_token():
    """讀取 FinMind Token：優先環境變數，其次本地檔案"""
    import os
    token = os.environ.get("FINMIND_TOKEN", "")
    if token: return token
    try:
        if os.path.exists(FINMIND_TOKEN_FILE):
            with open(FINMIND_TOKEN_FILE, encoding="utf-8") as f:
                return json.load(f).get("token", "")
    except: pass
    return ""

def _save_finmind_token(token):
    with open(FINMIND_TOKEN_FILE, "w", encoding="utf-8") as f:
        json.dump({"token": token}, f)

@app.route("/api/settings/finmind_token", methods=["GET"])
def get_finmind_token_api():
    token = _get_finmind_token()
    masked = token[:6]+"****"+token[-4:] if len(token) > 10 else ("已設定" if token else "")
    return jsonify({"has_token": bool(token), "token": bool(token), "masked": masked})

@app.route("/api/settings/finmind_token/test", methods=["POST"])
def test_finmind_token_api():
    """測試 FinMind Token 是否有效"""
    token = _get_finmind_token()
    if not token:
        return jsonify({"ok": False, "msg": "尚未設定 Token，請先到警報設定頁面填入"})
    try:
        r = SESSION.get(FINMIND_URL, params={
            "dataset": "TaiwanStockInfo",
        }, headers={"Authorization": f"Bearer {token}"}, timeout=12)
        d = r.json()
        status = d.get("status", 0)
        if status == 200:
            return jsonify({"ok": True, "msg": "FinMind 連線成功！籌碼資料可用"})
        elif status == 402:
            return jsonify({"ok": True, "msg": "Token 有效，但已達免費使用量上限（明日重置）"})
        elif status == 401:
            return jsonify({"ok": False, "msg": "Token 無效，請重新申請"})
        else:
            return jsonify({"ok": False, "msg": f"連線異常（狀態碼 {status}）"})
    except Exception as e:
        return jsonify({"ok": False, "msg": f"連線失敗：{str(e)}"})

@app.route("/api/settings/finmind_token", methods=["POST"])
def set_finmind_token_api():
    body  = request.get_json()
    token = body.get("token","").strip()
    if not token: return jsonify({"error": "Token 不能為空"}), 400
    # 用 TaiwanStockInfo 驗證（不需要 data_id，流量少）
    try:
        r = SESSION.get(FINMIND_URL, params={
            "dataset": "TaiwanStockInfo",
        }, headers={"Authorization": f"Bearer {token}"}, timeout=12)
        d = r.json()
        status = d.get("status", 0)
        if status != 200:
            msg = d.get("msg", "未知錯誤")
            # 常見狀況：402 是超過使用量，401 是 token 無效
            if status == 401:
                return jsonify({"error": "Token 無效，請重新確認"}), 400
            elif status == 402:
                # 超過免費額度但 token 本身有效，仍允許儲存
                pass
            else:
                return jsonify({"error": f"驗證失敗（{status}）：{msg}"}), 400
    except Exception as e:
        return jsonify({"error": f"無法連線到 FinMind：{str(e)}"}), 400
    _save_finmind_token(token)
    _finmind_cache.clear()
    return jsonify({"ok": True, "msg": "FinMind Token 已儲存，下次預測將自動使用法人資料！"})

def fetch_finmind(dataset, code, start_date, end_date=""):
    """
    通用 FinMind API 呼叫
    - 無 token：免費版 300次/小時
    - 有 token：600次/小時
    """
    cache_key = f"{dataset}_{code}_{start_date[:7]}"
    if cache_key in _finmind_cache:
        return _finmind_cache[cache_key]

    params = {
        "dataset":    dataset,
        "data_id":    code,
        "start_date": start_date,
    }
    if end_date:
        params["end_date"] = end_date

    token = _get_finmind_token()
    headers = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    try:
        r = SESSION.get(FINMIND_URL, params=params, headers=headers, timeout=15)
        data = r.json()
        if data.get("status") == 200:
            result = data.get("data", [])
            _finmind_cache[cache_key] = result
            print(f"  [FinMind] {dataset} {code}: {len(result)} 筆")
            return result
        else:
            msg = data.get("msg","unknown")
            print(f"  [FinMind] {dataset} {code} 失敗: {msg}")
            return []
    except Exception as e:
        print(f"  [FinMind] {dataset} {code} 例外: {e}")
        return []

def fetch_institutional_finmind(code, start_date, end_date):
    """
    FinMind 三大法人買賣超
    dataset: TaiwanStockInstitutionalInvestorsBuySell
    欄位: date, stock_id, name(外資/投信/自營商), buy, sell
    回傳: {date: {foreign_net, trust_net, dealer_net, total_net}}
    """
    rows = fetch_finmind("TaiwanStockInstitutionalInvestorsBuySell",
                         code, start_date, end_date)
    result = {}
    for row in rows:
        date = row.get("date","")[:10]
        name = row.get("name","")
        buy  = int(str(row.get("buy",0)).replace(",","") or 0)
        sell = int(str(row.get("sell",0)).replace(",","") or 0)
        net  = buy - sell
        if date not in result:
            result[date] = {"foreign_net":0,"trust_net":0,"dealer_net":0,"total_net":0}
        if "外資" in name:
            result[date]["foreign_net"] += net
        elif "投信" in name:
            result[date]["trust_net"]   += net
        elif "自營" in name:
            result[date]["dealer_net"]  += net
        result[date]["total_net"] = (result[date]["foreign_net"] +
                                     result[date]["trust_net"]   +
                                     result[date]["dealer_net"])
    return result

def fetch_per_finmind(code, start_date):
    """
    FinMind PER/PBR/殖利率
    dataset: TaiwanStockPER
    欄位: date, stock_id, PER, PBR, DividendYield
    回傳: {date: {per, pbr, yield_pct}}
    """
    rows = fetch_finmind("TaiwanStockPER", code, start_date)
    result = {}
    for row in rows:
        date = row.get("date","")[:10]
        result[date] = {
            "per":        float(row.get("PER",0)  or 0),
            "pbr":        float(row.get("PBR",0)  or 0),
            "yield_pct":  float(row.get("DividendYield",0) or 0),
        }
    return result

# 分析師評等特徵名稱對應
ANALYST_LABELS = {-2:"強烈賣出", -1:"賣出", 0:"中立", 1:"買入", 2:"強烈買入"}

def _analyst_score_finmind(closes, highs, lows, vols, i,
                            inst_map=None, per_map=None, dates=None):
    """
    結合 FinMind 真實資料的分析師評等模型：
    1. 法人連續買超方向（外資、投信主導）
    2. 基本面評估（PER、殖利率是否合理）
    3. 技術面趨勢強度

    回傳：(score, tp_pct, detail_dict)
    """
    if i < 60: return 0, 0, {}

    def ma(n): return sma(closes[max(0,i-n+1):i+1], n)
    def vma(n): return sma(vols[max(0,i-n+1):i+1], n)

    ma20  = ma(20); ma60 = ma(60); ma120 = ma(120) if i >= 120 else ma(60)
    c     = closes[i]
    score = 0
    details = {}

    # ① 技術面趨勢（-3 ~ +3）
    tech = 0
    if c > ma20 > ma60:     tech += 1
    if ma20 > ma60 > ma120: tech += 1
    if c < ma20 < ma60:     tech -= 1
    if ma20 < ma60 < ma120: tech -= 1
    # 季線突破
    if i > 0:
        if c > ma60 and closes[i-1] <= ma(60): tech += 1
        if c < ma60 and closes[i-1] >= ma(60): tech -= 1
    # 量能確認
    v20 = vma(20)
    if v20 > 0 and i > 0:
        vol_r = vols[i] / v20
        daily_ret = (c - closes[i-1]) / closes[i-1]
        if daily_ret > 0.02 and vol_r > 1.5:  tech += 1
        if daily_ret < -0.02 and vol_r > 1.5: tech -= 1
    score += max(-2, min(2, tech))
    details["技術評分"] = tech

    # ② 法人動向（使用 FinMind 資料）
    inst_score = 0
    if inst_map and dates and i < len(dates):
        # 取近 10 個交易日的法人買賣超
        cur_date = dates[i]
        recent_dates = sorted([d for d in inst_map.keys() if d <= cur_date])[-10:]
        if recent_dates:
            foreign_sum = sum(inst_map[d].get("foreign_net",0) for d in recent_dates)
            trust_sum   = sum(inst_map[d].get("trust_net",  0) for d in recent_dates)
            total_sum   = sum(inst_map[d].get("total_net",  0) for d in recent_dates)
            # 外資連續買超：最重要訊號
            if foreign_sum > 5000:   inst_score += 2   # 大買超 (5000張以上)
            elif foreign_sum > 1000: inst_score += 1
            elif foreign_sum < -5000: inst_score -= 2
            elif foreign_sum < -1000: inst_score -= 1
            # 投信加碼
            if trust_sum > 500:   inst_score += 1
            elif trust_sum < -500: inst_score -= 1
            details["外資10日"] = round(foreign_sum/1000,1)
            details["投信10日"] = round(trust_sum/1000,1)
        score += max(-2, min(2, inst_score))
    details["法人評分"] = inst_score

    # ③ 基本面評估（使用 FinMind PER 資料）
    fund_score = 0
    if per_map and dates and i < len(dates):
        cur_date = dates[i]
        recent_per_dates = sorted([d for d in per_map.keys() if d <= cur_date])
        if recent_per_dates:
            latest = per_map[recent_per_dates[-1]]
            per    = latest.get("per", 0)
            pbr    = latest.get("pbr", 0)
            dy     = latest.get("yield_pct", 0)
            # PER 合理範圍評估（台股平均約15~20倍）
            if 0 < per < 15:   fund_score += 1   # 低本益比：便宜
            elif per > 30:     fund_score -= 1   # 高本益比：貴
            # 殖利率
            if dy > 5:         fund_score += 1   # 高殖利率：支撐股價
            elif dy < 1 and dy > 0: fund_score -= 1
            # 股淨比
            if 0 < pbr < 1:    fund_score += 1   # 跌破淨值：潛在價值
            elif pbr > 3:      fund_score -= 1
            details["PER"]    = per
            details["殖利率"] = dy
        score += max(-1, min(1, fund_score))   # 基本面影響稍小
    details["基本面評分"] = fund_score

    # 總評 -2 ~ +2
    score = max(-2, min(2, score))

    # 目標價推算（基於趨勢斜率）
    slope = 0
    if i >= 20 and closes[i-20] > 0:
        slope = (c - closes[i-20]) / closes[i-20] / 20
    tp_pct = round(slope * 60 * 100, 1)

    return score, tp_pct, details

FEATURE_NAMES = [
    # 動量（5）
    "K值","D值","KD差","RSI(14)","RSI超買賣",
    # 趨勢（4）
    "MACD","MACD方向","布林位置","布林寬度",
    # 均線（7）
    "距MA5(%)","距MA10(%)","距MA20(%)","距MA60(%)",
    "MA5vsMA20","MA20vsMA60","MA20vsMA120",
    # 量能（3）
    "量比5日","量比20日","量能趨勢",
    # 價格動能（4）
    "近3日漲跌","近5日漲跌","近10日漲跌","近20日漲跌",
    # 波動（2）
    "ATR%","高低振幅%",
    # K線型態（3）
    "實體大小","上影線","下影線",
    # 連續性（2）
    "連漲天數","連跌天數",
    # 三大法人-FinMind（5）
    "外資買賣超(千張)","投信買賣超(千張)","自營買賣超(千張)",
    "法人合計(千張)","外資強度",
    # 分析師評等-FinMind（4）
    "綜合評等分","法人評分","基本面評分","目標漲跌幅%",
]  # 共 39 個特徵

def _build_all_features(closes, highs, lows, vols,
                        inst_map=None, per_map=None, dates=None):
    """升級版：39個特徵，整合 FinMind 三大法人 + 基本面 + 分析師評等"""
    n = len(closes)
    k_ser, d_ser = calc_kd_series(closes, highs, lows, 9)
    rsi_ser      = calc_rsi_series(closes, 14)
    atr_ser      = calc_atr(highs, lows, closes, 14)
    def ema_s(arr, p):
        k2=2/(p+1); r=[arr[0]]
        for v in arr[1:]: r.append(v*k2+r[-1]*(1-k2))
        return r
    ema12=ema_s(closes,12); ema26=ema_s(closes,26)
    macd_ser=[ema12[i]-ema26[i] for i in range(n)]

    features=[]
    for i in range(n):
        if i<60: features.append(None); continue
        c=closes[i]; h=highs[i]; l=lows[i]
        def ma(nn): return sma(closes[max(0,i-nn+1):i+1],nn)
        def vma(nn): return sma(vols[max(0,i-nn+1):i+1],nn)
        ma5=ma(5);ma10=ma(10);ma20=ma(20);ma60=ma(60)
        ma120=ma(120) if i>=120 else ma60
        v5=vma(5);v20=vma(20)
        kv=k_ser[i];dv=d_ser[i]
        rsi=rsi_ser[i]
        rsi_ob=1 if rsi>70 else (-1 if rsi<30 else 0)
        macd_v=macd_ser[i]
        macd_dir=1 if i>0 and macd_ser[i]>macd_ser[i-1] else -1
        sl20=closes[max(0,i-19):i+1]
        bm=sum(sl20)/len(sl20)
        bstd=(sum((x-bm)**2 for x in sl20)/len(sl20))**0.5+1e-9
        boll=(c-bm)/(bstd*2)
        bwid=round(bstd*4/bm*100,3) if bm>0 else 0
        atr_p=round(atr_ser[i]/c*100,3) if c>0 else 0
        vr5=round(vols[i]/v5,3) if v5>0 else 1
        vr20=round(vols[i]/v20,3) if v20>0 else 1
        vtrend=1 if v5>v20 else -1
        def ret(n2): return round((c-closes[i-n2])/closes[i-n2]*100,2) if i>=n2 and closes[i-n2]>0 else 0
        hl_p=round((h-l)/c*100,3) if c>0 else 0
        body=round(abs(c-closes[i-1])/c*100,3) if i>0 and c>0 else 0
        ush=round((h-max(c,closes[i-1]))/c*100,3) if i>0 and c>0 else 0
        lsh=round((min(c,closes[i-1])-l)/c*100,3) if i>0 and c>0 else 0
        up_d=dn_d=0
        for j in range(i-1,max(i-10,-1),-1):
            if j<1: break
            if closes[j]>closes[j-1]: up_d+=1
            else: break
        for j in range(i-1,max(i-10,-1),-1):
            if j<1: break
            if closes[j]<closes[j-1]: dn_d+=1
            else: break

        # 三大法人特徵（FinMind 真實資料）
        foreign_acc=trust_acc=dealer_acc=total_acc=0
        if inst_map and dates and i<len(dates):
            cur_date = dates[i]
            recent = sorted([d for d in inst_map.keys() if d<=cur_date])[-5:]
            for d in recent:
                rec = inst_map.get(d,{})
                foreign_acc += rec.get("foreign_net",0)
                trust_acc   += rec.get("trust_net",0)
                dealer_acc  += rec.get("dealer_net",0)
                total_acc   += rec.get("total_net",0)
        avg_vol    = vols[i] if vols[i]>0 else 1
        foreign_r  = round(foreign_acc/avg_vol, 3) if avg_vol>0 else 0

        # 分析師評等
        a_score, tp_pct, a_det = _analyst_score_finmind(
            closes, highs, lows, vols, i, inst_map, per_map, dates)
        inst_s = a_det.get("法人評分",0)
        fund_s = a_det.get("基本面評分",0)

        features.append([
            round(kv,1),round(dv,1),round(kv-dv,2),round(rsi,1),rsi_ob,
            round(macd_v,4),macd_dir,round(boll,4),bwid,
            round((c-ma5)/ma5*100,2)   if ma5>0   else 0,
            round((c-ma10)/ma10*100,2) if ma10>0  else 0,
            round((c-ma20)/ma20*100,2) if ma20>0  else 0,
            round((c-ma60)/ma60*100,2) if ma60>0  else 0,
            round((ma5-ma20)/ma20*100,2)    if ma20>0  else 0,
            round((ma20-ma60)/ma60*100,2)   if ma60>0  else 0,
            round((ma20-ma120)/ma120*100,2) if ma120>0 else 0,
            vr5,vr20,vtrend,
            ret(3),ret(5),ret(10),ret(20),
            atr_p,hl_p,body,ush,lsh,up_d,dn_d,
            round(foreign_acc/1000,1), round(trust_acc/1000,1),
            round(dealer_acc/1000,1),  round(total_acc/1000,1),
            round(foreign_r,3),
            a_score, inst_s, fund_s, round(tp_pct,2),
        ])
    return features

def simple_decision_tree(X_train, y_train, max_depth=6, max_features=None):
    import random as _r
    n_feat=len(X_train[0]) if X_train else 1
    if max_features is None: max_features=n_feat
    max_features=min(max_features,n_feat)
    def gini(labels):
        n=len(labels)
        if n==0: return 0
        counts={}
        for l in labels: counts[l]=counts.get(l,0)+1
        return 1-sum((v/n)**2 for v in counts.values())
    def best_split(X,y):
        bg,bf,bt=float("inf"),0,0; n=len(y)
        fi=_r.sample(range(n_feat),max_features)
        for f in fi:
            vals=sorted(set(x[f] for x in X))
            if len(vals)>20:
                step=len(vals)//20; vals=vals[::step]
            for ii in range(len(vals)-1):
                t=(vals[ii]+vals[ii+1])/2
                ly=[y[j] for j in range(n) if X[j][f]<=t]
                ry=[y[j] for j in range(n) if X[j][f]>t]
                if not ly or not ry: continue
                g=(len(ly)*gini(ly)+len(ry)*gini(ry))/n
                if g<bg: bg,bf,bt=g,f,t
        return bf,bt
    def majority(labels):
        counts={}
        for l in labels: counts[l]=counts.get(l,0)+1
        return max(counts,key=counts.get),counts
    def build(X,y,depth):
        if depth>=max_depth or len(set(y))==1 or len(y)<8:
            label,counts=majority(y)
            return {"leaf":True,"label":label,"counts":counts,"n":len(y)}
        f,t=best_split(X,y)
        lX=[X[j] for j in range(len(y)) if X[j][f]<=t]
        ly=[y[j] for j in range(len(y)) if X[j][f]<=t]
        rX=[X[j] for j in range(len(y)) if X[j][f]>t]
        ry=[y[j] for j in range(len(y)) if X[j][f]>t]
        if not ly or not ry:
            label,counts=majority(y)
            return {"leaf":True,"label":label,"counts":counts,"n":len(y)}
        return {"leaf":False,"feature":f,"threshold":t,
                "left":build(lX,ly,depth+1),"right":build(rX,ry,depth+1),"n":len(y)}
    return build(X_train,y_train,0)

def predict_tree(tree,x):
    if tree["leaf"]:
        counts=tree["counts"]; total=sum(counts.values())
        return tree["label"],round(counts.get(tree["label"],0)/total*100,1),counts
    if x[tree["feature"]]<=tree["threshold"]: return predict_tree(tree["left"],x)
    else: return predict_tree(tree["right"],x)

def random_forest_predict(trees,x):
    votes={}; probs={}
    for tree in trees:
        label,conf,counts=predict_tree(tree,x)
        votes[label]=votes.get(label,0)+1
        total=sum(counts.values())
        for l,c in counts.items(): probs[l]=probs.get(l,0)+c/total
    best=max(votes,key=votes.get)
    nt=len(trees)
    return best,round(votes[best]/nt*100,1),{l:round(probs[l]/nt*100,1) for l in probs}

def calc_feature_importance(tree,n_features):
    importance=[0.0]*n_features
    def traverse(node):
        if node["leaf"]: return
        importance[node["feature"]]+=node["n"]
        traverse(node["left"]); traverse(node["right"])
    traverse(tree)
    total=sum(importance) or 1
    return [round(v/total,4) for v in importance]

def calc_forest_importance(trees,n_features):
    total_imp=[0.0]*n_features
    for tree in trees:
        imp=calc_feature_importance(tree,n_features)
        for i,v in enumerate(imp): total_imp[i]+=v
    s=sum(total_imp) or 1
    return [round(v/s,4) for v in total_imp]

def oversample_minority(X,y,target_ratio=0.8):
    import random as _r
    counts={}
    for label in y: counts[label]=counts.get(label,0)+1
    max_count=max(counts.values()); target=int(max_count*target_ratio)
    X_new,y_new=list(X),list(y)
    for label,count in counts.items():
        if count<target:
            indices=[j for j,yj in enumerate(y) if yj==label]
            needed=target-count
            for _ in range(needed):
                X_new.append(X[_r.choice(indices)]); y_new.append(label)
    return X_new,y_new

_predict_tasks={}

@app.route("/api/predict",methods=["POST"])
def predict_start():
    import uuid as _u,random as _r
    body=request.get_json()
    code=body.get("code","").strip()
    train_years=int(body.get("train_years",2))
    pred_days=int(body.get("predict_days",10))
    threshold=float(body.get("threshold",3))
    n_trees=int(body.get("n_trees",15))
    # 自訂特徵索引（前端傳入選取的特徵索引列表，空=全用）
    selected_feat_idx = body.get("selected_features", [])
    if not code: return jsonify({"error":"請填入股票代號"}),400
    task_id=str(_u.uuid4())[:8]
    _predict_tasks[task_id]={"pct":0,"msg":"準備中...","done":False,"result":None,"error":None}
    def bg():
        prog=_predict_tasks[task_id]
        def cb(msg,pct):
            prog["msg"]=msg; prog["pct"]=round(pct,1)
            print(f"  [預測{pct:.0f}%] {msg}")
        try:
            cb("抓取歷史資料...",5)
            end_dt=datetime.today()
            start_dt=end_dt-timedelta(days=train_years*365+90)
            start_str=start_dt.strftime("%Y-%m-%d")
            end_str  =end_dt.strftime("%Y-%m-%d")
            records=fetch_history_range(code, start_str, end_str)
            if not records or len(records)<120:
                raise ValueError(f"歷史資料不足（{len(records) if records else 0}筆）")
            closes=[r["close"] for r in records]; highs=[r["high"] for r in records]
            lows=[r["low"] for r in records]; vols=[r["vol"] for r in records]
            dates=[r["date"] for r in records]

            cb("抓取三大法人買賣超資料（FinMind）...",14)
            inst_map = {}
            try:
                inst_map = fetch_institutional_finmind(code, start_str, end_str)
                print(f"  [法人] {code}: {len(inst_map)} 筆")
            except Exception as e:
                print(f"  [法人] 失敗（繼續）: {e}")

            cb("抓取 PER / 殖利率資料（FinMind）...",17)
            per_map = {}
            try:
                per_map = fetch_per_finmind(code, start_str)
                print(f"  [PER] {code}: {len(per_map)} 筆")
            except Exception as e:
                print(f"  [PER] 失敗（繼續）: {e}")

            cb(f"計算 {len(FEATURE_NAMES)} 個技術+法人+基本面特徵...", 22)
            all_feats = _build_all_features(closes, highs, lows, vols,
                                            inst_map, per_map, dates)

            # 確定使用的特徵索引
            valid_idx = [i for i in selected_feat_idx
                         if isinstance(i, int) and 0 <= i < len(FEATURE_NAMES)]
            if not valid_idx:
                valid_idx = list(range(len(FEATURE_NAMES)))  # 全選
            used_names = [FEATURE_NAMES[i] for i in valid_idx]
            cb(f"建立訓練樣本（使用 {len(valid_idx)} 個特徵）...", 30)
            X,y=[],[]
            for i in range(60,len(closes)-pred_days):
                f=all_feats[i]
                if f is None: continue
                ret=(closes[i+pred_days]-closes[i])/closes[i]*100
                y.append("漲" if ret>threshold else "跌" if ret<-threshold else "持平")
                X.append([f[j] for j in valid_idx])
            if len(X)<50: raise ValueError(f"訓練樣本不足（{len(X)}筆）")
            X_bal,y_bal=oversample_minority(X,y,0.85)
            cb(f"樣本平衡：{len(X_bal)}筆（原{len(X)}）",35)
            n_feat=len(valid_idx); max_f=max(1,int(n_feat**0.5))
            cb("交叉驗證...",42)
            n=len(X); fs=n//3; accs,pu_l,pd_l,pf_l=[],[],[],[]
            for fold in range(3):
                vs,ve=fold*fs,(fold+1)*fs
                Xtr=X[:vs]+X[ve:]; ytr=y[:vs]+y[ve:]
                Xvl=X[vs:ve]; yvl=y[vs:ve]
                if not Xtr or not Xvl: continue
                Xtb,ytb=oversample_minority(Xtr,ytr)
                vt=[]
                for _ in range(5):
                    si=[_r.randint(0,len(Xtb)-1) for _ in range(len(Xtb))]
                    vt.append(simple_decision_tree([Xtb[j] for j in si],[ytb[j] for j in si],max_depth=6,max_features=max_f))
                tp_u=fp_u=tp_d=fp_d=tp_f=fp_f=correct=0
                for xi,yi in zip(Xvl,yvl):
                    p,_,_=random_forest_predict(vt,xi)
                    if p==yi: correct+=1
                    if p=="漲" and yi!="漲": fp_u+=1
                    if p=="跌" and yi!="跌": fp_d+=1
                    if p=="持平" and yi!="持平": fp_f+=1
                    if yi=="漲" and p=="漲": tp_u+=1
                    if yi=="跌" and p=="跌": tp_d+=1
                    if yi=="持平" and p=="持平": tp_f+=1
                accs.append(correct/len(yvl)*100)
                pu_l.append(tp_u/(tp_u+fp_u)*100 if tp_u+fp_u>0 else 0)
                pd_l.append(tp_d/(tp_d+fp_d)*100 if tp_d+fp_d>0 else 0)
                pf_l.append(tp_f/(tp_f+fp_f)*100 if tp_f+fp_f>0 else 0)
            accuracy=round(sum(accs)/len(accs),1)
            prec_up=round(sum(pu_l)/len(pu_l),1) if pu_l else 0
            prec_dn=round(sum(pd_l)/len(pd_l),1) if pd_l else 0
            prec_flat=round(sum(pf_l)/len(pf_l),1) if pf_l else 0
            cb(f"訓練隨機森林（{n_trees}棵）...",58)
            final_trees=[]
            for ti in range(n_trees):
                if ti%3==0: cb(f"訓練第{ti+1}/{n_trees}棵...",58+ti/n_trees*22)
                si=[_r.randint(0,len(X_bal)-1) for _ in range(len(X_bal))]
                final_trees.append(simple_decision_tree([X_bal[j] for j in si],[y_bal[j] for j in si],max_depth=7,max_features=max_f))
            cb("計算特徵重要度...",82)
            imp_vals=calc_forest_importance(final_trees,n_feat)
            feat_imp=sorted([{"name":FEATURE_NAMES[i],"importance":imp_vals[i]} for i in range(n_feat)],key=lambda x:x["importance"],reverse=True)
            cb("預測未來走勢...",90)
            last_feat=all_feats[-1]; predictions=[]
            if last_feat:
                last_feat_sel = [last_feat[j] for j in valid_idx]
                pred_label,conf,probs=random_forest_predict(final_trees,last_feat_sel)
                same_rets=[]
                for i in range(60,len(closes)-pred_days):
                    f2=all_feats[i]
                    if f2 is None: continue
                    f2_sel=[f2[j] for j in valid_idx]
                    pl,_,_=random_forest_predict(final_trees,f2_sel)
                    if pl==pred_label: same_rets.append((closes[i+pred_days]-closes[i])/closes[i]*100)
                same_rets.sort()
                q25=same_rets[len(same_rets)//4] if same_rets else -3
                q75=same_rets[len(same_rets)*3//4] if same_rets else 3
                top1=feat_imp[0]["name"]; top2=feat_imp[1]["name"]
                future_dates=[]; cur=datetime.today()
                while len(future_dates)<pred_days:
                    cur+=timedelta(days=1)
                    if cur.weekday()<5: future_dates.append(cur.strftime("%Y-%m-%d"))
                for i,fdate in enumerate(future_dates):
                    decay=max(0.4,1-i*0.04)
                    predictions.append({"date":fdate,"direction":pred_label,
                        "confidence":max(round(conf*decay,1),40),
                        "range_low":round(q25*(1+i*0.04),2),
                        "range_high":round(q75*(1+i*0.04),2),
                        "reason":f"{top1}＋{top2}" if i==0 else f"延伸預測（{pred_label}趨勢）",
                        "probs":probs})
            history_prices=[{"date":dates[i],"close":closes[i]} for i in range(max(0,len(dates)-60),len(dates))]
            cb("完成！",100)
            prog["result"]={"code":code,"name":code,"accuracy":accuracy,"threshold":threshold,
                "n_trees":n_trees,"predictions":predictions,
                "feature_importance":feat_imp[:10],"history_prices":history_prices,
                "used_features": used_names,
                "model_stats":{"train_samples":len(X),"balanced_samples":len(X_bal),
                    "n_features": len(valid_idx),
                    "accuracy":accuracy,"prec_up":prec_up,"prec_dn":prec_dn,"prec_flat":prec_flat}}
            prog["done"]=True
            print(f"  [預測完成] {code} 準確率:{accuracy}% ({n_trees}棵樹)")
        except Exception as e:
            import traceback; traceback.print_exc()
            prog["error"]=str(e); prog["done"]=True
    threading.Thread(target=bg,daemon=True).start()
    return jsonify({"task_id":task_id})

@app.route("/api/predict/progress/<task_id>")
def predict_progress(task_id):
    prog=_predict_tasks.get(task_id)
    if not prog: return jsonify({"error":"找不到任務"}),404
    return jsonify(prog)

@app.route("/api/predict/features")
def get_feature_list():
    """回傳所有可用特徵名稱和分組"""
    groups = [
        {"group": "動量指標", "color": "#a855f7",
         "features": [{"idx":0,"name":"K值"},{"idx":1,"name":"D值"},{"idx":2,"name":"KD差"},
                      {"idx":3,"name":"RSI(14)"},{"idx":4,"name":"RSI超買賣"}]},
        {"group": "趨勢指標", "color": "#3d8bff",
         "features": [{"idx":5,"name":"MACD"},{"idx":6,"name":"MACD方向"},
                      {"idx":7,"name":"布林位置"},{"idx":8,"name":"布林寬度"}]},
        {"group": "均線偏離", "color": "#27d981",
         "features": [{"idx":9,"name":"距MA5(%)"},{"idx":10,"name":"距MA10(%)"},
                      {"idx":11,"name":"距MA20(%)"},{"idx":12,"name":"距MA60(%)"},
                      {"idx":13,"name":"MA5vsMA20"},{"idx":14,"name":"MA20vsMA60"},
                      {"idx":15,"name":"MA20vsMA120"}]},
        {"group": "量能指標", "color": "#f0b429",
         "features": [{"idx":16,"name":"量比5日"},{"idx":17,"name":"量比20日"},
                      {"idx":18,"name":"量能趨勢"}]},
        {"group": "價格動能", "color": "#ff4d5e",
         "features": [{"idx":19,"name":"近3日漲跌"},{"idx":20,"name":"近5日漲跌"},
                      {"idx":21,"name":"近10日漲跌"},{"idx":22,"name":"近20日漲跌"}]},
        {"group": "波動指標", "color": "#6a748f",
         "features": [{"idx":23,"name":"ATR%"},{"idx":24,"name":"高低振幅%"}]},
        {"group": "K線型態", "color": "#e4eaf5",
         "features": [{"idx":25,"name":"實體大小"},{"idx":26,"name":"上影線"},
                      {"idx":27,"name":"下影線"}]},
        {"group": "連續性", "color": "#6a748f",
         "features": [{"idx":28,"name":"連漲天數"},{"idx":29,"name":"連跌天數"}]},
        {"group": "三大法人（FinMind）", "color": "#f0b429",
         "features": [{"idx":30,"name":"外資買賣超(千張)"},{"idx":31,"name":"投信買賣超(千張)"},
                      {"idx":32,"name":"自營買賣超(千張)"},{"idx":33,"name":"法人合計(千張)"},
                      {"idx":34,"name":"外資強度"}]},
        {"group": "評等模型（FinMind）", "color": "#a855f7",
         "features": [{"idx":35,"name":"綜合評等分"},{"idx":36,"name":"法人評分"},
                      {"idx":37,"name":"基本面評分"},{"idx":38,"name":"目標漲跌幅%"}]},
    ]
    return jsonify({"groups": groups, "total": len(FEATURE_NAMES)})

# ══════════════════════════════════════════════════════
# 持股管理 + 每日自動預測
# ══════════════════════════════════════════════════════

import json, os

PORTFOLIO_FILE  = os.path.join(os.path.dirname(os.path.abspath(__file__)), "portfolio.json")
DAILY_PRED_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "daily_predictions.json")
TG_SETTINGS_FILE= os.path.join(os.path.dirname(os.path.abspath(__file__)), "tg_settings.json")

def load_portfolio():
    if not os.path.exists(PORTFOLIO_FILE): return []
    try:
        with open(PORTFOLIO_FILE, encoding="utf-8") as f: return json.load(f)
    except: return []

def save_portfolio(data):
    with open(PORTFOLIO_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def load_daily_preds():
    if not os.path.exists(DAILY_PRED_FILE): return {}
    try:
        with open(DAILY_PRED_FILE, encoding="utf-8") as f: return json.load(f)
    except: return {}

def save_daily_preds(data):
    with open(DAILY_PRED_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def load_tg_settings():
    if not os.path.exists(TG_SETTINGS_FILE): return {"token":"","chat_id":""}
    try:
        with open(TG_SETTINGS_FILE, encoding="utf-8") as f: return json.load(f)
    except: return {"token":"","chat_id":""}

def save_tg_settings(data):
    with open(TG_SETTINGS_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def send_tg(token, chat_id, msg):
    if not token or not chat_id: return False
    try:
        resp = SESSION.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": msg, "parse_mode": "HTML"},
            timeout=10)
        return resp.json().get("ok", False)
    except: return False

# ── 持股 API ──────────────────────────────────────────

@app.route("/api/portfolio", methods=["GET"])
def get_portfolio():
    stocks = load_portfolio()
    preds  = load_daily_preds()
    today  = datetime.now().strftime("%Y-%m-%d")
    try:
        url  = "https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL"
        resp = SESSION.get(url, timeout=10)
        price_map = {}
        if resp.ok:
            for r in resp.json():
                price_map[r.get("Code","")] = {
                    "price": safe_float(r.get("ClosingPrice")),
                    "chg":   safe_float(r.get("Change")),
                    "name":  r.get("Name",""),
                }
    except: price_map = {}

    result = []
    for s in stocks:
        code       = s["code"]
        live       = price_map.get(code, {})
        cur_price  = live.get("price", s.get("cost", 0))
        cost       = s.get("cost", 0)
        margin     = s.get("margin", 1.0)
        buy_date   = s.get("buy_date", s.get("added",""))
        fee_discount = s.get("fee_discount", 0.6)  # 手續費折扣，預設6折
        is_etf     = s.get("is_etf", False)         # ETF交易稅0.1%

        # 單位換算：支援「股」或「張」輸入
        # qty_unit: "share"=股數, "lot"=張數(預設)
        qty_unit  = s.get("qty_unit", "lot")
        qty_input = s.get("qty", 1)
        if qty_unit == "share":
            shares = qty_input              # 直接是股數
            lots   = shares / 1000          # 換算張數
        else:
            lots   = qty_input              # 輸入張數
            shares = lots * 1000            # 換算股數

        # 台股費率
        base_fee_rate = 0.001425
        fee_rate = base_fee_rate * fee_discount   # 折扣後手續費率
        tax_rate = 0.001 if is_etf else 0.003     # ETF 0.1%，一般股 0.3%

        buy_fee  = round(cost      * shares * fee_rate)
        sell_fee = round(cur_price * shares * fee_rate)
        sell_tax = round(cur_price * shares * tax_rate)

        # 損平價（含買入手續費）= 成本 + 買入手續費/股數
        breakeven = round(cost + buy_fee / shares, 3) if shares > 0 else cost

        # 融資利息（年率 6.35%，按持有天數）
        margin_interest = 0
        if margin > 1 and buy_date:
            try:
                bd   = datetime.strptime(buy_date, "%Y-%m-%d")
                days = (datetime.today() - bd).days
                loan = cost * shares * (1 - 1/margin)   # 融資借款金額
                margin_interest = round(loan * 0.0635 / 365 * days)
            except: pass

        # 損益計算（對齊券商邏輯）
        # 毛損益 = (現價 - 損平價) × 股數
        gross_pnl = round((cur_price - breakeven) * shares)
        # 淨損益 = 毛損益 - 賣出手續費 - 交易稅 - 融資利息
        net_pnl   = gross_pnl - sell_fee - sell_tax - margin_interest

        # 報酬率 = 淨損益 / 自備款(含買入手續費)
        self_cost = (cost * shares + buy_fee) / margin if margin > 0 else cost * shares + buy_fee
        net_pnl_pct   = round(net_pnl / self_cost * 100, 2)   if self_cost > 0 else 0
        gross_pnl_pct = round((cur_price - cost) / cost * 100, 2) if cost > 0  else 0

        pred_today = preds.get(today, {}).get(code)

        result.append({
            **s,
            "lots":            round(lots, 3),
            "shares":          int(shares),
            "cur_price":       cur_price,
            "chg":             live.get("chg", 0),
            "chg_pct":         round(live.get("chg",0)/cur_price*100,2) if cur_price>0 else 0,
            "breakeven":       breakeven,
            "pnl_pct":         gross_pnl_pct,
            "net_pnl_pct":     net_pnl_pct,
            "pnl_amt":         gross_pnl,
            "net_pnl_amt":     net_pnl,
            "buy_fee":         buy_fee,
            "sell_fee":        sell_fee,
            "sell_tax":        sell_tax,
            "margin_interest": margin_interest,
            "total_fee":       buy_fee + sell_fee + sell_tax + margin_interest,
            "self_cost":       round(self_cost),
            "pred_today":      pred_today,
            "last_update":     preds.get("_updated","—"),
        })
    return jsonify({"stocks": result, "today": today})

@app.route("/api/portfolio", methods=["POST"])
def add_portfolio():
    body  = request.get_json()
    code  = body.get("code","").strip()
    if not code: return jsonify({"error":"請填入股票代號"}), 400
    # 自動查詢股票名稱（若未傳入）
    name = body.get("name", "").strip()
    if not name or name == code:
        try:
            url  = "https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL"
            resp = SESSION.get(url, timeout=8)
            if resp.ok:
                for r in resp.json():
                    if r.get("Code","") == code:
                        name = r.get("Name", code)
                        break
        except: pass
    if not name: name = code

    stocks = load_portfolio()
    # 如果已存在，保留 added 欄位（編輯時不覆蓋原始加入日）
    existing = next((s for s in stocks if s["code"] == code), None)
    added    = existing.get("added", datetime.now().strftime("%Y-%m-%d")) if existing else datetime.now().strftime("%Y-%m-%d")
    stocks   = [s for s in stocks if s["code"] != code]
    stocks.append({
        "code":         code,
        "name":         name,
        "cost":         float(body.get("cost", 0)),
        "qty":          float(body.get("qty", 1)),
        "qty_unit":     body.get("qty_unit", "lot"),
        "group":        int(body.get("group", 0)),
        "margin":       float(body.get("margin", 1.0)),
        "buy_date":     body.get("buy_date", datetime.now().strftime("%Y-%m-%d")),
        "fee_discount": float(body.get("fee_discount", 0.6)),
        "is_etf":       bool(body.get("is_etf", False)),
        "added":        added,
    })
    save_portfolio(stocks)
    return jsonify({"ok": True, "name": name, "count": len(stocks)})

@app.route("/api/portfolio/<code>", methods=["PATCH"])
def edit_portfolio(code):
    """直接編輯持股的特定欄位，不影響其他欄位"""
    body   = request.get_json()
    stocks = load_portfolio()
    stock  = next((s for s in stocks if s["code"] == code), None)
    if not stock: return jsonify({"error": f"找不到 {code}"}), 404

    # 只更新有傳入的欄位
    editable = ["cost","qty","qty_unit","buy_date","margin","fee_discount","is_etf","group","name"]
    for field in editable:
        if field in body:
            val = body[field]
            if field in ("cost","margin","fee_discount","qty"):
                val = float(val)
            elif field == "group":
                val = int(val)
            elif field == "is_etf":
                val = bool(val)
            stock[field] = val

    save_portfolio(stocks)
    return jsonify({"ok": True, "stock": stock})

@app.route("/api/stock_name/<code>")
def get_stock_name(code):
    """查詢股票名稱"""
    try:
        url  = "https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL"
        resp = SESSION.get(url, timeout=8)
        if resp.ok:
            for r in resp.json():
                if r.get("Code","") == code:
                    return jsonify({"code": code, "name": r.get("Name", code)})
    except: pass
    return jsonify({"code": code, "name": code})

@app.route("/api/portfolio/<code>", methods=["DELETE"])
def del_portfolio(code):
    stocks = load_portfolio()
    stocks = [s for s in stocks if s["code"] != code]
    save_portfolio(stocks)
    return jsonify({"ok": True})

# ── 賣出持股 → 自動存歷史紀錄 ──────────────────────────

HISTORY_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "trade_history.json")

def load_history():
    if not os.path.exists(HISTORY_FILE): return []
    try:
        with open(HISTORY_FILE, encoding="utf-8") as f: return json.load(f)
    except: return []

def save_history(data):
    with open(HISTORY_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

@app.route("/api/portfolio/sell", methods=["POST"])
def sell_stock():
    """賣出持股，自動計算損益（含稅費）並存入歷史紀錄"""
    body       = request.get_json()
    code       = body.get("code","").strip()
    sell_price = float(body.get("sell_price", 0))
    sell_date  = body.get("sell_date", datetime.now().strftime("%Y-%m-%d"))
    if not code or sell_price <= 0:
        return jsonify({"error":"請填入股票代號與賣出價格"}), 400

    stocks = load_portfolio()
    stock  = next((s for s in stocks if s["code"] == code), None)
    if not stock:
        return jsonify({"error": f"持股中找不到 {code}"}), 404

    cost     = stock.get("cost", 0)
    qty      = stock.get("qty", 1)
    margin   = stock.get("margin", 1.0)
    buy_date = stock.get("buy_date", stock.get("added",""))
    name     = stock.get("name", code)

    fee_rate = 0.001425
    tax_rate = 0.003
    buy_fee  = round(cost * qty * 1000 * fee_rate)
    sell_fee = round(sell_price * qty * 1000 * fee_rate)
    sell_tax = round(sell_price * qty * 1000 * tax_rate)

    # 融資利息
    margin_interest = 0
    if margin > 1 and buy_date:
        try:
            bd   = datetime.strptime(buy_date, "%Y-%m-%d")
            sd   = datetime.strptime(sell_date, "%Y-%m-%d")
            days = max((sd - bd).days, 0)
            margin_interest = round(cost * qty * 1000 * (margin-1) * 0.0635 / 365 * days)
        except: pass

    gross_pnl    = (sell_price - cost) * qty * 1000
    total_fee    = buy_fee + sell_fee + sell_tax + margin_interest
    net_pnl      = gross_pnl - total_fee
    hold_days    = 0
    if buy_date:
        try:
            bd = datetime.strptime(buy_date, "%Y-%m-%d")
            sd = datetime.strptime(sell_date, "%Y-%m-%d")
            hold_days = max((sd-bd).days, 0)
        except: pass

    self_cost    = cost * qty * 1000 / margin if margin > 0 else cost * qty * 1000
    net_pnl_pct  = round(net_pnl / self_cost * 100, 2) if self_cost > 0 else 0
    gross_pnl_pct= round((sell_price-cost)/cost*100, 2) if cost > 0 else 0

    # 存入歷史紀錄
    record = {
        "code":        code,
        "name":        name,
        "group":       stock.get("group", 0),
        "buy_date":    buy_date,
        "sell_date":   sell_date,
        "buy_price":   cost,
        "sell_price":  sell_price,
        "qty":         qty,
        "margin":      margin,
        "buy_fee":     buy_fee,
        "sell_fee":    sell_fee,
        "sell_tax":    sell_tax,
        "margin_interest": margin_interest,
        "total_fee":   total_fee,
        "gross_pnl":   int(gross_pnl),
        "net_pnl":     int(net_pnl),
        "gross_pnl_pct": gross_pnl_pct,
        "net_pnl_pct": net_pnl_pct,
        "hold_days":   hold_days,
        "result":      "獲利" if net_pnl > 0 else "虧損",
    }
    history = load_history()
    history.insert(0, record)
    save_history(history)

    # 從持股清單移除
    stocks = [s for s in stocks if s["code"] != code]
    save_portfolio(stocks)

    return jsonify({"ok": True, "record": record})

@app.route("/api/portfolio/history", methods=["GET"])
def get_history():
    return jsonify(load_history())

@app.route("/api/portfolio/analysis", methods=["GET"])
def get_analysis():
    """分析歷史交易，找出需要加強的地方"""
    history = load_history()
    if not history:
        return jsonify({"error": "尚無歷史紀錄"}), 404

    wins   = [h for h in history if h["net_pnl"] > 0]
    losses = [h for h in history if h["net_pnl"] <= 0]
    total  = len(history)

    win_rate     = round(len(wins)/total*100, 1)
    avg_win_pct  = round(sum(h["net_pnl_pct"] for h in wins)/len(wins), 2)    if wins   else 0
    avg_loss_pct = round(sum(h["net_pnl_pct"] for h in losses)/len(losses), 2) if losses else 0
    profit_factor= round(abs(avg_win_pct / avg_loss_pct), 2) if avg_loss_pct != 0 else 0
    avg_hold     = round(sum(h["hold_days"] for h in history)/total, 1)
    total_net    = sum(h["net_pnl"] for h in history)
    total_fee    = sum(h["total_fee"] for h in history)

    # 各群組分析
    group_stats = {}
    for h in history:
        g = h.get("group", 0)
        if g not in group_stats:
            group_stats[g] = {"wins":0,"losses":0,"pnl":0}
        if h["net_pnl"] > 0: group_stats[g]["wins"] += 1
        else: group_stats[g]["losses"] += 1
        group_stats[g]["pnl"] += h["net_pnl"]

    # 最佳/最差交易
    best  = max(history, key=lambda x: x["net_pnl_pct"])
    worst = min(history, key=lambda x: x["net_pnl_pct"])

    # 快速出場（3日內）vs 長期持有
    quick = [h for h in history if h["hold_days"] <= 3]
    long_ = [h for h in history if h["hold_days"] > 20]
    quick_wr = round(len([h for h in quick if h["net_pnl"]>0])/len(quick)*100,1) if quick else 0
    long_wr  = round(len([h for h in long_ if h["net_pnl"]>0])/len(long_)*100,1) if long_ else 0

    # 產生建議
    suggestions = []
    if win_rate < 50:
        suggestions.append({"type":"warning","title":"勝率偏低",
            "desc":f"目前勝率 {win_rate}%，低於 50%。建議收緊進場條件，只在多個指標同時確認時才買入。"})
    if profit_factor < 1.5:
        suggestions.append({"type":"warning","title":"賺少賠多",
            "desc":f"平均獲利 {avg_win_pct}% vs 平均虧損 {avg_loss_pct}%，獲利因子 {profit_factor}。建議拉寬停利或縮緊停損。"})
    if total_fee > abs(total_net) * 0.2:
        suggestions.append({"type":"info","title":"手續費比重偏高",
            "desc":f"總手續費 {total_fee:,} 元，佔損益比重偏高。建議減少短線頻繁交易，或選擇有折扣的券商。"})
    if quick and quick_wr < 40:
        suggestions.append({"type":"warning","title":"短線成效差",
            "desc":f"持有 3 日內的交易勝率僅 {quick_wr}%。建議避免過度短線操作，給予足夠持有時間。"})
    if long_ and long_wr > 70:
        suggestions.append({"type":"success","title":"長期持有表現佳",
            "desc":f"持有 20 日以上的交易勝率達 {long_wr}%，建議增加長期持有比例。"})
    if avg_loss_pct < -10:
        suggestions.append({"type":"danger","title":"虧損單平均跌幅過大",
            "desc":f"平均虧損達 {avg_loss_pct}%，停損執行不夠確實。建議設定嚴格停損（5~8%），到點就出。"})
    if not suggestions:
        suggestions.append({"type":"success","title":"整體表現不錯",
            "desc":f"勝率 {win_rate}%，獲利因子 {profit_factor}，繼續保持紀律！"})

    return jsonify({
        "total":         total,
        "win_rate":      win_rate,
        "avg_win_pct":   avg_win_pct,
        "avg_loss_pct":  avg_loss_pct,
        "profit_factor": profit_factor,
        "avg_hold_days": avg_hold,
        "total_net_pnl": int(total_net),
        "total_fee":     int(total_fee),
        "best_trade":    best,
        "worst_trade":   worst,
        "quick_win_rate":quick_wr,
        "long_win_rate": long_wr,
        "group_stats":   group_stats,
        "suggestions":   suggestions,
        "history":       history[:50],
    })

@app.route("/api/portfolio/tg", methods=["GET"])
def get_tg_settings():
    return jsonify(load_tg_settings())

@app.route("/api/portfolio/tg", methods=["POST"])
def set_tg_settings():
    body = request.get_json()
    data = {"token": body.get("token",""), "chat_id": body.get("chat_id","")}
    save_tg_settings(data)
    return jsonify({"ok": True})

@app.route("/api/portfolio/tg/test", methods=["POST"])
def test_tg():
    tg = load_tg_settings()
    ok = send_tg(tg["token"], tg["chat_id"],
        "✅ <b>台股持股預測系統</b>\n\nTelegram 連線成功！\n"
        f"每日 08:30 將自動推播持股預測 📈\n⏰ {datetime.now().strftime('%Y/%m/%d %H:%M')}")
    return jsonify({"ok": ok})

# ── 每日自動預測（核心）─────────────────────────────────

def run_daily_predictions():
    """對所有持股跑隨機森林預測，儲存結果並推播 Telegram"""
    stocks = load_portfolio()
    if not stocks:
        print("[每日預測] 持股清單為空，跳過")
        return

    tg      = load_tg_settings()
    today   = datetime.now().strftime("%Y-%m-%d")
    preds   = load_daily_preds()
    results = {}

    print(f"\n[每日預測] {today} 開始，共 {len(stocks)} 支股票")

    # 取得即時股價
    try:
        url  = "https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL"
        resp = SESSION.get(url, timeout=15)
        price_map = {r["Code"]: {"price":safe_float(r.get("ClosingPrice")),
                                  "chg":  safe_float(r.get("Change")),
                                  "name": r.get("Name","")}
                     for r in resp.json()} if resp.ok else {}
    except: price_map = {}

    tg_lines = [f"📊 <b>持股每日預測報告</b>\n⏰ {today}\n{'─'*22}"]

    for s in stocks:
        code = s["code"]
        cost = s.get("cost", 0)
        qty  = s.get("qty",  1)
        name = s.get("name", code)
        live = price_map.get(code, {})
        cur  = live.get("price", cost)
        chg  = live.get("chg", 0)
        chg_pct = round(chg/cur*100, 2) if cur>0 else 0
        pnl_pct = round((cur-cost)/cost*100, 2) if cost>0 else 0

        print(f"  [{code}] {name} 現價:{cur} 持倉損益:{pnl_pct:+.1f}%")

        try:
            end_dt   = datetime.today()
            start_dt = end_dt - timedelta(days=2*365+90)
            start_str= start_dt.strftime("%Y-%m-%d")
            end_str  = end_dt.strftime("%Y-%m-%d")
            records  = fetch_history_range(code, start_str, end_str)

            if not records or len(records) < 120:
                results[code] = {"error": "資料不足"}
                continue

            closes=[r["close"] for r in records]; highs=[r["high"] for r in records]
            lows=[r["low"] for r in records];     vols=[r["vol"]   for r in records]

            # 抓 FinMind 法人 + PER 資料
            inst_map = {}
            per_map  = {}
            try:
                inst_map = fetch_institutional_finmind(code, start_str, end_str)
            except: pass
            try:
                per_map = fetch_per_finmind(code, start_str)
            except: pass

            all_feats = _build_all_features(closes, highs, lows, vols,
                                            inst_map, per_map,
                                            [r["date"] for r in records])

            # 建立訓練樣本（預測10日後）
            X, y = [], []
            pred_days = 10
            threshold = 3.0
            for i in range(60, len(closes)-pred_days):
                f = all_feats[i]
                if f is None: continue
                ret = (closes[i+pred_days]-closes[i])/closes[i]*100
                y.append("漲" if ret>threshold else "跌" if ret<-threshold else "持平")
                X.append(f)

            if len(X) < 50: continue

            import random as _r
            X_bal, y_bal = oversample_minority(X, y, 0.85)
            n_feat = len(FEATURE_NAMES); max_f = max(1, int(n_feat**0.5))

            # 訓練 10 棵（快速版）
            trees = []
            for _ in range(10):
                si = [_r.randint(0,len(X_bal)-1) for _ in range(len(X_bal))]
                trees.append(simple_decision_tree(
                    [X_bal[j] for j in si], [y_bal[j] for j in si],
                    max_depth=7, max_features=max_f))

            last_feat = all_feats[-1]
            if last_feat is None: continue

            pred_label, conf, probs = random_forest_predict(trees, last_feat)

            # 計算同類樣本漲跌幅區間
            same_rets = []
            for i in range(60, len(closes)-pred_days):
                f2 = all_feats[i]
                if f2 is None: continue
                pl,_,_ = random_forest_predict(trees, f2)
                if pl==pred_label:
                    same_rets.append((closes[i+pred_days]-closes[i])/closes[i]*100)
            same_rets.sort()
            q25 = same_rets[len(same_rets)//4]   if same_rets else -3
            q75 = same_rets[len(same_rets)*3//4] if same_rets else  3

            # 停利停損建議
            tp_suggest = round(cur*(1+q75/100), 2) if q75>0 else None
            sl_suggest = round(cur*(1+q25/100), 2) if q25<0 else round(cur*0.95, 2)

            result = {
                "code": code, "name": name,
                "direction":  pred_label,
                "confidence": conf,
                "probs":      probs,
                "range_low":  round(q25,2),
                "range_high": round(q75,2),
                "tp_suggest": tp_suggest,
                "sl_suggest": sl_suggest,
                "cur_price":  cur,
                "pnl_pct":    pnl_pct,
                "updated":    today,
            }
            results[code] = result

            # 組 Telegram 訊息
            dir_icon = "🟢" if pred_label=="漲" else "🔴" if pred_label=="跌" else "⚪"
            pnl_icon = "📈" if pnl_pct>=0 else "📉"
            tg_lines.append(
                f"\n<b>{name}（{code}）</b>\n"
                f"現價：{cur}  {pnl_icon} 持倉：{pnl_pct:+.1f}%\n"
                f"預測（10日）：{dir_icon} <b>{pred_label}</b>  信心 {conf}%\n"
                f"漲跌區間：{q25:+.1f}% ~ {q75:+.1f}%\n"
                f"建議停利：{tp_suggest or '—'}  停損：{sl_suggest}"
            )

        except Exception as e:
            print(f"  [{code}] 預測失敗: {e}")
            results[code] = {"error": str(e)}

    # 儲存結果
    preds[today]    = results
    preds["_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M")
    # 只保留最近 30 天
    keys = [k for k in preds if k != "_updated"]
    keys.sort()
    for old_key in keys[:-30]: del preds[old_key]
    save_daily_preds(preds)

    # 推播 Telegram
    if tg.get("token") and tg.get("chat_id") and len(tg_lines) > 1:
        full_msg = "\n".join(tg_lines)
        ok = send_tg(tg["token"], tg["chat_id"], full_msg)
        print(f"[每日預測] Telegram 推播: {'✅' if ok else '❌'}")

    print(f"[每日預測] 完成！{len(results)} 支股票")
    return results

@app.route("/api/portfolio/run_predict", methods=["POST"])
def manual_run_predict():
    """手動觸發每日預測（背景執行）"""
    threading.Thread(target=run_daily_predictions, daemon=True).start()
    return jsonify({"ok": True, "msg": "已啟動預測，請稍後刷新頁面"})

# ══════════════════════════════════════════════════════
# AI 自動分析（隨機森林）- 網頁版
# ══════════════════════════════════════════════════════

_analyze_tasks = {}        # task_id -> {pct, msg, done, result, error}
_latest_analysis_result = {}   # 記憶體快取，避免 Render 重啟後 JSON 消失

def _sma(arr, n):
    if not arr: return 0.0
    sl = arr[-n:] if len(arr) >= n else arr
    return sum(sl) / len(sl)

def _ema(arr, n):
    if not arr: return 0.0
    e = arr[0]; k = 2/(n+1)
    for v in arr[1:]: e = v*k + e*(1-k)
    return e

def _calc_rf_features(records):
    if len(records) < 40: return None
    closes = [r["close"] for r in records]
    highs  = [r["high"]  for r in records]
    lows   = [r["low"]   for r in records]
    opens  = [r["open"]  for r in records]
    vols   = [r["vol"]   for r in records]
    i = len(records)-1; c = closes[i]

    ma5  = _sma(closes,5);  ma10 = _sma(closes,10)
    ma20 = _sma(closes,20); ma60 = _sma(closes,60) if len(closes)>=60 else _sma(closes,len(closes))
    ma120= _sma(closes,120) if len(closes)>=120 else _sma(closes,len(closes))

    n9 = min(9, len(closes))
    rh = max(highs[-n9:]); rl = min(lows[-n9:])
    rsv= 0 if rh==rl else (c-rl)/(rh-rl)*100
    k,d=50.0,50.0
    for _ in range(5): k=k*2/3+rsv*1/3; d=d*2/3+k*1/3

    chgs = [closes[j+1]-closes[j] for j in range(len(closes)-1)]
    r14  = chgs[-14:] if len(chgs)>=14 else chgs
    g14  = sum(x for x in r14 if x>0)/max(len(r14),1)
    l14  = sum(-x for x in r14 if x<0)/max(len(r14),1)
    rsi  = 100-100/(1+g14/l14) if l14>0 else 100

    ema12=_ema(closes[-26:],12); ema26=_ema(closes[-26:],26)
    macd = (ema12-ema26)/c*100 if c>0 else 0

    ma20v= _sma(closes[-20:],20)
    std20= (sum((x-ma20v)**2 for x in closes[-20:])/20)**0.5
    boll = (c-(ma20v-2*std20))/(4*std20+0.001)

    avg5v = _sma(vols[:-1],5)  if len(vols)>5  else vols[-1]
    avg20v= _sma(vols[:-1],20) if len(vols)>20 else vols[-1]
    vr5   = vols[-1]/avg5v  if avg5v>0  else 1
    vr20  = vols[-1]/avg20v if avg20v>0 else 1

    ret3  = (c/closes[-4]-1)*100  if len(closes)>=4  else 0
    ret5  = (c/closes[-6]-1)*100  if len(closes)>=6  else 0
    ret10 = (c/closes[-11]-1)*100 if len(closes)>=11 else 0
    ret20 = (c/closes[-21]-1)*100 if len(closes)>=21 else 0

    trs = [max(highs[j]-lows[j],abs(highs[j]-closes[j-1]),abs(lows[j]-closes[j-1]))
           for j in range(max(1,i-13),i+1)]
    atr = (sum(trs)/len(trs) if trs else 0)/c*100 if c>0 else 0

    red_k = sum(1 for j in range(max(0,i-4),i+1) if closes[j]>=opens[j])/5
    consec= 0
    for j in range(i,max(-1,i-10),-1):
        if j==0: break
        if closes[j]>closes[j-1]: consec+=1
        else: break

    hi60 = max(highs[-60:]) if len(highs)>=60 else max(highs)
    lo60 = min(lows[-60:])  if len(lows)>=60  else min(lows)
    ppos = (c-lo60)/(hi60-lo60+0.001)

    return [
        (c-ma5)/ma5*100   if ma5>0  else 0,
        (c-ma20)/ma20*100 if ma20>0 else 0,
        (c-ma60)/ma60*100 if ma60>0 else 0,
        (c-ma120)/ma120*100 if ma120>0 else 0,
        (ma5-ma20)/ma20*100 if ma20>0 else 0,
        (ma20-ma60)/ma60*100 if ma60>0 else 0,
        k, d, rsi, macd, boll,
        vr5, vr20,
        ret3, ret5, ret10, ret20, atr,
        red_k, consec, ppos,
        k-d, rsi-50,
    ]

def _build_rf_train(records, pred_days=15, rise_thr=3.0):
    X, y = [], []
    closes = [r["close"] for r in records]
    for i in range(40, len(records)-pred_days):
        f = _calc_rf_features(records[:i+1])
        if f is None: continue
        label = 1 if (closes[i+pred_days]-closes[i])/closes[i]*100 >= rise_thr else 0
        X.append(f); y.append(label)
    return X, y

class _DT:
    def __init__(self, max_depth=7, min_s=4, n_feat=None):
        self.max_depth=max_depth; self.min_s=min_s; self.n_feat=n_feat; self.tree=None
    def _gini(self, y):
        n=len(y)
        if n==0: return 0
        cnt=defaultdict(int)
        for l in y: cnt[l]+=1
        return 1-sum((v/n)**2 for v in cnt.values())
    def _split(self, X, y):
        best=-1; bf=bt=None; n=len(y); gp=self._gini(y)
        feats=list(range(len(X[0])))
        if self.n_feat and self.n_feat<len(feats): feats=random.sample(feats,self.n_feat)
        for f in feats:
            vals=sorted(set(x[f] for x in X))
            if len(vals)<=1: continue
            step=max(1,len(vals)//8)
            for vi in range(0,len(vals)-1,step):
                thr=(vals[vi]+vals[vi+1])/2
                ly=[y[j] for j in range(n) if X[j][f]<=thr]
                ry=[y[j] for j in range(n) if X[j][f]>thr]
                if not ly or not ry: continue
                g=gp-(len(ly)/n*self._gini(ly)+len(ry)/n*self._gini(ry))
                if g>best: best=g; bf=f; bt=thr
        return bf,bt
    def _build(self,X,y,d):
        cnt=defaultdict(int)
        for l in y: cnt[l]+=1
        maj=max(cnt,key=cnt.get); prob=cnt.get(1,0)/len(y)
        if d>=self.max_depth or len(y)<=self.min_s or len(cnt)==1:
            return {"leaf":True,"prob":prob}
        f,t=self._split(X,y)
        if f is None: return {"leaf":True,"prob":prob}
        li=[i for i in range(len(y)) if X[i][f]<=t]
        ri=[i for i in range(len(y)) if X[i][f]>t]
        return {"leaf":False,"f":f,"t":t,
                "l":self._build([X[i] for i in li],[y[i] for i in li],d+1),
                "r":self._build([X[i] for i in ri],[y[i] for i in ri],d+1)}
    def fit(self,X,y): self.tree=self._build(X,y,0)
    def _pred(self,x,n):
        if n["leaf"]: return n["prob"]
        return self._pred(x,n["l"] if x[n["f"]]<=n["t"] else n["r"])
    def predict_proba(self,X): return [self._pred(x,self.tree) for x in X]

class _RF:
    def __init__(self,n=50,md=7,ms=4,nf=10):
        self.n=n; self.md=md; self.ms=ms; self.nf=nf; self.trees=[]
    def fit(self,X,y):
        self.trees=[]
        sz=len(X)
        np_=sum(y); nn_=sz-np_
        wp=sz/(2*np_) if np_>0 else 1; wn=sz/(2*nn_) if nn_>0 else 1
        ws=[wp if yi==1 else wn for yi in y]
        tw=sum(ws); cp=[]
        acc=0
        for w in ws: acc+=w/tw; cp.append(acc)
        for _ in range(self.n):
            idx=[]
            for _ in range(sz):
                r=random.random()
                for j,c in enumerate(cp):
                    if r<=c: idx.append(j); break
                else: idx.append(sz-1)
            t=_DT(self.md,self.ms,self.nf)
            t.fit([X[i] for i in idx],[y[i] for i in idx])
            self.trees.append(t)
    def predict_proba(self,X):
        ap=[t.predict_proba(X) for t in self.trees]
        return [sum(ap[t][i] for t in range(len(self.trees)))/len(self.trees) for i in range(len(X))]

def _analyze_one(code, name):
    """對單支股票跑隨機森林，回傳結果"""
    records = fetch_history_range(code,
        (datetime.today()-timedelta(days=730)).strftime("%Y-%m-%d"),
        datetime.today().strftime("%Y-%m-%d"))
    if len(records) < 80: return None

    X, y = _build_rf_train(records, 15, 3.0)
    if len(X)<50 or sum(y)<8 or sum(1-v for v in y)<8: return None

    n=len(X); fold=n//5; accs=[]
    for k in range(5):
        vs=k*fold; ve=min((k+1)*fold,n)
        if vs<30: continue
        rf=_RF(n=30,md=6,nf=8)
        rf.fit(X[:vs],y[:vs])
        probs=rf.predict_proba(X[vs:ve])
        preds=[1 if p>=0.5 else 0 for p in probs]
        acc=sum(preds[i]==y[vs+i] for i in range(len(preds)))/max(len(preds),1)
        accs.append(acc)

    accuracy = sum(accs)/len(accs) if accs else 0.5

    rf_final=_RF(n=50,md=7,nf=10)
    rf_final.fit(X,y)
    cf=_calc_rf_features(records)
    if cf is None: return None

    rise_prob = rf_final.predict_proba([cf])[0]
    confidence= accuracy*abs(rise_prob-0.5)*2

    closes=[r["close"] for r in records]
    return {
        "code":        code,
        "name":        name,
        "rise_prob":   round(rise_prob*100,1),
        "accuracy":    round(accuracy*100,1),
        "confidence":  round(confidence*100,1),
        "price":       records[-1]["close"],
        "chg_pct":     round((records[-1]["close"]/records[-2]["close"]-1)*100,2)
                       if len(records)>=2 else 0,
        "data_count":  len(records),
    }

def _run_analyze_task(task_id, max_stocks, top_n, model_ver='v2'):
    try:
        prog = _analyze_tasks[task_id]
        def cb(msg, pct):
            prog["msg"]=msg; prog["pct"]=round(pct,1)
            print(f"  [AI分析 {pct:.0f}%] {msg}")

        is_v2 = (model_ver == 'v2')
        history_months = 24 if is_v2 else 8

        # ── 取得上市股票（TWSE）────────────────────
        cb("從 TWSE 取得上市股票...", 1)
        stocks = []
        try:
            url  = "https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL"
            resp = SESSION.get(url, timeout=15); resp.raise_for_status()
            for r in resp.json():
                code=r.get("Code",""); price=safe_float(r.get("ClosingPrice"))
                vol=round(safe_float(r.get("TradeVolume"))/1000)
                chg=safe_float(r.get("Change")); prev=price-chg
                pct_=round(chg/prev*100,2) if prev>0 else 0
                if not (str(code).isdigit() and len(code)==4 and price>0): continue
                if price<10 or vol<100: continue
                if abs(pct_)>9.5: continue
                stocks.append({"code":code,"name":r.get("Name",""),
                               "price":price,"pct":pct_,"vol":vol,"market":"上市"})
            print(f"  上市：{len(stocks)} 支")
        except Exception as e:
            print(f"  上市取得失敗: {e}")

        # ── 取得上櫃股票（OTC）─────────────────────
        cb("從 OTC 取得上櫃股票...", 2)
        otc_count = 0
        try:
            otc_url = "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_quotes"
            r2 = SESSION.get(otc_url, timeout=15); r2.raise_for_status()
            for r in r2.json():
                code  = r.get("SecuritiesCompanyCode","") or r.get("code","")
                name  = r.get("CompanyName","") or r.get("name","")
                price = safe_float(r.get("Close","") or r.get("close",""))
                vol   = round(safe_float(r.get("TradingShares","") or r.get("volume","")) / 1000)
                chg   = safe_float(r.get("Change","") or r.get("change",""))
                if not (str(code).isdigit() and len(str(code)) in [4,5] and price>0): continue
                if price<10 or vol<100: continue
                prev  = price-chg
                pct_  = round(chg/prev*100,2) if prev>0 else 0
                if abs(pct_)>9.5: continue
                stocks.append({"code":code,"name":name,
                               "price":price,"pct":pct_,"vol":vol,"market":"上櫃"})
                otc_count += 1
            print(f"  上櫃：{otc_count} 支")
        except Exception as e:
            print(f"  上櫃取得失敗（使用備用方案）: {e}")
            # 備用：從 TWSE 抓上櫃
            try:
                r3 = SESSION.get(
                    "https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL",
                    timeout=15)
                # 上櫃代號通常 5 碼
                pass
            except: pass

        total_before = len(stocks)
        cb(f"取得 {total_before} 支股票（上市+上櫃）", 3)

        # 如果設定了數量上限且小於實際數量，隨機抽樣
        if max_stocks > 0 and len(stocks) > max_stocks:
            random.shuffle(stocks)
            stocks = stocks[:max_stocks]
            cb(f"隨機抽樣 {max_stocks} 支進行分析...", 4)
        else:
            cb(f"全部 {len(stocks)} 支進行分析...", 4)

        # v2.0：預先抓大盤資料
        market_closes = []
        if is_v2:
            cb("抓取大盤指數資料（v2.0）...", 4)
            try:
                today = datetime.today()
                for delta in range(history_months, -1, -1):
                    d = today - timedelta(days=delta*31)
                    ym = f"{d.year}{d.month:02d}01"
                    r2 = SESSION.get(
                        f"https://www.twse.com.tw/exchangeReport/STOCK_DAY"
                        f"?response=json&date={ym}&stockNo=Y9999", timeout=10)
                    data2 = r2.json()
                    if data2.get("stat")=="OK" and data2.get("data"):
                        for row in data2["data"]:
                            c2 = safe_float(row[6])
                            if c2 > 0: market_closes.append(c2)
                print(f"  大盤資料：{len(market_closes)} 筆")
            except Exception as e:
                print(f"  大盤抓取失敗: {e}")

        cb(f"開始{'v2.0 強化' if is_v2 else 'v1.0 基礎'}分析 {len(stocks)} 支股票...", 5)
        results=[]
        total=len(stocks)
        for idx, s in enumerate(stocks):
            pct_done = 5 + idx/total*88
            cb(f"{'[v2]' if is_v2 else '[v1]'} {s['code']} {s['name']} ({idx+1}/{total})", pct_done)
            try:
                if is_v2:
                    r = _analyze_one_v2(s["code"], s["name"], market_closes, history_months)
                else:
                    r = _analyze_one(s["code"], s["name"])
                if r and r["confidence"] > 5:   # 放寬門檻，讓更多股票進入排序
                    r["vol"]=s["vol"]; r["chg_pct"]=s["pct"]
                    results.append(r)
            except Exception as e:
                print(f"  [{s['code']}] 錯誤: {e}")

        cb("排序結果...", 95)
        results.sort(key=lambda x: x["rise_prob"]*0.6+x["confidence"]*0.4, reverse=True)
        top=results[:top_n]

        # 組合結果
        analysis_out = {
            "stocks":         top,
            "total_analyzed": len(results),
            "total_scanned":  len(stocks),
            "model_ver":      model_ver,
            "time":           datetime.now().strftime("%Y/%m/%d %H:%M"),
            "avg_accuracy":   round(sum(r.get("accuracy",0) for r in results)/max(len(results),1),1),
            "bullish":        sum(1 for r in results if r.get("rise_prob",0)>=50),
            "bearish":        sum(1 for r in results if r.get("rise_prob",0)<50),
            "predict_days":   15,
        }
        prog.update({"pct":100,"msg":"完成！","done":True,"error":None,
                     "result": analysis_out})
        # 同時存進記憶體快取（避免 Render 重啟後遺失）
        global _latest_analysis_result
        _latest_analysis_result = analysis_out
        print(f"  ✅ 分析結果已快取（{len(top)} 支推薦）")
    except Exception as e:
        import traceback; traceback.print_exc()
        _analyze_tasks[task_id].update({"done":True,"error":str(e)})

def _analyze_one_v2(code, name, market_closes, history_months=24):
    """v2.0 強化版：28個特徵 + 大盤相對強弱 + 時間序列驗證 + FinMind籌碼"""
    start_dt = (datetime.today()-timedelta(days=history_months*31)).strftime("%Y-%m-%d")
    end_dt   = datetime.today().strftime("%Y-%m-%d")
    records  = fetch_history_range(code, start_dt, end_dt)
    if len(records) < 60: return None

    closes = [r["close"] for r in records]
    highs  = [r["high"]  for r in records]
    lows   = [r["low"]   for r in records]
    opens  = [r["open"]  for r in records]
    vols   = [r["vol"]   for r in records]

    # ── 抓 FinMind 籌碼資料（有 Token 才用）────────
    inst_map = {}
    try:
        token = _get_finmind_token()
        if token:
            inst_map = fetch_institutional_finmind(code, start_dt, end_dt)
    except: pass

    # 把籌碼資料對應到每一天
    record_dates = [r["date"] for r in records]
    foreign_nets = []
    trust_nets   = []
    for d in record_dates:
        inst = inst_map.get(d, {})
        foreign_nets.append(inst.get("foreign_net", 0))
        trust_nets.append(inst.get("trust_net", 0))

    def feat(recs, mkt_closes, idx_offset=0):
        if len(recs) < 30: return None
        cl = [r["close"] for r in recs]
        hi = [r["high"]  for r in recs]
        lo = [r["low"]   for r in recs]
        op = [r["open"]  for r in recs]
        vo = [r["vol"]   for r in recs]
        i = len(recs)-1; c = cl[i]

        def _sma(a,n): sl=a[-n:] if len(a)>=n else a; return sum(sl)/len(sl)
        def _ema(a,n):
            e=a[0]; k=2/(n+1)
            for v in a[1:]: e=v*k+e*(1-k)
            return e

        ma5=_sma(cl,5); ma10=_sma(cl,10); ma20=_sma(cl,20)
        ma60=_sma(cl,60) if len(cl)>=60 else _sma(cl,len(cl))
        ma120=_sma(cl,120) if len(cl)>=120 else _sma(cl,len(cl))

        n9=min(9,len(cl)); rh=max(hi[-n9:]); rl=min(lo[-n9:])
        rsv=0 if rh==rl else (c-rl)/(rh-rl)*100
        k,d=50.0,50.0
        for _ in range(5): k=k*2/3+rsv*1/3; d=d*2/3+k*1/3

        chgs=[cl[j+1]-cl[j] for j in range(len(cl)-1)]
        r14=chgs[-14:] if len(chgs)>=14 else chgs
        g14=sum(x for x in r14 if x>0)/max(len(r14),1)
        l14=sum(-x for x in r14 if x<0)/max(len(r14),1)
        rsi=100-100/(1+g14/l14) if l14>0 else 100

        e12=_ema(cl[-26:],12); e26=_ema(cl[-26:],26)
        macd=(e12-e26)/c*100 if c>0 else 0

        mv20=_sma(cl[-20:],20)
        std20=(sum((x-mv20)**2 for x in cl[-20:])/20)**0.5
        boll=(c-(mv20-2*std20))/(4*std20+0.001)

        av5=_sma(vo[:-1],5) if len(vo)>5 else vo[-1]
        av20=_sma(vo[:-1],20) if len(vo)>20 else vo[-1]

        ret3=(c/cl[-4]-1)*100  if len(cl)>=4  else 0
        ret5=(c/cl[-6]-1)*100  if len(cl)>=6  else 0
        ret10=(c/cl[-11]-1)*100 if len(cl)>=11 else 0
        ret20=(c/cl[-21]-1)*100 if len(cl)>=21 else 0

        trs=[max(hi[j]-lo[j],abs(hi[j]-cl[j-1]),abs(lo[j]-cl[j-1]))
             for j in range(max(1,i-13),i+1)]
        atr=(sum(trs)/len(trs) if trs else 0)/c*100 if c>0 else 0

        red_k=sum(1 for j in range(max(0,i-4),i+1) if cl[j]>=op[j])/5
        consec=0
        for j in range(i,max(-1,i-10),-1):
            if j==0: break
            if cl[j]>cl[j-1]: consec+=1
            else: break
        hi60=max(hi[-60:]) if len(hi)>=60 else max(hi)
        lo60=min(lo[-60:])  if len(lo)>=60  else min(lo)
        ppos=(c-lo60)/(hi60-lo60+0.001)

        # 大盤相對強弱
        rel=0.0
        if mkt_closes and len(mkt_closes)>=11:
            mret10=(mkt_closes[-1]/mkt_closes[-11]-1)*100 if mkt_closes[-11]>0 else 0
            rel=ret10-mret10

        # FinMind 籌碼特徵（近5日外資/投信淨買超，標準化）
        actual_idx = idx_offset + i
        avg_vol = _sma(vo, 20) or 1
        f_net5 = sum(foreign_nets[max(0,actual_idx-4):actual_idx+1]) / avg_vol / 5 \
                 if foreign_nets and actual_idx < len(foreign_nets) else 0
        t_net5 = sum(trust_nets[max(0,actual_idx-4):actual_idx+1]) / avg_vol / 5 \
                 if trust_nets and actual_idx < len(trust_nets) else 0

        return [
            (c-ma5)/ma5*100   if ma5>0  else 0,
            (c-ma20)/ma20*100 if ma20>0 else 0,
            (c-ma60)/ma60*100 if ma60>0 else 0,
            (c-ma120)/ma120*100 if ma120>0 else 0,
            (ma5-ma20)/ma20*100 if ma20>0 else 0,
            (ma20-ma60)/ma60*100 if ma60>0 else 0,
            k, d, rsi, macd, boll,
            vo[-1]/av5  if av5>0  else 1,
            vo[-1]/av20 if av20>0 else 1,
            ret3, ret5, ret10, ret20, atr,
            red_k, consec, ppos,
            k-d, rsi-50, rel,
            f_net5, t_net5,          # FinMind 籌碼（共 25 個特徵）
        ]

    # 建立訓練資料
    X, y = [], []
    for i in range(40, len(records)-15):
        f = feat(records[:i+1], market_closes, idx_offset=i)
        if f is None: continue
        label = 1 if (closes[i+15]-closes[i])/closes[i]*100 >= 3.0 else 0
        X.append(f); y.append(label)

    if len(X)<25 or sum(y)<4 or sum(1-v for v in y)<4: return None

    # 時間序列 5 折交叉驗證
    n=len(X); fold=n//5; accs=[]; precs=[]
    for k in range(5):
        vs=k*fold; ve=min((k+1)*fold,n)
        if vs<15: continue
        rf=_RF(n=30,md=6,ms=4,nf=8)
        rf.fit(X[:vs],y[:vs])
        probs=rf.predict_proba(X[vs:ve])
        preds=[1 if p>=0.5 else 0 for p in probs]
        vy=y[vs:ve]
        acc=sum(preds[i]==vy[i] for i in range(len(preds)))/max(len(preds),1)
        tp=sum(1 for i in range(len(preds)) if preds[i]==1 and vy[i]==1)
        fp=sum(1 for i in range(len(preds)) if preds[i]==1 and vy[i]==0)
        prec=tp/(tp+fp) if (tp+fp)>0 else 0
        accs.append(acc); precs.append(prec)

    if not accs: return None
    accuracy  = sum(accs)/len(accs)
    precision = sum(precs)/len(precs)

    rf_final=_RF(n=50,md=7,ms=4,nf=10)
    rf_final.fit(X,y)
    cf=feat(records, market_closes, idx_offset=len(records)-1)
    if cf is None: return None

    rise_prob  = rf_final.predict_proba([cf])[0]
    confidence = accuracy*abs(rise_prob-0.5)*2

    # 計算大盤相對強弱
    rel_strength = 0.0
    if market_closes and len(market_closes)>=11 and len(closes)>=11:
        mret10=(market_closes[-1]/market_closes[-11]-1)*100 if market_closes[-11]>0 else 0
        sret10=(closes[-1]/closes[-11]-1)*100 if closes[-11]>0 else 0
        rel_strength = round(sret10 - mret10, 2)

    # ── 看漲/看跌原因分析 ──────────────────────────
    cur_price = records[-1]["close"]
    reasons_bull = []
    reasons_bear = []

    # 1. 均線多空
    def _sma_last(arr, n):
        sl = arr[-n:] if len(arr)>=n else arr
        return sum(sl)/len(sl) if sl else 0

    ma5  = _sma_last(closes, 5)
    ma20 = _sma_last(closes, 20)
    ma60 = _sma_last(closes, 60)
    ma120= _sma_last(closes, 120)

    if cur_price > ma5  > ma20: reasons_bull.append("股價站上5日及20日均線（短線多頭排列）")
    elif cur_price < ma5 < ma20: reasons_bear.append("股價跌破5日及20日均線（短線空頭排列）")
    if ma20 > ma60:  reasons_bull.append("月線在季線之上（中期多頭）")
    elif ma20 < ma60: reasons_bear.append("月線跌破季線（中期空頭）")
    if cur_price > ma120: reasons_bull.append("股價站上年線（長線支撐強）")
    elif cur_price < ma120*0.95: reasons_bear.append("股價遠低於年線（長線偏弱）")

    # 2. KD
    if cf and len(cf) >= 8:
        k_val = cf[6]; d_val = cf[7]
        if k_val > d_val and k_val < 80: reasons_bull.append(f"KD 多頭（K={k_val:.0f} > D={d_val:.0f}）")
        elif k_val < d_val and k_val > 20: reasons_bear.append(f"KD 空頭（K={k_val:.0f} < D={d_val:.0f}）")
        if k_val < 20: reasons_bull.append(f"KD 超賣區（K={k_val:.0f}），反彈機率高")
        if k_val > 80: reasons_bear.append(f"KD 超買區（K={k_val:.0f}），回檔風險")

    # 3. RSI
    if cf and len(cf) >= 9:
        rsi_val = cf[8]
        if 40 < rsi_val < 70: reasons_bull.append(f"RSI={rsi_val:.0f}，動能健康區間")
        elif rsi_val < 30: reasons_bull.append(f"RSI={rsi_val:.0f} 超賣，可能反彈")
        elif rsi_val > 75: reasons_bear.append(f"RSI={rsi_val:.0f} 超買，注意拉回")

    # 4. 量能
    if cf and len(cf) >= 13:
        vol_ratio5  = cf[11]
        vol_ratio20 = cf[12]
        if vol_ratio5 >= 2.0: reasons_bull.append(f"今日爆量（今量為5日均量 {vol_ratio5:.1f} 倍），主力積極")
        elif vol_ratio5 >= 1.5: reasons_bull.append(f"量能放大（{vol_ratio5:.1f} 倍5日均量）")
        if vol_ratio20 < 0.5: reasons_bear.append("成交量萎縮（不足20日均量一半），人氣不足")

    # 5. 大盤相對強弱
    if rel_strength >= 3:   reasons_bull.append(f"近10日超越大盤 +{rel_strength}%（強勢股）")
    elif rel_strength <= -3: reasons_bear.append(f"近10日落後大盤 {rel_strength}%（弱勢股）")

    # 6. 近期漲跌
    if cf and len(cf) >= 17:
        ret5  = cf[14]
        ret20 = cf[16]
        if ret5 > 5:   reasons_bull.append(f"近5日漲幅 +{ret5:.1f}%，短線動能強")
        elif ret5 < -5: reasons_bear.append(f"近5日跌幅 {ret5:.1f}%，短線偏弱")
        if ret20 > 10:  reasons_bull.append(f"近20日漲幅 +{ret20:.1f}%，中期趨勢向上")
        elif ret20 < -10: reasons_bear.append(f"近20日跌幅 {ret20:.1f}%，中期趨勢向下")

    # ── 預估漲跌幅（用歷史相似情境的平均報酬）──────
    # 找出訓練資料中上漲機率 >= rise_prob 的樣本，計算其後15日平均報酬
    similar_returns = []
    for i in range(len(X)):
        p = rf_final.predict_proba([X[i]])[0]
        if abs(p - rise_prob) <= 0.05:  # 相近機率的歷史樣本
            if i + 15 < len(closes):
                ret = (closes[i+15] - closes[i]) / closes[i] * 100
                similar_returns.append(ret)

    if similar_returns:
        avg_ret    = sum(similar_returns) / len(similar_returns)
        pos_rets   = [r for r in similar_returns if r > 0]
        neg_rets   = [r for r in similar_returns if r < 0]
        avg_up     = sum(pos_rets)/len(pos_rets)   if pos_rets else 0
        avg_down   = sum(neg_rets)/len(neg_rets)   if neg_rets else 0
        target_up  = round(cur_price * (1 + avg_up/100),   2) if avg_up   else 0
        target_dn  = round(cur_price * (1 + avg_down/100), 2) if avg_down else 0
        est_return = round(avg_ret, 1)
    else:
        avg_up=avg_down=target_up=target_dn=est_return=0

    return {
        "code":          code,
        "name":          name,
        "rise_prob":     round(rise_prob*100, 1),
        "accuracy":      round(accuracy*100,  1),
        "precision":     round(precision*100, 1),
        "confidence":    round(confidence*100,1),
        "price":         cur_price,
        "chg_pct":       round((closes[-1]/closes[-2]-1)*100,2) if len(closes)>=2 else 0,
        "data_years":    round(len(records)/250,1),
        "rel_strength":  rel_strength,
        "model_ver":     "v2",
        # 新增：原因 + 預估
        "reasons_bull":  reasons_bull[:4],   # 最多顯示4條
        "reasons_bear":  reasons_bear[:4],
        "est_return":    est_return,          # 預估平均報酬%
        "target_up":     target_up,           # 樂觀目標價
        "target_dn":     target_dn,           # 悲觀目標價
        "avg_up_pct":    round(avg_up,   1),
        "avg_down_pct":  round(avg_down, 1),
    }

@app.route("/api/analyze/start", methods=["POST"])
def start_analyze():
    import uuid
    body       = request.get_json() or {}
    max_stocks = int(body.get("max_stocks", 80))
    top_n      = int(body.get("top_n", 10))
    model_ver  = body.get("model_ver", "v2")
    task_id    = str(uuid.uuid4())[:8]
    _analyze_tasks[task_id] = {"pct":0,"msg":"準備中...","done":False,"result":None,"error":None}
    threading.Thread(target=_run_analyze_task,
                     args=(task_id, max_stocks, top_n, model_ver), daemon=True).start()
    return jsonify({"task_id": task_id})

@app.route("/api/analyze/progress/<task_id>")
def analyze_progress(task_id):
    prog = _analyze_tasks.get(task_id)
    if not prog: return jsonify({"error":"找不到任務"}), 404
    return jsonify(prog)

@app.route("/api/analyze/latest")
def get_latest_analysis():
    """讀取記憶體中的最新分析結果（或備用 JSON 檔）"""
    # 優先讀記憶體快取
    if _latest_analysis_result:
        return jsonify(_latest_analysis_result)
    # 備用：嘗試讀本地 JSON 檔（本機開發用）
    result_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "analysis_result.json")
    try:
        if os.path.exists(result_file):
            with open(result_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            return jsonify(data)
    except Exception:
        pass
    return jsonify({"error": "尚無分析結果，請先執行 auto_analysis.py"}), 404

@app.route("/api/analyze/run", methods=["POST"])
def trigger_auto_analysis():
    """手動觸發分析（背景執行，結果存記憶體）"""
    def run_bg():
        try:
            _run_auto_analysis()
        except Exception as e:
            print(f"[auto_analysis] 執行失敗: {e}")
    threading.Thread(target=run_bg, daemon=True).start()
    return jsonify({"ok": True, "msg": "已啟動 v2.0 分析，約 10~15 分鐘完成，請稍後重新整理"})

@app.route("/api/analyze/custom", methods=["POST"])
def start_custom_analyze():
    """自訂個股分析"""
    import uuid
    body      = request.get_json() or {}
    codes     = body.get("codes", [])
    model_ver = body.get("model_ver", "v2")

    if not codes:
        return jsonify({"error": "請提供股票代號"}), 400
    codes = [c.strip() for c in codes if c.strip()][:10]

    task_id = str(uuid.uuid4())[:8]
    _analyze_tasks[task_id] = {"pct":0,"msg":"準備中...","done":False,"result":None,"error":None}

    def run_custom():
        try:
            prog = _analyze_tasks[task_id]
            def cb(msg, pct):
                prog["msg"]=msg; prog["pct"]=round(pct,1)

            is_v2 = (model_ver == 'v2')
            history_months = 24 if is_v2 else 8

            # 取得大盤資料（v2 需要）
            market_closes = []
            if is_v2:
                cb("抓取大盤資料...", 3)
                try:
                    today = datetime.today()
                    for delta in range(history_months, -1, -1):
                        d  = today - timedelta(days=delta*31)
                        ym = f"{d.year}{d.month:02d}01"
                        r2 = SESSION.get(
                            f"https://www.twse.com.tw/exchangeReport/STOCK_DAY"
                            f"?response=json&date={ym}&stockNo=Y9999", timeout=10)
                        d2 = r2.json()
                        if d2.get("stat")=="OK" and d2.get("data"):
                            for row in d2["data"]:
                                c2 = safe_float(row[6])
                                if c2 > 0: market_closes.append(c2)
                except: pass

            # 取得股票今日資訊
            cb("取得股票報價...", 5)
            stock_info = {}
            try:
                url  = "https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL"
                resp = SESSION.get(url, timeout=15); resp.raise_for_status()
                for r in resp.json():
                    code = r.get("Code","")
                    if code in codes:
                        price = safe_float(r.get("ClosingPrice"))
                        chg   = safe_float(r.get("Change"))
                        vol   = round(safe_float(r.get("TradeVolume"))/1000)
                        prev  = price-chg
                        pct_  = round(chg/prev*100,2) if prev>0 else 0
                        stock_info[code] = {
                            "name":  r.get("Name",""),
                            "price": price, "pct": pct_, "vol": vol
                        }
            except: pass

            results = []
            total   = len(codes)
            for idx, code in enumerate(codes):
                info = stock_info.get(code, {})
                name = info.get("name", code)
                pct_done = 5 + idx/total*88
                cb(f"分析 {code} {name} ({idx+1}/{total})...", pct_done)
                try:
                    if is_v2:
                        r = _analyze_one_v2(code, name, market_closes, history_months)
                    else:
                        r = _analyze_one(code, name)
                    if r:
                        r["vol"]     = info.get("vol", 0)
                        r["chg_pct"] = info.get("pct", 0)
                        if not r.get("name"): r["name"] = name
                        results.append(r)
                    else:
                        # 資料不足仍顯示，標記為無法分析
                        results.append({
                            "code": code, "name": name,
                            "rise_prob": 50, "confidence": 0, "accuracy": 0,
                            "precision": 0, "price": info.get("price",0),
                            "chg_pct": info.get("pct",0), "vol": info.get("vol",0),
                            "data_years": 0, "error": "資料不足，無法分析",
                        })
                except Exception as e:
                    results.append({
                        "code": code, "name": name,
                        "rise_prob": 50, "confidence": 0, "accuracy": 0,
                        "precision": 0, "price": 0, "chg_pct": 0, "vol": 0,
                        "data_years": 0, "error": str(e),
                    })

            # 有效結果排前面，無效放後面
            valid   = [r for r in results if not r.get("error")]
            invalid = [r for r in results if r.get("error")]
            valid.sort(key=lambda x: x["rise_prob"]*0.6+x["confidence"]*0.4, reverse=True)
            final = valid + invalid

            prog.update({"pct":100,"msg":"完成！","done":True,"error":None,
                         "result":{
                             "stocks":        final,
                             "total_scanned": len(codes),
                             "total_analyzed":len(valid),
                             "model_ver":     model_ver,
                             "mode":          "custom",
                         }})
        except Exception as e:
            import traceback; traceback.print_exc()
            _analyze_tasks[task_id].update({"done":True,"error":str(e)})

    threading.Thread(target=run_custom, daemon=True).start()
    return jsonify({"task_id": task_id})

@app.route("/analyze")
def analyze_page():
    return send_from_directory(".", "analyze.html")

@app.route("/portfolio")
def portfolio_page():
    return send_from_directory(".", "portfolio.html")

@app.route("/api/prices")
def get_prices():
    """取得指定股票的即時報價"""
    codes_str = request.args.get("codes","")
    codes = [c.strip() for c in codes_str.split(",") if c.strip()]
    if not codes:
        return jsonify({"error":"請提供股票代號"}), 400
    try:
        url  = "https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL"
        resp = SESSION.get(url, timeout=15); resp.raise_for_status()
        rows = resp.json()
        prices = {}
        for r in rows:
            code  = r.get("Code","")
            if code not in codes: continue
            price = safe_float(r.get("ClosingPrice"))
            chg   = safe_float(r.get("Change"))
            if price <= 0: continue
            prev  = price - chg
            pct   = round(chg/prev*100, 2) if prev > 0 else 0
            prices[code] = {
                "price":  price,
                "change": chg,
                "chgPct": pct,
                "name":   r.get("Name",""),
            }
        return jsonify({"prices": prices, "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ── Keep-Alive（防止 Render 免費版休眠）────────────
# ══════════════════════════════════════════════════════
# 每日自動分析（排程 + _run_auto_analysis）
# ══════════════════════════════════════════════════════

def _run_auto_analysis(max_stocks=80, top_n=10, model_ver='v2'):
    """
    在記憶體中執行全市場 AI 分析，結果存到 _latest_analysis_result。
    由每日排程（14:35）或手動 POST /api/analyze/run 觸發。
    """
    global _latest_analysis_result
    import uuid
    task_id = "auto_" + str(uuid.uuid4())[:6]
    _analyze_tasks[task_id] = {"pct": 0, "msg": "自動分析啟動...", "done": False,
                                "result": None, "error": None}
    print(f"\n[自動分析] 開始 task_id={task_id}  {datetime.now().strftime('%Y/%m/%d %H:%M')}")
    try:
        _run_analyze_task(task_id, max_stocks, top_n, model_ver)
        result = _analyze_tasks[task_id].get("result")
        if result:
            _latest_analysis_result = result
            print(f"[自動分析] ✅ 完成，{len(result.get('stocks', []))} 支推薦存入記憶體")
        else:
            err = _analyze_tasks[task_id].get("error", "未知錯誤")
            print(f"[自動分析] ❌ 失敗: {err}")
    except Exception as e:
        print(f"[自動分析] ❌ 例外: {e}")
    finally:
        # 清理任務記憶體（保留最近 5 個）
        keys = list(_analyze_tasks.keys())
        for k in keys[:-5]:
            _analyze_tasks.pop(k, None)


def _start_daily_schedule():
    """
    每天 14:35 台灣時間（UTC+8）自動執行分析。
    在 Render 上 UTC 時間 = 台灣時間 - 8，所以 14:35 CST = 06:35 UTC。
    """
    import time as _time
    def _scheduler():
        print("[排程] 每日自動分析排程已啟動（台灣時間 14:35）")
        while True:
            try:
                now = datetime.utcnow()
                # 台灣時間 = UTC + 8
                tw_hour = (now.hour + 8) % 24
                tw_min  = now.minute
                # 每天 14:35 ~ 14:36 執行一次
                if tw_hour == 14 and tw_min == 35:
                    print(f"[排程] ⏰ 觸發每日分析 {datetime.now().strftime('%Y/%m/%d %H:%M')}")
                    _run_auto_analysis(max_stocks=80, top_n=10, model_ver='v2')
                    _time.sleep(90)  # 避免同一分鐘重複觸發
                else:
                    _time.sleep(30)  # 每 30 秒檢查一次
            except Exception as e:
                print(f"[排程] 錯誤: {e}")
                _time.sleep(60)
    t = threading.Thread(target=_scheduler, daemon=True)
    t.start()


import os as _os

def _start_keep_alive():
    """每 10 分鐘 ping 自己，防止 Render 免費版休眠"""
    render_url = _os.environ.get("RENDER_EXTERNAL_URL", "")
    if not render_url:
        return  # 本機不需要
    def _ping():
        while True:
            try:
                time.sleep(600)  # 10 分鐘
                SESSION.get(f"{render_url}/api/health", timeout=10)
                print(f"[Keep-Alive] Ping {render_url}")
            except: pass
    t = threading.Thread(target=_ping, daemon=True)
    t.start()
    print(f"[Keep-Alive] 已啟動，每 10 分鐘 ping {render_url}")

if __name__ == "__main__":
    print("="*50)
    print("  台股選股 + 回測 + 持股預測系統")
    print("  http://localhost:5000")
    print("="*50)
    print("\n安裝套件：pip install flask requests")
    print("雲端部署：pip install gunicorn\n")
    app.run(host="0.0.0.0", port=5000, debug=False)
else:
    # Gunicorn 啟動時執行 keep-alive + 每日排程
    _start_keep_alive()
    _start_daily_schedule()

