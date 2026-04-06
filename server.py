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

# ── 靜態頁面 ─────────────────────────────────────
@app.route("/")
def index():
    return send_from_directory(".", "index.html")

@app.route("/backtest")
def backtest_page():
    return send_from_directory(".", "backtest.html")

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

def fetch_history_range(code, start_date, end_date):
    """抓指定期間的歷史資料（含前3個月暖機資料）"""
    records = []
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt   = datetime.strptime(end_date,   "%Y-%m-%d")
    fetch_start = start_dt - timedelta(days=90)
    cur = datetime(fetch_start.year, fetch_start.month, 1)
    while cur <= end_dt:
        ym = f"{cur.year}{cur.month:02d}01"
        try:
            url = (f"https://www.twse.com.tw/exchangeReport/STOCK_DAY"
                   f"?response=json&date={ym}&stockNo={code}")
            r = SESSION.get(url, timeout=10)
            data = r.json()
            if data.get("stat") != "OK" or not data.get("data"):
                cur = (cur + timedelta(days=32)).replace(day=1)
                continue
            for row in data["data"]:
                parts = row[0].split("/")
                if len(parts) != 3: continue
                try:
                    dt = datetime(int(parts[0])+1911, int(parts[1]), int(parts[2]))
                except: continue
                c = safe_float(row[6])
                if c > 0:
                    records.append({
                        "date":   dt.strftime("%Y-%m-%d"),
                        "open":   safe_float(row[3]),
                        "high":   safe_float(row[4]),
                        "low":    safe_float(row[5]),
                        "close":  c,
                        "vol":    round(safe_float(row[1]) / 1000),
                        "change": safe_float(row[7]),
                    })
        except Exception as e:
            print(f"  [歷史] {code} {ym} 失敗: {e}")
        cur = (cur + timedelta(days=32)).replace(day=1)
    records.sort(key=lambda x: x["date"])
    return records

def fetch_history_recent(code):
    """抓最近兩個月（給選股用）"""
    records = []
    today = datetime.today()
    for delta in [1, 0]:
        d  = today - timedelta(days=delta * 32)
        ym = f"{d.year}{d.month:02d}01"
        try:
            url = (f"https://www.twse.com.tw/exchangeReport/STOCK_DAY"
                   f"?response=json&date={ym}&stockNo={code}")
            r    = SESSION.get(url, timeout=8)
            data = r.json()
            if data.get("stat") != "OK" or not data.get("data"): continue
            for row in data["data"]:
                c = safe_float(row[6])
                h = safe_float(row[4])
                l = safe_float(row[5])
                v = round(safe_float(row[1]) / 1000)
                if c > 0:
                    records.append({"close":c,"high":h,"low":l,"vol":v})
        except Exception as e:
            print(f"  [歷史] {code} {ym} 失敗: {e}")
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
# API 路由 - 健康檢查
# ══════════════════════════════════════════════════════

@app.route("/api/health")
def health():
    return jsonify({"status": "ok", "time": datetime.now().isoformat()})

# ══════════════════════════════════════════════════════
# API 路由 - 選股
# ══════════════════════════════════════════════════════

@app.route("/api/stocks")
def get_stocks():
    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] 開始抓取 TWSE 資料...")
    try:
        url  = "https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL"
        resp = SESSION.get(url, timeout=15)
        resp.raise_for_status()
        all_rows = resp.json()
        print(f"  ✅ TWSE 回傳 {len(all_rows)} 筆")
    except Exception as e:
        print(f"  ❌ TWSE 失敗: {e}")
        return jsonify({"error": f"TWSE API 失敗: {str(e)}"}), 500

    valid = [
        r for r in all_rows
        if str(r.get("Code","")).isdigit()
        and len(str(r.get("Code",""))) == 4
        and safe_float(r.get("ClosingPrice")) > 0
    ]
    print(f"  📋 有效 {len(valid)} 支，計算指標中...")

    stocks, limit = [], min(150, len(valid))
    for i, row in enumerate(valid[:limit]):
        code = row.get("Code","")
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
            prev_c = closes[-1] if closes else price
            chg_pct = round((price-prev_c)/prev_c*100,2) if prev_c>0 else 0
            ma5  = round(sma(all_c,5),2);  ma20 = round(sma(all_c,20),2)
            ma60 = round(sma(all_c,60),2)
            avg5v  = round(sma(all_v[:-1],5));  avg20v = round(sma(all_v[:-1],20))
            k_s, d_s = calc_kd_series(all_c, all_h, all_l, 9)
            kv, dv = k_s[-1], d_s[-1]
            pkv = k_s[-2] if len(k_s)>=2 else 50.0
            pdv = d_s[-2] if len(d_s)>=2 else 50.0
            rsi = calc_rsi_series(all_c,14)[-1]
            stocks.append({
                "code": code, "name": row.get("Name",""),
                "sector": "上市", "price": price,
                "chgPct": chg_pct, "chgAmt": change,
                "todayVol": today_vol, "avg5Vol": avg5v, "avg20Vol": avg20v,
                "volVsAvg5":  round(today_vol/avg5v,2)  if avg5v>0  else 0,
                "volVsAvg20": round(today_vol/avg20v,2) if avg20v>0 else 0,
                "priceVsMA20": round((price-ma20)/ma20*100,2) if ma20>0 else 0,
                "priceVsMA60": round((price-ma60)/ma60*100,2) if ma60>0 else 0,
                "ma20VsMA60":  round((ma20-ma60)/ma60*100,2)  if ma60>0 else 0,
                "ma5VsMA20":   round((ma5-ma20)/ma20*100,2)   if ma20>0 else 0,
                "kVal":kv,"dVal":dv,"prevK":pkv,"prevD":pdv,
                "rsi14":rsi,"spark":all_c[-20:],"isLive":True,
            })
            print(f"  [{i+1:3d}/{limit}] {code} {row.get('Name',''):<8} "
                  f"價:{price:.2f}  漲跌:{chg_pct:+.2f}%")
        except Exception as e:
            print(f"  [錯誤] {code}: {e}")

    print(f"\n  ✅ 完成！共 {len(stocks)} 支")
    return jsonify({"stocks":stocks,"count":len(stocks),
                    "time":datetime.now().strftime("%Y-%m-%d %H:%M:%S")})

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

    wins  = [t for t in trades if t["pnl"]>0 and not t.get("partial")]
    loses = [t for t in trades if t["pnl"]<=0 and not t.get("partial")]
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

    full_trades = [t for t in trades if not t.get("partial")]
    return jsonify({
        "code":code,"start_date":start_date,"end_date":end_date,
        "kline":kline,"trades":trades,"daily_equity":daily_equity,
        "stats":{
            "total_trades":len(full_trades),
            "win_trades":len(wins),"lose_trades":len(loses),
            "win_rate":round(len(wins)/len(full_trades)*100,1) if full_trades else 0,
            "total_pnl":round(sum(t["pnl"] for t in full_trades),2),
            "avg_win": round(sum(t["pnl"] for t in wins)/len(wins),2)  if wins  else 0,
            "avg_loss":round(sum(t["pnl"] for t in loses)/len(loses),2) if loses else 0,
            "max_drawdown":round(max_draw,2),
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
# API 路由 - 決策樹預測
# ══════════════════════════════════════════════════════

def build_features(closes, highs, lows, vols, i):
    """計算第 i 天的特徵向量"""
    if i < 60: return None
    c = closes[i]

    def ma(n): return sma(closes[max(0,i-n+1):i+1], n)
    def vol_ma(n): return sma(vols[max(0,i-n+1):i+1], n)

    ma5  = ma(5);  ma10 = ma(10); ma20 = ma(20); ma60 = ma(60)
    v5   = vol_ma(5)

    # KD
    k_s, d_s = calc_kd_series(closes[:i+1], highs[:i+1], lows[:i+1], 9)
    kval = k_s[-1]; dval = d_s[-1]

    # RSI
    rsi = calc_rsi_series(closes[:i+1], 14)[-1]

    # MACD (12-26-9)
    def ema(arr, n):
        e = arr[0]; k = 2/(n+1)
        for v in arr[1:]: e = v*k + e*(1-k)
        return e
    sl = closes[max(0,i-35):i+1]
    macd = ema(sl, 12) - ema(sl, 26) if len(sl)>26 else 0

    # 布林通道（20日）
    sl20 = closes[max(0,i-19):i+1]
    boll_mid = sum(sl20)/len(sl20)
    boll_std = (sum((x-boll_mid)**2 for x in sl20)/len(sl20))**0.5
    boll_pos = (c - boll_mid) / (boll_std*2+1e-9)  # -1~1

    # ATR (14日)
    tr_list = []
    for j in range(max(1,i-13), i+1):
        tr = max(highs[j]-lows[j], abs(highs[j]-closes[j-1]), abs(lows[j]-closes[j-1]))
        tr_list.append(tr)
    atr = sum(tr_list)/len(tr_list) if tr_list else 0
    atr_pct = atr / c * 100 if c > 0 else 0

    # 漲跌幅
    ret5  = (c - closes[i-5])  / closes[i-5]  * 100 if closes[i-5]>0  else 0
    ret10 = (c - closes[i-10]) / closes[i-10] * 100 if closes[i-10]>0 else 0
    ret20 = (c - closes[i-20]) / closes[i-20] * 100 if closes[i-20]>0 else 0

    # 量比
    vol_ratio = vols[i] / v5 if v5 > 0 else 1

    return [
        round(kval, 1), round(dval, 1), round(rsi, 1),
        round(macd, 4), round(boll_pos, 4),
        round((c-ma5)/ma5*100,  2) if ma5>0  else 0,
        round((c-ma10)/ma10*100, 2) if ma10>0 else 0,
        round((c-ma20)/ma20*100, 2) if ma20>0 else 0,
        round((c-ma60)/ma60*100, 2) if ma60>0 else 0,
        round(vol_ratio, 3),
        round(ret5, 2), round(ret10, 2), round(ret20, 2),
        round(atr_pct, 3),
    ]

FEATURE_NAMES = [
    "K值", "D值", "RSI(14)",
    "MACD", "布林位置",
    "距MA5(%)", "距MA10(%)", "距MA20(%)", "距MA60(%)",
    "量比(5日)",
    "近5日漲跌幅", "近10日漲跌幅", "近20日漲跌幅",
    "ATR波動率",
]

def simple_decision_tree(X_train, y_train, max_depth=6):
    """簡易決策樹（不依賴 sklearn），CART 演算法"""

    def gini(labels):
        n = len(labels)
        if n == 0: return 0
        counts = {}
        for l in labels: counts[l] = counts.get(l,0) + 1
        return 1 - sum((v/n)**2 for v in counts.values())

    def best_split(X, y):
        best_g, best_f, best_t = float('inf'), 0, 0
        n = len(y)
        for f in range(len(X[0])):
            vals = sorted(set(x[f] for x in X))
            for i in range(len(vals)-1):
                t = (vals[i]+vals[i+1])/2
                left_y  = [y[j] for j in range(n) if X[j][f] <= t]
                right_y = [y[j] for j in range(n) if X[j][f] >  t]
                if not left_y or not right_y: continue
                g = (len(left_y)*gini(left_y) + len(right_y)*gini(right_y)) / n
                if g < best_g:
                    best_g, best_f, best_t = g, f, t
        return best_f, best_t

    def majority(labels):
        counts = {}
        for l in labels: counts[l] = counts.get(l,0)+1
        return max(counts, key=counts.get), counts

    def build(X, y, depth):
        if depth >= max_depth or len(set(y)) == 1 or len(y) < 5:
            label, counts = majority(y)
            return {"leaf": True, "label": label, "counts": counts, "n": len(y)}
        f, t = best_split(X, y)
        left_X  = [X[j] for j in range(len(y)) if X[j][f] <= t]
        left_y  = [y[j] for j in range(len(y)) if X[j][f] <= t]
        right_X = [X[j] for j in range(len(y)) if X[j][f] >  t]
        right_y = [y[j] for j in range(len(y)) if X[j][f] >  t]
        if not left_y or not right_y:
            label, counts = majority(y)
            return {"leaf": True, "label": label, "counts": counts, "n": len(y)}
        return {
            "leaf": False, "feature": f, "threshold": t,
            "left": build(left_X, left_y, depth+1),
            "right": build(right_X, right_y, depth+1),
            "n": len(y),
        }

    return build(X_train, y_train, 0)

def predict_tree(tree, x):
    if tree["leaf"]:
        counts = tree["counts"]
        total  = sum(counts.values())
        conf   = round(counts.get(tree["label"],0)/total*100, 1)
        return tree["label"], conf, counts
    if x[tree["feature"]] <= tree["threshold"]:
        return predict_tree(tree["left"],  x)
    else:
        return predict_tree(tree["right"], x)

def calc_feature_importance(tree, n_features):
    importance = [0.0] * n_features
    def traverse(node):
        if node["leaf"]: return
        importance[node["feature"]] += node["n"]
        traverse(node["left"])
        traverse(node["right"])
    traverse(tree)
    total = sum(importance) or 1
    return [round(v/total, 4) for v in importance]

@app.route("/api/predict", methods=["POST"])
def predict():
    body        = request.get_json()
    code        = body.get("code","").strip()
    train_years = int(body.get("train_years", 2))
    pred_days   = int(body.get("predict_days", 10))
    threshold   = float(body.get("threshold", 3))

    if not code:
        return jsonify({"error": "請填入股票代號"}), 400

    # 計算日期範圍（多抓 3 個月暖機）
    end_dt   = datetime.today()
    start_dt = end_dt - timedelta(days=train_years*365 + 90)
    start_date = start_dt.strftime("%Y-%m-%d")
    end_date   = end_dt.strftime("%Y-%m-%d")

    print(f"\n[預測] {code} 訓練期: {start_date}~{end_date} 預測: {pred_days}日")

    records = fetch_history_range(code, start_date, end_date)
    if not records or len(records) < 120:
        return jsonify({"error": f"歷史資料不足，查無 {code} 或資料少於 120 日"}), 404

    closes = [r["close"] for r in records]
    highs  = [r["high"]  for r in records]
    lows   = [r["low"]   for r in records]
    vols   = [r["vol"]   for r in records]
    dates  = [r["date"]  for r in records]
    name   = code  # 簡化，可從 TWSE 取得

    # 建立特徵矩陣和標籤
    X, y, sample_dates = [], [], []
    for i in range(60, len(closes) - pred_days):
        feat = build_features(closes, highs, lows, vols, i)
        if feat is None: continue
        future_ret = (closes[i+pred_days] - closes[i]) / closes[i] * 100
        if future_ret > threshold:
            label = "漲"
        elif future_ret < -threshold:
            label = "跌"
        else:
            label = "持平"
        X.append(feat); y.append(label); sample_dates.append(dates[i])

    if len(X) < 50:
        return jsonify({"error": "訓練樣本不足（少於50筆）"}), 400

    # 交叉驗證（5折）
    n = len(X); fold_size = n // 5
    accuracies = []
    prec_up_list, prec_dn_list, prec_flat_list = [], [], []

    for fold in range(5):
        val_start = fold * fold_size
        val_end   = val_start + fold_size
        X_train = X[:val_start] + X[val_end:]
        y_train = y[:val_start] + y[val_end:]
        X_val   = X[val_start:val_end]
        y_val   = y[val_start:val_end]
        if not X_train or not X_val: continue
        tree = simple_decision_tree(X_train, y_train, max_depth=5)
        correct = 0
        tp_up=0; fp_up=0; tp_dn=0; fp_dn=0; tp_fl=0; fp_fl=0
        fn_up=0; fn_dn=0; fn_fl=0
        for xi, yi in zip(X_val, y_val):
            pred, _, _ = predict_tree(tree, xi)
            if pred == yi: correct += 1
            if yi=="漲":
                if pred=="漲": tp_up+=1
                else: fn_up+=1
            elif yi=="跌":
                if pred=="跌": tp_dn+=1
                else: fn_dn+=1
            else:
                if pred=="持平": tp_fl+=1
                else: fn_fl+=1
            if pred=="漲" and yi!="漲": fp_up+=1
            if pred=="跌" and yi!="跌": fp_dn+=1
            if pred=="持平" and yi!="持平": fp_fl+=1
        accuracies.append(correct/len(y_val)*100)
        prec_up_list.append(tp_up/(tp_up+fp_up)*100 if tp_up+fp_up>0 else 0)
        prec_dn_list.append(tp_dn/(tp_dn+fp_dn)*100 if tp_dn+fp_dn>0 else 0)
        prec_flat_list.append(tp_fl/(tp_fl+fp_fl)*100 if tp_fl+fp_fl>0 else 0)

    accuracy    = round(sum(accuracies)/len(accuracies), 1)
    prec_up     = round(sum(prec_up_list)/len(prec_up_list),1)   if prec_up_list   else 0
    prec_dn     = round(sum(prec_dn_list)/len(prec_dn_list),1)   if prec_dn_list   else 0
    prec_flat   = round(sum(prec_flat_list)/len(prec_flat_list),1) if prec_flat_list else 0

    # 用全部資料訓練最終模型
    final_tree = simple_decision_tree(X, y, max_depth=6)
    importance_vals = calc_feature_importance(final_tree, len(FEATURE_NAMES))
    feature_importance = sorted(
        [{"name": FEATURE_NAMES[i], "importance": importance_vals[i]}
         for i in range(len(FEATURE_NAMES))],
        key=lambda x: x["importance"], reverse=True
    )

    # 預測未來 pred_days 個交易日
    # 用最後一天的特徵預測
    last_feat = build_features(closes, highs, lows, vols, len(closes)-1)
    predictions = []

    if last_feat:
        pred_label, conf, counts = predict_tree(final_tree, last_feat)
        total_counts = sum(counts.values())

        # 根據歷史同類樣本估算漲跌幅區間
        same_label_rets = []
        for i in range(60, len(closes)-pred_days):
            fi = build_features(closes, highs, lows, vols, i)
            if fi is None: continue
            pl, _, _ = predict_tree(final_tree, fi)
            if pl == pred_label:
                ret = (closes[i+pred_days]-closes[i])/closes[i]*100
                same_label_rets.append(ret)

        if same_label_rets:
            same_label_rets.sort()
            q25 = same_label_rets[len(same_label_rets)//4]
            q75 = same_label_rets[len(same_label_rets)*3//4]
        else:
            q25, q75 = -3, 3

        # 找最重要的特徵作為依據說明
        top_feat = feature_importance[0]["name"]
        top_feat2= feature_importance[1]["name"]
        reason = f"{top_feat}＋{top_feat2}為主要依據"

        # 生成未來日期（跳過週末）
        future_dates = []
        cur = datetime.today()
        while len(future_dates) < pred_days:
            cur += timedelta(days=1)
            if cur.weekday() < 5:
                future_dates.append(cur.strftime("%Y-%m-%d"))

        # 每個預測日都用同一模型（簡化）
        for i, fdate in enumerate(future_dates):
            decay = 1 - i * 0.03  # 越遠信心越低
            adj_conf = max(round(conf * (0.5 + 0.5*decay), 1), 40)
            predictions.append({
                "date":       fdate,
                "direction":  pred_label,
                "confidence": adj_conf,
                "range_low":  round(q25 * (1 + i*0.05), 2),
                "range_high": round(q75 * (1 + i*0.05), 2),
                "reason":     reason if i==0 else f"延伸預測（信心遞減）",
            })

    # 歷史價格（近60日）
    history_prices = [
        {"date": dates[i], "close": closes[i]}
        for i in range(max(0, len(dates)-60), len(dates))
    ]

    print(f"  準確率: {accuracy}%  訓練樣本: {len(X)}")
    return jsonify({
        "code": code, "name": name,
        "accuracy":    accuracy,
        "threshold":   threshold,
        "predictions": predictions,
        "feature_importance": feature_importance[:8],
        "history_prices": history_prices,
        "model_stats": {
            "train_samples": len(X),
            "accuracy":  accuracy,
            "prec_up":   prec_up,
            "prec_dn":   prec_dn,
            "prec_flat": prec_flat,
        }
    })

if __name__ == "__main__":
    print("="*50)
    print("  台股選股 + 回測系統")
    print("  http://localhost:5000")
    print("="*50)
    print("\n安裝套件：pip install flask requests")
    print("雲端部署：pip install gunicorn\n")
    app.run(host="0.0.0.0", port=5000, debug=False)
