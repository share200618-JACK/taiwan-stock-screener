"""
台股選股系統 - Python 後端伺服器（含回測功能）
====================================
安裝套件（只需執行一次）：
    pip install flask requests

啟動伺服器：
    python server.py
"""

from flask import Flask, jsonify, request, send_from_directory
import requests
import urllib3
from datetime import datetime, timedelta
import os

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

app = Flask(__name__, static_folder=".", static_url_path="")

@app.after_request
def after_request(response):
    response.headers["Access-Control-Allow-Origin"]  = "*"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type"
    response.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS, DELETE"
    return response

# ── 靜態頁面路由 ──────────────────────────────────
@app.route("/")
def index():
    return send_from_directory(".", "index.html")

@app.route("/backtest")
def backtest_page():
    return send_from_directory(".", "backtest.html")

@app.route("/alert")
def alert_page():
    return send_from_directory(".", "alert.html")

SESSION = requests.Session()
SESSION.verify = False
SESSION.headers.update({"User-Agent": "Mozilla/5.0"})

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
    """計算整個時間序列的 KD 值"""
    k_series, d_series = [], []
    k, d = 50.0, 50.0
    for i in range(len(closes)):
        n = min(period, i + 1)
        rh = max(highs[max(0, i-n+1):i+1])
        rl = min(lows[max(0, i-n+1):i+1])
        rsv = 0.0 if rh == rl else (closes[i] - rl) / (rh - rl) * 100
        k = k * 2/3 + rsv * 1/3
        d = d * 2/3 + k  * 1/3
        k_series.append(round(k, 1))
        d_series.append(round(d, 1))
    return k_series, d_series

def calc_rsi_series(closes, period=14):
    """計算整個時間序列的 RSI"""
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

def fetch_all_history(code, start_date, end_date):
    """抓指定股票指定期間的每日歷史資料"""
    records = []
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt   = datetime.strptime(end_date,   "%Y-%m-%d")

    # 多抓 3 個月資料，確保均線計算有足夠資料
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
                # 日期格式：民國年/月/日 例如 115/03/28
                date_str = row[0]
                parts = date_str.split("/")
                if len(parts) == 3:
                    try:
                        y = int(parts[0]) + 1911
                        m = int(parts[1])
                        d = int(parts[2])
                        dt = datetime(y, m, d)
                    except: continue
                else: continue

                close  = safe_float(row[6])
                open_p = safe_float(row[3])
                high   = safe_float(row[4])
                low    = safe_float(row[5])
                vol    = round(safe_float(row[1]) / 1000)
                change = safe_float(row[7])

                if close > 0:
                    records.append({
                        "date":   dt.strftime("%Y-%m-%d"),
                        "open":   open_p,
                        "high":   high,
                        "low":    low,
                        "close":  close,
                        "vol":    vol,
                        "change": change,
                    })
        except Exception as e:
            print(f"  [歷史] {code} {ym} 失敗: {e}")
        cur = (cur + timedelta(days=32)).replace(day=1)

    records.sort(key=lambda x: x["date"])
    return records

# ══════════════════════════════════════════════════════
# 條件評估
# ══════════════════════════════════════════════════════

def eval_conditions(day_data, conditions):
    """根據單日資料評估所有篩選條件"""
    for c in conditions:
        key = c.get("key")
        op  = c.get("op", ">")
        val = c.get("val", 0)

        if key == "kdCross":
            kmin = c.get("kdMin", 0)
            kmax = c.get("kdMax", 100)
            ok = (day_data.get("kVal", 50) > day_data.get("dVal", 50) and
                  day_data.get("prevK", 50) <= day_data.get("prevD", 50) and
                  kmin <= day_data.get("kVal", 50) < kmax)
            if not ok: return False
            continue

        v = day_data.get(key)
        if v is None: return False
        if op == ">":  ok = v > val
        elif op == ">=": ok = v >= val
        elif op == "<":  ok = v < val
        elif op == "<=": ok = v <= val
        elif op == "=":  ok = v == val
        else: ok = False
        if not ok: return False
    return True

def build_day_data(i, closes, highs, lows, opens, vols,
                   k_series, d_series, rsi_series, dates):
    """組合單日的所有指標供條件評估用"""
    price = closes[i]
    prev  = closes[i-1] if i > 0 else price
    chg   = round((price - prev) / prev * 100, 2) if prev > 0 else 0

    avg5v  = round(sma(vols[max(0,i-5):i],  5))
    avg20v = round(sma(vols[max(0,i-20):i], 20))
    ma5    = round(sma(closes[max(0,i-5):i+1],  5),  2)
    ma20   = round(sma(closes[max(0,i-20):i+1], 20), 2)
    ma60   = round(sma(closes[max(0,i-60):i+1], 60), 2)

    today_vol = vols[i]

    return {
        "date":        dates[i],
        "price":       price,
        "chgPct":      chg,
        "todayVol":    today_vol,
        "avg5Vol":     avg5v,
        "avg20Vol":    avg20v,
        "volVsAvg5":   round(today_vol / avg5v,  2) if avg5v  > 0 else 0,
        "volVsAvg20":  round(today_vol / avg20v, 2) if avg20v > 0 else 0,
        "priceVsMA20": round((price-ma20)/ma20*100, 2) if ma20 > 0 else 0,
        "priceVsMA60": round((price-ma60)/ma60*100, 2) if ma60 > 0 else 0,
        "ma20VsMA60":  round((ma20-ma60)/ma60*100,  2) if ma60 > 0 else 0,
        "ma5VsMA20":   round((ma5-ma20)/ma20*100,   2) if ma20 > 0 else 0,
        "kVal":  k_series[i],
        "dVal":  d_series[i],
        "prevK": k_series[i-1] if i > 0 else 50,
        "prevD": d_series[i-1] if i > 0 else 50,
        "rsi14": rsi_series[i],
    }

# ══════════════════════════════════════════════════════
# 回測核心
# ══════════════════════════════════════════════════════

def run_backtest(records, conditions, start_date, end_date,
                 take_profit, stop_loss, hold_days, trailing_stop=0):
    """
    執行回測，回傳 trades（每筆交易）+ daily_equity（每日資產曲線）
    trailing_stop: 從持倉後最高點回落 X% 時賣出（0 = 不使用）
    """
    if not records: return [], []

    dates  = [r["date"]  for r in records]
    closes = [r["close"] for r in records]
    highs  = [r["high"]  for r in records]
    lows   = [r["low"]   for r in records]
    opens  = [r["open"]  for r in records]
    vols   = [r["vol"]   for r in records]

    k_series,   d_series   = calc_kd_series(closes, highs, lows, 9)
    rsi_series             = calc_rsi_series(closes, 14)

    trades       = []
    daily_equity = []
    position     = None   # {buy_date, buy_price, buy_idx, peak_price}
    capital      = 100.0

    for i, rec in enumerate(records):
        if rec["date"] < start_date: continue
        if rec["date"] > end_date:   break

        day = build_day_data(i, closes, highs, lows, opens, vols,
                             k_series, d_series, rsi_series, dates)

        if position:
            buy_price  = position["buy_price"]
            held       = i - position["buy_idx"]
            cur_close  = closes[i]
            cur_high   = highs[i]   # 今日最高（更新追蹤高點用）
            cur_chg    = (cur_close - buy_price) / buy_price * 100

            # 更新持倉期間最高點（用今日最高價）
            position["peak_price"] = max(position["peak_price"], cur_high)
            peak_price = position["peak_price"]

            sell_reason = None
            sell_price  = cur_close

            # ① 追蹤高點回落（優先判斷，避免已達停利卻沒鎖住）
            if trailing_stop > 0 and peak_price > buy_price:
                drop_from_peak = (peak_price - cur_close) / peak_price * 100
                if drop_from_peak >= trailing_stop:
                    sell_price  = round(peak_price * (1 - trailing_stop / 100), 2)
                    sell_reason = (f"追蹤高點回落 {trailing_stop}%"
                                   f"（高點 {round(peak_price,2)}→"
                                   f"跌 {round(drop_from_peak,1)}%）")

            # ② 停利
            if not sell_reason and take_profit > 0 and cur_chg >= take_profit:
                sell_price  = round(buy_price * (1 + take_profit / 100), 2)
                sell_reason = f"停利 +{take_profit}%"

            # ③ 停損
            if not sell_reason and stop_loss > 0 and cur_chg <= -stop_loss:
                sell_price  = round(buy_price * (1 - stop_loss / 100), 2)
                sell_reason = f"停損 -{stop_loss}%"

            # ④ 持有天數到期
            if not sell_reason and hold_days > 0 and held >= hold_days:
                sell_reason = f"持有{hold_days}日到期"

            if sell_reason:
                pnl = (sell_price - buy_price) / buy_price * 100
                capital *= (1 + pnl / 100)
                trades.append({
                    "buy_date":    position["buy_date"],
                    "buy_price":   round(buy_price, 2),
                    "peak_price":  round(peak_price, 2),
                    "sell_date":   rec["date"],
                    "sell_price":  round(sell_price, 2),
                    "sell_reason": sell_reason,
                    "pnl":         round(pnl, 2),
                    "held_days":   held,
                })
                position = None

        # 無持倉時，評估買入條件
        if not position and eval_conditions(day, conditions):
            position = {
                "buy_date":   rec["date"],
                "buy_price":  closes[i],
                "buy_idx":    i,
                "peak_price": closes[i],   # 初始高點 = 買入價
            }

        daily_equity.append({
            "date":   rec["date"],
            "equity": round(capital * (
                (closes[i] / position["buy_price"]) if position else 1.0
            ), 4),
        })

    # 回測結束仍持倉 → 強制平倉
    if position and records:
        last = records[-1]
        sell_price = last["close"]
        pnl = (sell_price - position["buy_price"]) / position["buy_price"] * 100
        capital *= (1 + pnl / 100)
        trades.append({
            "buy_date":    position["buy_date"],
            "buy_price":   round(position["buy_price"], 2),
            "peak_price":  round(position["peak_price"], 2),
            "sell_date":   last["date"],
            "sell_price":  round(sell_price, 2),
            "sell_reason": "回測結束強制平倉",
            "pnl":         round(pnl, 2),
            "held_days":   len(records) - 1 - position["buy_idx"],
        })

    return trades, daily_equity

# ══════════════════════════════════════════════════════
# API 路由
# ══════════════════════════════════════════════════════

def sma_val(arr, n):
    if not arr: return 0.0
    sl = arr[-n:] if len(arr) >= n else arr
    return sum(sl) / len(sl)

@app.route("/api/backtest", methods=["POST"])
def backtest():
    body       = request.get_json()
    code       = body.get("code", "").strip()
    start_date = body.get("start_date", "")
    end_date   = body.get("end_date",   "")
    conditions = body.get("conditions", [])
    take_profit   = float(body.get("take_profit", 0))
    stop_loss     = float(body.get("stop_loss",   0))
    hold_days     = int(body.get("hold_days",     0))
    trailing_stop = float(body.get("trailing_stop", 0))

    if not code or not start_date or not end_date:
        return jsonify({"error": "請填入股票代號與日期範圍"}), 400

    print(f"\n[回測] {code} {start_date}~{end_date} "
          f"停利:{take_profit}% 停損:{stop_loss}% "
          f"天數:{hold_days} 追蹤回落:{trailing_stop}%")
    records = fetch_all_history(code, start_date, end_date)
    if not records:
        return jsonify({"error": f"查無 {code} 的歷史資料"}), 404

    print(f"  資料筆數: {len(records)}")

    trades, daily_equity = run_backtest(
        records, conditions, start_date, end_date,
        take_profit, stop_loss, hold_days, trailing_stop
    )

    # 計算績效統計
    wins  = [t for t in trades if t["pnl"] > 0]
    loses = [t for t in trades if t["pnl"] <= 0]
    total_pnl = sum(t["pnl"] for t in trades)
    win_rate  = round(len(wins) / len(trades) * 100, 1) if trades else 0
    avg_win   = round(sum(t["pnl"] for t in wins)  / len(wins),  2) if wins  else 0
    avg_loss  = round(sum(t["pnl"] for t in loses) / len(loses), 2) if loses else 0
    max_draw  = 0.0
    peak = 100.0
    for d in daily_equity:
        if d["equity"] > peak: peak = d["equity"]
        dd = (peak - d["equity"]) / peak * 100
        if dd > max_draw: max_draw = dd

    # 整理 K 線資料（只回傳起訖區間）
    kline = []
    closes = [r["close"] for r in records]
    highs  = [r["high"]  for r in records]
    lows   = [r["low"]   for r in records]
    opens  = [r["open"]  for r in records]
    vols   = [r["vol"]   for r in records]
    dates  = [r["date"]  for r in records]
    k_ser, d_ser = calc_kd_series(closes, highs, lows, 9)
    rsi_ser = calc_rsi_series(closes, 14)

    for i, r in enumerate(records):
        if r["date"] < start_date or r["date"] > end_date: continue
        ma5  = round(sma_val(closes[max(0,i-4):i+1],  5),  2)
        ma20 = round(sma_val(closes[max(0,i-19):i+1], 20), 2)
        ma60 = round(sma_val(closes[max(0,i-59):i+1], 60), 2)
        kline.append({
            "date":  r["date"],
            "open":  r["open"],
            "high":  r["high"],
            "low":   r["low"],
            "close": r["close"],
            "vol":   r["vol"],
            "ma5":   ma5,
            "ma20":  ma20,
            "ma60":  ma60,
            "k":     k_ser[i],
            "d":     d_ser[i],
            "rsi":   rsi_ser[i],
        })

    return jsonify({
        "code":         code,
        "start_date":   start_date,
        "end_date":     end_date,
        "kline":        kline,
        "trades":       trades,
        "daily_equity": daily_equity,
        "stats": {
            "total_trades": len(trades),
            "win_trades":   len(wins),
            "lose_trades":  len(loses),
            "win_rate":     win_rate,
            "total_pnl":    round(total_pnl, 2),
            "avg_win":      avg_win,
            "avg_loss":     avg_loss,
            "max_drawdown": round(max_draw, 2),
        }
    })

# ══════════════════════════════════════════════════════
# 全市場回測：每天掃全市場，符合條件就買，同時持有多檔
# ══════════════════════════════════════════════════════

# 快取：避免重複抓同一支股票的歷史資料
_hist_cache = {}

def fetch_history_cached(code, start_date, end_date):
    key = f"{code}_{start_date[:7]}_{end_date[:7]}"
    if key not in _hist_cache:
        _hist_cache[key] = fetch_all_history(code, start_date, end_date)
    return _hist_cache[key]

def run_market_backtest(stock_codes, conditions, start_date, end_date,
                        take_profit, stop_loss, hold_days, trailing_stop,
                        max_positions, progress_cb=None):
    """
    全市場回測：
    - 每個交易日對所有股票計算指標，符合條件且有空位就買入
    - 同時持有最多 max_positions 檔，資金平均分配
    - 每檔獨立計算停利/停損/追蹤高點
    """
    # ① 先把所有股票的歷史資料抓回來並建立日期索引
    total = len(stock_codes)
    all_stock_data = {}   # code -> {date -> {open,high,low,close,vol}}
    all_dates_set  = set()

    for idx, code in enumerate(stock_codes):
        if progress_cb:
            progress_cb(f"抓取歷史資料 {code} ({idx+1}/{total})", idx / total * 40)
        try:
            records = fetch_history_cached(code, start_date, end_date)
            if len(records) < 10:
                continue
            date_map = {r["date"]: r for r in records}
            all_stock_data[code] = {"records": records, "date_map": date_map}
            all_dates_set.update(date_map.keys())
        except Exception as e:
            print(f"  [市場回測] {code} 失敗: {e}")

    # ② 取得所有交易日（排序）
    trading_days = sorted(d for d in all_dates_set
                          if start_date <= d <= end_date)
    if not trading_days:
        return [], [], {}

    # ③ 為每支股票預計算技術指標序列
    if progress_cb:
        progress_cb("計算各股技術指標...", 45)

    stock_indicators = {}  # code -> {date -> indicators_dict}
    for code, data in all_stock_data.items():
        recs = data["records"]
        closes = [r["close"] for r in recs]
        highs  = [r["high"]  for r in recs]
        lows   = [r["low"]   for r in recs]
        vols   = [r["vol"]   for r in recs]
        dates  = [r["date"]  for r in recs]
        k_ser, d_ser = calc_kd_series(closes, highs, lows, 9)
        rsi_ser = calc_rsi_series(closes, 14)

        ind_map = {}
        for i, r in enumerate(recs):
            prev_close = closes[i-1] if i > 0 else closes[i]
            chg = round((closes[i]-prev_close)/prev_close*100, 2) if prev_close>0 else 0
            avg5v  = round(sma(vols[max(0,i-5):i],  5))
            avg20v = round(sma(vols[max(0,i-20):i], 20))
            ma5    = round(sma(closes[max(0,i-4):i+1],  5),  2)
            ma20   = round(sma(closes[max(0,i-19):i+1], 20), 2)
            ma60   = round(sma(closes[max(0,i-59):i+1], 60), 2)
            tv = vols[i]
            ind_map[r["date"]] = {
                "price": closes[i], "open": r["open"],
                "high": highs[i], "low": lows[i], "vol": tv,
                "chgPct": chg,
                "todayVol": tv, "avg5Vol": avg5v, "avg20Vol": avg20v,
                "volVsAvg5":  round(tv/avg5v,  2) if avg5v>0  else 0,
                "volVsAvg20": round(tv/avg20v, 2) if avg20v>0 else 0,
                "priceVsMA20": round((closes[i]-ma20)/ma20*100,2) if ma20>0 else 0,
                "priceVsMA60": round((closes[i]-ma60)/ma60*100,2) if ma60>0 else 0,
                "ma20VsMA60":  round((ma20-ma60)/ma60*100, 2) if ma60>0 else 0,
                "ma5VsMA20":   round((ma5-ma20)/ma20*100,  2) if ma20>0 else 0,
                "kVal":  k_ser[i], "dVal": d_ser[i],
                "prevK": k_ser[i-1] if i>0 else 50,
                "prevD": d_ser[i-1] if i>0 else 50,
                "rsi14": rsi_ser[i],
            }
        stock_indicators[code] = ind_map

    # ④ 逐日模擬交易
    if progress_cb:
        progress_cb("模擬交易中...", 55)

    capital   = 100.0           # 基準 100（百分比）
    positions = {}              # code -> {buy_date,buy_price,buy_day_idx,peak_price}
    all_trades      = []
    daily_equity    = []
    day_signals     = {}        # date -> [符合條件的 code list]

    for day_idx, date in enumerate(trading_days):
        if progress_cb and day_idx % 20 == 0:
            pct = 55 + day_idx / len(trading_days) * 40
            progress_cb(f"模擬 {date}（{day_idx+1}/{len(trading_days)} 日）", pct)

        # ── 更新現有持倉（先處理賣出）──
        codes_to_sell = []
        for code, pos in positions.items():
            ind = stock_indicators.get(code, {}).get(date)
            if not ind:
                continue
            cur_price  = ind["price"]
            buy_price  = pos["buy_price"]
            held       = day_idx - pos["buy_day_idx"]
            cur_high   = ind["high"]

            # 更新最高點
            pos["peak_price"] = max(pos["peak_price"], cur_high)
            peak_price = pos["peak_price"]
            cur_chg    = (cur_price - buy_price) / buy_price * 100

            sell_reason = None
            sell_price  = cur_price

            if trailing_stop > 0 and peak_price > buy_price:
                drop = (peak_price - cur_price) / peak_price * 100
                if drop >= trailing_stop:
                    sell_price  = round(peak_price*(1-trailing_stop/100), 2)
                    sell_reason = f"追蹤高點回落{trailing_stop}%"

            if not sell_reason and take_profit > 0 and cur_chg >= take_profit:
                sell_price  = round(buy_price*(1+take_profit/100), 2)
                sell_reason = f"停利+{take_profit}%"

            if not sell_reason and stop_loss > 0 and cur_chg <= -stop_loss:
                sell_price  = round(buy_price*(1-stop_loss/100), 2)
                sell_reason = f"停損-{stop_loss}%"

            if not sell_reason and hold_days > 0 and held >= hold_days:
                sell_reason = f"持有{hold_days}日到期"

            if sell_reason:
                weight = 1.0 / max_positions
                pnl    = (sell_price - buy_price) / buy_price * 100
                capital_change = weight * pnl / 100
                capital *= (1 + capital_change)
                all_trades.append({
                    "code":        code,
                    "buy_date":    pos["buy_date"],
                    "buy_price":   round(buy_price, 2),
                    "peak_price":  round(peak_price, 2),
                    "sell_date":   date,
                    "sell_price":  round(sell_price, 2),
                    "sell_reason": sell_reason,
                    "pnl":         round(pnl, 2),
                    "held_days":   held,
                })
                codes_to_sell.append(code)

        for code in codes_to_sell:
            del positions[code]

        # ── 掃描買入訊號 ──
        signals = []
        for code, ind_map in stock_indicators.items():
            if code in positions:
                continue
            ind = ind_map.get(date)
            if ind and eval_conditions(ind, conditions):
                signals.append(code)

        day_signals[date] = signals

        # ── 買入（補足空位）──
        slots = max_positions - len(positions)
        for code in signals[:slots]:
            ind = stock_indicators.get(code, {}).get(date)
            if not ind: continue
            positions[code] = {
                "buy_date":    date,
                "buy_price":   ind["price"],
                "buy_day_idx": day_idx,
                "peak_price":  ind["price"],
            }

        # ── 計算當日資產 ──
        pos_value = 0.0
        for code, pos in positions.items():
            ind = stock_indicators.get(code, {}).get(date)
            cur = ind["price"] if ind else pos["buy_price"]
            w   = 1.0 / max_positions
            pos_value += w * (cur / pos["buy_price"] - 1) * 100

        daily_equity.append({
            "date":   date,
            "equity": round(capital * (1 + pos_value / 100), 4),
            "n_pos":  len(positions),
            "signals": len(signals),
        })

    # ── 強制平倉所有剩餘持倉 ──
    last_date = trading_days[-1] if trading_days else end_date
    for code, pos in list(positions.items()):
        ind = stock_indicators.get(code, {}).get(last_date)
        if not ind: continue
        sell_price = ind["price"]
        buy_price  = pos["buy_price"]
        pnl = (sell_price - buy_price) / buy_price * 100
        weight = 1.0 / max_positions
        capital *= (1 + weight * pnl / 100)
        all_trades.append({
            "code":        code,
            "buy_date":    pos["buy_date"],
            "buy_price":   round(buy_price, 2),
            "peak_price":  round(pos["peak_price"], 2),
            "sell_date":   last_date,
            "sell_price":  round(sell_price, 2),
            "sell_reason": "回測結束平倉",
            "pnl":         round(pnl, 2),
            "held_days":   len(trading_days) - 1 - pos["buy_day_idx"],
        })

    return all_trades, daily_equity, day_signals


# ── 進度追蹤 ─────────────────────────────────────────
import threading
_market_progress = {}  # task_id -> {pct, msg, done, result, error}

@app.route("/api/market_backtest", methods=["POST"])
def market_backtest():
    """啟動全市場回測（背景執行），立即回傳 task_id"""
    import uuid, time
    body       = request.get_json()
    start_date = body.get("start_date", "")
    end_date   = body.get("end_date",   "")
    conditions = body.get("conditions", [])
    take_profit   = float(body.get("take_profit", 0))
    stop_loss     = float(body.get("stop_loss",   0))
    hold_days     = int(body.get("hold_days",     0))
    trailing_stop = float(body.get("trailing_stop", 0))
    max_positions = int(body.get("max_positions", 5))
    max_stocks    = int(body.get("max_stocks", 100))  # 回測幾支股票

    if not start_date or not end_date or not conditions:
        return jsonify({"error": "請填入日期範圍與篩選條件"}), 400

    task_id = str(uuid.uuid4())[:8]
    _market_progress[task_id] = {"pct": 0, "msg": "準備中...", "done": False,
                                  "result": None, "error": None}

    def background():
        try:
            prog = _market_progress[task_id]

            def cb(msg, pct):
                prog["msg"] = msg
                prog["pct"] = round(pct, 1)
                print(f"  [{pct:.0f}%] {msg}")

            cb("從 TWSE 取得股票清單...", 2)

            # 取得上市股票清單
            url  = "https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL"
            resp = SESSION.get(url, timeout=15)
            resp.raise_for_status()
            all_rows = resp.json()
            codes = [
                r["Code"] for r in all_rows
                if str(r.get("Code","")).isdigit()
                and len(str(r.get("Code",""))) == 4
                and safe_float(r.get("ClosingPrice")) > 0
            ][:max_stocks]

            cb(f"取得 {len(codes)} 支股票，開始抓歷史資料...", 5)

            trades, daily_equity, day_signals = run_market_backtest(
                codes, conditions, start_date, end_date,
                take_profit, stop_loss, hold_days, trailing_stop,
                max_positions, progress_cb=cb
            )

            cb("計算績效統計...", 96)

            wins  = [t for t in trades if t["pnl"] > 0]
            loses = [t for t in trades if t["pnl"] <= 0]
            total_pnl = round(sum(t["pnl"]*1/max_positions for t in trades), 2)
            win_rate  = round(len(wins)/len(trades)*100, 1) if trades else 0
            avg_win   = round(sum(t["pnl"] for t in wins) /len(wins),  2) if wins  else 0
            avg_loss  = round(sum(t["pnl"] for t in loses)/len(loses), 2) if loses else 0
            max_draw  = 0.0
            peak_eq   = 100.0
            for d in daily_equity:
                if d["equity"] > peak_eq: peak_eq = d["equity"]
                dd = (peak_eq - d["equity"]) / peak_eq * 100
                if dd > max_draw: max_draw = dd

            # 每日信號統計（信號最多的前 20 日）
            top_signal_days = sorted(
                [{"date": d, "count": len(codes)} for d, codes in day_signals.items()],
                key=lambda x: x["count"], reverse=True
            )[:20]

            prog["result"] = {
                "start_date":   start_date,
                "end_date":     end_date,
                "stocks_tested": len(codes),
                "trades":       sorted(trades, key=lambda x: x["buy_date"]),
                "daily_equity": daily_equity,
                "top_signal_days": top_signal_days,
                "stats": {
                    "total_trades":  len(trades),
                    "win_trades":    len(wins),
                    "lose_trades":   len(loses),
                    "win_rate":      win_rate,
                    "total_return":  round(daily_equity[-1]["equity"] - 100, 2) if daily_equity else 0,
                    "avg_win":       avg_win,
                    "avg_loss":      avg_loss,
                    "max_drawdown":  round(max_draw, 2),
                    "max_positions": max_positions,
                }
            }
            prog["pct"] = 100
            prog["msg"] = "完成！"
            prog["done"] = True

        except Exception as e:
            import traceback
            traceback.print_exc()
            _market_progress[task_id]["error"] = str(e)
            _market_progress[task_id]["done"]  = True

    t = threading.Thread(target=background, daemon=True)
    t.start()
    return jsonify({"task_id": task_id})


@app.route("/api/market_backtest/progress/<task_id>")
def market_backtest_progress(task_id):
    prog = _market_progress.get(task_id)
    if not prog:
        return jsonify({"error": "找不到任務"}), 404
    return jsonify(prog)

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
    print(f"  📋 有效股票 {len(valid)} 支，開始計算指標...")

    def build_stock(row, hist):
        price   = safe_float(row.get("ClosingPrice"))
        high_p  = safe_float(row.get("HighestPrice"))
        low_p   = safe_float(row.get("LowestPrice"))
        change  = safe_float(row.get("Change"))
        vol_raw = safe_float(row.get("TradeVolume"))
        if price <= 0: return None
        today_vol = round(vol_raw / 1000)
        closes = [h["close"] for h in hist]
        highs  = [h["high"]  for h in hist]
        lows   = [h["low"]   for h in hist]
        vols   = [h["vol"]   for h in hist]
        all_c  = closes + [price]
        all_h  = highs  + [high_p or price]
        all_l  = lows   + [low_p  or price]
        all_v  = vols   + [today_vol]
        prev_close = closes[-1] if closes else price
        chg_pct = round((price-prev_close)/prev_close*100,2) if prev_close>0 else 0.0
        ma5    = round(sma(all_c, 5),  2)
        ma20   = round(sma(all_c, 20), 2)
        ma60   = round(sma(all_c, 60), 2)
        avg5v  = round(sma(all_v[:-1], 5))
        avg20v = round(sma(all_v[:-1], 20))
        kv,dv,pkv,pdv = calc_kd_series(all_c,all_h,all_l,9)[-1][0], \
                        calc_kd_series(all_c,all_h,all_l,9)[-1][0], 50.0, 50.0
        k_s, d_s = calc_kd_series(all_c, all_h, all_l, 9)
        kv, dv = k_s[-1], d_s[-1]
        pkv = k_s[-2] if len(k_s)>=2 else 50.0
        pdv = d_s[-2] if len(d_s)>=2 else 50.0
        rsi = calc_rsi_series(all_c, 14)[-1]
        return {
            "code": row.get("Code",""), "name": row.get("Name",""),
            "sector": "上市", "price": price, "chgPct": chg_pct,
            "chgAmt": change, "todayVol": today_vol,
            "avg5Vol": avg5v, "avg20Vol": avg20v,
            "volVsAvg5":  round(today_vol/avg5v, 2)  if avg5v>0  else 0.0,
            "volVsAvg20": round(today_vol/avg20v,2)  if avg20v>0 else 0.0,
            "priceVsMA20": round((price-ma20)/ma20*100,2) if ma20>0 else 0.0,
            "priceVsMA60": round((price-ma60)/ma60*100,2) if ma60>0 else 0.0,
            "ma20VsMA60":  round((ma20-ma60)/ma60*100, 2) if ma60>0 else 0.0,
            "ma5VsMA20":   round((ma5-ma20)/ma20*100,  2) if ma20>0 else 0.0,
            "kVal":kv, "dVal":dv, "prevK":pkv, "prevD":pdv,
            "rsi14":rsi, "spark":all_c[-20:], "isLive":True,
        }

    def fetch_history(code):
        records = []
        today = datetime.today()
        for delta in [1, 0]:
            d = today - timedelta(days=delta*32)
            ym = f"{d.year}{d.month:02d}01"
            try:
                url = (f"https://www.twse.com.tw/exchangeReport/STOCK_DAY"
                       f"?response=json&date={ym}&stockNo={code}")
                r = SESSION.get(url, timeout=8)
                data = r.json()
                if data.get("stat")!="OK" or not data.get("data"): continue
                for row in data["data"]:
                    c=safe_float(row[6]); h=safe_float(row[4])
                    l=safe_float(row[5]); v=round(safe_float(row[1])/1000)
                    if c>0: records.append({"close":c,"high":h,"low":l,"vol":v})
            except Exception as e:
                print(f"  [歷史] {code} {ym} 失敗: {e}")
        return records

    stocks = []
    limit  = min(150, len(valid))
    for i, row in enumerate(valid[:limit]):
        code = row.get("Code","")
        try:
            hist = fetch_history(code)
            if len(hist) < 5: continue
            obj = build_stock(row, hist)
            if obj:
                stocks.append(obj)
                print(f"  [{i+1:3d}/{limit}] {code} {obj['name']:<8} "
                      f"價:{obj['price']:.2f}  漲跌:{obj['chgPct']:+.2f}%")
        except Exception as e:
            print(f"  [錯誤] {code}: {e}")

    print(f"\n  ✅ 完成！共 {len(stocks)} 支")
    return jsonify({"stocks":stocks,"count":len(stocks),
                    "time":datetime.now().strftime("%Y-%m-%d %H:%M:%S")})

@app.route("/api/health")
def health():
    return jsonify({"status":"ok","time":datetime.now().isoformat()})

# ══════════════════════════════════════════════════════
# Telegram 通知模組
# ══════════════════════════════════════════════════════

import json, os, time, threading, schedule

# ── 設定檔路徑 ───────────────────────────────────────
SETTINGS_FILE = os.path.join(os.path.dirname(__file__), "alert_settings.json")

def load_settings():
    """載入警報設定"""
    default = {
        "telegram_token":   "",
        "telegram_chat_id": "",
        "enabled":          False,
        "check_interval":   5,      # 幾分鐘掃描一次（盤中）
        "watch_list":       [],     # [{code, name, buy_price, target_price, stop_price, direction}]
        "scan_conditions":  [],     # 選股條件（觸發買入訊號時通知）
        "notify_buy":       True,   # 觸發買入條件時通知
        "notify_sell":      True,   # 觸發賣出條件（停利/停損）時通知
    }
    if os.path.exists(SETTINGS_FILE):
        try:
            with open(SETTINGS_FILE, "r", encoding="utf-8") as f:
                saved = json.load(f)
            default.update(saved)
        except: pass
    return default

def save_settings(data):
    with open(SETTINGS_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def send_telegram(token, chat_id, message):
    """發送 Telegram 訊息"""
    if not token or not chat_id:
        print("[Telegram] 未設定 Token 或 Chat ID")
        return False
    try:
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        resp = SESSION.post(url, json={
            "chat_id":    chat_id,
            "text":       message,
            "parse_mode": "HTML",
        }, timeout=10)
        ok = resp.json().get("ok", False)
        if ok: print(f"[Telegram] ✅ 訊息已發送")
        else:  print(f"[Telegram] ❌ 發送失敗: {resp.text}")
        return ok
    except Exception as e:
        print(f"[Telegram] 錯誤: {e}")
        return False

# ── 取得即時股價 ─────────────────────────────────────
def get_realtime_prices(codes):
    """從 TWSE OpenAPI 取得最新收盤價"""
    try:
        url  = "https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL"
        resp = SESSION.get(url, timeout=15)
        resp.raise_for_status()
        rows = resp.json()
        price_map = {}
        for r in rows:
            code  = r.get("Code","")
            price = safe_float(r.get("ClosingPrice"))
            chg   = safe_float(r.get("Change"))
            if code in codes and price > 0:
                prev  = price - chg if chg else price
                pct   = round(chg/prev*100, 2) if prev > 0 else 0
                price_map[code] = {
                    "price":  price,
                    "change": chg,
                    "chgPct": pct,
                    "name":   r.get("Name",""),
                }
        return price_map
    except Exception as e:
        print(f"[警報] 取得股價失敗: {e}")
        return {}

# ── 警報檢查主邏輯 ────────────────────────────────────
def check_alerts():
    settings = load_settings()
    if not settings["enabled"]:
        return
    if not settings["telegram_token"] or not settings["telegram_chat_id"]:
        return

    token   = settings["telegram_token"]
    chat_id = settings["telegram_chat_id"]
    watch   = settings.get("watch_list", [])

    if not watch:
        return

    codes    = [w["code"] for w in watch]
    prices   = get_realtime_prices(codes)
    now_str  = datetime.now().strftime("%Y/%m/%d %H:%M")
    triggered = []

    for w in watch:
        code = w["code"]
        p    = prices.get(code)
        if not p: continue

        cur   = p["price"]
        name  = p["name"] or w.get("name","")
        pct   = p["chgPct"]
        chg   = p["change"]

        alerts_for_this = []

        # 目標價到達（停利）
        target = float(w.get("target_price") or 0)
        if target > 0 and cur >= target:
            alerts_for_this.append(
                f"🟢 <b>停利觸發</b>\n"
                f"股票：{name}（{code}）\n"
                f"現價：<b>{cur}</b> 元\n"
                f"目標價：{target} 元\n"
                f"漲跌：{'+' if chg>=0 else ''}{chg}（{'+' if pct>=0 else ''}{pct}%）\n"
                f"⏰ {now_str}"
            )

        # 停損價到達
        stop = float(w.get("stop_price") or 0)
        if stop > 0 and cur <= stop:
            alerts_for_this.append(
                f"🔴 <b>停損觸發</b>\n"
                f"股票：{name}（{code}）\n"
                f"現價：<b>{cur}</b> 元\n"
                f"停損價：{stop} 元\n"
                f"漲跌：{'+' if chg>=0 else ''}{chg}（{'+' if pct>=0 else ''}{pct}%）\n"
                f"⏰ {now_str}"
            )

        # 漲幅警報
        rise_pct = float(w.get("rise_alert_pct") or 0)
        if rise_pct > 0 and pct >= rise_pct:
            alerts_for_this.append(
                f"📈 <b>漲幅警報</b>\n"
                f"股票：{name}（{code}）\n"
                f"現價：<b>{cur}</b> 元\n"
                f"漲幅：<b>+{pct}%</b>（設定：+{rise_pct}%）\n"
                f"⏰ {now_str}"
            )

        # 跌幅警報
        fall_pct = float(w.get("fall_alert_pct") or 0)
        if fall_pct > 0 and pct <= -fall_pct:
            alerts_for_this.append(
                f"📉 <b>跌幅警報</b>\n"
                f"股票：{name}（{code}）\n"
                f"現價：<b>{cur}</b> 元\n"
                f"跌幅：<b>{pct}%</b>（設定：-{fall_pct}%）\n"
                f"⏰ {now_str}"
            )

        for msg in alerts_for_this:
            print(f"[警報] {msg[:60]}...")
            send_telegram(token, chat_id, msg)
            triggered.append({"code":code,"msg":msg})

    if triggered:
        print(f"[警報] 本次觸發 {len(triggered)} 個警報")
    else:
        print(f"[警報] {now_str} 掃描完畢，無觸發")

# ── 排程：交易時間每 N 分鐘掃一次 ──────────────────────
def is_trading_time():
    """判斷是否在台股交易時間內（週一至週五 09:00~13:35）"""
    now = datetime.now()
    if now.weekday() >= 5: return False   # 週末
    t = now.hour * 60 + now.minute
    return 9*60 <= t <= 13*60+35

_scheduler_started = False

def start_scheduler():
    global _scheduler_started
    if _scheduler_started: return
    _scheduler_started = True

    def run_loop():
        while True:
            settings = load_settings()
            interval = max(1, settings.get("check_interval", 5))
            # 重新設定排程（interval 可能被使用者更新）
            schedule.clear()
            schedule.every(interval).minutes.do(lambda: check_alerts() if is_trading_time() else None)
            print(f"[排程] 每 {interval} 分鐘掃描一次（交易時間內）")
            # 跑 interval 分鐘後重新讀設定
            end = time.time() + interval * 60
            while time.time() < end:
                schedule.run_pending()
                time.sleep(10)

    t = threading.Thread(target=run_loop, daemon=True)
    t.start()
    print("[排程] Telegram 警報排程已啟動")

# ══════════════════════════════════════════════════════
# Telegram 設定 API
# ══════════════════════════════════════════════════════

@app.route("/api/alert/settings", methods=["GET"])
def get_alert_settings():
    return jsonify(load_settings())

@app.route("/api/alert/settings", methods=["POST"])
def update_alert_settings():
    body = request.get_json()
    settings = load_settings()
    # 允許更新的欄位
    for key in ["telegram_token","telegram_chat_id","enabled",
                "check_interval","notify_buy","notify_sell","watch_list","scan_conditions"]:
        if key in body:
            settings[key] = body[key]
    save_settings(settings)
    # 如果啟用，確保排程在跑
    if settings.get("enabled"):
        start_scheduler()
    return jsonify({"ok": True, "settings": settings})

@app.route("/api/alert/test", methods=["POST"])
def test_telegram():
    """測試 Telegram 連線"""
    body    = request.get_json()
    token   = body.get("token","")
    chat_id = body.get("chat_id","")
    ok = send_telegram(token, chat_id,
        "✅ <b>台股智慧選股</b>\n\nTelegram 通知設定成功！\n\n"
        "你將在交易時間內收到買賣警報 📈\n"
        f"⏰ {datetime.now().strftime('%Y/%m/%d %H:%M')}")
    return jsonify({"ok": ok})

@app.route("/api/alert/trigger_now", methods=["POST"])
def trigger_check_now():
    """立即手動觸發一次掃描"""
    threading.Thread(target=check_alerts, daemon=True).start()
    return jsonify({"ok": True, "msg": "已觸發掃描"})

@app.route("/api/alert/watch", methods=["POST"])
def add_watch():
    """新增監控股票"""
    body     = request.get_json()
    settings = load_settings()
    watch    = settings.get("watch_list", [])
    code     = body.get("code","").strip()
    # 避免重複
    watch = [w for w in watch if w["code"] != code]
    watch.append({
        "code":           code,
        "name":           body.get("name",""),
        "buy_price":      float(body.get("buy_price",0)),
        "target_price":   float(body.get("target_price",0)),
        "stop_price":     float(body.get("stop_price",0)),
        "rise_alert_pct": float(body.get("rise_alert_pct",0)),
        "fall_alert_pct": float(body.get("fall_alert_pct",0)),
    })
    settings["watch_list"] = watch
    save_settings(settings)
    return jsonify({"ok": True, "watch_list": watch})

@app.route("/api/alert/watch/<code>", methods=["DELETE"])
def remove_watch(code):
    settings = load_settings()
    settings["watch_list"] = [w for w in settings.get("watch_list",[]) if w["code"] != code]
    save_settings(settings)
    return jsonify({"ok": True})

if __name__ == "__main__":
    print("="*50)
    print("  台股選股後端伺服器（含回測 + Telegram 警報）")
    print("  http://localhost:5000")
    print("="*50)
    print("\n安裝套件：pip install flask requests schedule\n")
    # 啟動時若已設定就自動啟動排程
    if load_settings().get("enabled"):
        start_scheduler()
    app.run(host="0.0.0.0", port=5000, debug=False)
