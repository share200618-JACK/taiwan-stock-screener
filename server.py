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
                        take_profit, stop_loss, hold_days, trailing_stop):
    if not records: return [], []
    closes = [r["close"] for r in records]
    highs  = [r["high"]  for r in records]
    lows   = [r["low"]   for r in records]
    vols   = [r["vol"]   for r in records]
    dates  = [r["date"]  for r in records]
    k_ser, d_ser = calc_kd_series(closes, highs, lows, 9)
    rsi_ser = calc_rsi_series(closes, 14)

    trades, daily_equity = [], []
    position = None
    capital  = 100.0

    for i, rec in enumerate(records):
        if rec["date"] < start_date: continue
        if rec["date"] > end_date:   break
        day = build_day_indicators(i, closes, highs, lows, vols, k_ser, d_ser, rsi_ser, dates)

        if position:
            buy_price  = position["buy_price"]
            held       = i - position["buy_idx"]
            cur_chg    = (closes[i] - buy_price) / buy_price * 100
            position["peak_price"] = max(position["peak_price"], highs[i])
            peak = position["peak_price"]
            sell_reason = None
            sell_price  = closes[i]

            if trailing_stop > 0 and peak > buy_price:
                drop = (peak - closes[i]) / peak * 100
                if drop >= trailing_stop:
                    sell_price  = round(peak*(1-trailing_stop/100), 2)
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
                pnl = (sell_price - buy_price) / buy_price * 100
                capital *= (1 + pnl/100)
                trades.append({
                    "buy_date":    position["buy_date"],
                    "buy_price":   round(buy_price, 2),
                    "peak_price":  round(peak, 2),
                    "sell_date":   rec["date"],
                    "sell_price":  round(sell_price, 2),
                    "sell_reason": sell_reason,
                    "pnl":         round(pnl, 2),
                    "held_days":   held,
                })
                position = None

        if not position and all(eval_cond(day, c) for c in conditions if c.get("enabled", True)):
            position = {"buy_date": rec["date"], "buy_price": closes[i],
                        "buy_idx": i, "peak_price": closes[i]}

        daily_equity.append({
            "date":   rec["date"],
            "equity": round(capital * ((closes[i]/position["buy_price"]) if position else 1.0), 4),
        })

    if position and records:
        last = records[-1]
        pnl  = (last["close"] - position["buy_price"]) / position["buy_price"] * 100
        capital *= (1 + pnl/100)
        trades.append({
            "buy_date":    position["buy_date"],
            "buy_price":   round(position["buy_price"], 2),
            "peak_price":  round(position["peak_price"], 2),
            "sell_date":   last["date"],
            "sell_price":  round(last["close"], 2),
            "sell_reason": "回測結束平倉",
            "pnl":         round(pnl, 2),
            "held_days":   len(records)-1-position["buy_idx"],
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
                        max_pos, progress_cb=None):
    total = len(codes)
    all_stock_ind = {}
    all_dates_set = set()

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

    if not code or not start_date or not end_date:
        return jsonify({"error":"請填入股票代號與日期範圍"}), 400

    print(f"\n[單股回測] {code} {start_date}~{end_date}")
    records = fetch_history_range(code, start_date, end_date)
    if not records:
        return jsonify({"error":f"查無 {code} 的歷史資料"}), 404

    trades, daily_equity = run_single_backtest(
        records, conditions, start_date, end_date,
        take_profit, stop_loss, hold_days, trailing_stop)

    wins  = [t for t in trades if t["pnl"]>0]
    loses = [t for t in trades if t["pnl"]<=0]
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

    return jsonify({
        "code":code,"start_date":start_date,"end_date":end_date,
        "kline":kline,"trades":trades,"daily_equity":daily_equity,
        "stats":{
            "total_trades":len(trades),
            "win_trades":len(wins),"lose_trades":len(loses),
            "win_rate":round(len(wins)/len(trades)*100,1) if trades else 0,
            "total_pnl":round(sum(t["pnl"] for t in trades),2),
            "avg_win": round(sum(t["pnl"] for t in wins)/len(wins),2)  if wins  else 0,
            "avg_loss":round(sum(t["pnl"] for t in loses)/len(loses),2) if loses else 0,
            "max_drawdown":round(max_draw,2),
        }
    })

# ══════════════════════════════════════════════════════
# API 路由 - 全市場回測（背景執行）
# ══════════════════════════════════════════════════════

_market_tasks = {}

@app.route("/api/market_backtest", methods=["POST"])
def market_backtest():
    import uuid
    body          = request.get_json()
    start_date    = body.get("start_date","")
    end_date      = body.get("end_date","")
    conditions    = body.get("conditions",[])
    take_profit   = float(body.get("take_profit",0))
    stop_loss     = float(body.get("stop_loss",0))
    hold_days     = int(body.get("hold_days",0))
    trailing_stop = float(body.get("trailing_stop",0))
    max_pos       = int(body.get("max_positions",5))
    max_stocks    = int(body.get("max_stocks",300))  # 預設改為全部上市股票

    if not start_date or not end_date or not conditions:
        return jsonify({"error":"請填入日期範圍與篩選條件"}), 400

    task_id = str(uuid.uuid4())[:8]
    _market_tasks[task_id] = {"pct":0,"msg":"準備中...","done":False,"result":None,"error":None}

    def bg():
        prog = _market_tasks[task_id]
        try:
            import random
            def cb(msg, pct):
                prog["msg"] = msg; prog["pct"] = round(pct,1)
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
            # 不限制時掃描全部，否則隨機抽樣確保不重複偏向特定股票
            if max_stocks >= len(all_codes):
                codes = all_codes
            else:
                codes = random.sample(all_codes, max_stocks)

            cb(f"掃描 {len(codes)} 支股票（上市共 {len(all_codes)} 支）...", 5)
            trades, daily_eq, day_signals = run_market_backtest(
                codes, conditions, start_date, end_date,
                take_profit, stop_loss, hold_days, trailing_stop, max_pos, cb)

            cb("計算績效統計...", 96)
            wins  = [t for t in trades if t["pnl"]>0]
            loses = [t for t in trades if t["pnl"]<=0]
            peak_eq=100.0; max_draw=0.0
            for d in daily_eq:
                if d["equity"]>peak_eq: peak_eq=d["equity"]
                dd=(peak_eq-d["equity"])/peak_eq*100
                if dd>max_draw: max_draw=dd

            prog["result"] = {
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
            prog["pct"]=100; prog["msg"]="完成！"; prog["done"]=True
        except Exception as e:
            import traceback; traceback.print_exc()
            prog["error"]=str(e); prog["done"]=True

    threading.Thread(target=bg, daemon=True).start()
    return jsonify({"task_id": task_id})

@app.route("/api/market_backtest/progress/<task_id>")
def market_backtest_progress(task_id):
    prog = _market_tasks.get(task_id)
    if not prog: return jsonify({"error":"找不到任務"}), 404
    return jsonify(prog)

# ══════════════════════════════════════════════════════
# 啟動
# ══════════════════════════════════════════════════════

if __name__ == "__main__":
    print("="*50)
    print("  台股選股 + 回測系統")
    print("  http://localhost:5000")
    print("="*50)
    print("\n安裝套件：pip install flask requests")
    print("雲端部署：pip install gunicorn\n")
    app.run(host="0.0.0.0", port=5000, debug=False)
