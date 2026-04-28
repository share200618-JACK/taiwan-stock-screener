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

# ══════════════════════════════════════════════════════
# Supabase 永久儲存（分析結果不怕 Render 重啟消失）
# ══════════════════════════════════════════════════════
# 設定方式：Render Dashboard → Environment → 新增以下兩個變數：
#   SUPABASE_URL  = https://xxx.supabase.co
#   SUPABASE_KEY  = 你的 anon public key

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")

def _sb_headers():
    return {
        "apikey":        SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type":  "application/json",
        "Prefer":        "return=representation",
    }

def supabase_save_analysis(data):
    """把每日分析結果存到 Supabase analysis_results 資料表"""
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("[Supabase] 未設定，跳過儲存")
        return False
    try:
        url = f"{SUPABASE_URL}/rest/v1/analysis_results"
        payload = {
            "date":         datetime.now().strftime("%Y-%m-%d"),
            "time":         data.get("time", ""),
            "model_ver":    data.get("model_ver", "v2"),
            "total_scanned":data.get("total_scanned", 0),
            "total_analyzed":data.get("total_analyzed", 0),
            "avg_accuracy": data.get("avg_accuracy", 0),
            "bullish":      data.get("bullish", 0),
            "bearish":      data.get("bearish", 0),
            "stocks":       json.dumps(data.get("stocks", []), ensure_ascii=False),
        }
        r = requests.post(url, json=payload, headers=_sb_headers(), timeout=10)
        if r.status_code in (200, 201):
            print(f"[Supabase] ✅ 分析結果已儲存（{len(data.get('stocks',[]))} 支推薦）")
            return True
        else:
            print(f"[Supabase] ❌ 儲存失敗 {r.status_code}: {r.text[:200]}")
            return False
    except Exception as e:
        print(f"[Supabase] ❌ 儲存例外: {e}")
        return False

def supabase_load_latest(date=None):
    """
    從 Supabase 讀取最新一筆分析結果。
    date: 指定日期（格式 YYYY-MM-DD），None = 最新一筆
    """
    if not SUPABASE_URL or not SUPABASE_KEY:
        return None
    try:
        url = f"{SUPABASE_URL}/rest/v1/analysis_results"
        params = {
            "order": "created_at.desc",
            "limit": "1",
        }
        if date:
            params["date"] = f"eq.{date}"
        r = requests.get(url, params=params, headers=_sb_headers(), timeout=10)
        if r.status_code == 200:
            rows = r.json()
            if rows:
                row = rows[0]
                # 把 stocks JSON 字串解回 list
                if isinstance(row.get("stocks"), str):
                    row["stocks"] = json.loads(row["stocks"])
                print(f"[Supabase] ✅ 讀取成功：{row.get('date')} {row.get('time')}")
                return row
        print(f"[Supabase] 讀取失敗 {r.status_code}")
        return None
    except Exception as e:
        print(f"[Supabase] 讀取例外: {e}")
        return None

def supabase_load_history(limit=30):
    """讀取最近 N 筆分析紀錄（用於顯示歷史頁面）"""
    if not SUPABASE_URL or not SUPABASE_KEY:
        return []
    try:
        url = f"{SUPABASE_URL}/rest/v1/analysis_results"
        params = {
            "select": "date,time,model_ver,total_scanned,total_analyzed,avg_accuracy,bullish,bearish,stocks",
            "order":  "created_at.desc",
            "limit":  str(limit),
        }
        r = requests.get(url, params=params, headers=_sb_headers(), timeout=10)
        if r.status_code == 200:
            rows = r.json()
            for row in rows:
                if isinstance(row.get("stocks"), str):
                    row["stocks"] = json.loads(row["stocks"])
            return rows
        return []
    except Exception as e:
        print(f"[Supabase] 歷史讀取失敗: {e}")
        return []


# ══════════════════════════════════════════════════════
# Supabase 追蹤清單 CRUD
# ══════════════════════════════════════════════════════

# ── 用戶認證 ──────────────────────────────────────

def _hash_pin(pin):
    """簡單 PIN hash（SHA256）"""
    import hashlib
    return hashlib.sha256(pin.encode()).hexdigest()

def _gen_token(username):
    """產生用戶唯一 token"""
    import hashlib, time
    raw = f"{username}_{time.time()}_{os.urandom(8).hex()}"
    return hashlib.sha256(raw.encode()).hexdigest()[:32]

def sb_user_register(username, pin):
    """註冊新用戶，回傳 token 或 None"""
    if not SUPABASE_URL or not SUPABASE_KEY:
        return None, "未設定資料庫"
    try:
        url   = f"{SUPABASE_URL}/rest/v1/tw_users"
        token = _gen_token(username)
        payload = {
            "username":   username.strip(),
            "pin_hash":   _hash_pin(pin),
            "user_token": token,
        }
        r = requests.post(url, json=payload, headers=_sb_headers(), timeout=10)
        if r.status_code in (200, 201):
            return token, None
        # 用戶名重複
        if "duplicate" in r.text.lower() or "unique" in r.text.lower():
            return None, "此暱稱已被使用，請換一個"
        return None, f"註冊失敗 ({r.status_code})"
    except Exception as e:
        return None, str(e)

def sb_user_login(username, pin):
    """登入，回傳 token 或 None"""
    if not SUPABASE_URL or not SUPABASE_KEY:
        return None, "未設定資料庫"
    try:
        url = f"{SUPABASE_URL}/rest/v1/tw_users"
        params = {"username": f"eq.{username.strip()}", "select": "user_token,pin_hash"}
        r = requests.get(url, params=params, headers=_sb_headers(), timeout=10)
        if r.status_code == 200:
            rows = r.json()
            if not rows:
                return None, "找不到此暱稱"
            row = rows[0]
            if row["pin_hash"] == _hash_pin(pin):
                return row["user_token"], None
            return None, "PIN 錯誤"
        return None, f"登入失敗 ({r.status_code})"
    except Exception as e:
        return None, str(e)

def sb_token_valid(token):
    """驗證 token 是否存在"""
    if not token or not SUPABASE_URL or not SUPABASE_KEY:
        return False
    try:
        url = f"{SUPABASE_URL}/rest/v1/tw_users"
        params = {"user_token": f"eq.{token}", "select": "id"}
        r = requests.get(url, params=params, headers=_sb_headers(), timeout=8)
        return r.status_code == 200 and len(r.json()) > 0
    except:
        return False

# ── Watchlist（支援多用戶）─────────────────────────

def sb_watchlist_load(user_token="default"):
    """讀取指定用戶的追蹤清單"""
    if not SUPABASE_URL or not SUPABASE_KEY:
        return []
    try:
        url = f"{SUPABASE_URL}/rest/v1/watchlist"
        params = {"user_token": f"eq.{user_token}",
                  "order": "created_at.desc", "limit": "200"}
        r = requests.get(url, params=params, headers=_sb_headers(), timeout=10)
        if r.status_code == 200:
            return r.json()
        return []
    except Exception as e:
        print(f"[Watchlist] 讀取失敗: {e}")
        return []

def sb_watchlist_add(code, name, add_price, add_time, sector, note="", user_token="default"):
    """新增追蹤股票"""
    if not SUPABASE_URL or not SUPABASE_KEY:
        return False
    try:
        url = f"{SUPABASE_URL}/rest/v1/watchlist"
        # 先刪除該用戶的同代號舊資料
        requests.delete(url, params={"code": f"eq.{code}",
                                     "user_token": f"eq.{user_token}"},
                       headers=_sb_headers(), timeout=8)
        payload = {"code": code, "name": name, "add_price": add_price,
                   "add_time": add_time, "sector": sector, "note": note,
                   "user_token": user_token}
        r = requests.post(url, json=payload, headers=_sb_headers(), timeout=10)
        return r.status_code in (200, 201)
    except Exception as e:
        print(f"[Watchlist] 新增失敗: {e}")
        return False

def sb_watchlist_remove(code, user_token="default"):
    """刪除追蹤股票"""
    if not SUPABASE_URL or not SUPABASE_KEY:
        return False
    try:
        url = f"{SUPABASE_URL}/rest/v1/watchlist"
        r = requests.delete(url, params={"code": f"eq.{code}",
                                         "user_token": f"eq.{user_token}"},
                           headers=_sb_headers(), timeout=8)
        return r.status_code in (200, 204)
    except Exception as e:
        print(f"[Watchlist] 刪除失敗: {e}")
        return False

def sb_watchlist_update_note(code, note, user_token="default"):
    """更新筆記"""
    if not SUPABASE_URL or not SUPABASE_KEY:
        return False
    try:
        url = f"{SUPABASE_URL}/rest/v1/watchlist"
        r = requests.patch(url, json={"note": note},
                          params={"code": f"eq.{code}",
                                  "user_token": f"eq.{user_token}"},
                          headers=_sb_headers(), timeout=8)
        return r.status_code in (200, 204)
    except Exception as e:
        print(f"[Watchlist] 更新筆記失敗: {e}")
        return False


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
    result = _market_map.get(str(code))
    # 若市場表為空（載入失敗），根據股票代號長度猜測
    if result is None:
        code_str = str(code)
        if len(code_str) == 4 and code_str.isdigit():
            # 5 開頭通常是上櫃，6 開頭通常是上市，其他不確定
            first = code_str[0]
            if first in ("5", "6") and int(code_str) >= 5000:
                return "otc"
    return result

# 背景預載市場表
threading.Thread(target=_load_market_map, daemon=True).start()

# ── 靜態頁面 ─────────────────────────────────────
@app.route("/")
def index():
    return send_from_directory(".", "index.html")

@app.route("/login")
def login_page():
    return send_from_directory(".", "login.html")

@app.route("/auth.js")
def auth_js():
    return send_from_directory(".", "auth.js", mimetype="application/javascript")

@app.route("/backtest")
def backtest_page():
    return send_from_directory(".", "backtest.html")

@app.route("/alert")
def alert_page():
    return send_from_directory(".", "alert.html")

@app.route("/predict")
def predict_page():
    return send_from_directory(".", "predict.html")

@app.route("/watchlist")
def watchlist_page():
    return send_from_directory(".", "watchlist.html")

# ── 用戶認證 API ──────────────────────────────────

@app.route("/api/auth/test")
def api_auth_test():
    """測試 Supabase tw_users 連線"""
    if not SUPABASE_URL or not SUPABASE_KEY:
        return jsonify({"ok": False, "error": "未設定 SUPABASE_URL 或 SUPABASE_KEY"})
    try:
        url = f"{SUPABASE_URL}/rest/v1/tw_users"
        r = requests.get(url, params={"limit": "1"}, headers=_sb_headers(), timeout=8)
        return jsonify({
            "ok": r.status_code == 200,
            "status": r.status_code,
            "supabase_url": SUPABASE_URL[:40] + "...",
            "key_prefix": SUPABASE_KEY[:20] + "...",
            "response": r.text[:200]
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})

@app.route("/api/auth/register", methods=["POST"])
def api_register():
    body     = request.get_json() or {}
    username = body.get("username","").strip()
    pin      = body.get("pin","").strip()
    if not username or len(username) < 2:
        return jsonify({"error": "暱稱至少 2 個字"}), 400
    if not pin or len(pin) != 4 or not pin.isdigit():
        return jsonify({"error": "PIN 必須是 4 位數字"}), 400
    token, err = sb_user_register(username, pin)
    if err:
        return jsonify({"error": err}), 400
    return jsonify({"ok": True, "token": token, "username": username})

@app.route("/api/auth/login", methods=["POST"])
def api_login():
    body     = request.get_json() or {}
    username = body.get("username","").strip()
    pin      = body.get("pin","").strip()
    if not username or not pin:
        return jsonify({"error": "請輸入暱稱和 PIN"}), 400
    token, err = sb_user_login(username, pin)
    if err:
        return jsonify({"error": err}), 401
    return jsonify({"ok": True, "token": token, "username": username})

@app.route("/api/auth/verify", methods=["POST"])
def api_verify():
    body  = request.get_json() or {}
    token = body.get("token","")
    valid = sb_token_valid(token)
    return jsonify({"valid": valid})

def _get_token():
    """從 request header 或 body 取得 user_token"""
    token = request.headers.get("X-User-Token","")
    if not token:
        body  = request.get_json(silent=True) or {}
        token = body.get("user_token","")
    return token or "default"

# ── Watchlist API ──────────────────────────────────

@app.route("/api/watchlist", methods=["GET"])
def api_watchlist_get():
    token = request.args.get("token", "default")
    rows  = sb_watchlist_load(token)
    return jsonify({"stocks": rows, "count": len(rows)})

@app.route("/api/watchlist", methods=["POST"])
def api_watchlist_add():
    body      = request.get_json() or {}
    code      = body.get("code","").strip()
    name      = body.get("name","")
    add_price = float(body.get("add_price", 0) or 0)
    add_time  = body.get("add_time", datetime.now().strftime("%Y/%m/%d %H:%M"))
    sector    = body.get("sector","")
    note      = body.get("note","")
    token     = body.get("user_token","default")
    if not code:
        return jsonify({"error": "缺少代號"}), 400
    ok = sb_watchlist_add(code, name, add_price, add_time, sector, note, token)
    return jsonify({"ok": ok, "code": code})

@app.route("/api/watchlist/<code>", methods=["DELETE"])
def api_watchlist_remove(code):
    body  = request.get_json(silent=True) or {}
    token = body.get("user_token") or request.args.get("token","default")
    ok    = sb_watchlist_remove(code, token)
    return jsonify({"ok": ok, "code": code})

@app.route("/api/watchlist/<code>/note", methods=["PATCH"])
def api_watchlist_note(code):
    body  = request.get_json() or {}
    note  = body.get("note","")
    token = body.get("user_token","default")
    ok    = sb_watchlist_update_note(code, note, token)
    return jsonify({"ok": ok})

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
        # ① 優先用 FinMind（穩定，不會被 TPEX block）
        token = _get_finmind_token()
        if token:
            try:
                fm_start = (datetime.strptime(start_date, "%Y-%m-%d") - timedelta(days=90)).strftime("%Y-%m-%d")
                r = SESSION.get("https://api.finmindtrade.com/api/v4/data",
                                params={"dataset":"TaiwanStockPrice","data_id":code,
                                        "start_date":fm_start,"end_date":end_date},
                                headers={"Authorization": f"Bearer {token}"},
                                timeout=15)
                data = r.json()
                if data.get("status") == 200:
                    for row in data.get("data", []):
                        c = safe_float(row.get("close", 0))
                        if c > 0:
                            otc_records.append({
                                "date":   row.get("date","")[:10],
                                "open":   safe_float(row.get("open", 0)),
                                "high":   safe_float(row.get("max", 0)),
                                "low":    safe_float(row.get("min", 0)),
                                "close":  c,
                                "vol":    round(safe_float(row.get("Trading_Volume", 0)) / 1000),
                                "change": safe_float(row.get("spread", 0)),
                            })
                    if otc_records:
                        print(f"  [OTC FinMind] {code}: {len(otc_records)} 筆")
            except Exception as e:
                print(f"  [OTC FinMind] {code}: {e}")

        # ② FinMind 沒資料才 fallback 到 TPEX
        if not otc_records:
            tmp_cur = cur
            while tmp_cur <= end_dt:
                roc_year = tmp_cur.year - 1911
                ym_otc   = f"{roc_year}/{tmp_cur.month:02d}"
                fetched  = False

                # 方法一：TPEX 舊版 API
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
                _time.sleep(2.0)  # 加長等待避免 TPEX block

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
        # ① 優先用 FinMind（穩定，不會被 block）
        token = _get_finmind_token()
        if token:
            try:
                start_fm = (today - timedelta(days=65)).strftime("%Y-%m-%d")
                r = SESSION.get("https://api.finmindtrade.com/api/v4/data",
                                params={"dataset":"TaiwanStockPrice","data_id":code,
                                        "start_date":start_fm},
                                headers={"Authorization": f"Bearer {token}"},
                                timeout=12)
                data = r.json()
                if data.get("status") == 200:
                    for row in data.get("data", []):
                        c = safe_float(row.get("close", 0))
                        if c > 0:
                            records.append({
                                "close": c,
                                "high":  safe_float(row.get("max", 0)),
                                "low":   safe_float(row.get("min", 0)),
                                "vol":   round(safe_float(row.get("Trading_Volume", 0)) / 1000),
                            })
            except Exception as e:
                print(f"  [OTC FinMind recent] {code}: {e}")

        # ② FinMind 沒資料才 fallback 到 TPEX（加長 sleep 避免 block）
        if not records:
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
                except Exception as e:
                    print(f"  [OTC TPEX fallback] {code} {ym_otc}: {e}")
                _time.sleep(2.0)  # 加長等待避免 TPEX block

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
    market = get_market(code)
    records = fetch_history_range(code, start_date, end_date)
    if not records:
        mkt_hint = "（上櫃，嘗試 TPEX）" if market == "otc" else "（上市，嘗試 TWSE）" if market == "twse" else "（市場別未知，兩邊都試過）"
        return jsonify({"error": f"查無 {code} 的歷史資料 {mkt_hint}，請確認代號正確或稍後再試"}), 404

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

def fetch_monthly_revenue_finmind(code, start_date):
    """
    FinMind 月營收（抓飆股的關鍵基本面指標）
    dataset: TaiwanStockMonthRevenue
    欄位: date, stock_id, revenue, revenue_month, revenue_year
    回傳: sorted list of {date, revenue, yoy_pct, mom_pct, accumulated_yoy}
          依日期升冪排序，已計算年增率、月增率
    """
    rows = fetch_finmind("TaiwanStockMonthRevenue", code, start_date)
    if not rows:
        return []

    # 建立 (year, month) -> revenue map（單月營收）
    rev_map = {}
    for row in rows:
        try:
            y = int(row.get("revenue_year", 0))
            m = int(row.get("revenue_month", 0))
            r = int(row.get("revenue", 0))
            if y > 0 and m > 0 and r > 0:
                rev_map[(y, m)] = {"revenue": r, "date": row.get("date","")[:10]}
        except: continue

    # 依時間排序並計算 YoY、MoM
    result = []
    for (y, m) in sorted(rev_map.keys()):
        cur   = rev_map[(y, m)]
        rev   = cur["revenue"]
        # 去年同月
        prev_y = rev_map.get((y-1, m))
        yoy = ((rev - prev_y["revenue"]) / prev_y["revenue"] * 100) if prev_y and prev_y["revenue"] > 0 else 0
        # 上個月
        pm_y, pm_m = (y, m-1) if m > 1 else (y-1, 12)
        prev_m = rev_map.get((pm_y, pm_m))
        mom = ((rev - prev_m["revenue"]) / prev_m["revenue"] * 100) if prev_m and prev_m["revenue"] > 0 else 0

        result.append({
            "date":    cur["date"],
            "year":    y, "month": m,
            "revenue": rev,
            "yoy_pct": round(yoy, 2),   # 年增率 %
            "mom_pct": round(mom, 2),   # 月增率 %
        })
    return result

def get_revenue_features(rev_list, ref_date):
    """
    取得指定日期當下可用的最新月營收特徵。
    台灣上市櫃公司每月 10 日前公布上月營收。
    為避免未來資料洩漏（look-ahead bias），只使用 ref_date 當月或之前的資料。

    回傳 4 個特徵：
      - latest_yoy:    最新月營收年增率 %（3167 在飆漲前 yoy 持續破 100%）
      - latest_mom:    最新月營收月增率 %
      - avg_yoy_3m:    近 3 個月平均年增率（看是否持續高成長）
      - yoy_trend:     近 3 個月年增率是「持續加速」= +1、持平 = 0、減速 = -1
    """
    if not rev_list or not ref_date:
        return {"latest_yoy": 0, "latest_mom": 0, "avg_yoy_3m": 0, "yoy_trend": 0}
    try:
        ref_dt = datetime.strptime(ref_date[:10], "%Y-%m-%d")
    except:
        return {"latest_yoy": 0, "latest_mom": 0, "avg_yoy_3m": 0, "yoy_trend": 0}

    # 只取 ref_date 之前至少一個月公布的資料（避免未來資料）
    # 假設公告日為資料月份的次月 10 日
    usable = []
    for r in rev_list:
        # r 的資料月份
        pub_dt = datetime(r["year"], r["month"], 15) + timedelta(days=30)  # 預估公告日
        if pub_dt <= ref_dt:
            usable.append(r)

    if not usable:
        return {"latest_yoy": 0, "latest_mom": 0, "avg_yoy_3m": 0, "yoy_trend": 0}

    # 最近 3 個月
    last3 = usable[-3:]
    latest = usable[-1]
    avg_yoy_3m = sum(x["yoy_pct"] for x in last3) / len(last3)

    # 趨勢：比較最後 3 個月的 YoY 斜率
    if len(last3) >= 3:
        if last3[-1]["yoy_pct"] > last3[-2]["yoy_pct"] > last3[-3]["yoy_pct"]:
            trend = 1     # 持續加速
        elif last3[-1]["yoy_pct"] < last3[-2]["yoy_pct"] < last3[-3]["yoy_pct"]:
            trend = -1    # 持續減速
        else:
            trend = 0
    else:
        trend = 0

    return {
        "latest_yoy":  round(latest["yoy_pct"], 2),
        "latest_mom":  round(latest["mom_pct"], 2),
        "avg_yoy_3m":  round(avg_yoy_3m, 2),
        "yoy_trend":   trend,
    }

def fetch_financial_finmind(code, start_date):
    """
    FinMind 綜合損益表（季報）—— 抓 EPS 與獲利品質
    dataset: TaiwanStockFinancialStatements
    欄位: date, stock_id, type(EPS/GrossProfit/OperatingIncome/...), value, origin_name

    FinMind 的財報是「每季一筆」，我們提取：
      - EPS（每股盈餘）
      - GrossProfit（毛利）→ 可推算毛利率
      - OperatingIncome（營業利益）
      - Revenue（營收，但用月營收更準）
      - IncomeAfterTax（稅後淨利）

    回傳: sorted list of {date, eps, gross_profit, operating_income, ...}
    """
    rows = fetch_finmind("TaiwanStockFinancialStatements", code, start_date)
    if not rows:
        return []

    # 依日期分組
    quarters = {}
    for row in rows:
        date = row.get("date","")[:10]
        typ  = row.get("type","")
        try:
            val = float(row.get("value", 0) or 0)
        except:
            val = 0
        if date not in quarters:
            quarters[date] = {"date": date}
        # 映射重要欄位
        if   typ == "EPS":              quarters[date]["eps"] = val
        elif typ == "GrossProfit":      quarters[date]["gross_profit"] = val
        elif typ == "OperatingIncome":  quarters[date]["operating_income"] = val
        elif typ == "IncomeAfterTax":   quarters[date]["net_income"] = val
        elif typ == "Revenue":          quarters[date]["revenue"] = val

    result = sorted(quarters.values(), key=lambda x: x["date"])
    # 計算 EPS 的年增率（與去年同季比較）
    for i, q in enumerate(result):
        q["eps_yoy"] = 0
        q["gross_margin"] = 0
        # 毛利率 = 毛利 / 營收
        rev = q.get("revenue", 0)
        gp  = q.get("gross_profit", 0)
        if rev > 0:
            q["gross_margin"] = round(gp / rev * 100, 2)
        # EPS 年增率：找 4 季前（約 1 年前）的 EPS
        cur_eps = q.get("eps", 0)
        if i >= 4:
            prev_eps = result[i-4].get("eps", 0)
            if prev_eps != 0:
                q["eps_yoy"] = round((cur_eps - prev_eps) / abs(prev_eps) * 100, 2)
    return result

def get_financial_features(fin_list, ref_date):
    """
    取得指定日期當下可用的最新財報特徵（避免未來資料洩漏）。
    台灣規定：季報要在季末後 45 天內公布（Q1→5/15、Q2→8/14、Q3→11/14、Q4→3/31）

    回傳 4 個特徵：
      - latest_eps:         最新一季 EPS
      - eps_yoy:            最新一季 EPS 年增率（與去年同季比）
      - gross_margin:       毛利率 %
      - gross_margin_trend: 毛利率趨勢（+1 上升、0 持平、-1 下降）
    """
    default = {"latest_eps": 0, "eps_yoy": 0,
               "gross_margin": 0, "gross_margin_trend": 0}
    if not fin_list or not ref_date:
        return default

    try:
        ref_dt = datetime.strptime(ref_date[:10], "%Y-%m-%d")
    except:
        return default

    # 只用 ref_date 之前 60 天（保守估計公告延遲）公布的季報
    usable = []
    for q in fin_list:
        try:
            q_dt = datetime.strptime(q["date"][:10], "%Y-%m-%d")
            pub_dt = q_dt + timedelta(days=60)  # 公告延遲 60 天
            if pub_dt <= ref_dt:
                usable.append(q)
        except: continue

    if not usable:
        return default

    latest = usable[-1]
    # 毛利率趨勢（近 3 季）
    trend = 0
    if len(usable) >= 3:
        last3 = usable[-3:]
        gm_vals = [q.get("gross_margin", 0) for q in last3]
        if gm_vals[-1] > gm_vals[-2] > gm_vals[-3]:   trend = 1
        elif gm_vals[-1] < gm_vals[-2] < gm_vals[-3]: trend = -1

    return {
        "latest_eps":         round(latest.get("eps", 0), 2),
        "eps_yoy":            round(latest.get("eps_yoy", 0), 2),
        "gross_margin":       round(latest.get("gross_margin", 0), 2),
        "gross_margin_trend": trend,
    }

# ══════════════════════════════════════════════════════
# 產業輪動系統
# ══════════════════════════════════════════════════════

# 全域快取
_sector_map    = {}      # code -> industry_category
_sector_cache  = {}      # industry_category -> 近期漲幅統計
_sector_loaded_date = "" # 最後載入日期

# 台股主要類股名稱（用於顯示）
SECTOR_DISPLAY = {
    "半導體業": "半導體",   "電腦及週邊設備業": "電腦週邊",
    "電子零組件業": "電子零件", "光電業": "光電",
    "其他電子業": "其他電子", "通信網路業": "通信網路",
    "電機機械": "電機機械",  "化學工業": "化學",
    "鋼鐵工業": "鋼鐵",     "航運業": "航運",
    "金融保險業": "金融",    "建材營造業": "建材營造",
    "生技醫療業": "生技醫療", "食品工業": "食品",
    "紡織纖維": "紡織",      "汽車工業": "汽車",
}

def _load_sector_map():
    """從 FinMind TaiwanStockInfo 建立股票代號 → 產業別對照表"""
    global _sector_map, _sector_loaded_date
    today = datetime.today().strftime("%Y-%m-%d")
    if _sector_loaded_date == today and _sector_map:
        return
    try:
        params = {"dataset": "TaiwanStockInfo"}
        token  = _get_finmind_token()
        headers = {"Authorization": f"Bearer {token}"} if token else {}
        r = SESSION.get(FINMIND_URL, params=params, headers=headers, timeout=15)
        data = r.json()
        if data.get("status") == 200:
            for row in data.get("data", []):
                code = str(row.get("stock_id","")).strip()
                cat  = row.get("industry_category","").strip()
                if code and cat:
                    _sector_map[code] = cat
            _sector_loaded_date = today
            print(f"[產業表] 載入 {len(_sector_map)} 支股票的產業別")
    except Exception as e:
        print(f"[產業表] 載入失敗: {e}")

def get_sector(code):
    """取得股票的產業別"""
    if not _sector_map:
        _load_sector_map()
    return _sector_map.get(str(code), "其他")

def compute_sector_rotation(all_stocks_data):
    """
    計算各類股近期輪動強弱。
    all_stocks_data: [{code, chg_pct, price, ...}, ...]（來自 TWSE 當日資料）

    回傳: {
      industry_category: {
        "avg_chg":    float,  # 類股平均漲跌幅
        "up_ratio":   float,  # 上漲股比例
        "rank":       int,    # 強弱排名（1=最強）
        "stock_count":int,
      }
    }
    """
    if not _sector_map:
        _load_sector_map()

    # 依產業分組計算漲跌
    sector_changes = {}
    for s in all_stocks_data:
        code    = str(s.get("code",""))
        chg_pct = float(s.get("chg_pct", s.get("pct", 0)))
        sector  = _sector_map.get(code, "其他")
        if sector not in sector_changes:
            sector_changes[sector] = []
        sector_changes[sector].append(chg_pct)

    result = {}
    for sector, changes in sector_changes.items():
        if len(changes) < 3:  # 不足 3 支不計算
            continue
        avg_chg  = round(sum(changes) / len(changes), 2)
        up_count = sum(1 for c in changes if c > 0)
        result[sector] = {
            "avg_chg":     avg_chg,
            "up_ratio":    round(up_count / len(changes), 2),
            "stock_count": len(changes),
        }

    # 依平均漲幅排名（用 avg_chg + up_ratio 加權）
    sorted_sectors = sorted(
        result.items(),
        key=lambda x: x[1]["avg_chg"] * 0.6 + x[1]["up_ratio"] * 5 * 0.4,
        reverse=True
    )
    for rank, (sector, data) in enumerate(sorted_sectors, 1):
        result[sector]["rank"] = rank
        result[sector]["is_hot"] = rank <= 5  # 前 5 名算「熱門類股」

    return result

def get_sector_feature(code, sector_rotation):
    """
    取得個股所屬類股的輪動特徵。
    回傳 3 個特徵：
      - sector_rank_pct:  類股排名百分位（0=最強 1=最弱）
      - sector_avg_chg:   類股平均漲跌幅
      - sector_up_ratio:  類股上漲股比例
    """
    sector  = _sector_map.get(str(code), "")
    data    = sector_rotation.get(sector, {})
    n       = len(sector_rotation)
    rank    = data.get("rank", n // 2)
    rank_pct= (rank - 1) / max(n - 1, 1)  # 0=最強, 1=最弱
    return {
        "sector_name":      sector,
        "sector_rank_pct":  round(1 - rank_pct, 3),  # 反轉：1=最強, 0=最弱
        "sector_avg_chg":   data.get("avg_chg", 0),
        "sector_up_ratio":  data.get("up_ratio", 0.5),
        "sector_is_hot":    1 if data.get("is_hot", False) else 0,
    }


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

def _screen_value_growth(stocks, top_n, task_id):
    """
    低基期價值成長股篩選器（v3.0 護城河模式）
    條件：
      1. 月營收年增率 >= 10%（成長中）
      2. 外資/投信近期淨買超（法人看好）
      3. 股價在 120 日低點附近（低基期，相對低點）
      4. EPS > 0（賺錢的公司）
      5. 毛利率 > 20%（有護城河，不是純低毛利代工）
      6. 股價未超漲（距 60 日高點 < 20%，還沒被炒高）
    """
    prog = _analyze_tasks.get(task_id, {})
    def cb(msg, pct):
        if prog: prog["msg"] = msg; prog["pct"] = round(pct, 1)
        print(f"  [價值選股 {pct:.0f}%] {msg}")

    cb("載入產業別對照表...", 2)
    _load_sector_map()

    start_dt = (datetime.today() - timedelta(days=26*31)).strftime("%Y-%m-%d")
    end_dt   = datetime.today().strftime("%Y-%m-%d")
    token    = _get_finmind_token()

    results  = []
    total    = len(stocks)

    for idx, s in enumerate(stocks):
        code = s["code"]
        name = s["name"]
        pct_done = 5 + idx / total * 88
        cb(f"[價值] {code} {name} ({idx+1}/{total})", pct_done)

        try:
            # 抓歷史股價
            records = fetch_history_range(code, start_dt, end_dt)
            if len(records) < 60:
                continue

            closes = [r["close"] for r in records]
            highs  = [r["high"]  for r in records]
            lows   = [r["low"]   for r in records]
            vols   = [r["vol"]   for r in records]
            cur    = closes[-1]

            # ── 技術面：低基期判斷 ──────────────────
            hi120 = max(highs[-120:]) if len(highs) >= 120 else max(highs)
            lo120 = min(lows[-120:])  if len(lows)  >= 120 else min(lows)
            hi60  = max(highs[-60:])  if len(highs) >= 60  else max(highs)
            lo60  = min(lows[-60:])   if len(lows)  >= 60  else min(lows)

            # 股價位置（在 120 日區間的百分位）
            price_pos_120 = (cur - lo120) / (hi120 - lo120 + 0.001)
            # 距 60 日高點距離（負值=在高點以下）
            dist_from_hi60 = (cur - hi60) / hi60 * 100

            # 條件：股價在 120 日區間的下半段（低基期）
            if price_pos_120 > 0.45:
                continue
            # 條件：距 60 日高點不超過 25%（不是已大幅回落的死股）
            if dist_from_hi60 < -25:
                continue

            # ── 均線多頭排列（起漲條件）─────────────
            def sma_last(arr, n):
                sl = arr[-n:] if len(arr) >= n else arr
                return sum(sl) / len(sl)

            ma20  = sma_last(closes, 20)
            ma60  = sma_last(closes, 60)
            ma120 = sma_last(closes, 120)

            # MA20 要在 MA60 附近（整理中，未噴出）
            ma20_vs_ma60 = (ma20 - ma60) / ma60 * 100
            if ma20_vs_ma60 > 15:   # 已漲太多
                continue
            if ma20_vs_ma60 < -10:  # 已跌太深
                continue

            # ── 基本面：FinMind 月營收 + 財報 ────────
            rev_yoy     = 0
            eps         = 0
            gross_margin= 0
            eps_yoy     = 0

            if token:
                rev_list = fetch_monthly_revenue_finmind(code, start_dt)
                if rev_list:
                    rf = get_revenue_features(rev_list, end_dt)
                    rev_yoy = rf["latest_yoy"]
                    # 月營收年增率需 >= 10%
                    if rev_yoy < 10:
                        continue
                else:
                    continue  # 沒有營收資料，跳過

                fin_list = fetch_financial_finmind(code, start_dt)
                if fin_list:
                    ff = get_financial_features(fin_list, end_dt)
                    eps          = ff["latest_eps"]
                    gross_margin = ff["gross_margin"]
                    eps_yoy      = ff["eps_yoy"]

                    # ★ 修正 1：允許轉型初期低 EPS（類似一詮起漲前的狀態）
                    # 條件：EPS > -0.5（不能大幅虧損）且 EPS YoY > 30%（獲利在改善）
                    # 或 EPS > 1（穩定獲利型）
                    if eps <= -0.5:
                        continue  # 嚴重虧損，排除
                    if eps <= 0 and eps_yoy < 30:
                        continue  # 虧損且獲利沒有改善跡象，排除
                    if 0 < eps <= 1 and eps_yoy < 20:
                        continue  # EPS 低且沒有成長動能，排除

                    # ★ 修正 2：放寬毛利率門檻（電子零件/設備業 15% 屬正常）
                    # 但要求毛利率趨勢向上（護城河在建立中）
                    gm_trend = ff["gross_margin_trend"]
                    if gross_margin < 15:
                        continue  # 毛利率太低，排除
                    if gross_margin < 20 and gm_trend < 0:
                        continue  # 毛利率偏低且還在下滑，排除
            else:
                # 沒有 token：只做技術面篩選
                pass

            # ── 籌碼面：法人近期買超 ──────────────
            foreign_buy = 0
            trust_buy   = 0
            inst_days   = 0

            if token:
                inst_map = fetch_institutional_finmind(code, start_dt, end_dt)
                if inst_map:
                    sorted_dates = sorted(inst_map.keys())[-20:]
                    for d in sorted_dates:
                        foreign_buy += inst_map[d].get("foreign_net", 0)
                        trust_buy   += inst_map[d].get("trust_net", 0)
                    # 法人近 20 日需為淨買超
                    inst_days = sum(1 for d in sorted_dates
                                    if inst_map[d].get("foreign_net",0) > 0
                                    or inst_map[d].get("trust_net",0) > 0)
                    if foreign_buy + trust_buy < 0:
                        continue

            # ── 計算綜合評分 ──────────────────────
            score = 0

            # ── 基本面分（滿分 60）────────────────
            # 月營收 YoY（最重要，最高 25 分）
            score += min(rev_yoy / 2, 25)

            # EPS YoY 加速（轉型訊號，最高 15 分）
            if eps_yoy >= 100:   score += 15   # 獲利爆發
            elif eps_yoy >= 50:  score += 10   # 獲利大幅成長
            elif eps_yoy >= 30:  score += 7    # 獲利明顯成長
            elif eps_yoy >= 0:   score += 3    # 小幅改善
            else:                score += 0    # 獲利衰退

            # 毛利率品質（最高 10 分）
            if gross_margin >= 40:   score += 10
            elif gross_margin >= 30: score += 7
            elif gross_margin >= 20: score += 5
            else:                    score += max(0, gross_margin - 15)  # 15~20% 得 0~5 分

            # 毛利率趨勢加分（護城河正在建立）
            if gm_trend > 0:  score += 5   # 毛利率持續提升
            elif gm_trend < 0: score -= 3  # 毛利率下滑扣分

            # EPS 轉型加分（一詮型態：EPS 極低但快速成長）
            if 0 < eps <= 1 and eps_yoy >= 100:
                score += 10  # ★ 轉型爆發型加分（最像飆股起漲前）
            elif eps > 5 and eps_yoy >= 20:
                score += 5   # 成熟獲利型加分

            # ── 籌碼分（滿分 30）─────────────────
            score += min(inst_days * 1.5, 15)       # 法人買超天數
            score += min((foreign_buy + trust_buy) / 1000, 15)  # 累計買超張數

            # ── 位置分（滿分 20）：越低分越高 ─────
            score += (1 - price_pos_120) * 20       # 越低基期得分越高

            # ── 修正 3：類股動能加分（最高 20 分）──
            # 所屬產業近期強勢 → 資金已開始輪動進來
            sector_data = get_sector_feature(code, {})
            sector_name = sector_data["sector_name"]
            sec_rank    = sector_data["sector_rank_pct"]   # 1=最強 0=最弱
            sec_avg_chg = sector_data["sector_avg_chg"]
            sec_is_hot  = sector_data["sector_is_hot"]

            if sec_is_hot:
                score += 20   # 當日熱門類股加 20 分（資金在輪動）
            elif sec_rank >= 0.7:
                score += 10   # 類股偏強加 10 分
            elif sec_rank <= 0.3:
                score -= 5    # 類股偏弱扣 5 分

            # ── 量能加分（有人在默默吃貨）────────
            avg20v = sma_last(vols[:-1], 20) if len(vols) > 20 else vols[-1]
            vol_ratio = vols[-1] / avg20v if avg20v > 0 else 1
            if vol_ratio >= 3:   score += 8   # 爆量，主力積極
            elif vol_ratio >= 2: score += 5   # 量明顯放大
            elif vol_ratio >= 1.5: score += 2 # 量溫和放大

            # ── 轉型股特別加分 ───────────────────
            # 月營收連續加速（rev_yoy_trend = +1 代表3個月持續加速）
            if token and rev_list:
                rf_full = get_revenue_features(rev_list, end_dt)
                if rf_full["yoy_trend"] == 1 and rev_yoy >= 15:
                    score += 10  # ★ 營收加速成長，飆股最強特徵

            # ── 組合結果 ──────────────────────────
            # 判斷股票類型（用於前端顯示）
            stock_type = "穩定獲利型"
            if 0 < eps <= 1 and eps_yoy >= 100:
                stock_type = "🔥 轉型爆發型"
            elif rev_yoy >= 50 and eps_yoy >= 50:
                stock_type = "⚡ 高速成長型"
            elif sec_is_hot:
                stock_type = "🏆 題材輪動型"
            elif eps > 5:
                stock_type = "💎 成熟獲利型"

            results.append({
                "code":         code,
                "name":         name,
                "price":        cur,
                "chg_pct":      s.get("pct", 0),
                "score":        round(score, 1),
                "stock_type":   stock_type,
                "price_pos":    round(price_pos_120 * 100, 1),
                "dist_hi60":    round(dist_from_hi60, 1),
                "rev_yoy":      round(rev_yoy, 1),
                "eps":          round(eps, 2),
                "eps_yoy":      round(eps_yoy, 1),
                "gross_margin": round(gross_margin, 1),
                "gm_trend":     gm_trend,
                "foreign_buy":  round(foreign_buy / 1000, 1),
                "trust_buy":    round(trust_buy / 1000, 1),
                "inst_days":    inst_days,
                "vol_ratio":    round(vol_ratio, 2) if 'vol_ratio' in dir() else 1.0,
                "sector":       sector_name,
                "sec_is_hot":   sec_is_hot,
                "sec_avg_chg":  round(sec_avg_chg, 2),
                "ma20_vs_ma60": round(ma20_vs_ma60, 1),
            })

        except Exception as e:
            print(f"  [{code}] 篩選錯誤: {e}")
            continue

    # 依綜合評分排序
    results.sort(key=lambda x: x["score"], reverse=True)
    top = results[:top_n]

    cb("完成！", 100)
    print(f"  ✅ 低基期價值成長股：找到 {len(results)} 支，推薦前 {len(top)} 支")
    return top


def _run_analyze_task(task_id, max_stocks, top_n, model_ver='v2'):
    try:
        prog = _analyze_tasks[task_id]
        def cb(msg, pct):
            prog["msg"]=msg; prog["pct"]=round(pct,1)
            print(f"  [AI分析 {pct:.0f}%] {msg}")

        is_v2 = (model_ver == 'v2')
        is_v3 = (model_ver == 'v3')   # v3 = 低基期價值成長股
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

        # ══════════════════════════════════════════════
        # 兩階段篩選
        # 第一階段：用當日技術指標快速過濾（約 1 分鐘）
        # 第二階段：對候選股做深度 AI 分析
        # ══════════════════════════════════════════════

        # 計算成交量門檻：全市場前 95 百分位（排除後 5%）
        all_vols = sorted([s["vol"] for s in stocks])
        n_stocks = len(all_vols)
        # 取第 5 百分位數作為下限（排除最低 5% 冷門股）
        vol_p5_idx = max(0, int(n_stocks * 0.05))
        vol_p5     = all_vols[vol_p5_idx] if all_vols else 500
        # 確保門檻至少 500 張（避免市場清淡時門檻過低）
        vol_min = max(vol_p5, 500)
        print(f"  成交量第5百分位：{vol_p5} 張，篩選門檻：{vol_min} 張")

        def _stage1_filter(s):
            """
            第一階段快速過濾（只用當日資料，不需歷史K線）：
            1. 成交量 >= 全市場第5百分位（前95%的有效股票）
            2. 漲跌幅 > -5%（排除崩跌股）
            3. 股價 >= 15 元（排除雞蛋水餃股）
            4. 漲幅 < 9.5%（排除已漲停）
            """
            if s["vol"] < vol_min:           return False  # 量太小
            if s.get("pct", 0) < -5:         return False  # 跌太多
            if s.get("pct", 0) >= 9.5:       return False  # 已漲停，追不上
            if s.get("price", 0) < 15:       return False  # 價格太低
            return True

        stocks_stage1 = [s for s in stocks if _stage1_filter(s)]
        cb(f"第一階段篩選：{total_before} → {len(stocks_stage1)} 支候選股", 3.5)
        print(f"  第一階段：{total_before} → {len(stocks_stage1)} 支")

        # 若候選股太少，放寬成交量門檻
        if len(stocks_stage1) < 100:
            vol_min = max(vol_p5 // 2, 200)
            stocks_stage1 = [s for s in stocks if s["vol"] >= vol_min
                             and s.get("pct", 0) > -5
                             and s.get("price", 0) >= 15]
            print(f"  放寬後：{len(stocks_stage1)} 支")

        stocks = stocks_stage1

        # 若還有設定 max_stocks 上限（手動觸發時），在候選股中再抽樣
        if max_stocks > 0 and len(stocks) > max_stocks:
            random.shuffle(stocks)
            stocks = stocks[:max_stocks]
            cb(f"候選股抽樣 {max_stocks} 支進行深度分析...", 4)
        else:
            cb(f"候選股 {len(stocks)} 支全部進行深度分析...", 4)

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

        # ── 產業輪動計算（分析前先算好，不用每支股票重算）──
        sector_rotation = {}
        if is_v2:
            cb("計算產業輪動強弱...", 4.5)
            try:
                _load_sector_map()
                sector_rotation = compute_sector_rotation(stocks)
                hot_sectors = [s for s, d in sector_rotation.items() if d.get("is_hot")]
                print(f"  熱門類股：{', '.join(hot_sectors[:5])}")
            except Exception as e:
                print(f"  產業輪動計算失敗: {e}")

        global _latest_analysis_result

        # ── v3.0 低基期價值成長股模式（直接篩選，不用 ML）──
        if is_v3:
            cb(f"🔍 低基期價值成長股篩選 {len(stocks)} 支...", 5)
            top = _screen_value_growth(stocks, top_n, task_id)
            analysis_out = {
                "stocks":         top,
                "total_analyzed": len(top),
                "total_scanned":  len(stocks),
                "model_ver":      "v3",
                "mode":           "value_growth",
                "time":           datetime.now().strftime("%Y/%m/%d %H:%M"),
                "avg_accuracy":   0,
                "bullish":        len(top),
                "bearish":        0,
                "predict_days":   0,
            }
            prog.update({"pct":100,"msg":"完成！","done":True,"error":None,"result":analysis_out})
            _latest_analysis_result = analysis_out
            threading.Thread(target=supabase_save_analysis, args=(analysis_out,), daemon=True).start()
            return

        cb(f"開始{'v2.0 強化' if is_v2 else 'v1.0 基礎'}分析 {len(stocks)} 支股票...", 5)
        results=[]
        total=len(stocks)
        for idx, s in enumerate(stocks):
            pct_done = 5 + idx/total*88
            cb(f"{'[v2]' if is_v2 else '[v1]'} {s['code']} {s['name']} ({idx+1}/{total})", pct_done)
            try:
                if is_v2:
                    r = _analyze_one_v2(s["code"], s["name"], market_closes, history_months,
                                        sector_rotation=sector_rotation)
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
        _latest_analysis_result = analysis_out
        print(f"  ✅ 分析結果已快取（{len(top)} 支推薦）")
        # ★ 永久儲存到 Supabase（重啟後也能讀回）
        threading.Thread(
            target=supabase_save_analysis,
            args=(analysis_out,), daemon=True
        ).start()
    except Exception as e:
        import traceback; traceback.print_exc()
        _analyze_tasks[task_id].update({"done":True,"error":str(e)})

def _analyze_one_v2(code, name, market_closes, history_months=24, sector_rotation=None):
    """v2.0 強化版：42個特徵 + 產業輪動 + 大盤相對強弱 + FinMind籌碼 + 基本面"""
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
    revenue_list = []
    financial_list = []
    try:
        token = _get_finmind_token()
        if token:
            inst_map       = fetch_institutional_finmind(code, start_dt, end_dt)
            revenue_list   = fetch_monthly_revenue_finmind(code, start_dt)
            financial_list = fetch_financial_finmind(code, start_dt)
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

        # ★ 突破前高特徵（飆股起漲點訊號）
        # breakout_60: 今天是否站上 60 日新高（飆股典型起點）
        # hi120:       是否站上 120 日新高（更強的多頭訊號）
        # dist_from_high: 距離 60 日高點的百分比（0=剛創新高，負數=已回檔）
        hi120 = max(hi[-120:]) if len(hi)>=120 else max(hi)
        hi60_prev = max(hi[-61:-1]) if len(hi)>=61 else hi60  # 昨天之前的60日高
        breakout_60  = 1 if c > hi60_prev else 0
        breakout_120 = 1 if c > hi120 * 0.995 else 0  # 接近120日高
        dist_from_high = (c - hi60) / hi60 * 100 if hi60 > 0 else 0

        # 爆量突破：突破同時伴隨 2 倍量（3167 2月初的典型特徵）
        vol_breakout = 1 if (breakout_60 == 1 and vo[-1] > _sma(vo[:-1], 20) * 2) else 0

        # 大盤相對強弱
        rel=0.0
        if mkt_closes and len(mkt_closes)>=11:
            mret10=(mkt_closes[-1]/mkt_closes[-11]-1)*100 if mkt_closes[-11]>0 else 0
            rel=ret10-mret10

        # FinMind 籌碼特徵
        actual_idx = idx_offset + i
        avg_vol = _sma(vo, 20) or 1

        # 近 5 日平均買超（標準化）
        f_net5 = sum(foreign_nets[max(0,actual_idx-4):actual_idx+1]) / avg_vol / 5 \
                 if foreign_nets and actual_idx < len(foreign_nets) else 0
        t_net5 = sum(trust_nets[max(0,actual_idx-4):actual_idx+1]) / avg_vol / 5 \
                 if trust_nets and actual_idx < len(trust_nets) else 0

        # ★ 法人連續買超強度（飆股關鍵：法人連續吃貨）
        # 近 20 日累計買超（標準化）
        f_net20 = sum(foreign_nets[max(0,actual_idx-19):actual_idx+1]) / avg_vol / 20 \
                  if foreign_nets and actual_idx < len(foreign_nets) else 0
        t_net20 = sum(trust_nets[max(0,actual_idx-19):actual_idx+1]) / avg_vol / 20 \
                  if trust_nets and actual_idx < len(trust_nets) else 0

        # 近 10 日法人連續買超「天數比例」（多少天是淨買超）
        f_buy_days = 0
        t_buy_days = 0
        if foreign_nets and actual_idx >= 9:
            for j in range(max(0, actual_idx-9), actual_idx+1):
                if j < len(foreign_nets) and foreign_nets[j] > 0: f_buy_days += 1
                if j < len(trust_nets)   and trust_nets[j]   > 0: t_buy_days += 1
        f_buy_ratio = f_buy_days / 10.0   # 0~1：外資近10日有幾天買超
        t_buy_ratio = t_buy_days / 10.0   # 0~1：投信近10日有幾天買超

        # 法人加權得分：外資+投信同步買超時訊號最強
        inst_score = f_net20 + t_net20 * 1.5   # 投信權重較高（投信較少作假）

        # ★ 基本面特徵：月營收年增率（抓飆股關鍵）
        ref_date = recs[i]["date"]
        rev_feat = get_revenue_features(revenue_list, ref_date)
        # ★ 財報特徵：EPS、毛利率
        fin_feat = get_financial_features(financial_list, ref_date)
        # ★ 產業輪動特徵
        sec_feat = get_sector_feature(code, sector_rotation or {})

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
            f_net5, t_net5,          # FinMind 籌碼：近 5 日
            # ★ 法人連續買超（共 5 個）—— 飆股關鍵
            f_net20, t_net20,        # 近 20 日平均買超
            f_buy_ratio, t_buy_ratio,# 近 10 日連續買超天數比例
            inst_score,              # 法人綜合分數
            # ★ 基本面：月營收 YoY、MoM、3 個月平均 YoY、YoY 趨勢
            rev_feat["latest_yoy"],
            rev_feat["latest_mom"],
            rev_feat["avg_yoy_3m"],
            rev_feat["yoy_trend"],
            # ★ 財報：EPS、EPS年增率、毛利率、毛利率趨勢
            fin_feat["latest_eps"],
            fin_feat["eps_yoy"],
            fin_feat["gross_margin"],
            fin_feat["gross_margin_trend"],
            # ★ 突破前高：60日/120日新高、距高點%、爆量突破
            breakout_60,
            breakout_120,
            dist_from_high,
            vol_breakout,
            # ★ 產業輪動：類股強弱排名、平均漲幅、上漲股比例（共 45 個特徵）
            sec_feat["sector_rank_pct"],
            sec_feat["sector_avg_chg"],
            sec_feat["sector_up_ratio"],
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

    # ★ 7. 法人連續買超（飆股關鍵訊號）
    if cf and len(cf) >= 29:
        f_net20_v    = cf[25]  # 外資近20日
        t_net20_v    = cf[26]  # 投信近20日
        f_buy_ratio_v= cf[27]  # 外資連續買超比例
        t_buy_ratio_v= cf[28]  # 投信連續買超比例

        # 外資 + 投信同步連續買超（最強訊號）
        if f_buy_ratio_v >= 0.7 and t_buy_ratio_v >= 0.7:
            reasons_bull.append(f"💎 外資+投信連續買超（外資{int(f_buy_ratio_v*10)}/10日、投信{int(t_buy_ratio_v*10)}/10日）")
        elif t_buy_ratio_v >= 0.7:
            reasons_bull.append(f"投信連續買超 {int(t_buy_ratio_v*10)}/10 日（投信認養訊號）")
        elif f_buy_ratio_v >= 0.7:
            reasons_bull.append(f"外資連續買超 {int(f_buy_ratio_v*10)}/10 日")
        elif f_buy_ratio_v <= 0.2 and t_buy_ratio_v <= 0.2:
            reasons_bear.append("法人連續賣超（籌碼鬆動）")

    # ★ 8. 基本面（月營收 YoY）—— 飆股關鍵指標
    if revenue_list:
        rev_feat = get_revenue_features(revenue_list, records[-1]["date"])
        latest_yoy = rev_feat["latest_yoy"]
        avg_yoy_3m = rev_feat["avg_yoy_3m"]
        trend      = rev_feat["yoy_trend"]
        if latest_yoy >= 50:
            reasons_bull.append(f"🔥 月營收年增 +{latest_yoy:.0f}%（飆股基本面訊號）")
        elif latest_yoy >= 20:
            reasons_bull.append(f"月營收年增 +{latest_yoy:.0f}%（成長動能強）")
        elif latest_yoy <= -20:
            reasons_bear.append(f"月營收年減 {latest_yoy:.0f}%（基本面轉弱）")
        if avg_yoy_3m >= 30 and trend == 1:
            reasons_bull.append(f"近3月營收 YoY 持續加速（均 +{avg_yoy_3m:.0f}%），趨勢向上")
        elif trend == -1 and avg_yoy_3m < 0:
            reasons_bear.append(f"近3月營收 YoY 持續減速，基本面惡化")

    # ★ 8b. 財報（EPS、毛利率）—— 獲利品質
    if financial_list:
        fin_feat = get_financial_features(financial_list, records[-1]["date"])
        eps        = fin_feat["latest_eps"]
        eps_yoy    = fin_feat["eps_yoy"]
        gm         = fin_feat["gross_margin"]
        gm_trend   = fin_feat["gross_margin_trend"]

        if eps_yoy >= 100:
            reasons_bull.append(f"💰 EPS 年增 +{eps_yoy:.0f}%（獲利爆發）")
        elif eps_yoy >= 30:
            reasons_bull.append(f"EPS 年增 +{eps_yoy:.0f}%（獲利成長強）")
        elif eps_yoy <= -30 and eps < 0:
            reasons_bear.append(f"EPS 年減 {eps_yoy:.0f}% 且本季轉虧（{eps}元）")
        elif eps_yoy <= -30:
            reasons_bear.append(f"EPS 年減 {eps_yoy:.0f}%（獲利衰退）")

        if gm >= 40 and gm_trend == 1:
            reasons_bull.append(f"毛利率 {gm:.0f}% 且持續提升（競爭優勢強）")
        elif gm_trend == -1 and gm < 20:
            reasons_bear.append(f"毛利率 {gm:.0f}% 且持續下滑（競爭力減弱）")

    # ★ 9. 突破前高訊號 —— 飆股起漲點
    if cf and len(cf) >= 43:
        brk60     = cf[39]   # breakout_60
        brk120    = cf[40]   # breakout_120
        dist_high = cf[41]   # dist_from_high
        vol_brk   = cf[42]   # vol_breakout
        if vol_brk == 1:
            reasons_bull.append("🚀 爆量突破 60 日新高（典型飆股起漲訊號）")
        elif brk120 == 1:
            reasons_bull.append("站上 120 日新高（長線多頭確認）")
        elif brk60 == 1:
            reasons_bull.append("突破 60 日新高（中線動能轉強）")
        elif dist_high < -15:
            reasons_bear.append(f"距 60 日高點已回檔 {abs(dist_high):.0f}%（動能趨弱）")

    # ★ 10. 產業輪動
    sec_feat_now = get_sector_feature(code, sector_rotation or {})
    sector_name  = sec_feat_now["sector_name"]
    rank_pct     = sec_feat_now["sector_rank_pct"]
    avg_chg      = sec_feat_now["sector_avg_chg"]
    is_hot       = sec_feat_now["sector_is_hot"]
    if is_hot and avg_chg > 0:
        reasons_bull.append(f"🏆 所屬類股「{sector_name}」為當日強勢產業（平均漲 +{avg_chg:.1f}%）")
    elif rank_pct < 0.2 and avg_chg < -0.5:
        reasons_bear.append(f"所屬類股「{sector_name}」當日偏弱（平均漲跌 {avg_chg:.1f}%）")

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

@app.route("/api/stock/analysis/<code>")
def stock_analysis(code):
    """
    個股技術分析 + 建議買賣價
    回傳：技術指標、支撐壓力、建議進場價、目標價、停損價、回測勝率
    """
    try:
        end_dt   = datetime.today().strftime("%Y-%m-%d")
        start_dt = (datetime.today() - timedelta(days=365)).strftime("%Y-%m-%d")
        records  = fetch_history_range(code, start_dt, end_dt)

        if len(records) < 30:
            return jsonify({"error": f"查無 {code} 足夠的歷史資料"}), 404

        closes = [r["close"] for r in records]
        highs  = [r["high"]  for r in records]
        lows   = [r["low"]   for r in records]
        vols   = [r["vol"]   for r in records]
        dates  = [r["date"]  for r in records]

        cur = closes[-1]

        # ── 均線 ────────────────────────────────────
        def sma(arr, n):
            sl = arr[-n:] if len(arr) >= n else arr
            return round(sum(sl)/len(sl), 2)

        ma5   = sma(closes, 5)
        ma10  = sma(closes, 10)
        ma20  = sma(closes, 20)
        ma60  = sma(closes, 60)
        ma120 = sma(closes, 120)
        ma240 = sma(closes, 240)

        # ── KD ──────────────────────────────────────
        k_series, d_series = calc_kd_series(closes, highs, lows, 9)
        k_val = k_series[-1];  d_val = d_series[-1]
        pk    = k_series[-2];  pd    = d_series[-2]

        # ── RSI ─────────────────────────────────────
        rsi_s = calc_rsi_series(closes, 14)
        rsi   = rsi_s[-1]
        rsi6  = calc_rsi_series(closes, 6)[-1]

        # ── MACD ────────────────────────────────────
        def ema(arr, n):
            e = arr[0]; k = 2/(n+1)
            for v in arr[1:]: e = e*(1-k) + v*k
            return round(e, 2)
        ema12 = ema(closes, 12); ema26 = ema(closes, 26)
        macd  = round(ema12 - ema26, 2)
        sig   = round(ema([(ema(closes[:i+1],12) - ema(closes[:i+1],26))
                           for i in range(max(0,len(closes)-9), len(closes))], 9), 2)
        hist_macd = round(macd - sig, 2)

        # ── ATR（波動度）─────────────────────────────
        atr = calc_atr(highs, lows, closes, 14)
        atr_val = round(atr[-1] if isinstance(atr, list) else atr, 2)

        # ── 布林通道 ─────────────────────────────────
        boll_mid = ma20
        std20    = (sum((c - boll_mid)**2 for c in closes[-20:])/20)**0.5
        boll_up  = round(boll_mid + 2*std20, 2)
        boll_dn  = round(boll_mid - 2*std20, 2)
        boll_pos = round((cur - boll_dn)/(boll_up - boll_dn + 0.001)*100, 1)

        # ── 支撐壓力（近60日最高/低 + 均線）────────
        hi60 = round(max(highs[-60:]), 2)
        lo60 = round(min(lows[-60:]),  2)
        hi20 = round(max(highs[-20:]), 2)
        lo20 = round(min(lows[-20:]),  2)

        # 支撐：近期低點 + 下方均線中最近的
        supports = sorted([lo20, lo60, boll_dn, ma20, ma60], reverse=False)
        support1 = next((s for s in supports if s < cur * 0.99), lo60)
        support2 = next((s for s in supports if s < support1 * 0.99), lo60 * 0.95)

        # 壓力：近期高點 + 上方均線
        resistances = sorted([hi20, hi60, boll_up, ma5], reverse=True)
        resist1 = next((r for r in resistances if r > cur * 1.01), hi60)
        resist2 = next((r for r in resistances if r > resist1 * 1.01), hi60 * 1.1)

        # ── 建議買賣價（核心功能）───────────────────
        # 進場策略：依目前技術面狀況給出建議
        trend = "多頭" if cur > ma20 > ma60 else "空頭" if cur < ma20 < ma60 else "盤整"
        kd_signal = "黃金交叉" if k_val > d_val and pk < pd else                     "死亡交叉" if k_val < d_val and pk > pd else                     "多頭排列" if k_val > d_val else "空頭排列"

        # 建議進場價：支撐附近（＝現價或稍微跌一點進場）
        if trend == "多頭" and k_val < 50:
            # 多頭回檔進場：在 ma20 附近或現價
            buy_suggest   = round(min(cur, ma20 * 1.01), 2)
            buy_reason    = f"多頭趨勢回測 MA20（{ma20}），KD 尚未過熱，逢回可買"
        elif k_val < 30 and rsi < 35:
            # 超賣反彈進場
            buy_suggest   = round(cur * 1.005, 2)  # 略高於現價確認反彈
            buy_reason    = f"KD={k_val:.0f} RSI={rsi:.0f} 雙超賣，反彈機率高，突破 {round(cur*1.005,2)} 可進場"
        elif cur > ma60 and hist_macd > 0:
            # 突破趨勢進場
            buy_suggest   = round(cur * 1.01, 2)
            buy_reason    = f"站上 MA60 且 MACD 翻正，追漲突破 {round(cur*1.01,2)} 可進場"
        else:
            buy_suggest   = round(support1 * 1.01, 2)
            buy_reason    = f"等待回測支撐 {support1}，反彈確認後進場"

        # 目標價：ATR 法則 + 壓力位
        target1 = round(buy_suggest * (1 + max(atr_val/cur*100, 5)/100), 2)   # 最低目標 5%
        target2 = round(min(resist1, buy_suggest * 1.12), 2)                   # 中目標
        target3 = round(min(resist2, buy_suggest * 1.20), 2)                   # 高目標

        # 停損價：ATR 法則（買入價 - 1.5×ATR）
        stop_loss_price = round(buy_suggest - atr_val * 1.5, 2)
        stop_loss_pct   = round((buy_suggest - stop_loss_price) / buy_suggest * 100, 1)

        # 風報比
        rr1 = round((target1 - buy_suggest) / (buy_suggest - stop_loss_price), 2) if buy_suggest > stop_loss_price else 0
        rr2 = round((target2 - buy_suggest) / (buy_suggest - stop_loss_price), 2) if buy_suggest > stop_loss_price else 0

        # ── 回測勝率（近半年出現類似條件的勝率）──
        win_count = 0; total_count = 0
        for i in range(30, len(closes) - 10):
            ki = k_series[i]; di = d_series[i]
            # 類似條件：KD 低檔或均線支撐
            if ki < 40 or (closes[i] > ma20 and closes[i] < ma20 * 1.02):
                total_count += 1
                future_max = max(closes[i:i+10])
                if future_max >= closes[i] * 1.05:  # 10天內漲 5%
                    win_count += 1
        hist_win_rate = round(win_count / total_count * 100, 1) if total_count > 0 else 50.0

        # ── 近30日 K線資料（前端繪圖用）─────────────
        recent_n = min(60, len(records))
        candles  = [{
            "date":  records[-recent_n+i]["date"],
            "open":  records[-recent_n+i].get("open", closes[-recent_n+i]),
            "high":  highs[-recent_n+i],
            "low":   lows[-recent_n+i],
            "close": closes[-recent_n+i],
            "vol":   vols[-recent_n+i],
            "ma5":   round(sum(closes[max(0,-recent_n+i-4):-recent_n+i+1])/min(5,i+1),2) if i>=0 else closes[-recent_n+i],
            "ma20":  round(sum(closes[max(0,-recent_n+i-19):-recent_n+i+1])/min(20,i+1),2) if i>=0 else closes[-recent_n+i],
        } for i in range(recent_n)]

        return jsonify({
            "code":   code,
            "price":  cur,
            "date":   dates[-1],
            # 技術指標
            "indicators": {
                "ma5":  ma5,  "ma10": ma10, "ma20": ma20,
                "ma60": ma60, "ma120":ma120,"ma240":ma240,
                "k": round(k_val,1), "d": round(d_val,1),
                "rsi14": round(rsi,1), "rsi6": round(rsi6,1),
                "macd":  macd, "macd_signal": sig, "macd_hist": hist_macd,
                "atr":   atr_val,
                "boll_up":  boll_up, "boll_mid": boll_mid, "boll_dn": boll_dn,
                "boll_pos": boll_pos,
                "trend":      trend,
                "kd_signal":  kd_signal,
                "hi60": hi60, "lo60": lo60,
                "hi20": hi20, "lo20": lo20,
            },
            # 支撐壓力
            "levels": {
                "support1":  round(support1,2), "support2":  round(support2,2),
                "resist1":   round(resist1,2),  "resist2":   round(resist2,2),
            },
            # 建議買賣價
            "advice": {
                "buy_suggest":      buy_suggest,
                "buy_reason":       buy_reason,
                "stop_loss_price":  stop_loss_price,
                "stop_loss_pct":    stop_loss_pct,
                "target1":          target1,
                "target2":          target2,
                "target3":          target3,
                "rr1":              rr1,
                "rr2":              rr2,
                "hist_win_rate":    hist_win_rate,
            },
            "candles": candles,
        })
    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.route("/api/sector/rotation")
def sector_rotation_api():
    """取得當日產業輪動熱度排行"""
    try:
        # 用 TWSE 當日資料計算
        resp = SESSION.get(
            "https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL",
            timeout=15)
        resp.raise_for_status()
        stocks_today = []
        for r in resp.json():
            code  = r.get("Code","")
            price = safe_float(r.get("ClosingPrice"))
            chg   = safe_float(r.get("Change"))
            if not (str(code).isdigit() and len(code)==4 and price>0): continue
            prev  = price - chg
            pct   = round(chg/prev*100,2) if prev>0 else 0
            stocks_today.append({"code": code, "chg_pct": pct})

        _load_sector_map()
        rotation = compute_sector_rotation(stocks_today)

        # 整理成排行榜格式
        ranking = sorted(
            [{"sector": s, **d} for s, d in rotation.items()],
            key=lambda x: x["avg_chg"] * 0.6 + x["up_ratio"] * 5 * 0.4,
            reverse=True
        )
        return jsonify({
            "ranking": ranking[:20],
            "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "total_sectors": len(rotation),
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/analyze/latest")
def get_latest_analysis():
    """
    讀取最新分析結果，優先順序：
    1. 記憶體快取（最快）
    2. Supabase 資料庫（重啟後仍有資料）
    3. 本地 JSON 備用（本機開發）
    """
    # 1. 記憶體快取
    global _latest_analysis_result
    if _latest_analysis_result:
        return jsonify({**_latest_analysis_result, "source": "memory"})

    # 2. Supabase 資料庫
    sb_data = supabase_load_latest()
    if sb_data:
        _latest_analysis_result = sb_data  # 同時回補記憶體
        return jsonify({**sb_data, "source": "database"})

    # 3. 本地 JSON（本機開發備用）
    result_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "analysis_result.json")
    try:
        if os.path.exists(result_file):
            with open(result_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            return jsonify({**data, "source": "file"})
    except Exception:
        pass

    return jsonify({"error": "尚無分析結果，請點「開始 AI 分析」或等待 22:00 自動執行"}), 404

@app.route("/api/analyze/history")
def get_analysis_history():
    """讀取最近 30 天的分析紀錄"""
    history = supabase_load_history(limit=30)
    if not history:
        return jsonify({"error": "尚無歷史紀錄，或 Supabase 未設定"}), 404
    # 每筆只回傳摘要（不含完整 stocks 詳細資料，節省流量）
    summary = []
    for row in history:
        stocks = row.get("stocks", [])
        summary.append({
            "date":           row.get("date",""),
            "time":           row.get("time",""),
            "model_ver":      row.get("model_ver",""),
            "total_scanned":  row.get("total_scanned",0),
            "total_analyzed": row.get("total_analyzed",0),
            "avg_accuracy":   row.get("avg_accuracy",0),
            "bullish":        row.get("bullish",0),
            "bearish":        row.get("bearish",0),
            "top3":           [{"code":s.get("code"),"name":s.get("name"),
                                "rise_prob":s.get("rise_prob")} for s in stocks[:3]],
            "stock_count":    len(stocks),
        })
    return jsonify({"history": summary, "count": len(summary)})

@app.route("/api/analyze/history/<date>", methods=["GET", "DELETE"])
def get_analysis_by_date(date):
    """讀取或刪除指定日期的分析結果"""
    if request.method == "DELETE":
        # 刪除 Supabase 中該日期的紀錄
        if not SUPABASE_URL or not SUPABASE_KEY:
            return jsonify({"error": "未設定 Supabase"}), 400
        try:
            url = f"{SUPABASE_URL}/rest/v1/analysis_results"
            params = {"date": f"eq.{date}"}
            r = requests.delete(url, params=params, headers=_sb_headers(), timeout=10)
            if r.status_code in (200, 204):
                print(f"[Supabase] 已刪除 {date} 的分析紀錄")
                return jsonify({"ok": True, "deleted": date})
            else:
                return jsonify({"error": f"刪除失敗 {r.status_code}"}), 500
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    # GET：讀取指定日期
    row = supabase_load_latest(date=date)
    if not row:
        return jsonify({"error": f"找不到 {date} 的分析紀錄"}), 404
    return jsonify(row)

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

def _run_auto_analysis(max_stocks=0, top_n=10, model_ver='v2'):  # 0=全部上市櫃
    """
    在記憶體中執行全市場 AI 分析，結果存到 _latest_analysis_result。
    由每日排程（22:00）或手動 POST /api/analyze/run 觸發。
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


def _get_last_analysis_date():
    """從 Supabase 查最新一筆分析的日期"""
    try:
        rows = supabase_load_history(limit=1)
        if rows:
            return rows[0].get("date","")
    except: pass
    return ""

def _start_daily_schedule():
    """
    每天 22:00 台灣時間自動分析。
    啟動時也會檢查：若昨天或今天沒有資料，自動補跑。
    """
    import time as _time

    def _should_run_now(tw_dt, last_db_date):
        """判斷現在是否需要跑分析"""
        tw_date = tw_dt.strftime("%Y-%m-%d")
        tw_hour = tw_dt.hour
        # 今天 22:00 後且今天還沒跑
        if tw_hour >= 22 and last_db_date != tw_date:
            return True, f"例行排程 {tw_date} 22:00"
        # 啟動補跑：昨天的資料不存在（Render 重啟錯過了）
        yesterday = (tw_dt - timedelta(days=1)).strftime("%Y-%m-%d")
        if last_db_date < yesterday and tw_hour < 22:
            return True, f"補跑昨日遺漏分析（上次：{last_db_date}）"
        return False, ""

    def _scheduler():
        print("[排程] 每日自動分析排程已啟動（台灣時間 22:00）")
        _time.sleep(30)  # 等伺服器完全啟動

        # 啟動時先從 DB 查最後執行日期
        last_db_date = _get_last_analysis_date()
        print(f"[排程] 資料庫最後分析日期：{last_db_date or '無'}")

        while True:
            try:
                now_utc = datetime.utcnow()
                tw_dt   = now_utc + timedelta(hours=8)
                tw_date = tw_dt.strftime("%Y-%m-%d")

                should_run, reason = _should_run_now(tw_dt, last_db_date)
                if should_run:
                    print(f"[排程] ⏰ {reason}")
                    _run_auto_analysis(max_stocks=0, top_n=20, model_ver='v2')
                    last_db_date = tw_date  # 更新記錄
                    print(f"[排程] ✅ 分析完成，下次：明天 22:00")
                    _time.sleep(120)
                else:
                    _time.sleep(60)
            except Exception as e:
                print(f"[排程] ❌ 錯誤: {e}")
                _time.sleep(60)

    t = threading.Thread(target=_scheduler, daemon=True)
    t.start()
    print("[排程] 背景執行緒已啟動（含啟動補跑機制）")


import os as _os

def _start_keep_alive():
    """每 4 分鐘 ping 自己，防止 Render 免費版休眠（搭配 UptimeRobot 5 分鐘外部 ping）"""
    render_url = _os.environ.get("RENDER_EXTERNAL_URL", "")
    if not render_url:
        print("[Keep-Alive] 本機環境，略過")
        return
    def _ping():
        time.sleep(30)  # 啟動後等 30 秒再開始 ping
        while True:
            try:
                r = SESSION.get(f"{render_url}/api/health", timeout=10)
                print(f"[Keep-Alive] ✅ Ping OK ({r.status_code})  {datetime.now().strftime('%H:%M:%S')}")
            except Exception as e:
                print(f"[Keep-Alive] ⚠️ Ping 失敗: {e}")
            time.sleep(240)  # 4 分鐘
    t = threading.Thread(target=_ping, daemon=True)
    t.start()
    print(f"[Keep-Alive] 已啟動，每 4 分鐘 ping → {render_url}")

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

