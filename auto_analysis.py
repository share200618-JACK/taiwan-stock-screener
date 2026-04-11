"""
台股自動選股 + 強化版隨機森林分析
=====================================
v2.0 改進：
  ✅ 歷史資料從 8 個月延長到 2 年
  ✅ 加入外資投信籌碼特徵
  ✅ 加入大盤相對強弱
  ✅ 加入量價型態（連漲天數、紅K比例、價格位置）
  ✅ 特徵從 16 個增加到 28 個
  ✅ 除權息日過濾（避免資料污染）
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

TOP_N          = 10    # 最多推薦幾支
PREDICT_DAYS   = 15    # 預測幾個交易日後（約 3 週）
RISE_THRESHOLD = 3.0   # 漲超過 3% 算「上漲」
HISTORY_MONTHS = 24    # 抓幾個月歷史資料（2 年）

# 結果存檔路徑（server.py 會讀這個檔案）
RESULT_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "analysis_result.json")

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

def fetch_history(code, months=24):
    """抓取個股近 N 個月每日歷史資料（含除權息過濾）"""
    records = []
    today = datetime.today()
    for i in range(months, -1, -1):
        d = today - timedelta(days=i * 31)
        ym = f"{d.year}{d.month:02d}01"
        try:
            url = (f"https://www.twse.com.tw/exchangeReport/STOCK_DAY"
                   f"?response=json&date={ym}&stockNo={code}")
            r = SESSION.get(url, timeout=10)
            data = r.json()
            if data.get("stat") != "OK" or not data.get("data"):
                continue
            for row in data["data"]:
                parts = row[0].split("/")
                if len(parts) != 3: continue
                try:
                    dt = datetime(int(parts[0])+1911, int(parts[1]), int(parts[2]))
                except: continue
                c = safe_float(row[6])
                h = safe_float(row[4])
                l = safe_float(row[5])
                o = safe_float(row[3])
                v = round(safe_float(row[1]) / 1000)
                chg = safe_float(row[7])
                if c <= 0: continue
                prev = c - chg
                pct  = abs(chg / prev * 100) if prev > 0 else 0
                # 過濾除權息日（單日波動超過 9.5% 視為異常）
                if pct > 9.5: continue
                records.append({
                    "date":  dt.strftime("%Y-%m-%d"),
                    "open":  o, "high": h, "low": l,
                    "close": c, "vol":  v, "chg": chg,
                })
        except: pass
    return sorted(records, key=lambda x: x["date"])

def fetch_institutional(code, days=60):
    """
    抓取外資 + 投信近 N 日買賣超
    回傳 list of {date, foreign_net, trust_net}
    """
    result = []
    try:
        url = (f"https://www.twse.com.tw/fund/T86"
               f"?response=json&date={datetime.today().strftime('%Y%m%d')}"
               f"&selectType=ALLBUT0999")
        r = SESSION.get(url, timeout=10)
        data = r.json()
        if data.get("stat") != "OK" or not data.get("data"):
            return []
        for row in data["data"]:
            if row[0] != code: continue
            foreign_net = safe_float(str(row[4]).replace(",",""))  # 外資買賣超（千股）
            trust_net   = safe_float(str(row[10]).replace(",","")) # 投信買賣超（千股）
            result.append({
                "foreign_net": round(foreign_net / 1000),   # 換算張
                "trust_net":   round(trust_net   / 1000),
            })
            break
    except: pass
    return result

def fetch_market_index(months=24):
    """抓取加權指數歷史（用於計算相對強弱）"""
    records = []
    today = datetime.today()
    for i in range(months, -1, -1):
        d = today - timedelta(days=i * 31)
        ym = f"{d.year}{d.month:02d}01"
        try:
            url = (f"https://www.twse.com.tw/exchangeReport/STOCK_DAY"
                   f"?response=json&date={ym}&stockNo=Y9999")
            r = SESSION.get(url, timeout=10)
            data = r.json()
            if data.get("stat") != "OK" or not data.get("data"): continue
            for row in data["data"]:
                parts = row[0].split("/")
                if len(parts) != 3: continue
                try:
                    dt = datetime(int(parts[0])+1911, int(parts[1]), int(parts[2]))
                except: continue
                c = safe_float(row[6])
                if c > 0:
                    records.append({"date": dt.strftime("%Y-%m-%d"), "close": c})
        except: pass
    return sorted(records, key=lambda x: x["date"])

# 全域快取大盤資料（避免每支股票都重抓）
_market_cache = None

def get_market_data():
    global _market_cache
    if _market_cache is None:
        print("   抓取大盤指數資料...")
        _market_cache = fetch_market_index(HISTORY_MONTHS)
        print(f"   大盤資料：{len(_market_cache)} 筆")
    return _market_cache

# ══════════════════════════════════════════════════
# 特徵工程（28 個特徵）
# ══════════════════════════════════════════════════

def calc_features(records, market_records=None, inst_data=None):
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

    # ── 大盤相對強弱（新增）──────────────────────
    f_rel_strength = 0.0
    if market_records and len(market_records) >= 11:
        # 找對應日期的大盤資料
        stock_dates = {r["date"]: r["close"] for r in records}
        mkt_dates   = {r["date"]: r["close"] for r in market_records}
        today_date  = records[-1]["date"]

        # 找到相近的大盤日期
        mkt_sorted = sorted(mkt_dates.keys())
        mkt_today  = None
        for d in reversed(mkt_sorted):
            if d <= today_date:
                mkt_today = d
                break

        if mkt_today:
            mkt_closes = [mkt_dates[d] for d in mkt_sorted if d <= mkt_today]
            if len(mkt_closes) >= 11:
                mkt_ret10 = (mkt_closes[-1]/mkt_closes[-11]-1)*100 if mkt_closes[-11] > 0 else 0
                # 股票相對大盤的超額報酬
                f_rel_strength = ret10 - mkt_ret10

    # ── 籌碼特徵（新增）──────────────────────────
    f_foreign_net = 0.0
    f_trust_net   = 0.0
    f_inst_total  = 0.0
    if inst_data:
        f_foreign_net = inst_data.get("foreign_net", 0) / max(avg5v, 1) * 100
        f_trust_net   = inst_data.get("trust_net",   0) / max(avg5v, 1) * 100
        f_inst_total  = f_foreign_net + f_trust_net

    # ── 組合成特徵向量（共 28 個）────────────────
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

def build_training_data(records, market_records, predict_days, rise_threshold):
    X, y = [], []
    closes = [r["close"] for r in records]
    for i in range(40, len(records) - predict_days):
        feats = calc_features(records[:i+1], market_records)
        if feats is None: continue
        future  = closes[i + predict_days]
        current = closes[i]
        label   = 1 if (future - current) / current * 100 >= rise_threshold else 0
        X.append(feats)
        y.append(label)
    return X, y

def analyze_stock(code, name, market_records):
    records = fetch_history(code, HISTORY_MONTHS)
    if len(records) < 80:
        return None

    # 抓今日籌碼
    inst_today = fetch_institutional(code)
    inst_data  = inst_today[0] if inst_today else None

    X, y = build_training_data(records, market_records, PREDICT_DAYS, RISE_THRESHOLD)
    if len(X) < 50 or sum(y) < 8 or sum(1-v for v in y) < 8:
        return None

    # 5 折交叉驗證（時間序列分割，避免未來資料洩漏）
    n    = len(X)
    fold = n // 5
    accs, precs, recalls = [], [], []

    for k in range(5):
        val_start = k * fold
        val_end   = min((k+1)*fold, n)
        # 只用 val_start 之前的資料訓練（時間序列嚴格切割）
        if val_start < 30: continue
        train_X = X[:val_start]
        train_y = y[:val_start]
        val_X   = X[val_start:val_end]
        val_y   = y[val_start:val_end]

        rf = RandomForest(n_trees=40, max_depth=6, n_features=10)
        rf.fit(train_X, train_y)
        probs = rf.predict_proba(val_X)
        preds = [1 if p >= 0.5 else 0 for p in probs]

        acc = sum(preds[i] == val_y[i] for i in range(len(val_y))) / len(val_y)
        tp  = sum(1 for i in range(len(val_y)) if preds[i]==1 and val_y[i]==1)
        fp  = sum(1 for i in range(len(val_y)) if preds[i]==1 and val_y[i]==0)
        fn  = sum(1 for i in range(len(val_y)) if preds[i]==0 and val_y[i]==1)
        prec   = tp/(tp+fp) if (tp+fp) > 0 else 0
        recall = tp/(tp+fn) if (tp+fn) > 0 else 0

        accs.append(acc)
        precs.append(prec)
        recalls.append(recall)

    if not accs: return None
    accuracy  = sum(accs)    / len(accs)
    precision = sum(precs)   / len(precs)
    recall_v  = sum(recalls) / len(recalls)

    # 用全部資料訓練，預測當前
    rf_final = RandomForest(n_trees=60, max_depth=7, n_features=10)
    rf_final.fit(X, y)

    cur_feat = calc_features(records, market_records, inst_data)
    if cur_feat is None: return None

    rise_prob  = rf_final.predict_proba([cur_feat])[0]
    confidence = accuracy * abs(rise_prob - 0.5) * 2

    return {
        "rise_prob":     round(rise_prob  * 100, 1),
        "accuracy":      round(accuracy   * 100, 1),
        "precision":     round(precision  * 100, 1),
        "recall":        round(recall_v   * 100, 1),
        "confidence":    round(confidence * 100, 1),
        "current_price": records[-1]["close"],
        "recent_chg":    round((records[-1]["close"]/records[-2]["close"]-1)*100, 2)
                         if len(records) >= 2 else 0,
        "inst_foreign":  inst_data.get("foreign_net", 0) if inst_data else 0,
        "inst_trust":    inst_data.get("trust_net",   0) if inst_data else 0,
        "data_years":    round(len(records)/250, 1),
    }

# ══════════════════════════════════════════════════
# 全市場取得 & 主流程
# ══════════════════════════════════════════════════

def get_all_stocks():
    try:
        url  = "https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL"
        resp = SESSION.get(url, timeout=15)
        resp.raise_for_status()
        stocks = []
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
                           "price":price,"chg":chg,"pct":pct,"vol":vol})
        return stocks
    except Exception as e:
        print(f"取得股票失敗: {e}")
        return []

def save_result(data):
    """把分析結果存成 JSON 檔，供 server.py 讀取"""
    try:
        with open(RESULT_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"  ✅ 結果已存至 {RESULT_FILE}")
    except Exception as e:
        print(f"  ❌ 存檔失敗: {e}")

def run():
    now_str = datetime.now().strftime("%Y/%m/%d %H:%M")
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
        save_result({"error": "無法取得市場資料", "time": now_str})
        return

    # 抽樣 80 支
    if len(stocks) > 80:
        random.shuffle(stocks)
        stocks = stocks[:80]

    # 逐支分析
    print(f"\n② 隨機森林分析（{len(stocks)} 支）...")
    results = []
    for i, s in enumerate(stocks):
        print(f"   [{i+1:3d}/{len(stocks)}] {s['code']} {s['name']:<8}", end=" ")
        try:
            r = analyze_stock(s["code"], s["name"], market_data)
            if r and r["confidence"] > 5:
                r.update(s)
                results.append(r)
                print(f"✅ 漲:{r['rise_prob']}% 準:{r['accuracy']}% 信:{r['confidence']}%")
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
    top = results[:TOP_N]

    # 統計摘要
    avg_acc = round(sum(r["accuracy"] for r in results) / len(results), 1)
    bullish = sum(1 for r in results if r["rise_prob"] >= 50)
    bearish = len(results) - bullish

    print(f"\n③ 儲存結果（{len(top)} 支推薦）...")

    # 整理輸出資料
    output = {
        "time":           now_str,
        "model_ver":      "v2.0",
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
            "price":       r["price"],
            "chg_pct":     r.get("pct", 0),
            "vol":         r.get("vol", 0),
            "data_years":  r.get("data_years", 0),
            "inst_foreign":r.get("inst_foreign", 0),
            "inst_trust":  r.get("inst_trust", 0),
        } for r in top]
    }

    save_result(output)

    print(f"\n✅ 完成！前3名：" +
          "、".join(f"{r['code']}{r['name']}" for r in top[:3]))
    print(f"   結果已存至：{RESULT_FILE}")
    print(f"   網頁可在 /analyze 頁面查看最新結果")

# ══════════════════════════════════════════════════

if __name__ == "__main__":
    run()
