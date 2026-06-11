#!/usr/bin/env python3
"""
台股選股系統 - GitHub Actions 獨立執行腳本
每天 22:15 台灣時間自動執行：
  1. 三合一選股
  2. 遊牧民選股（結果自動加入追蹤清單）
"""

import os, sys, json, time, requests
from datetime import datetime, timedelta

# ── 環境變數 ──────────────────────────────────────────
SUPABASE_URL   = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY   = os.environ.get("SUPABASE_KEY", "")
FINMIND_TOKEN  = os.environ.get("FINMIND_TOKEN", "85b8cba2f1ec0d8e28a25a00db26e121")
JACK_TOKEN     = os.environ.get("JACK_TOKEN", "85b8cba2f1ec0d8e28a25a00db26e121")

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "Mozilla/5.0"})

def safe_float(v, default=0.0):
    try:
        return float(str(v).replace(",","").replace("+","").strip())
    except:
        return default

def sb_headers():
    return {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal",
    }

def log(msg):
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)

# ══════════════════════════════════════════════════════
# 三合一加權計分（取代原本 5 個條件各 +20 相加）
# 權重來自 research/validate_trinity.py 的 [C]（120 檔大型股、2021–2026 實證）。
# 只保留通過顯著門檻（|t|≥2 且 IC>0）的因子；trust/dist_hi20/turnover 經驗證無預測力已移除。
# 注意：此結論偏大型股+趨勢盤，建議每季用更廣股票池重驗。
# ══════════════════════════════════════════════════════
TRINITY_WEIGHTS = {
    "ma_trend":  0.40,   # 多頭排列 ma20/ma60-1（最強，t=+4.2、多空年化 +21.7%）
    "ma20_pct":  0.31,   # 離 MA20 幅度（越延伸未來越好，t=+3.4）
    "dist_hi60": 0.29,   # 距 60 日高（t=+3.0；分層較弱，可自行再降權集中於 ma_trend）
}

# ── 必要條件開關（實證顯示這兩個硬篩反而拖累報酬，但它們也是風控）──
# 資料：離MA20≤8% 旗標 t=-13.41、投信≥3天 t=-2.83，關掉(設 False)在此樣本報酬較好。
# 預設維持原行為(True)較保守、可逆；想測「關掉」的效果就改 False，建議先觀察/回測再上線。
GATE_MA20_PCT_CAP = True   # True=維持「離MA20需≤8%」硬篩；False=取消此上限（不再排除延伸股）
GATE_TRUST_MIN    = True   # True=維持「投信連買≥3天」硬篩；False=不再用投信當必要條件

def _winsorized_zscore(values, lo_q=0.02, hi_q=0.02):
    """對一批數值去極值 + 標準化（z-score）。回傳同長度分數，缺值給 0。"""
    xs = [v for v in values if isinstance(v, (int, float))]
    if len(xs) < 3:
        return [0.0 for _ in values]
    xs_sorted = sorted(xs)
    lo = xs_sorted[int(len(xs_sorted) * lo_q)]
    hi = xs_sorted[min(len(xs_sorted) - 1, int(len(xs_sorted) * (1 - hi_q)))]
    clipped = [min(max(v, lo), hi) if isinstance(v, (int, float)) else None for v in values]
    valid = [v for v in clipped if v is not None]
    mu = sum(valid) / len(valid)
    var = sum((v - mu) ** 2 for v in valid) / max(len(valid) - 1, 1)
    sd = var ** 0.5
    if sd == 0:
        return [0.0 for _ in values]
    return [((v - mu) / sd if v is not None else 0.0) for v in clipped]

def weighted_score_trinity(candidates, weights=TRINITY_WEIGHTS):
    """對通過硬篩的候選股做橫斷面加權計分（連續分數，可細排，無大量同分）。

    為相容既有前端，total_score 仍輸出 0–100（依當批 min-max 映射），
    另存 raw_score（真正用來排序的加權 z-score）。
    """
    if not candidates:
        return []
    z = {key: _winsorized_zscore([c.get(key) for c in candidates]) for key in weights}
    for i, c in enumerate(candidates):
        c["raw_score"] = round(sum(weights[key] * z[key][i] for key in weights), 4)
    raws = [c["raw_score"] for c in candidates]
    lo, hi = min(raws), max(raws)
    for c in candidates:
        c["total_score"] = round(100 * (c["raw_score"] - lo) / (hi - lo)) if hi > lo else 50
    candidates.sort(key=lambda x: x["raw_score"], reverse=True)
    return candidates

# ══════════════════════════════════════════════════════
# 遊牧民設定
# ══════════════════════════════════════════════════════
# 排序依據：改用 validate_nomad.py [B] 中 IC 最高的因子（預設 k_d=KD動能，比原本 vol_ratio
# 更可能有預測力）。可選："k_d"、"ma60_pct"（距MA60）、"k"、"vol_ratio"。若你選的因子是
# 「越小越好」，把 NOMAD_SORT_DESC 設 False。
NOMAD_SORT_KEY  = "k_d"
NOMAD_SORT_DESC = True
NOMAD_MAX_SIGNALS = 30      # 訊號數上限（避免某天爆量灌爆追蹤清單；比照三合一取前 N）
NOMAD_HOLD_DAYS   = 15      # 依 validate_nomad.py [C] 事件研究的最佳持有天數（交易日）填入

# ── 抓個股歷史資料 ────────────────────────────────────
def fetch_history(code, start, end):
    try:
        r = SESSION.get("https://api.finmindtrade.com/api/v4/data",
            params={"dataset":"TaiwanStockPrice","data_id":code,
                    "start_date":start,"end_date":end},
            headers={"Authorization":f"Bearer {FINMIND_TOKEN}"}, timeout=15)
        rows = r.json().get("data",[])
        return [{"date":r["date"],"open":safe_float(r.get("open",0)),
                 "high":safe_float(r.get("max",0)),"low":safe_float(r.get("min",0)),
                 "close":safe_float(r.get("close",0)),"vol":safe_float(r.get("Trading_Volume",0))/1000}
                for r in rows if r.get("close")]
    except Exception as e:
        return []

# ── 抓法人買賣資料 ────────────────────────────────────
def fetch_inst(code, start):
    try:
        r = SESSION.get("https://api.finmindtrade.com/api/v4/data",
            params={"dataset":"TaiwanStockInstitutionalInvestorsBuySell",
                    "data_id":code,"start_date":start},
            headers={"Authorization":f"Bearer {FINMIND_TOKEN}"}, timeout=10)
        return r.json().get("data",[])
    except:
        return []

def get_trust_consec(inst_rows):
    """計算投信連續買超天數"""
    trust_dates = sorted(set(r["date"] for r in inst_rows if r.get("name")=="投信"))[-7:]
    consec = 0
    for td in reversed(trust_dates):
        net = sum(safe_float(r.get("buy",0))-safe_float(r.get("sell",0))
                  for r in inst_rows if r["date"]==td and r.get("name")=="投信")
        if net > 0: consec += 1
        else: break
    return consec

# ── 載入產業分類 ──────────────────────────────────────
_sector_map = {}
def load_sector_map():
    global _sector_map
    if _sector_map: return
    try:
        r = SESSION.get("https://api.finmindtrade.com/api/v4/data",
            params={"dataset":"TaiwanStockInfo"},
            headers={"Authorization":f"Bearer {FINMIND_TOKEN}"}, timeout=20)
        for row in r.json().get("data",[]):
            code = str(row.get("stock_id",""))
            cat  = row.get("industry_category","")
            if code and cat:
                _sector_map[code] = cat
        log(f"產業地圖載入 {len(_sector_map)} 支")
    except Exception as e:
        log(f"產業地圖載入失敗: {e}")

# ── 存入 Supabase ─────────────────────────────────────
def save_result(table, data):
    if not SUPABASE_URL or not SUPABASE_KEY:
        log(f"⚠️ 未設定 Supabase 環境變數"); return
    try:
        url   = f"{SUPABASE_URL}/rest/v1/{table}"
        today = data["date"]
        requests.delete(url, params={"date":f"eq.{today}"}, headers=sb_headers(), timeout=10)
        payload = {
            "date":          today,
            "time":          data["time"],
            "total_scanned": data["total_scanned"],
            "total_passed":  data["total_passed"],
            "stocks":        json.dumps(data["stocks"], ensure_ascii=False),
        }
        r = requests.post(url, json=payload, headers=sb_headers(), timeout=10)
        if r.status_code in (200,201):
            log(f"✅ {table} 已存入 {len(data['stocks'])} 支")
        else:
            log(f"❌ {table} 存入失敗 {r.status_code}: {r.text[:100]}")
    except Exception as e:
        log(f"❌ {table} 例外: {e}")

def add_to_watchlist(code, name, price, sector, note, token):
    """加入追蹤清單（若已存在則跳過）"""
    if not SUPABASE_URL or not SUPABASE_KEY: return
    try:
        # 先查有沒有重複
        url = f"{SUPABASE_URL}/rest/v1/watchlist"
        r = requests.get(url,
            params={"user_token":f"eq.{token}","code":f"eq.{code}"},
            headers=sb_headers(), timeout=10)
        if r.json(): return  # 已存在
        payload = {
            "code": code, "name": name, "add_price": price,
            "add_time": datetime.now().strftime("%Y/%m/%d %H:%M"),
            "sector": sector, "note": note, "user_token": token,
        }
        requests.post(url, json=payload, headers=sb_headers(), timeout=10)
    except: pass

# ══════════════════════════════════════════════════════
# 三合一選股
# ══════════════════════════════════════════════════════
def run_trinity():
    log("🎯 三合一選股開始...")
    load_sector_map()

    stocks = []
    all_turnover = []

    # 上市
    try:
        r = SESSION.get("https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL", timeout=15)
        for row in r.json():
            code     = row.get("Code","")
            price    = safe_float(row.get("ClosingPrice","0"))
            vol_s    = safe_float(row.get("TradeVolume","0").replace(",",""))
            turnover = safe_float(row.get("TradeValue","0").replace(",",""))
            chg      = safe_float(row.get("Change","0").replace(",",""))
            prev     = price - chg
            pct      = round(chg/prev*100,2) if prev>0 else 0
            if not (str(code).isdigit() and len(code)==4 and price>0): continue
            if price < 10: continue
            stocks.append({"code":code,"name":row.get("Name",""),
                           "price":price,"pct":pct,"vol_s":vol_s,"turnover":turnover})
            all_turnover.append((code, turnover))
    except Exception as e:
        log(f"上市失敗: {e}")

    # 上櫃
    try:
        r2 = SESSION.get("https://www.tpex.org.tw/openapi/v1/tpex_mainboard_quotes", timeout=15)
        for row in r2.json():
            code     = row.get("SecuritiesCompanyCode","") or row.get("code","")
            price    = safe_float(row.get("Close","") or row.get("close",""))
            vol_s    = safe_float(row.get("TradingShares","") or "0")
            turnover = safe_float(row.get("TradeValue","") or "0")
            chg      = safe_float(row.get("Change","") or "0")
            prev     = price - chg
            pct      = round(chg/prev*100,2) if prev>0 else 0
            if not (str(code).isdigit() and len(code)==4 and price>0): continue
            if price < 10: continue
            stocks.append({"code":code,"name":row.get("CompanyName","") or row.get("name",""),
                           "price":price,"pct":pct,"vol_s":vol_s,"turnover":turnover})
            all_turnover.append((code, turnover))
    except Exception as e:
        log(f"上櫃失敗: {e}")

    log(f"股票清單取得 {len(stocks)} 支")

    top200 = set(c for c,_ in sorted(all_turnover, key=lambda x:x[1], reverse=True)[:200])

    tw_now     = datetime.utcnow() + timedelta(hours=8)  # 台灣時間（UTC+8）
    today      = tw_now.strftime("%Y-%m-%d")
    start_dt   = (tw_now - timedelta(days=30)).strftime("%Y-%m-%d")
    start_hist = (tw_now - timedelta(days=95)).strftime("%Y-%m-%d")
    results    = []

    for idx, s in enumerate(stocks):
        code = s["code"]
        if idx % 200 == 0:
            log(f"  三合一進度 {idx+1}/{len(stocks)}")
        try:
            records = fetch_history(code, start_hist, today)
            if len(records) < 60: continue

            # ── FinMind 資料延遲補丁 ──────────────────────────
            # 若 FinMind 最新一筆不是今天，把今天的 OHLCV 補進去
            last_data_date = records[-1]["date"] if records else ""
            if last_data_date < today:
                today_high = s.get("high", s["price"])
                today_low  = s.get("low",  s["price"])
                today_vol  = s.get("vol_張", s.get("vol_s", 0) / 1000)
                if today_high > 0 and today_low > 0:
                    records.append({
                        "date":  today,
                        "open":  s["price"],
                        "high":  today_high,
                        "low":   today_low,
                        "close": s["price"],
                        "vol":   today_vol,
                    })
                # 第一支股票才印一次延遲警告，避免 log 爆炸
                if idx == 0:
                    log(f"  ⚠️ FinMind 資料延遲：最新={last_data_date}，今日={today}，已自動補今日快照資料")

            closes = [r["close"] for r in records]
            highs  = [r.get("high", r["close"]) for r in records]
            vols   = [r.get("vol", 0) for r in records]
            n      = len(closes)
            cur    = closes[-1]

            ma20   = sum(closes[-20:])/20
            ma60   = sum(closes[-60:])/60
            hi20   = max(highs[-21:-1]) if n>=21 else max(highs[:-1])
            avg20v = sum(vols[-21:-1])/20 if n>=21 else sum(vols[:-1])/max(n-1,1)

            # 必要條件
            if cur <= ma20: continue
            if ma20 <= ma60: continue
            if cur <= hi20: continue
            vol_ratio = vols[-1]/avg20v if avg20v>0 else 0
            if vol_ratio < 1.5: continue
            ma20_pct = (cur-ma20)/ma20*100
            if GATE_MA20_PCT_CAP and ma20_pct > 8: continue

            # 法人
            inst_rows    = fetch_inst(code, start_dt)
            trust_consec = get_trust_consec(inst_rows)
            if GATE_TRUST_MIN and trust_consec < 3: continue

            # ── 連續因子（加權計分用）＋ 顯示用旗標（沿用原本 detail 徽章）──
            hi20c = max(closes[-20:]); lo20c = min(closes[-20:])
            range_pct = (hi20c-lo20c)/lo20c*100 if lo20c>0 else 999
            hi60 = max(highs[-61:-1]) if n>=61 else max(highs[:-1])
            sector = _sector_map.get(code,"")

            detail = {"ma20":round(ma20,2),"ma60":round(ma60,2),
                      "ma20_pct":round(ma20_pct,1),"vol_ratio":round(vol_ratio,1),
                      "trust_days":trust_consec,"range_pct":round(range_pct,1),
                      "hi60":round(hi60,2)}
            if trust_consec >= 5: detail["trust5"]   = True
            if code in top200:    detail["top200"]   = True
            if range_pct < 8:     detail["platform"] = True
            if vol_ratio >= 2.0:  detail["vol2x"]    = True
            if cur > hi60:        detail["new_high"] = True

            results.append({
                "code":code,"name":s["name"],"price":cur,
                "chg_pct":s["pct"],"sector":sector,
                "trust_days":trust_consec,"vol_ratio":round(vol_ratio,1),
                "ma20":round(ma20,2),"ma60":round(ma60,2),
                "ma20_pct":round(ma20_pct,1),"detail":detail,
                # 連續因子（weighted_score_trinity 會用這些算分）
                "trust_consec": trust_consec,
                "dist_hi20": (cur/hi20 - 1) if hi20 else 0,
                "dist_hi60": (cur/hi60 - 1) if hi60 else 0,
                "ma_trend":  (ma20/ma60 - 1) if ma60 else 0,
                "turnover":  s["turnover"],
            })
            log(f"  ✅ {code} {s['name']} | 投信{trust_consec}天 | 量比{vol_ratio:.1f}x")
        except: pass
        time.sleep(0.2)

    # ── 加權計分（取代原本各 +20 相加；連續分數、無大量同分）──
    results = weighted_score_trinity(results)
    top = results[:30]
    out = {"stocks":top,"total_scanned":len(stocks),"total_passed":len(results),
           "date":today,"time":datetime.now().strftime("%Y/%m/%d %H:%M")}
    log(f"🎯 三合一完成：{len(results)} 支通過（共掃描 {len(stocks)} 支）")
    save_result("trinity_results", out)  # 不管 0 支或多支都存
    return out

# ══════════════════════════════════════════════════════
# 遊牧民選股
# ══════════════════════════════════════════════════════
def run_nomad():
    log("🐎 遊牧民選股開始...")
    load_sector_map()

    stocks = []
    try:
        r = SESSION.get("https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL", timeout=15)
        for row in r.json():
            code  = row.get("Code","")
            price = safe_float(row.get("ClosingPrice","0"))
            vol_s = safe_float(row.get("TradeVolume","0").replace(",",""))
            chg   = safe_float(row.get("Change","0").replace(",",""))
            prev  = price-chg; pct=round(chg/prev*100,2) if prev>0 else 0
            if not (str(code).isdigit() and len(code)==4 and price>0): continue
            if price < 10: continue
            high_s = safe_float(row.get("HighestPrice","0"))
            low_s  = safe_float(row.get("LowestPrice","0"))
            stocks.append({"code":code,"name":row.get("Name",""),
                           "price":price,"pct":pct,"vol_s":vol_s,
                           "high":high_s,"low":low_s,
                           "vol_張":round(vol_s/1000,1)})  # TWSE: 股→張
    except Exception as e: log(f"上市失敗: {e}")

    try:
        r2 = SESSION.get("https://www.tpex.org.tw/openapi/v1/tpex_mainboard_quotes", timeout=15)
        for row in r2.json():
            code  = row.get("SecuritiesCompanyCode","") or row.get("code","")
            price = safe_float(row.get("Close","") or row.get("close",""))
            vol_s = safe_float(row.get("TradingShares","") or "0")
            chg   = safe_float(row.get("Change","") or "0")
            prev  = price-chg; pct=round(chg/prev*100,2) if prev>0 else 0
            if not (str(code).isdigit() and len(code)==4 and price>0): continue
            if price < 10: continue
            high_s2 = safe_float(row.get("High","") or row.get("high","") or str(price))
            low_s2  = safe_float(row.get("Low","")  or row.get("low","")  or str(price))
            vol_千  = safe_float(row.get("TradingShares","") or "0")  # TPEX: 已是千股=張
            stocks.append({"code":code,"name":row.get("CompanyName","") or row.get("name",""),
                           "price":price,"pct":pct,"vol_s":vol_s,
                           "high":high_s2,"low":low_s2,
                           "vol_張":round(vol_千,1)})  # TPEX: 千股=張
    except Exception as e: log(f"上櫃失敗: {e}")

    log(f"股票清單取得 {len(stocks)} 支")

    tw_now     = datetime.utcnow() + timedelta(hours=8)  # 台灣時間（UTC+8）
    today      = tw_now.strftime("%Y-%m-%d")
    start_hist = (tw_now - timedelta(days=95)).strftime("%Y-%m-%d")
    results    = []

    for idx, s in enumerate(stocks):
        code = s["code"]
        if idx % 200 == 0:
            log(f"  遊牧民進度 {idx+1}/{len(stocks)}")
        try:
            records = fetch_history(code, start_hist, today)
            if len(records) < 60: continue

            closes = [r["close"] for r in records]
            highs  = [r.get("high", r["close"]) for r in records]
            lows   = [r.get("low",  r["close"]) for r in records]
            vols   = [r.get("vol", 0) for r in records]
            n      = len(closes)

            # ① 近20日均量 > 2000張
            avg20v = sum(vols[-20:])/20
            if avg20v < 2000: continue

            # ② 今日量 > 近5日均量×2倍
            vol5 = sum(vols[-6:-1])/5 if n>=6 else avg20v
            vol_ratio = vols[-1]/vol5 if vol5>0 else 0
            if vol_ratio < 2.0: continue

            # KD 計算
            k, d = 50.0, 50.0
            ks, ds = [], []
            for i in range(n):
                sl_h = highs[max(0,i-8):i+1]; sl_l = lows[max(0,i-8):i+1]
                rh=max(sl_h); rl=min(sl_l)
                rsv = 50 if rh==rl else (closes[i]-rl)/(rh-rl)*100
                k=k*2/3+rsv/3; d=d*2/3+k/3
                ks.append(k); ds.append(d)
            k_cur,d_cur = ks[-1],ds[-1]
            k_prv,d_prv = ks[-2],ds[-2]

            # ③ KD 黃金交叉
            if not (k_prv<=d_prv and k_cur>d_cur): continue
            # ④ K < 50
            if k_cur >= 50: continue
            # ⑤ K > 20
            if k_cur <= 20: continue
            # ⑥ 股價 > MA60
            ma60 = sum(closes[-60:])/60
            if closes[-1] <= ma60: continue

            sector = _sector_map.get(code,"其他")
            results.append({
                "code":code,"name":s["name"],"price":closes[-1],
                "chg_pct":s["pct"],"vol_today":round(vols[-1]),
                "avg20vol":round(avg20v),"vol_ratio":round(vol_ratio,1),
                "k":round(k_cur,1),"d":round(d_cur,1),"k_d":round(k_cur-d_cur,2),
                "ma60":round(ma60,2),"ma60_pct":round((closes[-1]-ma60)/ma60*100,1),
                "sector":sector,
            })
            log(f"  ✅ {code} {s['name']} | 量比{vol_ratio:.1f}x K={k_cur:.1f}")
        except: pass
        time.sleep(0.15)

    # ① 改用最有預測力的因子排序（取代原本的 vol_ratio）
    results.sort(key=lambda x: x.get(NOMAD_SORT_KEY, 0), reverse=NOMAD_SORT_DESC)
    # ② 訊號數上限，避免灌爆追蹤清單
    top = results[:NOMAD_MAX_SIGNALS]
    out = {"stocks":top,"total_scanned":len(stocks),"total_passed":len(results),
           "date":today,"time":datetime.now().strftime("%Y/%m/%d %H:%M")}
    log(f"🐎 遊牧民完成：{len(results)} 支通過 / 取前 {len(top)} 支（共掃描 {len(stocks)} 支）")
    save_result("nomad_results", out)  # 不管 0 支都存

    # ③ 加出場/檢視窗口：把「建議檢視到期日」寫進 note（不需改 DB schema）
    review_by = (tw_now + timedelta(days=int(NOMAD_HOLD_DAYS * 1.4))).strftime("%Y/%m/%d")
    note = f"遊牧民選股 | 檢視到期 {review_by}"

    # 自動加入追蹤清單（僅加入取前 N 的訊號）
    added = 0
    for s in top:
        add_to_watchlist(s["code"],s["name"],s["price"],s["sector"],note,JACK_TOKEN)
        added += 1
        time.sleep(0.1)
    log(f"🐎 自動加入追蹤清單 {added} 支（檢視到期 {review_by}）")
    return out

# ══════════════════════════════════════════════════════
# 主程式
# ══════════════════════════════════════════════════════
if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv)>1 else "all"
    log(f"=== 台股選股 GitHub Actions 執行 mode={mode} ===")

    if mode in ("all", "trinity"):
        run_trinity()

    if mode in ("all", "nomad"):
        run_nomad()

    log("=== 全部完成 ===")
