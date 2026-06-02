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
            if ma20_pct > 8: continue

            # 法人
            inst_rows    = fetch_inst(code, start_dt)
            trust_consec = get_trust_consec(inst_rows)
            if trust_consec < 3: continue

            # 加分
            score  = 0
            detail = {"ma20":round(ma20,2),"ma60":round(ma60,2),
                      "ma20_pct":round(ma20_pct,1),"vol_ratio":round(vol_ratio,1),
                      "trust_days":trust_consec}

            if trust_consec >= 5:  score += 20; detail["trust5"] = True
            if code in top200:     score += 20; detail["top200"] = True

            hi20c = max(closes[-20:]); lo20c = min(closes[-20:])
            range_pct = (hi20c-lo20c)/lo20c*100 if lo20c>0 else 999
            if range_pct < 8:      score += 20; detail["platform"] = True
            detail["range_pct"] = round(range_pct,1)

            if vol_ratio >= 2.0:   score += 20; detail["vol2x"] = True

            hi60 = max(highs[-61:-1]) if n>=61 else max(highs[:-1])
            if cur > hi60:         score += 20; detail["new_high"] = True
            detail["hi60"] = round(hi60,2)

            sector = _sector_map.get(code,"")
            results.append({
                "code":code,"name":s["name"],"price":cur,
                "chg_pct":s["pct"],"sector":sector,
                "total_score":score,"trust_days":trust_consec,
                "vol_ratio":round(vol_ratio,1),
                "ma20":round(ma20,2),"ma60":round(ma60,2),
                "ma20_pct":round(ma20_pct,1),"detail":detail,
            })
            log(f"  ✅ {code} {s['name']} | {score}分 | 投信{trust_consec}天 | 量比{vol_ratio:.1f}x")
        except: pass
        time.sleep(0.2)

    results.sort(key=lambda x: x["total_score"], reverse=True)
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
            stocks.append({"code":code,"name":row.get("Name",""),
                           "price":price,"pct":pct,"vol_s":vol_s})
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
            stocks.append({"code":code,"name":row.get("CompanyName","") or row.get("name",""),
                           "price":price,"pct":pct,"vol_s":vol_s})
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
                "k":round(k_cur,1),"d":round(d_cur,1),
                "ma60":round(ma60,2),"ma60_pct":round((closes[-1]-ma60)/ma60*100,1),
                "sector":sector,
            })
            log(f"  ✅ {code} {s['name']} | 量比{vol_ratio:.1f}x K={k_cur:.1f}")
        except: pass
        time.sleep(0.15)

    results.sort(key=lambda x: x["vol_ratio"], reverse=True)
    out = {"stocks":results,"total_scanned":len(stocks),"total_passed":len(results),
           "date":today,"time":datetime.now().strftime("%Y/%m/%d %H:%M")}
    log(f"🐎 遊牧民完成：{len(results)} 支通過（共掃描 {len(stocks)} 支）")
    save_result("nomad_results", out)  # 不管 0 支都存

    # 自動加入追蹤清單
    added = 0
    for s in results:
        add_to_watchlist(s["code"],s["name"],s["price"],s["sector"],"遊牧民選股",JACK_TOKEN)
        added += 1
        time.sleep(0.1)
    log(f"🐎 自動加入追蹤清單 {added} 支")
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
