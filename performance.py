"""
台股 AI 選股 — 實戰績效分析（階段1）
=====================================
讀取 Supabase analysis_results 裡每天的 AI 推薦，
抓推薦後的實際股價，計算：
  - 各持有期（5/10/20 日）的平均報酬、勝率、最大回撤
  - 依「上漲機率(rise_prob)」分組，驗證機率高的是否真的報酬高
結果寫回 Supabase performance_summary 表，供回測頁面顯示。

使用方式：
  python performance.py            # 分析全部歷史推薦
  python performance.py 5 10 20    # 自訂持有天數

由 GitHub Actions 排程執行（不佔用 Render 資源）。
"""
import os
import json
import requests
import urllib3
from datetime import datetime, timedelta
from collections import defaultdict

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

SUPABASE_URL  = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY  = os.environ.get("SUPABASE_KEY", "")
FINMIND_TOKEN = os.environ.get("FINMIND_TOKEN", "")

HOLD_DAYS   = [5, 10, 20]   # 預設評估的持有交易日數
MODEL_DAYS  = 15            # 模型預測的天數（auto_analysis PREDICT_DAYS）
WINDOW_DAYS = 120           # 只評估近 120 天的推薦，控制 FinMind 呼叫量

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "Mozilla/5.0"})


def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def sb_headers():
    return {
        "apikey":        SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type":  "application/json",
        "Prefer":        "return=minimal",
    }


def safe_float(x, d=0.0):
    try:
        return float(str(x).replace(",", "").replace("+", ""))
    except Exception:
        return d


# ── 讀取歷史推薦 ────────────────────────────────────
def load_all_recommendations():
    """從 Supabase 讀出每一天的 AI 推薦清單"""
    if not SUPABASE_URL or not SUPABASE_KEY:
        log("❌ 未設定 SUPABASE_URL / SUPABASE_KEY")
        return []
    url = f"{SUPABASE_URL}/rest/v1/analysis_results"
    params = {
        "select": "date,model_ver,stocks,created_at",
        "order":  "date.asc",
        "limit":  "500",
    }
    r = SESSION.get(url, params=params, headers=sb_headers(), timeout=20)
    if r.status_code != 200:
        log(f"❌ 讀取失敗 {r.status_code}: {r.text[:200]}")
        return []
    rows = r.json()
    # 同日期去重，保留最新一筆
    seen, deduped = set(), []
    for row in sorted(rows, key=lambda x: x.get("created_at", ""), reverse=True):
        d = row.get("date")
        if d in seen:
            continue
        seen.add(d)
        if isinstance(row.get("stocks"), str):
            try:
                row["stocks"] = json.loads(row["stocks"])
            except Exception:
                row["stocks"] = []
        deduped.append(row)
    deduped.sort(key=lambda x: x.get("date", ""))
    log(f"讀到 {len(deduped)} 天的推薦紀錄")
    return deduped


# ── 抓某股票一段期間的收盤價 ──────────────────────────
import time as _time
FINMIND_MIN_INTERVAL = float(os.environ.get("FINMIND_MIN_INTERVAL", "7"))
_fm = {"last": 0.0, "calls": 0, "quota_out": False}
_price_cache = {}

def fetch_closes(code, start, end):
    """
    回傳 [(date, close, ref), ...]；ref = 收盤 − 漲跌價差（＝當日參考價）。
    用 close/ref 連乘算報酬，除權息那天不會被當成下跌。
    與 auto_analysis 一樣每次間隔 7 秒；FinMind 回 402/429（額度用盡）就停止再抓。
    """
    key = (code, start, end)
    if key in _price_cache:
        return _price_cache[key]
    if _fm["quota_out"] or not FINMIND_TOKEN:
        return []
    wait = FINMIND_MIN_INTERVAL - (_time.time() - _fm["last"])
    if wait > 0:
        _time.sleep(wait)
    _fm["last"] = _time.time()
    _fm["calls"] += 1
    try:
        r = SESSION.get("https://api.finmindtrade.com/api/v4/data",
            params={"dataset": "TaiwanStockPrice", "data_id": code,
                    "start_date": start, "end_date": end},
            headers={"Authorization": f"Bearer {FINMIND_TOKEN}"}, timeout=20)
        body = {}
        try: body = r.json()
        except Exception: pass
        if r.status_code in (402, 429) or body.get("status") in (402, 429):
            _fm["quota_out"] = True
            log(f"  ⚠️ FinMind 額度用盡，已抓 {_fm['calls']} 次，其餘樣本本次略過")
            return []
        out = []
        for row in body.get("data", []) if r.status_code == 200 else []:
            cl = safe_float(row.get("close", 0))
            if cl > 0:
                ref = cl - safe_float(row.get("spread", 0))
                out.append((row["date"][:10], cl, ref if ref > 0 else None))
        out.sort()
        _price_cache[key] = out
        return out
    except Exception as e:
        log(f"  ⚠️ {code} 抓價失敗: {e}")
        return []


# ── 計算單支推薦的後續報酬 ────────────────────────────
def compute_forward_returns(code, rec_date, entry_price, closes_sorted, horizons=None):
    """
    回傳 {持有天數: 報酬%}。以推薦日收盤為基準，逐日連乘 close/參考價（除權息不失真）。
    """
    horizons = horizons or HOLD_DAYS
    future = [x for x in closes_sorted if x[0] >= rec_date]
    if len(future) < 2:
        return {}
    rets, growth = {}, 1.0
    for t in range(1, max(horizons) + 1):
        if t >= len(future):
            break
        cl, ref = future[t][1], future[t][2]
        prev = future[t-1][1]
        base = ref if ref else prev
        if base > 0:
            growth *= cl / base
        if t in horizons:
            rets[t] = round((growth - 1) * 100, 2)
    return rets


# ── 主流程 ──────────────────────────────────────────
def run():
    recs = load_all_recommendations()
    if not recs:
        log("無資料可分析")
        return

    today = (datetime.utcnow() + timedelta(hours=8)).strftime("%Y-%m-%d")

    # 只評估近 WINDOW_DAYS 天的推薦：控制 FinMind 呼叫量（每檔一次），排程時間才不會越跑越久
    cutoff = (datetime.utcnow() + timedelta(hours=8) - timedelta(days=WINDOW_DAYS)).strftime("%Y-%m-%d")
    recs = [r for r in recs if (r.get("date") or "") >= cutoff]

    # 收集每一筆 (推薦日, code, entry_price, rise_prob, 新/舊模型)
    # 新模型（10/23 起）的推薦帶有 edge 欄位，以此區分
    samples = []
    for day in recs:
        d = day.get("date")
        for s in day.get("stocks", []):
            samples.append({
                "date":      d,
                "code":      s.get("code", ""),
                "name":      s.get("name", ""),
                "entry":     safe_float(s.get("price", 0)),
                "rise_prob": safe_float(s.get("rise_prob", 0)),
                "model":     "new" if "edge" in s else "old",
            })
    if not samples:
        log(f"近 {WINDOW_DAYS} 天沒有推薦紀錄")
        return
    log(f"共 {len(samples)} 筆推薦樣本，開始抓後續股價...")

    # 為每筆抓後續價格並算報酬
    by_code = defaultdict(list)
    for smp in samples:
        by_code[smp["code"]].append(smp)

    # 基準：同一天買 0050，持有同樣天數（扣掉大盤漲跌，新舊模型才能公平比較）
    horizons = sorted(set(HOLD_DAYS) | {MODEL_DAYS})
    first = min(s["date"] for s in samples)
    bench = fetch_closes("0050", (datetime.strptime(first, "%Y-%m-%d") - timedelta(days=5)).strftime("%Y-%m-%d"), today)

    results = []
    for ci, (code, smps) in enumerate(by_code.items()):
        if ci % 20 == 0:
            log(f"  進度 {ci+1}/{len(by_code)} 檔")
        dates = [s["date"] for s in smps]
        start = (datetime.strptime(min(dates), "%Y-%m-%d") - timedelta(days=5)).strftime("%Y-%m-%d")
        closes = fetch_closes(code, start, today)
        if not closes:
            continue
        for smp in smps:
            rets = compute_forward_returns(code, smp["date"], smp["entry"], closes, horizons)
            if rets:
                smp["returns"] = rets
                if MODEL_DAYS in rets and bench:
                    b = compute_forward_returns("0050", smp["date"], 0, bench, [MODEL_DAYS])
                    if MODEL_DAYS in b:
                        smp["excess"] = round(rets[MODEL_DAYS] - b[MODEL_DAYS], 2)
                results.append(smp)

    log(f"成功計算 {len(results)} 筆（其餘因資料不足或非交易日略過）")
    if not results:
        return

    # ── 彙總統計 ──
    summary = {"generated": today, "sample_count": len(results), "by_hold": {}}
    for hd in HOLD_DAYS:
        vals = [r["returns"][hd] for r in results if hd in r["returns"]]
        if not vals:
            continue
        wins = [v for v in vals if v > 0]
        summary["by_hold"][str(hd)] = {
            "n":          len(vals),
            "avg_return": round(sum(vals) / len(vals), 2),
            "win_rate":   round(len(wins) / len(vals) * 100, 1),
            "max_gain":   round(max(vals), 2),
            "max_loss":   round(min(vals), 2),
            "median":     round(sorted(vals)[len(vals)//2], 2),
        }

    # ── 依上漲機率分組（驗證模型機率是否有效）──
    buckets = {"高(≥70%)": [], "中(50-70%)": [], "低(<50%)": []}
    for r in results:
        if 20 not in r["returns"]:
            continue
        p, ret20 = r["rise_prob"], r["returns"][20]
        if p >= 70:   buckets["高(≥70%)"].append(ret20)
        elif p >= 50: buckets["中(50-70%)"].append(ret20)
        else:         buckets["低(<50%)"].append(ret20)
    prob_check = {}
    for k, vals in buckets.items():
        if vals:
            prob_check[k] = {
                "n":          len(vals),
                "avg_return": round(sum(vals) / len(vals), 2),
                "win_rate":   round(len([v for v in vals if v > 0]) / len(vals) * 100, 1),
            }
    summary["prob_validation"] = prob_check

    # ── 新舊模型對比（持有 15 日 = 模型的預測天數；命中 = 漲 ≥3%，即模型自己的目標）──
    by_model = {}
    for m in ("old", "new"):
        all_m  = [s for s in samples if s["model"] == m]
        done   = [r for r in results if r["model"] == m and MODEL_DAYS in r["returns"]]
        exc    = [r["excess"] for r in done if "excess" in r]
        st = {"total": len(all_m), "n": len(done), "pending": len(all_m) - len(done),
              "first_date": min((s["date"] for s in all_m), default="")}
        if done:
            r15 = [r["returns"][MODEL_DAYS] for r in done]
            st.update({
                "hit_rate":   round(len([v for v in r15 if v >= 3]) / len(r15) * 100, 1),
                "win_rate":   round(len([v for v in r15 if v > 0]) / len(r15) * 100, 1),
                "avg_return": round(sum(r15) / len(r15), 2),
                "avg_excess": round(sum(exc) / len(exc), 2) if exc else None,
                "beat_bench": round(len([v for v in exc if v > 0]) / len(exc) * 100, 1) if exc else None,
            })
        by_model[m] = st
    summary["by_model"]   = by_model
    summary["model_days"] = MODEL_DAYS
    summary["window_days"] = WINDOW_DAYS
    summary["incomplete"] = _fm["quota_out"]

    # 印出摘要
    log("════════ AI 選股實戰績效摘要 ════════")
    for hd, st in summary["by_hold"].items():
        log(f"  持有{hd}日: 樣本{st['n']} | 勝率{st['win_rate']}% | "
            f"平均報酬{st['avg_return']}% | 最大獲利{st['max_gain']}% | 最大虧損{st['max_loss']}%")
    log("──── 上漲機率分組驗證（持有20日）────")
    for k, st in prob_check.items():
        log(f"  {k}: 樣本{st['n']} | 勝率{st['win_rate']}% | 平均報酬{st['avg_return']}%")
    log(f"──── 新舊模型對比（持有{MODEL_DAYS}日，基準 0050）────")
    for m, st in by_model.items():
        if st["n"]:
            log(f"  {'新' if m=='new' else '舊'}模型: 到期{st['n']}筆 待到期{st['pending']}筆 | 命中(≥3%){st['hit_rate']}% | "
                f"平均報酬{st['avg_return']}% | 超額報酬{st['avg_excess']}% | 贏過0050 {st['beat_bench']}%")
        else:
            log(f"  {'新' if m=='new' else '舊'}模型: 尚無到期樣本（待到期 {st['pending']} 筆）")
    log(f"  FinMind 呼叫 {_fm['calls']} 次{'（額度用盡，結果不完整）' if _fm['quota_out'] else ''}")

    # ── 寫回 Supabase performance_summary ──
    save_summary(summary, today)


def save_summary(summary, today):
    if not SUPABASE_URL or not SUPABASE_KEY:
        return
    try:
        url = f"{SUPABASE_URL}/rest/v1/performance_summary"
        SESSION.delete(url, params={"date": f"eq.{today}"}, headers=sb_headers(), timeout=10)
        payload = {"date": today, "summary": json.dumps(summary, ensure_ascii=False)}
        r = SESSION.post(url, json=payload, headers=sb_headers(), timeout=10)
        if r.status_code in (200, 201):
            log("✅ performance_summary 已寫入 Supabase")
        else:
            log(f"❌ 寫入失敗 {r.status_code}: {r.text[:200]}")
    except Exception as e:
        log(f"❌ 寫入例外: {e}")


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        HOLD_DAYS = [int(x) for x in sys.argv[1:]]
    run()
