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

HOLD_DAYS = [5, 10, 20]   # 預設評估的持有交易日數

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
_price_cache = {}

def fetch_closes(code, start, end):
    """回傳 {date: close} 字典（FinMind）"""
    key = (code, start, end)
    if key in _price_cache:
        return _price_cache[key]
    try:
        r = SESSION.get("https://api.finmindtrade.com/api/v4/data",
            params={"dataset": "TaiwanStockPrice", "data_id": code,
                    "start_date": start, "end_date": end},
            headers={"Authorization": f"Bearer {FINMIND_TOKEN}"}, timeout=15)
        if r.status_code != 200:
            _price_cache[key] = {}
            return {}
        rows = r.json().get("data", [])
        out = {row["date"]: safe_float(row.get("close", 0))
               for row in rows if row.get("close")}
        _price_cache[key] = out
        return out
    except Exception as e:
        log(f"  ⚠️ {code} 抓價失敗: {e}")
        _price_cache[key] = {}
        return {}


# ── 計算單支推薦的後續報酬 ────────────────────────────
def compute_forward_returns(code, rec_date, entry_price, closes_sorted):
    """
    rec_date: 推薦日期(字串)
    entry_price: 推薦當天價
    closes_sorted: [(date, close), ...] 已排序
    回傳 {hold_days: return_pct}
    """
    # 找出推薦日當天或之後的第一個交易日 index
    future = [(d, c) for d, c in closes_sorted if d >= rec_date]
    if len(future) < 2:
        return {}
    # 以推薦日後「第一個交易日開盤(用收盤近似)」為進場基準較保守，
    # 但這裡用推薦當天的 entry_price 為基準（AI 當天收盤後產生推薦）
    base = entry_price if entry_price > 0 else future[0][1]
    if base <= 0:
        return {}
    rets = {}
    for hd in HOLD_DAYS:
        if len(future) > hd:
            exit_price = future[hd][1]
            rets[hd] = round((exit_price - base) / base * 100, 2)
    return rets


# ── 主流程 ──────────────────────────────────────────
def run():
    recs = load_all_recommendations()
    if not recs:
        log("無資料可分析")
        return

    today = (datetime.utcnow() + timedelta(hours=8)).strftime("%Y-%m-%d")

    # 收集每一筆 (推薦日, code, entry_price, rise_prob)
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
            })
    log(f"共 {len(samples)} 筆推薦樣本，開始抓後續股價...")

    # 為每筆抓後續價格並算報酬
    by_code = defaultdict(list)
    for smp in samples:
        by_code[smp["code"]].append(smp)

    results = []
    for ci, (code, smps) in enumerate(by_code.items()):
        if ci % 20 == 0:
            log(f"  進度 {ci+1}/{len(by_code)} 檔")
        dates = [s["date"] for s in smps]
        start = (datetime.strptime(min(dates), "%Y-%m-%d") - timedelta(days=5)).strftime("%Y-%m-%d")
        closes = fetch_closes(code, start, today)
        if not closes:
            continue
        closes_sorted = sorted(closes.items())
        for smp in smps:
            rets = compute_forward_returns(code, smp["date"], smp["entry"], closes_sorted)
            if rets:
                smp["returns"] = rets
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

    # 印出摘要
    log("════════ AI 選股實戰績效摘要 ════════")
    for hd, st in summary["by_hold"].items():
        log(f"  持有{hd}日: 樣本{st['n']} | 勝率{st['win_rate']}% | "
            f"平均報酬{st['avg_return']}% | 最大獲利{st['max_gain']}% | 最大虧損{st['max_loss']}%")
    log("──── 上漲機率分組驗證（持有20日）────")
    for k, st in prob_check.items():
        log(f"  {k}: 樣本{st['n']} | 勝率{st['win_rate']}% | 平均報酬{st['avg_return']}%")

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
