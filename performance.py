"""
台股 AI 選股 — 實戰績效分析（階段1）
=====================================
讀取 Supabase analysis_results 裡每天的 AI 推薦，
抓推薦後的實際股價，計算：
  - 各持有期（5/10/20 日）的平均報酬、勝率、最大回撤
  - 依「上漲機率(rise_prob)」分組，驗證機率高的是否真的報酬高
另外追蹤「籌碼左側」名單（left_side_results）：持有 20 日（＝時間停損）報酬、
對 0050 的超額報酬，以及套用 −8% 停損後的報酬。左側樣本不混進 AI 的統計。
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
TG_BOT_TOKEN  = os.environ.get("TG_BOT_TOKEN", "")
TG_CHAT_ID    = os.environ.get("TG_CHAT_ID", "")
import strategy     # 選股條件與每月自動調整（與 auto_analysis 共用）

HOLD_DAYS   = [5, 10, 20]   # 預設評估的持有交易日數
MODEL_DAYS  = 15            # 模型預測的天數（auto_analysis PREDICT_DAYS）
WINDOW_DAYS = 120           # 只評估近 120 天的推薦，控制 FinMind 呼叫量
LEFT_DAYS   = 20            # 左側：時間停損天數（left_side.EXIT）
LEFT_STOP   = -8.0          # 左側：停損 %

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


def load_left_recommendations():
    """籌碼左側每日名單；還沒建表就回空 list（不影響 AI 績效）"""
    if not SUPABASE_URL or not SUPABASE_KEY:
        return []
    try:
        r = SESSION.get(f"{SUPABASE_URL}/rest/v1/left_side_results",
            params={"select": "date,stocks,created_at", "order": "date.asc", "limit": "500"},
            headers=sb_headers(), timeout=20)
    except Exception as e:
        log(f"⚠️ 讀取左側名單失敗: {e}")
        return []
    if r.status_code != 200:
        log(f"ℹ️ 讀不到 left_side_results（{r.status_code}），略過左側追蹤")
        return []
    seen, out = set(), []
    for row in sorted(r.json(), key=lambda x: x.get("created_at", ""), reverse=True):
        d = row.get("date")
        if d in seen:
            continue
        seen.add(d)
        if isinstance(row.get("stocks"), str):
            try: row["stocks"] = json.loads(row["stocks"])
            except Exception: row["stocks"] = []
        out.append(row)
    out.sort(key=lambda x: x.get("date", ""))
    log(f"讀到 {len(out)} 天的左側名單")
    return out


def load_candidates():
    """每天所有分析過的股票（含沒被推薦的），自動調整選股條件用"""
    if not SUPABASE_URL or not SUPABASE_KEY:
        return []
    try:
        r = SESSION.get(f"{SUPABASE_URL}/rest/v1/ai_candidates",
            params={"select": "date,candidates,created_at", "order": "date.asc", "limit": "400"},
            headers=sb_headers(), timeout=30)
    except Exception as e:
        log(f"⚠️ 讀取候選股失敗: {e}")
        return []
    if r.status_code != 200:
        log(f"ℹ️ 讀不到 ai_candidates（{r.status_code}），略過選股條件調整")
        return []
    seen, out = set(), []
    for row in sorted(r.json(), key=lambda x: x.get("created_at", ""), reverse=True):
        d = row.get("date")
        if d in seen:
            continue
        seen.add(d)
        if isinstance(row.get("candidates"), str):
            try: row["candidates"] = json.loads(row["candidates"])
            except Exception: row["candidates"] = []
        out.append(row)
    out.sort(key=lambda x: x.get("date", ""))
    log(f"讀到 {len(out)} 天的候選股")
    return out


def load_strategy_rows(limit=10):
    """選股條件的歷史（最新在前）"""
    if not SUPABASE_URL or not SUPABASE_KEY:
        return []
    try:
        r = SESSION.get(f"{SUPABASE_URL}/rest/v1/strategy_config",
            params={"select": "config,reason,created_at", "order": "created_at.desc", "limit": str(limit)},
            headers=sb_headers(), timeout=15)
        if r.status_code != 200:
            return []
        rows = r.json()
        for row in rows:
            if isinstance(row.get("config"), str):
                row["config"] = json.loads(row["config"])
        return rows
    except Exception:
        return []


def save_strategy(cfg, reason, stats):
    try:
        r = SESSION.post(f"{SUPABASE_URL}/rest/v1/strategy_config",
            json={"config": cfg, "reason": reason, "stats": stats}, headers=sb_headers(), timeout=15)
        ok = r.status_code in (200, 201)
        log(("✅ 已寫入新選股條件" if ok else f"❌ 新選股條件寫入失敗 {r.status_code}: {r.text[:150]}"))
        return ok
    except Exception as e:
        log(f"❌ 新選股條件寫入例外: {e}")
        return False


def send_telegram(msg):
    import notify
    return notify.send(msg)


def _record(data):
    try:
        import notify
        notify.record("perf", data)
    except Exception:
        pass


# ── 抓某股票一段期間的收盤價 ──────────────────────────
import time as _time
FINMIND_MIN_INTERVAL = float(os.environ.get("FINMIND_MIN_INTERVAL", "7"))
_fm = {"last": 0.0, "calls": 0, "quota_out": False, "reused": 0}
_price_cache = {}

_HERE = os.path.dirname(os.path.abspath(__file__))
# auto_analysis.py 同一個排程工作裡剛抓過的日K（今天的），能重用就不再向 FinMind 抓
PRICE_CACHE_FILE = os.environ.get("PRICE_CACHE_FILE", os.path.join(_HERE, "price_cache.json"))
# 已到期樣本的報酬不會再變，存起來（GitHub Actions cache 跨天保存），每天只算還沒到期的
PERF_CACHE_FILE  = os.environ.get("PERF_CACHE_FILE", os.path.join(_HERE, "perf_cache.json"))
_shared = {"prices": None}


def _shared_prices():
    if _shared["prices"] is None:
        _shared["prices"] = {}
        try:
            with open(PRICE_CACHE_FILE, encoding="utf-8") as f:
                j = json.load(f)
            today = (datetime.utcnow() + timedelta(hours=8)).strftime("%Y-%m-%d")
            if j.get("date") == today:
                _shared["prices"] = j.get("prices") or {}
                log(f"讀到今日股價快取 {len(_shared['prices'])} 支（AI 分析時抓的）")
        except FileNotFoundError:
            pass
        except Exception as e:
            log(f"⚠️ 股價快取讀取失敗: {e}")
    return _shared["prices"]


def _from_shared(code, start, end):
    rows = _shared_prices().get(code)
    if not rows or rows[0][0] > start:          # 快取沒涵蓋到起始日 → 照舊向 FinMind 抓
        return None
    out = []
    for d, cl, sp in rows:
        if start <= d <= end and cl and cl > 0:
            ref = (cl - sp) if sp is not None else None
            out.append((d, cl, ref if ref and ref > 0 else None))
    return out

def fetch_closes(code, start, end):
    """
    回傳 [(date, close, ref), ...]；ref = 收盤 − 漲跌價差（＝當日參考價）。
    用 close/ref 連乘算報酬，除權息那天不會被當成下跌。
    與 auto_analysis 一樣每次間隔 7 秒；FinMind 回 402/429（額度用盡）就停止再抓。
    """
    key = (code, start, end)
    if key in _price_cache:
        return _price_cache[key]
    shared = _from_shared(code, start, end)
    if shared:
        _fm["reused"] += 1
        _price_cache[key] = shared
        return shared
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
        if r.status_code != 200 or not body.get("data"):
            _fm["empty"] = _fm.get("empty", 0) + 1
            if _fm["empty"] <= 3:     # 之前失敗完全不印，績效一直是 0 筆卻看不出原因
                log(f"  ⚠️ FinMind {code} 沒有資料：HTTP {r.status_code} "
                    f"status={body.get('status')} msg={str(body.get('msg', ''))[:100]}")
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


# ── 已到期樣本快取 ────────────────────────────────────
def _skey(smp):
    return f"{smp['model']}|{smp['date']}|{smp['code']}|{smp['entry']}"


def _is_final(entry, model, horizons):
    """報酬已經不會再變的樣本（所有持有天數都到期、也算出對 0050 的超額報酬）"""
    rets = entry.get("returns") or {}
    if "excess" not in entry:
        return False
    if model == "left":
        return LEFT_DAYS in rets and "rule_ret" in entry
    if model == "cand":
        return strategy.HOLD in rets
    if strategy.HOLD in horizons and "excess20" not in entry:
        return False
    return all(h in rets for h in horizons)


def load_perf_cache():
    try:
        with open(PERF_CACHE_FILE, encoding="utf-8") as f:
            raw = json.load(f).get("samples", {})
        out = {}
        for k, v in raw.items():
            v = dict(v)
            v["returns"] = {int(h): r for h, r in (v.get("returns") or {}).items()}
            out[k] = v
        return out
    except FileNotFoundError:
        return {}
    except Exception as e:
        log(f"⚠️ 績效快取讀取失敗（改成全部重算）: {e}")
        return {}


def save_perf_cache(done_samples, horizons):
    keep = {}
    for smp in done_samples:
        if _is_final(smp, smp["model"], horizons):
            e = {"returns": smp["returns"], "excess": smp["excess"]}
            for k in ("rule_ret", "excess20"):
                if k in smp:
                    e[k] = smp[k]
            keep[_skey(smp)] = e
    try:
        with open(PERF_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump({"saved": datetime.utcnow().isoformat(), "samples": keep}, f)
        log(f"已到期樣本快取 {len(keep)} 筆（明天不用重抓）")
    except Exception as e:
        log(f"⚠️ 績效快取存檔失敗: {e}")


# ── 每月自動調整選股條件 ──────────────────────────────
def run_tuning(cand_recs, cand_results):
    """用已到期的候選股，比較不同選股條件「每天前 5 名」的一個月超額報酬；夠好才換（見 strategy.py）"""
    rows = load_strategy_rows()
    current = strategy.normalize(rows[0]["config"] if rows else None)
    since = (rows[0].get("created_at") or "")[:10] if rows else ""
    # 只用「有超額報酬」的候選股；一天至少一半的候選股算得出來才算到期
    by_date = defaultdict(list)
    for r in cand_results:
        if "excess" in r:
            by_date[r["date"]].append({**r["cand"], "excess": r["excess"]})
    totals = {d["date"]: len(d.get("candidates", [])) for d in cand_recs}
    days = {d: v for d, v in by_date.items() if totals.get(d) and len(v) >= totals[d] * 0.5}
    since_change = sum(1 for d in totals if d > since) if since else None
    dec = strategy.decide(current, days, since_change)
    dec["since"] = since
    dec["describe"] = strategy.describe(current)
    dec["history"] = [{"date": (r.get("created_at") or "")[:10], "describe": strategy.describe(r.get("config")),
                       "locked": strategy.normalize(r.get("config")).get("locked"),
                       "reason": r.get("reason", "")} for r in rows[:5]]
    log(f"──── 選股條件：{dec['describe']} → {dec['status']}：{dec['message']}")
    if dec["status"] == "changed":
        stats = {"gain": dec["gain"], "matured_days": dec["matured_days"], "table": dec["table"],
                 "current_eval": dec.get("current_eval")}
        if save_strategy(dec["new"], dec["message"], stats):
            send_telegram("🔧 <b>AI 選股條件已自動調整</b>\n"
                          f"原本：{strategy.describe(current)}\n"
                          f"改為：{strategy.describe(dec['new'])}\n\n{dec['message']}\n"
                          "（明天 14:35 的選股開始套用；不想自動調整可到回測頁鎖定）")
            dec["describe_new"] = strategy.describe(dec["new"])
    return dec


# ── 主流程 ──────────────────────────────────────────
def run():
    recs = load_all_recommendations()
    left_recs = load_left_recommendations()
    cand_recs = load_candidates()
    if not recs and not left_recs and not cand_recs:
        log("無資料可分析")
        return

    today = (datetime.utcnow() + timedelta(hours=8)).strftime("%Y-%m-%d")

    # 只評估近 WINDOW_DAYS 天的推薦：控制 FinMind 呼叫量（每檔一次），排程時間才不會越跑越久
    cutoff = (datetime.utcnow() + timedelta(hours=8) - timedelta(days=WINDOW_DAYS)).strftime("%Y-%m-%d")
    recs = [r for r in recs if (r.get("date") or "") >= cutoff]
    left_recs = [r for r in left_recs if (r.get("date") or "") >= cutoff]
    cand_recs = [r for r in cand_recs if (r.get("date") or "") >= cutoff]

    # 收集每一筆 (推薦日, code, entry_price, rise_prob, 新/舊模型)
    # 新模型（10/23 起）的推薦帶有 edge 欄位，以此區分
    samples = []
    for day in recs:
        d = day.get("date")
        for rank, s in enumerate(day.get("stocks", [])):
            samples.append({
                "rank":      rank,
                "date":      d,
                "code":      s.get("code", ""),
                "name":      s.get("name", ""),
                "entry":     safe_float(s.get("price", 0)),
                "rise_prob": safe_float(s.get("rise_prob", 0)),
                "model":     "new" if "edge" in s else "old",
            })
    left_samples = []
    for day in left_recs:
        for s in day.get("stocks", []):
            left_samples.append({"date": day.get("date"), "code": s.get("code", ""),
                                 "name": s.get("name", ""), "entry": safe_float(s.get("price", 0)),
                                 "rise_prob": 0, "model": "left"})
    cand_samples = []
    for day in cand_recs:
        for c in day.get("candidates", []):
            if c.get("code") and safe_float(c.get("price")) > 0:
                cand_samples.append({"date": day.get("date"), "code": str(c["code"]),
                                     "entry": safe_float(c.get("price")), "model": "cand", "cand": c})
    if not samples and not left_samples and not cand_samples:
        log(f"近 {WINDOW_DAYS} 天沒有推薦紀錄")
        return
    log(f"共 {len(samples)} 筆 AI 推薦、{len(left_samples)} 筆左側樣本、{len(cand_samples)} 筆候選股")

    horizons = sorted(set(HOLD_DAYS) | {MODEL_DAYS})
    left_h = list(range(1, LEFT_DAYS + 1))         # 左側要逐日路徑才能判斷停損

    # 已到期的樣本直接沿用，只抓還沒到期的
    cache = load_perf_cache()
    results, left_results, cand_results, pending = [], [], [], []
    bucket = {"left": left_results, "cand": cand_results}
    for smp in samples + left_samples + cand_samples:
        c = cache.get(_skey(smp))
        if c and _is_final(c, smp["model"], horizons):
            smp.update(c)
            bucket.get(smp["model"], results).append(smp)
        elif smp["model"] == "cand" and (datetime.strptime(today, "%Y-%m-%d") -
                                         datetime.strptime(smp["date"], "%Y-%m-%d")).days < 27:
            continue      # 候選股只看 20 個交易日報酬，未滿約 4 週一定還沒到期，不用抓價
        else:
            pending.append(smp)

    # 為每筆抓後續價格並算報酬（同一檔的 AI 與左側樣本共用一次抓價）
    by_code = defaultdict(list)
    for smp in pending:
        by_code[smp["code"]].append(smp)
    log(f"已到期沿用快取 {len(results) + len(left_results) + len(cand_results)} 筆；"
        f"需計算 {len(pending)} 筆（{len(by_code)} 檔），開始抓後續股價...")

    # 基準：同一天買 0050，持有同樣天數（扣掉大盤漲跌，新舊模型才能公平比較）
    bench = []
    if pending:
        first = min(s["date"] for s in pending)
        bench = fetch_closes("0050", (datetime.strptime(first, "%Y-%m-%d") - timedelta(days=5)).strftime("%Y-%m-%d"), today)

    for ci, (code, smps) in enumerate(by_code.items()):
        if ci % 20 == 0:
            log(f"  進度 {ci+1}/{len(by_code)} 檔")
        dates = [s["date"] for s in smps]
        start = (datetime.strptime(min(dates), "%Y-%m-%d") - timedelta(days=5)).strftime("%Y-%m-%d")
        closes = fetch_closes(code, start, today)
        if not closes:
            continue
        for smp in smps:
            if smp["model"] == "cand":
                rets = compute_forward_returns(code, smp["date"], smp["entry"], closes, [strategy.HOLD])
                if strategy.HOLD in rets:
                    smp["returns"] = rets
                    if bench:
                        b = compute_forward_returns("0050", smp["date"], 0, bench, [strategy.HOLD])
                        if strategy.HOLD in b:
                            smp["excess"] = round(rets[strategy.HOLD] - b[strategy.HOLD], 2)
                    cand_results.append(smp)
                continue
            if smp["model"] == "left":
                path = compute_forward_returns(code, smp["date"], smp["entry"], closes, left_h)
                if path:
                    smp["returns"] = path
                    if LEFT_DAYS in path:
                        # 照規則：收盤跌破 −8% 那天出場，否則持有到第 20 日
                        hit = next((path[t] for t in left_h if t in path and path[t] <= LEFT_STOP), None)
                        smp["rule_ret"] = hit if hit is not None else path[LEFT_DAYS]
                        if bench:
                            b = compute_forward_returns("0050", smp["date"], 0, bench, [LEFT_DAYS])
                            if LEFT_DAYS in b:
                                smp["excess"] = round(path[LEFT_DAYS] - b[LEFT_DAYS], 2)
                    left_results.append(smp)
                continue
            rets = compute_forward_returns(code, smp["date"], smp["entry"], closes, horizons)
            if rets:
                smp["returns"] = rets
                if bench:
                    b = compute_forward_returns("0050", smp["date"], 0, bench, [MODEL_DAYS, strategy.HOLD])
                    if MODEL_DAYS in rets and MODEL_DAYS in b:
                        smp["excess"] = round(rets[MODEL_DAYS] - b[MODEL_DAYS], 2)
                    if strategy.HOLD in rets and strategy.HOLD in b:
                        smp["excess20"] = round(rets[strategy.HOLD] - b[strategy.HOLD], 2)
                results.append(smp)

    log(f"成功計算 AI {len(results)} 筆、左側 {len(left_results)} 筆、候選股 {len(cand_results)} 筆"
        f"（其餘因未到期、資料不足或非交易日略過）")
    tuning = run_tuning(cand_recs, cand_results)
    _record({"ai_done": len(results), "left_done": len(left_results), "cand_done": len(cand_results),
             "fm_calls": _fm["calls"], "fm_empty": _fm.get("empty", 0), "reused": _fm["reused"],
             "quota_out": _fm["quota_out"],
             "tuning": {k: tuning.get(k) for k in ("status", "message", "matured_days", "need_days",
                                                   "describe", "describe_new")}})
    if not results and not left_results and not cand_results:
        log(f"  ⚠️ 全部沒算出來：FinMind 呼叫 {_fm['calls']} 次、沒資料 {_fm.get('empty', 0)} 次、"
            f"重用 AI 股價 {_fm['reused']} 檔、額度用盡={_fm['quota_out']}")
        return
    save_perf_cache(results + left_results + cand_results, horizons)

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

    # ── 每天前 5 名，追蹤 20 個交易日（約一個月），對 0050 ──
    H = strategy.HOLD
    top_all = [s for s in samples if s.get("rank", 99) < strategy.TOP_PICKS]
    top_done = [r for r in results if r.get("rank", 99) < strategy.TOP_PICKS and H in r["returns"]]
    st = {"total": len(top_all), "n": len(top_done), "pending": len(top_all) - len(top_done), "days": H,
          "first_date": min((s["date"] for s in top_all), default="")}
    if top_done:
        r20 = [r["returns"][H] for r in top_done]
        exc = [r["excess20"] for r in top_done if "excess20" in r]
        st.update({
            "win_rate":   round(len([v for v in r20 if v > 0]) / len(r20) * 100, 1),
            "avg_return": round(sum(r20) / len(r20), 2),
            "avg_excess": round(sum(exc) / len(exc), 2) if exc else None,
            "beat_bench": round(len([v for v in exc if v > 0]) / len(exc) * 100, 1) if exc else None,
        })
    by_model["top5"] = st
    _record({"top5": st})

    # ── 籌碼左側（持有 20 日；另算套用 −8% 停損的報酬）──
    done = [r for r in left_results if LEFT_DAYS in r["returns"]]
    exc = [r["excess"] for r in done if "excess" in r]
    st = {"total": len(left_samples), "n": len(done), "pending": len(left_samples) - len(done),
          "first_date": min((s["date"] for s in left_samples), default=""), "days": LEFT_DAYS}
    if done:
        r20 = [r["returns"][LEFT_DAYS] for r in done]
        rule = [r["rule_ret"] for r in done]
        st.update({
            "hit_rate":   round(len([v for v in r20 if v >= 3]) / len(r20) * 100, 1),
            "win_rate":   round(len([v for v in r20 if v > 0]) / len(r20) * 100, 1),
            "avg_return": round(sum(r20) / len(r20), 2),
            "avg_excess": round(sum(exc) / len(exc), 2) if exc else None,
            "beat_bench": round(len([v for v in exc if v > 0]) / len(exc) * 100, 1) if exc else None,
            "rule_avg":   round(sum(rule) / len(rule), 2),
            "rule_win":   round(len([v for v in rule if v > 0]) / len(rule) * 100, 1),
            "stopped":    len([r for r in done if r["rule_ret"] != r["returns"][LEFT_DAYS]]),
        })
    by_model["left"] = st
    summary["by_model"]   = by_model
    summary["tuning"]     = tuning
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
        if m not in ("old", "new"):
            continue
        if st["n"]:
            log(f"  {'新' if m=='new' else '舊'}模型: 到期{st['n']}筆 待到期{st['pending']}筆 | 命中(≥3%){st['hit_rate']}% | "
                f"平均報酬{st['avg_return']}% | 超額報酬{st['avg_excess']}% | 贏過0050 {st['beat_bench']}%")
        else:
            log(f"  {'新' if m=='new' else '舊'}模型: 尚無到期樣本（待到期 {st['pending']} 筆）")
    t5 = by_model["top5"]
    if t5["n"]:
        log(f"──── 每天前 5 名（持有{t5['days']}日）：到期{t5['n']}筆 待到期{t5['pending']}筆 | 勝率{t5['win_rate']}% | "
            f"平均{t5['avg_return']}% | 超額{t5['avg_excess']}% | 贏過0050 {t5['beat_bench']}%")
    lt = by_model["left"]
    if lt["n"]:
        log(f"──── 籌碼左側（持有{LEFT_DAYS}日）：到期{lt['n']}筆 待到期{lt['pending']}筆 | 勝率{lt['win_rate']}% | "
            f"平均{lt['avg_return']}% | 超額{lt['avg_excess']}% | 照停損規則平均{lt['rule_avg']}%（停損 {lt['stopped']} 筆）")
    else:
        log(f"──── 籌碼左側：尚無到期樣本（待到期 {lt['pending']} 筆）")
    log(f"  FinMind 呼叫 {_fm['calls']} 次（沒資料 {_fm.get('empty', 0)} 次）、重用 AI 已抓股價 {_fm['reused']} 檔"
        f"{'（額度用盡，結果不完整）' if _fm['quota_out'] else ''}")

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
