"""
存股＋營運動能（XQ 社群腳本 B）每日選股＋實際成績追蹤
=====================================================
回測（364 支、2019～2026，扣成本）：抱 60 天勝率 66%、平均 +7.0%（隨便買 +4.1%）、比同期平均多 3.5 個百分點（顯著）；
但 2025 年後（作者沒用來調參）優勢縮小到 +1.5%、只有 39 筆 → 這裡每筆都記錄，實際成績每天更新。

條件（收盤後判斷，隔天買）
  股價 ≥ 10、20 日均量 ≥ 300 張、殖利率 ≥ 5%、近四季 EPS > 0、負債比 0～60%、
  20 日波動 ≤ 2.5%、距 240 日高點 ≥ −10%、近 3 個月營收年增平均 ≥ 10%、單季 EPS 年增 ≥ 10%
建議：抱 60 個交易日（約 3 個月）；收盤跌破買進價 8% 就出場；不用融資
時間點：月營收 M+1 月 11 日、財報 Q1～Q3 季底後 45 天、Q4 後 90 天才使用（和回測一致）

資料（省 FinMind 額度）：先用全市場行情＋殖利率（各 1 次）篩出殖利率 ≥ 5% 的股票；
日 K 存在 div_cache.json（GitHub Actions cache），之後每天只用當日行情補一根；財報每季更新一次。
"""
import os
import json
import time
from datetime import datetime, timedelta

import requests

import notify
import revenue
import signal_alert as S
import xq_check as X

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")
FINMIND_TOKEN = os.environ.get("FINMIND_TOKEN", "").strip()
CACHE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "div_cache.json")
MAX_FETCH = int(os.environ.get("DIV_MAX_FETCH", "400"))
FM_INTERVAL = float(os.environ.get("FINMIND_MIN_INTERVAL", "7"))
HOLD = 60
STOP = 0.08
COOLDOWN_DAYS = 28          # 同一支 4 週內不重複推（回測是 20 個交易日）
KEEP = 270                  # 日 K 保留天數（240 日高點＋波動）

log = S.log
_last = [0.0]


def fm(dataset, code, start):
    """回傳 list；失敗回 None"""
    if not FINMIND_TOKEN:
        return None
    wait = FM_INTERVAL - (time.time() - _last[0])
    if wait > 0:
        time.sleep(wait)
    _last[0] = time.time()
    try:
        r = requests.get("https://api.finmindtrade.com/api/v4/data",
                         params={"dataset": dataset, "data_id": code, "start_date": start},
                         headers={"Authorization": f"Bearer {FINMIND_TOKEN}"}, timeout=40)
        return r.json().get("data") or [] if r.status_code == 200 else None
    except Exception as e:
        log(f"⚠️ {dataset} {code}：{e}")
        return None


def fetch_yield():
    """{代號: (本益比, 殖利率%)}"""
    out = {}
    try:
        for r in S.SESSION.get("https://openapi.twse.com.tw/v1/exchangeReport/BWIBBU_ALL", timeout=20).json():
            out[r.get("Code", "")] = (S._f(r.get("PEratio")), S._f(r.get("DividendYield")))
    except Exception as e:
        log(f"⚠️ 上市殖利率失敗：{e}")
    try:
        for r in S.SESSION.get("https://www.tpex.org.tw/openapi/v1/tpex_mainboard_peratio_analysis", timeout=30).json():
            code = r.get("SecuritiesCompanyCode", "")
            pe = next((r[k] for k in r if "PriceEarning" in k), None)
            y = next((r[k] for k in r if "Yield" in k), None)
            if code:
                out[code] = (S._f(pe), S._f(y))
    except Exception as e:
        log(f"⚠️ 上櫃殖利率失敗：{e}")
    return out


# ── 快取 ──────────────────────────────────────────────
def load_cache():
    try:
        with open(CACHE_FILE, encoding="utf-8") as f:
            c = json.load(f)
    except Exception:
        c = {}
    for k in ("px", "eps", "debt"):
        c.setdefault(k, {})
    return c


def save_cache(c):
    try:
        with open(CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(c, f)
    except Exception as e:
        log(f"⚠️ 快取存檔失敗：{e}")


def prev_weekday(day):
    d = datetime.strptime(day, "%Y-%m-%d") - timedelta(days=1)
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d.strftime("%Y-%m-%d")


def price_rows(cache, code, q, today, budget):
    """日 K（FinMind 格式）：快取接得上就只補今天，否則重抓 400 天"""
    rows = cache["px"].get(code)
    if rows and rows[-1]["date"] >= today:
        return rows
    if not rows or rows[-1]["date"] < prev_weekday(prev_weekday(today)):       # 斷了 2 天以上（或沒有）→ 重抓
        if budget["n"] <= 0:
            return None
        start = (datetime.strptime(today, "%Y-%m-%d") - timedelta(days=400)).strftime("%Y-%m-%d")
        got = fm("TaiwanStockPrice", code, start)
        budget["n"] -= 1
        if not got:
            return None
        rows = [{k: r.get(k) for k in ("date", "open", "max", "min", "close", "spread", "Trading_Volume")} for r in got]
    rows = S.with_today(rows, q, today)
    cache["px"][code] = rows[-KEEP:]
    return cache["px"][code]


def latest_avail_quarter(today):
    """today 已經可以使用的最新一季季底（Q1～Q3 +45 天、Q4 +90 天）"""
    d = datetime.strptime(today, "%Y-%m-%d")
    for y in (d.year, d.year - 1):
        for m in (12, 9, 6, 3):
            qend = f"{y}-{m:02d}-{30 if m in (6, 9) else 31}"
            if X._avail(qend) <= today:
                return qend
    return None


def fund_rows(cache, kind, code, today, budget):
    """財報（EPS／資產負債）：已有今天可用的最新一季就不抓；沒有就每 7 天最多試一次"""
    e = cache[kind].get(code)
    need_q = latest_avail_quarter(today)
    if e and (max((r[0] for r in e["rows"]), default="") >= need_q[:7] or
              (datetime.strptime(today, "%Y-%m-%d") - datetime.strptime(e["t"], "%Y-%m-%d")).days < 7):
        return e["rows"]
    if budget["n"] <= 0:
        return e["rows"] if e else None
    dataset = "TaiwanStockFinancialStatements" if kind == "eps" else "TaiwanStockBalanceSheet"
    got = fm(dataset, code, "2023-01-01")
    budget["n"] -= 1
    if got is None:
        return e["rows"] if e else None
    if kind == "eps":
        rows = [[d, v] for _, d, v in X.eps_series(got)]
    else:
        rows = []
        for avail, ratio in X.debt_series(got):
            q = next(r["date"][:10] for r in got if X._avail(r["date"][:10]) == avail)
            rows.append([q, ratio])
    cache[kind][code] = {"t": today, "rows": rows}
    return rows


def rev_list(code, today, budget):
    """[(可取得日, 年增%)]（用 revenue 模組的月營收快取，每支每月抓一次）"""
    if budget["n"] > 0 and revenue.need_fetch(code, today):
        budget["n"] -= 1
    revenue.ensure(code, today, lambda ds, c, st: fm(ds, c, st))
    m = (revenue._cache.get(code) or {}).get("m", {})
    out = []
    for k in sorted(m):
        y, mo = int(k[:4]), int(k[5:])
        ly = m.get(f"{y - 1}-{mo:02d}")
        if ly:
            ay, am = revenue._shift(y, mo, 1)
            out.append((f"{ay}-{am:02d}-11", (m[k] / ly - 1) * 100))
    return out


# ── Supabase：推薦紀錄與成績 ─────────────────────────────
def _sb():
    return {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}", "Content-Type": "application/json",
            "Prefer": "return=minimal"}


def load_store(key, limit=200):
    if not SUPABASE_URL or not SUPABASE_KEY:
        return []
    try:
        r = requests.get(f"{SUPABASE_URL}/rest/v1/analysis_store",
                         params={"select": "date,payload", "key": f"eq.{key}", "order": "date.desc", "limit": str(limit)},
                         headers=_sb(), timeout=20)
        return r.json() if r.status_code == 200 else []
    except Exception:
        return []


def save_store(key, day, payload):
    if not SUPABASE_URL or not SUPABASE_KEY:
        return False
    url = f"{SUPABASE_URL}/rest/v1/analysis_store"
    try:
        requests.delete(url, params={"key": f"eq.{key}", "date": f"eq.{day}"}, headers=_sb(), timeout=15)
        r = requests.post(url, json={"key": key, "date": day, "payload": payload}, headers=_sb(), timeout=30)
        return r.status_code in (200, 201)
    except Exception:
        return False


def evaluate(picks, today, budget, bench_rows=None):
    """
    已滿 60 個交易日的推薦 → 實際成績（隔天收盤買、抱 60 天；期間收盤跌破買價 8% 就當天收盤出場）
    picks: [{date, code, name, price}]；回傳 [{..., ret, bench, stopped, exit_date}]
    """
    out = []
    bench = None
    for p in picks:
        if (datetime.strptime(today, "%Y-%m-%d") - datetime.strptime(p["date"], "%Y-%m-%d")).days < 92:
            continue
        if budget["n"] <= 0:
            break
        rows = fm("TaiwanStockPrice", p["code"], p["date"])
        budget["n"] -= 1
        seq = S.C.ohlc_rows(rows or [])
        if len(seq) < HOLD + 2:
            continue
        lv = [x["lv"] for x in seq]
        entry = lv[1]
        exit_i, stopped = HOLD + 1, False
        for t in range(2, HOLD + 2):
            if lv[t] <= entry * (1 - STOP):
                exit_i, stopped = t, True
                break
        if bench is None:
            bench = {x["d"]: x["lv"] for x in S.C.ohlc_rows(bench_rows if bench_rows is not None else
                                                             (fm("TaiwanStockPrice", "0050", min(q["date"] for q in picks)) or []))}
        b0, b1 = bench.get(seq[1]["d"]), bench.get(seq[exit_i]["d"])
        out.append({**p, "entry_date": seq[1]["d"], "exit_date": seq[exit_i]["d"], "stopped": stopped,
                    "ret": round((lv[exit_i] / entry - 1) * 100 - 0.5, 2),
                    "bench": round((b1 / b0 - 1) * 100, 2) if b0 and b1 else None})
    return out


# ── 主流程 ────────────────────────────────────────────
def run(quotes=None, yields=None, cache=None, today=None, send=True, picks_hist=None, results_hist=None):
    today = today or S.today_tw()
    quotes = quotes if quotes is not None else S.fetch_quotes()
    if not quotes:
        log("❌ 抓不到今日行情")
        notify.record("div", {"error": "no_quotes"})
        return None
    yields = yields if yields is not None else fetch_yield()
    own_cache = cache is None
    cache = cache if cache is not None else load_cache()
    revenue.load()
    budget = {"n": MAX_FETCH}
    cands = sorted(c for c, q in quotes.items() if q["c"] >= 10 and (yields.get(c, (0, 0))[1] or 0) >= 5)
    log(f"殖利率 ≥ 5%、股價 ≥ 10 的有 {len(cands)} 支（日K快取 {sum(1 for c in cands if c in cache['px'])} 支）")
    passed_px, picks = [], []
    for code in cands:
        rows = price_rows(cache, code, quotes[code], today, budget)
        if not rows or rows[-1]["date"] != today:
            continue
        pe, y = yields[code]
        a = X.arrays(rows, [{"date": today, "PER": pe, "dividend_yield": y}])
        i = a["n"] - 1
        if not X.sig_div_pre(a, i):
            continue
        passed_px.append(code)
        eps = fund_rows(cache, "eps", code, today, budget)
        debt = fund_rows(cache, "debt", code, today, budget)
        rev = rev_list(code, today, budget)
        if eps is None or debt is None:
            continue
        fd = X.fund_at(today, [(X._avail(d), d, v) for d, v in eps], [(X._avail(d), r) for d, r in debt], rev)
        if X.sig_div(a, i, fd):
            q = quotes[code]
            picks.append({"date": today, "code": code, "name": q["name"] or code, "price": q["c"], "yld": round(y, 2),
                          "rev3": round(fd["rev3"], 1), "eps_yoy": round(min(fd["eps_yoy"], 999), 1),
                          "debt": round(fd["debt"], 1), "stop": round(q["c"] * (1 - STOP), 2)})
    if own_cache:
        save_cache(cache)
        revenue.save()
    log(f"價量條件通過 {len(passed_px)} 支 → 全部條件 {len(picks)} 支；FinMind 用了 {MAX_FETCH - budget['n']} 次")

    # 新推薦（4 週內推過的不重複推）
    hist = picks_hist if picks_hist is not None else load_store("div_picks", 60)
    cut = (datetime.strptime(today, "%Y-%m-%d") - timedelta(days=COOLDOWN_DAYS)).strftime("%Y-%m-%d")
    recent = {p["code"] for row in hist if row["date"] >= cut and row["date"] != today
              for p in (row.get("payload") or {}).get("picks", [])}
    new = [p for p in picks if p["code"] not in recent]
    if new:
        save_store("div_picks", today, {"picks": new})

    # 實際成績（已滿 60 天的推薦）
    res_rows = results_hist if results_hist is not None else load_store("div_results", 1)
    done = (res_rows[0]["payload"] if res_rows else {}).get("results", [])
    done_keys = {(r["date"], r["code"]) for r in done}
    all_picks = [p for row in hist for p in (row.get("payload") or {}).get("picks", [])]
    todo = [p for p in all_picks if (p["date"], p["code"]) not in done_keys]
    fresh = evaluate(todo, today, budget) if todo else []
    if fresh:
        done = done + fresh
        save_store("div_results", today, {"results": done})
    summary = summarize(done)

    msg = compose(today, new, picks, summary, fresh)
    sent = notify.send(msg) if (send and (new or fresh)) else False
    notify.record("div", {"picks": len(picks), "new": len(new), "names": [f"{p['name']}({p['code']})" for p in new[:8]],
                          "sent": sent, "track": summary})
    return {"picks": picks, "new": new, "summary": summary, "fresh": fresh, "msg": msg}


def summarize(results):
    rs = [r for r in results if r.get("ret") is not None]
    if not rs:
        return {"n": 0}
    b = [r["bench"] for r in rs if r.get("bench") is not None]
    return {"n": len(rs), "win": round(sum(1 for r in rs if r["ret"] > 0) / len(rs) * 100, 1),
            "avg": round(sum(r["ret"] for r in rs) / len(rs), 2),
            "bench": round(sum(b) / len(b), 2) if b else None,
            "beat": round(sum(1 for r in rs if r.get("bench") is not None and r["ret"] > r["bench"]) / len(rs) * 100, 1),
            "stopped": sum(1 for r in rs if r.get("stopped"))}


def compose(today, new, picks, summary, fresh):
    e = notify.esc
    L = [f"🏦 <b>存股＋營運動能</b>（{today[5:].replace('-', '/')} 收盤後）",
         "條件：殖利率≥5%、近四季有賺、負債≤60%、低波動、近一年高點附近、營收與EPS年增≥10%"]
    if new:
        L.append("")
        for p in new[:10]:
            L.append(f"• <b>{e(p['name'])}</b> {p['code']} 收 {p['price']}｜殖利率 {p['yld']}%｜營收年增 {p['rev3']}%"
                     f"｜EPS年增 {str(p['eps_yoy']) + '%' if p['eps_yoy'] < 999 else '轉盈'}｜負債 {p['debt']}%")
            L.append(f"　停損 {p['stop']}（−8%）")
        L.append("　隔天可買；抱約 3 個月（60 個交易日），收盤跌破停損就出場；不用融資")
    elif picks:
        L.append(f"今天符合 {len(picks)} 檔，但 4 週內都推過了")
    if fresh:
        L += ["", f"📒 <b>到期成績</b>（今天新增 {len(fresh)} 筆）"]
        for r in fresh[:6]:
            L.append(f"• {e(r['name'])} {r['date'][5:]} 推薦 → {r['ret']:+.1f}%（0050 {r['bench'] if r['bench'] is not None else '—'}%）"
                     f"{'，停損出場' if r['stopped'] else ''}")
    if summary.get("n"):
        L.append(f"累計 {summary['n']} 筆：勝率 {summary['win']}%、平均 {summary['avg']:+}%（同期 0050 {summary['bench']}%）、"
                 f"贏 0050 {summary['beat']}%")
    L += ["", "⚠️ 回測抱 60 天勝率 66%，但 2025 年後優勢變小、樣本少；僅供參考"]
    return "\n".join(L)


if __name__ == "__main__":
    run()
