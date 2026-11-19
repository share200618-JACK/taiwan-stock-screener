"""
個股融資維持率 < 130%（估計）每日名單
=====================================
交易所沒有公布「個股」融資維持率，這裡用公開資料估計：
  每天的融資買進用當天收盤價計成本，融資賣出／現金償還按原本的平均成本減少 → 得到「融資平均成本」
  估計維持率 ＝ 收盤價 ÷（融資平均成本 × 融資成數）× 100%
  融資成數：上市 6 成、上櫃 5 成（個別股票或券商可能更低，所以只是估計）
  起算：往前 130 個交易日（融資期限半年），之前就有的部位用當天收盤價當成本

資料（全部免費、不用 FinMind）：證交所／櫃買中心每日「融資融券餘額」與「收盤行情」（依日期查詢）
狀態存在 margin_state.json（GitHub Actions cache）；每天只補新的交易日，第一次會回補 130 天（約 25 分鐘）。
每天晚上 21:30 執行（融資資料晚上才公布），名單推 Telegram、存 Supabase（analysis_store, key=margin_watch）。
"""
import os
import re
import json
import time
from datetime import datetime, timedelta

import requests

import notify

STATE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "margin_state.json")
SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")
LOAN = {"twse": float(os.environ.get("LOAN_TWSE", "0.6")), "tpex": float(os.environ.get("LOAN_TPEX", "0.5"))}
SEED_DAYS = int(os.environ.get("MARGIN_SEED_DAYS", "130"))
MIN_BAL = int(os.environ.get("MARGIN_MIN_BAL", "300"))        # 融資餘額至少幾張才列入
THRESH = 130.0
H = {"User-Agent": "Mozilla/5.0"}
GAP = {"twse": 3.0, "tpex": 2.0}                                # 證交所連太快會被擋
_last = {"twse": 0.0, "tpex": 0.0}


def log(m):
    print(f"[{datetime.utcnow() + timedelta(hours=8):%H:%M:%S}] {m}", flush=True)


def _num(x):
    try:
        return float(str(x).replace(",", "").replace("+", "").strip() or 0)
    except ValueError:
        return None


def get(src, url, params):
    for k in range(4):
        wait = GAP[src] - (time.time() - _last[src])
        if wait > 0:
            time.sleep(wait)
        _last[src] = time.time()
        try:
            r = requests.get(url, params=params, headers=H, timeout=40)
            if r.status_code == 200:
                return r.json()
            log(f"⚠️ {src} {r.status_code}，重試")
        except Exception as e:
            log(f"⚠️ {src} {e}，重試")
        time.sleep(10 * (k + 1))
    return None


# ── 資料 ──────────────────────────────────────────────
def trading_days(end, n):
    """證交所每月成交資訊 → 最近 n 個交易日（含 end 之前）"""
    out, d = [], datetime.strptime(end, "%Y-%m-%d").replace(day=1)
    while len(out) < n + 5:
        j = get("twse", "https://www.twse.com.tw/rwd/zh/afterTrading/FMTQIK",
                {"date": d.strftime("%Y%m01"), "response": "json"}) or {}
        for row in j.get("data") or []:
            y, m, dd = row[0].split("/")
            day = f"{int(y) + 1911}-{m}-{dd}"
            if day <= end:
                out.append(day)
        d = (d - timedelta(days=1)).replace(day=1)
        if d.year < 2000:
            break
    return sorted(set(out))[-n:]


def _table(j, must):
    for t in (j or {}).get("tables") or []:
        f = t.get("fields") or []
        if all(any(m in x for x in f) for m in must):
            return t
    return None


def fetch_day(day):
    """{代號: {"m": "twse"/"tpex", "name", "buy", "sell", "repay", "prev", "bal", "close"}}；資料還沒出 → {}"""
    ymd, slash = day.replace("-", ""), day.replace("-", "/")
    out = {}
    j = get("twse", "https://www.twse.com.tw/rwd/zh/marginTrading/MI_MARGN",
            {"date": ymd, "selectType": "ALL", "response": "json"})
    t = _table(j, ["代號", "今日餘額"])
    if t and str(j.get("date")) == ymd:
        for r in t["data"]:
            if re.fullmatch(r"\d{4}", r[0]):
                out[r[0]] = {"m": "twse", "name": r[1].strip(), "buy": _num(r[2]), "sell": _num(r[3]),
                             "repay": _num(r[4]), "prev": _num(r[5]), "bal": _num(r[6])}
    j = get("twse", "https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX",
            {"date": ymd, "type": "ALLBUT0999", "response": "json"})
    t = _table(j, ["證券代號", "收盤價"])
    if t:
        ci = next(i for i, f in enumerate(t["fields"]) if "收盤價" in f)
        for r in t["data"]:
            if r[0] in out:
                out[r[0]]["close"] = _num(r[ci])
    tp = {}
    j = get("tpex", "https://www.tpex.org.tw/www/zh-tw/margin/balance", {"date": slash, "response": "json"})
    t = _table(j, ["代號", "資餘額"])
    if t and str(j.get("date")) == ymd:
        for r in t["data"]:
            if re.fullmatch(r"\d{4}", r[0]):
                tp[r[0]] = {"m": "tpex", "name": r[1].strip(), "prev": _num(r[2]), "buy": _num(r[3]), "sell": _num(r[4]),
                            "repay": _num(r[5]), "bal": _num(r[6])}
    j = get("tpex", "https://www.tpex.org.tw/www/zh-tw/afterTrading/dailyQuotes", {"date": slash, "response": "json"})
    t = _table(j, ["代號", "收盤"])
    if t and str(j.get("date")) == ymd:
        for r in t["data"]:
            if r[0] in tp:
                tp[r[0]]["close"] = _num(r[2])
    out.update(tp)
    return out


def fetch_summary(day):
    """
    證交所公布的「融資金額」總額（元）與未追蹤的 ETF（6 碼）融資市值，用來校正估計的平均成本
    （回補只從 130 天前開始，更早買進、成本較低的部位會被高估成本 → 維持率被低估）
    """
    ymd = day.replace("-", "")
    j = get("twse", "https://www.twse.com.tw/rwd/zh/marginTrading/MI_MARGN",
            {"date": ymd, "selectType": "ALL", "response": "json"})
    if not j or str(j.get("date")) != ymd:
        return None
    t0 = _table(j, ["項目", "今日餘額"])
    loan = None
    for r in (t0 or {}).get("data", []):
        if str(r[0]).startswith("融資金額"):
            loan = (_num(r[5]) or 0) * 1000
    t = _table(j, ["代號", "今日餘額"])
    other = {r[0]: _num(r[6]) or 0 for r in (t or {}).get("data", []) if not re.fullmatch(r"\d{4}", r[0])}
    jp = get("twse", "https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX",
             {"date": ymd, "type": "ALLBUT0999", "response": "json"})
    tp = _table(jp, ["證券代號", "收盤價"])
    val = 0.0
    if tp:
        ci = next(i for i, f in enumerate(tp["fields"]) if "收盤價" in f)
        for r in tp["data"]:
            if r[0] in other:
                val += other[r[0]] * 1000 * (_num(r[ci]) or 0)
    return {"loan": loan, "other_val": val} if loan else None


def calibration(state, summ):
    """k ＝ 證交所融資金額（扣掉未追蹤 ETF）÷ 估計的融資金額；之後成本 × k"""
    if not summ:
        return 1.0
    implied = sum(v[0] * 1000 * v[1] * LOAN["twse"] for v in state.get("s", {}).values() if v[3] == "twse" and v[1] > 0)
    target = summ["loan"] - LOAN["twse"] * summ["other_val"]
    if implied <= 0 or target <= 0:
        return 1.0
    return max(0.5, min(1.5, target / implied))


# ── 平均成本 ──────────────────────────────────────────
def update(state, day, data):
    """把一天的融資變化套進狀態：s[代號] = [餘額, 平均成本, 名稱, 市場, 前收盤]"""
    s = state.setdefault("s", {})
    for code, x in data.items():
        px, bal = x.get("close"), x.get("bal")
        if bal is None:
            continue
        cur = s.get(code)
        if not px or px <= 0:                                  # 沒成交：只更新餘額
            if cur:
                cur[0] = bal
            continue
        if not cur or cur[1] <= 0 or (cur[4] and abs(px / cur[4] - 1) > 0.4):     # 新股票或減資／分割 → 重新起算
            s[code] = [bal, px, x["name"], x["m"], px]
            continue
        prev_bal, cost = cur[0], cur[1]
        keep = max(prev_bal - (x.get("sell") or 0) - (x.get("repay") or 0), 0.0)
        buy = x.get("buy") or 0
        if keep + buy > 0:
            cost = (keep * cost + buy * px) / (keep + buy)
        s[code] = [bal, cost, x["name"], x["m"], px]
    state["date"] = day
    return state


def ratios(state, min_bal=MIN_BAL, k=None):
    k = state.get("k", 1.0) if k is None else k
    out = []
    for code, (bal, cost, name, m, px) in state.get("s", {}).items():
        if bal < min_bal or cost <= 0 or not px or code.startswith("0"):          # 0 開頭是 ETF，不列
            continue
        c = cost * k
        out.append({"code": code, "name": name, "m": m, "close": px, "bal": int(bal), "cost": round(c, 2),
                    "ratio": round(px / (c * LOAN[m]) * 100, 1),
                    "call_px": round(c * LOAN[m] * 1.3, 2)})
    return sorted(out, key=lambda r: r["ratio"])


# ── 主流程 ────────────────────────────────────────────
def load_state():
    try:
        with open(STATE_FILE, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def save_state(state):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f)


def save_store(day, payload):
    if not SUPABASE_URL or not SUPABASE_KEY:
        return
    h = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}", "Content-Type": "application/json",
         "Prefer": "return=minimal"}
    url = f"{SUPABASE_URL}/rest/v1/analysis_store"
    try:
        requests.delete(url, params={"key": "eq.margin_watch", "date": f"eq.{day}"}, headers=h, timeout=15)
        requests.post(url, json={"key": "margin_watch", "date": day, "payload": payload}, headers=h, timeout=30)
    except Exception:
        pass


def run(today=None, fetch=fetch_day, days_fn=trading_days, send=True, summary_fn=fetch_summary):
    today = today or (datetime.utcnow() + timedelta(hours=8)).strftime("%Y-%m-%d")
    state = load_state()
    days = days_fn(today, SEED_DAYS)
    todo = [d for d in days if d > state.get("date", "")]
    if not state.get("s"):
        log(f"第一次執行：回補 {len(todo)} 個交易日（{todo[0] if todo else ''} 起）")
    prev_bal = {c: v[0] for c, v in state.get("s", {}).items()}
    last_ok = state.get("date")
    for k, d in enumerate(todo):
        data = fetch(d)
        if not data:
            log(f"   {d} 資料還沒公布，停在 {last_ok}")
            break
        prev_bal = {c: v[0] for c, v in state.get("s", {}).items()}      # 最後一個有資料的交易日前的餘額（算今日增減）
        update(state, d, data)
        last_ok = d
        if (k + 1) % 10 == 0:
            log(f"   已處理 {k + 1}/{len(todo)} 天（{d}）")
            save_state(state)
    save_state(state)
    if not last_ok:
        log("❌ 沒有任何融資資料")
        notify.record("margin", {"error": "no_data"})
        return None
    summ = summary_fn(last_ok)
    k = calibration(state, summ)
    if summ:
        state["k"] = k
        save_state(state)
        log(f"校正：證交所融資金額 {summ['loan'] / 1e8:,.0f} 億（未追蹤 ETF 約 {LOAN['twse'] * summ['other_val'] / 1e8:,.0f} 億），"
            f"估計成本 × {k:.3f}")
    else:
        k = state.get("k", 1.0)
        log(f"⚠️ 抓不到證交所融資金額，沿用上次校正 × {k:.3f}")
    rows = ratios(state, k=k)
    low = [r for r in rows if r["ratio"] < THRESH]
    warn = [r for r in rows if THRESH <= r["ratio"] < 140]
    for r in low:
        r["chg"] = int(r["bal"] - prev_bal.get(r["code"], r["bal"]))
    log(f"資料日 {last_ok}：融資餘額 ≥ {MIN_BAL} 張 {len(rows)} 檔；維持率 < 130% {len(low)} 檔、130～140% {len(warn)} 檔")
    every = {r["code"]: [r["ratio"], r["call_px"], r["bal"]] for r in ratios(state, min_bal=1, k=k)}   # 網頁個股卡片用
    payload = {"date": last_ok, "low": low, "warn": [{k: r[k] for k in ("code", "name", "ratio")} for r in warn],
               "n_all": len(rows), "loan": LOAN, "all": every, "k": round(k, 3)}
    save_store(last_ok, payload)
    msg = compose(last_ok, low, warn, len(rows), today)
    sent = notify.send(msg) if send else False
    notify.record("margin", {"date": last_ok, "low": len(low), "warn": len(warn), "sent": sent})
    return {"date": last_ok, "low": low, "warn": warn, "msg": msg}


def compose(day, low, warn, n_all, today):
    e = notify.esc
    L = [f"🧯 <b>個股融資維持率 &lt; 130%</b>（估計，{day[5:].replace('-', '/')}）"
         + ("" if day == today else "　⚠️ 今天的融資資料還沒公布，這是前一個交易日")]
    if low:
        for r in low[:30]:
            chg = r.get("chg", 0)
            L.append(f"• <b>{e(r['name'])}</b> {r['code']}｜<b>{r['ratio']}%</b>｜收 {r['close']}｜融資均價 {r['cost']}"
                     f"｜餘額 {r['bal']:,} 張（{'+' if chg >= 0 else ''}{chg:,}）")
        if len(low) > 30:
            L.append(f"…另有 {len(low) - 30} 檔")
    else:
        L.append("今天沒有（融資餘額 300 張以上的股票）")
    L.append(f"130～140%（接近追繳）{len(warn)} 檔；共檢查 {n_all} 檔（成本已用證交所融資總金額校正）")
    L += ["", "說明：維持率 ＝ 收盤 ÷（融資平均成本 × 融資成數），上市 6 成、上櫃 5 成；低於 130% 代表很多融資戶被追繳，"
              "2 天內沒補錢可能被斷頭賣出，短線賣壓大。這是估計值，個別券商、個別股票成數可能不同。"]
    return "\n".join(L)


if __name__ == "__main__":
    run()
