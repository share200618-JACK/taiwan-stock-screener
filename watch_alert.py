"""
追蹤清單提醒（每日排程，收盤後跑；結果用 Telegram 通知）

① 價格提醒：在追蹤頁替個股設「跌到 / 漲到」價位，今天盤中最低／最高碰到就通知
   （每個價位只通知一次；想再收到就到追蹤頁重新設定）
② 買進訊號：追蹤股今天出現下列任一種
   - 改良版條件（與每日「📣 買進訊號」同一套：6 條件＋本益比≤25、不追漲停、不追急漲）
   - 存股＋營運動能（div_alert 今天的入選名單）
   - AI 推薦前 5 名
   - 月營收創 12 個月新高且年增 ≥ 20%（每個月份只通知一次）
   同一支、同一種訊號 10 天內不重複通知

設定：analysis_store key=watch_alerts  payload {"alerts": {code: {below, above, below_hit, above_hit}}}
紀錄：analysis_store key=watch_log     payload {"sent": {code: {訊號: 日期或月份}}, "recent": [...]}
"""
import os
from datetime import datetime, timedelta

import requests

import notify
import revenue
import signal_alert as S

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")
FINMIND_TOKEN = os.environ.get("FINMIND_TOKEN", "").strip()
REPEAT_DAYS = 10
KEEP_RECENT = 120
SIG_NAME = {"broker": "改良版條件", "div": "存股＋營運動能", "ai": "AI 前5名", "rev": "營收創新高",
            "below": "跌到提醒價", "above": "漲到提醒價"}


def log(m):
    S.log(m)


# ── Supabase ──────────────────────────────────────────
def _h():
    return {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}", "Content-Type": "application/json",
            "Prefer": "return=minimal"}


def load_watchlist(token="default"):
    if not SUPABASE_URL or not SUPABASE_KEY:
        return []
    try:
        r = requests.get(f"{SUPABASE_URL}/rest/v1/watchlist",
                         params={"select": "code,name,add_price", "user_token": f"eq.{token}", "limit": "300"},
                         headers=_h(), timeout=15)
        return r.json() if r.status_code == 200 else []
    except Exception as e:
        log(f"⚠️ 讀取追蹤清單失敗：{e}")
        return []


def load_store(key):
    if not SUPABASE_URL or not SUPABASE_KEY:
        return {}
    try:
        r = requests.get(f"{SUPABASE_URL}/rest/v1/analysis_store",
                         params={"select": "payload", "key": f"eq.{key}", "order": "date.desc", "limit": "1"},
                         headers=_h(), timeout=15)
        rows = r.json() if r.status_code == 200 else []
        return (rows[0].get("payload") or {}) if rows else {}
    except Exception:
        return {}


def save_store(key, day, payload):
    """一個 key 只留一列（先刪再寫）"""
    if not SUPABASE_URL or not SUPABASE_KEY:
        return False
    url = f"{SUPABASE_URL}/rest/v1/analysis_store"
    try:
        requests.delete(url, params={"key": f"eq.{key}"}, headers=_h(), timeout=15)
        r = requests.post(url, json={"key": key, "date": day, "payload": payload}, headers=_h(), timeout=15)
        return r.status_code in (200, 201, 204)
    except Exception as e:
        log(f"⚠️ 寫入 {key} 失敗：{e}")
        return False


# ── 判斷 ──────────────────────────────────────────────
def price_hits(code, q, a, today):
    """回傳 [(訊號, 說明)]，並把觸發日寫回 a"""
    out = []
    if not a or not q:
        return out
    below, above = a.get("below"), a.get("above")
    if below and not a.get("below_hit") and q["l"] <= below:
        a["below_hit"] = today
        out.append(("below", f"📉 跌到你設的 {below:g}（今天最低 {q['l']:g}、收 {q['c']:g}）"))
    if above and not a.get("above_hit") and q["h"] >= above:
        a["above_hit"] = today
        out.append(("above", f"📈 漲到你設的 {above:g}（今天最高 {q['h']:g}、收 {q['c']:g}）"))
    return out


def recently_sent(sent, code, sig, today):
    last = (sent.get(code) or {}).get(sig)
    if not last:
        return False
    try:
        return (datetime.strptime(today, "%Y-%m-%d") - datetime.strptime(last, "%Y-%m-%d")).days < REPEAT_DAYS
    except ValueError:
        return False


def buy_signals(code, q, rows, pe, ai_rank, div_codes, rev):
    """回傳 [(訊號, 說明, 去重用的值 or None)]"""
    out = []
    if rows:
        try:
            b = S.broker_signal(rows, pe)
        except Exception as e:
            log(f"⚠️ {code} 改良版條件計算失敗：{e}")
            b = None
        if b and b["ok"]:
            out.append(("broker", f"🟢 改良版條件買進訊號（本益比 {b['pe']:.1f}）｜停損 {b['stop']}（−{S.STOP_PCT}%）", None))
    if code in div_codes:
        out.append(("div", "🏦 入選存股＋營運動能（殖利率≥5%、營運成長；抱 60 天、停損 8%）", None))
    if code in ai_rank:
        r = ai_rank[code]
        out.append(("ai", f"🤖 AI 推薦第 {r['rank']} 名（上漲機率 {r.get('rise_prob')}%）", None))
    if rev and rev.get("rev_hot"):
        out.append(("rev", f"📈 {int(rev['rev_month'][5:])} 月營收創 12 個月新高、年增 {rev['rev_yoy']}%", rev["rev_month"]))
    return out


def compose(today, items):
    e = notify.esc
    L = [f"👁️ <b>追蹤股提醒</b>（{today[5:].replace('-', '/')} 收盤後）"]
    for it in items:
        q = it["q"]
        L += ["", f"• <b>{e(it['name'])}</b> {it['code']} 收 {q['c']:g}（{q['pct']:+.1f}%）"]
        L += [f"　{t}" for _, t in it["hits"]]
    if any(s in ("broker", "div", "ai", "rev") for it in items for s, _ in it["hits"]):
        L += ["", "⚠️ 訊號僅供參考，請自行判斷、控制部位"]
    if any(s in ("below", "above") for it in items for s, _ in it["hits"]):
        L.append("🔔 價格提醒每個價位只通知一次，要再提醒請到追蹤頁重新設定")
    return "\n".join(L)


def _fm_rev(dataset, code, start):
    """月營收（追蹤股才抓，很少）"""
    if not FINMIND_TOKEN:
        return None
    try:
        r = S.SESSION.get("https://api.finmindtrade.com/api/v4/data",
                          params={"dataset": dataset, "data_id": code, "start_date": start},
                          headers={"Authorization": f"Bearer {FINMIND_TOKEN}"}, timeout=30)
        return r.json().get("data") or [] if r.status_code == 200 else None
    except Exception:
        return None


# ── 主流程 ────────────────────────────────────────────
def run(watch=None, alerts=None, wlog=None, quotes=None, pe=None, cache=None, fetch=S.fm_price, status=None,
        rev_fetch=_fm_rev, today=None, send=True, save=True):
    today = today or S.today_tw()
    watch = watch if watch is not None else load_watchlist()
    if not watch:
        log("追蹤清單是空的，結束")
        notify.record("watch", {"n": 0})
        return None
    codes = list(dict.fromkeys(str(w.get("code", "")).strip() for w in watch if w.get("code")))
    names = {str(w.get("code")): w.get("name") or "" for w in watch}
    quotes = quotes if quotes is not None else S.fetch_quotes(extra=codes)
    if not quotes:
        log("❌ 抓不到今日行情，結束")
        notify.record("watch", {"n": len(codes), "error": "no_quotes"})
        return None
    pe = pe if pe is not None else S.fetch_pe()
    cache = cache if cache is not None else S.load_price_cache()
    status = status if status is not None else notify.load_status()
    alerts_p = alerts if alerts is not None else load_store("watch_alerts")
    wlog = wlog if wlog is not None else load_store("watch_log")
    amap = alerts_p.setdefault("alerts", {})
    sent = wlog.setdefault("sent", {})
    recent = wlog.setdefault("recent", [])

    ai_rank = {str(s.get("code")): {"rank": i, **s} for i, s in enumerate((status.get("ai") or {}).get("top5") or [], 1)}
    div_codes = set((status.get("div") or {}).get("codes") or [])
    revenue.load()

    items, alert_changed, n_fm = [], False, 0
    for code in codes:
        q = quotes.get(code)
        if not q:
            continue
        hits = []
        for sig, txt in price_hits(code, q, amap.get(code), today):
            hits.append((sig, txt))
            alert_changed = True
        rows = cache.get(code)
        if not rows and len(code) == 4 and code.isdigit():
            rows = fetch(code)
            n_fm += 1
        rows = S.with_today(rows, q, today) if rows else rows
        if rows and rows[-1]["date"][:10] != today:
            rows = None
        rev = None
        if len(code) == 4 and code.isdigit():
            revenue.ensure(code, today, rev_fetch)
            rev = revenue.status(code, today)
        for sig, txt, key in buy_signals(code, q, rows, pe.get(code), ai_rank, div_codes, rev):
            if key is not None:
                if (sent.get(code) or {}).get(sig) == key:
                    continue
            elif recently_sent(sent, code, sig, today):
                continue
            hits.append((sig, txt))
            sent.setdefault(code, {})[sig] = key if key is not None else today
        if hits:
            items.append({"code": code, "name": names.get(code) or q.get("name") or code, "q": q, "hits": hits})

    for it in items:
        for sig, txt in it["hits"]:
            recent.insert(0, {"date": today, "code": it["code"], "name": it["name"], "sig": sig, "text": txt})
    del recent[KEEP_RECENT:]
    wlog["sent"] = {c: v for c, v in sent.items() if c in codes}
    if save:
        if alert_changed:
            save_store("watch_alerts", today, alerts_p)
        if items:
            save_store("watch_log", today, wlog)
        revenue.save()

    msg = compose(today, items) if items else ""
    sent_ok = notify.send(msg) if (send and msg) else False
    n_price = sum(1 for it in items for s, _ in it["hits"] if s in ("below", "above"))
    n_buy = sum(1 for it in items for s, _ in it["hits"] if s not in ("below", "above"))
    log(f"追蹤 {len(codes)} 支、有價格提醒設定 {sum(1 for c in codes if amap.get(c))} 支；"
        f"今天通知 {len(items)} 支（價格 {n_price}、買進訊號 {n_buy}）；FinMind 補抓 {n_fm} 支")
    notify.record("watch", {"n": len(codes), "hits": len(items), "price": n_price, "buy": n_buy, "sent": sent_ok,
                            "names": [f"{it['name']}({it['code']})" for it in items[:8]]})
    return {"items": items, "msg": msg, "alerts": alerts_p, "log": wlog}


if __name__ == "__main__":
    run()
