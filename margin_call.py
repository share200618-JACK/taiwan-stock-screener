"""
融資追繳／斷頭統計（證交所「臺股儀表板」官方數據：全市場、整戶合併計算）

欄位（BFIJ3U）
- 追繳戶數 belowAccNum：截至當日，整戶擔保維持率 < 130% 的戶數（儀表板上的「追繳戶數」）
- 當日通知追繳 callAccNum／追繳金額 callAmt：當天券商發出追繳通知的戶數／應補繳金額
- 處分戶數／金額 exeAccNum／exeAmt：沒在期限內補足，「下一個營業日」要被券商賣掉（斷頭）的戶數／金額
- 全市場擔保維持率 keepRate
時程：D 日收盤後通知追繳 → D+2 收盤前要補 → 沒補、整戶仍 < 130% → D+2 列入處分 → D+3 開盤斷頭
（追繳紀錄還沒解除的戶，之後只要再跌破 130% 沒補，下一個營業日就可能被處分）

未來一週斷頭時程（資料日 T）
- T+1：處分戶數（官方已確定）
- T+2：T-1 通知追繳、T+1 收盤前沒補的戶（最多）
- T+3：T 通知追繳、T+2 收盤前沒補的戶（最多）
- 之後：要看 T+1 之後的新追繳
「最多」之外，再用過去大量追繳（≥300 戶）後實際被處分的比例，分「續跌／反彈」估計

模式：MCALL_MODE=push（早上，有壓力才推 Telegram）／report（每日排程，只記錄給日報）
"""
import os
import sys
import time
import statistics
from datetime import datetime, timedelta

import requests

import notify

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")
MODE = os.environ.get("MCALL_MODE", "push")
FORCE = os.environ.get("MCALL_FORCE", "") == "1"
TREND_URL = "https://www.twse.com.tw/rwd/zh/marginTrading/BFIJ3U_TREND"
HOLIDAY_URL = "https://www.twse.com.tw/rwd/zh/holidaySchedule/holidaySchedule"
H = {"User-Agent": "Mozilla/5.0"}
WD = "一二三四五六日"
BIG_CALL = 300          # 算處分比例用：當日通知追繳 ≥ 300 戶才算「大量追繳」
# 有壓力才推播（早上）
PUSH_BELOW, PUSH_EXE, PUSH_KEEP = 500, 100, 165.0


def log(m):
    print(m, flush=True)


def today_tw():
    return (datetime.utcnow() + timedelta(hours=8)).strftime("%Y-%m-%d")


def iso(d8):
    return f"{d8[:4]}-{d8[4:6]}-{d8[6:8]}"


def md(d):
    """2026-11-23 → 11/23（一）"""
    dt = datetime.strptime(d, "%Y-%m-%d")
    return f"{dt.month}/{dt.day}（{WD[dt.weekday()]}）"


# ── 資料 ──────────────────────────────────────────────
def fetch_trend(days=60):
    for i in range(3):
        try:
            r = requests.get(TREND_URL, params={"response": "json", "days": days}, headers=H, timeout=30)
            j = r.json()
            if j.get("stat") == "OK" and j.get("data"):
                rows = []
                for x in j["data"]:
                    rows.append({"date": iso(str(x["date"])), "keep": float(x["keepRate"]),
                                 "below": int(x["belowAccNum"]), "call": int(x["callAccNum"]),
                                 "call_amt": float(x["callAmt"]), "exe": int(x["exeAccNum"]),
                                 "exe_amt": float(x["exeAmt"]), "credit_acc": int(x.get("marginAccNum") or 0)})
                return sorted(rows, key=lambda r: r["date"])
            log(f"⚠️ 追繳資料回應：{j.get('stat')}")
        except Exception as e:
            log(f"⚠️ 抓追繳資料失敗（第 {i + 1} 次）：{e}")
        time.sleep(5)
    return []


def fetch_holidays(today):
    """休市日（今年＋明年）；抓不到就只排除週末"""
    out = set()
    y = int(today[:4]) - 1911
    for qy in (y, y + 1):
        try:
            r = requests.get(HOLIDAY_URL, params={"response": "json", "queryYear": qy}, headers=H, timeout=20)
            for row in r.json().get("data") or []:
                d, name = str(row[0]), str(row[1])
                if "交易" in name:          # 「開始交易日」「最後交易日」是有交易的
                    continue
                if len(d) == 10 and d[4] == "-":
                    out.add(d)
        except Exception:
            pass
    return out


def next_trading_days(after, n, holidays=()):
    out, d = [], datetime.strptime(after, "%Y-%m-%d")
    while len(out) < n:
        d += timedelta(days=1)
        s = d.strftime("%Y-%m-%d")
        if d.weekday() < 5 and s not in holidays:
            out.append(s)
    return out


def prev_trading_day(day, holidays=()):
    d = datetime.strptime(day, "%Y-%m-%d")
    while True:
        d -= timedelta(days=1)
        s = d.strftime("%Y-%m-%d")
        if d.weekday() < 5 and s not in holidays:
            return s


# ── 分析 ──────────────────────────────────────────────
def conversion(rows):
    """大量追繳（D）→ D+2 列入處分的比例；依 D→D+2 維持率漲跌分「續跌／反彈」"""
    ev = []
    for i, r in enumerate(rows[:-2]):
        if r["call"] < BIG_CALL:
            continue
        e = rows[i + 2]
        ev.append({"date": r["date"], "call": r["call"], "exe": e["exe"], "ratio": round(e["exe"] / r["call"], 4),
                   "move": "up" if e["keep"] > r["keep"] else "down"})

    def summ(xs):
        if not xs:
            return None
        rs = [x["ratio"] for x in xs]
        return {"n": len(rs), "min": min(rs), "med": round(statistics.median(rs), 4), "max": max(rs)}
    return {"events": ev, "down": summ([x for x in ev if x["move"] == "down"]),
            "up": summ([x for x in ev if x["move"] == "up"]), "all": summ(ev)}


def schedule(rows, holidays=()):
    """未來一週（5 個交易日）斷頭時程"""
    t = rows[-1]
    days = next_trading_days(t["date"], 5, holidays)
    conv = conversion(rows)
    est = lambda n, k: round(n * conv[k]["med"]) if conv.get(k) else None
    out = [{"date": days[0], "kind": "exe", "n": t["exe"], "amt": t["exe_amt"], "src": t["date"]}]
    for k, src in ((1, rows[-2] if len(rows) >= 2 else None), (2, t)):
        if src:
            out.append({"date": days[k], "kind": "pending", "n": src["call"], "amt": src["call_amt"], "src": src["date"],
                        "deadline": days[k - 1], "est_down": est(src["call"], "down"), "est_up": est(src["call"], "up")})
    for k in (3, 4):
        out.append({"date": days[k], "kind": "unknown", "src": days[k - 3]})
    return out, conv


def build(rows, holidays=()):
    t, p = rows[-1], (rows[-2] if len(rows) >= 2 else None)
    sch, conv = schedule(rows, holidays)
    return {"date": t["date"], "keep": t["keep"], "keep_chg": round(t["keep"] - p["keep"], 2) if p else None,
            "below": t["below"], "below_prev": p["below"] if p else None, "call": t["call"], "call_amt": t["call_amt"],
            "exe": t["exe"], "exe_amt": t["exe_amt"], "credit_acc": t["credit_acc"],
            "trend": [{k: r[k] for k in ("date", "keep", "below", "call", "call_amt", "exe", "exe_amt")} for r in rows[-40:]],
            "schedule": sch, "conv": {k: conv[k] for k in ("down", "up", "all")}}


def level(keep):
    return "🔴" if keep < 150 else "🟠" if keep < 160 else "🟡" if keep < 170 else "🟢"


def yi(x):
    return f"{x / 1e8:,.2f} 億"


def compose(p):
    L = [f"🧯 <b>融資追繳／斷頭</b>（{md(p['date'])} 證交所數據）",
         f"{level(p['keep'])} 全市場擔保維持率 <b>{p['keep']:.2f}%</b>"
         + (f"（{p['keep_chg']:+.2f}）" if p.get("keep_chg") is not None else "") + "｜追繳線 130%",
         f"追繳 <b>{p['below']:,} 戶</b>" + (f"（前一日 {p['below_prev']:,}）" if p.get("below_prev") is not None else "")
         + f"｜當天通知 {p['call']:,} 戶、應補繳 {yi(p['call_amt'])}",
         "", "📅 <b>未來一週斷頭時程</b>"]
    for s in p["schedule"]:
        if s["kind"] == "exe":
            L.append(f"{md(s['date'])} 開盤斷頭 <b>{s['n']:,} 戶</b>、{yi(s['amt'])} ← 已確定")
        elif s["kind"] == "pending":
            est = ""
            if s.get("est_down") is not None or s.get("est_up") is not None:
                est = "｜估 " + "、".join(x for x in (
                    f"續跌約 {s['est_down']:,}" if s.get("est_down") is not None else "",
                    f"反彈約 {s['est_up']:,}" if s.get("est_up") is not None else "") if x)
            L.append(f"{md(s['date'])} 最多 {s['n']:,} 戶{est}（{md(s['src'])[:-3]} 追繳、{md(s['deadline'])[:-3]} 收盤前沒補）")
    unk = [s for s in p["schedule"] if s["kind"] == "unknown"]
    if unk:
        L.append(f"{md(unk[0]['date'])} 起：看 {md(unk[0]['src'])[:-3]} 之後的新追繳")
    c = p.get("conv") or {}
    if c.get("down") or c.get("up"):
        parts = []
        if c.get("down"):
            parts.append(f"續跌時 {c['down']['min'] * 100:.0f}–{c['down']['max'] * 100:.0f}%")
        if c.get("up"):
            parts.append(f"反彈時 {c['up']['min'] * 100:.0f}–{c['up']['max'] * 100:.0f}%")
        n = (c.get("all") or {}).get("n", 0)
        L.append(f"過去 {n} 次大量追繳後被斷頭的比例：{'、'.join(parts)}")
    L.append("※ 戶數≈人數（同一人在不同券商開戶算多戶）")
    return "\n".join(L)


def pressure(p):
    return p["below"] >= PUSH_BELOW or p["exe"] >= PUSH_EXE or p["keep"] < PUSH_KEEP


# ── Supabase ──────────────────────────────────────────
def _h():
    return {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}", "Content-Type": "application/json",
            "Prefer": "return=minimal"}


def load_latest():
    if not SUPABASE_URL or not SUPABASE_KEY:
        return {}
    try:
        r = requests.get(f"{SUPABASE_URL}/rest/v1/analysis_store",
                         params={"select": "payload", "key": "eq.margin_call", "order": "date.desc", "limit": "1"},
                         headers=_h(), timeout=15)
        rows = r.json() if r.status_code == 200 else []
        return (rows[0].get("payload") or {}) if rows else {}
    except Exception:
        return {}


def save(p):
    if not SUPABASE_URL or not SUPABASE_KEY:
        return False
    url = f"{SUPABASE_URL}/rest/v1/analysis_store"
    try:
        requests.delete(url, params={"key": "eq.margin_call", "date": f"eq.{p['date']}"}, headers=_h(), timeout=15)
        r = requests.post(url, json={"key": "margin_call", "date": p["date"], "payload": p}, headers=_h(), timeout=15)
        return r.status_code in (200, 201, 204)
    except Exception as e:
        log(f"⚠️ 儲存失敗：{e}")
        return False


# ── 主流程 ────────────────────────────────────────────
def run(mode=MODE, rows=None, holidays=None, today=None, prev=None, send=True, store=True, wait=True):
    today = today or today_tw()
    holidays = holidays if holidays is not None else fetch_holidays(today)
    expect = prev_trading_day(today, holidays)          # 應該已經有的最新資料日
    rows = rows if rows is not None else fetch_trend()
    tries = 0
    while mode == "push" and wait and rows and rows[-1]["date"] < expect and tries < 6:
        tries += 1
        log(f"資料還停在 {rows[-1]['date']}（應有 {expect}），10 分鐘後再試（{tries}/6）")
        time.sleep(600)
        rows = fetch_trend()
    if len(rows) < 3:
        log("❌ 抓不到追繳資料")
        notify.record("mcall", {"error": "no_data"})
        return None
    p = build(rows, holidays)
    prev = prev if prev is not None else load_latest()
    p["pushed"] = prev.get("pushed") if prev.get("date") == p["date"] else None
    msg = compose(p)
    log(msg.replace("<b>", "").replace("</b>", ""))
    sent = False
    if mode == "push" and send:
        fresh = p["date"] >= expect
        if FORCE or (fresh and pressure(p) and not p["pushed"]):
            sent = notify.send(msg)
            if sent:
                p["pushed"] = today
        else:
            log("不推播：" + ("資料不是最新" if not fresh else "今天已推過" if p["pushed"] else "沒有明顯追繳壓力"))
    if store:
        save(p)
    nxt = p["schedule"][0]
    notify.record("mcall", {"date": p["date"], "keep": p["keep"], "keep_chg": p["keep_chg"], "below": p["below"],
                            "call_amt": p["call_amt"], "exe": p["exe"], "exe_amt": p["exe_amt"], "exe_day": nxt["date"],
                            "pending": sum(s["n"] for s in p["schedule"] if s["kind"] == "pending"), "sent": sent})
    return {"payload": p, "msg": msg, "sent": sent}


if __name__ == "__main__":
    r = run()
    sys.exit(0 if r else 1)
