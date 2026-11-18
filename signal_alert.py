"""
買進訊號 Telegram 提醒（每日收盤後，接在 AI 選股之後）
====================================================
① 改良版券商條件（回測驗證：大跌比例 40.6% → 29.7%、最慘一筆 −45% → −27%）
   你的「高報酬、遊牧民」6 條件：20 日均量 > 2000 張、量 > 前 5 日均量 2 倍、KD 黃金交叉、20 < K < 50、
   收盤 > 60 日線、突破上關價
   再加 4 個過濾：本益比 0～25、當天不是漲停、20 日漲幅 < 25%、60 日漲幅 < 30%
   → 隔天可買；停損 −8%（收盤跌破成本 8% 隔天賣），不攤平
② 遊牧民書中買點（NOMAD_ON 開啟的才推；回測驗證有效才開）
   pullback：爆量長紅後 1～2 天出現量縮小陰線 → 收盤附近買；停損＝爆量日最低點；破 5 日線或滿 5 天出場
   watch   ：今天爆量長紅 → 明日觀察，列出明天收盤前的買進條件
資料：TWSE／TPEx 當日全市場行情＋本益比（各 1 次），候選股的日 K 優先用 AI 選股剛抓的快取，不夠才向 FinMind 補。
"""
import os
import json
import time
from datetime import datetime, timedelta

import requests

import notify
import strategy_check as C
import nomad_check as N

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")
FINMIND_TOKEN = os.environ.get("FINMIND_TOKEN", "").strip()
PRICE_CACHE_FILE = os.environ.get("PRICE_CACHE_FILE",
                                  os.path.join(os.path.dirname(os.path.abspath(__file__)), "price_cache.json"))
NOMAD_ON = {s for s in os.environ.get("NOMAD_ON", "").split(",") if s}     # 例：pullback,watch
MAX_FETCH = int(os.environ.get("SIGNAL_MAX_FETCH", "200"))
FM_INTERVAL = float(os.environ.get("FINMIND_MIN_INTERVAL", "7"))
STOP_PCT = 8
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "Mozilla/5.0"})


def log(m):
    print(f"[{datetime.utcnow() + timedelta(hours=8):%H:%M:%S}] {m}", flush=True)


def today_tw():
    return (datetime.utcnow() + timedelta(hours=8)).strftime("%Y-%m-%d")


def _f(x, d=0.0):
    try:
        return float(str(x).replace(",", "").replace("+", "").strip())
    except (TypeError, ValueError):
        return d


# ── 全市場行情與本益比 ─────────────────────────────────
def fetch_quotes():
    """{代號: {name, o, h, l, c, chg, pct, vol(張), amt(億)}}"""
    out = {}
    try:
        for r in SESSION.get("https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL", timeout=20).json():
            code, c, chg = r.get("Code", ""), _f(r.get("ClosingPrice")), _f(r.get("Change"))
            if len(code) == 4 and code.isdigit() and c > 0:
                prev = c - chg
                out[code] = {"name": r.get("Name", ""), "o": _f(r.get("OpeningPrice"), c), "h": _f(r.get("HighestPrice"), c),
                             "l": _f(r.get("LowestPrice"), c), "c": c, "chg": chg,
                             "pct": chg / prev * 100 if prev > 0 else 0, "vol": _f(r.get("TradeVolume")) / 1000,
                             "amt": _f(r.get("TradeValue")) / 1e8}
    except Exception as e:
        log(f"⚠️ 上市行情失敗：{e}")
    try:
        for r in SESSION.get("https://www.tpex.org.tw/openapi/v1/tpex_mainboard_quotes", timeout=30).json():
            code = r.get("SecuritiesCompanyCode", "") or r.get("code", "")
            c, chg = _f(r.get("Close")), _f(r.get("Change"))
            if len(code) == 4 and code.isdigit() and c > 0:
                prev = c - chg
                out[code] = {"name": r.get("CompanyName", ""), "o": _f(r.get("Open"), c), "h": _f(r.get("High"), c),
                             "l": _f(r.get("Low"), c), "c": c, "chg": chg,
                             "pct": chg / prev * 100 if prev > 0 else 0, "vol": _f(r.get("TradingShares")) / 1000,
                             "amt": _f(r.get("TransactionAmount")) / 1e8}
    except Exception as e:
        log(f"⚠️ 上櫃行情失敗：{e}")
    return out


def fetch_pe():
    """{代號: 本益比}（虧損或沒有資料 → 0）"""
    out = {}
    try:
        for r in SESSION.get("https://openapi.twse.com.tw/v1/exchangeReport/BWIBBU_ALL", timeout=20).json():
            out[r.get("Code", "")] = _f(r.get("PEratio"))
    except Exception as e:
        log(f"⚠️ 上市本益比失敗：{e}")
    try:
        for r in SESSION.get("https://www.tpex.org.tw/openapi/v1/tpex_mainboard_peratio_analysis", timeout=30).json():
            code = r.get("SecuritiesCompanyCode", "")
            pe = next((r[k] for k in r if "PriceEarning" in k or k in ("PERatio", "PEratio")), None)
            if code:
                out[code] = _f(pe)
    except Exception as e:
        log(f"⚠️ 上櫃本益比失敗：{e}")
    return out


# ── 日 K ──────────────────────────────────────────────
def load_price_cache():
    """AI 選股剛存的快取：{code: [[date, close, chg, open, high, low, vol], ...]}（舊格式只有前 3 欄 → 不用）"""
    try:
        with open(PRICE_CACHE_FILE, encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return {}
    out = {}
    for code, rows in (data.get("prices") or {}).items():
        if rows and len(rows[0]) >= 7:
            out[code] = [{"date": r[0], "close": r[1], "spread": r[2], "open": r[3], "max": r[4], "min": r[5],
                          "Trading_Volume": (r[6] or 0) * 1000, "Trading_money": (r[6] or 0) * 1000 * r[1]}
                         for r in rows]
    return out


_last = [0.0]


def fm_price(code, days=200):
    if not FINMIND_TOKEN:
        return []
    wait = FM_INTERVAL - (time.time() - _last[0])
    if wait > 0:
        time.sleep(wait)
    _last[0] = time.time()
    start = (datetime.utcnow() + timedelta(hours=8) - timedelta(days=days)).strftime("%Y-%m-%d")
    try:
        r = SESSION.get("https://api.finmindtrade.com/api/v4/data",
                        params={"dataset": "TaiwanStockPrice", "data_id": code, "start_date": start},
                        headers={"Authorization": f"Bearer {FINMIND_TOKEN}"}, timeout=30)
        return r.json().get("data") or [] if r.status_code == 200 else []
    except Exception as e:
        log(f"⚠️ {code} 日K失敗：{e}")
        return []


def with_today(rows, q, today):
    """FinMind 還沒更新今天 → 用當日行情補一根"""
    if not rows or not q or rows[-1]["date"][:10] >= today:
        return rows
    return rows + [{"date": today, "open": q["o"], "max": q["h"], "min": q["l"], "close": q["c"], "spread": q["chg"],
                    "Trading_Volume": q["vol"] * 1000, "Trading_money": q["amt"] * 1e8}]


# ── 訊號 ──────────────────────────────────────────────
def broker_signal(rows, pe):
    """改良版券商條件：回傳說明 dict 或 None"""
    seq = C.ohlc_rows(rows)
    if len(seq) < 125:
        return None
    f = C.features(seq, {}, {}, [], {seq[-1]["d"]: pe})
    j = len(seq) - 1
    if not C.base_signal(seq, f, j, C.VOL_STRICT):
        return None
    x = f[j]
    ok, why = True, []
    if not (pe and 0 < pe <= 25):
        ok = False
        why.append(f"本益比 {pe or '虧損/無'}")
    if x["ret1"] >= 0.095:
        ok = False
        why.append("今天漲停")
    if x["r20"] >= 0.25:
        ok = False
        why.append(f"20日已漲 {x['r20'] * 100:.0f}%")
    if x["r60"] >= 0.30:
        ok = False
        why.append(f"60日已漲 {x['r60'] * 100:.0f}%")
    c = seq[j]["c"]
    return {"ok": ok, "why": why, "close": c, "stop": round(c * (1 - STOP_PCT / 100), 2), "pe": pe,
            "r20": round(x["r20"] * 100, 1), "r60": round(x["r60"] * 100, 1)}


def nomad_signals(rows):
    """遊牧民：今天爆量長紅（明日觀察）、今天是爆量後的量縮小陰線（買點）"""
    a, seq = N.build_arrays(rows)
    n = a["n"]
    out = {}
    if n < 70:
        return out
    j = n - 1
    raw = [x["c"] for x in seq]
    k = [raw[i] / a["C"][i] for i in range(n)]               # 還原價 → 原始價
    if N.is_surge(a, j):
        out["watch"] = {"vol_x": round(a["V"][j] / a["V"][j - 1], 1), "pct": round(a["ret"][j] * 100, 1),
                        "max_vol": round(a["V"][j] * 0.5), "min_close": round(a["C"][j] * 0.97 * k[j], 2),
                        "ma5": round((a["ma5"][j] or 0) * k[j], 2), "stop": round(a["L"][j] * k[j], 2),
                        "amt": round(a["A"][j], 1)}
    for s in (j - 1, j - 2):
        if s > 0 and N.is_surge(a, s) and N.pullback_entry(a, s) == j:
            out["pullback"] = {"close": raw[j], "stop": round(a["L"][s] * k[j], 2), "surge_date": a["d"][s],
                               "ma5": round((a["ma5"][j] or 0) * k[j], 2)}
            break
    return out


# ── 主流程 ────────────────────────────────────────────
def load_recent_surges(days=5):
    if not SUPABASE_URL or not SUPABASE_KEY:
        return set()
    try:
        r = SESSION.get(f"{SUPABASE_URL}/rest/v1/analysis_store",
                        params={"select": "date,payload", "key": "eq.nomad_watch", "order": "date.desc", "limit": str(days)},
                        headers={"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}, timeout=15)
        rows = r.json() if r.status_code == 200 else []
        return {c for row in rows for c in (row.get("payload") or {}).get("codes", [])}
    except Exception:
        return set()


def save_surges(codes, today):
    if not SUPABASE_URL or not SUPABASE_KEY:
        return
    h = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}", "Content-Type": "application/json",
         "Prefer": "return=minimal"}
    url = f"{SUPABASE_URL}/rest/v1/analysis_store"
    try:
        requests.delete(url, params={"key": "eq.nomad_watch", "date": f"eq.{today}"}, headers=h, timeout=15)
        requests.post(url, json={"key": "nomad_watch", "date": today, "payload": {"codes": sorted(codes)}},
                      headers=h, timeout=15)
    except Exception:
        pass


def pick_candidates(quotes, recent):
    broker = [c for c, q in quotes.items() if q["c"] >= 10 and q["vol"] >= 2000 and q["pct"] >= 1.5]
    surge = [c for c, q in quotes.items() if q["c"] >= 10 and q["pct"] >= 5 and q["amt"] >= 3]
    watch = [c for c in recent if c in quotes]
    order = list(dict.fromkeys(watch + sorted(surge, key=lambda c: -quotes[c]["amt"])
                               + sorted(broker, key=lambda c: -quotes[c]["vol"])))
    return order


def run(quotes=None, pe=None, cache=None, fetch=fm_price, recent=None, today=None, send=True):
    today = today or today_tw()
    quotes = quotes if quotes is not None else fetch_quotes()
    if not quotes:
        log("❌ 抓不到今日行情，結束")
        notify.record("signals", {"error": "no_quotes"})
        return None
    pe = pe if pe is not None else fetch_pe()
    cache = cache if cache is not None else load_price_cache()
    recent = recent if recent is not None else load_recent_surges()
    cands = pick_candidates(quotes, recent)
    log(f"今日行情 {len(quotes)} 支、候選 {len(cands)} 支（快取有 {sum(1 for c in cands if c in cache)} 支）")
    buys, rejected, watch, pull, surged, fetched = [], [], [], [], [], 0
    for code in cands:
        rows = cache.get(code)
        if not rows:
            if fetched >= MAX_FETCH:
                continue
            rows = fetch(code)
            fetched += 1
        rows = with_today(rows, quotes.get(code), today)
        if not rows or rows[-1]["date"][:10] != today:
            continue
        q = quotes[code]
        name = q["name"] or code
        try:
            b = broker_signal(rows, pe.get(code))
            if b:
                (buys if b["ok"] else rejected).append({"code": code, "name": name, **b})
            ns = nomad_signals(rows)
            if "watch" in ns:
                surged.append(code)
                watch.append({"code": code, "name": name, **ns["watch"]})
            if "pullback" in ns:
                pull.append({"code": code, "name": name, **ns["pullback"]})
        except Exception as e:
            log(f"⚠️ {code} 計算失敗：{e}")
    save_surges(surged, today)
    log(f"改良版條件 {len(buys)} 支（原始條件符合但被過濾 {len(rejected)} 支）；遊牧民 爆量長紅 {len(watch)}、量縮小陰線 {len(pull)}；"
        f"FinMind 補抓 {fetched} 支")
    msg = compose(today, buys, rejected, watch, pull)
    sent = notify.send(msg) if (send and msg) else False
    notify.record("signals", {"buys": len(buys), "rejected": len(rejected), "watch": len(watch), "pullback": len(pull),
                              "names": [f"{b['name']}({b['code']})" for b in buys[:8]], "sent": sent,
                              "nomad_on": sorted(NOMAD_ON)})
    return {"buys": buys, "rejected": rejected, "watch": watch, "pullback": pull, "msg": msg}


def compose(today, buys, rejected, watch, pull):
    e = notify.esc
    L = [f"📣 <b>買進訊號</b>（{today[5:].replace('-', '/')} 收盤後）"]
    L += ["", "🟢 <b>改良版遊牧民條件</b>（原 6 條件＋本益比≤25、不追漲停、不追急漲）"]
    if buys:
        for b in buys[:8]:
            L.append(f"• <b>{e(b['name'])}</b> {b['code']} 收 {b['close']}｜本益比 {b['pe']:.1f}｜停損 {b['stop']}（−{STOP_PCT}%）")
        L.append("　隔天可買；收盤跌破停損價隔天賣、不攤平，最多抱 20 天")
    else:
        L.append("今天沒有符合的股票")
    if rejected:
        L.append("　被過濾掉：" + "、".join(f"{e(r['name'])}（{'、'.join(r['why'])}）" for r in rejected[:5]))
    if "pullback" in NOMAD_ON and pull:
        L += ["", "🎯 <b>書的買點：爆量後量縮小陰線</b>"]
        for p in pull[:6]:
            L.append(f"• <b>{e(p['name'])}</b> {p['code']} 收 {p['close']}｜停損 {p['stop']}（{p['surge_date'][5:]} 爆量日低點）")
        L.append("　出場：收盤破 5 日線、單日跌 5%、或滿 5 天就賣")
    if "watch" in NOMAD_ON and watch:
        L += ["", "👀 <b>明日觀察：今天爆量長紅</b>（等量縮小陰線再買，不追）"]
        for w in watch[:6]:
            L.append(f"• <b>{e(w['name'])}</b> {w['code']} 今天 +{w['pct']}%、量 {w['vol_x']} 倍、{w['amt']} 億")
            L.append(f"　明天收盤前若：量 < {w['max_vol']:,} 張、收黑K、價 ≥ {w['min_close']}、在 5 日線 {w['ma5']} 上 → 買；停損 {w['stop']}")
    L += ["", "⚠️ 僅供參考，回測平均約和大盤持平，主要是少踩雷；請自行判斷、控制部位、少用融資"]
    return "\n".join(L)


if __name__ == "__main__":
    run()
