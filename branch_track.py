"""
券商分點追蹤（預設：凱基台北 9268）
資料：富邦 e 點通（MoneyDJ）分點進出排行（單日買超／賣超前 50 名，金額＋張數）＋ 證交所／櫃買每日行情

模式（BT_MODE）
- study：回測研究 —— 這個分點買超的股票長什麼樣（選股輪廓）、隔天是否倒貨（隔日沖）、
         跟單（看到資料隔天開盤買）的報酬
- daily：每天晚上抓當天名單 → 存 analysis_store（key=branch_<代號>）→ Telegram

報酬一律用「漲跌價差」還原（除權息不會算成跌），超額＝減掉同一天全體上市櫃股票平均
"""
import os
import re
import sys
import json
import time
import gzip
import statistics
from datetime import datetime, timedelta

import requests

import notify

BROKER = os.environ.get("BT_BROKER", "9200")
BRANCH = os.environ.get("BT_BRANCH", "9268")
BNAME = os.environ.get("BT_NAME", "凱基台北")
MODE = os.environ.get("BT_MODE", "daily")
DAYS = int(os.environ.get("BT_DAYS", "120") or 120)
CACHE = os.environ.get("BT_CACHE", "bt_cache")
SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")
H = {"User-Agent": "Mozilla/5.0"}
GAP = {"fubon": 1.0, "twse": 3.0, "tpex": 1.5}
_last = {k: 0.0 for k in GAP}
COST = 0.5          # 來回成本 %


def log(m):
    print(f"[{datetime.utcnow() + timedelta(hours=8):%H:%M:%S}] {m}", flush=True)


def _num(x):
    try:
        s = re.sub(r"<[^>]+>", "", str(x)).replace(",", "").replace("+", "").strip()
        return float(s) if s not in ("", "--", "---", "除權", "除息", "除權息") else None
    except ValueError:
        return None


def _get(src, url, params, as_json=True):
    for k in range(4):
        wait = GAP[src] - (time.time() - _last[src])
        if wait > 0:
            time.sleep(wait)
        _last[src] = time.time()
        try:
            r = requests.get(url, params=params, headers=H, timeout=40)
            if r.status_code == 200:
                if as_json:
                    return r.json()
                r.encoding = "cp950"
                return r.text
            log(f"⚠️ {src} {r.status_code}，重試")
        except Exception as e:
            log(f"⚠️ {src} {e}，重試")
        time.sleep(8 * (k + 1))
    return None


# ── 快取（每天一個檔，研究與每日共用）─────────────────────
def _cpath(kind, day):
    return os.path.join(CACHE, kind, f"{day}.json.gz")


def cache_get(kind, day):
    p = _cpath(kind, day)
    if os.path.exists(p):
        try:
            with gzip.open(p, "rt", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return None
    return None


def cache_put(kind, day, data):
    p = _cpath(kind, day)
    os.makedirs(os.path.dirname(p), exist_ok=True)
    with gzip.open(p, "wt", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)


# ── 分點進出 ──────────────────────────────────────────
def _hex(code):
    return "".join(f"{ord(c):04X}" for c in code)


ROW_RE = re.compile(
    r"(?:Link2Stk\('([^']+)'\);\">([^<]+)</a>|GenLink2stk\('A[A-Z]([^']+)','([^']+)'\))"
    r".*?<td[^>]*>([\d,\-]+)</td>\s*<td[^>]*>([\d,\-]+)</td>\s*<td[^>]*>([\d,\-]+)</td>", re.S)


def parse_branch(html):
    """→ (資料日期 YYYY-MM-DD, {"buy": [[代號, 名稱, 買, 賣, 差]], "sell": [...]})"""
    m = re.search(r"資料日期：(\d{8})", html or "")
    if not m:
        return None, None
    d = m.group(1)
    day = f"{d[:4]}-{d[4:6]}-{d[6:]}"
    i, j = html.find(">買超<"), html.find(">賣超<")
    if i < 0 or j < 0:
        return day, {"buy": [], "sell": []}

    def rows(seg):
        out = []
        for g in ROW_RE.finditer(seg):
            code = g.group(1) or g.group(3)
            name = g.group(2) or g.group(4)
            if g.group(1) and name.startswith(code):
                name = name[len(code):]
            out.append([code, name.strip(), *[int(x.replace(",", "")) for x in g.groups()[4:]]])
        return out
    return day, {"buy": rows(html[i:j]), "sell": rows(html[j:])}


def fetch_branch(day, use_cache=True):
    """{"buy": [[代號, 名稱, 買張, 賣張, 差張, 買金額, 賣金額, 差金額(千元)]], "sell": [...]}；資料日不符 → None"""
    if use_cache:
        c = cache_get(f"br{BRANCH}", day)
        if c is not None:
            return c
    url = "https://fubon-ebrokerdj.fbs.com.tw/z/zg/zgb/zgb0.djhtm"
    got = {}
    for kind in ("E", "B"):           # E＝張數、B＝金額（千元）
        html = _get("fubon", url, {"a": BROKER, "b": _hex(BRANCH), "c": kind, "e": day, "f": day}, as_json=False)
        dd, data = parse_branch(html)
        if dd != day or data is None:
            return None
        got[kind] = data
    out = {}
    for side in ("buy", "sell"):
        rows = {}
        for r in got["E"][side]:
            rows[r[0]] = [r[0], r[1], r[2], r[3], r[4], None, None, None]
        for r in got["B"][side]:
            x = rows.setdefault(r[0], [r[0], r[1], None, None, None, None, None, None])
            x[5:8] = r[2:5]
        out[side] = list(rows.values())
    cache_put(f"br{BRANCH}", day, out)
    return out


# ── 每日行情 ──────────────────────────────────────────
def _table(j, must):
    for t in (j or {}).get("tables") or []:
        f = t.get("fields") or []
        if all(any(m in x for x in f) for m in must):
            return t
    return None


def _col(fields, *names):
    for n in names:
        for i, f in enumerate(fields):
            if n in f:
                return i
    return None


def fetch_quotes(day, use_cache=True):
    """{代號: [開, 高, 低, 收, 參考價(前收還原), 成交張數, 成交金額(元), 市場]}"""
    if use_cache:
        c = cache_get("q", day)
        if c:
            return c
    ymd, slash = day.replace("-", ""), day.replace("-", "/")
    out = {}
    j = _get("twse", "https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX",
             {"date": ymd, "type": "ALLBUT0999", "response": "json"})
    t = _table(j, ["證券代號", "收盤價"])
    if t and str((j or {}).get("date")) == ymd:
        f = t["fields"]
        io, ih, il, ic = _col(f, "開盤價"), _col(f, "最高價"), _col(f, "最低價"), _col(f, "收盤價")
        isg, idf, iv, ia = _col(f, "漲跌(+/-)"), _col(f, "漲跌價差"), _col(f, "成交股數"), _col(f, "成交金額")
        for r in t["data"]:
            c = _num(r[ic])
            if not c:
                continue
            sign = -1 if "-" in str(r[isg]) else 1
            diff = _num(r[idf]) or 0.0
            out[r[0].strip()] = [_num(r[io]), _num(r[ih]), _num(r[il]), c, round(c - sign * diff, 4),
                                 round((_num(r[iv]) or 0) / 1000), _num(r[ia]) or 0, "twse"]
    j = _get("tpex", "https://www.tpex.org.tw/www/zh-tw/afterTrading/dailyQuotes", {"date": slash, "response": "json"})
    t = _table(j, ["代號", "收盤"])
    if t and str((j or {}).get("date")) == ymd:
        f = t["fields"]
        ic, idf, io, ih, il = _col(f, "收盤"), _col(f, "漲跌"), _col(f, "開盤"), _col(f, "最高"), _col(f, "最低")
        iv, ia = _col(f, "成交股數"), _col(f, "成交金額")
        for r in t["data"]:
            c = _num(r[ic])
            if not c:
                continue
            diff = _num(r[idf]) or 0.0
            if str(r[idf]).strip().startswith("-"):
                diff = -abs(diff)
            out[r[0].strip()] = [_num(r[io]), _num(r[ih]), _num(r[il]), c, round(c - diff, 4),
                                 round((_num(r[iv]) or 0) / 1000), _num(r[ia]) or 0, "tpex"]
    if len(out) > 1000:
        cache_put("q", day, out)
    return out


def trading_days(end, n):
    out, d = [], datetime.strptime(end, "%Y-%m-%d").replace(day=1)
    while len(out) < n + 5:
        j = _get("twse", "https://www.twse.com.tw/rwd/zh/afterTrading/FMTQIK",
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


def is_stock(code):
    return bool(re.fullmatch(r"[1-9]\d{3}", code))


# ── 分析 ──────────────────────────────────────────────
def features(code, i, days, Q):
    """第 i 天（T）的特徵；Q[day][code]"""
    q = Q[days[i]].get(code)
    if not q or not q[4]:
        return None
    o, h, l, c, ref, vol, amt, mkt = q
    pct = (c / ref - 1) * 100
    lim = round(ref * 1.1, 2)
    hist = [Q[days[k]].get(code) for k in range(max(0, i - 20), i)]
    hist = [x for x in hist if x]
    v20 = statistics.mean([x[5] for x in hist]) if len(hist) >= 10 else None
    r20 = 1.0
    for k in range(max(1, i - 19), i + 1):
        x = Q[days[k]].get(code)
        if x and x[4]:
            r20 *= x[3] / x[4]
    return {"pct": round(pct, 2), "limit": c >= lim - 0.011 or pct >= 9.4, "at_high": bool(h and c >= h - 1e-9),
            "vr": round(vol / v20, 2) if v20 else None, "r20": round((r20 - 1) * 100, 1), "px": c, "amt": amt,
            "mkt": mkt, "gap_dn": bool(o and o < ref)}


def path_ret(code, i, days, Q, start, hold):
    """start='open1'（T+1 開盤買）／'close0'（T 收盤）／'close1'（T+1 收盤）；持有到 T+hold 收盤；還原報酬 %"""
    if i + hold >= len(days):
        return None
    q1 = Q[days[i + 1]].get(code)
    if not q1 or not q1[4]:
        return None
    if start == "open1":
        if not q1[0]:
            return None
        r, k0 = q1[3] / q1[0], i + 2
    elif start == "close0":
        r, k0 = 1.0, i + 1
    else:
        r, k0 = 1.0, i + 2
    for k in range(k0, i + hold + 1):
        x = Q[days[k]].get(code)
        if not x or not x[4]:
            return None
        r *= x[3] / x[4]
    return (r - 1) * 100


def market_ret(i, days, Q, start, hold, _memo={}):
    key = (id(Q), i, start, hold)
    if key not in _memo:
        rs = [path_ret(c, i, days, Q, start, hold) for c in Q[days[i]] if is_stock(c)]
        rs = [x for x in rs if x is not None and abs(x) < 80]
        _memo[key] = statistics.mean(rs) if rs else 0.0
    return _memo[key]


def summarize(xs):
    xs = [x for x in xs if x is not None]
    if len(xs) < 5:
        return None
    m = statistics.mean(xs)
    sd = statistics.pstdev(xs) or 1e-9
    return {"n": len(xs), "mean": round(m, 2), "med": round(statistics.median(xs), 2),
            "win": round(sum(1 for x in xs if x > 0) / len(xs) * 100, 1), "t": round(m / sd * len(xs) ** 0.5, 2)}


def study(days, B, Q, min_amt=10000):
    """B[day] 分點名單；min_amt：買超金額門檻（千元，預設 1,000 萬）"""
    ev = []
    for i, d in enumerate(days):
        br = B.get(d)
        if not br:
            continue
        nxt = B.get(days[i + 1]) if i + 1 < len(days) else None
        sells1 = {r[0]: r for r in (nxt or {}).get("sell", [])}
        buys1 = {r[0]: r for r in (nxt or {}).get("buy", [])}
        for r in br["buy"]:
            code = r[0]
            if not is_stock(code) or (r[7] or 0) < min_amt:
                continue
            f = features(code, i, days, Q)
            if not f:
                continue
            e = {"day": d, "code": code, "name": r[1], "net_lots": r[4], "net_amt": r[7], **f,
                 "share": round(r[7] * 1000 / f["amt"] * 100, 2) if f["amt"] else None}
            if nxt is not None:
                s = sells1.get(code)
                e["sold1"] = round(-s[7] / r[7], 2) if s and s[7] else 0.0
                e["inbuy1"] = code in buys1
            for name, start, hold in (("gap", "open1", 0), ("o1c1", "open1", 1), ("o1c2", "open1", 2),
                                      ("o1c5", "open1", 5), ("o1c10", "open1", 10),
                                      ("c0c1", "close0", 1), ("c1c6", "close1", 6)):
                if name == "gap":
                    q1 = Q[days[i + 1]].get(code) if i + 1 < len(days) else None
                    e["gap"] = round((q1[0] / q1[4] - 1) * 100, 2) if q1 and q1[0] and q1[4] else None
                    continue
                r_ = path_ret(code, i, days, Q, start, hold)
                e[name] = None if r_ is None else round(r_ - market_ret(i, days, Q, start, hold), 2)
            ev.append(e)
    return ev


def base_rates(days, Q):
    """同期全體股票（成交金額 ≥ 1 億）的特徵分布，當對照"""
    lim = at = 0
    n, pcts, vrs = 0, [], []
    for i in range(20, len(days)):
        for code in Q[days[i]]:
            if not is_stock(code) or (Q[days[i]][code][6] or 0) < 1e8:
                continue
            f = features(code, i, days, Q)
            if not f:
                continue
            n += 1
            lim += f["limit"]
            at += f["at_high"]
            pcts.append(f["pct"])
            if f["vr"]:
                vrs.append(f["vr"])
    return {"n": n, "limit": round(lim / n * 100, 1) if n else None, "at_high": round(at / n * 100, 1) if n else None,
            "pct_med": round(statistics.median(pcts), 2) if pcts else None,
            "vr_med": round(statistics.median(vrs), 2) if vrs else None}


def profile(ev):
    if not ev:
        return {}
    pct = lambda k: round(sum(1 for e in ev if e.get(k)) / len(ev) * 100, 1)
    med = lambda k: round(statistics.median([e[k] for e in ev if e.get(k) is not None]), 2)
    sold = [e["sold1"] for e in ev if "sold1" in e]
    return {"n": len(ev), "days": len({e["day"] for e in ev}), "limit": pct("limit"), "at_high": pct("at_high"),
            "pct_med": med("pct"), "vr_med": med("vr"), "r20_med": med("r20"), "px_med": med("px"),
            "share_med": med("share"), "net_amt_med": med("net_amt"),
            "tpex": round(sum(1 for e in ev if e["mkt"] == "tpex") / len(ev) * 100, 1),
            "sold1_any": round(sum(1 for x in sold if x > 0) / len(sold) * 100, 1) if sold else None,
            "sold1_half": round(sum(1 for x in sold if x >= 0.5) / len(sold) * 100, 1) if sold else None,
            "sold1_med": round(statistics.median(sold), 2) if sold else None}


def returns(ev):
    out = {}
    for k in ("gap", "o1c1", "o1c2", "o1c5", "o1c10", "c0c1", "c1c6"):
        xs = [e.get(k) for e in ev]
        if k.startswith("o1") or k == "c1c6":
            xs = [None if x is None else x - COST for x in xs]
        out[k] = summarize(xs)
    return out


def groups(ev):
    g = {"全部": ev,
         "漲停買": [e for e in ev if e["limit"]],
         "非漲停": [e for e in ev if not e["limit"]],
         "大額(≥5000萬)": [e for e in ev if e["net_amt"] >= 50000],
         "占成交≥5%": [e for e in ev if (e.get("share") or 0) >= 5],
         "隔天有倒貨": [e for e in ev if e.get("sold1", 0) > 0],
         "隔天沒倒貨": [e for e in ev if "sold1" in e and e["sold1"] == 0]}
    return {k: {"profile": profile(v), "ret": returns(v)} for k, v in g.items() if len(v) >= 5}


# ── 研究模式 ──────────────────────────────────────────
def run_study(end=None, n=DAYS):
    end = end or (datetime.utcnow() + timedelta(hours=8)).strftime("%Y-%m-%d")
    days = trading_days(end, n + 25)
    log(f"交易日 {days[0]} ～ {days[-1]}（{len(days)} 天），分點 {BNAME}({BRANCH})")
    Q, B = {}, {}
    for k, d in enumerate(days):
        Q[d] = fetch_quotes(d)
        if k >= 25:
            B[d] = fetch_branch(d)
        if k % 20 == 0:
            log(f"  {d}：行情 {len(Q[d])} 檔、分點 {len((B.get(d) or {}).get('buy', []))} 筆買超")
    miss = [d for d in days[25:] if not B.get(d)]
    if miss:
        log(f"⚠️ 分點資料缺 {len(miss)} 天：{miss[:5]}")
    ev = study(days, B, Q)
    ev_all = study(days, B, Q, min_amt=0)
    res = {"branch": BRANCH, "name": BNAME, "from": days[25], "to": days[-1], "base": base_rates(days, Q),
           "groups": groups(ev), "n_all": len(ev_all),
           "top_names": _top_names(ev), "etf_share": _etf_share(days, B)}
    print_report(res)
    print("STUDYJSON " + json.dumps(res, ensure_ascii=False))
    print("EVENTSJSON " + json.dumps(ev, ensure_ascii=False))
    return res, ev


def _top_names(ev, k=15):
    cnt = {}
    for e in ev:
        c = cnt.setdefault(e["code"], [e["name"], 0, 0])
        c[1] += 1
        c[2] += e["net_amt"]
    return sorted(([k_, *v] for k_, v in cnt.items()), key=lambda x: -x[3])[:k]


def _etf_share(days, B):
    etf = stk = 0
    for d in days:
        for r in (B.get(d) or {}).get("buy", []):
            if r[7] is None:
                continue
            if is_stock(r[0]):
                stk += r[7]
            else:
                etf += r[7]
    return round(etf / (etf + stk) * 100, 1) if etf + stk else None


def print_report(res):
    log(f"===== {res['name']}（{res['branch']}）{res['from']} ～ {res['to']} =====")
    log(f"買超名單中 ETF 占金額 {res['etf_share']}%；個股買超（≥1,000 萬）事件見下")
    b = res["base"]
    log(f"對照（全體成交≥1億）：漲停 {b['limit']}%、收最高 {b['at_high']}%、當日漲幅中位 {b['pct_med']}%、量比中位 {b['vr_med']}")
    for g, v in res["groups"].items():
        p, r = v["profile"], v["ret"]
        log(f"--- {g}：{p['n']} 筆／{p['days']} 天｜漲停 {p['limit']}%、收最高 {p['at_high']}%、當日漲幅中位 {p['pct_med']}%、"
            f"量比 {p['vr_med']}、20日漲幅 {p['r20_med']}%、股價 {p['px_med']}、占成交 {p['share_med']}%、上櫃 {p['tpex']}%"
            f"｜隔天出現在賣超 {p['sold1_any']}%、賣掉一半以上 {p['sold1_half']}%")
        for k, lab in (("gap", "隔天開盤跳空"), ("c0c1", "T收→T+1收(超額)"), ("o1c1", "跟單:T+1開→T+1收"),
                       ("o1c2", "跟單:T+1開→T+2收"), ("o1c5", "跟單:T+1開→T+5收"), ("o1c10", "跟單:T+1開→T+10收"),
                       ("c1c6", "T+1收→T+6收")):
            s = r.get(k)
            if s:
                log(f"      {lab:<18} 平均 {s['mean']:+.2f}% 中位 {s['med']:+.2f}% 勝率 {s['win']}% t={s['t']}（{s['n']}）")
    log("最常買（金額）：" + "、".join(f"{n}({c}) {k}次" for c, n, k, a in res["top_names"][:12]))


# ── 每日模式 ──────────────────────────────────────────
def main():
    if MODE == "study":
        run_study()
    else:
        log("daily 模式尚未啟用")


if __name__ == "__main__":
    main()
