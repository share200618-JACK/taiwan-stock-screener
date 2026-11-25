"""
AI 2.0 資料層：全市場每日資料（證交所＋櫃買中心公開資料，按日期抓、可回補數年）

每天一個檔 ai2_cache/day/YYYY-MM-DD.json.gz：
  {代號: [開, 高, 低, 收, 參考價, 成交張數, 成交金額(元), 本益比, 淨值比, 殖利率, 外資(張), 投信(張), 自營(張), 市場]}
- 參考價＝前一日收盤（已含除權息調整），報酬一律用 收/參考價 串起來 → 不會把除權息算成下跌
- 只留個股（4 碼）與 ETF（00 開頭），不留權證
- 證交所與櫃買分兩條執行緒抓（各自限速），中斷後重跑會從快取接續
"""
import os
import re
import sys
import json
import gzip
import time
import threading
from datetime import datetime, timedelta

import requests

CACHE = os.environ.get("AI2_CACHE", "ai2_cache")
H = {"User-Agent": "Mozilla/5.0"}
GAP = {"twse": 3.2, "tpex": 1.6}
_last = {k: 0.0 for k in GAP}
_lock = {k: threading.Lock() for k in GAP}
F = ["o", "h", "l", "c", "ref", "vol", "amt", "pe", "pb", "yld", "fi", "it", "dl", "mkt"]
IDX = {k: i for i, k in enumerate(F)}
KEEP = re.compile(r"^(?:[1-9]\d{3}|00\d{2,4}[A-Z]?)$")


def log(m):
    print(f"[{datetime.utcnow() + timedelta(hours=8):%H:%M:%S}] {m}", flush=True)


def _num(x):
    s = re.sub(r"<[^>]+>", "", str(x)).replace(",", "").replace("+", "").strip()
    if s in ("", "-", "--", "---", "N/A", "除權", "除息", "除權息"):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _get(src, url, params):
    for k in range(5):
        with _lock[src]:
            wait = GAP[src] - (time.time() - _last[src])
            if wait > 0:
                time.sleep(wait)
            _last[src] = time.time()
        try:
            r = requests.get(url, params=params, headers=H, timeout=60)
            if r.status_code == 200:
                return r.json()
            log(f"⚠️ {src} {r.status_code} {url.rsplit('/', 1)[-1]}，重試")
        except Exception as e:
            log(f"⚠️ {src} {type(e).__name__} {url.rsplit('/', 1)[-1]}，重試")
        time.sleep(10 * (k + 1))
    return None


def _tables(j):
    if not j:
        return []
    if j.get("tables"):
        return j["tables"]
    if j.get("fields"):
        return [{"fields": j["fields"], "data": j.get("data") or []}]
    return []


def _table(j, must):
    for t in _tables(j):
        f = t.get("fields") or []
        if all(any(m in x for x in f) for m in must):
            return t
    return None


def _col(fields, name, nth=0):
    hits = [i for i, f in enumerate(fields) if name in f]
    return hits[nth] if len(hits) > nth else None


def _blank():
    return [None] * len(F)


# ── 證交所 ────────────────────────────────────────────
def fetch_twse(day):
    ymd = day.replace("-", "")
    out = {}
    j = _get("twse", "https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX",
             {"date": ymd, "type": "ALLBUT0999", "response": "json"})
    if not j or str(j.get("date")) != ymd:
        return None
    t = _table(j, ["證券代號", "收盤價"])
    if not t:
        return None
    f = t["fields"]
    io, ih, il, ic = _col(f, "開盤價"), _col(f, "最高價"), _col(f, "最低價"), _col(f, "收盤價")
    isg, idf, iv, ia = _col(f, "漲跌(+/-)"), _col(f, "漲跌價差"), _col(f, "成交股數"), _col(f, "成交金額")
    for r in t["data"]:
        code = str(r[0]).strip()
        c = _num(r[ic])
        if not KEEP.match(code) or not c:
            continue
        sign = -1 if "-" in str(r[isg]) else 1
        x = _blank()
        x[0:7] = [_num(r[io]), _num(r[ih]), _num(r[il]), c, round(c - sign * (_num(r[idf]) or 0.0), 4),
                  round((_num(r[iv]) or 0) / 1000), _num(r[ia]) or 0]
        x[IDX["mkt"]] = 0
        out[code] = x
    j = _get("twse", "https://www.twse.com.tw/rwd/zh/fund/T86", {"date": ymd, "selectType": "ALLBUT0999", "response": "json"})
    t = _table(j, ["證券代號", "投信買賣超股數"])
    if t:
        f = t["fields"]
        ifi, iit, idl = _col(f, "外陸資買賣超股數"), _col(f, "投信買賣超股數"), _col(f, "自營商買賣超股數")
        for r in t["data"]:
            x = out.get(str(r[0]).strip())
            if x:
                x[IDX["fi"]], x[IDX["it"]], x[IDX["dl"]] = (round((_num(r[i]) or 0) / 1000) for i in (ifi, iit, idl))
    j = _get("twse", "https://www.twse.com.tw/rwd/zh/afterTrading/BWIBBU_d", {"date": ymd, "selectType": "ALL", "response": "json"})
    t = _table(j, ["證券代號", "本益比"])
    if t:
        f = t["fields"]
        ipe, ipb, iy = _col(f, "本益比"), _col(f, "股價淨值比"), _col(f, "殖利率")
        for r in t["data"]:
            x = out.get(str(r[0]).strip())
            if x:
                x[IDX["pe"]], x[IDX["pb"]], x[IDX["yld"]] = _num(r[ipe]), _num(r[ipb]), _num(r[iy])
    return out


# ── 櫃買中心 ──────────────────────────────────────────
def fetch_tpex(day):
    ymd, slash = day.replace("-", ""), day.replace("-", "/")
    out = {}
    j = _get("tpex", "https://www.tpex.org.tw/www/zh-tw/afterTrading/dailyQuotes", {"date": slash, "response": "json"})
    if not j or str(j.get("date")) != ymd:
        return None
    t = _table(j, ["代號", "收盤"])
    if not t:
        return None
    f = t["fields"]
    ic, idf, io, ih, il = _col(f, "收盤"), _col(f, "漲跌"), _col(f, "開盤"), _col(f, "最高"), _col(f, "最低")
    iv, ia = _col(f, "成交股數"), _col(f, "成交金額")
    for r in t["data"]:
        code = str(r[0]).strip()
        c = _num(r[ic])
        if not KEEP.match(code) or not c:
            continue
        diff = _num(r[idf]) or 0.0
        if str(r[idf]).strip().startswith("-"):
            diff = -abs(diff)
        x = _blank()
        x[0:7] = [_num(r[io]), _num(r[ih]), _num(r[il]), c, round(c - diff, 4),
                  round((_num(r[iv]) or 0) / 1000), _num(r[ia]) or 0]
        x[IDX["mkt"]] = 1
        out[code] = x
    j = _get("tpex", "https://www.tpex.org.tw/www/zh-tw/insti/dailyTrade",
             {"type": "Daily", "sect": "AL", "date": slash, "response": "json"})
    t = _table(j, ["代號", "三大法人買賣超股數合計"])
    if t:
        # 欄位：外資(不含自營) 買/賣/超、外資自營 ×3、外資合計 ×3、投信 ×3、自營(自行) ×3、自營(避險) ×3、自營合計 ×3、三大法人合計
        for r in t["data"]:
            x = out.get(str(r[0]).strip())
            if x and len(r) >= 24:
                x[IDX["fi"]], x[IDX["it"]], x[IDX["dl"]] = (round((_num(r[i]) or 0) / 1000) for i in (4, 13, 22))
    j = _get("tpex", "https://www.tpex.org.tw/www/zh-tw/afterTrading/peQryDate", {"date": slash, "response": "json"})
    t = _table(j, ["股票代號", "本益比"])
    if t:
        f = t["fields"]
        ipe, ipb, iy = _col(f, "本益比"), _col(f, "股價淨值比"), _col(f, "殖利率")
        for r in t["data"]:
            x = out.get(str(r[0]).strip())
            if x:
                x[IDX["pe"]], x[IDX["pb"]], x[IDX["yld"]] = _num(r[ipe]), _num(r[ipb]), _num(r[iy])
    return out


# ── 快取 ──────────────────────────────────────────────
def _path(day):
    return os.path.join(CACHE, "day", f"{day}.json.gz")


def load_day(day):
    p = _path(day)
    if not os.path.exists(p):
        return None
    try:
        with gzip.open(p, "rt", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def save_day(day, data):
    os.makedirs(os.path.dirname(_path(day)), exist_ok=True)
    tmp = _path(day) + ".tmp"
    with gzip.open(tmp, "wt", encoding="utf-8") as f:
        json.dump(data, f, separators=(",", ":"))
    os.replace(tmp, _path(day))


def trading_days(end, n):
    """證交所交易日曆（FMTQIK 每月一次）"""
    out, d = [], datetime.strptime(end, "%Y-%m-%d").replace(day=1)
    months = 0
    while len(out) < n and months < 240:
        j = _get("twse", "https://www.twse.com.tw/rwd/zh/afterTrading/FMTQIK", {"date": d.strftime("%Y%m01"), "response": "json"}) or {}
        for row in j.get("data") or []:
            y, m, dd = row[0].split("/")
            day = f"{int(y) + 1911}-{m}-{dd}"
            if day <= end:
                out.append(day)
        d = (d - timedelta(days=1)).replace(day=1)
        months += 1
        out = sorted(set(out))
    return out[-n:]


def backfill(days, deadline=None):
    """抓缺的日子；兩個市場平行抓，都成功才存檔"""
    todo = [d for d in days if load_day(d) is None]
    log(f"需要抓 {len(todo)} 天（快取已有 {len(days) - len(todo)} 天）")
    res = {"twse": {}, "tpex": {}}

    def worker(src, fn):
        for d in todo:
            if deadline and time.time() > deadline:
                return
            try:
                res[src][d] = fn(d)
            except Exception as e:
                log(f"⚠️ {src} {d} {e}")
                res[src][d] = None
            _merge(d)

    merged = set()
    mlock = threading.Lock()

    def _merge(d):
        with mlock:
            if d in merged or d not in res["twse"] or d not in res["tpex"]:
                return
            merged.add(d)
            a, b = res["twse"].pop(d), res["tpex"].pop(d)
            if a and b and len(a) > 500 and len(b) > 300:
                save_day(d, {**a, **b})
                n = len(merged)
                if n % 25 == 0:
                    log(f"  已存 {n}/{len(todo)} 天（最新 {d}：{len(a)}＋{len(b)} 檔）")
            else:
                log(f"⚠️ {d} 資料不完整（證交所 {len(a or {})}、櫃買 {len(b or {})}），下次再抓")

    ts = [threading.Thread(target=worker, args=("twse", fetch_twse)), threading.Thread(target=worker, args=("tpex", fetch_tpex))]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    ok = sum(1 for d in days if load_day(d) is not None)
    log(f"完成：{ok}/{len(days)} 天有資料")
    for d in (days[0], days[len(days) // 2], days[-1]):
        x = load_day(d)
        if x:
            stk = [v for k, v in x.items() if len(k) == 4]
            cov = {f: round(sum(1 for v in stk if v[i] is not None) / len(stk) * 100) for i, f in enumerate(F)}
            log(f"  {d}：個股 {len(stk)} 檔（上櫃 {sum(1 for v in stk if v[IDX['mkt']] == 1)}），欄位覆蓋率 {cov}")
    return ok


def load_panel(days):
    """{day: {code: row}}，缺的日子略過"""
    out = {}
    for d in days:
        x = load_day(d)
        if x:
            out[d] = x
    return out


if __name__ == "__main__":
    n = int(os.environ.get("AI2_DAYS", "650"))
    end = os.environ.get("AI2_END") or (datetime.utcnow() + timedelta(hours=8) - timedelta(hours=18)).strftime("%Y-%m-%d")
    days = trading_days(end, n)
    log(f"交易日 {days[0]} ～ {days[-1]}（{len(days)} 天）")
    mins = float(os.environ.get("AI2_MINUTES", "140"))
    backfill(days, deadline=time.time() + mins * 60)
    sys.exit(0)
