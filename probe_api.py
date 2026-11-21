"""研究用：檢查公開資料 API 的欄位（GitHub Actions 手動執行，結果看 log）
PROBE_URLS：空白分隔的網址
PROBE_FOLLOW=1：網頁的話，順著同網站的 <script src> 找出裡面用到的 API 路徑
"""
import os
import re
import sys
import json
from urllib.parse import urljoin, urlparse
import requests

URLS = [u for u in os.environ.get("PROBE_URLS", "").split() if u]
FOLLOW = os.environ.get("PROBE_FOLLOW", "") == "1"
GREP = os.environ.get("PROBE_GREP", "")   # 網頁／JS：印出符合的字附近 160 字
H = {"User-Agent": "Mozilla/5.0"}
API_RE = re.compile(r"""["'`]((?:https?://[^"'`\s]+)?/[^"'`\s]*(?:api|rwd|json|data|dashboard|credit|margin)[^"'`\s]*)["'`]""", re.I)


def endpoints(text):
    out = []
    for m in API_RE.findall(text):
        if m.endswith((".css", ".png", ".svg", ".jpg", ".ico", ".woff", ".woff2")) or len(m) > 200:
            continue
        if m not in out:
            out.append(m)
    return out


def show_json(j):
    if isinstance(j, list):
        print(f" list len {len(j)}; first: {json.dumps(j[0], ensure_ascii=False)[:700] if j else None}")
        if len(j) > 1:
            print(f" last: {json.dumps(j[-1], ensure_ascii=False)[:700]}")
        code_rows = [x for x in j if isinstance(x, dict) and any(str(v) in ("2330", "6133", "1101") for v in x.values())]
        if code_rows:
            print(f" sample: {json.dumps(code_rows[0], ensure_ascii=False)[:700]}")
    elif isinstance(j, dict):
        print(" keys:", list(j.keys())[:30])
        for k in ("fields", "fields1", "title", "date", "stat", "tables"):
            if k in j:
                print(f" {k}: {json.dumps(j[k], ensure_ascii=False)[:900]}")
        for k in ("data", "data1", "aaData"):
            if k in j and j[k]:
                print(f" {k}[0]: {json.dumps(j[k][0], ensure_ascii=False)[:500]}")
                print(f" {k}[-1]: {json.dumps(j[k][-1], ensure_ascii=False)[:500]}")
        print(" dump:", json.dumps(j, ensure_ascii=False)[:1500])


for u in URLS:
    try:
        r = requests.get(u, headers=H, timeout=30)
        print(f"==== {u}\n status {r.status_code}  bytes {len(r.content)}  type {r.headers.get('content-type')}")
        try:
            show_json(r.json())
            continue
        except Exception:
            pass
        r.encoding = r.apparent_encoding or r.encoding
        t = r.text
        print(" text:", t[:400].replace("\n", " "))
        print(" endpoints:", endpoints(t)[:60])
        if GREP:
            for m in list(re.finditer(GREP, t))[:40]:
                print(" grep:", t[max(0, m.start() - 160): m.end() + 160].replace("\n", " "))
        if FOLLOW:
            host = urlparse(u).netloc
            for src in re.findall(r"""<script[^>]+src=["']([^"']+)["']""", t, re.I):
                full = urljoin(u, src)
                if urlparse(full).netloc != host:
                    continue
                try:
                    js = requests.get(full, headers=H, timeout=30).text
                    eps = endpoints(js)
                    print(f" -- script {full}  bytes {len(js)}  endpoints: {eps[:80]}")
                except Exception as e:
                    print(f" -- script {full} error {e}")
    except Exception as e:
        print(f"==== {u}\n error {e}")
sys.stdout.flush()
