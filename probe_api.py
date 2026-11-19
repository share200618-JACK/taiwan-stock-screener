"""研究用：檢查公開資料 API 的欄位（GitHub Actions 手動執行，結果看 log）"""
import os
import sys
import json
import requests

URLS = [u for u in os.environ.get("PROBE_URLS", "").split() if u]
H = {"User-Agent": "Mozilla/5.0"}
for u in URLS:
    try:
        r = requests.get(u, headers=H, timeout=30)
        print(f"==== {u}\n status {r.status_code}  bytes {len(r.content)}  type {r.headers.get('content-type')}")
        try:
            j = r.json()
        except Exception:
            print(" text:", r.text[:400].replace("\n", " "))
            continue
        if isinstance(j, list):
            print(f" list len {len(j)}; first: {json.dumps(j[0], ensure_ascii=False)[:700] if j else None}")
            code_rows = [x for x in j if isinstance(x, dict) and any(str(v) in ("2330", "6133", "1101") for v in x.values())]
            if code_rows:
                print(f" sample: {json.dumps(code_rows[0], ensure_ascii=False)[:700]}")
        elif isinstance(j, dict):
            print(" keys:", list(j.keys())[:20])
            for k in ("fields", "fields1", "title", "date", "stat", "tables"):
                if k in j:
                    print(f" {k}: {json.dumps(j[k], ensure_ascii=False)[:900]}")
            for k in ("data", "data1", "aaData"):
                if k in j and j[k]:
                    print(f" {k}[0]: {json.dumps(j[k][0], ensure_ascii=False)[:500]}")
    except Exception as e:
        print(f"==== {u}\n error {e}")
sys.stdout.flush()
