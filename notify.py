"""
Telegram 推播與排程狀態（GitHub Actions 每日排程共用）
=====================================================
- send(msg)：推播到 TG_BOT_TOKEN / TG_CHAT_ID（GitHub Secrets），超過 4000 字自動分段
- record(section, data)：各步驟把執行狀況寫進 run_status.json，最後由 daily_report.py 整理成每日報告
"""
import os
import json
import html
import requests

TG_BOT_TOKEN = os.environ.get("TG_BOT_TOKEN", "").strip()
TG_CHAT_ID   = os.environ.get("TG_CHAT_ID", "").strip()
STATUS_FILE  = os.environ.get("RUN_STATUS_FILE",
                              os.path.join(os.path.dirname(os.path.abspath(__file__)), "run_status.json"))
LIMIT = 4000   # Telegram 單則上限 4096 字


_chat = {"id": TG_CHAT_ID, "tried": bool(TG_CHAT_ID)}


def _tg_get(method, tries=4):
    """Telegram API GET，連線被重設時重試（GitHub 主機偶爾連不上）"""
    import time as _t
    last = None
    for k in range(tries):
        try:
            return requests.get(f"https://api.telegram.org/bot{TG_BOT_TOKEN}/{method}", timeout=20)
        except Exception as e:
            last = e
            _t.sleep(3 * (k + 1))
    raise last


def _sb():
    url, key = os.environ.get("SUPABASE_URL", ""), os.environ.get("SUPABASE_KEY", "")
    return (url, {"apikey": key, "Authorization": f"Bearer {key}", "Content-Type": "application/json",
                  "Prefer": "return=minimal"}) if url and key else (None, None)


def chat_id():
    """
    TG_CHAT_ID 沒設定時自動找：
      1. Supabase analysis_store（key=tg_chat）存過的
      2. 機器人最近收到的私訊（使用者傳過任何一句話給機器人就找得到，24 小時內）→ 找到就存進 Supabase
    """
    if _chat["id"] or _chat["tried"] or not TG_BOT_TOKEN:
        return _chat["id"]
    _chat["tried"] = True
    url, h = _sb()
    if url:
        try:
            r = requests.get(f"{url}/rest/v1/analysis_store", params={"select": "payload", "key": "eq.tg_chat",
                             "order": "date.desc", "limit": "1"}, headers=h, timeout=10)
            rows = r.json() if r.status_code == 200 else []
            if rows and (rows[0].get("payload") or {}).get("chat_id"):
                _chat["id"] = str(rows[0]["payload"]["chat_id"])
                return _chat["id"]
        except Exception:
            pass
    try:
        r = _tg_get("getUpdates")
        ups = (r.json() or {}).get("result") or []
        chats = [u.get("message", {}).get("chat", {}) for u in ups if u.get("message")]
        chats = [c for c in chats if c.get("type") == "private" and c.get("id")]
        if chats:
            _chat["id"] = str(chats[-1]["id"])
            print(f"✅ 找到你的 Telegram Chat ID：{_chat['id']}（已記住，不用再設定）", flush=True)
            if url:
                from datetime import datetime, timedelta
                today = (datetime.utcnow() + timedelta(hours=8)).strftime("%Y-%m-%d")
                requests.post(f"{url}/rest/v1/analysis_store", headers=h, timeout=10,
                              json={"key": "tg_chat", "date": today, "payload": {"chat_id": _chat["id"]}})
        elif r.status_code == 200:
            me = ""
            try:
                gm = _tg_get("getMe")
                body = gm.json()
                me = "@" + body["result"]["username"] if body.get("ok") else ""
                if not me:
                    print(f"   getMe {gm.status_code}: {str(body)[:120]}", flush=True)
            except Exception as e:
                print(f"   getMe 例外：{e}", flush=True)
            import re
            print(f"   Token 格式：長度 {len(TG_BOT_TOKEN)}、{'正確（數字:英數）' if re.fullmatch(r'[0-9]{6,12}:[A-Za-z0-9_-]{30,}', TG_BOT_TOKEN) else '看起來不對'}", flush=True)
            print(f"ℹ️ 機器人 {me} 還沒收到你的訊息（共 {len(ups)} 筆更新）：請在 Telegram 打開 {me}，按「開始」或傳任何一句話", flush=True)
        else:
            print(f"❌ Telegram Token 可能有誤：{str((r.json() or {}).get('description', ''))[:100]}", flush=True)
    except Exception as e:
        print(f"⚠️ 查詢 Telegram Chat ID 失敗：{e}", flush=True)
    return _chat["id"]


def configured():
    return bool(TG_BOT_TOKEN and chat_id())


def esc(s):
    """HTML 模式下股票名稱等文字要跳脫"""
    return html.escape(str(s if s is not None else ""), quote=False)


def _chunks(msg):
    if len(msg) <= LIMIT:
        return [msg]
    out, cur = [], ""
    for line in msg.split("\n"):
        if len(cur) + len(line) + 1 > LIMIT and cur:
            out.append(cur)
            cur = ""
        cur += line + "\n"
    if cur:
        out.append(cur)
    return out


def send(msg, silent=False):
    """回傳 True/False；沒設定時印提示並回 False（不會讓排程失敗）；連線被重設時重試"""
    if not configured():
        print("ℹ️ Telegram 還沒設定好（缺 TG_BOT_TOKEN，或機器人還沒收到你的訊息），略過推播", flush=True)
        return False
    import time as _t
    ok = True
    for part in _chunks(msg):
        done, err = False, ""
        for k in range(3):
            try:
                r = requests.post(f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage",
                                  json={"chat_id": chat_id(), "text": part, "parse_mode": "HTML",
                                        "disable_web_page_preview": True, "disable_notification": silent},
                                  timeout=20)
                try:
                    body = r.json()
                except Exception:
                    body = {}
                if r.status_code == 200 and body.get("ok"):
                    done = True
                    break
                err = f"{r.status_code}: {str(body.get('description', r.text))[:150]}"
                if r.status_code < 500 and r.status_code != 429:
                    break                                    # 參數錯誤（例如 chat id 不對）不用重試
            except Exception as e:
                err = f"例外 {e}"
            _t.sleep(3 * (k + 1))
        if not done:
            ok = False
            print(f"❌ Telegram 推播失敗 {err}", flush=True)
    if ok:
        print("✅ Telegram 已推播", flush=True)
    return ok


def load_status():
    try:
        with open(STATUS_FILE, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def record(section, data):
    """把某一步驟的狀態合併寫入 run_status.json（失敗不影響主流程）"""
    try:
        st = load_status()
        st.setdefault(section, {}).update(data)
        with open(STATUS_FILE, "w", encoding="utf-8") as f:
            json.dump(st, f, ensure_ascii=False)
    except Exception as e:
        print(f"⚠️ 狀態檔寫入失敗: {e}", flush=True)
