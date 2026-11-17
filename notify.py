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

TG_BOT_TOKEN = os.environ.get("TG_BOT_TOKEN", "")
TG_CHAT_ID   = os.environ.get("TG_CHAT_ID", "")
STATUS_FILE  = os.environ.get("RUN_STATUS_FILE",
                              os.path.join(os.path.dirname(os.path.abspath(__file__)), "run_status.json"))
LIMIT = 4000   # Telegram 單則上限 4096 字


def configured():
    return bool(TG_BOT_TOKEN and TG_CHAT_ID)


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
    """回傳 True/False；沒設定時印提示並回 False（不會讓排程失敗）"""
    if not configured():
        print("ℹ️ 未設定 TG_BOT_TOKEN / TG_CHAT_ID，略過 Telegram 推播", flush=True)
        return False
    ok = True
    for part in _chunks(msg):
        try:
            r = requests.post(f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage",
                              json={"chat_id": TG_CHAT_ID, "text": part, "parse_mode": "HTML",
                                    "disable_web_page_preview": True, "disable_notification": silent},
                              timeout=15)
            body = {}
            try:
                body = r.json()
            except Exception:
                pass
            if r.status_code != 200 or not body.get("ok"):
                ok = False
                print(f"❌ Telegram 推播失敗 {r.status_code}: {str(body.get('description', r.text))[:150]}", flush=True)
        except Exception as e:
            ok = False
            print(f"❌ Telegram 推播例外: {e}", flush=True)
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
