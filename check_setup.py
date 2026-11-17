"""
連線測試：檢查 GitHub Secrets 裡的 FinMind token 與 Telegram 設定是否可用
（GitHub → Actions →「連線測試」→ Run workflow，約 30 秒）
"""
import os
import sys
from datetime import datetime, timedelta

import requests
import notify

TOKEN = os.environ.get("FINMIND_TOKEN", "").strip()
SITE_URL = os.environ.get("SITE_URL", "https://taiwan-stock-screener-783z.onrender.com").rstrip("/")
# 各次更新加入的網址：回得出來＝網站已部署到那一版
SITE_CHECKS = [
    ("/api/left/latest", "左側選股（10/23）"),
    ("/api/performance", "實戰績效 API（11/17）"),
    ("/api/strategy", "選股條件自動調整（11/17）"),
    ("/api/etf-wave/latest", "正2波浪（11/17）"),
    ("/api/signal-lab/latest", "個股勝率實驗室（11/18）"),
]


def check_site():
    """Render 有沒有部署到最新版：免費方案休眠時第一次要等約 1 分鐘喚醒"""
    out, all_ok = [], True
    for i, (path, label) in enumerate(SITE_CHECKS):
        try:
            r = requests.get(SITE_URL + path, timeout=120 if i == 0 else 30)
            ok = r.status_code == 200
        except Exception as e:
            ok, r = False, None
            print(f"   {path}: {e}")
        all_ok &= ok
        out.append(f"{'✅' if ok else '❌'} {label}" + ("" if ok else f"（HTTP {r.status_code if r is not None else '—'}）"))
    return all_ok, out


def check_finmind():
    if not TOKEN:
        return False, "沒有設定 FINMIND_TOKEN"
    if TOKEN != os.environ.get("FINMIND_TOKEN", ""):
        print("⚠️ FINMIND_TOKEN 前後有空白或換行，建議重新貼上（程式會自動忽略）")
    start = (datetime.utcnow() + timedelta(hours=8) - timedelta(days=20)).strftime("%Y-%m-%d")
    results = []
    for dataset, label in (("TaiwanStockPrice", "日K"),
                           ("TaiwanStockInstitutionalInvestorsBuySell", "法人"),
                           ("TaiwanStockMarginPurchaseShortSale", "融資")):
        try:
            r = requests.get("https://api.finmindtrade.com/api/v4/data",
                             params={"dataset": dataset, "data_id": "2330", "start_date": start},
                             headers={"Authorization": f"Bearer {TOKEN}"}, timeout=20)
            body = r.json()
        except Exception as e:
            return False, f"{label}：連線失敗 {e}"
        if r.status_code != 200:
            return False, f"{label}：HTTP {r.status_code} {body.get('msg', '')}"
        rows = body.get("data") or []
        last = max((x.get("date", "") for x in rows), default="—")
        results.append(f"{label} {len(rows)} 筆（最新 {last}）")
    # 額度（有些帳號才有這個欄位，拿不到就略過）
    try:
        u = requests.get("https://api.web.finmindtrade.com/v2/user_info",
                         headers={"Authorization": f"Bearer {TOKEN}"}, timeout=15).json()
        if u.get("user_count") is not None:
            results.append(f"本小時已用 {u.get('user_count')}/{u.get('api_request_limit')} 次")
    except Exception:
        pass
    return True, "、".join(results)


def main():
    fm_ok, fm_msg = check_finmind()
    print(("✅ FinMind 正常：" if fm_ok else "❌ FinMind 失敗：") + fm_msg, flush=True)
    site_ok, site_lines = check_site()
    print("網站（Render）部署狀態：", flush=True)
    for line in site_lines:
        print("   " + line, flush=True)
    if not site_ok:
        print("   ⚠️ 有功能回不出來：Render 可能還沒部署最新版（Render → Manual Deploy → Deploy latest commit）", flush=True)
    tg_ok = notify.configured()
    if tg_ok:
        sent = notify.send("🔌 <b>台股選股系統・連線測試</b>\n"
                           f"{'✅' if fm_ok else '❌'} FinMind：{notify.esc(fm_msg)}\n"
                           "網站：\n" + "\n".join(notify.esc(x) for x in site_lines) + "\n"
                           "✅ Telegram：收得到這則就代表設定正確")
        print("✅ Telegram 已推播" if sent else "❌ Telegram 推播失敗（token 或 chat id 有誤，見上方訊息）")
        tg_ok = sent
    else:
        print("❌ Telegram 尚未設定：GitHub Secrets 缺 TG_BOT_TOKEN 或 TG_CHAT_ID")
    sys.exit(0 if fm_ok and tg_ok else 1)


if __name__ == "__main__":
    main()
