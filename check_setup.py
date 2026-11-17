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
    tg_ok = notify.configured()
    if tg_ok:
        sent = notify.send("🔌 <b>台股選股系統・連線測試</b>\n"
                           f"{'✅' if fm_ok else '❌'} FinMind：{notify.esc(fm_msg)}\n"
                           "✅ Telegram：收得到這則就代表設定正確")
        print("✅ Telegram 已推播" if sent else "❌ Telegram 推播失敗（token 或 chat id 有誤，見上方訊息）")
        tg_ok = sent
    else:
        print("❌ Telegram 尚未設定：GitHub Secrets 缺 TG_BOT_TOKEN 或 TG_CHAT_ID")
    sys.exit(0 if fm_ok and tg_ok else 1)


if __name__ == "__main__":
    main()
