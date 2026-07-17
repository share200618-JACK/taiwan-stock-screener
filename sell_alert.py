"""
台股持倉賣出提醒（每日收盤後檢查，Telegram 推播）
=====================================================
檢查交易日誌裡「持倉中」的股票，符合下列任一條件就提醒賣出：
  1. 技術面破位：收盤跌破 MA20
  2. AI 轉看跌：最新 AI 分析中該股 rise_prob < 50%
  3. （附帶）已跌破設定的停損價

由 GitHub Actions 每日排程執行。
需要的環境變數（GitHub Secrets）：
  SUPABASE_URL, SUPABASE_KEY, FINMIND_TOKEN,
  TG_BOT_TOKEN, TG_CHAT_ID
"""
import os
import json
import requests
import urllib3
from datetime import datetime, timedelta

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

SUPABASE_URL  = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY  = os.environ.get("SUPABASE_KEY", "")
FINMIND_TOKEN = os.environ.get("FINMIND_TOKEN", "")
TG_BOT_TOKEN  = os.environ.get("TG_BOT_TOKEN", "")
TG_CHAT_ID    = os.environ.get("TG_CHAT_ID", "")

USER_TOKEN = "default"

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "Mozilla/5.0"})


def log(m): print(f"[{datetime.now().strftime('%H:%M:%S')}] {m}", flush=True)

def safe_float(x, d=0.0):
    try: return float(str(x).replace(",", "").replace("+", ""))
    except: return d

def sb_headers():
    return {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}",
            "Content-Type": "application/json"}


# ── 讀取持倉中的交易 ────────────────────────────────
def load_holdings():
    if not SUPABASE_URL or not SUPABASE_KEY:
        log("❌ 未設定 Supabase")
        return []
    try:
        r = SESSION.get(f"{SUPABASE_URL}/rest/v1/trade_journal",
            params={"user_token": f"eq.{USER_TOKEN}", "sell_price": "is.null",
                    "select": "id,code,name,buy_price,stop_loss,stop_loss_pct,source"},
            headers=sb_headers(), timeout=15)
        return r.json() if r.status_code == 200 else []
    except Exception as e:
        log(f"❌ 讀持倉失敗: {e}")
        return []


# ── 抓最新 AI 分析（判斷是否轉看跌）──────────────────
def load_latest_ai():
    """回傳 {code: rise_prob} 字典"""
    if not SUPABASE_URL or not SUPABASE_KEY:
        return {}
    try:
        r = SESSION.get(f"{SUPABASE_URL}/rest/v1/analysis_results",
            params={"select": "stocks,created_at", "order": "created_at.desc", "limit": "1"},
            headers=sb_headers(), timeout=15)
        if r.status_code == 200 and r.json():
            stocks = r.json()[0].get("stocks", [])
            if isinstance(stocks, str):
                stocks = json.loads(stocks)
            return {s.get("code"): safe_float(s.get("rise_prob", 50)) for s in stocks}
    except Exception as e:
        log(f"⚠️ 讀 AI 分析失敗: {e}")
    return {}


# ── 抓歷史算 MA20 與現價 ────────────────────────────
def fetch_ma20_and_price(code):
    """回傳 (現價, MA20)。抓不到回 (None, None)"""
    if not FINMIND_TOKEN:
        return None, None
    try:
        end_d = (datetime.utcnow() + timedelta(hours=8)).strftime("%Y-%m-%d")
        start_d = (datetime.utcnow() + timedelta(hours=8) - timedelta(days=60)).strftime("%Y-%m-%d")
        r = SESSION.get("https://api.finmindtrade.com/api/v4/data",
            params={"dataset": "TaiwanStockPrice", "data_id": code,
                    "start_date": start_d, "end_date": end_d},
            headers={"Authorization": f"Bearer {FINMIND_TOKEN}"}, timeout=15)
        if r.status_code != 200:
            return None, None
        closes = [safe_float(x.get("close", 0)) for x in r.json().get("data", []) if safe_float(x.get("close", 0)) > 0]
        if len(closes) < 20:
            return (closes[-1] if closes else None), None
        cur = closes[-1]
        ma20 = round(sum(closes[-20:]) / 20, 2)
        return cur, ma20
    except Exception as e:
        log(f"⚠️ {code} 抓價失敗: {e}")
        return None, None


def send_telegram(msg):
    if not TG_BOT_TOKEN or not TG_CHAT_ID:
        log("❌ 未設定 TG_BOT_TOKEN / TG_CHAT_ID，無法推播")
        return False
    try:
        r = SESSION.post(f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage",
            json={"chat_id": TG_CHAT_ID, "text": msg, "parse_mode": "HTML"}, timeout=15)
        return r.status_code == 200
    except Exception as e:
        log(f"❌ Telegram 推播失敗: {e}")
        return False


def run():
    holdings = load_holdings()
    if not holdings:
        log("目前無持倉中的交易，結束")
        return
    log(f"檢查 {len(holdings)} 檔持倉...")

    ai_probs = load_latest_ai()
    alerts = []

    for h in holdings:
        code = h.get("code", "")
        name = h.get("name", "") or code
        buy  = safe_float(h.get("buy_price", 0))
        stop = safe_float(h.get("stop_loss", 0))

        cur, ma20 = fetch_ma20_and_price(code)
        if cur is None:
            continue

        reasons = []
        # 條件1：跌破 MA20
        if ma20 and cur < ma20:
            reasons.append(f"跌破月線MA20({ma20})")
        # 條件2：AI 轉看跌
        prob = ai_probs.get(code)
        if prob is not None and prob < 50:
            reasons.append(f"AI轉看跌(上漲機率{prob:.0f}%)")
        # 條件3：跌破停損價
        if stop and cur < stop:
            reasons.append(f"跌破停損價({stop})")

        if reasons:
            pnl_pct = round((cur - buy) / buy * 100, 1) if buy > 0 else 0
            alerts.append({
                "name": name, "code": code, "cur": cur, "buy": buy,
                "pnl_pct": pnl_pct, "reasons": reasons
            })
            log(f"  ⚠️ {name}({code}) 觸發: {', '.join(reasons)}")

    if not alerts:
        log("✅ 所有持倉均未觸發賣出條件")
        return

    # 組 Telegram 訊息
    today = (datetime.utcnow() + timedelta(hours=8)).strftime("%Y-%m-%d")
    lines = [f"🔔 <b>賣出提醒</b> ({today})", "", f"以下 {len(alerts)} 檔觸發賣出訊號："]
    for a in alerts:
        pnl_icon = "🔴" if a["pnl_pct"] < 0 else "🟢"
        lines.append("")
        lines.append(f"<b>{a['name']}</b> ({a['code']})")
        lines.append(f"　現價 {a['cur']} / 成本 {a['buy']} {pnl_icon}{a['pnl_pct']:+.1f}%")
        lines.append(f"　訊號：{' + '.join(a['reasons'])}")
    lines.append("")
    lines.append("⚠️ 僅供參考，請依自身紀律判斷")

    msg = "\n".join(lines)
    ok = send_telegram(msg)
    log(f"Telegram 推播: {'✅ 成功' if ok else '❌ 失敗'}")


if __name__ == "__main__":
    run()
