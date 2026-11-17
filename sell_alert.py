"""
台股持倉賣出提醒（每日收盤後檢查，Telegram 推播）
=====================================================
檢查交易日誌裡「持倉中」的股票，符合下列任一條件就提醒賣出：
  一般持倉（AI推薦 / 三合一 / 自選…）
    1. 技術面破位：收盤跌破 MA20
    2. AI 轉看跌：最新 AI 分析中該股 rise_prob < 50%
    3. （附帶）已跌破設定的停損價
  左側持倉（來源 = 左側）：買在跌勢中，本來就在 MA20 下，不能用「跌破月線」判斷
    1. 跌破停損價（預設進場價 −8%）
    2. 回到 MA20 → 提醒先出一半
    3. 持有滿 20 個交易日仍未回到 MA20 → 時間停損

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
                    "select": "id,code,name,buy_price,buy_date,stop_loss,stop_loss_pct,source"},
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


LEFT_SOURCE = "左側"
LEFT_TIME_STOP_DAYS = 20     # 與 left_side.EXIT 一致
LEFT_STOP_PCT = 8


# ── 抓歷史算 MA20 與現價 ────────────────────────────
def fetch_ma20_and_price(code):
    """回傳 (現價, MA20)。抓不到回 (None, None)"""
    cur, ma20, _ = fetch_price_info(code)
    return cur, ma20


def fetch_price_info(code):
    """回傳 (現價, MA20, 交易日list)。抓不到回 (None, None, [])"""
    if not FINMIND_TOKEN:
        return None, None, []
    try:
        end_d = (datetime.utcnow() + timedelta(hours=8)).strftime("%Y-%m-%d")
        start_d = (datetime.utcnow() + timedelta(hours=8) - timedelta(days=60)).strftime("%Y-%m-%d")
        r = SESSION.get("https://api.finmindtrade.com/api/v4/data",
            params={"dataset": "TaiwanStockPrice", "data_id": code,
                    "start_date": start_d, "end_date": end_d},
            headers={"Authorization": f"Bearer {FINMIND_TOKEN}"}, timeout=15)
        if r.status_code != 200:
            return None, None, []
        rows = sorted((x for x in r.json().get("data", []) if safe_float(x.get("close", 0)) > 0),
                      key=lambda x: x.get("date", ""))
        closes = [safe_float(x.get("close", 0)) for x in rows]
        dates = [x.get("date", "")[:10] for x in rows]
        if len(closes) < 20:
            return (closes[-1] if closes else None), None, dates
        cur = closes[-1]
        ma20 = round(sum(closes[-20:]) / 20, 2)
        return cur, ma20, dates
    except Exception as e:
        log(f"⚠️ {code} 抓價失敗: {e}")
        return None, None, []


def trading_days_held(buy_date, dates):
    """買進後經過幾個交易日（買進日不算）。買進日早於資料起點 → 至少是資料筆數"""
    if not buy_date or not dates:
        return None
    if buy_date < dates[0]:
        return len(dates)
    return sum(1 for d in dates if d > buy_date)


def left_reasons(h, cur, ma20, dates):
    """左側持倉的出場訊號"""
    buy = safe_float(h.get("buy_price", 0))
    stop = safe_float(h.get("stop_loss", 0))
    if not stop and buy > 0:
        stop = round(buy * (1 - (safe_float(h.get("stop_loss_pct"), LEFT_STOP_PCT) or LEFT_STOP_PCT) / 100), 2)
    reasons = []
    if stop and cur < stop:
        reasons.append(f"跌破停損價({stop})")
        return reasons                                   # 停損優先，其他不用看
    if ma20 and cur >= ma20:
        reasons.append(f"已回到MA20({ma20})，可先出一半")
    held = trading_days_held(str(h.get("buy_date") or "")[:10], dates)
    if held is not None and held >= LEFT_TIME_STOP_DAYS and not (ma20 and cur >= ma20):
        reasons.append(f"持有{held}個交易日仍未反彈，時間停損")
    return reasons


def send_telegram(msg):
    import notify
    return notify.send(msg)


def _record(data):
    try:
        import notify
        notify.record("sell", data)
    except Exception:
        pass


def run():
    holdings = load_holdings()
    if not holdings:
        log("目前無持倉中的交易，結束")
        _record({"holdings": 0, "alerts": 0})
        return
    log(f"檢查 {len(holdings)} 檔持倉...")

    ai_probs = load_latest_ai()
    alerts = []

    for h in holdings:
        code = h.get("code", "")
        name = h.get("name", "") or code
        buy  = safe_float(h.get("buy_price", 0))
        stop = safe_float(h.get("stop_loss", 0))

        cur, ma20, dates = fetch_price_info(code)
        if cur is None:
            continue

        is_left = (h.get("source") or "") == LEFT_SOURCE
        if is_left:
            reasons = left_reasons(h, cur, ma20, dates)
            if reasons:
                pnl_pct = round((cur - buy) / buy * 100, 1) if buy > 0 else 0
                alerts.append({"name": name, "code": code, "cur": cur, "buy": buy,
                               "pnl_pct": pnl_pct, "reasons": reasons, "left": True})
                log(f"  ⚠️ [左側] {name}({code}) 觸發: {', '.join(reasons)}")
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
        _record({"holdings": len(holdings), "alerts": 0})
        return

    # 組 Telegram 訊息
    today = (datetime.utcnow() + timedelta(hours=8)).strftime("%Y-%m-%d")
    lines = [f"🔔 <b>賣出提醒</b> ({today})", "", f"以下 {len(alerts)} 檔觸發賣出訊號："]
    for a in alerts:
        pnl_icon = "🔴" if a["pnl_pct"] < 0 else "🟢"
        lines.append("")
        lines.append(f"<b>{a['name']}</b> ({a['code']}){' 🪃左側' if a.get('left') else ''}")
        lines.append(f"　現價 {a['cur']} / 成本 {a['buy']} {pnl_icon}{a['pnl_pct']:+.1f}%")
        lines.append(f"　訊號：{' + '.join(a['reasons'])}")
    lines.append("")
    lines.append("⚠️ 僅供參考，請依自身紀律判斷")

    msg = "\n".join(lines)
    ok = send_telegram(msg)
    log(f"Telegram 推播: {'✅ 成功' if ok else '❌ 失敗'}")
    _record({"holdings": len(holdings), "alerts": len(alerts), "sent": ok,
             "names": [f"{a['name']}({a['code']})" for a in alerts[:10]]})


if __name__ == "__main__":
    run()
