"""
每日 Telegram 報告（GitHub Actions 每日排程的最後一步，前面步驟失敗也會跑）
=========================================================================
把當天各步驟寫在 run_status.json 的結果整理成一則訊息：
  AI 前 5 名、籌碼左側名單、持倉提醒、前 5 名一個月成績、選股條件狀態、系統警示
賣出提醒與選股條件調整仍會「另外」即時推播，這則是每日總覽。

用法：python daily_report.py          → 推播今日報告
      python daily_report.py --test   → 推播一則測試訊息（確認 Telegram 設定）
"""
import os
import sys
from datetime import datetime, timedelta

import notify
from notify import esc

SITE_URL = os.environ.get("SITE_URL", "https://taiwan-stock-screener-783z.onrender.com").rstrip("/")
WEEK = "一二三四五六日"
STEP_NAMES = {"AI_OUTCOME": "AI 選股分析", "SELL_OUTCOME": "持倉賣出提醒", "SIGNALS_OUTCOME": "買進訊號",
              "DIV_OUTCOME": "存股＋營運動能", "WATCH_OUTCOME": "追蹤股提醒", "MCALL_OUTCOME": "融資追繳統計", "PERF_OUTCOME": "績效計算",
              "WAVE_OUTCOME": "正2 波浪分析"}


def _num(v, fmt="{:+.1f}", unit="%"):
    return "—" if v is None else fmt.format(v) + unit


def build(status, outcomes, now=None):
    now = now or (datetime.utcnow() + timedelta(hours=8))
    ai, left, sell, perf, wave = (status.get(k, {}) for k in ("ai", "left", "sell", "perf", "wave"))
    failed = [STEP_NAMES[k] for k, v in outcomes.items() if v in ("failure", "cancelled")]
    L = [f"📊 <b>台股選股日報</b> {now.month}/{now.day}（{WEEK[now.weekday()]}）"]
    if failed:
        L.append(f"⚠️ 有步驟沒跑完：{'、'.join(failed)}")
    elif ai.get("minutes") is not None:
        L.append(f"✅ 排程完成・AI 分析 {ai['minutes']} 分鐘（{ai.get('analyzed', 0)}/{ai.get('planned', 0)} 支）")

    # AI 前 5 名
    top = ai.get("top5") or []
    L += ["", "🤖 <b>AI 前 5 名</b>（漲機率｜勝率優勢）"]
    if top:
        for i, s in enumerate(top, 1):
            L.append(f"{i}. {esc(s.get('code'))} {esc(s.get('name'))} {s.get('price')} 元｜"
                     f"{s.get('rise_prob')}%｜{(s.get('edge') or 0):+.1f}{' 📈營收創高' if s.get('rev_hot') else ''}")
        if ai.get("edge_filter") is False:
            L.append("<i>（今天沒有股票通過勝率門檻，以上為原始排序，參考性較低）</i>")
        if ai.get("strategy"):
            L.append(f"<i>條件：{esc(ai['strategy'])}</i>")
    else:
        L.append("今天沒有產生推薦（分析失敗或沒有有效結果）")

    # 籌碼左側
    L += ["", "🪃 <b>籌碼左側</b>（觀察中）"]
    if left.get("n"):
        for i, p in enumerate(left.get("picks") or [], 1):
            L.append(f"{i}. {esc(p.get('code'))} {esc(p.get('name'))} {p.get('price')}｜"
                     f"停損 {p.get('stop')}｜目標 {p.get('target')}")
    elif left:
        L.append(f"今天沒有符合的股票（檢查 {left.get('scanned', 0)} 支，差一個條件 {left.get('near_miss', 0)} 支）")
    else:
        L.append("今天沒有執行")

    # 持倉
    sig = status.get("signals", {})
    if sig and not sig.get("error"):
        L += ["", f"📣 <b>買進訊號</b>：改良版條件 {sig.get('buys', 0)} 檔"
                  f"{'（已另外推播）' if sig.get('sent') else ''}"
                  + (f"：{esc('、'.join(sig.get('names') or []))}" if sig.get("names") else "")
                  + (f"｜遊牧民爆量 {sig.get('watch', 0)}、量縮陰線 {sig.get('pullback', 0)}" if sig.get("nomad_on") else "")]
    dv = status.get("div", {})
    if dv and not dv.get("error"):
        tr = dv.get("track") or {}
        L += ["", f"🏦 <b>存股＋營運動能</b>：今天符合 {dv.get('picks', 0)} 檔、新推薦 {dv.get('new', 0)} 檔"
                  + (f"：{esc('、'.join(dv.get('names') or []))}" if dv.get("names") else "")
                  + (f"｜累計 {tr['n']} 筆到期 勝率 {tr['win']}% 平均 {tr['avg']:+}%（0050 {tr['bench']}%）" if tr.get("n")
                     else "｜實際成績：第一批推薦約 3 個月後到期")]
    mc = status.get("mcall", {})
    if mc.get("date") and not mc.get("error"):
        d = lambda x: f"{int(x[5:7])}/{int(x[8:10])}"
        L += ["", f"🧯 <b>融資追繳</b>（{d(mc['date'])}）：全市場維持率 {mc['keep']:.2f}%"
                  + (f"（{mc['keep_chg']:+.2f}）" if mc.get("keep_chg") is not None else "")
                  + f"｜追繳 {mc['below']:,} 戶、應補繳 {mc['call_amt'] / 1e8:,.2f} 億"
                  + f"｜{d(mc['exe_day'])} 開盤斷頭 {mc['exe']:,} 戶、{mc['exe_amt'] / 1e8:,.2f} 億"]
    wt = status.get("watch", {})
    if wt.get("n") and not wt.get("error"):
        L += ["", f"👁️ <b>追蹤股</b>：{wt['n']} 檔中 {wt.get('hits', 0)} 檔有提醒"
                  + (f"{'（已另外推播）' if wt.get('sent') else ''}：{esc('、'.join(wt.get('names') or []))}"
                     if wt.get("hits") else "")]
    if sell:
        if not sell.get("holdings"):
            L += ["", "🔔 <b>持倉</b>：目前沒有持倉中的交易"]
        elif sell.get("alerts"):
            L += ["", f"🔔 <b>持倉</b>：{sell['holdings']} 檔中 {sell['alerts']} 檔觸發賣出訊號"
                      f"{'（已另外推播）' if sell.get('sent') else ''}：{esc('、'.join(sell.get('names') or []))}"]
        else:
            L += ["", f"🔔 <b>持倉</b>：{sell['holdings']} 檔都沒有觸發賣出訊號"]

    # 前 5 名一個月成績＋選股條件
    t5 = perf.get("top5") or {}
    L += ["", "🎯 <b>前 5 名一個月成績</b>（持有 20 交易日 vs 0050）"]
    if t5.get("n"):
        L.append(f"到期 {t5['n']} 筆｜勝率 {t5.get('win_rate')}%｜平均 {_num(t5.get('avg_return'))}｜"
                 f"超額 {_num(t5.get('avg_excess'))}｜贏 0050 {_num(t5.get('beat_bench'), '{:.0f}')}")
    elif t5.get("total"):
        L.append(f"已記錄 {t5['total']} 筆，每筆追蹤滿 20 個交易日才計分")
    else:
        L.append("尚無資料")
    tun = perf.get("tuning") or {}
    if tun.get("status") == "changed":
        L.append(f"🔧 <b>選股條件已自動調整</b>：{esc(tun.get('describe_new') or '')}")
    elif tun.get("message"):
        icon = {"collecting": "⏳", "cooldown": "🕒", "kept": "✅", "locked": "🔒"}.get(tun.get("status"), "ℹ️")
        L.append(f"{icon} 選股條件：{esc(tun['message'])}")

    # 正2 波浪（θ 8%）
    wg = wave.get("groups") or {}
    if wg:
        L += ["", "🌊 <b>正2 波浪</b>（θ 8%，合成正2 持有 60 日）"]
        for g in wg.values():
            if not g.get("state_name"):
                continue
            tag = "🆕 今天確認：" if g.get("new_today") else f"{(g.get('pivot_date') or '')[5:].replace('-', '/')} "
            stat = (f"｜過去勝率 {g['win60']}%（任意日 {g.get('base_win60')}%）n={g.get('n60')}"
                    if g.get("win60") is not None else "")
            L.append(f"・{esc(g['name'])}：{tag}{esc(g['state_name'])}，"
                     f"目前{'上漲' if g.get('trend') == 'up' else '下跌'}段 {_num(g.get('since_pivot_pct'))}{stat}")
            vname = {"effective": "穩定有效", "lean": "傾向較好", "avoid": "應該避開", "lean_avoid": "傾向較差"}
            for t in g.get("tech_today") or []:
                L.append(f"  📐 今天出現「{esc(t['name'])}」（{vname.get(t['verdict'], '')}，"
                         f"過去 60 日勝率 {t.get('win60')}% vs 任意日 {g.get('tech_base_win60')}%）")
    if wave and wave.get("saved") is False:
        L.append("<i>（正2 波浪結果沒存進 Supabase，請建立 etf_wave_results 表）</i>")

    # 系統警示
    warn = []
    errs = ai.get("fm_errors") or {}
    if ai.get("fm_token_bad") or perf.get("token_bad"):
        warn.append("<b>FinMind token 無效</b>（Token is illegal）→ 法人/融資資料抓不到、上櫃股票無法分析、"
                    "左側選股不會有結果。請到 FinMind 網站複製新的 API token，更新 GitHub Secrets 的 FINMIND_TOKEN")
    elif errs:
        first = next(iter(errs.items()))
        warn.append(f"FinMind 抓資料失敗（{esc(first[0])}：{esc(first[1])}）→ 法人/融資/大盤資料可能缺，"
                    "請檢查 GitHub Secrets 的 FINMIND_TOKEN")
    if ai.get("fm_quota_out"):
        warn.append("FinMind 每小時額度用完，部分股票改用備援資料")
    if ai and ai.get("market_rows") == 0:
        warn.append("大盤資料 0 筆（相對強弱特徵沒有作用）")
    if ai.get("candidates_saved") is False:
        warn.append("候選股沒存進 Supabase（請建立 ai_candidates 表），選股條件無法自動調整")
    if perf and not perf.get("token_bad") and perf.get("fm_calls") and perf.get("fm_empty", 0) >= max(3, perf["fm_calls"] * 0.5):
        warn.append(f"績效計算向 FinMind 抓價 {perf['fm_calls']} 次有 {perf['fm_empty']} 次沒資料")
    if sell.get("alerts") and sell.get("sent") is False:
        warn.append("賣出提醒推播失敗")
    if warn:
        L += ["", "⚠️ <b>系統警示</b>"] + [f"・{w}" for w in warn]

    run_url = ""
    if os.environ.get("GITHUB_RUN_ID") and os.environ.get("GITHUB_REPOSITORY"):
        run_url = f"https://github.com/{os.environ['GITHUB_REPOSITORY']}/actions/runs/{os.environ['GITHUB_RUN_ID']}"
    links = [f'<a href="{SITE_URL}/analyze">AI 分析</a>', f'<a href="{SITE_URL}/backtest">回測績效</a>',
             f'<a href="{SITE_URL}/etf-wave">正2 波浪</a>']
    if run_url:
        links.append(f'<a href="{run_url}">執行紀錄</a>')
    L += ["", "🔗 " + "｜".join(links)]
    return "\n".join(L)


def main():
    if "--test" in sys.argv:
        ok = notify.send("✅ <b>台股選股系統</b>\nTelegram 設定成功！之後每個交易日排程跑完會收到一則日報，"
                         "持倉觸發賣出訊號、選股條件自動調整時也會另外通知。")
        sys.exit(0 if ok else 1)
    outcomes = {k: os.environ.get(k, "") for k in STEP_NAMES}
    msg = build(notify.load_status(), outcomes)
    print(msg)
    notify.send(msg)


if __name__ == "__main__":
    main()
