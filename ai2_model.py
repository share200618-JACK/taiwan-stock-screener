"""
AI 2.0：全市場模型（一個模型學所有股票，取代「每支股票各自訓練」）

為什麼：舊模型每支股票只用自己 2 年資料訓練，樣本太少，實戰「上漲機率」反而越高越差。
做法：
- 特徵（收盤後可得）：動能／反轉（1、5、20、60 日、120~20 日）、波動、離 52 週高點、均線乖離、
  量能變化、成交金額（規模）、K 棒型態、本益比／淨值比／殖利率、外資／投信／自營買賣超（5、20 日）
  每天在可交易股票中轉成百分位排名（不受大盤漲跌影響）
- 目標：隔天開盤買、持有 H 天的「超額報酬」（減同日全體平均）排名
- 可交易：個股、股價 ≥ 10、20 日均成交 ≥ 3,000 萬、上市滿 60 天；隔天開盤漲停買不到 → 不算
- 走動式驗證：每 20 個交易日重訓一次，只用「當時已經知道結果」的資料（標籤期間不重疊），
  預測接下來 20 天 —— 每一筆績效都是模型沒看過的
- 自動選設定：8 組設定（持有 5／20 天 × 特徵組 × 複雜度）先在測試期前半選最好的，
  再看它在後半（完全沒參與挑選）的成績；另外和 6 個簡單因子比，確認機器學習真的有加分

模式（AI2_MODE）：study＝回測研究
"""
import os
import sys
import json
import glob
import gzip
import math
import time
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

CACHE = os.environ.get("AI2_CACHE", "ai2_cache")
MODE = os.environ.get("AI2_MODE", "study")
DAYS = int(os.environ.get("AI2_DAYS", "650") or 650)
COST = 0.5                  # 來回成本 %（手續費＋證交稅，含折扣後約 0.4~0.6）
RETRAIN = 20
WARMUP = 120                # 特徵暖身
MIN_TRAIN_DAYS = 250        # 第一次訓練至少要有的交易日
MIN_PRICE, MIN_AMT = 10.0, 3e7
F = ["o", "h", "l", "c", "ref", "vol", "amt", "pe", "pb", "yld", "fi", "it", "dl", "mkt"]

PX_FEATS = ["r1", "r5", "r20", "r60", "m120_20", "vol20", "max20", "hi240", "ma20", "ma60", "v5_60", "lamt",
            "clv", "gap", "up_sh"]
FUND_FEATS = ["ep", "bp", "yld", "fi1", "fi5", "fi20", "it1", "it5", "it20", "dl5", "mkt"]
CONFIGS = [{"h": h, "feats": fs, "leaves": lv}
           for h in (5, 20) for fs in ("px", "all") for lv in (15, 63)]
BASELINES = {"動能(120~20日)": ("m120_20", 1), "短線反轉(5日跌)": ("r5", -1), "近52週高": ("hi240", 1),
             "低波動": ("vol20", -1), "低本益比": ("ep", 1), "外資20日買超": ("fi20", 1)}


def log(m):
    print(f"[{datetime.utcnow() + timedelta(hours=8):%H:%M:%S}] {m}", flush=True)


# ── 讀資料 → 寬表（日期 × 代號）────────────────────────
def load_wide(n=DAYS, cache=CACHE):
    files = sorted(glob.glob(os.path.join(cache, "day", "*.json.gz")))[-n:]
    days, recs = [], []
    for i, p in enumerate(files):
        with gzip.open(p, "rt", encoding="utf-8") as f:
            d = json.load(f)
        days.append(os.path.basename(p)[:10])
        for code, row in d.items():
            recs.append((i, code, *[np.nan if v is None else float(v) for v in row]))
    df = pd.DataFrame.from_records(recs, columns=["di", "code", *F])
    W = {}
    for f in F:
        w = df.pivot(index="di", columns="code", values=f)
        w.index = pd.Index(days, name="day")
        W[f] = w
    codes = W["c"].columns
    for f in F:
        W[f] = W[f].reindex(columns=codes)
    return days, W


# ── 特徵 ──────────────────────────────────────────────
def build(W):
    c, ref, o, h, l = W["c"], W["ref"], W["o"], W["h"], W["l"]
    vol, amt = W["vol"].fillna(0), W["amt"].fillna(0)
    r = (c / ref - 1).where(ref > 0)
    r = r.clip(-0.5, 0.5)
    P = (1 + r.fillna(0)).cumprod()                         # 還原價格指數
    traded = c.notna() & (vol > 0)
    hist = traded.cumsum()
    X = {}
    X["r1"] = r
    for n in (5, 20, 60):
        X[f"r{n}"] = P / P.shift(n) - 1
    X["m120_20"] = P.shift(20) / P.shift(120) - 1
    X["vol20"] = r.rolling(20, min_periods=15).std()
    X["max20"] = r.rolling(20, min_periods=15).max()
    X["hi240"] = P / P.rolling(240, min_periods=120).max()
    X["ma20"] = P / P.rolling(20, min_periods=15).mean() - 1
    X["ma60"] = P / P.rolling(60, min_periods=40).mean() - 1
    v5, v60 = vol.rolling(5).mean(), vol.rolling(60, min_periods=40).mean()
    X["v5_60"] = (v5 / v60).where(v60 > 0)
    a20 = amt.rolling(20, min_periods=15).mean()
    X["lamt"] = np.log(a20.where(a20 > 0))
    rng = (h - l)
    X["clv"] = ((c - l) / rng).where(rng > 0)
    X["gap"] = (o / ref - 1).where(ref > 0)
    X["up_sh"] = ((h - np.maximum(o, c)) / ref).where(ref > 0)
    pe, pb = W["pe"], W["pb"]
    X["ep"] = (1 / pe).where(pe > 0)
    X["bp"] = (1 / pb).where(pb > 0)
    X["yld"] = W["yld"]
    v20 = vol.rolling(20, min_periods=15).mean().replace(0, np.nan)
    for k in ("fi", "it", "dl"):
        s = W[k].fillna(0)
        X[f"{k}5"] = s.rolling(5).sum() / v20
        if k != "dl":
            X[f"{k}1"] = s / v20
        if k != "dl":
            X[f"{k}20"] = s.rolling(20, min_periods=15).sum() / v20
    X["mkt"] = W["mkt"]
    codes = c.columns
    is_stock = pd.Series([len(x) == 4 and x[0] != "0" for x in codes], index=codes)
    uni = traded & (c >= MIN_PRICE) & (a20 >= MIN_AMT) & (hist >= 60) & is_stock.values[None, :]
    return P, r, uni, X


def forward(W, P, h):
    """隔天開盤買、第 h 天收盤賣的報酬（%）；隔天開盤漲停（買不到）→ NaN"""
    o1, ref1 = W["o"].shift(-1), W["ref"].shift(-1)
    entry = P * (o1 / ref1)
    fwd = (P.shift(-h) / entry - 1) * 100
    locked = o1 >= ref1 * 1.095
    return fwd.where(~locked & (o1 > 0))


def to_long(X, uni, names):
    """每天在可交易股票中轉百分位 → (列索引, 欄索引, 特徵表)"""
    rows, cols = np.nonzero(uni.values)
    data = {}
    for k in names:
        v = X[k].where(uni)
        v = v.rank(axis=1, pct=True) if k != "mkt" else v
        data[k] = v.values[rows, cols]
    return rows, cols, pd.DataFrame(data)


def excess(W, P, uni, h):
    fwd = forward(W, P, h)
    ex = fwd.where(uni)
    return fwd, ex.sub(ex.mean(axis=1), axis=0)


def fit_model(Xtr, ytr, leaves):
    import lightgbm as lgb
    m = lgb.LGBMRegressor(n_estimators=300, learning_rate=0.05, num_leaves=leaves, min_child_samples=500,
                          subsample=0.7, subsample_freq=1, colsample_bytree=0.8, reg_lambda=1.0,
                          random_state=7, n_jobs=-1, verbose=-1)
    m.fit(Xtr, ytr)
    return m


def feat_names(cfg):
    return PX_FEATS + (FUND_FEATS if cfg["feats"] == "all" else [])


# ── 走動式驗證 ────────────────────────────────────────
def walk_forward(days, W, P, uni, X, cfg, LX=None):
    """每 RETRAIN 天重訓；訓練只用標籤已實現的資料（t ≤ 重訓日 − h − 1）"""
    h = cfg["h"]
    rows, cols, L = LX if LX is not None else to_long(X, uni, PX_FEATS + FUND_FEATS)
    A = L[feat_names(cfg)].values
    fwd, ex = excess(W, P, uni, h)
    y = ex.where(uni).rank(axis=1, pct=True).values[rows, cols]
    S = np.full(uni.shape, np.nan)
    for t0 in range(WARMUP + MIN_TRAIN_DAYS, len(days), RETRAIN):
        tr = (rows >= WARMUP) & (rows <= t0 - h - 1) & ~np.isnan(y) & (rows % 2 == 0)
        te = (rows >= t0) & (rows < t0 + RETRAIN)
        if tr.sum() < 5000 or te.sum() == 0:
            continue
        m = fit_model(A[tr], y[tr], cfg["leaves"])
        S[rows[te], cols[te]] = m.predict(A[te])
    return pd.DataFrame(S, index=days, columns=uni.columns), fwd, ex


def _P0050(W):
    c, ref = W["c"]["0050"], W["ref"]["0050"]
    r = (c / ref - 1).clip(-0.5, 0.5).fillna(0)
    return (1 + r).cumprod(), W["o"]["0050"].shift(-1), W["ref"]["0050"].shift(-1)


def forward_0050(W, h):
    P, o1, ref1 = _P0050(W)
    return (P.shift(-h) / (P * o1 / ref1) - 1) * 100


def stats(df, h):
    """以「月」為單位算 t 值（持有期重疊，逐日樣本不獨立）"""
    if df is None or len(df) < 20:
        return None
    m = df.assign(mon=df["day"].str[:7]).groupby("mon")
    mon = m["ex"].mean()
    t = mon.mean() / (mon.std(ddof=1) or 1e-9) * math.sqrt(len(mon)) if len(mon) > 2 else float("nan")
    ic_m = m["ic"].mean()
    out = {"days": len(df), "months": len(mon), "ex": round(df["ex"].mean(), 2), "raw": round(df["raw"].mean(), 2),
           "win": round((df["ex"] > 0).mean() * 100, 1), "t": round(t, 2),
           "pos_months": round((mon > 0).mean() * 100, 0), "ic": round(df["ic"].mean(), 3),
           "ic_t": round(ic_m.mean() / (ic_m.std(ddof=1) or 1e-9) * math.sqrt(len(ic_m)), 2) if len(ic_m) > 2 else None,
           "per_year": round(df["ex"].mean() * 250 / h, 1)}
    if "b0050" in df and df["b0050"].notna().any():
        out["vs0050"] = round((df["raw"] - df["b0050"]).mean(), 2)
    return out


def baseline_scores(X, uni, name):
    feat, sign = BASELINES[name]
    return (X[feat].where(uni) * sign).rank(axis=1, pct=True)


# ── 研究 ──────────────────────────────────────────────
def run_study():
    t0 = time.time()
    days, W = load_wide()
    log(f"資料 {days[0]} ～ {days[-1]}（{len(days)} 天、{W['c'].shape[1]} 檔）")
    P, r, uni, X = build(W)
    log(f"可交易股票每天約 {int(uni.sum(axis=1).iloc[-60:].mean())} 檔；特徵 {len(PX_FEATS) + len(FUND_FEATS)} 個")
    LX = to_long(X, uni, PX_FEATS + FUND_FEATS)
    first = days[WARMUP + MIN_TRAIN_DAYS]
    test_days = [d for d in days if d >= first]
    mid = test_days[len(test_days) // 2]
    log(f"測試期 {first} ～ {days[-1]}；前半（挑設定）到 {mid}、後半（驗收）{mid} 之後")
    b0050 = {h: forward_0050(W, h) for h in (5, 20)} if "0050" in W["c"].columns else {}
    res = {"from": first, "mid": mid, "to": days[-1], "configs": [], "baselines": []}
    best = best_ic = None
    SS, FX = {}, {}
    for cfg in CONFIGS:
        S, fwd, ex = walk_forward(days, W, P, uni, X, cfg, LX=LX)
        SS[cfg_key(cfg)] = S
        FX.setdefault(cfg["h"], (fwd, ex))
        for k in (5, 10):
            df = evaluate_simple(S, fwd, ex, test_days, cfg["h"], k, b0050.get(cfg["h"]))
            a, b = df[df["day"] < mid], df[df["day"] >= mid]
            row = {**cfg, "k": k, "all": stats(df, cfg["h"]), "first": stats(a, cfg["h"]), "second": stats(b, cfg["h"])}
            res["configs"].append(row)
            if k == 5 and row["first"] and (best is None or row["first"]["ex"] / cfg["h"] > best["first"]["ex"] / best["h"]):
                best = row
            if k == 5 and row["first"] and (best_ic is None or row["first"]["ic"] > best_ic["first"]["ic"]):
                best_ic = row
        log(f"  設定 {cfg} 完成（{(time.time() - t0) / 60:.0f} 分鐘）")
    # 綜合（多組設定的排名平均）：不挑單一設定，降低「挑到剛好最好」的風險
    res["ensembles"] = []
    ens = {"綜合(全部8組)": list(SS), "綜合(持有20天4組)": [k for k in SS if k.startswith("h20")],
           "綜合(持有5天4組)": [k for k in SS if k.startswith("h5")]}
    for name, keys in ens.items():
        E = sum(SS[k].rank(axis=1, pct=True) for k in keys) / len(keys)
        for h in (5, 20):
            fwd, ex = FX[h]
            for k in (5, 10, 20, 50):
                df = evaluate_simple(E, fwd, ex, test_days, h, k, b0050.get(h))
                a, b = df[df["day"] < mid], df[df["day"] >= mid]
                res["ensembles"].append({"name": name, "h": h, "k": k, "all": stats(df, h), "first": stats(a, h),
                                         "second": stats(b, h)})
            sp = spread(E, ex, test_days)
            res["ensembles"].append({"name": name, "h": h, "k": "前10%−後10%", "all": stats(sp, h),
                                     "first": stats(sp[sp["day"] < mid], h), "second": stats(sp[sp["day"] >= mid], h)})
    res["best_ic"] = best_ic
    for name in BASELINES:
        S = baseline_scores(X, uni, name)
        for h in (5, 20):
            fwd, ex = excess(W, P, uni, h)
            df = evaluate_simple(S, fwd, ex, test_days, h, 5, b0050.get(h))
            a, b = df[df["day"] < mid], df[df["day"] >= mid]
            res["baselines"].append({"name": name, "h": h, "all": stats(df, h), "first": stats(a, h), "second": stats(b, h)})
    res["best"] = best
    report(res)
    print("AI2STUDY " + json.dumps(res, ensure_ascii=False, default=float))
    return res


def cfg_key(cfg):
    return f"h{cfg['h']}_{cfg['feats']}_{cfg['leaves']}"


def spread(S, ex, days):
    """前 10% 減後 10% 的超額（不扣成本，看模型排序力）"""
    rows = []
    for d in days:
        s = S.loc[d].dropna()
        e = ex.loc[d].reindex(s.index)
        s = s[e.notna().values]
        if len(s) < 100:
            continue
        n = max(len(s) // 10, 1)
        top, bot = s.nlargest(n).index, s.nsmallest(n).index
        rows.append({"day": d, "ex": float(ex.loc[d, top].mean() - ex.loc[d, bot].mean()) + COST, "raw": np.nan,
                     "b0050": np.nan, "ic": float(s.corr(e.reindex(s.index), method="spearman"))})
    return pd.DataFrame(rows)


def evaluate_simple(S, fwd, ex, days, h, k, b0050=None):
    rows = []
    for d in days:
        if d not in S.index:
            continue
        s = S.loc[d].dropna()
        if len(s) < 50:
            continue
        f = fwd.loc[d]
        s = s[f.reindex(s.index).notna().values]
        if len(s) < 50:
            continue
        top = s.nlargest(k).index
        e = ex.loc[d]
        rows.append({"day": d, "ex": float(e[top].mean()) - COST, "raw": float(f[top].mean()) - COST,
                     "b0050": float(b0050.loc[d]) if b0050 is not None and pd.notna(b0050.loc[d]) else np.nan,
                     "ic": float(s.corr(e.reindex(s.index), method="spearman"))})
    return pd.DataFrame(rows)


def _fmt(s, h):
    if not s:
        return "—"
    v = f"｜比0050 {s['vs0050']:+.2f}%" if s.get("vs0050") is not None else ""
    return (f"超額 {s['ex']:+.2f}%/筆（年化約 {s['per_year']:+.0f}%）勝率 {s['win']}% 月t={s['t']} "
            f"正月份 {s['pos_months']:.0f}% IC {s['ic']:+.3f}(t={s['ic_t']}){v}")


def report(res):
    log(f"===== AI 2.0 走動式驗證 {res['from']} ～ {res['to']}（已扣來回成本 {COST}%）=====")
    log("── 模型設定（每天前 k 檔，持有 h 天）")
    for c in res["configs"]:
        if c["k"] != 5:
            continue
        log(f"  h={c['h']:>2} 特徵={c['feats']:<3} 葉={c['leaves']:<2} 前5：前半 {_fmt(c['first'], c['h'])}")
        log(f"        後半 {_fmt(c['second'], c['h'])}")
    log("── 綜合模型（不挑設定，多組平均）")
    for e in res.get("ensembles", []):
        log(f"  {e['name']:<14} 持有{e['h']:>2}天 前{e['k']}：全期 {_fmt(e['all'], e['h'])}")
        log(f"        前半 {_fmt(e['first'], e['h'])}｜後半 {_fmt(e['second'], e['h'])}")
    bi = res.get("best_ic")
    if bi:
        log(f"★ 前半用 IC 挑中：h={bi['h']} 特徵={bi['feats']} 葉={bi['leaves']} → 後半 {_fmt(bi['second'], bi['h'])}")
    log("── 簡單因子（前 5 檔）")
    for b in res["baselines"]:
        log(f"  {b['name']:<10} h={b['h']:>2}：全期 {_fmt(b['all'], b['h'])}｜後半 {_fmt(b['second'], b['h'])}")
    b = res.get("best")
    if b:
        log(f"★ 前半自動挑中：h={b['h']} 特徵={b['feats']} 葉={b['leaves']} → 後半（沒參與挑選）{_fmt(b['second'], b['h'])}")


if __name__ == "__main__":
    if MODE == "study":
        run_study()
    else:
        log(f"模式 {MODE} 尚未啟用")
