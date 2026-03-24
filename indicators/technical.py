"""
69+ technical indicator calculations for BTC backtesting.

Usage:
    import pandas as pd
    from indicators.technical import calculate_indicators

    df = pd.read_csv("data/btc_1h.csv")
    df = calculate_indicators(df)
    print(df.columns.tolist())  # see all available indicators
"""

import numpy as np
import pandas as pd
import warnings


def calculate_indicators(df):
    """Calculate all technical indicators on a DataFrame with OHLCV columns.

    Required columns: close, volume
    Optional columns: open, high, low (will be inferred if missing)

    Returns the DataFrame with ~200+ indicator columns added.
    """
    warnings.filterwarnings("ignore", message=".*DataFrame is highly fragmented.*")

    _c_close = pd.to_numeric(df["close"], errors="coerce")
    if "open" not in df.columns:
        df["open"] = _c_close.shift(1).fillna(_c_close)
    _c_open = pd.to_numeric(df["open"], errors="coerce")
    if "high" not in df.columns:
        df["high"] = pd.concat([_c_open, _c_close], axis=1).max(axis=1)
    if "low" not in df.columns:
        df["low"] = pd.concat([_c_open, _c_close], axis=1).min(axis=1)

    # ---- SMA ----
    SMA_PERIODS = (5, 7, 10, 14, 20, 21, 25, 30, 50, 60, 90, 100, 120, 150, 180, 200, 210, 250, 300, 350, 400, 500)
    for p in SMA_PERIODS:
        df[f"ma{p}"] = df["close"].rolling(int(p)).mean()

    # ---- EMA ----
    EMA_PERIODS = (5, 7, 8, 9, 10, 12, 13, 20, 21, 25, 26, 30, 50, 55, 60, 90, 100, 120, 150, 200, 210, 250)
    for p in EMA_PERIODS:
        df[f"ema{p}"] = df["close"].ewm(span=int(p), adjust=False).mean()

    # ---- RSI ----
    _rd = df["close"].diff()
    _rg = _rd.where(_rd > 0, 0.0)
    _rl = (-_rd.where(_rd < 0, 0.0))
    for p in (6, 7, 9, 14, 21, 25, 28):
        _ag = _rg.rolling(window=int(p)).mean()
        _al = _rl.rolling(window=int(p)).mean()
        _rs = _ag / _al
        df[f"rsi{p}"] = 100 - (100 / (1 + _rs))
    df["rsi"] = df["rsi14"]

    # ---- Bollinger Bands ----
    BB_CFG = [
        (10, 1.5), (10, 2.0), (10, 2.5), (10, 3.0),
        (20, 1.5), (20, 2.0), (20, 2.5), (20, 3.0),
        (30, 1.5), (30, 2.0), (30, 2.5), (30, 3.0),
        (50, 1.5), (50, 2.0), (50, 2.5), (50, 3.0),
    ]
    for bp, bm in BB_CFG:
        suf = f"{int(bp)}_{str(bm).replace('.', '')}"
        mid = df["close"].rolling(int(bp)).mean()
        std = df["close"].rolling(int(bp)).std()
        df[f"bb_upper_{suf}"] = mid + float(bm) * std
        df[f"bb_lower_{suf}"] = mid - float(bm) * std
        df[f"bb_middle_{suf}"] = mid
    df["bb_middle"] = df["close"].rolling(20).mean()
    df["bb_std"] = df["close"].rolling(20).std()
    df["bb_upper"] = df["bb_middle"] + 2 * df["bb_std"]
    df["bb_lower"] = df["bb_middle"] - 2 * df["bb_std"]

    # ---- MACD ----
    MACD_CFG = [(12, 26, 9), (8, 21, 5), (8, 17, 9), (5, 35, 5)]
    for mf, ms, msg in MACD_CFG:
        ef = df["close"].ewm(span=int(mf), adjust=False).mean()
        es = df["close"].ewm(span=int(ms), adjust=False).mean()
        sfx = f"{int(mf)}_{int(ms)}_{int(msg)}"
        df[f"macd_{sfx}"] = ef - es
        df[f"macd_signal_{sfx}"] = df[f"macd_{sfx}"].ewm(span=int(msg), adjust=False).mean()
        df[f"macd_hist_{sfx}"] = df[f"macd_{sfx}"] - df[f"macd_signal_{sfx}"]
    df["macd"] = df["macd_12_26_9"]
    df["macd_signal"] = df["macd_signal_12_26_9"]
    df["macd_hist"] = df["macd_hist_12_26_9"]

    # ---- ATR ----
    for p in (7, 14, 20, 21, 50):
        hl = df["high"] - df["low"]
        hc = (df["high"] - df["close"].shift(1)).abs()
        lc = (df["low"] - df["close"].shift(1)).abs()
        tr = hl.combine(hc, max).combine(lc, max)
        df[f"atr{int(p)}"] = tr.rolling(int(p)).mean()
    df["atr"] = df["atr14"]

    # ---- VWAP ----
    for p in (24, 48, 168):
        pv = (df["close"] * df["volume"]).rolling(int(p)).sum()
        vl = df["volume"].rolling(int(p)).sum()
        df[f"vwap{int(p)}"] = pv / vl.replace(0, np.nan)
    df["vwap"] = df["vwap24"]

    # ---- Stochastic RSI ----
    for p in (14,):
        rsi_col = df[f"rsi{int(p)}"]
        sr_min = rsi_col.rolling(int(p)).min()
        sr_max = rsi_col.rolling(int(p)).max()
        rng = (sr_max - sr_min).replace(0, np.nan)
        df[f"stoch_rsi_k{int(p)}"] = ((rsi_col - sr_min) / rng * 100).fillna(50.0)
        df[f"stoch_rsi_d{int(p)}"] = df[f"stoch_rsi_k{int(p)}"].rolling(3).mean()
    df["stoch_rsi_k"] = df["stoch_rsi_k14"]
    df["stoch_rsi_d"] = df["stoch_rsi_d14"]

    # ---- KDJ ----
    for p in (9, 14):
        low_min = df["low"].rolling(int(p)).min()
        high_max = df["high"].rolling(int(p)).max()
        den = (high_max - low_min).replace(0, np.nan)
        rsv = ((df["close"] - low_min) / den * 100).fillna(50.0)
        df[f"kdj_k{int(p)}"] = rsv.ewm(com=2, adjust=False).mean()
        df[f"kdj_d{int(p)}"] = df[f"kdj_k{int(p)}"].ewm(com=2, adjust=False).mean()
        df[f"kdj_j{int(p)}"] = 3 * df[f"kdj_k{int(p)}"] - 2 * df[f"kdj_d{int(p)}"]
    df["kdj_k"] = df["kdj_k9"]
    df["kdj_d"] = df["kdj_d9"]
    df["kdj_j"] = df["kdj_j9"]

    # ---- Volume & Price Change ----
    df["volume_avg20"] = df["volume"].rolling(20).mean()
    df["price_change_1h"] = df["close"].pct_change(1) * 100
    df["price_change_4h"] = df["close"].pct_change(4) * 100
    df["price_change_24h"] = df["close"].pct_change(24) * 100

    df = df.copy()
    return df
