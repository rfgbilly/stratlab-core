import pandas as pd
import numpy as np
import json
import sys
import os

# ====== 數據路徑 ======
BASE = os.path.expanduser("~/strategy-platform")
DATA_PATH = os.path.join(BASE, "data/btc_klines/btc_1h_2024_2025.csv")

# ====== 策略參數（由 code_generator 填入）======
# {{STRATEGY_PARAMS}}
DIRECTION = "long"
TAKE_PROFIT_PCT = 2.0
STOP_LOSS_PCT = 1.5
MAX_HOLD_HOURS = 48

# ====== 技術指標計算（與 code_generator 預設集對齊，供模板/手動測試）======
def calculate_indicators(df):
    import warnings

    warnings.filterwarnings("ignore", message=".*DataFrame is highly fragmented.*")

    _c_close = pd.to_numeric(df["close"], errors="coerce")
    if "open" not in df.columns:
        df["open"] = _c_close.shift(1).fillna(_c_close)
    _c_open = pd.to_numeric(df["open"], errors="coerce")
    if "high" not in df.columns:
        df["high"] = pd.concat([_c_open, _c_close], axis=1).max(axis=1)
    if "low" not in df.columns:
        df["low"] = pd.concat([_c_open, _c_close], axis=1).min(axis=1)

    _SMA_PERIODS = (
        5, 7, 10, 14, 20, 21, 25, 30, 50, 60, 90, 100, 120, 150, 180, 200, 210, 250, 300, 350, 400, 500,
    )
    for period in _SMA_PERIODS:
        df["ma" + str(period)] = df["close"].rolling(int(period)).mean()

    _EMA_PERIODS = (
        5, 7, 8, 9, 10, 12, 13, 20, 21, 25, 26, 30, 50, 55, 60, 90, 100, 120, 150, 200, 210, 250,
    )
    for period in _EMA_PERIODS:
        df["ema" + str(period)] = df["close"].ewm(span=int(period), adjust=False).mean()

    _rd = df["close"].diff()
    _rg = _rd.where(_rd > 0, 0.0)
    _rl = (-_rd.where(_rd < 0, 0.0))
    for period in (6, 7, 9, 14, 21, 25, 28):
        _ag = _rg.rolling(window=int(period)).mean()
        _al = _rl.rolling(window=int(period)).mean()
        _rs = _ag / _al
        df["rsi" + str(period)] = 100 - (100 / (1 + _rs))
    df["rsi"] = df["rsi14"]

    _BB_CFG = [
        (10, 1.5), (10, 2.0), (10, 2.5), (10, 3.0),
        (20, 1.5), (20, 2.0), (20, 2.5), (20, 3.0),
        (30, 1.5), (30, 2.0), (30, 2.5), (30, 3.0),
        (50, 1.5), (50, 2.0), (50, 2.5), (50, 3.0),
    ]
    for _bp, _bm in _BB_CFG:
        _suf = str(int(_bp)) + "_" + str(_bm).replace(".", "").replace(",", "")
        _mid = df["close"].rolling(int(_bp)).mean()
        _std = df["close"].rolling(int(_bp)).std()
        df["bb_upper_" + _suf] = _mid + float(_bm) * _std
        df["bb_lower_" + _suf] = _mid - float(_bm) * _std
        df["bb_middle_" + _suf] = _mid

    df["bb_middle"] = df["close"].rolling(20).mean()
    df["bb_std"] = df["close"].rolling(20).std()
    df["bb_upper"] = df["bb_middle"] + 2 * df["bb_std"]
    df["bb_lower"] = df["bb_middle"] - 2 * df["bb_std"]

    _MACD_CFG = [(12, 26, 9), (8, 21, 5), (8, 17, 9), (5, 35, 5)]
    for _mf, _ms, _msg in _MACD_CFG:
        _ef = df["close"].ewm(span=int(_mf), adjust=False).mean()
        _es = df["close"].ewm(span=int(_ms), adjust=False).mean()
        _sfx = str(int(_mf)) + "_" + str(int(_ms)) + "_" + str(int(_msg))
        df["macd_" + _sfx] = _ef - _es
        df["macd_signal_" + _sfx] = df["macd_" + _sfx].ewm(span=int(_msg), adjust=False).mean()
        df["macd_hist_" + _sfx] = df["macd_" + _sfx] - df["macd_signal_" + _sfx]
    df["macd"] = df["macd_12_26_9"]
    df["macd_signal"] = df["macd_signal_12_26_9"]
    df["macd_hist"] = df["macd_hist_12_26_9"]

    for period in (7, 14, 20, 21, 50):
        high_low = df["high"] - df["low"]
        high_close = (df["high"] - df["close"].shift(1)).abs()
        low_close = (df["low"] - df["close"].shift(1)).abs()
        true_range = high_low.combine(high_close, max).combine(low_close, max)
        df["atr" + str(int(period))] = true_range.rolling(int(period)).mean()
    df["atr"] = df["atr14"]

    for period in (24, 48, 168):
        pv = (df["close"] * df["volume"]).rolling(int(period)).sum()
        vl = df["volume"].rolling(int(period)).sum()
        df["vwap" + str(int(period))] = pv / vl.replace(0, np.nan)
    df["vwap"] = df["vwap24"]

    for period in (14,):
        rsi_col = df["rsi" + str(int(period))]
        stoch_rsi_min = rsi_col.rolling(int(period)).min()
        stoch_rsi_max = rsi_col.rolling(int(period)).max()
        _rng = (stoch_rsi_max - stoch_rsi_min).replace(0, np.nan)
        df["stoch_rsi_k" + str(int(period))] = ((rsi_col - stoch_rsi_min) / _rng * 100).fillna(50.0)
        df["stoch_rsi_d" + str(int(period))] = df["stoch_rsi_k" + str(int(period))].rolling(3).mean()
    df["stoch_rsi_k"] = df["stoch_rsi_k14"]
    df["stoch_rsi_d"] = df["stoch_rsi_d14"]

    for period in (9, 14):
        low_min = df["low"].rolling(int(period)).min()
        high_max = df["high"].rolling(int(period)).max()
        _den = (high_max - low_min).replace(0, np.nan)
        rsv = ((df["close"] - low_min) / _den * 100).fillna(50.0)
        df["kdj_k" + str(int(period))] = rsv.ewm(com=2, adjust=False).mean()
        df["kdj_d" + str(int(period))] = df["kdj_k" + str(int(period))].ewm(com=2, adjust=False).mean()
        df["kdj_j" + str(int(period))] = 3 * df["kdj_k" + str(int(period))] - 2 * df["kdj_d" + str(int(period))]
    df["kdj_k"] = df["kdj_k9"]
    df["kdj_d"] = df["kdj_d9"]
    df["kdj_j"] = df["kdj_j9"]

    df["volume_avg20"] = df["volume"].rolling(20).mean()
    df["price_change_1h"] = df["close"].pct_change(1) * 100
    df["price_change_4h"] = df["close"].pct_change(4) * 100
    df["price_change_24h"] = df["close"].pct_change(24) * 100

    df = df.copy()
    return df


# ====== 入場條件檢查 ======
def check_entry(row, prev_row):
    """
    檢查是否滿足入場條件
    這個函數的內容由 code_generator 根據 LLM 解析結果生成
    """
    # {{ENTRY_LOGIC}}
    return row["rsi"] < 30


# ====== 出場條件 ======
# {{EXIT_PARAMS}} 已在上方 STRATEGY_PARAMS 區塊設定

# ====== 回測主循環 ======
def run_backtest():
    df = pd.read_csv(DATA_PATH)
    df = calculate_indicators(df)

    trades = []
    position = None

    for i in range(200, len(df)):
        row = df.iloc[i]
        prev_row = df.iloc[i - 1]

        if position is None:
            if check_entry(row, prev_row):
                position = {
                    "entry_price": row["close"],
                    "entry_idx": i,
                    "entry_time": row["timestamp"],
                }
        else:
            current_price = row["close"]
            entry_price = position["entry_price"]
            hold_hours = i - position["entry_idx"]

            if DIRECTION == "long":
                pnl_pct = (current_price - entry_price) / entry_price * 100
            else:
                pnl_pct = (entry_price - current_price) / entry_price * 100

            exit_reason = None
            if pnl_pct >= TAKE_PROFIT_PCT:
                exit_reason = "take_profit"
            elif pnl_pct <= -STOP_LOSS_PCT:
                exit_reason = "stop_loss"
            elif hold_hours >= MAX_HOLD_HOURS:
                exit_reason = "timeout"

            if exit_reason:
                trades.append({
                    "entry_price": entry_price,
                    "exit_price": current_price,
                    "pnl_pct": round(pnl_pct, 4),
                    "hold_hours": hold_hours,
                    "exit_reason": exit_reason,
                    "entry_time": position["entry_time"],
                    "exit_time": row["timestamp"],
                })
                position = None

    if len(trades) == 0:
        results = {
            "total_trades": 0,
            "win_rate": 0,
            "total_pnl_pct": 0,
            "avg_pnl_pct": 0,
            "max_drawdown_pct": 0,
            "best_trade_pct": 0,
            "worst_trade_pct": 0,
            "avg_hold_hours": 0,
            "sharpe_ratio": 0,
        }
    else:
        pnls = [t["pnl_pct"] for t in trades]
        wins = len([p for p in pnls if p > 0])
        cumulative = np.cumsum(pnls)
        running_max = np.maximum.accumulate(cumulative)
        drawdowns = cumulative - running_max
        max_dd = float(np.min(drawdowns)) if len(drawdowns) > 0 else 0
        sharpe = float(np.mean(pnls) / np.std(pnls)) if np.std(pnls) > 0 else 0

        results = {
            "total_trades": len(trades),
            "win_rate": round(wins / len(trades) * 100, 1),
            "total_pnl_pct": round(sum(pnls), 2),
            "avg_pnl_pct": round(np.mean(pnls), 4),
            "max_drawdown_pct": round(max_dd, 2),
            "best_trade_pct": round(max(pnls), 4),
            "worst_trade_pct": round(min(pnls), 4),
            "avg_hold_hours": round(np.mean([t["hold_hours"] for t in trades]), 1),
            "sharpe_ratio": round(sharpe, 3),
        }

    print(json.dumps(results))


if __name__ == "__main__":
    run_backtest()
