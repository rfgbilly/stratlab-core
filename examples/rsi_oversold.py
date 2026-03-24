"""
RSI Oversold Strategy
Buy when RSI(14) < 30, exit when RSI(14) > 70 or stop loss at 2%.

Usage:
    python examples/rsi_oversold.py
"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pandas as pd
from indicators.technical import calculate_indicators

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
DATA_FILE = os.path.join(DATA_DIR, "btc_1h.csv")

DIRECTION = "long"
STOP_LOSS_PCT = 2.0
MAX_HOLD_HOURS = 48


def run():
    if not os.path.exists(DATA_FILE):
        print(f"Data file not found: {DATA_FILE}")
        print("Run: python data/download_klines.py")
        return

    df = pd.read_csv(DATA_FILE)
    df = calculate_indicators(df)
    print(f"Loaded {len(df)} bars, indicators: {len(df.columns)} columns")

    trades = []
    position = None

    for i in range(200, len(df)):
        row = df.iloc[i]

        if position is None:
            # Entry: RSI < 30
            if row["rsi"] < 30:
                position = {
                    "entry_price": row["close"],
                    "entry_idx": i,
                    "entry_time": row.get("timestamp", i),
                }
        else:
            price = row["close"]
            entry = position["entry_price"]
            hold = i - position["entry_idx"]
            pnl_pct = (price - entry) / entry * 100

            # Exit: RSI > 70 or stop loss or max hold
            exit_reason = None
            if row["rsi"] > 70:
                exit_reason = "RSI > 70"
            elif pnl_pct <= -STOP_LOSS_PCT:
                exit_reason = "Stop loss"
            elif hold >= MAX_HOLD_HOURS:
                exit_reason = "Max hold"

            if exit_reason:
                trades.append({
                    "entry_price": entry,
                    "exit_price": price,
                    "pnl_pct": round(pnl_pct, 4),
                    "hold_hours": hold,
                    "reason": exit_reason,
                })
                position = None

    # Results
    if not trades:
        print("No trades triggered.")
        return

    wins = [t for t in trades if t["pnl_pct"] > 0]
    total_pnl = sum(t["pnl_pct"] for t in trades)
    win_rate = len(wins) / len(trades) * 100

    print(f"\n--- RSI Oversold Strategy Results ---")
    print(f"Total trades:  {len(trades)}")
    print(f"Win rate:      {win_rate:.1f}%")
    print(f"Total PnL:     {total_pnl:+.2f}%")
    print(f"Avg PnL/trade: {total_pnl/len(trades):+.2f}%")
    print(f"Best trade:    {max(t['pnl_pct'] for t in trades):+.2f}%")
    print(f"Worst trade:   {min(t['pnl_pct'] for t in trades):+.2f}%")


if __name__ == "__main__":
    run()
