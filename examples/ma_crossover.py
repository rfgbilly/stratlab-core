"""
MA Crossover Strategy
Buy when MA20 crosses above MA50 (golden cross).
Sell when MA20 crosses below MA50 (death cross) or stop loss at 3%.

Usage:
    python examples/ma_crossover.py
"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pandas as pd
from indicators.technical import calculate_indicators

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
DATA_FILE = os.path.join(DATA_DIR, "btc_1h.csv")

STOP_LOSS_PCT = 3.0
MAX_HOLD_HOURS = 168  # 1 week


def run():
    if not os.path.exists(DATA_FILE):
        print(f"Data file not found: {DATA_FILE}")
        print("Run: python data/download_klines.py")
        return

    df = pd.read_csv(DATA_FILE)
    df = calculate_indicators(df)
    print(f"Loaded {len(df)} bars")

    trades = []
    position = None

    for i in range(200, len(df)):
        row = df.iloc[i]
        prev = df.iloc[i - 1]

        if position is None:
            # Entry: MA20 crosses above MA50
            if prev["ma20"] <= prev["ma50"] and row["ma20"] > row["ma50"]:
                position = {
                    "entry_price": row["close"],
                    "entry_idx": i,
                }
        else:
            price = row["close"]
            entry = position["entry_price"]
            hold = i - position["entry_idx"]
            pnl_pct = (price - entry) / entry * 100

            exit_reason = None
            # Exit: death cross
            if prev["ma20"] >= prev["ma50"] and row["ma20"] < row["ma50"]:
                exit_reason = "Death cross"
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

    if not trades:
        print("No trades triggered.")
        return

    wins = [t for t in trades if t["pnl_pct"] > 0]
    total_pnl = sum(t["pnl_pct"] for t in trades)
    win_rate = len(wins) / len(trades) * 100

    print(f"\n--- MA Crossover Strategy Results ---")
    print(f"Total trades:  {len(trades)}")
    print(f"Win rate:      {win_rate:.1f}%")
    print(f"Total PnL:     {total_pnl:+.2f}%")
    print(f"Avg PnL/trade: {total_pnl/len(trades):+.2f}%")
    print(f"Avg hold:      {sum(t['hold_hours'] for t in trades)/len(trades):.0f}h")


if __name__ == "__main__":
    run()
