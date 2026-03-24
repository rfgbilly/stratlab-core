"""
Whale Volume Spike Strategy
Buy when volume spikes 3x above 20-bar average AND price dips > 2% in 4 hours.
This detects potential sell climax / capitulation events.

Exit: take profit at 3%, stop loss at 2%, max hold 24h.

Usage:
    python examples/whale_volume_spike.py
"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pandas as pd
from indicators.technical import calculate_indicators

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
DATA_FILE = os.path.join(DATA_DIR, "btc_1h.csv")

VOLUME_SPIKE_THRESHOLD = 3.0  # volume > 3x avg
PRICE_DIP_PCT = -2.0          # 4h price change < -2%
TAKE_PROFIT_PCT = 3.0
STOP_LOSS_PCT = 2.0
MAX_HOLD_HOURS = 24


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

        if position is None:
            # Entry: volume spike + price dip
            vol_avg = row.get("volume_avg20", 0)
            vol_ratio = row["volume"] / vol_avg if vol_avg > 0 else 0
            price_4h = row.get("price_change_4h", 0)

            if vol_ratio >= VOLUME_SPIKE_THRESHOLD and price_4h <= PRICE_DIP_PCT:
                position = {
                    "entry_price": row["close"],
                    "entry_idx": i,
                    "vol_ratio": round(vol_ratio, 1),
                    "price_dip": round(price_4h, 2),
                }
        else:
            price = row["close"]
            entry = position["entry_price"]
            hold = i - position["entry_idx"]
            pnl_pct = (price - entry) / entry * 100

            exit_reason = None
            if pnl_pct >= TAKE_PROFIT_PCT:
                exit_reason = "Take profit"
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
                    "vol_spike": position["vol_ratio"],
                    "entry_dip": position["price_dip"],
                })
                position = None

    if not trades:
        print("No trades triggered.")
        return

    wins = [t for t in trades if t["pnl_pct"] > 0]
    total_pnl = sum(t["pnl_pct"] for t in trades)
    win_rate = len(wins) / len(trades) * 100

    print(f"\n--- Whale Volume Spike Strategy Results ---")
    print(f"Total trades:  {len(trades)}")
    print(f"Win rate:      {win_rate:.1f}%")
    print(f"Total PnL:     {total_pnl:+.2f}%")
    print(f"Avg PnL/trade: {total_pnl/len(trades):+.2f}%")

    print(f"\nExit reasons:")
    reasons = {}
    for t in trades:
        reasons[t["reason"]] = reasons.get(t["reason"], 0) + 1
    for r, c in sorted(reasons.items(), key=lambda x: -x[1]):
        print(f"  {r}: {c}")

    print(f"\nSample trades:")
    for t in trades[:5]:
        print(f"  Vol {t['vol_spike']}x, dip {t['entry_dip']}% -> {t['pnl_pct']:+.2f}% ({t['reason']})")


if __name__ == "__main__":
    run()
