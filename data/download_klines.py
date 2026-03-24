#!/usr/bin/env python3
"""
從 Binance 公開 API 下載 BTCUSDT 1小時 K 線，存為 CSV。
時間範圍：2024-01-01 至今。分批下載，每批最多 1000 根。
"""
import os
import time
import requests
from datetime import datetime, timezone

BASE_URL = "https://api.binance.com/api/v3/klines"
OUTPUT_DIR = os.path.expanduser("~/strategy-platform/data/btc_klines")
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "btc_1h_2024_2025.csv")

# 2024-01-01 00:00:00 UTC 毫秒
START_MS = int(datetime(2024, 1, 1).timestamp() * 1000)
END_MS = int(datetime.now().timestamp() * 1000)


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    if os.path.exists(OUTPUT_FILE):
        os.remove(OUTPUT_FILE)

    rows = []
    current_start = START_MS

    while current_start < END_MS:
        r = requests.get(
            BASE_URL,
            params={
                "symbol": "BTCUSDT",
                "interval": "1h",
                "limit": 1000,
                "startTime": current_start,
                "endTime": END_MS,
            },
        )
        r.raise_for_status()
        data = r.json()
        if not data:
            break
        for candle in data:
            ts_ms, o, h, l, c, v = candle[0], candle[1], candle[2], candle[3], candle[4], candle[5]
            ts_iso = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            rows.append((ts_iso, o, h, l, c, v))
        current_start = data[-1][0] + 3600 * 1000
        time.sleep(0.5)

    with open(OUTPUT_FILE, "w") as f:
        f.write("timestamp,open,high,low,close,volume\n")
        for r in rows:
            f.write(",".join(str(x) for x in r) + "\n")

    print(f"總行數: {len(rows)}")
    if rows:
        print(f"時間範圍: {rows[0][0]} ~ {rows[-1][0]}")


if __name__ == "__main__":
    main()
