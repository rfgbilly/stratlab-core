# StratLab Core

**Open-source BTC backtesting engine | 開源 BTC 回測引擎**

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

---

## What is this? | 這是什麼？

A Python backtesting framework for BTC futures trading strategies. Supports 69+ technical indicators, 6 years of historical data, and whale activity metrics.

Python 寫的 BTC 期貨策略回測框架。支持 69+ 技術指標、6 年歷史數據、鯨魚活動指標。

**Want the full AI-powered experience?** Try [stratlab.tech](https://stratlab.tech) — describe your strategy in Chinese or English, AI backtests it automatically.

**想用 AI 全自動回測？** 試試 [stratlab.tech](https://stratlab.tech) — 用中文或英文描述策略，AI 自動回測。

## Features | 功能

- **69+ technical indicators** — SMA, EMA, RSI, MACD, Bollinger Bands, ATR, VWAP, KDJ, Stochastic RSI...
- **Dynamic exit conditions** — trailing stop, breakeven stop, indicator-based exit
- **Complete metrics** — Sharpe ratio, max drawdown, win rate, equity curve, trade details
- **Benchmark comparison** — buy & hold comparison built-in
- **Safe execution** — sandboxed subprocess runner with timeout
- **Whale indicators** — whale buy/sell ratio, abnormal activity detection

---

- **69+ 技術指標** — SMA、EMA、RSI、MACD、布林帶、ATR、VWAP、KDJ、Stochastic RSI...
- **動態出場條件** — 移動止損、保本止損、指標觸發出場
- **完整指標** — 夏普比率、最大回撤、勝率、資金曲線、逐筆交易明細
- **基準對比** — 內建買入持有對比
- **安全執行** — 沙箱子進程隔離，支持超時
- **鯨魚指標** — 鯨魚買賣比、異常活躍度偵測

## Quick Start | 快速開始

```bash
# Clone
git clone https://github.com/rfgbilly/stratlab-core.git
cd stratlab-core

# Install dependencies | 安裝依賴
pip install pandas numpy

# Download BTC data | 下載 BTC 數據
python data/download_klines.py

# Run example | 運行範例
python examples/rsi_oversold.py
```

## Examples | 範例

See `examples/` directory:

| File | Strategy | 策略說明 |
|------|----------|---------|
| `rsi_oversold.py` | Buy RSI < 30, sell RSI > 70 | RSI 低於 30 做多，高於 70 平倉 |
| `ma_crossover.py` | MA20/MA50 golden cross | 均線黃金交叉做多 |
| `whale_volume_spike.py` | Volume spike + price dip | 成交量暴增 + 價格回調做多 |

## Project Structure | 項目結構

```
stratlab-core/
├── engine/
│   ├── backtest_template.py   # Core backtest loop | 回測主循環
│   ├── sandbox_runner.py      # Safe execution wrapper | 沙箱執行器
│   └── code_generator.py      # Strategy → Python code | 策略轉 Python
├── indicators/
│   └── technical.py           # 69+ indicator calculations | 技術指標計算
├── examples/
│   ├── rsi_oversold.py
│   ├── ma_crossover.py
│   └── whale_volume_spike.py
├── data/
│   └── download_klines.py     # Binance K-line downloader | K線下載器
├── CONTRIBUTING.md
├── LICENSE
└── README.md
```

## Run Locally | 本地運行

### Prerequisites | 前置需求

- Python 3.9+
- pip

### Setup | 設定

```bash
# 1. Create virtual environment (optional) | 建立虛擬環境（可選）
python -m venv venv
source venv/bin/activate

# 2. Install dependencies | 安裝依賴
pip install pandas numpy

# 3. Download data | 下載數據
python data/download_klines.py
# Downloads BTC 1h candles from Binance (2019–2025)
# 從 Binance 下載 BTC 1小時K線（2019–2025）

# 4. Run an example | 運行範例
python examples/ma_crossover.py
```

### Write your own strategy | 寫你自己的策略

```python
import pandas as pd

df = pd.read_csv("data/btc_1h.csv")
df["sma20"] = df["close"].rolling(20).mean()
df["sma50"] = df["close"].rolling(50).mean()

# Your entry/exit logic here
# 在這裡寫你的進出場邏輯
```

## Full Platform | 完整平台

This repo is the open-source backtesting engine. The full platform at [stratlab.tech](https://stratlab.tech) adds:

這個 repo 是開源回測引擎。完整平台 [stratlab.tech](https://stratlab.tech) 額外提供：

- Natural language strategy input (Chinese + English) | 自然語言策略輸入（中英文）
- AI-powered strategy parsing | AI 策略解析
- Web UI with charts and analytics | 網頁介面、圖表、分析
- Real-time paper trading simulator | 即時模擬交易
- Whale signal detection | 鯨魚信號偵測
- Market data dashboard | 市場數據面板

## Contributing | 如何貢獻

See [CONTRIBUTING.md](CONTRIBUTING.md) for details.

Pull requests welcome! Areas where help is needed:
歡迎 PR！目前需要幫助的方向：

- New technical indicators | 新技術指標
- Bug fixes | 修 Bug
- Documentation improvements | 文檔改進
- Performance optimization | 效能優化
- Multi-asset support (ETH, SOL) | 多幣種支持

## License | 授權

[MIT](LICENSE) - Copyright (c) 2026 Billy Chan
