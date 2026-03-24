# StratLab Core

Open-source BTC trading strategy backtesting engine.

## What is this?

A Python backtesting framework for BTC futures trading strategies. Supports 69+ technical indicators, 6 years of data, and whale activity metrics.

**Want the full AI-powered experience without writing code?** Try [stratlab.tech](https://stratlab.tech)

## Features

- 69+ technical indicators (SMA, EMA, RSI, MACD, Bollinger Bands, ATR, VWAP, KDJ, Stochastic RSI, and more)
- Custom indicator support
- Dynamic exit conditions (trailing stop, breakeven, indicator-based exit)
- Equity curve and trade detail output
- Benchmark comparison (buy & hold)
- Sharpe ratio, max drawdown, win rate calculation

## Quick Start

```bash
# Clone
git clone https://github.com/billychan/stratlab-core.git
cd stratlab-core

# Install dependencies
pip install pandas numpy

# Download BTC data
python data/download_klines.py

# Run example backtest
python examples/rsi_oversold.py
```

## Examples

See the `examples/` directory:
- `rsi_oversold.py` — Buy when RSI < 30, sell when RSI > 70
- `ma_crossover.py` — Golden cross / death cross strategy
- `whale_volume_spike.py` — Buy on whale volume spike + price dip

## Project Structure

```
stratlab-core/
├── engine/
│   ├── backtest_template.py   # Core backtest loop
│   ├── sandbox_runner.py      # Safe execution wrapper
│   └── code_generator.py      # Strategy → Python code
├── indicators/
│   └── technical.py           # 69+ indicator calculations
├── examples/
│   ├── rsi_oversold.py
│   ├── ma_crossover.py
│   └── whale_volume_spike.py
├── data/
│   └── download_klines.py     # Binance K-line downloader
└── README.md
```

## Full Platform

This is the open-source backtesting engine. For the full AI-powered platform with:
- Natural language strategy input (Chinese + English)
- AI-powered strategy parsing
- Web UI with charts and analytics
- Real-time paper trading
- Whale signal detection
- Market data dashboard

Visit **[stratlab.tech](https://stratlab.tech)**

## Contributing

Pull requests welcome! Areas where help is needed:
- New technical indicators
- Bug fixes
- Documentation improvements
- Performance optimization
- Multi-asset support (ETH, SOL)

## License

MIT
