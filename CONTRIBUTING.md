# Contributing to StratLab Core | 貢獻指南

Thanks for your interest in contributing! 感謝你的貢獻！

## How to Contribute | 如何貢獻

### 1. Fork

Click the **Fork** button on [GitHub](https://github.com/rfgbilly/stratlab-core).

在 GitHub 上點擊 **Fork**。

### 2. Clone & Branch

```bash
git clone https://github.com/YOUR_USERNAME/stratlab-core.git
cd stratlab-core
git checkout -b feature/your-feature-name
```

### 3. Make Changes | 修改代碼

```bash
# Install dependencies | 安裝依賴
pip install pandas numpy

# Download test data | 下載測試數據
python data/download_klines.py

# Test your changes | 測試你的修改
python examples/rsi_oversold.py
```

### 4. Commit

Write clear commit messages. 寫清楚的 commit 訊息。

```bash
git add .
git commit -m "Add: new RSI divergence indicator"
```

Commit message format | 格式：
- `Add:` new feature | 新功能
- `Fix:` bug fix | 修 Bug
- `Update:` improve existing feature | 改進現有功能
- `Docs:` documentation | 文檔

### 5. Push & PR

```bash
git push origin feature/your-feature-name
```

Go to GitHub and click **New Pull Request**. 回到 GitHub 點擊 **New Pull Request**。

In your PR description, include | PR 描述請包含：
- What you changed | 改了什麼
- Why | 為什麼
- How to test | 怎麼測試

## What We're Looking For | 歡迎的貢獻

- New technical indicators | 新技術指標
- Bug fixes | 修 Bug
- Performance improvements | 效能改進
- Documentation (English or Chinese) | 文檔（中文或英文）
- Multi-asset support (ETH, SOL) | 多幣種支持
- New example strategies | 新範例策略

## Code Style | 代碼風格

- Python 3.9+
- Use `pandas` / `numpy` for data operations
- Keep functions focused and readable
- Add comments for non-obvious logic

## Questions? | 有問題？

Open an issue on GitHub. 在 GitHub 上開 issue。
