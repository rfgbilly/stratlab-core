import subprocess
import json
import tempfile
import os


def run_backtest(code_string, timeout=300):
    """
    在沙盒中執行回測代碼

    Args:
        code_string: 完整的Python回測腳本字串
        timeout: 超時秒數

    Returns:
        dict: 回測結果 或 錯誤信息
    """
    with tempfile.NamedTemporaryFile(
        suffix=".py", mode="w", delete=False, dir="/tmp"
    ) as f:
        f.write(code_string)
        tmp_path = f.name
    debug_save = True
    if debug_save:
        try:
            with open("/tmp/stratlab_last_backtest.py", "w") as dbg:
                dbg.write(code_string)
            print("[sandbox] 已保存回測代碼到 /tmp/stratlab_last_backtest.py")
        except Exception:
            pass

    try:
        result = subprocess.run(
            ["python3", tmp_path],
            capture_output=True,
            text=True,
            timeout=timeout,
            cwd=os.path.expanduser("~/strategy-platform"),
        )

        if result.returncode == 0:
            try:
                return {"success": True, "results": json.loads(result.stdout)}
            except json.JSONDecodeError:
                return {
                    "success": False,
                    "error": f"回測輸出格式錯誤: {result.stdout[:500]}",
                }
        else:
            return {
                "success": False,
                "error": f"回測執行錯誤: {result.stderr}",
            }

    except subprocess.TimeoutExpired:
        return {"success": False, "error": "回測超時（超過300秒）"}

    finally:
        os.unlink(tmp_path)


# === 測試 ===
if __name__ == "__main__":
    test_code = """
import pandas as pd
import numpy as np
import json

df = pd.read_csv("data/btc_klines/btc_1h_2024_2025.csv")
print(json.dumps({"total_trades": 0, "status": "template_works", "rows": len(df)}))
"""
    result = run_backtest(test_code)
    print(json.dumps(result, indent=2, ensure_ascii=False))
