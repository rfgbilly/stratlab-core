import json
import os
import re

BASE = os.path.expanduser("~/strategy-platform")
DATA_PATH_REL = "data/btc_klines/btc_1h_2024_2025.csv"


def _use_5m_data(strategy_json, timeframe):
    """是否使用 5 分鐘數據：timeframe=5m 強制；auto 時若含鯨魚多 bar 形態則用 5m。"""
    if timeframe == "5m":
        return True
    if timeframe == "1h":
        return False
    # auto
    entry_conds_list = strategy_json.get("entry_conditions", [])
    whale_multibar = ("consecutive_whale_sell_increase", "whale_sell_peak_decline")
    return any(c.get("indicator") in whale_multibar for c in entry_conds_list)


def _walk_strategy_conditions(strategy_json):
    out = []
    out.extend(strategy_json.get("entry_conditions") or [])
    out.extend((strategy_json.get("exit_conditions") or {}).get("exit_indicators") or [])
    return out


def _collect_tech_indicator_params(strategy_json):
    """
    合併預設與策略 JSON 中引用的技術指標週期/參數，供生成的 calculate_indicators 使用。
    """
    sma = {5, 7, 10, 14, 20, 21, 25, 30, 50, 60, 90, 100, 120, 150, 180, 200, 210, 250, 300, 350, 400, 500}
    ema = {5, 7, 8, 9, 10, 12, 13, 20, 21, 25, 26, 30, 50, 55, 60, 90, 100, 120, 150, 200, 210, 250}
    rsi = {6, 7, 9, 14, 21, 25, 28}
    atr = {7, 14, 20, 21, 50}
    vwap = {24, 48, 168}
    kdj = {9, 14}
    stoch_rsi_p = {14}
    bb_set = set()
    for p in (10, 20, 30, 50):
        for m in (1.5, 2.0, 2.5, 3.0):
            bb_set.add((p, float(m)))
    macd_set = {(12, 26, 9), (8, 21, 5), (8, 17, 9), (5, 35, 5)}

    def _add_ma_str(val):
        if isinstance(val, str):
            for m in re.findall(r"ma(\d+)", val, re.I):
                sma.add(int(m))

    def _add_ema_str(val):
        if isinstance(val, str):
            for m in re.findall(r"ema(\d+)", val, re.I):
                ema.add(int(m))

    for c in _walk_strategy_conditions(strategy_json):
        if not isinstance(c, dict):
            continue
        ind = (c.get("indicator") or "").strip()
        val = c.get("value")
        if ind == "ma_cross":
            _add_ma_str(val)
        elif ind == "ema_cross":
            _add_ema_str(val)
        elif ind in ("price_above_ma", "price_below_ma"):
            v = val
            if isinstance(v, (int, float)) and not isinstance(v, bool):
                sma.add(int(v))
            elif isinstance(v, str):
                vs = v.strip()
                if vs.isdigit():
                    sma.add(int(vs))
                else:
                    _add_ma_str(v)
        elif ind in ("price_above_ema", "price_below_ema"):
            v = val
            if isinstance(v, (int, float)) and not isinstance(v, bool):
                ema.add(int(v))
            elif isinstance(v, str) and v.strip().isdigit():
                ema.add(int(v.strip()))
        elif ind == "rsi":
            try:
                rsi.add(int(c.get("period", 14)))
            except (TypeError, ValueError):
                rsi.add(14)
        elif ind == "bollinger":
            try:
                bp = int(c.get("bb_period", 20))
                bm = float(c.get("bb_mult", 2))
                bb_set.add((bp, bm))
            except (TypeError, ValueError):
                pass
        elif ind in ("macd_cross", "macd_hist"):
            try:
                f, s, sg = (
                    int(c.get("macd_fast", 12)),
                    int(c.get("macd_slow", 26)),
                    int(c.get("macd_signal", 9)),
                )
                macd_set.add((f, s, sg))
            except (TypeError, ValueError):
                pass
        elif ind == "atr":
            try:
                atr.add(int(c.get("period", 14)))
            except (TypeError, ValueError):
                atr.add(14)
        elif ind in ("price_above_vwap", "price_below_vwap"):
            try:
                vwap.add(int(c.get("period", 24)))
            except (TypeError, ValueError):
                pass
        elif ind == "stoch_rsi":
            try:
                sp = int(c.get("period", 14))
                stoch_rsi_p.add(sp)
                rsi.add(sp)
            except (TypeError, ValueError):
                stoch_rsi_p.add(14)
                rsi.add(14)
        elif ind in ("kdj_j", "kdj_cross"):
            try:
                kdj.add(int(c.get("period", 9)))
            except (TypeError, ValueError):
                kdj.add(9)

    rsi.add(14)
    bb_set.add((20, 2.0))

    bb_list = sorted(bb_set, key=lambda x: (x[0], x[1]))
    macd_list = sorted(macd_set)

    return {
        "sma": sorted(x for x in sma if x > 0),
        "ema": sorted(x for x in ema if x > 0),
        "rsi": sorted(x for x in rsi if x > 0),
        "atr": sorted(x for x in atr if x > 0),
        "vwap": sorted(x for x in vwap if x > 0),
        "kdj": sorted(x for x in kdj if x > 0),
        "stoch_rsi": sorted(x for x in stoch_rsi_p if x > 0),
        "bb": bb_list,
        "macd": macd_list,
    }


def generate_backtest_code(strategy_json, date_start=None, date_end=None, timeframe="auto"):
    """
    根據策略JSON生成完整的回測Python腳本

    Args:
        strategy_json: dict，LLM解析出的策略結構
        date_start: 可選，回測開始日期 "YYYY-MM-DD"
        date_end: 可選，回測結束日期 "YYYY-MM-DD"
        timeframe: "auto" | "1h" | "5m"，回測粒度（auto 時有鯨魚多 bar 形態則用 5m）

    Returns:
        str: 完整可執行的Python回測腳本
    """
    use_5m = _use_5m_data(strategy_json, timeframe or "auto")
    direction = strategy_json.get("direction", "long")
    exit_conds = strategy_json.get("exit_conditions", {})
    tp = exit_conds.get("take_profit_pct")
    sl = exit_conds.get("stop_loss_pct", 1.5)
    max_hold = exit_conds.get("max_hold_hours", 48)
    trailing = exit_conds.get("trailing_stop_pct")
    breakeven = exit_conds.get("breakeven_trigger_pct")
    exit_indicators = exit_conds.get("exit_indicators", [])

    entry_lines = []
    for cond in strategy_json.get("entry_conditions", []):
        line = _generate_condition_code(cond, for_entry=True)
        if line:
            entry_lines.append(line)

    entry_conds_list = strategy_json.get("entry_conditions", [])
    has_consecutive = any(c.get("indicator") == "consecutive_candles" for c in entry_conds_list)
    if not entry_lines:
        entry_return = "False"
    else:
        entry_return = " and ".join([f"({line})" for line in entry_lines])

    # 5m 鯨魚多 bar 策略可選：不破低、吸收倍數、冷卻、CVD 過濾
    entry_options = strategy_json.get("entry_options", {})
    no_break_low = entry_options.get("no_break_low", True)
    absorption_min = entry_options.get("absorption_min")
    if absorption_min is None and use_5m and any(c.get("indicator") in ("consecutive_whale_sell_increase", "whale_sell_peak_decline") for c in entry_conds_list):
        absorption_min = 0.4
    cooldown_hours = entry_options.get("cooldown_hours")
    if cooldown_hours is None and use_5m and any(c.get("indicator") in ("consecutive_whale_sell_increase", "whale_sell_peak_decline") for c in entry_conds_list):
        cooldown_hours = 2
    cvd_filter = entry_options.get("cvd_filter", False)
    extra_5m_conditions = []
    if use_5m:
        if no_break_low:
            extra_5m_conditions.append("(float(_col_arrays['low'][i]) >= float(_col_arrays['low'][i-1]))")
        if absorption_min is not None:
            extra_5m_conditions.append("(float(_col_arrays['absorption_ratio'][i]) >= {})".format(absorption_min))
        if cvd_filter:
            extra_5m_conditions.append("(float(_col_arrays['whale_delta'][i]) > 0)")
    if extra_5m_conditions:
        entry_return = "(" + entry_return + ") and " + " and ".join(extra_5m_conditions)
    cooldown_sec = (cooldown_hours or 0) * 3600
    if use_5m and cooldown_sec > 0:
        cooldown_init = "    last_signal_time = None\n"
        cooldown_check = """            if last_signal_time is not None:
                _dt = pd.to_datetime(row['timestamp']) - pd.to_datetime(last_signal_time)
                if _dt.total_seconds() < COOLDOWN_SEC:
                    continue
"""
        cooldown_update = "                last_signal_time = row['timestamp']\n"
    else:
        cooldown_init = ""
        cooldown_check = ""
        cooldown_update = ""

    if has_consecutive:
        consecutive_helper = """    def _consecutive_pattern(i, pattern, count, then_spec):
        if i < count:
            return False
        for j in range(count):
            idx = i - count + j
            o = float(_col_arrays['open'][idx])
            c = float(_col_arrays['close'][idx])
            if pattern == "bearish" and c >= o:
                return False
            if pattern == "bullish" and c <= o:
                return False
        cur = _RowProxy(i)
        co, cc = float(cur['open']), float(cur['close'])
        if then_spec == "bullish" and cc <= co:
            return False
        if then_spec == "bearish" and cc >= co:
            return False
        return True

"""
    else:
        consecutive_helper = ""

    exit_indicator_lines = []
    for ei in exit_indicators:
        line = _generate_condition_code(ei, for_entry=False)
        if line:
            exit_indicator_lines.append(line)

    exit_indicator_check = "False"
    if exit_indicator_lines:
        exit_indicator_check = " or ".join([f"({line})" for line in exit_indicator_lines])

    tp_value = "None" if tp is None else str(tp)
    trailing_value = "None" if trailing is None else str(trailing)
    breakeven_value = "None" if breakeven is None else str(breakeven)

    date_filter_code = ""
    if date_start:
        date_filter_code += f"    df = df[df['timestamp'] >= '{date_start}']\n"
    if date_end:
        date_filter_code += f"    df = df[df['timestamp'] <= '{date_end} 23:59:59']\n"
    if date_filter_code:
        date_filter_code += "    df = df.reset_index(drop=True)\n"

    if use_5m:
        data_path_rel = "data/btc_whale_5min.csv"
        data_load_block = """    df = pd.read_csv(DATA_PATH)
    if 'datetime' in df.columns:
        df['timestamp'] = df['datetime']
    if 'whale_buy' in df.columns:
        df['whale_buy_vol'] = df['whale_buy']
    if 'whale_sell' in df.columns:
        df['whale_sell_vol'] = df['whale_sell']
    if 'whale_trade_count' not in df.columns:
        df['whale_trade_count'] = 0.0
    if 'whale_avg_size' not in df.columns:
        df['whale_avg_size'] = 0.0
    if 'volume' not in df.columns:
        df['volume'] = df['whale_buy_vol'].fillna(0) + df['whale_sell_vol'].fillna(0)
    _hl = df['high'].astype(float) - df['low'].astype(float)
    _hl = _hl.replace(0, np.nan)
    _vr = df['volume'].astype(float) / _hl
    _m = _vr.median()
    _med = 1.0 if (pd.isna(_m) or _m == 0) else float(_m)
    df['absorption_ratio'] = (_vr / _med).fillna(0).replace([np.inf, -np.inf], 0)
"""
        bar_hours = "1/12"
        data_timeframe = "'5m'"
        cooldown_constant = "COOLDOWN_SEC = {}\n".format(cooldown_sec) if cooldown_sec > 0 else ""
    else:
        data_path_rel = DATA_PATH_REL
        data_load_block = """    df = pd.read_csv(DATA_PATH)
    _whale_csv = os.path.join(BASE, 'data', 'btc_whale_hourly.csv')
    if os.path.isfile(_whale_csv):
        _wdf = pd.read_csv(_whale_csv)
        _wdf['_mts'] = pd.to_datetime(_wdf['datetime']).dt.strftime('%Y-%m-%d %H:%M:%S')
        df['_mts'] = pd.to_datetime(df['timestamp']).dt.strftime('%Y-%m-%d %H:%M:%S')
        _wcols = [
            'whale_buy_vol', 'whale_sell_vol', 'whale_delta', 'whale_trade_count', 'whale_avg_size',
            'retail_buy_vol', 'retail_sell_vol', 'retail_delta', 'whale_retail_divergence',
            'retail_buy', 'retail_sell',
            'long_liq', 'short_liq', 'total_liq', 'liq_ratio',
            'long_liq_count', 'short_liq_count',
            'oi_value', 'oi_btc', 'oi_change_1h', 'oi_change_4h', 'oi_change_24h',
            'funding_rate',
            'cg_long_liq', 'cg_short_liq', 'cg_total_liq', 'cg_liq_ratio',
            'long_ratio', 'short_ratio', 'long_short_ratio',
            'taker_buy_vol', 'taker_sell_vol', 'taker_buy_sell_ratio',
            'top_account_long', 'top_account_short', 'top_account_ratio',
            'top_position_long', 'top_position_short', 'top_position_ratio',
            'cg_oi', 'cg_oi_change_1h', 'cg_oi_change_24h',
        ]
        _use = ['_mts'] + [c for c in _wcols if c in _wdf.columns]
        _wdf = _wdf[_use].drop_duplicates(subset=['_mts'], keep='last')
        df = df.merge(_wdf, on='_mts', how='left')
        df = df.drop(columns=['_mts'])
        _zero_fill = [
            'whale_buy_vol', 'whale_sell_vol', 'whale_delta', 'whale_trade_count', 'whale_avg_size',
            'retail_buy_vol', 'retail_sell_vol', 'retail_delta', 'whale_retail_divergence',
            'retail_buy', 'retail_sell',
            'long_liq', 'short_liq', 'total_liq', 'liq_ratio',
            'long_liq_count', 'short_liq_count',
        ]
        for _c in _zero_fill:
            if _c in df.columns:
                df[_c] = pd.to_numeric(df[_c], errors='coerce').fillna(0)
        _soft_numeric = [
            'oi_value', 'oi_btc', 'oi_change_1h', 'oi_change_4h', 'oi_change_24h',
            'funding_rate', 'cg_long_liq', 'cg_short_liq', 'cg_total_liq', 'cg_liq_ratio',
            'long_ratio', 'short_ratio', 'long_short_ratio',
            'taker_buy_vol', 'taker_sell_vol', 'taker_buy_sell_ratio',
            'top_account_long', 'top_account_short', 'top_account_ratio',
            'top_position_long', 'top_position_short', 'top_position_ratio',
            'cg_oi', 'cg_oi_change_1h', 'cg_oi_change_24h',
        ]
        for _c in _soft_numeric:
            if _c in df.columns:
                df[_c] = pd.to_numeric(df[_c], errors='coerce')
"""
        bar_hours = "1"
        data_timeframe = "'1h'"
        cooldown_constant = ""

    _tech = _collect_tech_indicator_params(strategy_json)
    sma_periods_literal = str(tuple(_tech["sma"]))
    ema_periods_literal = str(tuple(_tech["ema"]))
    rsi_periods_literal = str(tuple(_tech["rsi"]))
    atr_periods_literal = str(tuple(_tech["atr"]))
    vwap_periods_literal = str(tuple(_tech["vwap"]))
    kdj_periods_literal = str(tuple(_tech["kdj"]))
    stoch_rsi_periods_literal = str(tuple(_tech["stoch_rsi"]))
    bb_configs_repr = repr(_tech["bb"])
    macd_configs_repr = repr(_tech["macd"])

    code = f'''import pandas as pd
import numpy as np
import json
import os

BASE = os.path.expanduser("~/strategy-platform")
DATA_PATH = os.path.join(BASE, "{data_path_rel}")
BAR_HOURS = {bar_hours}
DATA_TIMEFRAME = {data_timeframe}
{cooldown_constant}
DIRECTION = "{direction}"
TAKE_PROFIT_PCT = {tp_value}
STOP_LOSS_PCT = {sl}
MAX_HOLD_HOURS = {max_hold}
TRAILING_STOP_PCT = {trailing_value}
BREAKEVEN_TRIGGER_PCT = {breakeven_value}

def calculate_indicators(df):
    # 技術指標（多週期 SMA/EMA/RSI/布林/MACD/ATR/VWAP/StochRSI/KDJ）+ 鯨魚等獨家欄位
    import warnings
    warnings.filterwarnings('ignore', message='.*DataFrame is highly fragmented.*')

    _c_close = pd.to_numeric(df['close'], errors='coerce')
    if 'open' not in df.columns:
        df['open'] = _c_close.shift(1).fillna(_c_close)
    _c_open = pd.to_numeric(df['open'], errors='coerce')
    if 'high' not in df.columns:
        df['high'] = pd.concat([_c_open, _c_close], axis=1).max(axis=1)
    if 'low' not in df.columns:
        df['low'] = pd.concat([_c_open, _c_close], axis=1).min(axis=1)

    _SMA_PERIODS = {sma_periods_literal}
    for period in _SMA_PERIODS:
        df['ma' + str(period)] = df['close'].rolling(int(period)).mean()

    _EMA_PERIODS = {ema_periods_literal}
    for period in _EMA_PERIODS:
        df['ema' + str(period)] = df['close'].ewm(span=int(period), adjust=False).mean()

    _rd = df['close'].diff()
    _rg = _rd.where(_rd > 0, 0.0)
    _rl = (-_rd.where(_rd < 0, 0.0))
    _RSI_PERIODS = {rsi_periods_literal}
    for period in _RSI_PERIODS:
        _ag = _rg.rolling(window=int(period)).mean()
        _al = _rl.rolling(window=int(period)).mean()
        _rs = _ag / _al
        df['rsi' + str(period)] = 100 - (100 / (1 + _rs))
    df['rsi'] = df['rsi14']

    _BB_CFG = {bb_configs_repr}
    for _bp, _bm in _BB_CFG:
        _suf = str(int(_bp)) + '_' + str(_bm).replace('.', '').replace(',', '')
        _mid = df['close'].rolling(int(_bp)).mean()
        _std = df['close'].rolling(int(_bp)).std()
        df['bb_upper_' + _suf] = _mid + float(_bm) * _std
        df['bb_lower_' + _suf] = _mid - float(_bm) * _std
        df['bb_middle_' + _suf] = _mid

    df['bb_middle'] = df['close'].rolling(20).mean()
    df['bb_std'] = df['close'].rolling(20).std()
    df['bb_upper'] = df['bb_middle'] + 2 * df['bb_std']
    df['bb_lower'] = df['bb_middle'] - 2 * df['bb_std']

    _MACD_CFG = {macd_configs_repr}
    for _mf, _ms, _msg in _MACD_CFG:
        _ef = df['close'].ewm(span=int(_mf), adjust=False).mean()
        _es = df['close'].ewm(span=int(_ms), adjust=False).mean()
        _sfx = str(int(_mf)) + '_' + str(int(_ms)) + '_' + str(int(_msg))
        df['macd_' + _sfx] = _ef - _es
        df['macd_signal_' + _sfx] = df['macd_' + _sfx].ewm(span=int(_msg), adjust=False).mean()
        df['macd_hist_' + _sfx] = df['macd_' + _sfx] - df['macd_signal_' + _sfx]
    df['macd'] = df['macd_12_26_9']
    df['macd_signal'] = df['macd_signal_12_26_9']
    df['macd_hist'] = df['macd_hist_12_26_9']

    _ATR_PERIODS = {atr_periods_literal}
    for period in _ATR_PERIODS:
        high_low = df['high'] - df['low']
        high_close = (df['high'] - df['close'].shift(1)).abs()
        low_close = (df['low'] - df['close'].shift(1)).abs()
        true_range = high_low.combine(high_close, max).combine(low_close, max)
        df['atr' + str(int(period))] = true_range.rolling(int(period)).mean()
    df['atr'] = df['atr14']

    _VWAP_PERIODS = {vwap_periods_literal}
    for period in _VWAP_PERIODS:
        pv = (df['close'] * df['volume']).rolling(int(period)).sum()
        vl = df['volume'].rolling(int(period)).sum()
        df['vwap' + str(int(period))] = pv / vl.replace(0, np.nan)
    df['vwap'] = df['vwap24']

    _STOCH_RSI_PERIODS = {stoch_rsi_periods_literal}
    for period in _STOCH_RSI_PERIODS:
        rsi_col = df['rsi' + str(int(period))]
        stoch_rsi_min = rsi_col.rolling(int(period)).min()
        stoch_rsi_max = rsi_col.rolling(int(period)).max()
        _rng = (stoch_rsi_max - stoch_rsi_min).replace(0, np.nan)
        df['stoch_rsi_k' + str(int(period))] = ((rsi_col - stoch_rsi_min) / _rng * 100).fillna(50.0)
        df['stoch_rsi_d' + str(int(period))] = df['stoch_rsi_k' + str(int(period))].rolling(3).mean()
    df['stoch_rsi_k'] = df['stoch_rsi_k14']
    df['stoch_rsi_d'] = df['stoch_rsi_d14']

    _KDJ_PERIODS = {kdj_periods_literal}
    for period in _KDJ_PERIODS:
        low_min = df['low'].rolling(int(period)).min()
        high_max = df['high'].rolling(int(period)).max()
        _den = (high_max - low_min).replace(0, np.nan)
        rsv = ((df['close'] - low_min) / _den * 100).fillna(50.0)
        df['kdj_k' + str(int(period))] = rsv.ewm(com=2, adjust=False).mean()
        df['kdj_d' + str(int(period))] = df['kdj_k' + str(int(period))].ewm(com=2, adjust=False).mean()
        df['kdj_j' + str(int(period))] = 3 * df['kdj_k' + str(int(period))] - 2 * df['kdj_d' + str(int(period))]
    df['kdj_k'] = df['kdj_k9']
    df['kdj_d'] = df['kdj_d9']
    df['kdj_j'] = df['kdj_j9']

    df['volume_avg20'] = df['volume'].rolling(20).mean()
    df['price_change_1h'] = df['close'].pct_change(1) * 100
    df['price_change_4h'] = df['close'].pct_change(4) * 100
    df['price_change_24h'] = df['close'].pct_change(24) * 100

    # 未預算的 maN/emaN/rsiN/atrN 可由 _RowProxy 在存取時動態補算

    _wc = ['whale_buy_vol', 'whale_sell_vol', 'whale_delta', 'whale_trade_count', 'whale_avg_size']
    for _c in _wc:
        if _c not in df.columns:
            df[_c] = 0.0
        else:
            df[_c] = pd.to_numeric(df[_c], errors='coerce').fillna(0)
    df['whale_total_vol'] = df['whale_buy_vol'] + df['whale_sell_vol']
    df['whale_buy_sell_ratio'] = np.where(
        df['whale_sell_vol'] > 0, df['whale_buy_vol'] / df['whale_sell_vol'], 1.0)
    df['whale_delta_ma4'] = df['whale_delta'].rolling(4).mean().fillna(0)
    _m20c = df['whale_trade_count'].rolling(20).mean()
    df['whale_activity_ratio'] = np.where(
        _m20c > 0, df['whale_trade_count'] / _m20c, 1.0)
    df['whale_sell_vol_ma20'] = df['whale_sell_vol'].rolling(20).mean().fillna(0)
    df['whale_buy_vol_ma20'] = df['whale_buy_vol'].rolling(20).mean().fillna(0)
    _sm = df['whale_sell_vol_ma20'].replace(0, np.nan)
    df['whale_sell_spike'] = (df['whale_sell_vol'] / _sm).fillna(0).replace([np.inf, -np.inf], 0)
    _bm = df['whale_buy_vol_ma20'].replace(0, np.nan)
    df['whale_buy_spike'] = (df['whale_buy_vol'] / _bm).fillna(0).replace([np.inf, -np.inf], 0)

    # ====== CVD（累積 Delta）指標（whale_delta_ma4 已於上計算）======
    df['whale_cvd'] = df['whale_delta'].cumsum()
    df['cvd_boost'] = df['whale_delta'].diff().fillna(0)
    df['whale_cvd_acceleration'] = df['whale_delta'].diff(2).fillna(0)
    df['cvd_momentum'] = df['whale_delta'] - df['whale_delta_ma4']

    # ====== 散戶指標 ======
    if 'retail_buy' in df.columns and 'retail_sell' in df.columns:
        df['retail_buy_vol'] = pd.to_numeric(df['retail_buy'], errors='coerce').fillna(0)
        df['retail_sell_vol'] = pd.to_numeric(df['retail_sell'], errors='coerce').fillna(0)
    if 'retail_buy_vol' in df.columns and 'retail_sell_vol' in df.columns:
        df['retail_buy_vol'] = pd.to_numeric(df['retail_buy_vol'], errors='coerce').fillna(0)
        df['retail_sell_vol'] = pd.to_numeric(df['retail_sell_vol'], errors='coerce').fillna(0)
        df['retail_delta'] = df['retail_buy_vol'] - df['retail_sell_vol']
        df['retail_buy_sell_ratio'] = df['retail_buy_vol'] / df['retail_sell_vol'].clip(lower=1)
        _rsm = df['retail_sell_vol'].rolling(20).mean()
        df['retail_sell_spike'] = (df['retail_sell_vol'] / _rsm.clip(lower=1)).fillna(0).replace([np.inf, -np.inf], 0)
        _rbm = df['retail_buy_vol'].rolling(20).mean()
        df['retail_buy_spike'] = (df['retail_buy_vol'] / _rbm.clip(lower=1)).fillna(0).replace([np.inf, -np.inf], 0)
    else:
        if 'retail_delta' not in df.columns:
            df['retail_delta'] = 0.0
        else:
            df['retail_delta'] = pd.to_numeric(df['retail_delta'], errors='coerce').fillna(0)
        df['retail_buy_vol'] = 0.0
        df['retail_sell_vol'] = 0.0
        df['retail_buy_sell_ratio'] = 1.0
        df['retail_sell_spike'] = 0.0
        df['retail_buy_spike'] = 0.0

    df['retail_cvd'] = df['retail_delta'].cumsum()
    df['retail_cvd_boost'] = df['retail_delta'].diff().fillna(0)

    # ====== 鯨魚 vs 散戶背離 ======
    df['whale_retail_divergence'] = df['whale_delta'] - df['retail_delta']
    df['divergence_signal'] = 0
    df.loc[(df['whale_delta'] > 0) & (df['retail_delta'] < 0), 'divergence_signal'] = 1
    df.loc[(df['whale_delta'] < 0) & (df['retail_delta'] > 0), 'divergence_signal'] = -1

    # ====== 清算指標 ======
    if 'long_liq' in df.columns:
        df['long_liq'] = pd.to_numeric(df['long_liq'], errors='coerce').fillna(0)
        if 'short_liq' in df.columns:
            df['short_liq'] = pd.to_numeric(df['short_liq'], errors='coerce').fillna(0)
        else:
            df['short_liq'] = 0.0
        if 'total_liq' in df.columns:
            df['total_liq'] = pd.to_numeric(df['total_liq'], errors='coerce').fillna(0)
        else:
            df['total_liq'] = df['long_liq'] + df['short_liq']
        df['liq_ratio'] = df['long_liq'] / df['total_liq'].clip(lower=1)
        _tlm = df['total_liq'].rolling(20).mean()
        df['liq_spike'] = (df['total_liq'] / _tlm.clip(lower=1)).fillna(0).replace([np.inf, -np.inf], 0)
        _llm = df['long_liq'].rolling(20).mean()
        df['long_liq_spike'] = (df['long_liq'] / _llm.clip(lower=1)).fillna(0).replace([np.inf, -np.inf], 0)
        _slm = df['short_liq'].rolling(20).mean()
        df['short_liq_spike'] = (df['short_liq'] / _slm.clip(lower=1)).fillna(0).replace([np.inf, -np.inf], 0)
    else:
        df['long_liq'] = 0.0
        df['short_liq'] = 0.0
        df['total_liq'] = 0.0
        df['liq_ratio'] = 0.5
        df['liq_spike'] = 0.0
        df['long_liq_spike'] = 0.0
        df['short_liq_spike'] = 0.0

    # ====== OI 指標 ======
    if 'oi_value' in df.columns:
        df['oi_value'] = pd.to_numeric(df['oi_value'], errors='coerce').ffill()
        if 'oi_change_1h' not in df.columns:
            df['oi_change_1h'] = df['oi_value'].pct_change(1) * 100
        else:
            df['oi_change_1h'] = pd.to_numeric(df['oi_change_1h'], errors='coerce')
        if 'oi_change_4h' not in df.columns:
            df['oi_change_4h'] = df['oi_value'].pct_change(4) * 100
        else:
            df['oi_change_4h'] = pd.to_numeric(df['oi_change_4h'], errors='coerce')
        if 'oi_change_24h' not in df.columns:
            df['oi_change_24h'] = df['oi_value'].pct_change(24) * 100
        else:
            df['oi_change_24h'] = pd.to_numeric(df['oi_change_24h'], errors='coerce')
        if 'oi_btc' in df.columns:
            df['oi_btc'] = pd.to_numeric(df['oi_btc'], errors='coerce').ffill()
        _oi_abs = df['oi_value'].diff().abs()
        _oi_chg_ma = _oi_abs.rolling(20).mean()
        df['oi_spike'] = (_oi_abs / _oi_chg_ma.clip(lower=1)).fillna(0).replace([np.inf, -np.inf], 0)
    else:
        df['oi_value'] = float('nan')
        df['oi_change_1h'] = 0.0
        df['oi_change_4h'] = 0.0
        df['oi_change_24h'] = 0.0
        df['oi_spike'] = 0.0

    # ====== 資金費率 ======
    if 'funding_rate' in df.columns:
        df['funding_rate'] = pd.to_numeric(df['funding_rate'], errors='coerce').ffill()
        df['funding_rate_ma'] = df['funding_rate'].rolling(20).mean()
        _m168 = df['funding_rate'].rolling(168).mean()
        _s168 = df['funding_rate'].rolling(168).std().clip(lower=0.0001)
        df['funding_rate_zscore'] = (df['funding_rate'] - _m168) / _s168
    else:
        df['funding_rate'] = float('nan')
        df['funding_rate_ma'] = float('nan')
        df['funding_rate_zscore'] = 0.0

    # ====== Coinglass 清算（日級展開到小時）======
    if 'cg_total_liq' in df.columns:
        df['cg_total_liq'] = pd.to_numeric(df['cg_total_liq'], errors='coerce').fillna(0)
        df['cg_long_liq'] = pd.to_numeric(df['cg_long_liq'], errors='coerce').fillna(0)
        df['cg_short_liq'] = pd.to_numeric(df['cg_short_liq'], errors='coerce').fillna(0)
        if 'cg_liq_ratio' not in df.columns:
            df['cg_liq_ratio'] = df['cg_long_liq'] / df['cg_total_liq'].clip(lower=1)
        _cg_ma = df['cg_total_liq'].rolling(20 * 24).mean()
        df['cg_liq_spike'] = (df['cg_total_liq'] / _cg_ma.clip(lower=1)).fillna(0).replace([np.inf, -np.inf], 0)
        _cgl_ma = df['cg_long_liq'].rolling(20 * 24).mean()
        df['cg_long_liq_spike'] = (df['cg_long_liq'] / _cgl_ma.clip(lower=1)).fillna(0).replace([np.inf, -np.inf], 0)
    else:
        df['cg_total_liq'] = 0.0
        df['cg_long_liq'] = 0.0
        df['cg_short_liq'] = 0.0
        df['cg_liq_ratio'] = 0.5
        df['cg_liq_spike'] = 0.0
        df['cg_long_liq_spike'] = 0.0

    # ====== 多空比指標 ======
    if 'long_short_ratio' in df.columns:
        df['long_ratio'] = pd.to_numeric(df['long_ratio'], errors='coerce').ffill()
        df['short_ratio'] = pd.to_numeric(df['short_ratio'], errors='coerce').ffill()
        df['long_short_ratio'] = pd.to_numeric(df['long_short_ratio'], errors='coerce').ffill()
    else:
        df['long_ratio'] = float('nan')
        df['short_ratio'] = float('nan')
        df['long_short_ratio'] = float('nan')

    # ====== Taker Buy/Sell 指標 ======
    if 'taker_buy_sell_ratio' in df.columns:
        df['taker_buy_vol'] = pd.to_numeric(df['taker_buy_vol'], errors='coerce').ffill()
        df['taker_sell_vol'] = pd.to_numeric(df['taker_sell_vol'], errors='coerce').ffill()
        df['taker_buy_sell_ratio'] = pd.to_numeric(df['taker_buy_sell_ratio'], errors='coerce').ffill()
        taker_ma = df['taker_buy_sell_ratio'].rolling(20).mean()
        df['taker_spike'] = (df['taker_buy_sell_ratio'] / taker_ma.clip(lower=0.01)).fillna(0).replace([np.inf, -np.inf], 0)
    else:
        df['taker_buy_vol'] = float('nan')
        df['taker_sell_vol'] = float('nan')
        df['taker_buy_sell_ratio'] = float('nan')
        df['taker_spike'] = 0.0

    # ====== 大戶多空比 ======
    if 'top_account_ratio' in df.columns:
        df['top_account_ratio'] = pd.to_numeric(df['top_account_ratio'], errors='coerce').ffill()
        df['top_account_long'] = pd.to_numeric(df['top_account_long'], errors='coerce').ffill()
        df['top_account_short'] = pd.to_numeric(df['top_account_short'], errors='coerce').ffill()
    else:
        df['top_account_ratio'] = float('nan')
        df['top_account_long'] = float('nan')
        df['top_account_short'] = float('nan')

    if 'top_position_ratio' in df.columns:
        df['top_position_ratio'] = pd.to_numeric(df['top_position_ratio'], errors='coerce').ffill()
        df['top_position_long'] = pd.to_numeric(df['top_position_long'], errors='coerce').ffill()
        df['top_position_short'] = pd.to_numeric(df['top_position_short'], errors='coerce').ffill()
    else:
        df['top_position_ratio'] = float('nan')
        df['top_position_long'] = float('nan')
        df['top_position_short'] = float('nan')

    # ====== Coinglass OI ======
    if 'cg_oi' in df.columns:
        df['cg_oi'] = pd.to_numeric(df['cg_oi'], errors='coerce').ffill()
        if 'cg_oi_change_1h' not in df.columns:
            df['cg_oi_change_1h'] = df['cg_oi'].pct_change(1) * 100
        else:
            df['cg_oi_change_1h'] = pd.to_numeric(df['cg_oi_change_1h'], errors='coerce')
        if 'cg_oi_change_24h' not in df.columns:
            df['cg_oi_change_24h'] = df['cg_oi'].pct_change(24) * 100
        else:
            df['cg_oi_change_24h'] = pd.to_numeric(df['cg_oi_change_24h'], errors='coerce')
    else:
        df['cg_oi'] = float('nan')
        df['cg_oi_change_1h'] = 0.0
        df['cg_oi_change_24h'] = 0.0

    df = df.copy()
    return df

def check_exit_indicators(row, prev_row):
    return {exit_indicator_check}

def run_backtest():
{cooldown_init}{data_load_block}{date_filter_code}    df = calculate_indicators(df)
    _close = df['close'].astype(float).values
    _timestamp = df['timestamp'].values
    data_start = _timestamp[0]
    data_end = _timestamp[-1]

{consecutive_helper}    def check_entry(row, prev_row, i):
        return {entry_return}

    trades = []
    position = None

    _n = len(df)
    _df_box = [df]
    try:
        _col_arrays = {{str(c): df[c].to_numpy(copy=False) for c in df.columns}}
    except Exception:
        _col_arrays = {{}}
        for _cx in df.columns:
            try:
                _col_arrays[str(_cx)] = df[_cx].to_numpy(copy=False)
            except Exception:
                pass
    class _RowProxy:
        __slots__ = ('_i',)
        def __init__(self, i):
            self._i = i
        def __getitem__(self, key):
            try:
                return _col_arrays[key][self._i]
            except KeyError:
                import re as _re_fb
                _df = _df_box[0]
                m = _re_fb.match(r'^(ma|ema|rsi|atr)(\\d+)$', key)
                if not m:
                    raise
                typ, ps = m.group(1), int(m.group(2))
                close = _df['close']
                if typ == 'ma':
                    _df[key] = close.rolling(ps).mean()
                elif typ == 'ema':
                    _df[key] = close.ewm(span=ps, adjust=False).mean()
                elif typ == 'rsi':
                    d = close.diff()
                    g = d.where(d > 0, 0.0)
                    ls = (-d.where(d < 0, 0.0))
                    ag = g.rolling(window=ps).mean()
                    al = ls.rolling(window=ps).mean()
                    rs = ag / al
                    _df[key] = 100 - (100 / (1 + rs))
                else:
                    high_low = _df['high'] - _df['low']
                    high_close = (_df['high'] - close.shift(1)).abs()
                    low_close = (_df['low'] - close.shift(1)).abs()
                    true_range = high_low.combine(high_close, max).combine(low_close, max)
                    _df[key] = true_range.rolling(ps).mean()
                _col_arrays[key] = _df[key].values
                return _col_arrays[key][self._i]
        def get(self, key, default=None):
            if key in _col_arrays:
                return _col_arrays[key][self._i]
            return default

    for i in range(200, _n):
        row = _RowProxy(i)
        prev_row = _RowProxy(i - 1)

        if position is None:
{cooldown_check}            if check_entry(row, prev_row, i):
                position = {{
                    'entry_price': float(_close[i]),
                    'entry_idx': i,
                    'entry_time': _timestamp[i],
                    'highest_price': float(_close[i]),
                    'breakeven_activated': False
                }}
{cooldown_update}        else:
            current_price = float(_close[i])
            entry_price = position['entry_price']
            hold_hours = (i - position['entry_idx']) * BAR_HOURS

            if DIRECTION == 'long':
                position['highest_price'] = max(position['highest_price'], current_price)
                pnl_pct = (current_price - entry_price) / entry_price * 100
            else:
                position['highest_price'] = min(position['highest_price'], current_price)
                pnl_pct = (entry_price - current_price) / entry_price * 100

            if BREAKEVEN_TRIGGER_PCT is not None and not position['breakeven_activated']:
                if pnl_pct >= BREAKEVEN_TRIGGER_PCT:
                    position['breakeven_activated'] = True

            exit_reason = None

            if TAKE_PROFIT_PCT is not None and pnl_pct >= TAKE_PROFIT_PCT:
                exit_reason = 'take_profit'

            if TRAILING_STOP_PCT is not None and exit_reason is None:
                if DIRECTION == 'long':
                    trailing_pnl = (current_price - position['highest_price']) / position['highest_price'] * 100
                else:
                    trailing_pnl = (position['highest_price'] - current_price) / position['highest_price'] * 100
                if trailing_pnl <= -TRAILING_STOP_PCT:
                    exit_reason = 'trailing_stop'

            if position['breakeven_activated'] and exit_reason is None:
                if pnl_pct <= 0:
                    exit_reason = 'breakeven_stop'

            if STOP_LOSS_PCT is not None and exit_reason is None and pnl_pct <= -STOP_LOSS_PCT:
                exit_reason = 'stop_loss'

            if exit_reason is None and check_exit_indicators(row, prev_row):
                exit_reason = 'indicator_exit'

            if exit_reason is None and hold_hours >= MAX_HOLD_HOURS:
                exit_reason = 'timeout'

            if exit_reason:
                trades.append({{
                    'entry_idx': position['entry_idx'],
                    'entry_price': entry_price,
                    'exit_price': current_price,
                    'pnl_pct': round(pnl_pct, 4),
                    'hold_hours': hold_hours,
                    'exit_reason': exit_reason,
                    'exit_idx': i
                }})
                position = None

    _START = 200
    _fc = float(_close[_START])
    _lc = float(_close[-1])
    _benchmark = round((_lc - _fc) / _fc * 100, 2)

    _max_klines = 2000
    _kline_indices = list(range(len(df)))
    if len(_kline_indices) > _max_klines:
        _step = len(_kline_indices) / _max_klines
        _kline_indices = [int(i * _step) for i in range(_max_klines)]
        if _kline_indices[-1] != len(df) - 1:
            _kline_indices.append(len(df) - 1)
    _trade_indices = set()
    for _t in trades[:50]:
        _trade_indices.add(_t['entry_idx'])
        _trade_indices.add(_t['exit_idx'])
    _kline_indices = sorted(set(_kline_indices) | _trade_indices)

    _kl = []
    for _ki in _kline_indices:
        _kr = _RowProxy(_ki)
        _kl.append({{
            'time': int(pd.Timestamp(_timestamp[_ki]).timestamp()),
            'open': round(float(_kr['open']), 2),
            'high': round(float(_kr['high']), 2),
            'low': round(float(_kr['low']), 2),
            'close': round(float(_kr['close']), 2)
        }})

    if len(trades) == 0:
        results = {{
            'total_trades': 0, 'win_rate': 0, 'total_pnl_pct': 0,
            'avg_pnl_pct': 0, 'max_drawdown_pct': 0,
            'best_trade_pct': 0, 'worst_trade_pct': 0,
            'avg_hold_hours': 0, 'sharpe_ratio': 0,
            'data_start': data_start,
            'data_end': data_end,
            'benchmark_return': _benchmark,
            'kline_data': _kl,
            'data_timeframe': DATA_TIMEFRAME
        }}
    else:
        pnls = [t['pnl_pct'] for t in trades]
        wins = len([p for p in pnls if p > 0])
        cumulative = np.cumsum(pnls)
        running_max = np.maximum.accumulate(cumulative)
        drawdowns = cumulative - running_max
        max_dd = float(np.min(drawdowns)) if len(drawdowns) > 0 else 0
        sharpe = float(np.mean(pnls) / np.std(pnls)) if np.std(pnls) > 0 else 0

        equity_curve = [{{'x': str(_timestamp[_START]), 'y': 100.0}}]
        _eq = 100.0
        for _t in trades:
            _eq *= (1 + _t['pnl_pct'] / 100.0)
            equity_curve.append({{'x': str(_timestamp[_t['exit_idx']]), 'y': round(_eq, 2)}})

        _td = []
        for _t in trades[:50]:
            _td.append({{
                'entry_time': str(_timestamp[_t['entry_idx']]),
                'exit_time': str(_timestamp[_t['exit_idx']]),
                'entry_ts': int(pd.Timestamp(_timestamp[_t['entry_idx']]).timestamp()),
                'exit_ts': int(pd.Timestamp(_timestamp[_t['exit_idx']]).timestamp()),
                'entry_price': round(float(_t['entry_price']), 2),
                'exit_price': round(float(_t['exit_price']), 2),
                'pnl_pct': round(float(_t['pnl_pct']), 4),
                'direction': DIRECTION
            }})

        results = {{
            'total_trades': len(trades),
            'win_rate': round(wins / len(trades) * 100, 1),
            'total_pnl_pct': round(sum(pnls), 2),
            'avg_pnl_pct': round(float(np.mean(pnls)), 4),
            'max_drawdown_pct': round(max_dd, 2),
            'best_trade_pct': round(max(pnls), 4),
            'worst_trade_pct': round(min(pnls), 4),
            'avg_hold_hours': round(float(np.mean([t['hold_hours'] for t in trades])), 1),
            'sharpe_ratio': round(sharpe, 3),
            'data_start': data_start,
            'data_end': data_end,
            'benchmark_return': _benchmark,
            'equity_curve': equity_curve,
            'trade_details': _td,
            'total_trades_shown': len(_td),
            'kline_data': _kl,
            'data_timeframe': DATA_TIMEFRAME
        }}

    print(json.dumps(results))

if __name__ == '__main__':
    run_backtest()
'''
    return code


def _bb_suffix_from_cond(cond):
    bp = int(cond.get("bb_period", 20))
    bm = float(cond.get("bb_mult", 2))
    return str(bp) + "_" + str(bm).replace(".", "").replace(",", "")


def _macd_suffix_from_cond(cond):
    return f"{int(cond.get('macd_fast', 12))}_{int(cond.get('macd_slow', 26))}_{int(cond.get('macd_signal', 9))}"


def _generate_condition_code(cond, for_entry=True):
    """把單個條件JSON轉成Python表達式。for_entry=True 時用 row/prev_row（與 check_entry 一致）。"""
    indicator = cond.get("indicator", "")
    operator = cond.get("operator", "")
    value = cond.get("value", "")
    timeframe = cond.get("timeframe", "1h")
    if for_entry:
        r, p = "row", "prev_row"
    else:
        r, p = "row", "prev_row"

    if indicator == "price_change_pct":
        tf_map = {"1h": "price_change_1h", "4h": "price_change_4h", "24h": "price_change_24h"}
        col = tf_map.get(timeframe, "price_change_1h")
        return f"{r}['{col}'] {operator} {value}"

    elif indicator == "volume_ratio":
        return f"({r}['volume'] / {r}['volume_avg20']) {operator} {value}"

    elif indicator == "rsi":
        try:
            rp = int(cond.get("period", 14))
        except (TypeError, ValueError):
            rp = 14
        return f"float({r}['rsi{rp}']) {operator} {value}"

    elif indicator == "price_above_ma":
        try:
            _n = int(float(value)) if value is not None else 20
        except (TypeError, ValueError):
            _n = 20
        ma_col = f"ma{_n}"
        return f"{r}['close'] > {r}['{ma_col}']"

    elif indicator == "price_below_ma":
        try:
            _n = int(float(value)) if value is not None else 20
        except (TypeError, ValueError):
            _n = 20
        ma_col = f"ma{_n}"
        return f"{r}['close'] < {r}['{ma_col}']"

    elif indicator == "bollinger":
        _bs = _bb_suffix_from_cond(cond)
        if value == "below_lower":
            return f"{r}['close'] < {r}['bb_lower_{_bs}']"
        elif value == "above_upper":
            return f"{r}['close'] > {r}['bb_upper_{_bs}']"
        elif value == "below_middle":
            return f"{r}['close'] < {r}['bb_middle_{_bs}']"
        elif value == "above_middle":
            return f"{r}['close'] > {r}['bb_middle_{_bs}']"
        return None

    elif indicator == "ma_cross":
        if isinstance(value, str) and "_above_" in value:
            parts = value.split("_above_")
            return f"{r}['{parts[0]}'] > {r}['{parts[1]}'] and {p}['{parts[0]}'] <= {p}['{parts[1]}']"
        elif isinstance(value, str) and "_below_" in value:
            parts = value.split("_below_")
            return f"{r}['{parts[0]}'] < {r}['{parts[1]}'] and {p}['{parts[0]}'] >= {p}['{parts[1]}']"
        return None

    elif indicator == "ema_cross":
        if isinstance(value, str) and "_above_" in value:
            parts = value.split("_above_")
            return f"{r}['{parts[0]}'] > {r}['{parts[1]}'] and {p}['{parts[0]}'] <= {p}['{parts[1]}']"
        elif isinstance(value, str) and "_below_" in value:
            parts = value.split("_below_")
            return f"{r}['{parts[0]}'] < {r}['{parts[1]}'] and {p}['{parts[0]}'] >= {p}['{parts[1]}']"
        return None

    elif indicator == "price_above_ema":
        try:
            _en = int(float(value)) if value is not None else 20
        except (TypeError, ValueError):
            _en = 20
        return f"{r}['close'] > {r}['ema{_en}']"

    elif indicator == "price_below_ema":
        try:
            _en = int(float(value)) if value is not None else 20
        except (TypeError, ValueError):
            _en = 20
        return f"{r}['close'] < {r}['ema{_en}']"

    elif indicator == "macd_cross":
        sfx = _macd_suffix_from_cond(cond)
        if value == "bullish":
            return (
                f"{r}['macd_{sfx}'] > {r}['macd_signal_{sfx}'] "
                f"and {p}['macd_{sfx}'] <= {p}['macd_signal_{sfx}']"
            )
        elif value == "bearish":
            return (
                f"{r}['macd_{sfx}'] < {r}['macd_signal_{sfx}'] "
                f"and {p}['macd_{sfx}'] >= {p}['macd_signal_{sfx}']"
            )
        return None

    elif indicator == "macd_hist":
        sfx = _macd_suffix_from_cond(cond)
        return f"float({r}['macd_hist_{sfx}']) {operator} {value}"

    elif indicator == "atr":
        try:
            ap = int(cond.get("period", 14))
        except (TypeError, ValueError):
            ap = 14
        return f"float({r}['atr{ap}']) {operator} {value}"

    elif indicator == "price_above_vwap":
        try:
            vp = int(cond.get("period", 24))
        except (TypeError, ValueError):
            vp = 24
        return f"float({r}['close']) > float({r}['vwap{vp}'])"

    elif indicator == "price_below_vwap":
        try:
            vp = int(cond.get("period", 24))
        except (TypeError, ValueError):
            vp = 24
        return f"float({r}['close']) < float({r}['vwap{vp}'])"

    elif indicator == "stoch_rsi":
        try:
            sp = int(cond.get("period", 14))
        except (TypeError, ValueError):
            sp = 14
        return f"float({r}['stoch_rsi_k{sp}']) {operator} {value}"

    elif indicator == "kdj_j":
        try:
            kp = int(cond.get("period", 9))
        except (TypeError, ValueError):
            kp = 9
        return f"float({r}['kdj_j{kp}']) {operator} {value}"

    elif indicator == "kdj_cross":
        try:
            kp = int(cond.get("period", 9))
        except (TypeError, ValueError):
            kp = 9
        if value == "bullish":
            return (
                f"{r}['kdj_k{kp}'] > {r}['kdj_d{kp}'] "
                f"and {p}['kdj_k{kp}'] <= {p}['kdj_d{kp}']"
            )
        elif value == "bearish":
            return (
                f"{r}['kdj_k{kp}'] < {r}['kdj_d{kp}'] "
                f"and {p}['kdj_k{kp}'] >= {p}['kdj_d{kp}']"
            )
        return None

    elif indicator == "consecutive_candles":
        if not for_entry:
            return None
        pat = cond.get("pattern", "bearish")
        cnt = int(cond.get("count", 3))
        then_v = cond.get("then", "bullish")
        return f"_consecutive_pattern(i, {repr(pat)}, {cnt}, {repr(then_v)})"

    elif indicator == "consecutive_whale_sell_increase":
        if not for_entry:
            return None
        bars = int(cond.get("bars", 3) or 3)
        min_peak = cond.get("min_peak")
        if min_peak is None:
            min_peak = 50000000
        col = "whale_sell_vol"
        # 連續 bars 根遞增：i-bars, i-bars+1, ..., i-1（高峰在 i-1），當前 bar i 為「下一根」
        parts = [f"float(_col_arrays['{col}'][i-{bars}+{j}]) < float(_col_arrays['{col}'][i-{bars}+{j+1}])" for j in range(bars - 1)]
        expr = " and ".join(parts)
        guard = f"i >= {bars}"
        if min_peak is not None:
            expr += f" and float(_col_arrays['{col}'][i-1]) >= {min_peak}"
        return f"({guard} and {expr})"

    elif indicator == "whale_sell_peak_decline":
        if not for_entry:
            return None
        ratio = float(cond.get("decline_ratio") if cond.get("decline_ratio") is not None else 0.3)
        return f"(i >= 1 and float(_col_arrays['whale_sell_vol'][i]) < float(_col_arrays['whale_sell_vol'][i-1]) * {ratio})"

    elif indicator == "whale_sell_vol":
        return f"float({r}['whale_sell_vol']) {operator} {value}"
    elif indicator == "whale_buy_vol":
        return f"float({r}['whale_buy_vol']) {operator} {value}"
    elif indicator == "whale_delta":
        return f"float({r}['whale_delta']) {operator} {value}"
    elif indicator == "whale_buy_sell_ratio":
        return f"float({r}['whale_buy_sell_ratio']) {operator} {value}"
    elif indicator == "whale_trade_count":
        return f"float({r}['whale_trade_count']) {operator} {value}"
    elif indicator == "whale_activity_ratio":
        return f"float({r}['whale_activity_ratio']) {operator} {value}"
    elif indicator == "whale_sell_spike":
        return f"float({r}['whale_sell_spike']) {operator} {value}"
    elif indicator == "whale_buy_spike":
        return f"float({r}['whale_buy_spike']) {operator} {value}"

    elif indicator == "whale_cvd":
        return f"float({r}['whale_cvd']) {operator} {value}"
    elif indicator == "cvd_boost":
        return f"float({r}['cvd_boost']) {operator} {value}"
    elif indicator == "whale_cvd_acceleration":
        return f"float({r}['whale_cvd_acceleration']) {operator} {value}"
    elif indicator == "whale_delta_ma4":
        return f"float({r}['whale_delta_ma4']) {operator} {value}"
    elif indicator == "cvd_momentum":
        return f"float({r}['cvd_momentum']) {operator} {value}"
    elif indicator == "retail_cvd":
        return f"float({r}['retail_cvd']) {operator} {value}"
    elif indicator == "retail_cvd_boost":
        return f"float({r}['retail_cvd_boost']) {operator} {value}"

    elif indicator == "retail_sell_vol":
        return f"float({r}['retail_sell_vol']) {operator} {value}"
    elif indicator == "retail_buy_vol":
        return f"float({r}['retail_buy_vol']) {operator} {value}"
    elif indicator == "retail_delta":
        return f"float({r}['retail_delta']) {operator} {value}"
    elif indicator == "retail_buy_sell_ratio":
        return f"float({r}['retail_buy_sell_ratio']) {operator} {value}"
    elif indicator == "retail_sell_spike":
        return f"float({r}['retail_sell_spike']) {operator} {value}"
    elif indicator == "retail_buy_spike":
        return f"float({r}['retail_buy_spike']) {operator} {value}"

    elif indicator == "whale_retail_divergence":
        return f"float({r}['whale_retail_divergence']) {operator} {value}"
    elif indicator == "divergence_signal":
        return f"float({r}['divergence_signal']) {operator} {value}"

    elif indicator == "long_liq":
        return f"float({r}['long_liq']) {operator} {value}"
    elif indicator == "short_liq":
        return f"float({r}['short_liq']) {operator} {value}"
    elif indicator == "total_liq":
        return f"float({r}['total_liq']) {operator} {value}"
    elif indicator == "liq_ratio":
        return f"float({r}['liq_ratio']) {operator} {value}"
    elif indicator == "liq_spike":
        return f"float({r}['liq_spike']) {operator} {value}"
    elif indicator == "long_liq_spike":
        return f"float({r}['long_liq_spike']) {operator} {value}"
    elif indicator == "short_liq_spike":
        return f"float({r}['short_liq_spike']) {operator} {value}"

    elif indicator == "oi_value":
        return f"float({r}['oi_value']) {operator} {value}"
    elif indicator == "oi_change_1h":
        return f"float({r}['oi_change_1h']) {operator} {value}"
    elif indicator == "oi_change_4h":
        return f"float({r}['oi_change_4h']) {operator} {value}"
    elif indicator == "oi_change_24h":
        return f"float({r}['oi_change_24h']) {operator} {value}"
    elif indicator == "oi_spike":
        return f"float({r}['oi_spike']) {operator} {value}"

    elif indicator == "funding_rate":
        return f"float({r}['funding_rate']) {operator} {value}"
    elif indicator == "funding_rate_ma":
        return f"float({r}['funding_rate_ma']) {operator} {value}"
    elif indicator == "funding_rate_zscore":
        return f"float({r}['funding_rate_zscore']) {operator} {value}"

    elif indicator == "cg_long_liq":
        return f"float({r}['cg_long_liq']) {operator} {value}"
    elif indicator == "cg_short_liq":
        return f"float({r}['cg_short_liq']) {operator} {value}"
    elif indicator == "cg_total_liq":
        return f"float({r}['cg_total_liq']) {operator} {value}"
    elif indicator == "cg_liq_ratio":
        return f"float({r}['cg_liq_ratio']) {operator} {value}"
    elif indicator == "cg_liq_spike":
        return f"float({r}['cg_liq_spike']) {operator} {value}"
    elif indicator == "cg_long_liq_spike":
        return f"float({r}['cg_long_liq_spike']) {operator} {value}"
    elif indicator == "long_short_ratio":
        return f"float({r}['long_short_ratio']) {operator} {value}"
    elif indicator == "long_ratio":
        return f"float({r}['long_ratio']) {operator} {value}"
    elif indicator == "short_ratio":
        return f"float({r}['short_ratio']) {operator} {value}"
    elif indicator == "taker_buy_sell_ratio":
        return f"float({r}['taker_buy_sell_ratio']) {operator} {value}"
    elif indicator == "taker_buy_vol":
        return f"float({r}['taker_buy_vol']) {operator} {value}"
    elif indicator == "taker_sell_vol":
        return f"float({r}['taker_sell_vol']) {operator} {value}"
    elif indicator == "taker_spike":
        return f"float({r}['taker_spike']) {operator} {value}"
    elif indicator == "top_account_ratio":
        return f"float({r}['top_account_ratio']) {operator} {value}"
    elif indicator == "top_account_long":
        return f"float({r}['top_account_long']) {operator} {value}"
    elif indicator == "top_position_ratio":
        return f"float({r}['top_position_ratio']) {operator} {value}"
    elif indicator == "top_position_long":
        return f"float({r}['top_position_long']) {operator} {value}"
    elif indicator == "cg_oi":
        return f"float({r}['cg_oi']) {operator} {value}"
    elif indicator == "cg_oi_change_1h":
        return f"float({r}['cg_oi_change_1h']) {operator} {value}"
    elif indicator == "cg_oi_change_24h":
        return f"float({r}['cg_oi_change_24h']) {operator} {value}"

    return None


# === 測試 ===
if __name__ == "__main__":
    test1 = {
        "entry_conditions": [
            {"indicator": "rsi", "operator": "<", "value": 30}
        ],
        "exit_conditions": {
            "take_profit_pct": 3.0,
            "stop_loss_pct": 2.0,
            "max_hold_hours": 48,
            "trailing_stop_pct": None,
            "breakeven_trigger_pct": None,
            "exit_indicators": []
        },
        "direction": "long"
    }

    test2 = {
        "entry_conditions": [
            {"indicator": "macd_cross", "value": "bullish"}
        ],
        "exit_conditions": {
            "take_profit_pct": None,
            "stop_loss_pct": 3.0,
            "max_hold_hours": 72,
            "trailing_stop_pct": 2.0,
            "breakeven_trigger_pct": None,
            "exit_indicators": []
        },
        "direction": "long"
    }

    test3 = {
        "entry_conditions": [
            {"indicator": "rsi", "operator": "<", "value": 30}
        ],
        "exit_conditions": {
            "take_profit_pct": None,
            "stop_loss_pct": 2.0,
            "max_hold_hours": 48,
            "trailing_stop_pct": None,
            "breakeven_trigger_pct": None,
            "exit_indicators": [
                {"indicator": "rsi", "operator": ">", "value": 70}
            ]
        },
        "direction": "long"
    }

    for i, test in enumerate([test1, test2, test3], 1):
        print(f"\n{'='*40}")
        print(f"測試 {i}")
        print(f"{'='*40}")
        code = generate_backtest_code(test)
        with open(f"/tmp/test_dynamic_exit_{i}.py", "w") as f:
            f.write(code)
        print(f"代碼已存到 /tmp/test_dynamic_exit_{i}.py")

    test4 = {
        "entry_conditions": [
            {"indicator": "rsi", "operator": "<", "value": 30}
        ],
        "exit_conditions": {
            "take_profit_pct": 2.0,
            "stop_loss_pct": 1.5,
            "max_hold_hours": 48,
            "trailing_stop_pct": None,
            "breakeven_trigger_pct": None,
            "exit_indicators": []
        },
        "direction": "long"
    }

    print(f"\n{'='*40}")
    print("測試 4（帶日期範圍 2024-06-01 ~ 2024-12-31）")
    print(f"{'='*40}")
    code = generate_backtest_code(test4, date_start="2024-06-01", date_end="2024-12-31")
    with open("/tmp/test_date_range.py", "w") as f:
        f.write(code)
    print("代碼已存到 /tmp/test_date_range.py")

    # 測試 5：多bar鯨魚形態 - 連續三根鯨魚賣出遞增且高峰超過5000萬，下一根收斂後做多
    test5_whale = {
        "entry_conditions": [
            {"indicator": "consecutive_whale_sell_increase", "bars": 3, "min_peak": 50000000},
            {"indicator": "whale_sell_peak_decline", "decline_ratio": 0.3}
        ],
        "exit_conditions": {
            "take_profit_pct": None,
            "stop_loss_pct": 1.5,
            "max_hold_hours": 48,
            "trailing_stop_pct": 1.5,
            "breakeven_trigger_pct": None,
            "exit_indicators": []
        },
        "direction": "long"
    }
    print(f"\n{'='*40}")
    print("測試 5（鯨魚多bar：連續三根遞增+高峰5000萬+收斂後做多，止損1.5% 移動止損1.5%）")
    print(f"{'='*40}")
    code5 = generate_backtest_code(test5_whale, date_start="2024-06-01", date_end="2024-12-31")
    with open("/tmp/test_whale_multibar.py", "w") as f:
        f.write(code5)
    print("代碼已存到 /tmp/test_whale_multibar.py")
