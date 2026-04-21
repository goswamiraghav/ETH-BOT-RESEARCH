"""
transformation_v2.py
─────────────────────────────────────────────────────────────────────────────
Drop-in replacement for transformation_clean.py.
Adds EMA50 and EMA200 required by signal_generator_v2.
All existing indicators preserved.
"""

import pandas as pd


def add_ema(df, span, column_name):
    df = df.copy()
    df[column_name] = df['close'].ewm(span=span, adjust=False).mean()
    return df


def add_ema9_ema20(df):
    df = add_ema(df, 9,  'ema_9')
    df = add_ema(df, 20, 'ema_20')
    return df


def add_ema50_ema200(df):
    """NEW: required for trend filter in signal_generator_v2."""
    df = add_ema(df, 50,  'ema50')
    df = add_ema(df, 200, 'ema200')
    return df


def add_macd(df):
    df = df.copy()
    ema_12 = df['close'].ewm(span=12, adjust=False).mean()
    ema_26 = df['close'].ewm(span=26, adjust=False).mean()
    df['macd']           = ema_12 - ema_26
    df['macd_signal']    = df['macd'].ewm(span=9, adjust=False).mean()
    df['macd_histogram'] = df['macd'] - df['macd_signal']
    return df


def add_rsi(df, period=14):
    df = df.copy()
    delta    = df['close'].diff()
    gain     = delta.clip(lower=0)
    loss     = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1/period, adjust=False, min_periods=period).mean()
    avg_loss = loss.ewm(alpha=1/period, adjust=False, min_periods=period).mean()
    rs       = avg_gain / avg_loss.replace(0, pd.NA)
    df['rsi'] = 100 - (100 / (1 + rs))
    return df


def add_bollinger_bands(df, period=20, multiplier=2):
    df = df.copy()
    df['bb_middle'] = df['close'].rolling(window=period, min_periods=period).mean()
    rolling_std     = df['close'].rolling(window=period, min_periods=period).std()
    df['bb_upper']  = df['bb_middle'] + multiplier * rolling_std
    df['bb_lower']  = df['bb_middle'] - multiplier * rolling_std
    return df


def add_atr(df, period=14):
    df = df.copy()
    prev_close = df['close'].shift(1)
    tr1 = df['high'] - df['low']
    tr2 = (df['high'] - prev_close).abs()
    tr3 = (df['low']  - prev_close).abs()
    tr  = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    df['atr'] = tr.rolling(window=period, min_periods=period).mean()
    return df


def add_vwap(df):
    df = df.copy()
    typical_price = (df['high'] + df['low'] + df['close']) / 3
    if 'timestamp' in df.columns:
        session  = pd.to_datetime(df['timestamp']).dt.date
        cum_pv   = (typical_price * df['volume']).groupby(session).cumsum()
        cum_vol  = df['volume'].groupby(session).cumsum()
        df['vwap'] = cum_pv / cum_vol
    else:
        df['vwap'] = (typical_price * df['volume']).cumsum() / df['volume'].cumsum()
    return df


def add_volume_features(df, window=20):
    df = df.copy()
    df['volume_ma_20'] = df['volume'].rolling(window=window, min_periods=window).mean()
    df['volume_ratio'] = df['volume'] / df['volume_ma_20']
    return df


def add_all_indicators_v2(df):
    """
    Full indicator suite including EMA50 + EMA200.
    Use this instead of add_all_indicators from transformation_clean.py
    when running signal_generator_v2.
    """
    df = add_ema9_ema20(df)
    df = add_ema50_ema200(df)     # NEW
    df = add_macd(df)
    df = add_rsi(df)
    df = add_bollinger_bands(df)
    df = add_atr(df)
    df = add_vwap(df)
    df = add_volume_features(df)
    return df


# ── Backward compat alias ─────────────────────────────────────────────────
def add_all_indicators(df):
    """Original function — now also includes EMA50/200."""
    return add_all_indicators_v2(df)
