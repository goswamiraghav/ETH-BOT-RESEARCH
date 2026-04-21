"""
signal_generator_5m.py
─────────────────────────────────────────────────────────────────────────────
Scalp signal for ETH/USDT on the 5m timeframe.

SIGNAL LOGIC:
  Hard gates (ALL must pass or no trade):
    1. Trend  — EMA9 > EMA20
    2. MACD   — MACD line > Signal line (positive histogram)
    3. RSI    — RSI between 45 and 65 (momentum without being overbought)
    4. Volume — volume_ratio >= 1.5x 20-period average
    5. Close  — close >= candle high * 0.996  (strong momentum close)
    6. Body   — candle body > 0.3x ATR  (not a doji/indecision candle)

  All 6 gates must pass simultaneously. No scoring — all-or-nothing.

REQUIRES in DataFrame:
  Pre-computed columns (add via transformation_v2.add_all_indicators_v2):
    ema_9, ema_20, macd, macd_signal, rsi,
    atr, volume_ratio, vwap
"""

from __future__ import annotations


def generate_signal_5m(df) -> dict:
    """
    Evaluate the 5m scalp signal on the last row of df.

    Parameters
    ----------
    df : pd.DataFrame
        Sliding window passed by the backtester or live bot.
        Must contain pre-computed indicator columns.

    Returns
    -------
    dict with keys:
        final_signal      : bool
        match_score       : int   (always 6 if signal fires, 0 otherwise)
        signal_combo_name : str
        logic_debug_note  : str
        rsi               : float  (only when final_signal=True)
        volume_ratio      : float  (only when final_signal=True)
        + individual gate booleans
    """
    current = df.iloc[-1]

    required = [
        'atr', 'rsi', 'ema_9', 'ema_20',
        'macd', 'macd_signal', 'volume_ratio',
    ]
    if current[required].isna().any():
        return _no_signal('missing_indicators')

    # ── Gate 1: Trend alignment ────────────────────────────────────────────
    g1_trend = bool(current['ema_9'] > current['ema_20'])
    if not g1_trend:
        return _no_signal('trend_fail')

    # ── Gate 2: MACD positive ──────────────────────────────────────────────
    g2_macd = bool(current['macd'] > current['macd_signal'])
    if not g2_macd:
        return _no_signal('macd_fail')

    # ── Gate 3: RSI in momentum zone ──────────────────────────────────────
    g3_rsi = bool(45 <= current['rsi'] <= 65)
    if not g3_rsi:
        return _no_signal('rsi_out_of_range')

    # ── Gate 4: Volume spike >= 1.5x ──────────────────────────────────────
    g4_volume = bool(current.get('volume_ratio', 0) >= 1.5)
    if not g4_volume:
        return _no_signal('volume_fail')

    # ── Gate 5: Strong close (near candle high) ────────────────────────────
    g5_close_near_high = bool(current['close'] >= current['high'] * 0.996)
    if not g5_close_near_high:
        return _no_signal('weak_close')

    # ── Gate 6: Meaningful body (not a doji) ──────────────────────────────
    body = abs(current['close'] - current['open'])
    g6_body = bool(body > current['atr'] * 0.3)
    if not g6_body:
        return _no_signal('small_body')

    # ── All gates passed ───────────────────────────────────────────────────
    return {
        'final_signal':       True,
        'match_score':        6,
        'signal_combo_name':  'ema_cross+macd+rsi45_65+vol1.5x+close_hi+body',
        'logic_debug_note':   'all 6 gates passed',
        'g1_trend':           True,
        'g2_macd_pos':        True,
        'g3_rsi_zone':        True,
        'g4_volume_1p5x':     True,
        'g5_close_near_high': True,
        'g6_strong_body':     True,
        'rsi':                round(float(current['rsi']), 1),
        'volume_ratio':       round(float(current.get('volume_ratio', 0)), 2),
    }


def _no_signal(reason: str) -> dict:
    return {
        'final_signal':      False,
        'match_score':       0,
        'signal_combo_name': 'none',
        'logic_debug_note':  reason,
    }
