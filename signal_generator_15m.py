"""
signal_generator_15m.py
─────────────────────────────────────────────────────────────────────────────
Swing signal for ETH/USDT on the 15m timeframe with 1h trend confirmation.

SIGNAL LOGIC:
  Hard gates (ALL must pass or no trade):
    1. 1h trend  — close > ema20_1h  AND  macd_1h > macd_signal_1h
    2. 15m trend — EMA9 > EMA20
    3. MACD      — MACD line > Signal line (positive histogram)
    4. Volume    — volume_ratio >= 1.3x 20-period average
    5. ATR size  — current ATR > 0.5% of price (filters ultra-low volatility)
    6. Pullback  — prev candle low touched within 0.2% of EMA20, current close
                   back above EMA20 (clean pullback entry, not a midair breakout)
    7. RSI       — RSI between 45 and 70 (momentum, not overbought)
    8. Close     — close >= candle high * 0.996  (strong momentum close)
    9. Body      — candle body > 0.25x ATR  (not a doji/indecision candle)

  All 9 gates must pass simultaneously. No scoring — all-or-nothing.

REQUIRES in DataFrame:
  From transformation_v2.add_all_indicators_v2 (15m):
    ema_9, ema_20, ema50, macd, macd_signal, rsi, atr, volume_ratio

  Pre-joined from 1h data (via pd.merge_asof in run_backtest_v2.py):
    ema20_1h, macd_1h, macd_signal_1h
"""

from __future__ import annotations


def generate_signal_15m(df) -> dict:
    """
    Evaluate the 15m swing signal on the last row of df.

    Parameters
    ----------
    df : pd.DataFrame
        Sliding window passed by the backtester or live bot.
        Must contain pre-computed indicator columns including 1h trend columns.

    Returns
    -------
    dict with keys:
        final_signal      : bool
        match_score       : int   (always 8 if signal fires, 0 otherwise)
        signal_combo_name : str
        logic_debug_note  : str
        rsi               : float  (only when final_signal=True)
        volume_ratio      : float  (only when final_signal=True)
        + individual gate booleans
    """
    current = df.iloc[-1]

    if len(df) < 2:
        return _no_signal('insufficient_history')

    current = df.iloc[-1]
    prev    = df.iloc[-2]

    required = [
        'atr', 'rsi',
        'ema_9', 'ema_20',
        'macd', 'macd_signal', 'volume_ratio',
        'ema20_1h', 'macd_1h', 'macd_signal_1h',
    ]
    if current[required].isna().any():
        return _no_signal('missing_indicators')

    # ── Gate 1: 1h trend + MACD confirmation ──────────────────────────────
    g1_1h_trend = bool(
        current['close']   > current['ema20_1h']       and
        current['macd_1h'] > current['macd_signal_1h']
    )
    if not g1_1h_trend:
        return _no_signal('1h_trend_fail')

    # ── Gate 2: 15m trend alignment ───────────────────────────────────────
    g2_15m_trend = bool(current['ema_9'] > current['ema_20'])
    if not g2_15m_trend:
        return _no_signal('15m_trend_fail')

    # ── Gate 3: 15m MACD positive ─────────────────────────────────────────
    g3_macd = bool(current['macd'] > current['macd_signal'])
    if not g3_macd:
        return _no_signal('macd_fail')

    # ── Gate 4: Volume spike >= 1.3x ──────────────────────────────────────
    g4_volume = bool(current.get('volume_ratio', 0) >= 1.3)
    if not g4_volume:
        return _no_signal('volume_fail')

    # ── Gate 5: Minimum ATR size (filters ultra-low volatility) ───────────
    g5_atr_size = bool(current['atr'] > current['close'] * 0.005)
    if not g5_atr_size:
        return _no_signal('atr_too_small')

    # ── Gate 6: 15m pullback to EMA20 ─────────────────────────────────────
    # Prev candle low must have touched within 0.2% of EMA20, and current
    # candle must close back above EMA20 — clean pullback, not a breakout.
    ema20        = current['ema_20']
    prev_touched = bool(prev['low'] <= ema20 * 1.002)
    curr_above   = bool(current['close'] > ema20)
    g6_pullback  = prev_touched and curr_above
    if not g6_pullback:
        return _no_signal('no_pullback')

    # ── Gate 7: RSI in momentum zone ──────────────────────────────────────
    g7_rsi = bool(45 <= current['rsi'] <= 70)
    if not g7_rsi:
        return _no_signal('rsi_out_of_range')

    # ── Gate 8: Strong close (near candle high) ────────────────────────────
    g8_close_near_high = bool(current['close'] >= current['high'] * 0.996)
    if not g8_close_near_high:
        return _no_signal('weak_close')

    # ── Gate 9: Meaningful body (not a doji) ─────────────────────────────
    body    = abs(current['close'] - current['open'])
    g9_body = bool(body > current['atr'] * 0.25)
    if not g9_body:
        return _no_signal('small_body')

    # ── All gates passed ───────────────────────────────────────────────────
    return {
        'final_signal':        True,
        'match_score':         9,
        'signal_combo_name':   '1h_close>ema20+macd1h+15m_ema9>ema20+macd+vol1.3x+atr_size+pullback+rsi45_70+close_hi+body',
        'logic_debug_note':    'all 9 gates passed',
        'g1_1h_trend':         True,
        'g2_15m_trend':        True,
        'g3_macd_pos':         True,
        'g4_volume_1.3x':      True,
        'g5_atr_size':         True,
        'g6_pullback':         True,
        'g7_rsi_zone':         True,
        'g8_close_near_high':  True,
        'g9_strong_body':      True,
        'rsi':                 round(float(current['rsi']), 1),
        'volume_ratio':        round(float(current.get('volume_ratio', 0)), 2),
    }


def _no_signal(reason: str) -> dict:
    return {
        'final_signal':      False,
        'match_score':       0,
        'signal_combo_name': 'none',
        'logic_debug_note':  reason,
    }
