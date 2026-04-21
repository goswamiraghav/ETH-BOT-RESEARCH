"""
signal_generator_v2.py
─────────────────────────────────────────────────────────────────────────────
Backtested signal for ETH/USDT on the 1h timeframe.

RESULTS (fixed SL, 9 months Apr 2025 – Jan 2026):
  Trades : 45  (~5/month)
  Win rate: 82.2%
  Net PnL : +5.61%  (tp=0.7x ATR, sl=1.2x ATR)
  PF      : 1.798
  Max DD  : -2.9%

SIGNAL LOGIC:
  Hard gates (ALL must pass or no trade):
    1. Trend  — EMA9 > EMA20 > EMA50  AND  close > EMA200
    2. MACD   — MACD line > Signal line (positive histogram)
    3. Volume — volume_ratio >= 2.0x 20-period average
    4. Close  — close >= candle high * 0.996  (strong momentum close)
    5. Body   — candle body > 0.5x ATR  (not a doji/indecision candle)

  All 5 gates must pass simultaneously. No scoring — all-or-nothing.

REQUIRES in DataFrame:
  Pre-computed columns (add via transformation_v2.add_all_indicators_v2):
    ema_9, ema_20, ema50, ema200, macd, macd_signal,
    atr, volume_ratio, vwap, rsi, bb_middle, bb_upper, bb_lower

NOTE:
  ema50 and ema200 must be pre-computed on the FULL dataset before calling
  this function. The backtester only passes a 22-row window, which is too
  short for a reliable EMA50/200.
"""

from __future__ import annotations


def generate_signal_v2(df) -> dict:
    """
    Evaluate the 1h long-only signal on the last row of df.

    Parameters
    ----------
    df : pd.DataFrame
        Sliding window (typically 22 rows from backtester, or 60+ rows live).
        Must contain pre-computed indicator columns including ema50 and ema200.

    Returns
    -------
    dict with keys:
        final_signal      : bool
        direction         : str   (always 'long' when final_signal=True)
        match_score       : int   (always 5 if signal fires, 0 otherwise)
        signal_combo_name : str
        logic_debug_note  : str
        + individual gate booleans
    """
    current = df.iloc[-1]

    required = [
        'atr', 'rsi', 'ema_9', 'ema_20', 'ema50', 'ema200',
        'macd', 'macd_signal', 'volume_ratio', 'vwap',
    ]
    if current[required].isna().any():
        return _no_signal('missing_indicators')

    # ── Gate 1: Trend alignment ────────────────────────────────────────────
    g1_trend = bool(
        current['ema_9']  > current['ema_20'] and
        current['ema_20'] > current['ema50']  and
        current['close']  > current['ema200']
    )
    if not g1_trend:
        return _no_signal('trend_fail')

    # ── Gate 2: MACD positive ──────────────────────────────────────────────
    g2_macd = bool(current['macd'] > current['macd_signal'])
    if not g2_macd:
        return _no_signal('macd_fail')

    # ── Gate 3: Volume spike >= 2x ─────────────────────────────────────────
    g3_volume = bool(current.get('volume_ratio', 0) >= 2.0)
    if not g3_volume:
        return _no_signal('volume_fail')

    # ── Gate 4: Strong close (near candle high) ────────────────────────────
    g4_close_near_high = bool(current['close'] >= current['high'] * 0.996)
    if not g4_close_near_high:
        return _no_signal('weak_close')

    # ── Gate 5: Meaningful body (not a doji) ──────────────────────────────
    body = abs(current['close'] - current['open'])
    g5_body = bool(body > current['atr'] * 0.5)
    if not g5_body:
        return _no_signal('small_body')

    # ── All gates passed ───────────────────────────────────────────────────
    return {
        'final_signal':       True,
        'direction':          'long',
        'match_score':        5,
        'signal_combo_name':  'long: trend+macd+vol2x+close_hi+body',
        'logic_debug_note':   'all 5 gates passed',
        'g1_trend':           True,
        'g2_macd_pos':        True,
        'g3_volume_2x':       True,
        'g4_close_near_high': True,
        'g5_strong_body':     True,
        'rsi':                round(float(current['rsi']), 1),
        'volume_ratio':       round(float(current.get('volume_ratio', 0)), 2),
        'ema_spread_pct':     round((current['ema_9'] - current['ema_20']) / current['close'] * 100, 3),
    }


def _no_signal(reason: str) -> dict:
    return {
        'final_signal':      False,
        'match_score':       0,
        'signal_combo_name': 'none',
        'logic_debug_note':  reason,
    }
