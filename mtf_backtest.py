"""
mtf_backtest.py
─────────────────────────────────────────────────────────────────────────────
Multi-timeframe confirmation experiment.

Three strategies:
  (a) 5m standalone   — baseline
  (b) 15m standalone  — baseline
  (c) MTF combined    — 5m entry only when the most recent 15m bar ALSO
                        fired final_signal=True

All three use tp_k=2.5, sl_k=1.0, score_threshold=6.

Metrics reported per strategy:
  total_trades, win_rate_pct, net_pnl_pct, max_drawdown_pct,
  profit_factor, sharpe_ratio, avg_duration_candles

Outputs:
  paper_outputs/mtf_trades_5m.csv
  paper_outputs/mtf_trades_15m.csv
  paper_outputs/mtf_trades_combined.csv
  paper_outputs/mtf_comparison.csv

Note: signal_generator_clean.py does not exist in this repo.
  The 5m and 15m signal generators are signal_generator_5m.py and
  signal_generator_15m.py respectively — those are used directly.
"""

import os
import math
import pandas as pd
import numpy as np

from transformation_v2    import add_all_indicators_v2
from signal_generator_5m  import generate_signal_5m
from signal_generator_15m import generate_signal_15m
from backtester_clean     import run_backtest_v2, summarize_backtest

os.makedirs('paper_outputs', exist_ok=True)

TP_K         = 2.5
SL_K         = 1.0
SCORE_THRESH = 6
MAX_DUR      = 10
FEE_BPS      = 10
WINDOW       = 21   # sliding window used inside run_backtest_v2


# ── Data helpers ──────────────────────────────────────────────────────────

def load_ohlcv(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df = df.sort_values('timestamp').reset_index(drop=True)
    if 'symbol' not in df.columns:
        df['symbol'] = 'ETH/USDT'
    return df


def add_1h_trend_from_15m(df_15m: pd.DataFrame) -> pd.DataFrame:
    """Resample 15m → 1h, compute EMAs/MACD, merge_asof back onto 15m."""
    df = df_15m.set_index('timestamp')
    df_1h = df[['open', 'high', 'low', 'close', 'volume']].resample('1h').agg(
        {'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum'}
    ).dropna().reset_index()
    df_1h = add_all_indicators_v2(df_1h)
    df_1h_slim = df_1h[['timestamp', 'ema_20', 'macd', 'macd_signal']].rename(columns={
        'ema_20': 'ema20_1h', 'macd': 'macd_1h', 'macd_signal': 'macd_signal_1h',
    })
    df_out = pd.merge_asof(
        df_15m.sort_values('timestamp'), df_1h_slim,
        on='timestamp', direction='backward',
    ).reset_index(drop=True)
    df_out['atr_ma_20'] = df_out['atr'].rolling(20).mean()
    return df_out


# ── Extended metrics ──────────────────────────────────────────────────────

def extended_metrics(trades_df: pd.DataFrame) -> dict:
    """Add Sharpe ratio and avg_duration_candles to summarize_backtest output."""
    base = summarize_backtest(trades_df)

    if trades_df.empty:
        base['sharpe_ratio']         = None
        base['avg_duration_candles'] = None
        return base

    pnl = trades_df['pnl_pct']

    # Sharpe: annualise using number of trades per year as the sampling frequency.
    # We treat each trade return as an observation.  With 0% risk-free rate,
    # Sharpe = mean / std * sqrt(N_annual).
    n = len(pnl)
    date_range_days = (
        pd.to_datetime(trades_df['timestamp'].max()) -
        pd.to_datetime(trades_df['timestamp'].min())
    ).days or 1
    trades_per_year = n / (date_range_days / 365.25)
    std = pnl.std(ddof=1)
    if std > 0:
        sharpe = round((pnl.mean() / std) * math.sqrt(trades_per_year), 3)
    else:
        sharpe = None

    base['sharpe_ratio']         = sharpe
    base['avg_duration_candles'] = round(trades_df['duration_candles'].mean(), 2)
    return base


# ── 15m signal pre-computation ────────────────────────────────────────────

def precompute_15m_signals(df_15m: pd.DataFrame) -> pd.Series:
    """
    Run the 15m signal generator over every bar (same sliding window as the
    backtester) and return a boolean Series indexed by the 15m timestamp.
    This is used to build the MTF confirmation gate on the 5m frame.
    """
    fired = {}
    for i in range(WINDOW, len(df_15m)):
        window = df_15m.iloc[i - WINDOW: i + 1].copy()
        sig = generate_signal_15m(window)
        fired[df_15m.iloc[i]['timestamp']] = bool(sig.get('final_signal', False))
    return pd.Series(fired, name='mtf_15m_fired')


def build_combined_df(df_5m: pd.DataFrame, sig_15m: pd.Series) -> pd.DataFrame:
    """
    Merge pre-computed 15m signal flags onto the 5m frame (merge_asof backward)
    so each 5m candle carries the most recent 15m bar's signal result.
    """
    sig_df = sig_15m.reset_index()
    sig_df.columns = ['timestamp', 'mtf_15m_fired']

    df_out = pd.merge_asof(
        df_5m.sort_values('timestamp'),
        sig_df.sort_values('timestamp'),
        on='timestamp',
        direction='backward',
    ).reset_index(drop=True)

    df_out['mtf_15m_fired'] = df_out['mtf_15m_fired'].fillna(False)
    return df_out


def make_combined_signal(df_combined: pd.DataFrame):
    """
    Return a signal function that:
      1. Evaluates generate_signal_5m on the given window.
      2. Looks up whether the most recent 15m bar fired (from the pre-merged
         mtf_15m_fired column on the last row of the window).
    If either gate fails the signal is suppressed.
    """
    def combined_signal(window_df):
        sig = generate_signal_5m(window_df)
        if not sig.get('final_signal', False):
            return sig
        # Check MTF gate from the pre-merged column on the current bar
        mtf_ok = bool(window_df.iloc[-1].get('mtf_15m_fired', False))
        if not mtf_ok:
            return {
                'final_signal':      False,
                'match_score':       0,
                'signal_combo_name': 'none',
                'logic_debug_note':  'mtf_15m_gate_fail',
            }
        sig['signal_combo_name'] = 'mtf_5m+15m_confirm'
        sig['logic_debug_note']  = '5m all gates + 15m confirmation'
        return sig

    return combined_signal


# ── Printing ──────────────────────────────────────────────────────────────

def print_results(label: str, trades: pd.DataFrame, metrics: dict) -> None:
    width = 54
    print('\n' + '═' * width)
    print(f'  {label}')
    print('═' * width)
    for k, v in metrics.items():
        print(f'  {k:<26s}: {v}')
    if trades.empty:
        print('  No trades fired.')
        return
    ts = pd.to_datetime(trades['timestamp'])
    print(f'\n  Date range : {ts.min().date()}  →  {ts.max().date()}')
    print('  Exit breakdown:')
    for reason, cnt in trades['exit_reason'].value_counts().items():
        print(f'    {reason:<14s}: {cnt}')


# ── Load & prepare data ───────────────────────────────────────────────────

print('Loading and preparing 5m data...')
df5 = load_ohlcv('ethusdt_5m_1y.csv')
df5 = add_all_indicators_v2(df5)
df5 = df5.drop(columns=['ema50', 'ema200'], errors='ignore')

print('Loading and preparing 15m data (+ 1h trend columns)...')
df15 = load_ohlcv('ethusdt_15m_1y.csv')
df15 = add_all_indicators_v2(df15)
df15 = df15.drop(columns=['ema200'], errors='ignore')
df15 = add_1h_trend_from_15m(df15)

print('Pre-computing 15m signals for MTF gate...')
sig_15m = precompute_15m_signals(df15)
print(f'  15m bars evaluated: {len(sig_15m)}  |  signals fired: {sig_15m.sum()}')

df5_combined = build_combined_df(df5, sig_15m)
print(f'  5m bars with active 15m gate: {df5_combined["mtf_15m_fired"].sum()}\n')


# ── (a) 5m standalone ─────────────────────────────────────────────────────

print('Running (a) 5m standalone backtest...')
trades_5m = run_backtest_v2(
    df5, signal_function=generate_signal_5m,
    score_threshold=SCORE_THRESH, tp_k=TP_K, sl_k=SL_K,
    max_duration=MAX_DUR, fee_bps=FEE_BPS,
)
metrics_5m = extended_metrics(trades_5m)
print_results('5m STANDALONE (baseline)', trades_5m, metrics_5m)
trades_5m.to_csv('paper_outputs/mtf_trades_5m.csv', index=False)


# ── (b) 15m standalone ────────────────────────────────────────────────────

print('\nRunning (b) 15m standalone backtest...')
trades_15m = run_backtest_v2(
    df15, signal_function=generate_signal_15m,
    score_threshold=SCORE_THRESH, tp_k=TP_K, sl_k=SL_K,
    max_duration=MAX_DUR, fee_bps=FEE_BPS,
)
metrics_15m = extended_metrics(trades_15m)
print_results('15m STANDALONE (baseline)', trades_15m, metrics_15m)
trades_15m.to_csv('paper_outputs/mtf_trades_15m.csv', index=False)


# ── (c) MTF combined ──────────────────────────────────────────────────────

print('\nRunning (c) MTF combined backtest (5m entry + 15m confirmation)...')
combined_signal_fn = make_combined_signal(df5_combined)
trades_mtf = run_backtest_v2(
    df5_combined, signal_function=combined_signal_fn,
    score_threshold=SCORE_THRESH, tp_k=TP_K, sl_k=SL_K,
    max_duration=MAX_DUR, fee_bps=FEE_BPS,
)
metrics_mtf = extended_metrics(trades_mtf)
print_results('MTF COMBINED (5m + 15m confirmation)', trades_mtf, metrics_mtf)
trades_mtf.to_csv('paper_outputs/mtf_trades_combined.csv', index=False)


# ── Comparison table ──────────────────────────────────────────────────────

rows = [
    {'strategy': '5m Standalone',       **metrics_5m},
    {'strategy': '15m Standalone',      **metrics_15m},
    {'strategy': 'MTF 5m+15m Combined', **metrics_mtf},
]
comparison_df = pd.DataFrame(rows).set_index('strategy')
comparison_df.to_csv('paper_outputs/mtf_comparison.csv')

print('\n' + '═' * 70)
print('  MTF COMPARISON SUMMARY')
print('═' * 70)
print(comparison_df.to_string())

print('\nSaved:')
print('  paper_outputs/mtf_trades_5m.csv')
print('  paper_outputs/mtf_trades_15m.csv')
print('  paper_outputs/mtf_trades_combined.csv')
print('  paper_outputs/mtf_comparison.csv')
