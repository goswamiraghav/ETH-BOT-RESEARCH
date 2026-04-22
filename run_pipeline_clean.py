"""
run_pipeline_clean.py
─────────────────────────────────────────────────────────────────────────────
Research-paper pipeline: runs 5m and 15m backtests and writes results to
paper_outputs/.

Fixes applied vs earlier drafts:
  - Imports run_backtest_v2 (not run_backtest) from backtester_clean
  - Imports summarize_backtest from backtester_clean
  - 1h trend columns for the 15m signal are derived by resampling the
    15m OHLCV data to 1h — no separate 1h CSV required

Usage:
    python run_pipeline_clean.py

Outputs:
    paper_outputs/trades_5m.csv
    paper_outputs/trades_15m.csv
    paper_outputs/summary.csv
"""

import os
import pandas as pd

from transformation_v2    import add_all_indicators_v2
from signal_generator_5m  import generate_signal_5m
from signal_generator_15m import generate_signal_15m
from backtester_clean     import run_backtest_v2, summarize_backtest

os.makedirs('paper_outputs', exist_ok=True)

FEE_BPS = 10  # 10 bps per side (KuCoin taker)


# ── Helpers ───────────────────────────────────────────────────────────────

def load_ohlcv(csv_path: str) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df = df.sort_values('timestamp').reset_index(drop=True)
    if 'symbol' not in df.columns:
        df['symbol'] = 'ETH/USDT'
    return df


def add_1h_trend_from_15m(df_15m: pd.DataFrame) -> pd.DataFrame:
    """
    Resample 15m OHLCV to 1h, compute EMA/MACD on the 1h bars, then
    merge_asof back onto the 15m frame.  This avoids needing a separate
    1h CSV while keeping look-ahead-free alignment.
    """
    df = df_15m.set_index('timestamp')

    df_1h = df[['open', 'high', 'low', 'close', 'volume']].resample('1h').agg({
        'open':   'first',
        'high':   'max',
        'low':    'min',
        'close':  'last',
        'volume': 'sum',
    }).dropna().reset_index()

    df_1h = add_all_indicators_v2(df_1h)

    df_1h_slim = df_1h[['timestamp', 'ema_20', 'macd', 'macd_signal']].rename(columns={
        'ema_20':      'ema20_1h',
        'macd':        'macd_1h',
        'macd_signal': 'macd_signal_1h',
    })

    df_out = pd.merge_asof(
        df_15m.sort_values('timestamp'),
        df_1h_slim,
        on='timestamp',
        direction='backward',
    ).reset_index(drop=True)

    df_out['atr_ma_20'] = df_out['atr'].rolling(20).mean()
    return df_out


def print_results(label: str, trades: pd.DataFrame, summary: dict) -> None:
    width = 52
    print('\n' + '═' * width)
    print(f'  BACKTEST RESULTS — ETH/USDT {label}')
    print('═' * width)
    for k, v in summary.items():
        print(f'  {k:<24s}: {v}')

    if trades.empty:
        print('  No trades fired.')
        return

    print(f'\n  Date range : {trades["timestamp"].min().date()}  →  {trades["timestamp"].max().date()}')

    print('\n  Exit breakdown:')
    for reason, count in trades['exit_reason'].value_counts().items():
        print(f'    {reason:<14s}: {count}')

    print('\n  Monthly PnL:')
    trades = trades.copy()
    trades['month'] = pd.to_datetime(trades['timestamp']).dt.to_period('M')
    monthly = trades.groupby('month').agg(
        trades_n=('pnl_pct', 'count'),
        win_rate=('was_profitable', 'mean'),
        net_pnl =('pnl_pct', 'sum'),
    ).round(3)
    print(monthly.to_string())


# ── 5m Scalp ──────────────────────────────────────────────────────────────

print('\nLoading ethusdt_5m_1y.csv...')
df5 = load_ohlcv('ethusdt_5m_1y.csv')
df5 = add_all_indicators_v2(df5)
df5 = df5.drop(columns=['ema50', 'ema200'], errors='ignore')

print('Running 5m scalp backtest  (tp=0.6x ATR, sl=0.8x ATR, max_dur=10, score>=6)...')
trades_5m = run_backtest_v2(
    df5,
    signal_function=generate_signal_5m,
    score_threshold=6,
    tp_k=0.6,
    sl_k=0.8,
    max_duration=10,
    fee_bps=FEE_BPS,
)
summary_5m = summarize_backtest(trades_5m)
print_results('5m (Scalp)', trades_5m, summary_5m)
trades_5m.to_csv('paper_outputs/trades_5m.csv', index=False)


# ── 15m Swing ─────────────────────────────────────────────────────────────

print('\nLoading ethusdt_15m_1y.csv...')
df15 = load_ohlcv('ethusdt_15m_1y.csv')
df15 = add_all_indicators_v2(df15)
df15 = df15.drop(columns=['ema200'], errors='ignore')

print('  Deriving 1h trend columns from 15m data (resample)...')
df15 = add_1h_trend_from_15m(df15)

print('Running 15m swing backtest  (tp=1.0x ATR, sl=0.8x ATR, max_dur=10, score>=9)...')
trades_15m = run_backtest_v2(
    df15,
    signal_function=generate_signal_15m,
    score_threshold=9,
    tp_k=1.0,
    sl_k=0.8,
    max_duration=10,
    fee_bps=FEE_BPS,
)
summary_15m = summarize_backtest(trades_15m)
print_results('15m (Swing)', trades_15m, summary_15m)
trades_15m.to_csv('paper_outputs/trades_15m.csv', index=False)


# ── Combined summary ──────────────────────────────────────────────────────

all_summaries = [
    {'timeframe': '5m  (Scalp)', **summary_5m},
    {'timeframe': '15m (Swing)', **summary_15m},
]
summary_df = pd.DataFrame(all_summaries).set_index('timeframe')

print('\n' + '═' * 70)
print('  COMBINED SUMMARY')
print('═' * 70)
print(summary_df.to_string())
summary_df.to_csv('paper_outputs/summary.csv')

print('\nSaved:')
print('  paper_outputs/trades_5m.csv')
print('  paper_outputs/trades_15m.csv')
print('  paper_outputs/summary.csv')
