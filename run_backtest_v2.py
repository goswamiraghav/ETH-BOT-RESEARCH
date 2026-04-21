"""
run_backtest_v2.py
─────────────────────────────────────────────────────────────────────────────
Run backtests for all three timeframe strategies independently.

Usage:
    python run_backtest_v2.py

Outputs to paper_outputs/:
    trades_5m_v2.csv   — 5m scalp trades
    trades_15m_v2.csv  — 15m swing trades  (requires ethusdt_1h_1y.csv for
                         1h trend gate — run fetch_data_1h.py first)
    trades_1h_v2.csv   — 1h position trades
    summary_all_v2.csv — combined headline stats across all three
"""

import os
import pandas as pd

from transformation_v2    import add_all_indicators_v2
from signal_generator_5m  import generate_signal_5m
from signal_generator_15m import generate_signal_15m
from signal_generator_v2  import generate_signal_v2
from backtester_clean     import run_backtest_v2, summarize_backtest

FEE_BPS = 10   # 10 bps per side (KuCoin taker)

CONFIGS = [
    {
        'label':        '5m  (Scalp)',
        'csv':          'ethusdt_5m_1y.csv',
        'signal_fn':    generate_signal_5m,
        'tp_k':         0.6,
        'sl_k':         0.8,
        'max_duration': 10,
        'score_thresh': 6,
        'output':       'paper_outputs/trades_5m_v2.csv',
        'drop_cols':    ['ema50', 'ema200'],  # 5m signal uses only ema_9/ema_20
        'extra_prep':   None,
    },
    {
        'label':        '15m (Swing)',
        'csv':          'ethusdt_15m_1y.csv',
        'signal_fn':    generate_signal_15m,
        'tp_k':         1.0,
        'sl_k':         0.8,
        'max_duration': 10,
        'score_thresh': 10,
        'output':       'paper_outputs/trades_15m_v2.csv',
        'drop_cols':    ['ema200'],           # 15m signal uses ema50 but not standalone ema200
        'extra_prep':   'add_1h_trend',       # sentinel — handled inline below
    },
    {
        'label':        '1h  (Position)',
        'csv':          'ethusdt_1h_1y.csv',
        'signal_fn':    generate_signal_v2,
        'tp_k':         0.7,
        'sl_k':         1.2,
        'max_duration': 10,
        'score_thresh': 5,
        'output':       'paper_outputs/trades_1h_v2.csv',
        'drop_cols':    [],                   # 1h signal uses ema50 + ema200
        'extra_prep':   None,
    },
]


def load_and_prepare(csv_path: str, drop_cols: list) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df = df.sort_values('timestamp').reset_index(drop=True)
    if 'symbol' not in df.columns:
        df['symbol'] = 'ETH/USDT'
    df = add_all_indicators_v2(df)
    if drop_cols:
        df = df.drop(columns=drop_cols, errors='ignore')
    return df


def add_1h_trend_columns(df_15m: pd.DataFrame) -> pd.DataFrame:
    """
    Join 1h EMA9/20/50/200 values onto the 15m dataframe using merge_asof
    (each 15m candle gets the most recent closed 1h candle's EMA values).
    Also computes atr_ma_20 required by the ATR-expanding gate.
    """
    print("  Loading 1h data for trend gate...")
    df_1h = pd.read_csv('ethusdt_1h_1y.csv')
    df_1h['timestamp'] = pd.to_datetime(df_1h['timestamp'])
    df_1h = df_1h.sort_values('timestamp').reset_index(drop=True)
    df_1h = add_all_indicators_v2(df_1h)

    # Keep only what we need and rename to avoid column collisions
    df_1h = df_1h[['timestamp', 'ema_9', 'ema_20', 'ema50', 'ema200', 'macd', 'macd_signal']].rename(columns={
        'ema_9':        'ema9_1h',
        'ema_20':       'ema20_1h',
        'ema50':        'ema50_1h',
        'ema200':       'ema200_1h',
        'macd':         'macd_1h',
        'macd_signal':  'macd_signal_1h',
    })

    # For each 15m candle, attach the most recent 1h candle's EMAs
    df_15m = pd.merge_asof(
        df_15m.sort_values('timestamp'),
        df_1h,
        on='timestamp',
        direction='backward',
    ).reset_index(drop=True)

    # ATR expanding gate: current ATR vs rolling 20-period ATR mean
    df_15m['atr_ma_20'] = df_15m['atr'].rolling(20).mean()

    return df_15m


def print_results(label: str, trades: pd.DataFrame, summary: dict) -> None:
    width = 48
    print("\n" + "═" * width)
    print(f"  BACKTEST RESULTS — ETH/USDT {label}")
    print("═" * width)

    for k, v in summary.items():
        print(f"  {k:<22s}: {v}")

    if trades.empty:
        print("  No trades fired.")
        return

    print(f"\n  Date range : {trades['timestamp'].min().date()}  →  {trades['timestamp'].max().date()}")

    print(f"\n  Exit breakdown:")
    for reason, count in trades['exit_reason'].value_counts().items():
        print(f"    {reason:<12s}: {count}")

    print(f"\n  Monthly PnL:")
    trades = trades.copy()
    trades['month'] = pd.to_datetime(trades['timestamp']).dt.to_period('M')
    monthly = trades.groupby('month').agg(
        trades_n=('pnl_pct', 'count'),
        win_rate=('was_profitable', 'mean'),
        net_pnl =('pnl_pct', 'sum'),
    ).round(3)
    print(monthly.to_string())


# ── Run all three ─────────────────────────────────────────────────────────
os.makedirs('paper_outputs', exist_ok=True)

all_summaries = []

for cfg in CONFIGS:
    label = cfg['label']
    print(f"\nLoading {cfg['csv']}...")

    try:
        df = load_and_prepare(cfg['csv'], cfg['drop_cols'])
    except FileNotFoundError:
        print(f"  [SKIP] {cfg['csv']} not found — run the fetch script first.")
        continue

    if cfg['extra_prep'] == 'add_1h_trend':
        try:
            df = add_1h_trend_columns(df)
        except FileNotFoundError:
            print("  [SKIP] ethusdt_1h_1y.csv not found — needed for 1h trend gate.")
            continue

    print(f"Running {label} backtest  "
          f"(tp={cfg['tp_k']}x ATR, sl={cfg['sl_k']}x ATR, "
          f"max_dur={cfg['max_duration']}, score>={cfg['score_thresh']})...")

    trades = run_backtest_v2(
        df,
        signal_function=cfg['signal_fn'],
        score_threshold=cfg['score_thresh'],
        tp_k=cfg['tp_k'],
        sl_k=cfg['sl_k'],
        max_duration=cfg['max_duration'],
        fee_bps=FEE_BPS,
    )

    summary = summarize_backtest(trades)
    print_results(label, trades, summary)

    trades.to_csv(cfg['output'], index=False)
    all_summaries.append({'timeframe': label, **summary})

# ── Combined summary table ────────────────────────────────────────────────
if all_summaries:
    print("\n" + "═" * 70)
    print("  COMBINED SUMMARY")
    print("═" * 70)
    summary_df = pd.DataFrame(all_summaries).set_index('timeframe')
    print(summary_df.to_string())
    summary_df.to_csv('paper_outputs/summary_all_v2.csv')

print("\nSaved:")
for cfg in CONFIGS:
    print(f"  {cfg['output']}")
print("  paper_outputs/summary_all_v2.csv")
