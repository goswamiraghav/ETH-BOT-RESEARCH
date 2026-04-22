"""
run_optimization.py
─────────────────────────────────────────────────────────────────────────────
Parameter sweep experiments for the ETH/USDT trading signal pipeline.

Outputs to paper_outputs/:
    exp1_5m_tp_sl.csv       — 5m TP/SL ratio sweep
    exp2_5m_score.csv       — 5m score threshold sweep
    exp3_15m_tp_sl.csv      — 15m TP/SL ratio sweep
    exp4_15m_score.csv      — 15m score threshold sweep
"""

import os
import pandas as pd

from transformation_v2    import add_all_indicators_v2
from signal_generator_5m  import generate_signal_5m
from signal_generator_15m import generate_signal_15m
from backtester_clean     import run_backtest_v2, summarize_backtest

os.makedirs('paper_outputs', exist_ok=True)

FEE_BPS = 10


# ── Data prep (done once) ─────────────────────────────────────────────────

def load_ohlcv(csv_path):
    df = pd.read_csv(csv_path)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df = df.sort_values('timestamp').reset_index(drop=True)
    if 'symbol' not in df.columns:
        df['symbol'] = 'ETH/USDT'
    return df


def add_1h_trend_from_15m(df_15m):
    df = df_15m.set_index('timestamp')
    df_1h = df[['open', 'high', 'low', 'close', 'volume']].resample('1h').agg({
        'open': 'first', 'high': 'max', 'low': 'min',
        'close': 'last', 'volume': 'sum',
    }).dropna().reset_index()
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


def run_single(df, signal_fn, tp_k, sl_k, score_threshold, max_duration=10):
    trades = run_backtest_v2(
        df, signal_function=signal_fn,
        score_threshold=score_threshold,
        tp_k=tp_k, sl_k=sl_k,
        max_duration=max_duration,
        fee_bps=FEE_BPS,
    )
    s = summarize_backtest(trades)
    return {
        'tp_k': tp_k, 'sl_k': sl_k,
        'score_threshold': score_threshold,
        'total_trades': s['total_trades'],
        'win_rate_pct': s['win_rate_pct'],
        'net_pnl_pct': s['net_pnl_pct'],
        'avg_trade_pct': s['avg_trade_pct'],
        'max_drawdown_pct': s['max_drawdown_pct'],
        'profit_factor': s['profit_factor'],
    }


print('Preparing data...')
df5_raw = load_ohlcv('ethusdt_5m_1y.csv')
df5_raw = add_all_indicators_v2(df5_raw)
df5_raw = df5_raw.drop(columns=['ema50', 'ema200'], errors='ignore')

df15_raw = load_ohlcv('ethusdt_15m_1y.csv')
df15_raw = add_all_indicators_v2(df15_raw)
df15_raw = df15_raw.drop(columns=['ema200'], errors='ignore')
df15_raw = add_1h_trend_from_15m(df15_raw)
print('Data ready.\n')


# ── Experiment 1 — 5m TP/SL sweep (score fixed at 6) ─────────────────────

print('=' * 60)
print('Experiment 1 — 5m TP/SL ratio sweep (score_threshold=6)')
print('=' * 60)

EXP1_CONFIGS = [
    (1.5, 0.8),
    (2.0, 0.8),
    (2.5, 1.0),
    (3.0, 1.0),
]

exp1_rows = []
for tp_k, sl_k in EXP1_CONFIGS:
    row = run_single(df5_raw, generate_signal_5m, tp_k=tp_k, sl_k=sl_k, score_threshold=6)
    exp1_rows.append(row)
    pf = row['profit_factor']
    print(f"  tp={tp_k:.1f}x sl={sl_k:.1f}x | trades={row['total_trades']:4d} | "
          f"win={row['win_rate_pct']:5.1f}% | net_pnl={row['net_pnl_pct']:8.2f}% | "
          f"pf={pf if pf else 'N/A'}")

exp1_df = pd.DataFrame(exp1_rows)
exp1_df.to_csv('paper_outputs/exp1_5m_tp_sl.csv', index=False)
print('  → saved paper_outputs/exp1_5m_tp_sl.csv\n')


# ── Experiment 2 — 5m score threshold sweep (tp/sl fixed at 2.0/0.8) ─────

print('=' * 60)
print('Experiment 2 — 5m score threshold sweep (tp=2.0x, sl=0.8x)')
print('=' * 60)

exp2_rows = []
for score in [5, 6, 7]:
    row = run_single(df5_raw, generate_signal_5m, tp_k=2.0, sl_k=0.8, score_threshold=score)
    exp2_rows.append(row)
    pf = row['profit_factor']
    print(f"  score>={score} | trades={row['total_trades']:4d} | "
          f"win={row['win_rate_pct']:5.1f}% | net_pnl={row['net_pnl_pct']:8.2f}% | "
          f"pf={pf if pf else 'N/A'}")

exp2_df = pd.DataFrame(exp2_rows)
exp2_df.to_csv('paper_outputs/exp2_5m_score.csv', index=False)
print('  → saved paper_outputs/exp2_5m_score.csv\n')


# ── Experiment 3 — 15m TP/SL sweep (score fixed at 9) ────────────────────

print('=' * 60)
print('Experiment 3 — 15m TP/SL ratio sweep (score_threshold=9)')
print('=' * 60)

EXP3_CONFIGS = [
    (2.0, 1.0),
    (2.5, 1.0),
    (3.0, 1.2),
]

exp3_rows = []
for tp_k, sl_k in EXP3_CONFIGS:
    row = run_single(df15_raw, generate_signal_15m, tp_k=tp_k, sl_k=sl_k, score_threshold=9)
    exp3_rows.append(row)
    pf = row['profit_factor']
    print(f"  tp={tp_k:.1f}x sl={sl_k:.1f}x | trades={row['total_trades']:4d} | "
          f"win={row['win_rate_pct']:5.1f}% | net_pnl={row['net_pnl_pct']:8.2f}% | "
          f"pf={pf if pf else 'N/A'}")

exp3_df = pd.DataFrame(exp3_rows)
exp3_df.to_csv('paper_outputs/exp3_15m_tp_sl.csv', index=False)
print('  → saved paper_outputs/exp3_15m_tp_sl.csv\n')


# ── Experiment 4 — 15m score threshold sweep (tp=2.5x, sl=1.0x) ──────────

print('=' * 60)
print('Experiment 4 — 15m score threshold sweep (tp=2.5x, sl=1.0x)')
print('=' * 60)

exp4_rows = []
for score in [5, 6]:
    row = run_single(df15_raw, generate_signal_15m, tp_k=2.5, sl_k=1.0, score_threshold=score)
    exp4_rows.append(row)
    pf = row['profit_factor']
    print(f"  score>={score} | trades={row['total_trades']:4d} | "
          f"win={row['win_rate_pct']:5.1f}% | net_pnl={row['net_pnl_pct']:8.2f}% | "
          f"pf={pf if pf else 'N/A'}")

exp4_df = pd.DataFrame(exp4_rows)
exp4_df.to_csv('paper_outputs/exp4_15m_score.csv', index=False)
print('  → saved paper_outputs/exp4_15m_score.csv\n')


# ── Best combos ───────────────────────────────────────────────────────────

def best_by_pf(df, label):
    valid = df[df['profit_factor'].notna()]
    if valid.empty:
        print(f'  {label}: no profitable configuration found.')
        return
    best = valid.loc[valid['profit_factor'].idxmax()]
    print(f'\n  Best {label}:')
    for k, v in best.items():
        print(f'    {k:<22s}: {v}')

print('=' * 60)
print('BEST PARAMETER COMBINATIONS BY PROFIT FACTOR')
print('=' * 60)

all_5m = pd.concat([exp1_df, exp2_df], ignore_index=True)
all_15m = pd.concat([exp3_df, exp4_df], ignore_index=True)

best_by_pf(all_5m,  '5m  (across Exp 1 + 2)')
best_by_pf(all_15m, '15m (across Exp 3 + 4)')
