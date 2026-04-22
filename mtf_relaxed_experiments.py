"""
mtf_relaxed_experiments.py
─────────────────────────────────────────────────────────────────────────────
Relaxed MTF gate experiments to reach 30-100 trades for statistical validity.

Background: the strict 15m gate (all 9 gates must pass) produced only 9 MTF
confirmed trades over a year.  These experiments progressively relax that
requirement to find the best balance of trade frequency vs signal quality.

Experiment A — Partial 15m score gate (lookback=1 bar):
  MTF gate fires when the most recent 15m bar passes >= N of the 9 gates.
  Thresholds: N = 3, 4, 5.
  The 15m signal is all-or-nothing (score=9 or 0) so this requires a
  partial scorer that checks each gate independently.

Experiment B — Lookback window (last 3 × 15m bars):
  MTF gate fires if ANY of the 3 most recent 15m bars had partial score >= N.
  Thresholds: N = 4, 5.

Experiment C — Minimal 2-gate confirmation:
  MTF gate fires when the 15m bar satisfies BOTH:
    (1) ema_9 > ema_20   (15m trend aligned)
    (2) close > vwap     (price above session VWAP)
  No score requirement, no final_signal requirement.

All 5m entries use:  tp_k=2.5, sl_k=1.0, score_threshold=6, fee=10bps

Outputs:
  paper_outputs/mtf_exp_a_partial_score.csv
  paper_outputs/mtf_exp_b_lookback.csv
  paper_outputs/mtf_exp_c_simple_gate.csv
  paper_outputs/mtf_relaxed_experiments.csv   ← full comparison
"""

import os
import math
import pandas as pd

from transformation_v2    import add_all_indicators_v2
from signal_generator_5m  import generate_signal_5m
from backtester_clean     import run_backtest_v2, summarize_backtest

os.makedirs('paper_outputs', exist_ok=True)

TP_K         = 2.5
SL_K         = 1.0
SCORE_THRESH = 6
MAX_DUR      = 10
FEE_BPS      = 10
WINDOW       = 21


# ── Data helpers ──────────────────────────────────────────────────────────

def load_ohlcv(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df = df.sort_values('timestamp').reset_index(drop=True)
    if 'symbol' not in df.columns:
        df['symbol'] = 'ETH/USDT'
    return df


def add_1h_trend_from_15m(df_15m: pd.DataFrame) -> pd.DataFrame:
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


# ── Partial 15m gate scorer ───────────────────────────────────────────────

def partial_score_15m(window_df: pd.DataFrame) -> int:
    """
    Independently evaluate each of the 9 15m gates and return a count (0-9).
    Mirrors gate logic in signal_generator_15m.py exactly, but counts instead
    of failing early.  Used for Experiments A and B.
    """
    if len(window_df) < 2:
        return 0
    current = window_df.iloc[-1]
    prev    = window_df.iloc[-2]

    def safe(col):
        v = current.get(col, None)
        return v if (v is not None and pd.notna(v)) else None

    score = 0

    # G1 — 1h trend: close > ema20_1h AND macd_1h > macd_signal_1h
    c1 = safe('close'); e1h = safe('ema20_1h')
    m1h = safe('macd_1h'); ms1h = safe('macd_signal_1h')
    if all(v is not None for v in [c1, e1h, m1h, ms1h]):
        if c1 > e1h and m1h > ms1h:
            score += 1

    # G2 — 15m EMA trend: ema_9 > ema_20
    e9 = safe('ema_9'); e20 = safe('ema_20')
    if e9 is not None and e20 is not None:
        if e9 > e20:
            score += 1

    # G3 — MACD positive: macd > macd_signal
    mac = safe('macd'); macs = safe('macd_signal')
    if mac is not None and macs is not None:
        if mac > macs:
            score += 1

    # G4 — Volume spike >= 1.3x
    vr = safe('volume_ratio')
    if vr is not None and vr >= 1.3:
        score += 1

    # G5 — ATR size > 0.5% of price
    atr = safe('atr'); cl = safe('close')
    if atr is not None and cl is not None and cl > 0:
        if atr > cl * 0.005:
            score += 1

    # G6 — Pullback: prev low ≤ ema20 * 1.002  AND  current close > ema20
    if e20 is not None and pd.notna(prev.get('low', None)):
        if prev['low'] <= e20 * 1.002 and current['close'] > e20:
            score += 1

    # G7 — RSI between 45 and 70
    rsi = safe('rsi')
    if rsi is not None and 45 <= rsi <= 70:
        score += 1

    # G8 — Strong close: close >= high * 0.996
    hi = safe('high')
    if hi is not None and cl is not None and hi > 0:
        if cl >= hi * 0.996:
            score += 1

    # G9 — Meaningful body > 0.25 * ATR
    body = abs(current['close'] - current['open'])
    if atr is not None and atr > 0:
        if body > atr * 0.25:
            score += 1

    return score


def precompute_15m_partial_scores(df_15m: pd.DataFrame) -> pd.Series:
    """Score every 15m bar (0-9).  Returns a Series indexed by timestamp."""
    scores = {}
    for i in range(WINDOW, len(df_15m)):
        window = df_15m.iloc[i - WINDOW: i + 1]
        scores[df_15m.iloc[i]['timestamp']] = partial_score_15m(window)
    return pd.Series(scores, name='mtf_15m_score', dtype=int)


def precompute_15m_simple_gate(df_15m: pd.DataFrame) -> pd.Series:
    """
    Minimal 2-gate flag: ema_9 > ema_20  AND  close > vwap.
    Returns a boolean Series indexed by timestamp.
    """
    fired = {}
    for i in range(WINDOW, len(df_15m)):
        row = df_15m.iloc[i]
        e9  = row.get('ema_9', None)
        e20 = row.get('ema_20', None)
        vwap = row.get('vwap', None)
        cl   = row.get('close', None)
        if any(v is None or pd.isna(v) for v in [e9, e20, vwap, cl]):
            fired[row['timestamp']] = False
        else:
            fired[row['timestamp']] = bool(e9 > e20 and cl > vwap)
    return pd.Series(fired, name='mtf_simple_fired')


# ── Build 5m+15m merged frames ────────────────────────────────────────────

def merge_scores_onto_5m(df_5m: pd.DataFrame, scores_15m: pd.Series,
                         col_name: str = 'mtf_15m_score') -> pd.DataFrame:
    score_df = scores_15m.rename(col_name).reset_index()
    score_df.columns = ['timestamp', col_name]
    df_out = pd.merge_asof(
        df_5m.sort_values('timestamp'),
        score_df.sort_values('timestamp'),
        on='timestamp', direction='backward',
    ).reset_index(drop=True)
    df_out[col_name] = df_out[col_name].fillna(0)
    return df_out


def apply_lookback_window(scores_15m: pd.Series, lookback_n: int,
                          min_score: int) -> pd.Series:
    """
    Rolling OR: for each 15m bar, True if ANY of the last lookback_n bars
    had partial score >= min_score.
    """
    fired = (scores_15m >= min_score).astype(int)
    any_recent = fired.rolling(lookback_n, min_periods=1).max().astype(bool)
    any_recent.index = scores_15m.index
    return any_recent


# ── Signal function factory ───────────────────────────────────────────────

def make_gate_fn(df_5m_with_col: pd.DataFrame, col: str, threshold):
    """
    Returns a signal function that passes the 5m signal through only when
    df_5m_with_col[col] >= threshold on the bar being evaluated.
    Works for both numeric scores and boolean flags.
    """
    def gated_signal(window_df):
        sig = generate_signal_5m(window_df)
        if not sig.get('final_signal', False):
            return sig
        gate_val = window_df.iloc[-1].get(col, 0)
        if gate_val < threshold if isinstance(threshold, (int, float)) else not gate_val:
            return {
                'final_signal':      False,
                'match_score':       0,
                'signal_combo_name': 'none',
                'logic_debug_note':  f'mtf_gate_fail ({col}={gate_val} < {threshold})',
            }
        sig['signal_combo_name'] = f'mtf_5m+15m_{col}>={threshold}'
        return sig
    return gated_signal


# ── Extended metrics (identical to mtf_backtest.py) ──────────────────────

def extended_metrics(trades_df: pd.DataFrame) -> dict:
    base = summarize_backtest(trades_df)
    if trades_df.empty:
        base['sharpe_ratio']         = None
        base['avg_duration_candles'] = None
        return base
    pnl = trades_df['pnl_pct']
    n = len(pnl)
    days = max((pd.to_datetime(trades_df['timestamp'].max()) -
                pd.to_datetime(trades_df['timestamp'].min())).days, 1)
    ann_factor = math.sqrt(n / (days / 365.25))
    std = pnl.std(ddof=1)
    sharpe = round((pnl.mean() / std) * ann_factor, 3) if std > 0 else None
    base['sharpe_ratio']         = sharpe
    base['avg_duration_candles'] = round(trades_df['duration_candles'].mean(), 2)
    return base


def run_config(df_5m, col, threshold, label):
    sig_fn = make_gate_fn(df_5m, col, threshold)
    trades = run_backtest_v2(
        df_5m, signal_function=sig_fn,
        score_threshold=SCORE_THRESH, tp_k=TP_K, sl_k=SL_K,
        max_duration=MAX_DUR, fee_bps=FEE_BPS,
    )
    m = extended_metrics(trades)
    pf = m['profit_factor']
    print(f"  {label:<42s} | trades={m['total_trades']:4d} | "
          f"win={m['win_rate_pct']:5.1f}% | "
          f"net_pnl={m['net_pnl_pct']:7.2f}% | "
          f"dd={m['max_drawdown_pct']:6.2f}% | "
          f"pf={str(pf) if pf is not None else 'N/A':>6s} | "
          f"sharpe={str(m['sharpe_ratio']) if m['sharpe_ratio'] is not None else 'N/A':>7s}")
    return {'label': label, **m}, trades


# ── Load & prepare data (once) ────────────────────────────────────────────

print('Loading and preparing 5m data...')
df5 = load_ohlcv('ethusdt_5m_1y.csv')
df5 = add_all_indicators_v2(df5)
df5 = df5.drop(columns=['ema50', 'ema200'], errors='ignore')

print('Loading and preparing 15m data (+ 1h trend columns)...')
df15 = load_ohlcv('ethusdt_15m_1y.csv')
df15 = add_all_indicators_v2(df15)
df15 = df15.drop(columns=['ema200'], errors='ignore')
df15 = add_1h_trend_from_15m(df15)

print('Pre-computing 15m partial scores (0-9) for all bars...')
scores_15m = precompute_15m_partial_scores(df15)
score_counts = scores_15m.value_counts().sort_index()
print('  Score distribution:')
for score_val, cnt in score_counts.items():
    bar = '█' * min(cnt // 50, 40)
    print(f'    {score_val}/9  {cnt:5d}  {bar}')

print('\nPre-computing 15m simple gate (ema_trend + vwap_above)...')
simple_gate_15m = precompute_15m_simple_gate(df15)
print(f'  Simple gate fires on {simple_gate_15m.sum()} of {len(simple_gate_15m)} 15m bars '
      f'({simple_gate_15m.mean()*100:.1f}%)\n')

all_results = []

# ── Experiment A — Partial score gate (lookback = 1) ─────────────────────

print('=' * 80)
print('Experiment A — Partial 15m score gate, lookback=1 bar')
print('  (most recent 15m bar must pass >= N of 9 gates)')
print('=' * 80)

df5_scored = merge_scores_onto_5m(df5, scores_15m, 'mtf_15m_score')
exp_a_rows = []

for thresh in [3, 4, 5]:
    active_bars = (df5_scored['mtf_15m_score'] >= thresh).sum()
    label = f'Exp A  score>={thresh}/9  (5m bars w/ active gate: {active_bars})'
    row, trades = run_config(df5_scored, 'mtf_15m_score', thresh, label)
    row['experiment'] = 'A'
    row['gate_description'] = f'partial_score>={thresh}'
    row['lookback_bars'] = 1
    exp_a_rows.append(row)
    all_results.append(row)

pd.DataFrame(exp_a_rows).to_csv('paper_outputs/mtf_exp_a_partial_score.csv', index=False)
print('  → saved paper_outputs/mtf_exp_a_partial_score.csv\n')


# ── Experiment B — Lookback window (any of last 3 × 15m bars) ─────────────

print('=' * 80)
print('Experiment B — Partial score gate + lookback=3 bars')
print('  (any of the last 3 fifteen-minute bars must pass >= N gates)')
print('=' * 80)

exp_b_rows = []

for thresh in [4, 5]:
    # Rolling OR: was there a bar with score>=thresh in the last 3 15m bars?
    any_recent = apply_lookback_window(scores_15m, lookback_n=3, min_score=thresh)
    # Merge the boolean flag onto 5m
    flag_df = any_recent.reset_index()
    flag_df.columns = ['timestamp', 'mtf_lookback_fired']
    df5_lb = pd.merge_asof(
        df5.sort_values('timestamp'),
        flag_df.sort_values('timestamp'),
        on='timestamp', direction='backward',
    ).reset_index(drop=True)
    df5_lb['mtf_lookback_fired'] = (
        df5_lb['mtf_lookback_fired']
        .infer_objects(copy=False)
        .fillna(False)
        .astype(bool)
    )

    active_bars = df5_lb['mtf_lookback_fired'].sum()
    label = f'Exp B  score>={thresh}/9  lookback=3  (5m bars: {active_bars})'
    row, trades = run_config(df5_lb, 'mtf_lookback_fired', True, label)
    row['experiment'] = 'B'
    row['gate_description'] = f'partial_score>={thresh}_any_of_last_3'
    row['lookback_bars'] = 3
    exp_b_rows.append(row)
    all_results.append(row)

pd.DataFrame(exp_b_rows).to_csv('paper_outputs/mtf_exp_b_lookback.csv', index=False)
print('  → saved paper_outputs/mtf_exp_b_lookback.csv\n')


# ── Experiment C — Minimal 2-gate: ema_trend + vwap_above ────────────────

print('=' * 80)
print('Experiment C — Minimal 2-gate MTF confirmation')
print('  (15m bar must satisfy: ema_9 > ema_20  AND  close > vwap)')
print('  No final_signal requirement, no pullback gate, no 1h trend gate')
print('=' * 80)

df5_simple = merge_scores_onto_5m(df5, simple_gate_15m.astype(int), 'mtf_simple_gate')
active_bars = (df5_simple['mtf_simple_gate'] >= 1).sum()
label = f'Exp C  ema_trend + vwap_above        (5m bars: {active_bars})'
row, trades_c = run_config(df5_simple, 'mtf_simple_gate', 1, label)
row['experiment'] = 'C'
row['gate_description'] = 'ema_9>ema_20 AND close>vwap'
row['lookback_bars'] = 1
all_results.append(row)
trades_c.to_csv('paper_outputs/mtf_exp_c_simple_gate.csv', index=False)
print('  → saved paper_outputs/mtf_exp_c_simple_gate.csv\n')


# ── Full comparison table ─────────────────────────────────────────────────

comparison_df = pd.DataFrame(all_results)
comparison_df.to_csv('paper_outputs/mtf_relaxed_experiments.csv', index=False)

print('=' * 80)
print('FULL COMPARISON')
print('=' * 80)
display_cols = [
    'label', 'total_trades', 'win_rate_pct', 'net_pnl_pct',
    'max_drawdown_pct', 'profit_factor', 'sharpe_ratio', 'avg_duration_candles',
]
print(comparison_df[display_cols].to_string(index=False))

# ── Best config: >= 50 trades, highest profit factor ─────────────────────
print('\n' + '=' * 80)
print('BEST CONFIGURATION (>= 50 trades, highest profit factor)')
print('=' * 80)
valid = comparison_df[
    comparison_df['total_trades'] >= 50
].dropna(subset=['profit_factor'])

if valid.empty:
    print('  No configuration reached 50 trades. Best by profit factor:')
    valid = comparison_df.dropna(subset=['profit_factor'])
    if not valid.empty:
        best = valid.loc[valid['profit_factor'].idxmax()]
    else:
        print('  No configuration produced a valid profit factor.')
        best = None
else:
    best = valid.loc[valid['profit_factor'].idxmax()]

if best is not None:
    print(f'\n  Strategy       : {best["label"]}')
    print(f'  Gate           : {best["gate_description"]}')
    print(f'  Total trades   : {best["total_trades"]}')
    print(f'  Win rate       : {best["win_rate_pct"]}%')
    print(f'  Net PnL        : {best["net_pnl_pct"]}%')
    print(f'  Max drawdown   : {best["max_drawdown_pct"]}%')
    print(f'  Profit factor  : {best["profit_factor"]}')
    print(f'  Sharpe ratio   : {best["sharpe_ratio"]}')
    print(f'  Avg duration   : {best["avg_duration_candles"]} candles')

print('\nSaved:')
print('  paper_outputs/mtf_exp_a_partial_score.csv')
print('  paper_outputs/mtf_exp_b_lookback.csv')
print('  paper_outputs/mtf_exp_c_simple_gate.csv')
print('  paper_outputs/mtf_relaxed_experiments.csv')
