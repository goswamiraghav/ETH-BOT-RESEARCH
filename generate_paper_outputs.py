"""
generate_paper_outputs.py
─────────────────────────────────────────────────────────────────────────────
Produces all paper-ready outputs:

  1. paper_outputs/final_results_summary.csv
  2. figures/equity_curve_comparison.png
  3. figures/trade_distribution.png
  4. figures/metrics_comparison.png
  5. figures/signal_score_distribution.png
  6. Prints statistical test (t-test on MTF trade returns vs zero)
"""

import os
import math
import warnings

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from scipy import stats

from transformation_v2 import add_all_indicators_v2

warnings.filterwarnings('ignore')

os.makedirs('paper_outputs', exist_ok=True)
os.makedirs('figures', exist_ok=True)

# ── Style ──────────────────────────────────────────────────────────────────
plt.rcParams.update({
    'figure.dpi':        150,
    'figure.facecolor':  'white',
    'axes.facecolor':    '#f8f8f8',
    'axes.grid':         True,
    'grid.color':        '#e0e0e0',
    'grid.linewidth':    0.8,
    'axes.spines.top':   False,
    'axes.spines.right': False,
    'font.family':       'DejaVu Sans',
    'font.size':         11,
    'axes.titlesize':    13,
    'axes.labelsize':    11,
    'legend.fontsize':   10,
    'legend.framealpha': 0.85,
})

PALETTE = {
    '5m Standalone':  '#2196F3',
    '15m Standalone': '#FF9800',
    'MTF Combined':   '#4CAF50',
    'Exp C (Relaxed)':'#9C27B0',
}

WINDOW = 21   # must match backtester_clean.py


# ── Load trades ────────────────────────────────────────────────────────────

def load_trades(path):
    df = pd.read_csv(path)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    return df


trades_5m   = load_trades('paper_outputs/mtf_trades_5m.csv')
trades_15m  = load_trades('paper_outputs/mtf_trades_15m.csv')
trades_mtf  = load_trades('paper_outputs/mtf_trades_combined.csv')
trades_expc = load_trades('paper_outputs/mtf_exp_c_simple_gate.csv')


# ── Metrics helpers ────────────────────────────────────────────────────────

def compute_metrics(trades_df, label):
    if trades_df.empty:
        return {
            'Strategy': label,
            'Total Trades': 0, 'Win Rate (%)': 0.0, 'Net PnL (%)': 0.0,
            'Avg Trade (%)': 0.0, 'Max Drawdown (%)': 0.0,
            'Profit Factor': None, 'Sharpe Ratio': None,
            'Avg Duration (candles)': None,
        }
    pnl = trades_df['pnl_pct']
    equity = (1 + pnl / 100).cumprod()
    dd = ((equity / equity.cummax()) - 1) * 100
    gains  = pnl[pnl > 0].sum()
    losses = -pnl[pnl < 0].sum()
    pf = round(gains / losses, 3) if losses > 0 else None

    n = len(pnl)
    days = max((trades_df['timestamp'].max() - trades_df['timestamp'].min()).days, 1)
    ann_factor = math.sqrt(n / (days / 365.25))
    std = pnl.std(ddof=1)
    sharpe = round((pnl.mean() / std) * ann_factor, 3) if std > 0 else None

    return {
        'Strategy':               label,
        'Total Trades':           n,
        'Win Rate (%)':           round(trades_df['was_profitable'].mean() * 100, 2),
        'Net PnL (%)':            round(pnl.sum(), 2),
        'Avg Trade (%)':          round(pnl.mean(), 4),
        'Max Drawdown (%)':       round(dd.min(), 2),
        'Profit Factor':          pf,
        'Sharpe Ratio':           sharpe,
        'Avg Duration (candles)': round(trades_df['duration_candles'].mean(), 2),
    }


m5   = compute_metrics(trades_5m,   '5m Standalone')
m15  = compute_metrics(trades_15m,  '15m Standalone')
mmtf = compute_metrics(trades_mtf,  'MTF Combined')
mexc = compute_metrics(trades_expc, 'Exp C (Relaxed)')


# ══════════════════════════════════════════════════════════════════════════
# 1 — final_results_summary.csv
# ══════════════════════════════════════════════════════════════════════════

summary_df = pd.DataFrame([m5, m15, mmtf, mexc])
summary_df.to_csv('paper_outputs/final_results_summary.csv', index=False)
print('✓ Saved paper_outputs/final_results_summary.csv')
print(summary_df.set_index('Strategy').to_string())
print()


# ══════════════════════════════════════════════════════════════════════════
# 2 — equity_curve_comparison.png
# ══════════════════════════════════════════════════════════════════════════

def equity_series_by_date(trades_df):
    """Map trades to a daily equity index (forward-filled between trades)."""
    if trades_df.empty:
        return pd.Series(dtype=float)
    pnl = trades_df.set_index('timestamp')['pnl_pct'].sort_index()
    equity = (1 + pnl / 100).cumprod()
    # resample to daily, forward-fill
    equity = equity.resample('D').last().ffill()
    return equity


fig, ax = plt.subplots(figsize=(12, 6))

date_min = min(t['timestamp'].min() for t in [trades_5m, trades_15m, trades_mtf] if not t.empty)
date_max = max(t['timestamp'].max() for t in [trades_5m, trades_15m, trades_mtf] if not t.empty)
full_idx  = pd.date_range(date_min, date_max, freq='D')

for trades, label in [
    (trades_5m,  '5m Standalone'),
    (trades_15m, '15m Standalone'),
    (trades_mtf, 'MTF Combined'),
]:
    eq = equity_series_by_date(trades)
    eq = eq.reindex(full_idx).ffill().fillna(1.0)
    lw = 2.5 if label == 'MTF Combined' else 1.5
    ax.plot(eq.index, eq.values,
            label=f'{label}  (n={len(trades)})',
            color=PALETTE[label], linewidth=lw,
            zorder=3 if label == 'MTF Combined' else 2)

ax.axhline(1.0, color='#333333', linewidth=0.8, linestyle='--', alpha=0.6, label='Break-even')
ax.set_title('Equity Curves — ETH/USDT Strategy Comparison', fontweight='bold', pad=14)
ax.set_xlabel('Date')
ax.set_ylabel('Cumulative Equity (starting = 1.0)')
ax.legend(loc='lower left')
ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda y, _: f'{y:.2f}x'))
fig.autofmt_xdate()
fig.tight_layout()
fig.savefig('figures/equity_curve_comparison.png', dpi=150, bbox_inches='tight')
plt.close(fig)
print('✓ Saved figures/equity_curve_comparison.png')


# ══════════════════════════════════════════════════════════════════════════
# 3 — trade_distribution.png
# ══════════════════════════════════════════════════════════════════════════

strategies = ['5m Standalone', '15m Standalone', 'MTF Combined', 'Exp C (Relaxed)']
counts     = [m5['Total Trades'], m15['Total Trades'], mmtf['Total Trades'], mexc['Total Trades']]
colors     = [PALETTE[s] for s in strategies]

fig, ax = plt.subplots(figsize=(9, 5))
bars = ax.bar(strategies, counts, color=colors, edgecolor='white', linewidth=0.8, width=0.55)

for bar, cnt in zip(bars, counts):
    ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 15,
            f'{cnt:,}', ha='center', va='bottom', fontsize=11, fontweight='bold')

ax.axhline(50, color='#e74c3c', linewidth=1.2, linestyle='--',
           label='Statistical minimum (50 trades)', zorder=5)
ax.set_title('Total Trades Generated per Strategy', fontweight='bold', pad=14)
ax.set_ylabel('Number of Trades')
ax.set_ylim(0, max(counts) * 1.18)
ax.legend()
fig.tight_layout()
fig.savefig('figures/trade_distribution.png', dpi=150, bbox_inches='tight')
plt.close(fig)
print('✓ Saved figures/trade_distribution.png')


# ══════════════════════════════════════════════════════════════════════════
# 4 — metrics_comparison.png
# ══════════════════════════════════════════════════════════════════════════

strats3 = ['5m Standalone', '15m Standalone', 'MTF Combined']
ms3     = [m5, m15, mmtf]
cols3   = [PALETTE[s] for s in strats3]
x       = np.arange(len(strats3))
w       = 0.62

fig, axes = plt.subplots(1, 3, figsize=(15, 5))

# Win rate
wr_vals = [m['Win Rate (%)'] for m in ms3]
axes[0].bar(x, wr_vals, color=cols3, width=w, edgecolor='white')
axes[0].axhline(50, color='#e74c3c', linewidth=1.1, linestyle='--', alpha=0.7, label='50% baseline')
for i, v in enumerate(wr_vals):
    axes[0].text(i, v + 0.5, f'{v:.1f}%', ha='center', va='bottom', fontsize=10, fontweight='bold')
axes[0].set_title('Win Rate (%)', fontweight='bold')
axes[0].set_xticks(x); axes[0].set_xticklabels(strats3, rotation=12, ha='right')
axes[0].set_ylim(0, 60)
axes[0].legend(fontsize=9)

# Profit factor
pf_vals = [m['Profit Factor'] if m['Profit Factor'] else 0.0 for m in ms3]
axes[1].bar(x, pf_vals, color=cols3, width=w, edgecolor='white')
axes[1].axhline(1.0, color='#e74c3c', linewidth=1.1, linestyle='--', alpha=0.7, label='Breakeven (PF=1)')
for i, v in enumerate(pf_vals):
    axes[1].text(i, v + 0.01, f'{v:.3f}', ha='center', va='bottom', fontsize=10, fontweight='bold')
axes[1].set_title('Profit Factor', fontweight='bold')
axes[1].set_xticks(x); axes[1].set_xticklabels(strats3, rotation=12, ha='right')
axes[1].set_ylim(0, max(pf_vals) * 1.3 + 0.2)
axes[1].legend(fontsize=9)

# Max drawdown (absolute, so higher bar = worse)
dd_vals = [abs(m['Max Drawdown (%)']) for m in ms3]
axes[2].bar(x, dd_vals, color=cols3, width=w, edgecolor='white')
for i, v in enumerate(dd_vals):
    axes[2].text(i, v + 0.5, f'-{v:.1f}%', ha='center', va='bottom', fontsize=10, fontweight='bold')
axes[2].set_title('Max Drawdown (%) — lower is better', fontweight='bold')
axes[2].set_xticks(x); axes[2].set_xticklabels(strats3, rotation=12, ha='right')
axes[2].set_ylim(0, max(dd_vals) * 1.15)

fig.suptitle('Strategy Metrics Comparison — ETH/USDT', fontsize=14, fontweight='bold', y=1.02)
fig.tight_layout()
fig.savefig('figures/metrics_comparison.png', dpi=150, bbox_inches='tight')
plt.close(fig)
print('✓ Saved figures/metrics_comparison.png')


# ══════════════════════════════════════════════════════════════════════════
# 5 — signal_score_distribution.png  (recompute from 15m data)
# ══════════════════════════════════════════════════════════════════════════

print('Computing 15m partial scores for distribution plot...')

def load_ohlcv(path):
    df = pd.read_csv(path)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df = df.sort_values('timestamp').reset_index(drop=True)
    return df

def add_1h_trend(df_15m):
    df = df_15m.set_index('timestamp')
    df_1h = df[['open','high','low','close','volume']].resample('1h').agg(
        {'open':'first','high':'max','low':'min','close':'last','volume':'sum'}
    ).dropna().reset_index()
    df_1h = add_all_indicators_v2(df_1h)
    slim = df_1h[['timestamp','ema_20','macd','macd_signal']].rename(columns={
        'ema_20':'ema20_1h','macd':'macd_1h','macd_signal':'macd_signal_1h'})
    return pd.merge_asof(df_15m.sort_values('timestamp'), slim,
                         on='timestamp', direction='backward').reset_index(drop=True)

def partial_score_15m(window_df):
    if len(window_df) < 2:
        return 0
    c = window_df.iloc[-1]
    p = window_df.iloc[-2]
    def s(col): v = c.get(col, None); return v if (v is not None and pd.notna(v)) else None
    score = 0
    c1,e1h,m1h,ms1h = s('close'),s('ema20_1h'),s('macd_1h'),s('macd_signal_1h')
    if all(v is not None for v in [c1,e1h,m1h,ms1h]):
        if c1 > e1h and m1h > ms1h: score += 1
    e9,e20 = s('ema_9'),s('ema_20')
    if e9 and e20 and e9 > e20: score += 1
    mac,macs = s('macd'),s('macd_signal')
    if mac is not None and macs is not None and mac > macs: score += 1
    vr = s('volume_ratio')
    if vr is not None and vr >= 1.3: score += 1
    atr,cl = s('atr'),s('close')
    if atr and cl and cl > 0 and atr > cl * 0.005: score += 1
    if e20 is not None and pd.notna(p.get('low', None)):
        if p['low'] <= e20 * 1.002 and c['close'] > e20: score += 1
    rsi = s('rsi')
    if rsi is not None and 45 <= rsi <= 70: score += 1
    hi = s('high')
    if hi and cl and cl >= hi * 0.996: score += 1
    if atr and atr > 0 and abs(c['close'] - c['open']) > atr * 0.25: score += 1
    return score

df15 = load_ohlcv('ethusdt_15m_1y.csv')
df15 = add_all_indicators_v2(df15)
df15 = df15.drop(columns=['ema200'], errors='ignore')
df15 = add_1h_trend(df15)

score_list = []
for i in range(WINDOW, len(df15)):
    score_list.append(partial_score_15m(df15.iloc[i - WINDOW: i + 1]))

score_counts = pd.Series(score_list).value_counts().sort_index()
total_scored = len(score_list)

fig, ax = plt.subplots(figsize=(10, 5))
bar_colors = ['#90CAF9'] * 10
bar_colors[9] = '#4CAF50'   # score=9 (full signal) highlighted green

bars = ax.bar(score_counts.index, score_counts.values,
              color=[bar_colors[i] for i in score_counts.index],
              edgecolor='white', linewidth=0.8, width=0.7)

for bar, (score_val, cnt) in zip(bars, score_counts.items()):
    pct = cnt / total_scored * 100
    ax.text(bar.get_x() + bar.get_width() / 2,
            bar.get_height() + 40,
            f'{cnt:,}\n({pct:.1f}%)',
            ha='center', va='bottom', fontsize=9,
            fontweight='bold' if score_val == 9 else 'normal')

ax.set_title('15m Gate: Partial Score Distribution (0–9 gates passing)\n'
             'Strict MTF gate (score = 9) is shown in green — only 64 bars / year',
             fontweight='bold', pad=14)
ax.set_xlabel('Number of Gates Passing (out of 9)')
ax.set_ylabel('Number of 15m Bars')
ax.set_xticks(range(10))
ax.set_xticklabels([f'{i}/9' for i in range(10)])

# annotation arrow for score=9
if 9 in score_counts.index:
    ax.annotate('Strict gate\n(only 64 bars)',
                xy=(9, score_counts[9]),
                xytext=(7.5, score_counts[9] + 500),
                arrowprops=dict(arrowstyle='->', color='#333', lw=1.5),
                fontsize=10, color='#2e7d32', fontweight='bold')

fig.tight_layout()
fig.savefig('figures/signal_score_distribution.png', dpi=150, bbox_inches='tight')
plt.close(fig)
print('✓ Saved figures/signal_score_distribution.png')


# ══════════════════════════════════════════════════════════════════════════
# 6 — Statistical test on MTF trade returns
# ══════════════════════════════════════════════════════════════════════════

print()
print('=' * 60)
print('STATISTICAL TEST: MTF Combined strategy trade returns')
print('=' * 60)

returns = trades_mtf['pnl_pct'].values
n       = len(returns)
mean_r  = np.mean(returns)
std_r   = np.std(returns, ddof=1)
sem_r   = std_r / np.sqrt(n)

t_stat, p_value = stats.ttest_1samp(returns, popmean=0)

print(f'  Sample size (n)        : {n}')
print(f'  Mean return            : {mean_r:.4f}%')
print(f'  Std deviation          : {std_r:.4f}%')
print(f'  Standard error         : {sem_r:.4f}%')
print(f'  95% CI                 : [{mean_r - 1.96*sem_r:.4f}%, {mean_r + 1.96*sem_r:.4f}%]')
print(f'  t-statistic            : {t_stat:.4f}')
print(f'  p-value (two-tailed)   : {p_value:.4f}')
print()

alpha = 0.05
if p_value < alpha:
    print(f'  RESULT: Statistically significant at p < {alpha}.')
    print(f'          The mean return of {mean_r:.4f}% is significantly different from zero.')
else:
    print(f'  RESULT: NOT statistically significant at p < {alpha}  (p = {p_value:.4f}).')
    print(f'          Cannot reject H0 (mean return = 0) at the 5% level.')

print()
print('  ⚠ LIMITATION — n=9 is critically small:')
print(f'    • Power to detect a true effect is very low (~{(1-stats.t.cdf(stats.t.ppf(0.975, n-1), n-1, loc=abs(mean_r)/sem_r))*100:.0f}% approx.)')
print( '    • The t-distribution assumes normality; with n=9 this is untestable.')
print( '    • Results are illustrative only. A minimum of ~30 trades is generally')
print( '      required for a meaningful test; 100+ for reliable inference.')
print( '    • Recommended: extend data window to 3–5 years before drawing')
print( '      conclusions from the MTF Combined strategy.')

# Save stat test to CSV too
stat_result = pd.DataFrame([{
    'strategy':       'MTF Combined',
    'n':              n,
    'mean_return_pct': round(mean_r, 4),
    'std_pct':         round(std_r, 4),
    'sem_pct':         round(sem_r, 4),
    'ci_lower_95':     round(mean_r - 1.96*sem_r, 4),
    'ci_upper_95':     round(mean_r + 1.96*sem_r, 4),
    't_statistic':     round(t_stat, 4),
    'p_value':         round(p_value, 4),
    'significant_p05': p_value < 0.05,
    'note':            'n=9 — critically underpowered; illustrative only',
}])
stat_result.to_csv('paper_outputs/mtf_statistical_test.csv', index=False)
print()
print('✓ Saved paper_outputs/mtf_statistical_test.csv')

print()
print('─' * 60)
print('ALL OUTPUTS READY')
print('─' * 60)
print('paper_outputs/final_results_summary.csv')
print('paper_outputs/mtf_statistical_test.csv')
print('figures/equity_curve_comparison.png')
print('figures/trade_distribution.png')
print('figures/metrics_comparison.png')
print('figures/signal_score_distribution.png')
