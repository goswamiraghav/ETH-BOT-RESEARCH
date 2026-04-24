"""
fix_equity_curve.py
Regenerates figures/equity_curve_comparison.png with:
  - Log Y-axis so all three curves are visible simultaneously
  - Y-axis anchored at 1.0 start, ranging from actual minimum to above 1.0
  - Equity computed by cumulative product at each trade (not daily resample),
    then plotted against trade-entry date for accuracy
"""

import math
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import matplotlib.dates as mdates

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
    'legend.framealpha': 0.90,
})

PALETTE = {
    '5m Standalone':  '#2196F3',
    '15m Standalone': '#FF9800',
    'MTF Combined':   '#4CAF50',
}


def load_trades(path):
    df = pd.read_csv(path)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    return df.sort_values('timestamp').reset_index(drop=True)


def equity_curve(trades_df):
    """
    Returns a (timestamps, equity_values) pair.
    Equity starts at 1.0 before the first trade and updates at each
    trade exit — no daily resampling, so the full collapse is preserved.
    """
    if trades_df.empty:
        return pd.Series(dtype=float)
    pnl = trades_df['pnl_pct'].values / 100.0
    eq  = [1.0]                           # starting equity
    ts  = [trades_df['timestamp'].iloc[0] - pd.Timedelta(days=1)]  # anchor point
    cumulative = 1.0
    for i, r in enumerate(pnl):
        cumulative *= (1.0 + r)
        eq.append(cumulative)
        ts.append(trades_df['timestamp'].iloc[i])
    return pd.Series(eq, index=ts)


trades_5m  = load_trades('paper_outputs/mtf_trades_5m.csv')
trades_15m = load_trades('paper_outputs/mtf_trades_15m.csv')
trades_mtf = load_trades('paper_outputs/mtf_trades_combined.csv')

eq5   = equity_curve(trades_5m)
eq15  = equity_curve(trades_15m)
eqmtf = equity_curve(trades_mtf)

# Debug — confirm curves reach expected ranges
for name, eq in [('5m', eq5), ('15m', eq15), ('MTF', eqmtf)]:
    print(f'  {name}: start={eq.iloc[0]:.4f}  end={eq.iloc[-1]:.4f}  '
          f'min={eq.min():.4f}  max={eq.max():.4f}')

# ── Plot ───────────────────────────────────────────────────────────────────
fig, ax = plt.subplots(figsize=(12, 6))

for eq, label in [(eq5, '5m Standalone'), (eq15, '15m Standalone'), (eqmtf, 'MTF Combined')]:
    n = len(trades_5m) if label == '5m Standalone' else \
        len(trades_15m) if label == '15m Standalone' else len(trades_mtf)
    lw = 2.5 if label == 'MTF Combined' else 1.6
    ax.plot(eq.index, eq.values,
            label=f'{label}  (n={n})',
            color=PALETTE[label],
            linewidth=lw,
            zorder=3 if label == 'MTF Combined' else 2)

ax.axhline(1.0, color='#333333', linewidth=1.0, linestyle='--',
           alpha=0.7, label='Break-even (1.0×)', zorder=1)

# Log scale — shows all three curves despite 100× difference in range
ax.set_yscale('log')

# Y limits: from slightly below actual minimum to slightly above 1.0
all_vals = pd.concat([eq5, eq15, eqmtf])
y_min = max(all_vals.min() * 0.5, 1e-4)   # 50% below actual min, floor at 0.0001
y_max = all_vals.max() * 1.3
ax.set_ylim(y_min, y_max)

# Tick labels as multipliers (e.g. 0.01×, 0.1×, 1.0×)
ax.yaxis.set_major_formatter(
    mticker.FuncFormatter(lambda y, _: f'{y:.3g}×')
)
ax.yaxis.set_minor_formatter(mticker.NullFormatter())

ax.set_title('Equity Curves — ETH/USDT Strategy Comparison\n'
             '(log scale — all three strategies visible simultaneously)',
             fontweight='bold', pad=14)
ax.set_xlabel('Date')
ax.set_ylabel('Cumulative Equity  [log scale,  start = 1.0×]')
ax.legend(loc='lower left')
ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %Y'))
ax.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
fig.autofmt_xdate()
fig.tight_layout()

out = 'figures/equity_curve_comparison.png'
fig.savefig(out, dpi=150, bbox_inches='tight')
plt.close(fig)
print(f'✓ Saved {out}')
