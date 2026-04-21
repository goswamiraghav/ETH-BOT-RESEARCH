import pandas as pd


def run_backtest_v2(df, signal_function, score_threshold=4, tp_k=1.8, sl_k=1.2, max_duration=3, fee_bps=10):
    trades = []
    window = 21
    fee_pct = fee_bps / 10000

    for i in range(window, len(df) - max_duration):
        window_df = df.iloc[i - window:i + 1].copy()
        signal = signal_function(window_df)
        if not signal.get('final_signal', False):
            continue
        if signal.get('match_score', 0) < score_threshold:
            continue

        entry_row   = df.iloc[i]
        entry_price = entry_row['close']
        atr         = entry_row['atr']
        direction   = signal.get('direction', 'long')
        if pd.isna(atr) or atr <= 0:
            continue

        if direction == 'long':
            tp_price    = entry_price + tp_k * atr
            sl_price    = entry_price - sl_k * atr
        else:
            tp_price    = entry_price - tp_k * atr
            sl_price    = entry_price + sl_k * atr

        trailing_sl = sl_price
        exit_price  = None
        exit_reason = 'timeout'
        mfe = float('-inf')
        mae = float('inf')

        for j in range(1, max_duration + 1):
            row         = df.iloc[i + j]
            high        = row['high']
            low         = row['low']
            close       = row['close']
            current_atr = row['atr'] if pd.notna(row['atr']) else atr

            if direction == 'long':
                trailing_sl = max(trailing_sl, close - sl_k * current_atr)
                mfe = max(mfe, (high - entry_price) / atr)
                mae = min(mae, (low  - entry_price) / atr)
                if high >= tp_price:
                    exit_price  = tp_price
                    exit_reason = 'tp_hit'
                    duration    = j
                    break
                if low <= trailing_sl:
                    exit_price  = trailing_sl
                    exit_reason = 'sl_hit'
                    duration    = j
                    break
            else:  # short
                trailing_sl = min(trailing_sl, close + sl_k * current_atr)
                mfe = max(mfe, (entry_price - low)  / atr)
                mae = min(mae, (entry_price - high) / atr)
                if low <= tp_price:
                    exit_price  = tp_price
                    exit_reason = 'tp_hit'
                    duration    = j
                    break
                if high >= trailing_sl:
                    exit_price  = trailing_sl
                    exit_reason = 'sl_hit'
                    duration    = j
                    break
        else:
            exit_price = df.iloc[i + max_duration]['close']
            duration   = max_duration

        if direction == 'long':
            gross_pnl_pct = (exit_price - entry_price) / entry_price
        else:
            gross_pnl_pct = (entry_price - exit_price) / entry_price

        net_pnl_pct = gross_pnl_pct - 2 * fee_pct
        trades.append({
            'timestamp':        entry_row['timestamp'],
            'symbol':           entry_row['symbol'],
            'direction':        direction,
            'entry_price':      entry_price,
            'exit_price':       exit_price,
            'exit_reason':      exit_reason,
            'duration_candles': duration,
            'pnl_pct':          round(net_pnl_pct * 100, 4),
            'was_profitable':   net_pnl_pct > 0,
            'tp_price':         tp_price,
            'sl_price':         sl_price,
            'mfe_atr':          round(mfe, 4),
            'mae_atr':          round(mae, 4),
            'match_score':      signal['match_score'],
            'signal_combo_name': signal.get('signal_combo_name', 'unknown'),
            'logic_debug_note': signal.get('logic_debug_note', ''),
        })

    return pd.DataFrame(trades)


def summarize_backtest(trades_df):
    if trades_df.empty:
        return {
            'total_trades': 0,
            'win_rate_pct': 0.0,
            'net_pnl_pct': 0.0,
            'avg_trade_pct': 0.0,
            'max_drawdown_pct': 0.0,
            'profit_factor': 0.0,
        }

    pnl = trades_df['pnl_pct']
    equity = (1 + pnl / 100).cumprod()
    rolling_peak = equity.cummax()
    drawdown = (equity / rolling_peak - 1) * 100
    gains = pnl[pnl > 0].sum()
    losses = -pnl[pnl < 0].sum()

    return {
        'total_trades': int(len(trades_df)),
        'win_rate_pct': round((trades_df['was_profitable'].mean() * 100), 2),
        'net_pnl_pct': round(pnl.sum(), 2),
        'avg_trade_pct': round(pnl.mean(), 4),
        'max_drawdown_pct': round(drawdown.min(), 2),
        'profit_factor': round(gains / losses, 3) if losses > 0 else None,
    }
