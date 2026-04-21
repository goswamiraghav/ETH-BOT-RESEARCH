"""
telegram_bot.py
─────────────────────────────────────────────────────────────────────────────
Live Telegram alert bot for ETH/USDT — scans 15m and 1h independently.

SETUP (do once):
  1. Message @BotFather on Telegram → /newbot → copy BOT_TOKEN
  2. Message your new bot, then open:
       https://api.telegram.org/bot<TOKEN>/getUpdates
     Copy the "id" field from "chat" → that's your CHAT_ID
  3. pip install ccxt pandas requests

RUN:
  python telegram_bot.py

WHAT IT DOES:
  - Polls every 60 seconds across 15m and 1h timeframes
  - 15m signal requires 1h trend confirmation — fetches 1h candles separately
    and merges EMA/MACD values onto the 15m dataframe via pd.merge_asof
  - Fires a Telegram alert with entry/TP/SL when all gates pass
  - Tracks last alerted candle per timeframe — no duplicate alerts
  - Sends a startup message and daily summary at 00:00 UTC
"""

import os
import time
import traceback
from datetime import datetime, timezone

import ccxt
import pandas as pd
import requests
from dotenv import load_dotenv

from transformation_v2    import add_all_indicators_v2
from signal_generator_15m import generate_signal_15m
from signal_generator_v2  import generate_signal_v2

load_dotenv()

# ── CONFIG ────────────────────────────────────────────────────────────────
BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
CHAT_ID   = os.environ.get("TELEGRAM_CHAT_ID", "")

SYMBOL    = "ETH/USDT"
POLL_SECS = 60

TIMEFRAMES = {
    '15m': {
        'signal_fn': generate_signal_15m,
        'tp_k':      1.0,
        'sl_k':      0.8,
        'candles':   150,
        'label':     'Swing',
    },
    '1h': {
        'signal_fn': generate_signal_v2,
        'tp_k':      0.7,
        'sl_k':      1.2,
        'candles':   250,
        'label':     'Position',
    },
}

# How many 1h candles to fetch when building the 15m trend-confirmation columns.
# 250 gives ~10 days of 1h history — enough for EMA200 to stabilise.
CANDLES_1H_FOR_15M = 250
# ─────────────────────────────────────────────────────────────────────────


def send_telegram(text: str) -> None:
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    try:
        r = requests.post(url, data={
            "chat_id":    CHAT_ID,
            "text":       text,
            "parse_mode": "Markdown",
        }, timeout=10)
        if not r.ok:
            print(f"[TELEGRAM ERROR] {r.status_code}: {r.text}")
    except Exception as e:
        print(f"[TELEGRAM ERROR] {e}")


def fetch_candles(timeframe: str, limit: int) -> pd.DataFrame:
    exchange = ccxt.kucoin()
    ohlcv = exchange.fetch_ohlcv(SYMBOL, timeframe=timeframe, limit=limit)
    df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
    df['symbol'] = SYMBOL
    return df


def attach_1h_trend_columns(df_15m: pd.DataFrame) -> pd.DataFrame:
    """
    Fetch 1h candles, compute indicators, and left-join EMA/MACD columns
    onto the 15m dataframe using merge_asof (backward match — each 15m
    candle gets the most recently closed 1h candle's values).
    """
    df_1h = fetch_candles('1h', CANDLES_1H_FOR_15M)
    df_1h = df_1h.iloc[:-1].copy()          # drop unclosed 1h candle
    df_1h = add_all_indicators_v2(df_1h)

    df_1h = df_1h[['timestamp', 'ema_9', 'ema_20', 'ema50', 'ema200',
                    'macd', 'macd_signal']].rename(columns={
        'ema_9':       'ema9_1h',
        'ema_20':      'ema20_1h',
        'ema50':       'ema50_1h',
        'ema200':      'ema200_1h',
        'macd':        'macd_1h',
        'macd_signal': 'macd_signal_1h',
    })

    df_15m = pd.merge_asof(
        df_15m.sort_values('timestamp'),
        df_1h,
        on='timestamp',
        direction='backward',
    ).reset_index(drop=True)

    # ATR expanding gate: rolling 20-period mean of 15m ATR
    df_15m['atr_ma_20'] = df_15m['atr'].rolling(20).mean()

    return df_15m


def build_alert(current_row, entry: float, tp: float, sl: float,
                signal: dict, timeframe: str, cfg: dict) -> str:
    tp_k    = cfg['tp_k']
    sl_k    = cfg['sl_k']
    label   = cfg['label']
    atr     = float(current_row['atr'])
    tp_pct  = round((tp - entry) / entry * 100, 2)
    sl_pct  = round((entry - sl) / entry * 100, 2)
    rr      = round(tp_k / sl_k, 2)
    rsi     = signal.get('rsi', '?')
    vol_rat = signal.get('volume_ratio', '?')
    ts      = str(current_row['timestamp'])[:16]

    return (
        f"🚨 *LONG SIGNAL — {SYMBOL}*\n"
        f"📋 *{label}* · `{timeframe}` timeframe\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📈 *Entry :* `{round(entry, 2)}`\n"
        f"✅ *TP    :* `{round(tp, 2)}`  (+{tp_pct}%)\n"
        f"🛑 *SL    :* `{round(sl, 2)}`  (−{sl_pct}%)\n"
        f"⚖️  *R/R   :* `{rr}`\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 *RSI   :* `{rsi}`\n"
        f"📦 *Vol   :* `{vol_rat}x avg`\n"
        f"🧮 *ATR   :* `{round(atr, 2)}`\n"
        f"🕐 `{ts} UTC`\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"_Gates: {signal.get('signal_combo_name', '?')}_"
    )


def check_1h(cfg: dict, last_ts: str) -> str:
    """Fetch, compute indicators, evaluate 1h signal."""
    df = fetch_candles('1h', cfg['candles'])
    df = df.iloc[:-1].copy()
    df = add_all_indicators_v2(df)

    current   = df.iloc[-1]
    candle_ts = str(current['timestamp'])

    if candle_ts == last_ts:
        return last_ts

    signal = cfg['signal_fn'](df)

    if signal.get('final_signal', False):
        entry = float(current['close'])
        atr   = float(current['atr'])
        tp    = round(entry + cfg['tp_k'] * atr, 4)
        sl    = round(entry - cfg['sl_k'] * atr, 4)
        msg   = build_alert(current, entry, tp, sl, signal, '1h', cfg)
        send_telegram(msg)
        print(
            f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] "
            f"SIGNAL 1h (Position) — entry={entry:.2f} TP={tp:.2f} SL={sl:.2f}"
        )
        return candle_ts
    else:
        reason = signal.get('logic_debug_note', '?')
        print(
            f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] "
            f"1h  no-signal  gate={reason:<22s}  close={current['close']:.2f}"
        )
        return last_ts


def check_15m(cfg: dict, last_ts: str) -> str:
    """
    Fetch 15m candles, attach 1h trend columns, evaluate 15m signal.
    The 1h fetch is done inside here so it's always fresh on each poll.
    """
    df = fetch_candles('15m', cfg['candles'])
    df = df.iloc[:-1].copy()
    df = add_all_indicators_v2(df)

    # Drop the standalone ema200 column — 15m signal uses ema200_1h instead
    df = df.drop(columns=['ema200'], errors='ignore')

    # Attach 1h trend confirmation columns
    df = attach_1h_trend_columns(df)

    current   = df.iloc[-1]
    candle_ts = str(current['timestamp'])

    if candle_ts == last_ts:
        return last_ts

    signal = cfg['signal_fn'](df)

    if signal.get('final_signal', False):
        entry = float(current['close'])
        atr   = float(current['atr'])
        tp    = round(entry + cfg['tp_k'] * atr, 4)
        sl    = round(entry - cfg['sl_k'] * atr, 4)
        msg   = build_alert(current, entry, tp, sl, signal, '15m', cfg)
        send_telegram(msg)
        print(
            f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] "
            f"SIGNAL 15m (Swing) — entry={entry:.2f} TP={tp:.2f} SL={sl:.2f}"
        )
        return candle_ts
    else:
        reason = signal.get('logic_debug_note', '?')
        print(
            f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] "
            f"15m no-signal  gate={reason:<22s}  close={current['close']:.2f}"
        )
        return last_ts


def daily_summary(counts: dict) -> None:
    lines = "\n".join(
        f"  `{tf}` ({TIMEFRAMES[tf]['label']}): `{counts.get(tf, 0)}` signals"
        for tf in TIMEFRAMES
    )
    msg = (
        f"📅 *Daily summary* — {datetime.now(timezone.utc).strftime('%Y-%m-%d')}\n"
        f"{lines}\n"
        f"Bot is running normally ✅"
    )
    send_telegram(msg)


# ── Main loop ─────────────────────────────────────────────────────────────
def main():
    print("=" * 50)
    print("  ETH/USDT Signal Bot — 15m Swing + 1h Position")
    print("=" * 50)

    if not BOT_TOKEN or not CHAT_ID:
        print("\n[ERROR] TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID must be set in .env\n")
        return

    send_telegram(
        f"🤖 *Bot started*\n"
        f"Monitoring *{SYMBOL}*\n"
        f"  · `15m` Swing  (tp=1.0x ATR, sl=0.8x ATR)\n"
        f"  · `1h`  Position  (tp=0.7x ATR, sl=1.2x ATR)\n"
        f"Polling every `{POLL_SECS}s`"
    )

    last_alerted = {tf: "" for tf in TIMEFRAMES}
    daily_counts = {tf: 0  for tf in TIMEFRAMES}
    last_day     = datetime.now(timezone.utc).date()

    print("Bot running. Ctrl-C to stop.\n")

    while True:
        try:
            today = datetime.now(timezone.utc).date()
            if today != last_day:
                daily_summary(daily_counts)
                daily_counts = {tf: 0 for tf in TIMEFRAMES}
                last_day     = today

            new_ts = check_15m(TIMEFRAMES['15m'], last_alerted['15m'])
            if new_ts != last_alerted['15m']:
                daily_counts['15m'] += 1
                last_alerted['15m']  = new_ts

            new_ts = check_1h(TIMEFRAMES['1h'], last_alerted['1h'])
            if new_ts != last_alerted['1h']:
                daily_counts['1h'] += 1
                last_alerted['1h']  = new_ts

        except ccxt.NetworkError as e:
            print(f"[NETWORK] {e} — retrying in 60s")
            time.sleep(60)
            continue
        except ccxt.ExchangeError as e:
            print(f"[EXCHANGE] {e} — retrying in 120s")
            time.sleep(120)
            continue
        except Exception:
            print(f"[ERROR]\n{traceback.format_exc()}")
            time.sleep(60)
            continue

        time.sleep(POLL_SECS)


if __name__ == "__main__":
    main()
