# ETH/USDT Trading Signal Pipeline

A multi-timeframe signal pipeline for ETH/USDT built on KuCoin data. Covers the full workflow from raw candle fetching through indicator computation, signal generation, backtesting, and live Telegram alerts.

---

## How it works

```
fetch_data_*.py  →  transformation_v2.py  →  signal_generator_*.py
                                                      ↓
                                          backtester_clean.py / run_backtest_v2.py
                                                      ↓
                                          telegram_bot.py  (live alerts)
```

Three independent strategies run in parallel:

| Timeframe | Style    | Signal file               |
|-----------|----------|---------------------------|
| 5m        | Scalp    | `signal_generator_5m.py`  |
| 15m       | Swing    | `signal_generator_15m.py` |
| 1h        | Position | `signal_generator_v2.py`  |

---

## File descriptions

### Data fetching
| File | What it does |
|------|--------------|
| `extracting.py` | Low-level utility — paginates KuCoin's REST API to pull arbitrarily long candle history into a DataFrame |
| `fetch_data.py` | Fetches ~105,000 5m candles (~1 year) and saves to `ethusdt_5m_1y.csv` |
| `fetch_data_15m.py` | Fetches ~35,000 15m candles (~1 year) and saves to `ethusdt_15m_1y.csv` |
| `fetch_data_1h.py` | Fetches ~9,000 1h candles (~1 year) and saves to `ethusdt_1h_1y.csv` |

### Indicators
| File | What it does |
|------|--------------|
| `transformation_v2.py` | Adds all technical indicators to a raw OHLCV DataFrame: EMA9/20/50/200, MACD, RSI, ATR, VWAP, and volume ratio |

### Signal generators
| File | What it does |
|------|--------------|
| `signal_generator_5m.py` | 6-gate scalp signal (EMA trend, MACD, RSI 45–65, volume ≥1.5x, strong close, meaningful body) |
| `signal_generator_15m.py` | 9-gate swing signal with 1h trend confirmation (close > EMA20 on 1h, MACD on 1h, EMA9 > EMA20 on 15m, volume ≥1.3x, ATR size, EMA20 pullback, RSI 45–70, strong close, body > 0.25× ATR) |
| `signal_generator_v2.py` | 1h position signal — evaluates longer-horizon momentum and trend conditions |

### Backtesting
| File | What it does |
|------|--------------|
| `backtester_clean.py` | Core backtesting engine — walks a DataFrame candle by candle, fires the signal function on each window, simulates trades with take-profit, trailing stop-loss, timeout exits, and fees |
| `run_backtest_v2.py` | Orchestrates backtests across all three timeframes, joins 1h trend columns onto 15m data, and writes trade logs + a summary table to `paper_outputs/` |
| `prepare_signals.py` | Runs signal logic row-by-row on historical 1h data and saves the full annotated DataFrame (with all indicator columns and gate results) to `paper_outputs/signals_1h.csv` |

### Analysis
| File | What it does |
|------|--------------|
| `run_sql_analysis.py` | Runs DuckDB SQL queries from `sql/signal_analysis.sql` against the CSVs in `paper_outputs/` and prints results to the terminal |

### Live bot
| File | What it does |
|------|--------------|
| `telegram_bot.py` | Polls KuCoin every 60 seconds, evaluates both the 15m swing and 1h position signals on fresh candles, and sends a Telegram alert with entry / TP / SL when all gates pass |
| `test_telegram.py` | One-shot connectivity check — sends a test message to your Telegram bot to confirm the token and chat ID are working |

---

## Setup

### 1. Install dependencies

```bash
pip install pandas ccxt requests python-dotenv
```

For the SQL analysis step:
```bash
pip install duckdb
```

### 2. Configure credentials

Copy the example env file and fill in your values:

```bash
cp .env.example .env   # or create .env manually
```

`.env` should contain:
```
TELEGRAM_BOT_TOKEN=your_bot_token_here
TELEGRAM_CHAT_ID=your_chat_id_here
```

To get these:
1. Message `@BotFather` on Telegram → `/newbot` → copy the token
2. Message your new bot, then open `https://api.telegram.org/bot<TOKEN>/getUpdates` and copy the `id` field from the `chat` object

### 3. Fetch historical data

Run once to build local CSV datasets for backtesting:

```bash
python fetch_data.py        # 5m  — ethusdt_5m_1y.csv
python fetch_data_15m.py    # 15m — ethusdt_15m_1y.csv
python fetch_data_1h.py     # 1h  — ethusdt_1h_1y.csv
```

### 4. Run backtests

```bash
python run_backtest_v2.py
```

Results are written to `paper_outputs/`.

### 5. Run the live bot

```bash
python telegram_bot.py
```

---

## Notes

- **CSV data files are not included in this repo.** They are generated locally by the fetch scripts and excluded via `.gitignore`. Re-run the fetch scripts to rebuild them.
- `paper_outputs/` is also excluded from the repo — regenerate with `run_backtest_v2.py` or `prepare_signals.py`.
- The bot uses KuCoin's public API for live candles — no API key is needed for read-only market data.
