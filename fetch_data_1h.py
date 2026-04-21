"""
fetch_data_1h.py
─────────────────────────────────────────────────────────────────────────────
Fetches ~1 year of 1h ETH/USDT candles from KuCoin (~8,760 candles).
Run this once to build your historical dataset for backtesting.
"""

from extracting import fetch_kucoin_candles_paginated

symbol    = 'ETH/USDT'
timeframe = '1h'

df = fetch_kucoin_candles_paginated(
    symbol=symbol,
    timeframe=timeframe,
    total_candles=9000          # ~1 year + buffer
)

df.to_csv('ethusdt_1h_1y.csv', index=False)
print(f"Rows fetched : {len(df)}")
print(f"Date range  : {df['timestamp'].min()}  →  {df['timestamp'].max()}")
print("Saved ethusdt_1h_1y.csv")
