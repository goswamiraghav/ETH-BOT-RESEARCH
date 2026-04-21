from extracting import fetch_kucoin_candles_paginated

symbol = 'ETH/USDT'
timeframe = '5m'

df = fetch_kucoin_candles_paginated(
    symbol=symbol,
    timeframe=timeframe,
    total_candles=105000
)

df.to_csv('ethusdt_5m_1y.csv', index=False)
print("Rows fetched:", len(df))
print("Saved ethusdt_5m_1y.csv")