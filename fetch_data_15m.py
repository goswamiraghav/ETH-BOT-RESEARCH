from extracting import fetch_kucoin_candles_paginated

symbol = 'ETH/USDT'
timeframe = '15m'

df = fetch_kucoin_candles_paginated(
    symbol=symbol,
    timeframe=timeframe,
    total_candles=35000
)

df.to_csv('ethusdt_15m_1y.csv', index=False)
print("Rows fetched:", len(df))
print("Saved ethusdt_15m_1y.csv")