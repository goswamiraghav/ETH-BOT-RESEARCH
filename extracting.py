# extracting.py

"""
def fetch_kucoin_candles(symbol='ETH/USDT', timeframe='1m', limit=3000):
    kucoin = ccxt.kucoin()
    ohlcv = kucoin.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
    df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
    df['symbol'] = symbol
    df['exchange'] = 'KuCoin'
    df['interval'] = timeframe
    return df  # <<< returning the DataFrame, not printing
#the function fetch kucoin candles gets called in main.py
# 1. Connect to KuCoin exchange

# 2. Fetch last 100 1-minute candles for ETH/USDT
#kucoin.fetch_ohlcv(...) is a function from ccxt.
#fetch_ohlcv means "fetch historical candlestick data."
#OHLCV" stands for:

#Open → price at start of the minute

#High → highest price during the minute

#Low → lowest price during the minute

#Close → price at end of the minute

#Volume → amount of ETH traded during the minute
#market-'ETH/USDT'
#timefram=1m(1m Candles)
#limit 100(How many candles, in this case, last 100 minutes)


# 3. Convert raw data to pandas DataFrame

#You take the list of lists returned by fetch_ohlcv.
#You load it into a pandas DataFrame
#You manually specify the column names
#
#

# 4. Convert timestamp from milliseconds to human-readable datetime

# 5. Add extra fields to match BigQuery table


# 6. Show the final DataFrame



#if you wanna test out the function above
#df=fetch_kucoin_candles()
#print(df.tail())

'''import ccxt
import pandas as pd
from datetime import datetime

# 1. Connect to KuCoin
kucoin = ccxt.kucoin()

# 2. Fetch last 100 1-minute candles for ETH/USDT
ohlcv = kucoin.fetch_ohlcv('ETH/USDT', timeframe='1m', limit=100)

# 3. Convert to pandas DataFrame
df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])

# 4. Convert timestamp
df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')

# 5. Add symbol, exchange, interval
df['symbol'] = 'ETH/USDT'
df['exchange'] = 'KuCoin'
df['interval'] = '1m'

# 6. Add EMA indicators
df['ema_9'] = df['close'].ewm(span=9, adjust=False).mean()
df['ema_20'] = df['close'].ewm(span=20, adjust=False).mean()

# 7. Show final DataFrame
print(df.tail())
'''
"""

# the above code is limited to fetching 1500 rows, we need more historical data for backtesting which is what we will do next:
"""
 Use Pagination with since Parameter
"""
import ccxt
import pandas as pd
import time

def fetch_kucoin_candles_paginated(symbol, timeframe='5m', total_candles=10000):
    exchange = ccxt.kucoin()

    timeframe_ms = exchange.parse_timeframe(timeframe) * 1000
    now = exchange.milliseconds()

    # Start far enough in the past
    since = now - (total_candles * timeframe_ms)

    all_ohlcv = []
    limit = 1500

    try:
        while len(all_ohlcv) < total_candles:
            batch = exchange.fetch_ohlcv(symbol, timeframe=timeframe, since=since, limit=limit)

            if not batch:
                break

            all_ohlcv.extend(batch)

            # move forward from the last candle returned
            since = batch[-1][0] + timeframe_ms

            print(f"Fetched {len(all_ohlcv)} candles so far...")
            time.sleep(0.5)

            if len(batch) < limit:
                break
    except Exception as e:
        print("Stopped early due to error:", e)
        print(f"Saving partial data: {len(all_ohlcv)} candles")

    # remove duplicates just in case
    df = pd.DataFrame(all_ohlcv, columns=[
        'timestamp', 'open', 'high', 'low', 'close', 'volume'
    ]).drop_duplicates(subset='timestamp')

    df = df.iloc[:total_candles].copy()
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
    return df