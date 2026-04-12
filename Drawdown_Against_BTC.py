import ccxt
import pandas as pd
from datetime import datetime, timezone
import ps
import time

exchange_id = 'binanceusdm'
exchange_class = getattr(ccxt, exchange_id)

binance = exchange_class({
    'apiKey': ps.binance_apiKey,
    'secret': ps.binance_secret,
    'enableRateLimit': True,
})

# =========================
# FETCH SYMBOLS
# =========================
tickers = binance.fetch_tickers()
symbol_list = list(tickers.keys())

symbol_list_usdt_pairs = []
for x in range(0, len(symbol_list)):
    if "USDT" in symbol_list[x]:
        usdt_ticker = symbol_list[x].replace(":USDT", "")
        if "-" in usdt_ticker:
            pass
        else:
            symbol_usdt_pairs = usdt_ticker.replace("/", "")
            symbol_list_usdt_pairs.append(symbol_usdt_pairs)
    else:
        pass

# =========================
# FETCH BTC DATA
# =========================
limits = 1000

btc_ohlcv = binance.fetch_ohlcv('BTCUSDT', timeframe='1d', limit=limits)

btc_df = pd.DataFrame(btc_ohlcv, columns=[
    'timestamp', 'open', 'high', 'low', 'close', 'volume'
])

btc_df['timestamp'] = pd.to_datetime(btc_df['timestamp'], unit='ms')

# Keep only needed column
btc_df = btc_df[['timestamp', 'close']]
btc_df.rename(columns={'close': 'btc_close'}, inplace=True)

# =========================
# LOOP
# =========================
result_df = pd.DataFrame()
sizing = len(symbol_list_usdt_pairs)

print("Total assets:", sizing)

for x in range(0, sizing):
    time.sleep(0.1)
    symbol = symbol_list_usdt_pairs[x]
    print(f"{x}: {symbol}")

    try:
        ohlcv = binance.fetch_ohlcv(symbol, timeframe='1d', limit=limits)

        if not ohlcv:
            continue

        df = pd.DataFrame(ohlcv, columns=[
            'timestamp', 'open', 'high', 'low', 'close', 'volume'
        ])

        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')

        # =========================
        # ALIGN WITH BTC
        # =========================
        merged = pd.merge(df, btc_df, on='timestamp', how='inner')

        if merged.empty:
            continue

        # =========================
        # CREATE ASSET/BTC SERIES
        # =========================
        merged['asset_btc_close'] = merged['close'] / merged['btc_close']

        # (Optional but better ATH accuracy)
        merged['asset_btc_high'] = merged['high'] / merged['btc_close']

        # =========================
        # CALCULATIONS
        # =========================
        ath_price = merged['asset_btc_high'].max()

        ath_row = merged.loc[merged['asset_btc_high'].idxmax()]
        ath_time = ath_row['timestamp']

        current_price = merged['asset_btc_close'].iloc[-1]

        drawdown_pct = ((current_price - ath_price) / ath_price) * 100

        now = datetime.now(timezone.utc)
        time_since_ath = now - ath_time.to_pydatetime().replace(tzinfo=timezone.utc)

        single_result = pd.DataFrame([{
            'symbol': symbol,
            'ath_asset_btc': ath_price,
            'ath_time': ath_time,
            'current_asset_btc': current_price,
            'drawdown_%': drawdown_pct,
            'time_since_ath_days': time_since_ath.days,
            'time_since_ath_hours': time_since_ath.total_seconds() / 3600
        }])

        result_df = pd.concat([result_df, single_result], ignore_index=True)

    except Exception as e:
        print(f"Error with {symbol}: {e}")
        continue

# =========================
# OUTPUT
# =========================

# ✅ LEAST REKT FIRST (closest to ATH)
result_df_sorted = result_df.sort_values(by=['drawdown_%'], ascending=False)

timestamp_str = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
filename = rf"{ps.drawdown_csv_output_path}{timestamp_str}_btc_drawdown.csv"

result_df_sorted.to_csv(filename, index=False)

print(f"\nSaved to {filename}")