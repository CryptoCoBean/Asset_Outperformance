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

tickers = binance.fetch_tickers()
symbol_list = list(tickers.keys())
    # print(symbol_list)

symbol_list_usdt_pairs = []
for x in range (0, len(symbol_list)):
    if "USDT" in symbol_list[x]:
        usdt_ticker = symbol_list[x].replace(":USDT", "")
        if "-" in usdt_ticker:
            pass
        else:
            symbol_usdt_pairs = usdt_ticker.replace("/", "")
            symbol_list_usdt_pairs.append(symbol_usdt_pairs)
            
    else:
        pass

limits = 1000
result_df = pd.DataFrame()
sizing = len(symbol_list_usdt_pairs)
print("Total assets: ", sizing)
for x in range(0, sizing):
    time.sleep(0.1)
    symbol = symbol_list_usdt_pairs[x]
    print(f"{x}: {symbol}")
    ohlcv = binance.fetch_ohlcv(symbol, timeframe='1d', limit=limits)

    if not ohlcv:
        continue
    
    else:

        # Convert to DataFrame
        df = pd.DataFrame(ohlcv, columns=[
            'timestamp', 'open', 'high', 'low', 'close', 'volume'
        ])

        # Convert timestamp
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')

        # =========================
        # CALCULATIONS
        # =========================

        # ATH
        ath_price = df['high'].max()

        # Time of ATH
        ath_row = df.loc[df['high'].idxmax()]
        ath_time = ath_row['timestamp']

        # Current price (last close)
        current_price = df['close'].iloc[-1]

        # Drawdown (%)
        drawdown_pct = ((current_price - ath_price) / ath_price) * 100

        # Time since ATH
        now = datetime.now(timezone.utc)
        time_since_ath = now - ath_time.to_pydatetime().replace(tzinfo=timezone.utc)

        df['volume_usdt'] = df['volume'] * df['close']
        latest_volume = df['volume_usdt'].iloc[-1]
        avg_volume_7d = df['volume_usdt'].tail(7).mean()

        # =========================
        # RESULT DATAFRAME
        # =========================
        single_result = pd.DataFrame([{
            'symbol': symbol,
            'ath_price': ath_price,
            'ath_time': ath_time,
            'current_price': current_price,
            'drawdown_%': drawdown_pct,
            'time_since_ath_days': time_since_ath.days,
            'time_since_ath_hours': time_since_ath.total_seconds() / 3600,
            'volume_latest': latest_volume,
            'usd_volume_7d_avg': avg_volume_7d
        }])

        result_df = pd.concat([result_df, single_result], ignore_index=True)

# =========================
# OUTPUT
# =========================
result_df_volume_sort = result_df.sort_values(by=['drawdown_%'], ascending=False)

# Export CSV
timestamp_str = now.strftime("%Y-%m-%d_%H-%M-%S")
filename = rf"{ps.drawdown_csv_output_path}{timestamp_str}_ath_drawdown.csv"
result_df_volume_sort.to_csv(filename, index=False)

print(f"\nSaved to {filename}")