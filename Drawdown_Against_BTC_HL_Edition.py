import ccxt
import pandas as pd
from datetime import datetime, timezone
import ps
import time
import fetch_ochlv_for_hl

# =========================
# INIT EXCHANGE
# =========================
exchange = ccxt.hyperliquid({
    "walletAddress": ps.HL_WALLET,
    "privateKey": ps.HL_PRIVATE_KEY,
    "enableRateLimit": True,
    "timeout": 10000,
    "options": {
        "defaultSlippage": 0.01,
    }
})

markets = exchange.load_markets()

# =========================
# FETCH SYMBOLS (SPOT + PERPS)
# =========================
symbol_list_all = [
    s for s, m in markets.items()
    if m.get('quote') == 'USDC'
    and m.get('active', True)
]

print(symbol_list_all)
print("Total assets:", len(symbol_list_all))
print()

# =========================
# FETCH BTC DATA (PERP BASE)
# =========================
limits = 1000

btc_ohlcv = fetch_ochlv_for_hl.fetch_ohlcv_hl_adaptation(
    'BTC',
    timeframe="1d",
    limit=limits
)

btc_df = pd.DataFrame(btc_ohlcv, columns=[
    'timestamp', 'open', 'high', 'low', 'close', 'volume'
])

btc_df['timestamp'] = pd.to_datetime(btc_df['timestamp'], unit='ms')

btc_df = btc_df[['timestamp', 'close']]
btc_df.rename(columns={'close': 'btc_close'}, inplace=True)

print(btc_df)

# =========================
# LOOP
# =========================
result_df = pd.DataFrame()
sizing = len(symbol_list_all)

print("Total assets:", sizing)

for x in range(0, sizing):
    time.sleep(0.1)

    symbol_full = symbol_list_all[x]
    print(f"{x}: {symbol_full}")

    try:
        market = markets[symbol_full]

        # =========================
        # FETCH DATA (HYBRID)
        # =========================
        if market.get('swap'):
            # PERP
            market_type = 'perp'
            base = symbol_full.split("/")[0]

            # handle HIP-3 style perps too
            if "-" in base:
                market_type = 'hip3'
                parts = base.split('-')
                base =  f"{parts[0].lower()}:{parts[1].upper()}"
                print(base)

            ohlcv = fetch_ochlv_for_hl.fetch_ohlcv_hl_adaptation(
                base,
                timeframe='1d',
                limit=limits
            )

        elif market.get('spot'):
            # SPOT
            market_type = 'spot'

            ohlcv = exchange.fetch_ohlcv(
                symbol_full,
                timeframe='1d',
                limit=limits
            )
            # continue

        else:
            continue

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

        # =========================
        # VOLUME METRICS
        # =========================
        merged['volume_usdt'] = merged['volume'] * merged['close']
        latest_volume = merged['volume_usdt'].iloc[-1]
        avg_volume_7d = merged['volume_usdt'].tail(7).mean()

        # =========================
        # STORE RESULT
        # =========================
        symbol_clean = symbol_full.split("/")[0]

        single_result = pd.DataFrame([{
            'symbol': symbol_clean,
            'market_type': market_type,
            'ath_asset_btc': ath_price,
            'ath_time': ath_time,
            'current_asset_btc': current_price,
            'drawdown_%': drawdown_pct,
            'time_since_ath_days': time_since_ath.days,
            'time_since_ath_hours': time_since_ath.total_seconds() / 3600,
            'volume_latest': latest_volume,
            'usd_volume_7d_avg': avg_volume_7d
        }])

        result_df = pd.concat([result_df, single_result], ignore_index=True)

    except Exception as e:
        print(f"Error with {symbol_full}: {e}")
        continue
    
    time.sleep(0.1)

print(result_df)

# =========================
# OUTPUT
# =========================
result_df_sorted = result_df.sort_values(by=['drawdown_%'], ascending=False)

timestamp_str = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
filename = rf"{ps.drawdown_csv_output_path}{timestamp_str}_HL_btc_drawdown.csv"

result_df_sorted.to_csv(filename, index=False)

print(f"\nSaved to {filename}")