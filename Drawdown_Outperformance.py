import ccxt
import pandas as pd
from datetime import datetime, timezone
import ps

# =========================
# CONFIG
# =========================
SYMBOL = 'BTC/USDT'
TIMEFRAME = '1d'   # you can change (1h, 4h, etc.)
LIMIT = 1000       # max candles per fetch

# =========================
# INIT EXCHANGE
# =========================
exchange = ccxt.binanceusdm({
    'enableRateLimit': True,
})

# =========================
# FETCH DATA
# =========================
ohlcv = exchange.fetch_ohlcv(SYMBOL, timeframe=TIMEFRAME, limit=LIMIT)

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

# =========================
# RESULT DATAFRAME
# =========================
result = pd.DataFrame([{
    'symbol': SYMBOL,
    'ath_price': ath_price,
    'ath_time': ath_time,
    'current_price': current_price,
    'drawdown_%': drawdown_pct,
    'time_since_ath_days': time_since_ath.days,
    'time_since_ath_hours': time_since_ath.total_seconds() / 3600
}])

# =========================
# OUTPUT
# =========================
print(result)

# Export CSV
filename = f"{SYMBOL.replace('/', '_')}_ath_drawdown.csv"
result.to_csv(filename, index=False)

print(f"\nSaved to {filename}")