import time
from typing import List, Optional

from hyperliquid.info import Info
from hyperliquid.utils import constants

# create Info client (skip websocket)
info = Info(constants.MAINNET_API_URL,perp_dexs=['','xyz','flx','vntl','hyna','km','cash','para'], skip_ws=True)


def _parse_timeframe_ms(tf: str) -> int:
    """Parse timeframe like '1m','5m','15m','1h','1d' -> milliseconds"""
    unit = tf[-1]
    value = int(tf[:-1])
    if unit == "m":
        return value * 60 * 1000
    if unit == "h":
        return value * 60 * 60 * 1000
    if unit == "d":
        return value * 24 * 60 * 60 * 1000
    if unit == "w":
        return value * 7 * 24 * 60 * 60 * 1000
    raise ValueError(f"Unsupported timeframe: {tf}")


def fetch_ohlcv_hl_adaptation(
    name: str,
    timeframe: str = "1m",
    since: Optional[int] = None,
    limit: int = 100,
) -> List[List[float]]:
    """
    Returns a list of [timestamp_ms, open, high, low, close, volume]
    - name: coin name used by the SDK (e.g. "BTC")
    - timeframe: "1m", "5m", "1h", "1d", ...
    - since: optional start timestamp in milliseconds (inclusive)
    - limit: max number of candles to return
    """
    interval_ms = _parse_timeframe_ms(timeframe)
    now_ms = int(time.time() * 1000)

    # If user provided since, get candles from since -> since + limit*interval
    # Otherwise fetch the most recent `limit` candles.
    if since is not None:
        start_ms = int(since)
        end_ms = start_ms + limit * interval_ms
    else:
        end_ms = now_ms
        start_ms = end_ms - limit * interval_ms

    # Make the request to the SDK -> this calls the /info candleSnapshot endpoint.
    candles = info.candles_snapshot(name, timeframe, start_ms, end_ms)

    # The SDK returns candle dicts with keys like:
    # t (open time ms), T (close time ms), o,h,l,c (strings), v (string)
    # Convert to numeric CCXT-like rows: [timestamp, open, high, low, close, volume]
    ohlcv = []
    for c in candles:
        ts = int(c["t"])
        o = float(c["o"])
        h = float(c["h"])
        l = float(c["l"])
        cl = float(c["c"])
        v = float(c["v"])
        ohlcv.append([ts, o, h, l, cl, v])

    # Ensure sorted oldest -> newest
    ohlcv.sort(key=lambda x: x[0])
    # Return at most `limit` rows (edge-case protection)
    return ohlcv[-limit:]

