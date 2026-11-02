import logging
import time
from datetime import datetime, timezone, timedelta
import requests
from src.config import SYMBOLS
from src.logger import setup_logging
from src.storage.db_client import get_db_connection, save_candle

logger = logging.getLogger(__name__)

BINANCE_KLINES_URL = "https://api.binance.com/api/v3/klines"

def download_klines(symbol: str, start_ms: int, end_ms: int) -> list:
    all_klines = []
    while start_ms < end_ms:
        params = {
            "symbol": symbol,
            "interval": "1m",
            "startTime": start_ms,
            "endTime": end_ms,
            "limit": 100
        }
        response = requests.get(BINANCE_KLINES_URL, params = params)
        response.raise_for_status()
        klines = response.json()
        if len(klines) == 0:
            break
        all_klines.extend(klines)
        start_ms = klines[-1][0] + 60 * 1000
        time.sleep(0.2)
    return all_klines

def kline_to_candle(symbol: str, kline: list) -> dict | None:
    # binance candle format = [opening_time, open, high, low, close, volume,
    #                          closing_time, quote_volume, num_of_transactions, ...]
    volume = float(kline[5])
    if volume <= 0:
        return None
    quote_volume = float(kline[7])
    minute_start = datetime.fromtimestamp(kline[0] / 1000, timezone.utc)
    return {
        "symbol": symbol,
        "minute_start": minute_start.strftime("%Y-%m-%d %H:%M:%S"),
        "avg_price": quote_volume / volume,
        "min_price": float(kline[3]),
        "max_price": float(kline[2]),
        "volume": volume,
        "trade_count": int(kline[8])
    }

def download_history(symbol: str, days: int) -> None:
    now = datetime.now(timezone.utc)
    end_ms = (int(now.timestamp() * 1000) - 60 * 1000)
    start_ms = int((now - timedelta(days = days)).timestamp() * 1000)

    klines = download_klines(symbol, start_ms, end_ms)

    conn = get_db_connection()
    try:
        saved = 0
        for kline in klines:
            candle = kline_to_candle(symbol, kline)
            if candle is not None:
                save_candle(conn, candle)
                saved += 1
        conn.commit()
    finally:
        conn.close()
    logger.info(f"{saved} candles saved")

if __name__ == "__main__":
    setup_logging()
    for symbol in SYMBOLS:
        download_history(symbol, days = 30)