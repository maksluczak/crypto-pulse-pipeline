import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor

from src.config import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD

def get_db_connection() -> psycopg2.connect:
    return psycopg2.connect(
        host = DB_HOST,
        port = DB_PORT,
        database = DB_NAME,
        user = DB_USER,
        password = DB_PASSWORD
    )

def save_candle(conn: psycopg2.connect, candle: dict):
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO crypto_candles (symbol, minute_start, avg_price, min_price, max_price, volume, trade_count)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (symbol, minute_start) DO UPDATE SET
            avg_price = EXCLUDED.avg_price,
            min_price = EXCLUDED.min_price,
            max_price = EXCLUDED.max_price,
            volume = EXCLUDED.volume,
            trade_count = EXCLUDED.trade_count;
    """, (
        candle["symbol"],
        candle["minute_start"],
        candle["avg_price"],
        candle["min_price"],
        candle["max_price"],
        candle["volume"],
        candle["trade_count"]
    ))
    cursor.close()

def get_previous_price(conn: psycopg2.connect, symbol: str, minute_start: str) -> float | None:
    cursor = conn.cursor()
    cursor.execute("""
        SELECT avg_price FROM crypto_candles
        WHERE symbol = %s
        AND minute_start = %s
        ORDER BY avg_price DESC
        LIMIT 1;
    """, (
        symbol,
        minute_start
    ))
    row = cursor.fetchone()
    cursor.close()
    if row:
        return float(row[0])
    else:
        return None

def save_anomaly(conn: psycopg2.connect, anomaly: dict) -> bool:
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO crypto_anomalies (symbol, minute_start, price, price_change_pct, volume) 
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (symbol, minute_start) DO NOTHING;
    """, (
        anomaly["symbol"],
        anomaly["minute_start"],
        anomaly["price"],
        anomaly["price_change_pct"],
        anomaly["volume"]
    ))
    is_new_anomaly = cursor.rowcount == 1
    cursor.close()
    return is_new_anomaly

def get_candles_for_training(conn: psycopg2.connect, symbol: str, days: int) -> pd.DataFrame:
    cursor = conn.cursor()
    cursor.execute("""
        SELECT minute_start, avg_price, min_price, max_price, volume FROM crypto_candles
        WHERE symbol = %s AND minute_start >= NOW() - INTERVAL '1 day' * %s
        ORDER BY minute_start DESC;
    """, (
        symbol,
        days
    ))
    rows = cursor.fetchall()
    cursor.close()
    columns = ["minute_start", "avg_price", "min_price", "max_price", "volume"]
    return pd.DataFrame.from_records(rows, columns = columns)

def get_recent_candles(conn: psycopg2.connect, symbol: str, minutes: int) -> pd.DataFrame:
    cursor = conn.cursor()
    cursor.execute("""
        SELECT minute_start, avg_price, min_price, max_price, volume, trade_count FROM crypto_candles
        WHERE symbol = %s AND minute_start >= NOW() - INTERVAL '1 minute' * %s
        ORDER BY minute_start DESC;
    """, (
        symbol,
        minutes
    ))
    rows = cursor.fetchall()
    cursor.close()
    return rows

def get_recent_anomalies(conn: psycopg2.connect, limit: int) -> list:
    cursor = conn.cursor(cursor_factory = RealDictCursor)
    cursor.execute("""
        SELECT * FROM crypto_anomalies ORDER BY detected_at DESC LIMIT %s
    """, (
        limit
    ))
    rows = cursor.fetchall()
    cursor.close()
    return rows