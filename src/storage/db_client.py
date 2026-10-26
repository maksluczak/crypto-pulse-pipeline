import pandas as pd
import psycopg2

DB_NAME = "CryptoDb"
DB_USER = "postgres"
DB_PASS = "postgres"
DB_HOST = "localhost"
DB_PORT = "54332"

def get_db_connection() -> psycopg2.connect:
    try:
        conn = psycopg2.connect(
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS,
            host=DB_HOST,
            port=DB_PORT
        )
        print("Database connection established")
        return conn
    except Exception as e:
        print("Database connection failed")
        print(e)

def init_db_tables(conn: psycopg2.connect) -> None:
    if not conn:
        return

    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS crypto_prices
        (
            id INT PRIMARY KEY NOT NULL,
            symbol TEXT NOT NULL,
            price REAL NOT NULL,
            volume REAL NOT NULL,
            timestamp TIMESTAMP NOT NULL
        );
    """)

    cursor.execute("""
            CREATE TABLE IF NOT EXISTS crypto_anomalies
            (
                id INT PRIMARY KEY NOT NULL,
                symbol TEXT NOT NULL,
                price REAL NOT NULL,
                volume,
                detected_at TIMESTAMP NOT NULL
            );
        """)

    conn.commit()
    cursor.close()

def save_anomaly_record(conn: psycopg2.connect, anomaly_data: dict) -> bool:
    if not conn:
        return False

    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO crypto_anomalies (symbol, price, volume, detected_at) 
            VALUES (%s, %s, %s, %s);
        """ % (
            anomaly_data["symbol"],
            anomaly_data["price"],
            anomaly_data.get("volume", 0.0),
            anomaly_data["detected_at"]
        ))

        conn.commit()
        cursor.close()
        return True
    except Exception as e:
        print(e)
        conn.rollback()
        cursor.close()
        return False

def fetch_historical_data(conn: psycopg2.connect, symbol: str, limit: int = 100) -> pd.DataFrame:
    if not conn:
        return pd.DataFrame()

    query = """
        SELECT symbol, price, volume, detected_at FROM crypto_prices
        WHERE symbol = %s
        ORDER BY timestamp DESC
        LIMIT %s;
    """

    df = pd.read_sql(query, conn, params = (symbol, limit))
    return df