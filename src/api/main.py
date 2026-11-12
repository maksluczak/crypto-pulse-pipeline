from fastapi import FastAPI, HTTPException
from src.config import SYMBOLS
from src.storage.db_client import get_db_connection, get_recent_candles, get_recent_anomalies

app = FastAPI(title = "Crypto Pulse Pipeline", version = "1.0")

@app.get("/health")
def health_check() -> dict:
    return { "status": "ok" }

@app.get("/symbols")
def get_symbols() -> list:
    return SYMBOLS

@app.get("/prices/{symbol}")
def get_prices(symbol: str, minutes: int = 60) -> list | None:
    symbol = symbol.upper()
    if symbol not in SYMBOLS:
        raise HTTPException(status_code = 404, detail = "Unknown symbol: " + symbol)

    if minutes < 1 or minutes > 1440:
        raise HTTPException(status_code = 400, detail = "Minutes must be between 1 and 1440")

    conn = get_db_connection()
    try:
        prices = get_recent_candles(conn, symbol, minutes)
        return prices
    finally:
        conn.close()

@app.get("/anomalies")
def get_anomalies(limit: int = 20) -> list | None:
    if limit < 1 or limit > 200:
        raise HTTPException(status_code = 400, detail = "Limit must be between 1 and 200")

    conn = get_db_connection()
    try:
        anomalies = get_recent_anomalies(conn, limit)
        return anomalies
    finally:
        conn.close()