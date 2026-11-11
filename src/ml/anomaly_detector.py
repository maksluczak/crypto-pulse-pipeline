import logging
import os
import joblib
import psycopg2
from pandas import DataFrame
from src.alerts.telegram import send_anomaly_alert
from src.config import SYMBOLS, MODELS_DIR
from src.logger import setup_logging
from src.ml.features import FEATURE_NAMES, make_features
from src.storage.db_client import get_previous_price, get_db_connection, save_candle, save_anomaly

logger = logging.getLogger(__name__)

models = {}

def load_models() -> int:
    for symbol in SYMBOLS:
        model_path = os.path.join(MODELS_DIR, f"{symbol}.joblib")
        if os.path.exists(model_path):
            models[symbol] = joblib.load(model_path)
            logger.info(f"Loaded {symbol} model")
        else:
            logger.info(f"No model for {symbol}: {model_path}")
    return len(models)

def is_anomaly(symbol: str, features: tuple) -> bool:
    model = models.get(symbol)
    if model is None:
        return False
    features_df = DataFrame([features], columns = FEATURE_NAMES)
    return model.predict(features_df)[0] == -1

def check_candle(conn: psycopg2.connect, candle: dict) -> dict | None:
    previous_price = get_previous_price(conn, candle["symbol"], candle["minute_start"])
    if previous_price is None:
        return None

    features = make_features(
        candle["avg_price"],
        previous_price,
        candle["min_price"],
        candle["max_price"],
        candle["volume"]
    )
    if not is_anomaly(candle["symbol"], features):
        return None

    return {
        "symbol": candle["symbol"],
        "minute_start": candle["minute_start"],
        "price": candle["avg_price"],
        "price_change_pct": features[0],
        "volume": candle["volume"]
    }

def process_batch(batch_df: DataFrame, batch_id: int) -> None:
    rows = batch_df.collect()
    if len(rows) == 0:
        return

    new_anomalies = []
    conn = get_db_connection()
    try:
        for row in rows:
            candle = row.asDict()
            save_candle(conn, candle)
            anomaly = check_candle(conn, candle)
            if anomaly is not None and save_anomaly(conn, anomaly):
                new_anomalies.append(anomaly)
        conn.commit()
    finally:
        conn.close()

    for anomaly in new_anomalies:
        logger.warning(f"Anomaly detected: {anomaly}")
        send_anomaly_alert(anomaly)

    logger.info(f"Batch {batch_id}: {len(rows)} candles, {len(new_anomalies)} anomalies")

if __name__ == "__main__":
    setup_logging()
    load_models()