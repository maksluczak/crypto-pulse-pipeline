import logging
import os
import joblib
import psycopg2
from pandas import DataFrame
from src.config import SYMBOLS, MODELS_DIR
from src.logger import setup_logging
from src.ml.features import FEATURE_NAMES, make_features
from src.storage.db_client import get_previous_price

logger = logging.getLogger(__name__)

PRICE_JUMP_PCT = 1.0
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

def is_price_jump(price_change_pct: float) -> bool:
    return abs(price_change_pct) >= PRICE_JUMP_PCT

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

    if not is_price_jump(features[0]) and not is_anomaly(candle["symbol"], features):
        return None

    return {
        "symbol": candle["symbol"],
        "minute_start": candle["minute_start"],
        "price": candle["avg_price"],
        "price_change_pct": features[0],
        "volume": candle["volume"]
    }

if __name__ == "__main__":
    setup_logging()