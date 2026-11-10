import logging
import os
import joblib
import psycopg2
from pandas import DataFrame, Timedelta
from sklearn.ensemble import IsolationForest
from src.config import MODELS_DIR
from src.ml.features import make_features, FEATURE_NAMES
from src.storage.db_client import get_candles_for_training

logger = logging.getLogger(__name__)

MIN_CANDLES = 1000

def build_training_data(candles_df: DataFrame) -> DataFrame:
    previous_candle = None
    rows = []
    for _, candle in candles_df.iterrows():
        if previous_candle is None:
            previous_candle = candle
        else:
            time_gap = previous_candle["minute_start"] - candle["minute_start"]
            if time_gap == Timedelta(minutes = 1):
                candle_feature = make_features(
                    candle["avg_price"],
                    previous_candle["avg_price"],
                    candle["min_price"],
                    candle["max_price"],
                    candle["volume"]
                )
                rows.append(candle_feature)
            previous_candle = candle
    return DataFrame(rows, columns = FEATURE_NAMES)

def train_model(training_data: DataFrame) -> IsolationForest:
    model = IsolationForest(
        n_estimators = 100,
        contamination = 0.001,
        random_state = 42
    )
    model.fit(training_data)
    return model

def save_model(model: IsolationForest, symbol: str) -> str:
    os.makedirs(MODELS_DIR, exist_ok = True)
    model_path = os.path.join(MODELS_DIR, f"{symbol}.joblib")
    joblib.dump(model, model_path)
    return model_path

def train_model_for_symbol(symbol: str, days: int = 30) -> bool | None:
    conn = psycopg2.connect()
    try:
        candles_df = get_candles_for_training(conn, symbol, days)
    finally:
        conn.close()

    if len(candles_df) < MIN_CANDLES:
        logger.warning(f"{symbol}: too little candles found ({len(candles_df)}), skipping")
        return False

    training_data = build_training_data(candles_df)
    model = train_model(training_data)
    model_path = save_model(model, symbol)

    anomalies_count = (model.predict(training_data) == -1).sum()
    logger.info(f"{symbol}: saved {model_path}, {anomalies_count} anomalies in {days} days")
    return True