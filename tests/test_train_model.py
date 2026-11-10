import numpy as np
import pandas as pd
from src.ml.features import FEATURE_NAMES
from src.ml.train_models import build_training_data, train_model

def make_candles(minutes: list, prices: list) -> pd.DataFrame:
    start = pd.Timestamp("2026-01-01 12:00:00")
    rows = []
    for minute, price in zip(minutes, prices):
        rows.append({
            "minute_start": start + pd.Timedelta(minute),
            "avg_price": price,
            "min_price": price - 1,
            "max_price": price + 1,
            "volume": 5.0
        })
    return pd.DataFrame(rows)

def test_first_candle_has_no_features():
    candles = make_candles([0, 1, 2], [100.0, 101.0, 102.0])
    assert len(build_training_data(candles)) == 2

def test_columns_match_feature_names():
    candles = make_candles([0, 1], [100.0, 101.0])
    assert list(build_training_data(candles).columns) == FEATURE_NAMES

def test_gap_in_time_is_skipped():
    candles = make_candles([0, 1, 60], [100.0, 101.0, 200.0])
    assert len(build_training_data(candles)) == 1

def test_model_marks_huge_jump_as_anomaly():
    rng = np.random.default_rng(42)
    training_data = pd.DataFrame({
        "price_change_pct": rng.normal(0, 0.05, 2000),
        "range_pct": rng.uniform(0.01, 0.2, 2000),
        "volume": rng.uniform(1, 20, 2000),
    }) [FEATURE_NAMES]
    model = train_model(training_data)
    huge_jump = pd.DataFrame([[5.0, 6.0, 500.0]], columns = FEATURE_NAMES)
    assert model.predict(huge_jump)[0] == -1