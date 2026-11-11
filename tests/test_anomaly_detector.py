from src.ml import anomaly_detector
from src.ml.anomaly_detector import check_candle, is_anomaly

class FakeModel:
    def __init__(self, prediction):
        self.prediction = prediction

    def predict(self, features_df):
        return [self.prediction]

CANDLE = {
    "symbol": "BTCUSDT",
    "minute_start": "2026-01-01 12:00:00",
    "avg_price": 110.0,
    "min_price": 100.0,
    "max_price": 112.0,
    "volume": 5.0,
    "trade_count": 3
}

def test_no_model_means_no_anomaly():
    anomaly_detector.models.clear()

    assert is_anomaly("BTCUSDT", (1.0, 1.0, 1.0)) is False

def test_model_prediction_minus_one_is_anomaly():
    anomaly_detector.models["BTCUSDT"] = FakeModel(-1)

    assert is_anomaly("BTCUSDT", (1.0, 1.0, 1.0)) == True

def test_check_candle_without_previous_price(monkeypatch):
    monkeypatch.setattr(anomaly_detector, "get_previous_price", lambda c, s, m: None)

    assert check_candle(None, CANDLE) is None

def test_check_candle_returns_anomaly(monkeypatch):
    anomaly_detector.models["BTCUSDT"] = FakeModel(-1)
    monkeypatch.setattr(anomaly_detector, "get_previous_price", lambda c, s, m: 100.0)
    anomaly = check_candle(None, CANDLE)

    assert anomaly["price"] == 110.0
    assert round(anomaly["price_change_pct"], 2) == 10.0