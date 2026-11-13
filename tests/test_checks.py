from datetime import datetime
from src.monitoring.checks import build_daily_summary, find_stale_symbols

NOW = datetime(2026, 1, 1, 12, 0)

def test_fresh_data_is_not_stale():
    assert find_stale_symbols([("BTCUSDT", datetime(2026, 1, 1, 11, 55))], NOW) == []

def test_old_data_is_stale():
    rows = [("BTCUSDT", datetime(2026, 1, 1, 11, 55)), ("ETHUSDT", datetime(2026, 1, 1, 10, 0))]

    assert find_stale_symbols(rows, NOW) == ["ETHUSDT"]

def test_summary_without_anomalies():
    assert "No anomalies" in build_daily_summary([])

def test_summary_with_anomalies():
    assert "BTCUSDT: 2" in build_daily_summary([("BTCUSDT", 2)])