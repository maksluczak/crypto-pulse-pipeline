from src.alerts import telegram
from src.alerts.telegram import format_alert_message, should_send_alert

def test_message_contains_important_info():
    anomaly = {
        "symbol": "BTCUSDT",
        "minute_start": "2026-01-01 12:00:00",
        "price": 67000.12,
        "price_change_pct": 2.5,
        "volume": 10
    }
    message = format_alert_message(anomaly)

    assert "BTCUSDT" in message
    assert "67000.12" in message
    assert "+2.50%" in message

def test_first_alert_is_allowed():
    telegram.last_alert_time.clear()

    assert should_send_alert("BTCUSDT", 1000.0) is True

def test_alert_blocked_during_cooldown():
    telegram.last_alert_time["BTCUSDT"] = 1000.0

    assert should_send_alert("BTCUSDT", 1100.0) is False

def test_alert_allowed_after_cooldown():
    telegram.last_alert_time["BTCUSDT"] = 1000.0

    assert should_send_alert("BTCUSDT", 1600.0) is True