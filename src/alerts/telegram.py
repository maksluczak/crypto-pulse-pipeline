import logging
import time
import requests
from src.config import TELEGRAM_TOKEN, TELEGRAM_CHAT_ID
from src.logger import setup_logging

logger = logging.getLogger(__name__)

COOLDOWN_SECONDS = 600
last_alert_time = {}

def format_alert_message(anomaly: dict) -> str:
    return (
        f"Symbol: {anomaly['symbol']}\n"
        f"Time: {anomaly['minute_start']} UTC\n"
        f"Price: {anomaly['price']:.2f}\n"
        f"Price Change: {anomaly['price_change_pct']:+.2f}%"
    )

def should_send_alert(symbol: str, now: float) -> bool:
    last_time = last_alert_time.get(symbol)
    if last_time is None:
        return True
    return now - last_time >= COOLDOWN_SECONDS

def send_telegram_message(text: str) -> bool:
    if TELEGRAM_TOKEN == "" or TELEGRAM_CHAT_ID == "":
        logger.warning("Telegram is not configured, skipping message")
        return False

    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage?chat_id={TELEGRAM_CHAT_ID}&text={text}"
    try:
        response = requests.post(url, timeout = 10)
        response.raise_for_status()
        return True
    except requests.RequestException as e:
        logger.error(f"Telegram request failed: {type(e).__name__}")
        return False

def send_anomaly_alert(anomaly: dict) -> None:
    text_message = format_alert_message(anomaly)
    symbol = anomaly['symbol']
    now = time.time()

    if not should_send_alert(symbol, now):
        logger.info(f"Skipping alert for {symbol}, cooldown is active")
        return
    if send_telegram_message(text_message):
        last_alert_time[symbol] = now

if __name__ == "__main__":
    setup_logging()