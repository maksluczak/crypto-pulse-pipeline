import os
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "54332")
DB_NAME = os.getenv("DB_NAME", "CryptoDb")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")

KAFKA_SERVER = os.getenv("KAFKA_SERVER", "kafka:9094")
TOPIC_TRADES = "crypto_trades"

SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]

MODELS_DIR = os.getenv("MODELS_DIR", "models")

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")