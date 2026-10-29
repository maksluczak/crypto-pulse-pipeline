import asyncio
import json
import logging
import websockets
from kafka import KafkaProducer
from src.config import KAFKA_SERVER, SYMBOLS, TOPIC_TRADES
from src.logger import setup_logging

logger = logging.getLogger(__name__)

def build_binance_url(symbols: list) -> str:
    streams = []
    for symbol in symbols:
        streams.append(symbol.lower() + "@aggTrade")
    return "wss://stream.binance.com:9443/stream?streams=" + "/".join(streams)

def create_kafka_producer() -> KafkaProducer:
    producer = KafkaProducer(
        bootstrap_servers = [KAFKA_SERVER],
        value_serializer = lambda v: json.dumps(v).encode('utf-8')
    )
    return producer

def parse_trade(raw_message: str) -> dict | None:
    try:
        data = json.loads(raw_message)["data"]
        trade = {
            "symbol": str(data["s"]),
            "price": float(data["p"]),
            "quantity": float(data["q"]),
            "trade_time": int(data["T"])
        }

        if trade["price"] <= 0 or trade["quantity"] <= 0:
            return None
        return trade
    except (KeyError, ValueError, TypeError) as e:
        logger.warning(f"Invalid message: {e}")
        return None

def send_to_kafka(producer: KafkaProducer, trade: dict) -> None:
    producer.send(TOPIC_TRADES, key = trade["symbol"], value = trade)

async def run_producer_loop() -> None:
    url = build_binance_url(SYMBOLS)
    producer = create_kafka_producer()
    wait_seconds = 1
    send_count = 0

    while True:
        try:
            async with websockets.connect(url) as websocket:
                logger.info("Connected to Binance API")
                while True:
                    raw_message = await websocket.recv()
                    trade = parse_trade(raw_message)
                    if trade is None:
                        continue
                    send_to_kafka(producer, trade)
                    send_count += 1
                    if send_count % 1000 == 0:
                        logger.info(f"Sent {send_count} trades")
        except Exception as e:
            logger.error(
                f"Failed to connect to Binance API: {e}. Trying again in {wait_seconds} seconds."
            )
            await asyncio.sleep(wait_seconds)
            wait_seconds = min(wait_seconds * 2, 60)

if __name__ == "__main__":
    setup_logging()
    try:
        asyncio.run(run_producer_loop())
    except KeyboardInterrupt:
        logger.info("Shutting down")
        pass