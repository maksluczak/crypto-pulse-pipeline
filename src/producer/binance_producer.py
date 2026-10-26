import asyncio
import json
import websockets
from kafka import KafkaProducer
from kafka.errors import KafkaError

TOPIC_NAME = 'crypto-raw-prices'

def create_kafka_producer() -> KafkaProducer:
    try:
        producer = KafkaProducer(
            bootstrap_servers = ['localhost:9092'],
            value_serializer = lambda v: json.dumps(v).encode('utf-8')
        )
        return producer
    except Exception as ex:
        raise ex

def parse_ticker_data(raw_message: str) -> dict:
    parsed_data = json.loads(raw_message)
    return {
        "symbol": str(parsed_data["data"]["s"]).upper(),
        "price": float(parsed_data["data"]["c"]),
        "volume": float(parsed_data["data"]["v"]),
        "timestamp": int(parsed_data["data"]["T"])
    }

def send_to_kafka(producer: KafkaProducer, topic_name: str, data: dict) -> None:
    try:
        producer.send(topic_name, data)
        producer.flush()
    except KafkaError as e:
        print(e)

async def run_producer_loop() -> None:
    binance_url = "wss://[stream.binance.com:9443/stream?streams=btcusdt@ticker/ethusdt@ticker/solusdt@ticker]"
    producer = create_kafka_producer()

    while True:
        try:
            async with websockets.connect(binance_url) as websocket:
                while True:
                    raw_data = await websocket.recv()
                    message = parse_ticker_data(raw_data)
                    send_to_kafka(producer, TOPIC_NAME, message)
        except websockets.exceptions.ConnectionClosed as e:
            print(e)
            await asyncio.sleep(5)
        except Exception as e:
            print(e)
            await asyncio.sleep(5)

if __name__ == "__main__":
    asyncio.run(run_producer_loop())