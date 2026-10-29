import json
from src.producer.binance_producer import parse_trade, build_binance_url

def make_message(price = 78740.93, quantity="0.01"):
    data = {
        "s": "BTCUSDT",
        "p": price,
        "q": quantity,
        "T": 1700000000000
    }
    return json.dumps({ "stream": "btcusdt@aggTrade", "data": data })

def test_parse_correct_trade():
    message = make_message()
    trade = parse_trade(message)

    assert trade["symbol"] == "BTCUSDT"
    assert trade["price"] == 78740.93
    assert trade["trade_time"] == 1700000000000

def test_parse_return_none_for_zero_price():
    message = make_message(price = 0)
    trade = parse_trade(message)

    assert trade is None

def test_parse_returns_none_for_broken_json():
    message = "not json"
    trade = parse_trade(message)

    assert trade is None

def test_url_contains_all_symbols():
    symbols = ["BTCUSDT", "ETHUSDT"]
    url = build_binance_url(symbols)

    assert "btcusdt@aggTrade" in url
    assert "ethusdt@aggTrade" in url