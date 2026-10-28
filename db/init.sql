CREATE TABLE IF NOT EXISTS crypto_candles (
    symbol TEXT NOT NULL,
    minute_start TIMESTAMP NOT NULL,
    avg_price DOUBLE PRECISION NOT NULL,
    min_price DOUBLE PRECISION NOT NULL,
    max_price DOUBLE PRECISION NOT NULL,
    volume DOUBLE PRECISION NOT NULL,
    trade_count INTEGER NOT NULL,
    PRIMARY KEY (symbol, minute_start)
);

CREATE TABLE IF NOT EXISTS crypto_anomalies (
    id SERIAL PRIMARY KEY,
    symbol TEXY NOT NULL,
    minute_start TIMESTAMP NOT NULL,
    price DOUBLE PRECISION NOT NULL,
    price_change_pct DOUBLE PRECISION,
    volume DOUBLE PRECISION,
    detected_at TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE (symbol, minute_start)
);
