from datetime import datetime, timedelta

def find_stale_symbols(last_candles: list, now: datetime, max_delay_minutes: int = 10) -> list:
    stale_symbols = []
    for symbol, last_minute in last_candles:
        if now - last_minute > timedelta(minutes = max_delay_minutes):
            stale_symbols.append(symbol)
    return stale_symbols

def build_daily_summary(anomaly_counts: list) -> str:
    if len(anomaly_counts) == 0:
        return "No anomalies"
    text = "Daily summary"
    for symbol, anomalies_num in anomaly_counts:
        text += f"\n{symbol.upper()}: {anomalies_num} "
    return text