FEATURE_NAMES = ["price_change_ptc", "range_ptc", "volume"]

def make_features(avg_price: float, previous_price: float,
                  min_price: float, max_price: float, volume: float):
    price_change_ptc = (avg_price - previous_price) / previous_price * 100
    range_ptc = (max_price - min_price) / avg_price * 100
    return price_change_ptc, range_ptc, volume