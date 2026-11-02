import pytest
from src.ml.features import make_features

TEST_FEATURES = make_features(avg_price = 105.0, previous_price = 100.0,
                             min_price = 90.0, max_price = 111.0, volume = 7.5)

def test_price_up_one_percent():
    assert TEST_FEATURES[0] == pytest.approx(5.0)

def test_range_ptc():
    assert TEST_FEATURES[1] == pytest.approx(20.0)

def test_volume():
    assert TEST_FEATURES[2] == pytest.approx(7.5)