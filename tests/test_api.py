from fastapi.testclient import TestClient
from src.api.main import app

client = TestClient(app)

def test_healthcheck():
    response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}

def test_symbol_list():
    response = client.get("/symbols")

    assert "BTCUSDT" in response.json()

def test_unknown_symbol_returns_404():
    response = client.get("/prices/DOGEUSDT")

    assert response.status_code == 404

def test_too_many_minutes_returns_400():
    response = client.get("/prices/BTCUSDT?minutes=5000")

    assert response.status_code == 400

def test_limit_too_big_returns_400():
    response = client.get("/anomalies?limit=500")

    assert response.status_code == 400