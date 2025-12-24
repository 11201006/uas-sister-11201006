import requests

BASE = "http://localhost:8080"

def test_stats_available():
    res = requests.get(f"{BASE}/stats")
    assert res.status_code == 200

    body = res.json()
    assert "received" in body
    assert "unique_processed" in body
    assert "duplicate_dropped" in body
