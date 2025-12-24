import requests

BASE = "http://localhost:8080"

def test_validation_missing_field():
    event = {
        "topic": "invalid",
        "timestamp": "2025-01-01T10:00:00Z",
        "source": "pytest",
        "payload": {}
    }

    res = requests.post(f"{BASE}/publish", json=event)
    assert res.status_code == 422
