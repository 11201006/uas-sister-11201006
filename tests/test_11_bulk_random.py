import requests, uuid, random

BASE = "http://localhost:8080"

def test_bulk_random():
    for _ in range(50):
        event = {
            "topic": "rnd",
            "event_id": str(uuid.uuid4()),
            "timestamp": "2025-01-02T00:00:00Z",
            "source": "pytest",
            "payload": {"x": random.random()}
        }
        r = requests.post(f"{BASE}/publish", json=event)
        assert r.status_code == 200
