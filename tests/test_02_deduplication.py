import requests, uuid

BASE = "http://localhost:8080"

def test_deduplication():
    eid = str(uuid.uuid4())

    event = {
        "topic": "dedup",
        "event_id": eid,
        "timestamp": "2025-01-01T10:00:00Z",
        "source": "pytest",
        "payload": {"index": 1}
    }

    r1 = requests.post(f"{BASE}/publish", json=event)
    r2 = requests.post(f"{BASE}/publish", json=event)

    assert r1.status_code == 200
    assert r2.status_code == 200

    stats = requests.get(f"{BASE}/stats").json()

    assert "unique_processed" in stats
    assert "duplicate_dropped" in stats
