import requests, uuid

BASE = "http://localhost:8080"

def test_get_events():
    eid = str(uuid.uuid4())
    event = {
        "topic": "history",
        "event_id": eid,
        "timestamp": "2025-01-01T12:00:00Z",
        "source": "pytest",
        "payload": {"x": 123}
    }

    requests.post(f"{BASE}/publish", json=event)

    data = requests.get(f"{BASE}/events?topic=history").json()

    assert any(ev["event_id"] == eid for ev in data)
