import requests, uuid

BASE = "http://localhost:8080"

def test_basic_publish():
    eid = str(uuid.uuid4())
    event = {
        "topic": "basic",
        "event_id": eid,
        "timestamp": "2025-01-01T10:00:00Z",
        "source": "pytest",
        "payload": {"msg": "ok"}
    }

    res = requests.post(f"{BASE}/publish", json=event)
    assert res.status_code == 200
