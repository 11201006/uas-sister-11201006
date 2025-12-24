import requests, uuid

BASE = "http://localhost:8080"

def test_topic_isolation():
    eid = str(uuid.uuid4())

    ev1 = {
        "topic": "t1",
        "event_id": eid,
        "timestamp": "2025-01-01T10:00:00Z",
        "source": "pytest",
        "payload": {"a": 1}
    }

    ev2 = {**ev1, "topic": "t2"}

    requests.post(f"{BASE}/publish", json=ev1)
    requests.post(f"{BASE}/publish", json=ev2)

    e1 = requests.get(f"{BASE}/events?topic=t1").json()
    e2 = requests.get(f"{BASE}/events?topic=t2").json()

    assert len(e1) == 1
    assert len(e2) == 1
