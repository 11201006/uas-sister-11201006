import requests, uuid

BASE = "http://localhost:8080"

def test_multi_topic():
    for topic in ["a", "b", "c"]:
        requests.post(f"{BASE}/publish", json={
            "topic": topic,
            "event_id": str(uuid.uuid4()),
            "timestamp": "2025-01-01",
            "source": "pytest",
            "payload": {"topic": topic}
        })

    for t in ["a", "b", "c"]:
        data = requests.get(f"{BASE}/events?topic={t}").json()
        assert len(data) >= 1
