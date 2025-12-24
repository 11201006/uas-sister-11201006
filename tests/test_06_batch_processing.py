import requests, uuid

BASE = "http://localhost:8080"

def test_batch_processing():
    batch = []
    for i in range(10):
        batch.append({
            "topic": "batch",
            "event_id": str(uuid.uuid4()),
            "timestamp": "2025-01-01T10:00:00Z",
            "source": "pytest",
            "payload": {"i": i}
        })

    res = requests.post(f"{BASE}/publish", json=batch)
    assert res.status_code == 200
