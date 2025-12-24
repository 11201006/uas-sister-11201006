import requests, uuid, concurrent.futures

BASE = "http://localhost:8080"

def send(ev):
    r = requests.post(f"{BASE}/publish", json=ev)
    return r.status_code

def test_concurrent_same_event():
    eid = str(uuid.uuid4())
    event = {
        "topic": "concurrent",
        "event_id": eid,
        "timestamp": "2025-01-01T10:00:00Z",
        "source": "pytest",
        "payload": {"i": 1}
    }

    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as exec:
        results = list(exec.map(send, [event]*20))

    assert all(r == 200 for r in results)

    stats = requests.get(f"{BASE}/stats").json()
    assert stats["unique_processed"] >= 1
