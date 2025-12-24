import requests, uuid, subprocess, time

BASE = "http://localhost:8080"

def test_persistency_after_restart():
    eid = str(uuid.uuid4())

    event = {
        "topic": "persist",
        "event_id": eid,
        "timestamp": "2025-01-01T10:00:00Z",
        "source": "pytest",
        "payload": {"i": 1}
    }

    requests.post(f"{BASE}/publish", json=event)

    subprocess.run(["docker", "compose", "restart"], check=True)
    time.sleep(6)

    requests.post(f"{BASE}/publish", json=event)

    stats = requests.get(f"{BASE}/stats").json()
    assert "duplicate_dropped" in stats
