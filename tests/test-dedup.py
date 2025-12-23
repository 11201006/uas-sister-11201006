import requests
import uuid

def test_dedup():
    url = "http://aggregator:8080/publish"
    event_id = str(uuid.uuid4())

    payload = {
        "topic": "test",
        "event_id": event_id,
        "timestamp": "2024-01-01T00:00:00Z",
        "source": "test",
        "payload": {}
    }

    requests.post(url, json=payload)
    requests.post(url, json=payload)  # duplicate

    stats = requests.get("http://aggregator:8080/stats").json()

    assert stats["unique_processed"] >= 1
    assert stats["duplicate_dropped"] >= 1
