import time
import uuid
import requests
import random
import threading

TARGET = "http://aggregator:8080/publish"

def generate_event():
    return {
        "topic": random.choice(["alpha", "beta", "gamma"]),
        "event_id": str(uuid.uuid4()),
        "timestamp": "2025-01-01T10:00:00Z",
        "source": "publisher",
        "payload": {"value": random.randint(1, 100)}
    }

def worker():
    while True:
        try:
            ev = generate_event()
            requests.post(TARGET, json=ev)
        except:
            pass
        time.sleep(0.05)

if __name__ == "__main__":
    for _ in range(5):
        threading.Thread(target=worker, daemon=True).start()
    while True:
        time.sleep(1)
