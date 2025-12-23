import os
import json
import uuid
import time
import requests
import threading
from datetime import datetime

TARGET = os.getenv("TARGET_URL")

def run():
    while True:
        payload = {
            "topic": "system",
            "event_id": str(uuid.uuid4()),
            "timestamp": datetime.utcnow().isoformat(),
            "source": "publisher",
            "payload": {"value": "log test"}
        }

        try:
            requests.post(TARGET, json=payload)
        except Exception as e:
            print("failed:", e)

        time.sleep(0.01)


if __name__ == "__main__":
    for _ in range(3):
        threading.Thread(target=run, daemon=True).start()

    while True:
        time.sleep(1)
