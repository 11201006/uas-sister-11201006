import requests
import uuid
import random
from datetime import datetime
import time

TARGET = "http://aggregator:8080/publish"

def run():
    topics = ["auth", "billing", "user"]
    while True:
        event_id = str(uuid.uuid4())
        topic = random.choice(topics)

        payload = {
            "topic": topic,
            "event_id": event_id,
            "timestamp": datetime.utcnow().isoformat(),
            "source": "publisher",
            "payload": { "value": random.randint(1,100) }
        }
        requests.post(TARGET, json=payload)

        # send duplicates sometimes
        if random.random() < 0.3:
            requests.post(TARGET, json=payload)

        time.sleep(0.05)
