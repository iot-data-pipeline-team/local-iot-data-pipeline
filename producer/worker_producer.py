import json
import random
import time

from datetime import datetime, timezone
from itertools import cycle

from kafka import KafkaProducer




WORKERS = [
    {"worker_id": "W001", "floor": "A"},
    {"worker_id": "W002", "floor": "B"},
    {"worker_id": "W003", "floor": "C"},
    {"worker_id": "W004", "floor": "A"}
]

def generate_worker_event(worker):

    fatigue = random.randint(10, 100)

    if fatigue > 80:
        danger_zone = random.random() < 0.30
    else:
        danger_zone = random.random() < 0.10

    helmet_on = random.random() > 0.05

    return {
        "worker_id": worker["worker_id"],
        "timestamp": datetime.now(
            timezone.utc
        ).isoformat(),

        "floor": worker["floor"],

        "helmet_on": helmet_on,
        "danger_zone": danger_zone,
        "fatigue_score": fatigue
    }


producer = KafkaProducer(
    bootstrap_servers=[
        "localhost:19092",
        "localhost:19093",
        "localhost:19094"
    ],

    value_serializer=lambda v:
        json.dumps(v).encode("utf-8"),

    key_serializer=lambda k:
        k.encode("utf-8")
)

worker_cycle = cycle(WORKERS)

while True:

    worker = next(worker_cycle)

    event = generate_worker_event(worker)

    producer.send(
        "worker-events",
        key=event["worker_id"],
        value=event
    )

    print(event)

    time.sleep(1)