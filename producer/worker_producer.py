import json
import random
import time

from datetime import datetime, timezone
from itertools import cycle

from kafka import KafkaProducer




WORKERS = [
    {
        "worker_id": "W001",
        "floor": "A",
        "zone_id": "ZONE_1"
    },
    {
        "worker_id": "W002",
        "floor": "B",
        "zone_id": "ZONE_2"
    },
    {
        "worker_id": "W003",
        "floor": "C",
        "zone_id": "ZONE_3"
    },
    {
        "worker_id": "W004",
        "floor": "A",
        "zone_id": "ZONE_1"
    }
]


def generate_worker_event(worker):

    fatigue = random.randint(10, 100)

    heart_rate = random.randint(60, 130)

    if random.random() < 0.01:
        heart_rate = -20

    if fatigue > 80:
        danger_zone = random.random() < 0.30
    else:
        danger_zone = random.random() < 0.10

    helmet_on = random.random() > 0.05

    safety_vest_on = random.random() > 0.08

    movement_status = random.choice([
        "ACTIVE",
        "IDLE",
        "WALKING"
    ])

    if random.random() < 0.01:
        movement_status = "FLYING"

    if random.random() < 0.01:
        timestamp = None
    else:
        timestamp = datetime.now(
            timezone.utc
        ).isoformat()   

    worker_id = worker["worker_id"]

    if random.random() < 0.01:
        worker_id = ""         

    return {
        "worker_id": worker_id,

        "timestamp": timestamp,

        "floor": worker["floor"],

        "zone_id": worker["zone_id"],

        "helmet_on": helmet_on,

        "safety_vest_on": safety_vest_on,

        "heart_rate": heart_rate,

        "movement_status": movement_status,

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
        key=event["worker_id"] or "UNKNOWN",
        value=event
    )

    print(event)

    time.sleep(1)