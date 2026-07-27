import json
import random
import time
import logging
import argparse
import sys

from datetime import datetime, timezone
from itertools import cycle

from kafka import KafkaProducer
from kafka.errors import KafkaError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [PRODUCER] %(message)s",
    datefmt="%H:%M:%S",
)

log = logging.getLogger(__name__)

# Host default uses EXTERNAL listeners from docker-compose (19092-19094).
# Inside Docker network use: kafka1:9092,kafka2:9093,kafka3:9094
KAFKA_BOOTSTRAP = "localhost:9094,localhost:9095,localhost:9096"
TOPIC = "worker-events"
INTERVAL   = 1.0       # seconds between each worker reading




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

# # producer = KafkaProducer(
#     bootstrap_servers=[
#         "localhost:19092",
#         "localhost:19093",
#         "localhost:19094"
#     ],
    

#     value_serializer=lambda v:
#         json.dumps(v).encode("utf-8"),

#     key_serializer=lambda k:
#         k.encode("utf-8")
# )


def main(interval, count):

    log.info(f"Connecting to Kafka: {KAFKA_BOOTSTRAP}")

    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8"),
            acks="all",
            retries=3,
            linger_ms=5,
            compression_type="gzip",
        )

        log.info(f"Connected to Kafka | Topic: {TOPIC}")

    except KafkaError as e:
        log.error(f"Kafka connection failed: {e}")
        sys.exit(1)


    worker_cycle = cycle(WORKERS)

    sent_total = 0

    target = "∞" if count == 0 else count

    print("┌─────────────────────────────────────────────────────┐")
    print("│          Worker Safety Kafka Producer               │")
    print("├─────────────────────────────────────────────────────┤")
    print(f"│ Brokers  : {KAFKA_BOOTSTRAP}")
    print(f"│ Topic    : {TOPIC}")
    print(f"│ Interval : {interval}s")
    print(f"│ Target   : {target} events")
    print("└─────────────────────────────────────────────────────┘\n")


    try:
        while True:

            worker = next(worker_cycle)

            event = generate_worker_event(worker)

            future = producer.send(
                TOPIC,
                key=event["worker_id"] or "UNKNOWN",
                value=event
            )

            try:
                metadata = future.get(timeout=5)

                sent_total += 1

               
                print(
                    f"[{sent_total:>4}] "
                    f"partition={metadata.partition} | "
                    f"worker={event['worker_id'] or 'UNKNOWN':<7} | "
                    f"floor={event['floor']} | "
                    f"zone={event['zone_id']} | "
                    f"HR={event['heart_rate']} | "
                    f"fatigue={event['fatigue_score']} | "
                    f"helmet={event['helmet_on']} | "
                    f"vest={event['safety_vest_on']} | "
                    f"danger={event['danger_zone']} | "
                    f"status={event['movement_status']}"
                )

                if sent_total > 0 and sent_total % 20 == 0:
                    log.info(f"── {sent_total} worker events sent ──")

                if count > 0 and sent_total >= count:
                    break


            except KafkaError as e:
                log.error(f"Failed to send event: {e}")
        

            time.sleep(interval)

    except KeyboardInterrupt:
        print(f"\n⛔ Producer stopped.")
        print(f"Total events sent: {sent_total}")

    finally:
        producer.flush()
        producer.close()
        print("Producer closed.")


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Worker Safety Kafka Producer"
    )

    parser.add_argument(
        "--interval",
        type=float,
        default=INTERVAL,
        help="Seconds between worker events"
    )

    parser.add_argument(
        "--count",
        type=int,
        default=0,
        help="Stop after N events (0 = run forever)"
    )

    parser.add_argument(
        "--bootstrap",
        default=KAFKA_BOOTSTRAP,
        help="Kafka bootstrap servers"
    )

    args = parser.parse_args()

    KAFKA_BOOTSTRAP = args.bootstrap

    main(args.interval, args.count)