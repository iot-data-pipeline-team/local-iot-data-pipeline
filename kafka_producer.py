"""
CNC Factory — Kafka Producer (Async)
======================================
Polls all 4 machines simultaneously every second and publishes
4 events to Kafka — one reading per machine per second.

Architecture:
    Gateway API (gateway_api.py)
        ↓  HTTP GET /machines/{id}  (4 parallel calls per second)
    This Producer
        ↓  Kafka binary protocol / TCP 9092
    Kafka Broker → PySpark → Elasticsearch / MinIO / PostgreSQL

Usage:
    python kafka_producer.py              # normal mode → Kafka
    python kafka_producer.py --dry-run    # print to console, no Kafka needed
"""

import json
import sys
import time
import asyncio
import argparse
import aiohttp
from datetime import datetime

# ── Try importing Kafka ────────────────────────────────────────────────────────
try:
    from kafka import KafkaProducer
    from kafka.errors import NoBrokersAvailable
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False

# ─────────────────────────────────────────────────────────────────────────────
#  CONFIG
# ─────────────────────────────────────────────────────────────────────────────

GATEWAY_URL   = "http://localhost:8000"
KAFKA_BROKERS = ["localhost:9094", "localhost:9095", "localhost:9096"]
KAFKA_TOPIC   = "iot-sensors"
MACHINES      = ["CNC_01", "ROB_01", "CNV_01", "PMP_01"]

# ── Console colours ────────────────────────────────────────────────────────────
GREEN  = "\033[32m"
YELLOW = "\033[33m"
RED    = "\033[31m"
BOLD   = "\033[1m"
RESET  = "\033[0m"
GRAY   = "\033[90m"

STATUS_COLOR = {"running": GREEN, "warning": YELLOW, "fault": RED}
MACHINE_COLOR = {
    "CNC_01": "\033[38;5;208m",
    "ROB_01": "\033[32m",
    "CNV_01": "\033[34m",
    "PMP_01": "\033[35m",
}


# ─────────────────────────────────────────────────────────────────────────────
#  KAFKA
# ─────────────────────────────────────────────────────────────────────────────

def connect_kafka():
    if not KAFKA_AVAILABLE:
        print(f"{YELLOW}[WARN]{RESET} kafka-python not installed → dry-run mode.")
        return None
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8"),
            acks="all",
            compression_type="gzip",
            retries=3,
            linger_ms=5,
        )
        print(f"{GREEN}[OK]{RESET}   Kafka connected → {KAFKA_BROKERS}")
        return producer
    except NoBrokersAvailable:
        print(f"{YELLOW}[WARN]{RESET} Kafka unavailable → dry-run mode.")
        return None


def send(producer, event: dict):
    producer.send(
        KAFKA_TOPIC,
        key=event["machine_id"],
        value=event,
    )


# ─────────────────────────────────────────────────────────────────────────────
#  ASYNC GATEWAY POLLING — all 4 machines in parallel
# ─────────────────────────────────────────────────────────────────────────────

async def poll_machine(session: aiohttp.ClientSession, machine_id: str) -> dict | None:
    """Poll one machine asynchronously."""
    try:
        async with session.get(
            f"{GATEWAY_URL}/machines/{machine_id}",
            timeout=aiohttp.ClientTimeout(total=3)
        ) as resp:
            return await resp.json()
    except Exception as e:
        print(f"{YELLOW}[WARN]{RESET} {machine_id} poll failed: {e}")
        return None


async def poll_all(session: aiohttp.ClientSession) -> list[dict]:
    """Poll all 4 machines simultaneously — returns 4 readings at once."""
    tasks = [poll_machine(session, mid) for mid in MACHINES]
    results = await asyncio.gather(*tasks)
    # Filter out failed polls
    return [r for r in results if r is not None]


async def check_gateway(session: aiohttp.ClientSession) -> bool:
    try:
        async with session.get(
            f"{GATEWAY_URL}/machines/CNC_01",
            timeout=aiohttp.ClientTimeout(total=3)
        ) as resp:
            return resp.status == 200
    except Exception:
        return False


# ─────────────────────────────────────────────────────────────────────────────
#  CONSOLE PRINT
# ─────────────────────────────────────────────────────────────────────────────

def print_event(event: dict):
    ts         = event.get("timestamp", "")[:19].replace("T", " ")
    mid        = event.get("machine_id", "?")
    status     = event.get("status", "?")
    metrics    = event.get("metrics", {})
    error_code = event.get("error_code")
    is_fault   = event.get("is_fault", False)

    mc      = MACHINE_COLOR.get(mid, GRAY)
    sc      = STATUS_COLOR.get(status, GRAY)
    err_str = f"  {BOLD}{RED}[{error_code}]{RESET}" if is_fault and error_code else ""

    print(
        f"  {GRAY}{ts}{RESET}  "
        f"{mc}{BOLD}{mid:<8}{RESET}  "
        f"status={sc}{status:<8}{RESET}  "
        f"temp={metrics.get('temperature', 0):5.1f}°C  "
        f"vib={metrics.get('vibration', 0):4.2f}mm/s  "
        f"rpm={int(metrics.get('rpm', 0)):4d}  "
        f"pwr={metrics.get('power_kw', 0):4.1f}kW"
        f"{err_str}"
    )


def print_cycle_separator(cycle: int, sent: int, faults: int):
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"\n  {GRAY}── cycle {cycle:>5}  {ts}  sent={sent}  faults={faults} ──{RESET}")


# ─────────────────────────────────────────────────────────────────────────────
#  MAIN ASYNC LOOP
# ─────────────────────────────────────────────────────────────────────────────

async def run_async(args, producer, dry_run: bool):
    total_sent  = 0
    fault_count = 0
    cycle       = 0

    print(f"\n  Polling all 4 machines in parallel every 1 second\n")

    async with aiohttp.ClientSession() as session:

        # ── Check gateway ────────────────────────────────────────────
        print("  Checking gateway ... ", end="", flush=True)
        if not await check_gateway(session):
            print(f"\n{RED}[ERROR]{RESET} Gateway not reachable at {GATEWAY_URL}")
            print("        Run:  python gateway_api.py")
            sys.exit(1)
        print("OK\n")

        try:
            while True:
                cycle_start = time.time()
                cycle += 1

                # ── Poll all 4 machines simultaneously ───────────────
                events = await poll_all(session)

                if not events:
                    print(f"{YELLOW}[WARN]{RESET} All polls failed — retrying in 2s")
                    await asyncio.sleep(2)
                    continue

                # ── Publish or print ─────────────────────────────────
                for event in events:
                    if dry_run:
                        print_event(event)
                    else:
                        send(producer, event)

                    total_sent += 1
                    if event.get("is_fault"):
                        fault_count += 1

                if not dry_run:
                    producer.flush()

                # ── Stats every 10 cycles (10 seconds) ──────────────
                if cycle % 10 == 0:
                    print_cycle_separator(cycle, total_sent, fault_count)

                # ── Sleep remainder of 1 second ──────────────────────
                elapsed    = time.time() - cycle_start
                sleep_time = max(0.0, 1.0 - elapsed)
                await asyncio.sleep(sleep_time)

        except KeyboardInterrupt:
            print(f"\n\n  Stopped.  cycles={cycle}  sent={total_sent}  faults={fault_count}")
            if producer:
                producer.flush()
                producer.close()


# ─────────────────────────────────────────────────────────────────────────────
#  ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

def run(args):
    dry_run = args.dry_run or not KAFKA_AVAILABLE

    print(f"\n{'='*56}")
    print(f"  CNC Factory — Kafka Producer")
    print(f"{'='*56}")
    print(f"  Gateway   : {GATEWAY_URL}")
    print(f"  Topic     : {KAFKA_TOPIC}")
    print(f"  Mode      : 4 machines × 1 reading/sec = 4 msg/sec")
    print(f"  Dry run   : {dry_run}")
    print(f"{'='*56}")

    producer = None if dry_run else connect_kafka()
    if producer is None:
        dry_run = True

    asyncio.run(run_async(args, producer, dry_run))


# ─────────────────────────────────────────────────────────────────────────────
#  CLI
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CNC Gateway → Kafka Producer")
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print events to console instead of sending to Kafka",
    )
    run(parser.parse_args())
