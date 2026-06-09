"""
CNC Factory — Kafka Consumer
==============================
Reads events from the iot-sensors topic and prints to console.

Usage:
    python kafka_consumer.py                   # one line per event
    python kafka_consumer.py --raw             # full JSON per event
    python kafka_consumer.py --faults-only     # only show fault events
    python kafka_consumer.py --machine CNC_01  # filter one machine
    python kafka_consumer.py --from-beginning  # read all stored events
"""

import json
import argparse
from datetime import datetime
from kafka import KafkaConsumer
from kafka.errors import KafkaError

# ── CONFIG ────────────────────────────────────────────────────────────────────
BOOTSTRAP_SERVERS = ["localhost:9094", "localhost:9095", "localhost:9096"]
TOPIC             = "iot-sensors"
GROUP_ID          = "iot-console-consumer"

# ── COLOURS ───────────────────────────────────────────────────────────────────
RESET  = "\033[0m"
BOLD   = "\033[1m"
GREEN  = "\033[32m"
YELLOW = "\033[33m"
RED    = "\033[31m"
GRAY   = "\033[90m"
CYAN   = "\033[36m"

MACHINE_COLOR = {
    "CNC_01": "\033[38;5;208m",   # orange
    "ROB_01": "\033[32m",         # green
    "CNV_01": "\033[34m",         # blue
    "PMP_01": "\033[35m",         # purple
}

STATUS_COLOR = {
    "running": GREEN,
    "warning": YELLOW,
    "fault":   RED,
}


# ── PRINT MODES ───────────────────────────────────────────────────────────────

def print_summary(event):
    """One clean line per event."""
    mid     = event.get("machine_id", "?")
    status  = event.get("status", "?")
    metrics = event.get("metrics", {})
    error_code = event.get("error_code")
    is_fault   = event.get("is_fault", False)

    mc = MACHINE_COLOR.get(mid, GRAY)
    sc = STATUS_COLOR.get(status, GRAY)

    ts = datetime.now().strftime("%H:%M:%S")

    error_str = ""
    if is_fault and error_code:
        error_str = f"  {BOLD}{RED}[{error_code}]{RESET}"

    print(
        f"  {GRAY}{ts}{RESET}  "
        f"{mc}{BOLD}{mid:<8}{RESET}  "
        f"status={sc}{status:<8}{RESET}  "
        f"temp={metrics.get('temperature', 0):5.1f}°C  "
        f"vib={metrics.get('vibration', 0):4.2f}mm/s  "
        f"rpm={int(metrics.get('rpm', 0)):4d}  "
        f"pwr={metrics.get('power_kw', 0):4.1f}kW"
        f"{error_str}"
    )


def print_raw(event, offset, partition):
    """Full JSON exactly as stored in Kafka."""
    print(f"\n{GRAY}# offset={offset}  partition={partition}{RESET}")
    print(json.dumps(event, indent=2))


# ── MAIN ──────────────────────────────────────────────────────────────────────

def run(args):
    auto_offset = "earliest" if args.from_beginning else "latest"

    print(f"\n{'='*50}")
    print(f"  CNC Factory — Kafka Consumer")
    print(f"{'='*50}")
    print(f"  Brokers : {BOOTSTRAP_SERVERS}")
    print(f"  Topic   : {TOPIC}")
    print(f"  Mode    : {'raw JSON' if args.raw else 'summary'}")
    print(f"  From    : {'beginning' if args.from_beginning else 'latest'}")
    if args.machine:
        print(f"  Filter  : machine_id = {args.machine}")
    if args.faults_only:
        print(f"  Filter  : faults only")
    print(f"{'='*50}\n")

    try:
        consumer = KafkaConsumer(
            TOPIC,
            bootstrap_servers  = BOOTSTRAP_SERVERS,
            group_id           = GROUP_ID,
            auto_offset_reset  = auto_offset,
            enable_auto_commit = True,
            value_deserializer = lambda v: json.loads(v.decode("utf-8")),
            key_deserializer   = lambda k: k.decode("utf-8") if k else None,
            consumer_timeout_ms = -1,
        )
        print(f"  Connected — listening on '{TOPIC}' (Ctrl+C to stop)\n")

    except KafkaError as e:
        print(f"  [ERROR] Kafka connection failed: {e}")
        return

    received = 0
    faults   = 0

    try:
        for msg in consumer:
            event = msg.value

            # ── Filters ──────────────────────────────────────────────
            if args.machine and event.get("machine_id") != args.machine:
                continue
            if args.faults_only and not event.get("is_fault"):
                continue

            received += 1
            if event.get("is_fault"):
                faults += 1

            # ── Print ─────────────────────────────────────────────────
            if args.raw:
                print_raw(event, msg.offset, msg.partition)
            else:
                print_summary(event)

    except KeyboardInterrupt:
        pct = faults / max(1, received) * 100
        print(f"\n\n  Stopped — received={received}  faults={faults} ({pct:.1f}%)")
    finally:
        consumer.close()


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CNC Factory Kafka Consumer")
    parser.add_argument(
        "--raw", action="store_true",
        help="Print full JSON instead of summary line",
    )
    parser.add_argument(
        "--machine", type=str, default=None,
        choices=["CNC_01", "ROB_01", "CNV_01", "PMP_01"],
        help="Only show events from this machine",
    )
    parser.add_argument(
        "--faults-only", action="store_true",
        help="Only print events where is_fault=true",
    )
    parser.add_argument(
        "--from-beginning", action="store_true",
        help="Read from start of topic (default: new messages only)",
    )
    run(parser.parse_args())
