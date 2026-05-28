"""
kafka_consumer.py — IoT Kafka Consumer
Reads events from Kafka and prints full JSON to console.

Install: pip install kafka-python
Run:     python kafka_consumer.py
Pretty:  python kafka_consumer.py --pretty
Raw:     python kafka_consumer.py --raw
Filter:  python kafka_consumer.py --machine CNC_01
Faults:  python kafka_consumer.py --faults-only
"""

import json
import argparse
from datetime import datetime, timezone
from kafka    import KafkaConsumer
from kafka.errors import KafkaError

# ── CONFIG ────────────────────────────────────────────────────────
BOOTSTRAP_SERVERS = ["localhost:9094", "localhost:9095", "localhost:9096"]
TOPIC             = "iot-sensors"
GROUP_ID          = "iot-console-consumer"

# ── DISPLAY HELPERS ───────────────────────────────────────────────

# ANSI colours
R  = "\033[0m"       # reset
OR = "\033[38;5;208m" # orange
TL = "\033[38;5;43m"  # teal
AM = "\033[38;5;220m" # amber
RD = "\033[38;5;203m" # red
GN = "\033[38;5;83m"  # green
BL = "\033[38;5;75m"  # blue
PU = "\033[38;5;141m" # purple
GY = "\033[38;5;245m" # gray
BD = "\033[1m"        # bold

MACHINE_COLOURS = {
    "CNC_01": OR, "CNC_02": OR,
    "ROB_01": GN,
    "CNV_01": BL,
    "PMP_01": PU,
}

def colour_for(machine_id):
    return MACHINE_COLOURS.get(machine_id, GY)

def print_summary(event):
    """One-line summary — what the producer already prints."""
    mid   = event.get("machine_id", "?")
    mtype = event.get("machine_type", "?")
    col   = colour_for(mid)
    status = event.get("status", "?")
    icon   = {"running": "🟢", "idle": "🟡", "fault": "🔴"}.get(status, "⚪")
    m      = event.get("metrics", {})
    ferr   = f"  ⚠️  {BD}{RD}{event['error_code']}{R}" if event.get("is_fault") else ""
    mtype_short = mtype.replace("_", " ").upper()

    print(
        f"{col}{BD}{mid}{R}  "
        f"[{GY}{mtype_short}{R}]  "
        f"{icon} {status:<8}  "
        f"temp={BD}{m.get('temperature','?'):>5}°C{R}  "
        f"rpm={m.get('rpm','?'):>7}  "
        f"vib={m.get('vibration','?'):>5}"
        f"{ferr}"
    )

def print_pretty(event, offset, partition):
    """Full formatted JSON with colour hints."""
    mid  = event.get("machine_id", "?")
    col  = colour_for(mid)
    ts   = event.get("timestamp", "")

    print(f"\n{GY}{'─'*70}{R}")
    print(f"  {BD}offset={offset}  partition={partition}  received={datetime.now().strftime('%H:%M:%S.%f')[:-3]}{R}")
    print(f"  machine: {col}{BD}{mid}{R}  |  type: {event.get('machine_type','')}  |  floor: {event.get('floor','')}  |  shift: {event.get('shift','')}")
    print(f"  timestamp: {GY}{ts}{R}")
    print()

    # status line
    status = event.get("status", "?")
    icon   = {"running": "🟢", "idle": "🟡", "fault": "🔴"}.get(status, "⚪")
    err    = event.get("error_code")
    is_fault = event.get("is_fault", False)

    if is_fault:
        print(f"  status: {RD}{BD}FAULT{R}  error_code: {RD}{BD}{err}{R}")
    else:
        print(f"  status: {GN}running{R}" if status == "running" else f"  status: {AM}idle{R}")

    # metrics
    m = event.get("metrics", {})
    print(f"\n  {BD}metrics:{R}")
    print(f"    temperature : {BD}{m.get('temperature','?')}{R} °C")
    print(f"    vibration   : {m.get('vibration','?')} mm/s")
    print(f"    rpm         : {m.get('rpm','?')}")
    print(f"    power_kw    : {m.get('power_kw','?')} kW")

    # machine-specific sensors
    for sensor_key in ["cnc_sensors", "robot_sensors", "conveyor_sensors", "pump_sensors"]:
        sensors = event.get(sensor_key)
        if sensors:
            print(f"\n  {BD}{sensor_key}:{R}")
            for k, v in sensors.items():
                print(f"    {k:<28}: {BD}{v}{R}")

    print()

def print_raw(event):
    """Raw JSON — exactly what Kafka stored."""
    print(json.dumps(event, indent=2))
    print()

# ── MAIN ──────────────────────────────────────────────────────────

def run(mode, machine_filter, faults_only, from_beginning):

    auto_offset = "earliest" if from_beginning else "latest"

    print(f"Connecting to Kafka: {BOOTSTRAP_SERVERS}")
    print(f"Topic:     {TOPIC}")
    print(f"Group ID:  {GROUP_ID}")
    print(f"Mode:      {mode}")
    print(f"From:      {'beginning' if from_beginning else 'latest (new messages only)'}")
    if machine_filter:
        print(f"Filter:    machine_id = {machine_filter}")
    if faults_only:
        print(f"Filter:    faults only")
    print()

    try:
        consumer = KafkaConsumer(
            TOPIC,
            bootstrap_servers    = BOOTSTRAP_SERVERS,
            group_id             = GROUP_ID,
            auto_offset_reset    = auto_offset,
            enable_auto_commit   = True,
            value_deserializer   = lambda v: json.loads(v.decode("utf-8")),
            key_deserializer     = lambda k: k.decode("utf-8") if k else None,
            consumer_timeout_ms  = -1,    # block forever waiting for messages
        )
        print(f"✅  Listening on topic '{TOPIC}' — Ctrl+C to stop\n")
        print("─" * 70)

    except KafkaError as e:
        print(f"❌  Kafka connection failed: {e}")
        return

    received = 0
    faults   = 0

    try:
        for message in consumer:
            event = message.value

            # apply filters
            if machine_filter and event.get("machine_id") != machine_filter:
                continue
            if faults_only and not event.get("is_fault"):
                continue

            received += 1
            faults   += int(event.get("is_fault", False))

            if mode == "raw":
                print(f"# offset={message.offset}  partition={message.partition}  key={message.key}")
                print_raw(event)

            elif mode == "pretty":
                print_pretty(event, message.offset, message.partition)

            else:  # summary (default)
                print(
                    f"  [{received:>4}]  "
                    f"off={message.offset:<6}  "
                    f"part={message.partition}  ",
                    end=""
                )
                print_summary(event)

    except KeyboardInterrupt:
        print(f"\n\n⛔  Stopped — {received} received | {faults} faults ({faults/max(1,received)*100:.1f}%)")
    finally:
        consumer.close()
        print("Consumer closed.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="IoT Kafka Consumer")
    parser.add_argument("--mode",      choices=["summary", "pretty", "raw"],
                        default="summary",
                        help="Output format (default: summary)")
    parser.add_argument("--pretty",    action="store_true",
                        help="Shortcut for --mode pretty")
    parser.add_argument("--raw",       action="store_true",
                        help="Shortcut for --mode raw — prints exact JSON")
    parser.add_argument("--machine",   type=str, default=None,
                        help="Only show events from this machine_id")
    parser.add_argument("--faults-only", action="store_true",
                        help="Only print fault events")
    parser.add_argument("--from-beginning", action="store_true",
                        help="Read from beginning of topic (default: new messages only)")
    parser.add_argument("--topic",     type=str, default=TOPIC,
                        help=f"Kafka topic (default: {TOPIC})")
    parser.add_argument("--group",     type=str, default=GROUP_ID,
                        help=f"Consumer group ID (default: {GROUP_ID})")
    args = parser.parse_args()

    mode = args.mode
    if args.pretty: mode = "pretty"
    if args.raw:    mode = "raw"

    TOPIC    = args.topic
    GROUP_ID = args.group

    run(
        mode           = mode,
        machine_filter = args.machine,
        faults_only    = args.faults_only,
        from_beginning = args.from_beginning,
    )
