"""
Spark-processed sensor stream consumer (dev/testing console).

  producer → Kafka (sensor-events) → Spark → Kafka (sensor-processed) ← this script
"""
import json
import os
import argparse
from datetime import datetime
from kafka import KafkaConsumer
from kafka.errors import KafkaError

KAFKA_BOOTSTRAP = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:19092,localhost:19093,localhost:19094",
)
TOPIC = "sensor-processed"
GROUP_ID = "dashboard-consumer"

# ANSI colours
R = "\033[0m"
OR = "\033[38;5;208m"
TL = "\033[38;5;43m"
AM = "\033[38;5;220m"
RD = "\033[38;5;203m"
GN = "\033[38;5;83m"
BL = "\033[38;5;75m"
PU = "\033[38;5;141m"
GY = "\033[38;5;245m"
BD = "\033[1m"

MACHINE_COLOURS = {
    "CNC_01": OR,
    "ROB_01": GN,
    "CNV_01": BL,
    "PMP_01": PU,
}


def colour_for(machine_id):
    return MACHINE_COLOURS.get(machine_id, GY)


def health_icon(score):
    if score is None:
        return "⚪"
    if score >= 75:
        return "🟢"
    if score >= 50:
        return "🟡"
    return "🔴"


def print_summary(event):
    mid = event.get("machine_id", "?")
    col = colour_for(mid)
    status = event.get("status", "?")
    icon = {"running": "🟢", "idle": "🟡", "fault": "🔴"}.get(status, "⚪")
    m = event.get("metrics", {})
    health = event.get("health_score")
    hicon = health_icon(health)

    extras = []
    if health is not None:
        extras.append(f"health={hicon}{health}%")
    if event.get("efficiency") is not None:
        extras.append(f"eff={event['efficiency']}")
    if event.get("alert"):
        extras.append(f"⚠️ {event['alert']}")
    if event.get("anomaly"):
        extras.append("🚨 anomaly")
    if event.get("maintenance_risk") == "high":
        extras.append("🔧 maint=HIGH")
    extra_str = f"  {' | '.join(extras)}" if extras else ""

    print(
        f"{col}{BD}{mid}{R}  "
        f"{icon} {status:<8}  "
        f"temp={BD}{m.get('temperature', '?'):>5}°C{R}  "
        f"rpm={m.get('rpm', '?'):>7}  "
        f"vib={m.get('vibration', '?'):>5}"
        f"{extra_str}"
    )


def print_pretty(event, offset, partition):
    mid = event.get("machine_id", "?")
    col = colour_for(mid)
    ts = event.get("timestamp", "")

    print(f"\n{GY}{'─' * 70}{R}")
    print(
        f"  {BD}offset={offset}  partition={partition}  "
        f"received={datetime.now().strftime('%H:%M:%S.%f')[:-3]}{R}"
    )
    print(
        f"  machine: {col}{BD}{mid}{R}  |  type: {event.get('machine_type', '')}  "
        f"|  floor: {event.get('floor', '')}  |  shift: {event.get('shift', '')}"
    )
    if event.get("location"):
        print(f"  location: {event['location']}  |  department: {event.get('department', '')}")
    print(f"  timestamp: {GY}{ts}{R}\n")

    status = event.get("status", "?")
    icon = {"running": "🟢", "idle": "🟡", "fault": "🔴"}.get(status, "⚪")
    print(f"  status: {icon} {status}")

    analytics = []
    if event.get("health_score") is not None:
        analytics.append(f"health_score={event['health_score']}%")
    if event.get("efficiency") is not None:
        analytics.append(f"efficiency={event['efficiency']}")
    if event.get("maintenance_risk"):
        analytics.append(f"maintenance_risk={event['maintenance_risk']}")
    if event.get("anomaly"):
        analytics.append("anomaly=TRUE")
    if analytics:
        print(f"  analytics: {', '.join(analytics)}")

    if event.get("alert"):
        sev = event.get("alert_severity", "warning")
        colour = RD if sev == "critical" else AM
        print(f"  alert: {colour}{BD}{event['alert']} ({sev}){R}")

    m = event.get("metrics", {})
    print(f"\n  {BD}metrics:{R}")
    print(f"    temperature : {m.get('temperature', '?')} °C")
    print(f"    vibration   : {m.get('vibration', '?')} mm/s")
    print(f"    rpm         : {m.get('rpm', '?')}")
    print(f"    power_kw    : {m.get('power_kw', '?')} kW")

    for sensor_key in ("cnc_sensors", "robot_sensors", "conveyor_sensors", "pump_sensors"):
        sensors = event.get(sensor_key)
        if sensors:
            print(f"\n  {BD}{sensor_key}:{R}")
            for k, v in sensors.items():
                print(f"    {k:<28}: {BD}{v}{R}")
    print()


def print_raw(event):
    print(json.dumps(event, indent=2))
    print()


def run(mode, machine_filter, faults_only, from_beginning, bootstrap):
    auto_offset = "earliest" if from_beginning else "latest"

    print(f"Connecting to Kafka: {bootstrap}")
    print(f"Topic:     {TOPIC}")
    print(f"Group ID:  {GROUP_ID}")
    print(f"Mode:      {mode}")
    print(f"From:      {'beginning' if from_beginning else 'latest (new messages only)'}")
    if machine_filter:
        print(f"Filter:    machine_id = {machine_filter}")
    if faults_only:
        print("Filter:    faults / alerts only")
    print()

    try:
        consumer = KafkaConsumer(
            TOPIC,
            bootstrap_servers=bootstrap,
            group_id=GROUP_ID,
            auto_offset_reset=auto_offset,
            enable_auto_commit=True,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            key_deserializer=lambda k: k.decode("utf-8") if k else None,
            consumer_timeout_ms=-1,
        )
        print(f"✅  Listening on topic '{TOPIC}' — Ctrl+C to stop\n")
        print("─" * 70)
    except KafkaError as e:
        print(f"❌  Kafka connection failed: {e}")
        return

    received = 0
    alerts = 0

    try:
        for message in consumer:
            event = message.value

            if machine_filter and event.get("machine_id") != machine_filter:
                continue
            if faults_only and not (event.get("is_fault") or event.get("alert")):
                continue

            received += 1
            alerts += int(bool(event.get("alert")) or event.get("is_fault", False))

            if mode == "raw":
                print(f"# offset={message.offset}  partition={message.partition}  key={message.key}")
                print_raw(event)
            elif mode == "pretty":
                print_pretty(event, message.offset, message.partition)
            else:
                print(
                    f"  [{received:>4}]  off={message.offset:<6}  part={message.partition}  ",
                    end="",
                )
                print_summary(event)

    except KeyboardInterrupt:
        pct = alerts / max(1, received) * 100
        print(f"\n\n⛔  Stopped — {received} received | {alerts} alerts/faults ({pct:.1f}%)")
    finally:
        consumer.close()
        print("Consumer closed.")


def main():
    parser = argparse.ArgumentParser(description="Consume Spark-processed sensor events")
    parser.add_argument(
        "--mode", choices=["summary", "pretty", "raw"], default="summary",
        help="Display mode (default: summary)",
    )
    parser.add_argument("--machine", default=None, help="Filter by machine_id")
    parser.add_argument("--faults-only", action="store_true", help="Show faults/alerts only")
    parser.add_argument(
        "--from-beginning", action="store_true",
        help="Read from earliest offset instead of latest",
    )
    parser.add_argument("--bootstrap", default=KAFKA_BOOTSTRAP, help="Kafka bootstrap servers")
    args = parser.parse_args()
    run(args.mode, args.machine, args.faults_only, args.from_beginning, args.bootstrap)


if __name__ == "__main__":
    main()
