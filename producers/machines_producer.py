"""
Sends full JSON events for 4 machines in round-robin order:
  CNC_01 → ROB_01 → CNV_01 → PMP_01 → CNC_01 → ...

Each machine sends directly to Kafka — no middleware, no data loss.

Install: pip install kafka-python
Run:     python kafka_producer.py
Run fast:  python kafka_producer.py --interval 0
Run N:     python kafka_producer.py --count 40
"""


import json
import sys
import time
import random
import argparse
import logging
from datetime import datetime, timezone
import math
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
TOPIC = "sensor-events"
INTERVAL   = 1.0       # seconds between each machine reading

# ── MACHINE DEFINITIONS ───────────────────────────────────────────
MACHINES = [
    {
        "machine_id":    "CNC_01",
        "machine_type":  "cnc_machine",
        "floor":         "A",
        "base_temp":     74.0,
        "base_rpm":      1500,
        "base_vib":      2.5,
        "base_power":    4.4,
        "oil_level":     82.0,
        "oil_drain":     0.006,
        "base_coolant":  3.2,
        "fault_rate":    0.015,
        "fault_type":    "overheat",
        "fault_streak":  0,
    },
    {
        "machine_id":    "ROB_01",
        "machine_type":  "robot_arm",
        "floor":         "B",
        "base_temp":     65.0,
        "base_rpm":      1502,
        "base_vib":      2.2,
        "base_power":    4.7,
        "base_torque":   138.0,
        "base_force":    85.0,
        "fault_rate":    0.020,
        "fault_type":    "rpm_drop",
        "fault_streak":  0,
    },
    {
        "machine_id":    "CNV_01",
        "machine_type":  "conveyor_belt",
        "floor":         "C",
        "base_temp":     55.0,
        "base_rpm":      900,
        "base_vib":      1.8,
        "base_power":    2.1,
        "base_tension":  4500.0,
        "base_load":     320.0,
        "fault_rate":    0.010,
        "fault_type":    "vibration",
        "fault_streak":  0,
    },
    {
        "machine_id":    "PMP_01",
        "machine_type":  "pump",
        "floor":         "C",
        "base_temp":     80.0,
        "base_rpm":      1480,
        "base_vib":      3.1,
        "base_power":    5.2,
        "oil_level":     55.0,
        "oil_drain":     0.010,
        "base_flow":     48.0,
        "base_inlet_p":  2.8,
        "fault_rate":    0.025,
        "fault_type":    "overheat",
        "fault_streak":  0,
    },
]




# ── HELPERS ───────────────────────────────────────────────────────

def get_shift(hour):
    if 6  <= hour < 14: return "morning"
    if 14 <= hour < 22: return "evening"
    return "night"

def shift_load(shift):
    return {"morning": 1.0, "evening": 0.85, "night": 0.60}[shift]

def ambient_temp(hour):
    return 18.0 + 6.0 * math.sin((hour - 4) * math.pi / 12)

def apply_fault(fault_type, m):
    if fault_type == "overheat":
        return (
            m["base_temp"] + random.uniform(14, 24),
            m["base_vib"]  + random.uniform(0.2, 0.8),
            m["base_rpm"]  * random.uniform(0.85, 0.95),
            random.choice(["E001", "E003"]),
        )
    elif fault_type == "vibration":
        return (
            m["base_temp"] + random.uniform(3, 7),
            m["base_vib"]  + random.uniform(3.5, 6.0),
            m["base_rpm"]  * random.uniform(0.90, 0.98),
            random.choice(["E002", "E004"]),
        )
    elif fault_type == "rpm_drop":
        return (
            m["base_temp"] + random.uniform(2, 5),
            m["base_vib"]  + random.uniform(1.0, 2.5),
            m["base_rpm"]  * random.uniform(0.35, 0.55),
            random.choice(["E005", "E002"]),
        )
    return (m["base_temp"], m["base_vib"], m["base_rpm"], None)

def inject_dirty_data(event):

    # 3% missing temperature
    if random.random() < 0.03:
        event["metrics"]["temperature"] = None

    # 2% impossible temperature
    if random.random() < 0.02:
        event["metrics"]["temperature"] = 999

    # 1% negative rpm
    if random.random() < 0.01:
        event["metrics"]["rpm"] = -500

    # 1% empty machine id
    if random.random() < 0.01:
        event["machine_id"] = ""

    return event

# ── GENERATE ONE READING ──────────────────────────────────────────

def generate_reading(m):
    now   = datetime.now(timezone.utc)
    if random.random() < 0.01:
        timestamp = None
    else:
        timestamp = now.isoformat()    
    hour  = now.hour
    shift = get_shift(hour)
    load  = shift_load(shift)
    amb   = ambient_temp(hour)

    # oil drain for CNC and Pump
    if "oil_level" in m:
        m["oil_level"] = max(0.0, m["oil_level"] - m["oil_drain"])
        oil_penalty    = max(0, (30 - m["oil_level"]) / 30) * 0.04
    else:
        oil_penalty = 0.0

    # fault logic with burst streaks
    fault_prob = m["fault_rate"] + oil_penalty
    if m["fault_streak"] > 0:
        fault_prob        = min(0.9, fault_prob * 3)
        m["fault_streak"] -= 1

    is_fault = random.random() < fault_prob

    if is_fault:
        m["fault_streak"] = random.randint(1, 4)
        temp, vib, rpm, err = apply_fault(m["fault_type"], m)
        status = "fault"
        power  = round(m["base_power"] * 0.7 + random.uniform(-0.2, 0.2), 3)
    else:
        temp   = m["base_temp"] + (amb - 18) * 0.3 + random.gauss(0, 1.5) * load
        vib    = m["base_vib"]  + random.gauss(0, 0.15) * load
        rpm    = m["base_rpm"]  + random.gauss(0, 30) * load
        err    = None
        power  = round(m["base_power"] * load + random.gauss(0, 0.3), 3)
        status = "idle" if (load < 0.7 and random.random() < 0.2) else "running"
        if status == "idle":
            rpm  = 0.0
            temp -= 4.0
     # base event — fields every machine has

    if random.random() < 0.01:
        power = -5

    if random.random() < 0.01:
        status = "BROKEN_STATUS"     
    event = {
        "event_id":     f"{m['machine_id']}_{int(now.timestamp() * 1000)}",
        "timestamp":    timestamp,
        "machine_id":   m["machine_id"],
        "machine_type": m["machine_type"],
        "floor":        m["floor"],
        "shift":        shift,
        "status":       status,
        "error_code":   err,
        "is_fault":     is_fault,
        "metrics": {
            "temperature": round(float(temp),  2),
            "vibration":   round(float(vib),   3),
            "rpm":         round(float(rpm),   1),
            "power_kw":    round(float(power), 3),
        },
    }
    


    # machine-specific sensors — only the relevant block, no nulls
    mtype = m["machine_type"]

    if mtype == "cnc_machine":
        event["cnc_sensors"] = {
            "oil_level_pct":        round(m["oil_level"], 2),
            "coolant_pressure_bar": round(
                m["base_coolant"] + random.gauss(0, 0.1)
                + (-0.8 if is_fault else 0), 3
            ),
        }

    elif mtype == "robot_arm":
        event["robot_sensors"] = {
            "joint_torque_nm":      round(
                m["base_torque"] * load + random.gauss(0, 5)
                + (30 if is_fault else 0), 2
            ),
            "end_effector_force_n": round(
                m["base_force"] * load + random.gauss(0, 3), 2
            ),
        }

    elif mtype == "conveyor_belt":
        event["conveyor_sensors"] = {
            "belt_tension_n":  round(
                m["base_tension"] + random.gauss(0, 80)
                + (600 if is_fault else 0), 1
            ),
            "load_weight_kg":  round(
                m["base_load"] * load + random.gauss(0, 20), 1
            ),
        }

    elif mtype == "pump":
        event["pump_sensors"] = {
            "oil_level_pct":      round(m["oil_level"], 2),
            "flow_rate_lpm":      round(
                m["base_flow"] * load + random.gauss(0, 1.5)
                + (-18 if is_fault else 0), 2
            ),
            "inlet_pressure_bar": round(
                m["base_inlet_p"] + random.gauss(0, 0.1)
                + (0.6 if is_fault else 0), 3
            ),
        }
    event = inject_dirty_data(event)

    return event

# ── MAIN ──────────────────────────────────────────────────────────

def main(interval, count):

    # connect to Kafka
    log.info(f"Connecting to Kafka: {KAFKA_BOOTSTRAP}")
    try:
        producer = KafkaProducer(
            bootstrap_servers   = KAFKA_BOOTSTRAP,
            value_serializer    = lambda v: json.dumps(v).encode("utf-8"),
            key_serializer      = lambda k: k.encode("utf-8"),
            acks                = "all",      # wait for all replicas
            retries             = 3,
            linger_ms           = 5,          # small batch window
            compression_type    = "gzip",
        )
        log.info(f"Connected to Kafka | Topic: {TOPIC}")
    except KafkaError as e:
        log.error(f"Kafka connection failed: {e}")
        sys.exit(1)

    machine_cycle = cycle(MACHINES)
    sent_total    = 0
    fault_total   = 0

    print("┌─────────────────────────────────────────────────────────────┐")
    print("│  IoT Kafka Producer — Round-Robin                           │")
    print("│  CNC_01 → ROB_01 → CNV_01 → PMP_01 → repeat                │")
    print(f"│  Brokers: {KAFKA_BOOTSTRAP:<47}│")
    print(f"│  Topic:   {TOPIC:<47}  │")
    print(f"│  Interval: {interval}s  |  Target: {'∞' if count == 0 else count} events{' ' * 20}│")
    print("└─────────────────────────────────────────────────────────────┘\n")

    try:
        while True:
            m     = next(machine_cycle)
            event = generate_reading(m)

            # send to Kafka — key = machine_id ensures same machine
            # always goes to the same partition (ordered per machine)
            future = producer.send(
                TOPIC,
                key   = event["machine_id"] or "UNKNOWN",
                value = event,
            )

            # block briefly to catch send errors
            try:
                record_metadata = future.get(timeout=5)
                sent_total  += 1
                fault_total += int(event["is_fault"])

                # console log
                icon  = {"running": "🟢", "idle": "🟡", "fault": "🔴"}.get(event["status"], "⚪")
                ferr  = f"  ⚠️  {event['error_code']}" if event["is_fault"] else ""
                mtype = event["machine_type"]
                
                temp = event["metrics"]["temperature"]

                temp_display = (
                    "NULL"
                    if temp is None
                    else f"{temp:>5}"
                )                

                if mtype == "cnc_machine":
                    s     = event["cnc_sensors"]
                    extra = f"oil={s['oil_level_pct']}%  coolant={s['coolant_pressure_bar']}bar"
                elif mtype == "robot_arm":
                    s     = event["robot_sensors"]
                    extra = f"torque={s['joint_torque_nm']}Nm  force={s['end_effector_force_n']}N"
                elif mtype == "conveyor_belt":
                    s     = event["conveyor_sensors"]
                    extra = f"tension={s['belt_tension_n']}N  load={s['load_weight_kg']}kg"
                elif mtype == "pump":
                    s     = event["pump_sensors"]
                    extra = f"oil={s['oil_level_pct']}%  flow={s['flow_rate_lpm']}L/min"

                print(
                    f"  → [{sent_total:>4}]  "
                    f"partition={record_metadata.partition}  "
                    f"{event['machine_id']:<8}  "
                    f"{icon} {event['status']:<8}  "
                    f"temp={temp_display}°C  "                    
                    f"rpm={event['metrics']['rpm']:>7}  "
                    f"{extra}"
                    f"{ferr}"
                )

            except KafkaError as e:
                log.error(f"Failed to send event #{sent_total + 1}: {e}")

            # summary every 20 events
            if sent_total > 0 and sent_total % 20 == 0:
             log.info(
                    f"{sent_total} events sent | "
                    f"Faults: {fault_total} "
                    f"({fault_total/sent_total*100:.1f}%)"
                )

            if count > 0 and sent_total >= count:
                break

            time.sleep(interval)

    except KeyboardInterrupt:
        log.info("Producer interrupted by user.")
    finally:
        producer.flush()
        producer.close()

        fault_rate = (
            fault_total / sent_total * 100
            if sent_total else 0
        )

        print("\n========== Summary ==========")
        print(f"Events sent : {sent_total}")
        print(f"Faults      : {fault_total}")
        print(f"Fault rate  : {fault_rate:.1f}%")
        print("Producer closed.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="IoT sensor Kafka producer")
    parser.add_argument(
        "--interval", type=float, default=INTERVAL,
        help="Seconds between events (default: 1.0)",
    )
    parser.add_argument(
        "--count", type=int, default=0,
        help="Stop after N events (0 = run forever)",
    )
    parser.add_argument(
        "--bootstrap", default=KAFKA_BOOTSTRAP,
        help="Kafka bootstrap servers",
    )
    args = parser.parse_args()
    KAFKA_BOOTSTRAP = args.bootstrap
    main(args.interval, args.count)