from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers = 'localhost:9094',
    value_serializer = lambda v: json.dumps(v).encode('utf-8')
)

devices = {
    "sensor_1": {
        "device_type": "boiler",
        "location": "Cairo",
        "technician": "Ahmed"
    },
    "sensor_2": {
        "device_type": "motor",
        "location": "Giza",
        "technician": "Omar"
    },
    "sensor_3": {
        "device_type": "pump",
        "location": "Alex",
        "technician": "Youssef"
    },
    "sensor_4": {
        "device_type": "boiler",
        "location": "Cairo",
        "technician": "Ali"
    },
    "sensor_5": {
        "device_type": "motor",
        "location": "Giza",
        "technician": "Hassan"
    }
}

while True:
    device_id = random.choice(list(devices.keys()))
    meta = devices[device_id]

    data = {
        "device_id": device_id,
        "device_type": meta["device_type"],
        "location": meta["location"],
        "technician": meta["technician"],
        "temperature": round(random.uniform(20, 40), 2),
        "humidity": round(random.uniform(30, 80), 2),
        "timestamp": datetime.now().isoformat()
    }

    # status logic
    if data["temperature"] > 35:
        data["status"] = "high"
    elif data["temperature"] < 22:
        data["status"] = "low"
    else:
        data["status"] = "normal"

    print("Sending:", data)
    producer.send("iot-data", data)

    time.sleep(2)




