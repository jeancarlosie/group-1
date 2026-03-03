import uuid
import random
import time
import json
import yaml
from datetime import datetime, timedelta
from faker import Faker
from fastavro import writer, parse_schema
import os

fake = Faker()

# Load config
with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

# Create output folders
os.makedirs("../samples/json", exist_ok=True)
os.makedirs("../samples/avro", exist_ok=True)

# Load schemas
with open("../schemas/order_events.avsc") as f:
    order_schema = parse_schema(json.load(f))

with open("../schemas/courier_state_events.avsc") as f:
    courier_schema = parse_schema(json.load(f))

order_events = []
courier_events = []

zones = [f"zone_{i}" for i in range(config["zones"])]
restaurants = [
    f"rest_{z}_{i}"
    for z in zones
    for i in range(config["restaurants_per_zone"])
]
couriers = [f"courier_{i}" for i in range(config["couriers"])]

start_time = datetime.utcnow()

def current_millis(dt):
    return int(dt.timestamp() * 1000)

for minute in range(config["simulation_minutes"]):
    simulated_time = start_time + timedelta(minutes=minute)

    orders_this_minute = config["base_orders_per_minute"]

    for _ in range(orders_this_minute):
        order_id = str(uuid.uuid4())
        restaurant_id = random.choice(restaurants)
        zone_id = restaurant_id.split("_")[1]
        courier_id = random.choice(couriers)

        event_time = simulated_time
        ingest_time = simulated_time

        order_created = {
            "schema_version": 1,
            "event_id": str(uuid.uuid4()),
            "event_type": "ORDER_CREATED",
            "order_id": order_id,
            "restaurant_id": restaurant_id,
            "courier_id": None,
            "zone_id": zone_id,
            "order_value": round(random.uniform(10, 50), 2),
            "event_time": current_millis(event_time),
            "ingest_time": current_millis(ingest_time)
        }

        order_events.append(order_created)

        courier_online = {
            "schema_version": 1,
            "event_id": str(uuid.uuid4()),
            "event_type": "ONLINE",
            "courier_id": courier_id,
            "order_id": None,
            "zone_id": zone_id,
            "event_time": current_millis(event_time),
            "ingest_time": current_millis(ingest_time)
        }

        courier_events.append(courier_online)

# Write JSON
with open("../samples/json/order_events_sample.jsonl", "w") as f:
    for e in order_events:
        f.write(json.dumps(e) + "\n")

with open("../samples/json/courier_state_events_sample.jsonl", "w") as f:
    for e in courier_events:
        f.write(json.dumps(e) + "\n")

# Write AVRO
with open("../samples/avro/order_events_sample.avro", "wb") as out:
    writer(out, order_schema, order_events)

with open("../samples/avro/courier_state_events_sample.avro", "wb") as out:
    writer(out, courier_schema, courier_events)

print("Sample data generated successfully.")
