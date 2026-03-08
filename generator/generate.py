import uuid
import random
import json
import yaml
import os
from datetime import datetime, timedelta
from fastavro import writer, parse_schema

# Load config
with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

os.makedirs("../samples/json", exist_ok=True)
os.makedirs("../samples/avro", exist_ok=True)

with open("../schemas/order_events.avsc") as f:
    order_schema = parse_schema(json.load(f))

with open("../schemas/courier_state_events.avsc") as f:
    courier_schema = parse_schema(json.load(f))

# Helpers
def millis(dt):
    return int(dt.timestamp() * 1000)

def maybe_late(event_time):
    if random.random() < config["late_event_probability"]:
        delay = random.randint(2, 10)
        return event_time + timedelta(minutes=delay)
    return event_time

def maybe_duplicate(event, collection):
    collection.append(event)
    if random.random() < config["duplicate_probability"]:
        collection.append(event.copy())

# Static Entities
zones = [f"zone_{i}" for i in range(config["zones"])]
restaurants = [
    f"rest_{z}_{i}"
    for z in zones
    for i in range(config["restaurants_per_zone"])
]
couriers = [f"courier_{i}" for i in range(config["couriers"])]

# Simulation
order_events = []
courier_events = []

start_time = datetime.utcnow()

for minute in range(config["simulation_minutes"]):
    simulated_time = start_time + timedelta(minutes=minute)

    # Lunch / Dinner Peaks
    hour = simulated_time.hour
    multiplier = 1

    if 11 <= hour <= 14:
        multiplier = 2
    elif 18 <= hour <= 21:
        multiplier = 2.5

    orders_this_minute = int(config["base_orders_per_minute"] * multiplier)

    for _ in range(orders_this_minute):

        order_id = str(uuid.uuid4())
        restaurant_id = random.choice(restaurants)
        zone_number = restaurant_id.split("_")[2]
        zone_id = f"zone_{zone_number}"
        courier_id = random.choice(couriers)
        order_value = round(random.uniform(10, 50), 2)

        # Durations
        prep_minutes = random.randint(5, 20)
        delivery_minutes = random.randint(10, 30)

        # Anomaly injection
        if random.random() < config["anomaly_probability"]:
            delivery_minutes = -5  # impossible duration

        # Event timeline
        created_time = simulated_time
        accepted_time = created_time + timedelta(minutes=1)
        prep_done_time = accepted_time + timedelta(minutes=prep_minutes)
        pickup_time = prep_done_time + timedelta(minutes=5)
        delivered_time = pickup_time + timedelta(minutes=delivery_minutes)

        # Missing step simulation
        skip_pickup = random.random() < config["missing_step_probability"]

        # Cancellation simulation
        cancelled = random.random() < config["cancellation_probability"]

        # ORDER EVENTS

        def create_order_event(event_type, event_time, courier=None):
            ingest_time = maybe_late(event_time)

            return {
                "schema_version": 1,
                "event_id": str(uuid.uuid4()),
                "event_type": event_type,
                "order_id": order_id,
                "restaurant_id": restaurant_id,
                "courier_id": courier,
                "zone_id": zone_id,
                "order_value": order_value,
                "event_time": millis(event_time),
                "ingest_time": millis(ingest_time)
            }

        maybe_duplicate(create_order_event("ORDER_CREATED", created_time), order_events)
        maybe_duplicate(create_order_event("RESTAURANT_ACCEPTED", accepted_time), order_events)

        if cancelled:
            maybe_duplicate(create_order_event("CANCELLED", accepted_time + timedelta(minutes=1)), order_events)
            continue

        maybe_duplicate(create_order_event("PREP_DONE", prep_done_time), order_events)
        maybe_duplicate(create_order_event("COURIER_ASSIGNED", prep_done_time, courier_id), order_events)

        if not skip_pickup:
            maybe_duplicate(create_order_event("PICKED_UP", pickup_time, courier_id), order_events)

        maybe_duplicate(create_order_event("DELIVERED", delivered_time, courier_id), order_events)

        # COURIER EVENTS

        def create_courier_event(event_type, event_time, order=None):
            ingest_time = maybe_late(event_time)

            return {
                "schema_version": 1,
                "event_id": str(uuid.uuid4()),
                "event_type": event_type,
                "courier_id": courier_id,
                "order_id": order,
                "zone_id": zone_id,
                "event_time": millis(event_time),
                "ingest_time": millis(ingest_time)
            }

        maybe_duplicate(create_courier_event("ONLINE", created_time), courier_events)
        maybe_duplicate(create_courier_event("ARRIVED_RESTAURANT", prep_done_time, order_id), courier_events)

        if not skip_pickup:
            maybe_duplicate(create_courier_event("PICKED_UP_ORDER", pickup_time, order_id), courier_events)

        maybe_duplicate(create_courier_event("ARRIVED_CUSTOMER", delivered_time, order_id), courier_events)

        if random.random() < config["courier_offline_probability"]:
            maybe_duplicate(create_courier_event("OFFLINE", delivered_time + timedelta(minutes=1)), courier_events)

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

print("Enhanced sample data generated successfully.")
