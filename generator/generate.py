import uuid
import random
import json
import yaml
import os
from datetime import datetime, timedelta
from fastavro import writer, parse_schema

# Base directory (generator/)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Load config
with open(os.path.join(BASE_DIR, "config.yaml"), "r") as f:
    config = yaml.safe_load(f)

SAMPLES_JSON_DIR = os.path.join(BASE_DIR, "..", "samples", "json")
SAMPLES_AVRO_DIR = os.path.join(BASE_DIR, "..", "samples", "avro")

os.makedirs(SAMPLES_JSON_DIR, exist_ok=True)
os.makedirs(SAMPLES_AVRO_DIR, exist_ok=True)

# Load AVRO schemas
with open(os.path.join(BASE_DIR, "..", "schemas", "order_events.avsc")) as f:
    order_schema = parse_schema(json.load(f))

with open(os.path.join(BASE_DIR, "..", "schemas", "courier_state_events.avsc")) as f:
    courier_schema = parse_schema(json.load(f))
    
#--------

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

zone_weights = config.get("zone_demand_weights")
if zone_weights is None:
    zone_weights = [1.0] * len(zones)

if len(zone_weights) != len(zones):
    raise ValueError("zone_demand_weights length must be equal to the number of zones")

# Restaurants mapping (for weighted selection)
restaurants_by_zone = {z: [] for z in zones}
for r in restaurants:
    parts = r.split("_")
    z = f"zone_{parts[2]}"
    restaurants_by_zone[z].append(r)

couriers = [f"courier_{i}" for i in range(config["couriers"])]
courier_is_online = {cid: True for cid in couriers}
courier_home_zone = {cid: random.choice(zones) for cid in couriers}

# -------

# Simulation
order_events = []
courier_events = []

# Configurable start time for reproducible peaks
now = datetime.utcnow()
start_hour = int(config.get("start_hour", 11))
start_weekday = int(config.get("start_weekday", 5))

start_time = now.replace(hour=start_hour, minute=0, second=0, microsecond=0)
while start_time.weekday() != start_weekday:
    start_time += timedelta(days=1)

zone_centers = {
    z: (40.0 + i * 0.02, -3.7 + i * 0.02)
    for i, z in enumerate(zones)
}

def jitter_location(zone_id, scale=0.005):
    base_lat, base_lon = zone_centers[zone_id]
    return (
        base_lat + random.uniform(-scale, scale),
        base_lon + random.uniform(-scale, scale),
    )

# Initial Online event for each courier
for courier_id in couriers:
    courier_events.append({
        "schema_version": 1,
        "event_id": str(uuid.uuid4()),
        "event_type": "ONLINE",
        "courier_id": courier_id,
        "order_id": None,
        "zone_id": courier_home_zone[courier_id],
        "latitude": None,
        "longitude": None,
        "event_time": millis(start_time),
        "ingest_time": millis(start_time)
    })

for minute in range(config["simulation_minutes"]):
    simulated_time = start_time + timedelta(minutes=minute)

    # Lunch / Dinner Peaks
    hour = simulated_time.hour
    weekday = simulated_time.weekday()
    multiplier = 1

    if 11 <= hour <= 14:
        multiplier = config["lunch_peak_multiplier"]
    elif 18 <= hour <= 21:
        multiplier = config["dinner_peak_multiplier"]

    # Weekend boost
    if weekday >= 5:  # Saturday (5) or Sunday (6)
        multiplier *= 1.3

    # Promo/surge period simulation
    if random.random() < config.get("promo_probability", 0.0):
        multiplier *= config.get("promo_multiplier", 1.0)
    
    orders_this_minute = int(config["base_orders_per_minute"] * multiplier)

    for _ in range(orders_this_minute):

        sequence = 1

        order_id = str(uuid.uuid4())
        chosen_zone = random.choices(zones, weights=zone_weights, k=1)[0]
        restaurant_id = random.choice(restaurants_by_zone[chosen_zone])
        zone_number = restaurant_id.split("_")[2]
        zone_id = f"zone_{zone_number}"
        online_pool = [cid for cid, online in courier_is_online.items() if online]

        # Prefer couriers whose home zone matches the order zone
        zone_matched = [cid for cid in online_pool if courier_home_zone[cid] == zone_id]
        if zone_matched:
            courier_id = random.choice(zone_matched)
        elif online_pool:
            courier_id = random.choice(online_pool)
        else:
            courier_id = random.choice(couriers)
        order_value = round(min(max(random.lognormvariate(3, 0.5), 8), 80), 2)

        # Durations
        prep_minutes = random.randint(5, 20)
        delivery_minutes = random.randint(10, 30)

        # Anomaly injection
        if random.random() < config["anomaly_probability"]:
            r = random.random()
            if r < 0.2:
                delivery_minutes = -random.randint(1, 10) # rare time inversion anomaly
            elif r < 0.6:
                delivery_minutes = random.randint(1, 5) # unrealistically fast delivery
            else:
                delivery_minutes = random.randint(60, 120) # unrealistically slow delivery

        # Event timeline
        created_time = simulated_time
        accepted_time = created_time + timedelta(minutes=1)
        prep_done_time = accepted_time + timedelta(minutes=prep_minutes)
        pickup_delay = random.randint(1, 8)
        pickup_time = prep_done_time + timedelta(minutes=pickup_delay)
        delivered_time = pickup_time + timedelta(minutes=delivery_minutes)

        # Missing step simulation
        skip_pickup = random.random() < config["missing_step_probability"]

        # Cancellation simulation
        cancelled = random.random() < config["cancellation_probability"]

        # ORDER EVENTS

        def create_order_event(event_type, event_time, courier=None, seq=None):
            ingest_time = maybe_late(event_time)

            return {
                "schema_version": 1,
                "event_id": str(uuid.uuid4()),
                "event_sequence": seq,
                "event_type": event_type,
                "order_id": order_id,
                "restaurant_id": restaurant_id,
                "courier_id": courier,
                "zone_id": zone_id,
                "order_value": order_value,
                "event_time": millis(event_time),
                "ingest_time": millis(ingest_time)
            }

        maybe_duplicate(create_order_event("ORDER_CREATED", created_time, seq=sequence), order_events)
        sequence += 1
        maybe_duplicate(create_order_event("RESTAURANT_ACCEPTED", accepted_time, seq=sequence), order_events)
        sequence += 1

        # PREP_STARTED (not always present)
        if random.random() < 0.7:  # 70% of orders include it
            prep_started_time = accepted_time + timedelta(minutes=1)
            maybe_duplicate(create_order_event("PREP_STARTED", prep_started_time, seq=sequence), order_events)
            sequence += 1

        if cancelled:
            cancel_time_options = [
                created_time + timedelta(minutes=random.randint(0, 2)),
                accepted_time + timedelta(minutes=random.randint(0, 3)),
                prep_done_time - timedelta(minutes=random.randint(1, 3)),
            ]

            # Rare late cancellations (edge case)
            if random.random() < 0.2:
                cancel_time_options.append(prep_done_time + timedelta(minutes=random.randint(0, 3)))
            if (not skip_pickup) and random.random() < 0.05:
                cancel_time_options.append(pickup_time + timedelta(minutes=random.randint(0, 2)))
            
            cancel_time = random.choice(cancel_time_options)

            maybe_duplicate(create_order_event("CANCELLED", cancel_time, seq=sequence), order_events)
            continue

        maybe_duplicate(create_order_event("PREP_DONE", prep_done_time, seq=sequence), order_events)
        sequence += 1
        maybe_duplicate(create_order_event("COURIER_ASSIGNED", prep_done_time, courier_id, seq=sequence), order_events)
        sequence += 1

        if not skip_pickup:
            maybe_duplicate(create_order_event("PICKED_UP", pickup_time, courier_id, seq=sequence), order_events)
            sequence += 1

        maybe_duplicate(create_order_event("DELIVERED", delivered_time, courier_id, seq=sequence), order_events)
        sequence += 1

        # COURIER EVENTS

        def create_courier_event(event_type, event_time, order=None, lat=None, lon=None):
            ingest_time = maybe_late(event_time)

            if event_type == "OFFLINE":
                courier_is_online[courier_id] = False
            elif event_type == "ONLINE":
                courier_is_online[courier_id] = True

            return {
                "schema_version": 1,
                "event_id": str(uuid.uuid4()),
                "event_type": event_type,
                "courier_id": courier_id,
                "order_id": order,
                "zone_id": zone_id,
                "latitude": lat,
                "longitude": lon,
                "event_time": millis(event_time),
                "ingest_time": millis(ingest_time)
            }

        arrival_restaurant_time = prep_done_time - timedelta(minutes=random.randint(0, 5))
        maybe_duplicate(create_courier_event("ARRIVED_RESTAURANT", arrival_restaurant_time, order_id), courier_events)

        if not skip_pickup:
            maybe_duplicate(create_courier_event("PICKED_UP_ORDER", pickup_time, order_id), courier_events)

        # Mid-delivery location update (IF pickup event exists)
        if (not skip_pickup) and delivery_minutes > 0:
            mid_time = pickup_time + timedelta(minutes=delivery_minutes // 2)
            lat, lon = jitter_location(zone_id)
            maybe_duplicate(
                create_courier_event("LOCATION_UPDATE", mid_time, order_id, lat=lat, lon=lon),
                courier_events
            )

        if delivery_minutes > 0:
            maybe_duplicate(create_courier_event("ARRIVED_CUSTOMER", delivered_time, order_id), courier_events)
        else:
            pass

        if random.random() < config["courier_offline_probability"]:
            if (not skip_pickup) and delivery_minutes > 0 and random.random() < 0.5:
                offline_time = pickup_time + timedelta(minutes=max(1, delivery_minutes // 3))
                lat, lon = jitter_location(zone_id) #Last known location update right before going offline
                maybe_duplicate(create_courier_event("LOCATION_UPDATE", offline_time - timedelta(seconds=30), order_id, lat=lat, lon=lon), courier_events)
            else:
                offline_time = delivered_time + timedelta(minutes=1)

            maybe_duplicate(create_courier_event("OFFLINE", offline_time), courier_events)

            # Come back online later
            comeback_delay = random.randint(5, 20)
            online_again_time = offline_time + timedelta(minutes=comeback_delay)
            maybe_duplicate(create_courier_event("ONLINE", online_again_time), courier_events)

#---------

# Simulate broker arrival order
order_events.sort(key=lambda e: e["ingest_time"])
courier_events.sort(key=lambda e: e["ingest_time"])

# Write JSON
order_json_path = os.path.join(SAMPLES_JSON_DIR, "order_events_sample.jsonl")
courier_json_path = os.path.join(SAMPLES_JSON_DIR, "courier_state_events_sample.jsonl")

with open(order_json_path, "w") as f:
    for e in order_events:
        f.write(json.dumps(e) + "\n")

with open(courier_json_path, "w") as f:
    for e in courier_events:
        f.write(json.dumps(e) + "\n")

# Write AVRO
order_avro_path = os.path.join(SAMPLES_AVRO_DIR, "order_events_sample.avro")
courier_avro_path = os.path.join(SAMPLES_AVRO_DIR, "courier_state_events_sample.avro")

with open(order_avro_path, "wb") as out:
    writer(out, order_schema, order_events)

with open(courier_avro_path, "wb") as out:
    writer(out, courier_schema, courier_events)

print("Sample data has been generated successfully.")
