import sys, uuid, random, io, time
from datetime import datetime, timedelta
import fastavro
from fastavro import parse_schema
from confluent_kafka import Producer

# AVRO Schema
courier_schema_dict = {
    "type": "record",
    "name": "CourierStateEvent",
    "namespace": "com.fooddelivery.events",
    "fields": [
        {"name": "schema_version", "type": "int",  "default": 1},
        {"name": "event_id",   "type": "string"},
        {"name": "event_type",
         "type": {
             "type": "enum", "name": "CourierEventType",
             "symbols": ["ONLINE", "OFFLINE", "LOCATION_UPDATE",
                         "ARRIVED_RESTAURANT", "PICKED_UP_ORDER", "ARRIVED_CUSTOMER"]
         }},
        {"name": "courier_id", "type": "string"},
        {"name": "order_id",   "type": ["null", "string"], "default": None},
        {"name": "zone_id",    "type": "string"},
        {"name": "latitude",   "type": ["null", "double"], "default": None},
        {"name": "longitude",  "type": ["null", "double"], "default": None},
        {"name": "event_time",  "type": "long"},
        {"name": "ingest_time", "type": "long"}
    ]
}
parsed_schema = parse_schema(courier_schema_dict)

# Simulation parameters
ZONES    = [f'zone_{i}' for i in range(5)]
COURIERS = [f'courier_{i}' for i in range(50)]

DUPLICATE_PROB   = 0.05
LATE_EVENT_PROB  = 0.10
OFFLINE_PROB     = 0.05
BATCH_SLEEP_SECS = 0.5

ZONE_CENTERS = {f'zone_{i}': (40.0 + i * 0.02, -3.7 + i * 0.02) for i in range(5)}
courier_zone = {c: random.choice(ZONES) for c in COURIERS}

# Helpers
def millis(dt):
    return int(dt.timestamp() * 1000)

def maybe_late(t):
    if random.random() < LATE_EVENT_PROB:
        return t + timedelta(minutes=random.randint(2, 10))
    return t

def jitter(zone):
    lat, lon = ZONE_CENTERS[zone]
    return lat + random.uniform(-0.005, 0.005), lon + random.uniform(-0.005, 0.005)

def avro_serialize(record):
    with io.BytesIO() as buf:
        fastavro.schemaless_writer(buf, parsed_schema, record)
        return buf.getvalue()

def delivery_report(err, msg):
    if err:
        print(f'[ERROR] {err}')
    else:
        print(f'[OK] {msg.topic()} partition={msg.partition()} offset={msg.offset()}')

def make_courier_event(etype, cid, zone, t, order=None, lat=None, lon=None):
    return {
        'schema_version': 1,
        'event_id':   str(uuid.uuid4()),
        'event_type': etype,
        'courier_id': cid,
        'order_id':   order,
        'zone_id':    zone,
        'latitude':   lat,
        'longitude':  lon,
        'event_time':  millis(t),
        'ingest_time': millis(maybe_late(t))
    }

# Argument parsing
if len(sys.argv) < 4:
    print('Usage: python avro_courier_producer.py <namespace> <eventhub_name> <connection_string>')
    sys.exit(1)

event_hub_namespace = sys.argv[1]
eventhub_name       = sys.argv[2]
connection_string   = sys.argv[3]

# Kafka / Event Hub Producer config
conf = {
    'bootstrap.servers': f'{event_hub_namespace}.servicebus.windows.net:9093',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms':   'PLAIN',
    'sasl.username':     '$ConnectionString',
    'sasl.password':     connection_string,
    'client.id':         'group01-courier-producer'
}
producer = Producer(**conf)
print(f'Courier producer started -> {eventhub_name} @ {event_hub_namespace}')

# Initial ONLINE burst for all 50 couriers
now = datetime.utcnow()
for cid in COURIERS:
    ev = make_courier_event('ONLINE', cid, courier_zone[cid], now)
    producer.produce(topic=eventhub_name, value=avro_serialize(ev), callback=delivery_report)
    producer.poll(0)
producer.flush()
print(f'Sent ONLINE event for {len(COURIERS)} couriers.')

# Main production loop
batch = 0
while True:
    batch += 1
    now = datetime.utcnow()
    events = []

    active = random.sample(COURIERS, k=random.randint(3, 10))

    for cid in active:
        zone     = courier_zone[cid]
        order_id = str(uuid.uuid4())
        lat, lon = jitter(zone)
        t = now

        sequence_type = random.choice(['pickup_delivery', 'restaurant_arrival', 'location_only'])

        if sequence_type == 'pickup_delivery':
            events.append(make_courier_event('ARRIVED_RESTAURANT', cid, zone, t, order_id))
            t += timedelta(seconds=random.randint(30, 120))
            events.append(make_courier_event('PICKED_UP_ORDER', cid, zone, t, order_id))
            t += timedelta(seconds=random.randint(60, 300))
            mid_lat, mid_lon = jitter(zone)
            events.append(make_courier_event('LOCATION_UPDATE', cid, zone, t, order_id, mid_lat, mid_lon))
            t += timedelta(seconds=random.randint(60, 300))
            events.append(make_courier_event('ARRIVED_CUSTOMER', cid, zone, t, order_id))
        elif sequence_type == 'restaurant_arrival':
            events.append(make_courier_event('ARRIVED_RESTAURANT', cid, zone, t, order_id))
        else:
            events.append(make_courier_event('LOCATION_UPDATE', cid, zone, t, None, lat, lon))

        if random.random() < OFFLINE_PROB:
            t_off = t + timedelta(minutes=1)
            t_on  = t_off + timedelta(minutes=random.randint(5, 20))
            events.append(make_courier_event('OFFLINE', cid, zone, t_off))
            events.append(make_courier_event('ONLINE',  cid, zone, t_on))

    for ev in events:
        copies = [ev]
        if random.random() < DUPLICATE_PROB:
            copies.append(ev.copy())
        for copy in copies:
            producer.produce(topic=eventhub_name, value=avro_serialize(copy), callback=delivery_report)
            producer.poll(0)

    producer.flush()
    print(f'[Batch {batch}] {len(events)} courier events @ {now.strftime("%H:%M:%S")}')
    time.sleep(BATCH_SLEEP_SECS)
