import sys, uuid, random, io, time
from datetime import datetime, timedelta
import fastavro
from fastavro import parse_schema
from confluent_kafka import Producer

# AVRO Schema
order_schema_dict = {
    "type": "record",
    "name": "OrderEvent",
    "namespace": "com.fooddelivery.events",
    "fields": [
        {"name": "schema_version", "type": "int",  "default": 1},
        {"name": "event_id",       "type": "string"},
        {"name": "event_sequence", "type": ["null", "int"], "default": None},
        {"name": "event_type",
         "type": {
             "type": "enum", "name": "OrderEventType",
             "symbols": ["ORDER_CREATED", "RESTAURANT_ACCEPTED", "PREP_STARTED",
                         "PREP_DONE", "COURIER_ASSIGNED", "PICKED_UP", "DELIVERED", "CANCELLED"]
         }},
        {"name": "order_id",      "type": "string"},
        {"name": "restaurant_id", "type": "string"},
        {"name": "zone_id",       "type": "string"},
        {"name": "courier_id",    "type": ["null", "string"], "default": None},
        {"name": "order_value",   "type": ["null", "double"], "default": None},
        {"name": "event_time",  "type": "long"},
        {"name": "ingest_time", "type": "long"}
    ]
}
parsed_schema = parse_schema(order_schema_dict)

# Simulation parameters (config.yaml)
ZONES = [f'zone_{i}' for i in range(5)]
RESTAURANTS = {z: [f'rest_{z}_{i}' for i in range(20)] for z in ZONES}
COURIERS = [f'courier_{i}' for i in range(50)]
ZONE_WEIGHTS = [1.0, 1.0, 1.8, 0.7, 1.3]

CANCELLATION_PROB = 0.10
DUPLICATE_PROB = 0.05
LATE_EVENT_PROB = 0.10
MISSING_STEP_PROB = 0.05
ANOMALY_PROB = 0.05
BASE_ORDERS_PER_BATCH = 5
BATCH_SLEEP_SECONDS = 1.0

# Helpers
def millis(dt):
    return int(dt.timestamp() * 1000)

def maybe_late(t):
    if random.random() < LATE_EVENT_PROB:
        return t + timedelta(minutes=random.randint(2, 10))
    return t

def avro_serialize(record):
    with io.BytesIO() as buf:
        fastavro.schemaless_writer(buf, parsed_schema, record)
        return buf.getvalue()

def delivery_report(err, msg):
    if err:
        print(f'[ERROR] Delivery failed: {err}')
    else:
        print(f'[OK] {msg.topic()} partition={msg.partition()} offset={msg.offset()}')

# Argument parsing
if len(sys.argv) < 4:
    print('Usage: python avro_orders_producer.py <namespace> <eventhub_name> <connection_string>')
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
    'client.id':         'group01-orders-producer'
}
producer = Producer(**conf)
print(f'Orders producer started -> {eventhub_name} @ {event_hub_namespace}')

# Main production loop
batch = 0
while True:
    batch += 1
    now = datetime.utcnow()

    hour = now.hour
    multiplier = 1.0
    if 11 <= hour <= 14:
        multiplier = 2.0
    elif 18 <= hour <= 21:
        multiplier = 2.5
    if now.weekday() >= 5:
        multiplier *= 1.3

    n_orders = max(1, int(BASE_ORDERS_PER_BATCH * multiplier))

    for _ in range(n_orders):
        order_id    = str(uuid.uuid4())
        zone_id     = random.choices(ZONES, weights=ZONE_WEIGHTS, k=1)[0]
        restaurant  = random.choice(RESTAURANTS[zone_id])
        courier_id  = random.choice(COURIERS)
        order_value = round(min(max(random.lognormvariate(3, 0.5), 8), 80), 2)

        prep_min     = random.randint(5, 20)
        delivery_min = random.randint(10, 30)

        if random.random() < ANOMALY_PROB:
            r = random.random()
            if r < 0.2:
                delivery_min = -random.randint(1, 10)
            elif r < 0.6:
                delivery_min = random.randint(1, 5)
            else:
                delivery_min = random.randint(60, 120)

        t_created   = now
        t_accepted  = t_created  + timedelta(minutes=1)
        t_prep_done = t_accepted + timedelta(minutes=prep_min)
        t_pickup    = t_prep_done + timedelta(minutes=random.randint(1, 8))
        t_delivered = t_pickup + timedelta(minutes=delivery_min)

        skip_pickup = random.random() < MISSING_STEP_PROB
        cancelled   = random.random() < CANCELLATION_PROB

        def make_event(etype, t, cid=None, seq=None):
            return {
                'schema_version': 1,
                'event_id':       str(uuid.uuid4()),
                'event_sequence': seq,
                'event_type':     etype,
                'order_id':       order_id,
                'restaurant_id':  restaurant,
                'zone_id':        zone_id,
                'courier_id':     cid,
                'order_value':    order_value,
                'event_time':     millis(t),
                'ingest_time':    millis(maybe_late(t))
            }

        seq = 1
        events = []
        events.append(make_event('ORDER_CREATED',       t_created,  seq=seq)); seq += 1
        events.append(make_event('RESTAURANT_ACCEPTED', t_accepted, seq=seq)); seq += 1

        if random.random() < 0.95:
            events.append(make_event('PREP_STARTED',
                          t_accepted + timedelta(minutes=1), seq=seq)); seq += 1

        if cancelled:
            events.append(make_event('CANCELLED',
                          t_accepted + timedelta(minutes=random.randint(0, 3)), seq=seq))
        else:
            events.append(make_event('PREP_DONE',        t_prep_done, seq=seq)); seq += 1
            events.append(make_event('COURIER_ASSIGNED', t_prep_done, courier_id, seq=seq)); seq += 1
            if not skip_pickup:
                events.append(make_event('PICKED_UP', t_pickup, courier_id, seq=seq)); seq += 1
            events.append(make_event('DELIVERED', t_delivered, courier_id, seq=seq))

        for ev in events:
            copies = [ev]
            if random.random() < DUPLICATE_PROB:
                copies.append(ev.copy())
            for copy in copies:
                producer.produce(topic=eventhub_name, value=avro_serialize(copy), callback=delivery_report)
                producer.poll(0)

    producer.flush()
    print(f'[Batch {batch}] {n_orders} orders @ {now.strftime("%H:%M:%S")} (x{multiplier:.1f})')
    time.sleep(BATCH_SLEEP_SECONDS)
