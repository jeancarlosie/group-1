# Generator

Synthetic streaming data generator for the food delivery simulation pipeline. Produces order lifecycle and courier state events, including intentional anomalies and data quality issues (written as `.jsonl` and `.avro` files).

---

## Quick Start
```bash
pip install -r requirements.txt
python generate.py
```

Output is written to `../samples/json/` and `../samples/avro/`.

---

## Files

| File | Description |
|------|-------------|
| `generate.py` | Main simulation script |
| `config.yaml` | All simulation parameters |
| `requirements.txt` | Python dependencies (`fastavro`, `pyyaml`) |

---

## Configuration

All behaviour is controlled via `config.yaml`. Parameters are grouped below by concern.

### Timing

| Parameter | Default | Description |
|-----------|---------|-------------|
| `start_hour` | `11` | Simulation start hour (UTC, 0–23) |
| `start_weekday` | `5` | Simulation start day (0 = Monday, 6 = Sunday) |
| `simulation_minutes` | `10` | Total duration of the simulation |

### Platform Scale

| Parameter | Default | Description |
|-----------|---------|-------------|
| `zones` | `5` | Number of geographic zones |
| `zone_demand_weights` | `[1.0, 1.0, 1.8, 0.7, 1.3]` | Relative demand weight per zone (length must equal `zones`) |
| `restaurants_per_zone` | `20` | Restaurants per zone |
| `couriers` | `50` | Number of active couriers |

### Demand Shaping

| Parameter | Default | Description |
|-----------|---------|-------------|
| `base_orders_per_minute` | `5` | Baseline order rate |
| `lunch_peak_multiplier` | `2.0` | Rate multiplier during lunch (11:00–14:00) |
| `dinner_peak_multiplier` | `2.5` | Rate multiplier during dinner (18:00–21:00) |
| `promo_probability` | `0.15` | Probability of a promo surge occurring each minute |
| `promo_multiplier` | `1.6` | Additional rate multiplier during a promo surge |

> Weekday vs. weekend: simulations starting on Saturday/Sunday (`start_weekday` ≥ 5) apply an additional 1.3× demand boost on top of peak multipliers.

### Streaming Edge Cases

| Parameter | Default | Description |
|-----------|---------|-------------|
| `cancellation_probability` | `0.1` | Probability an order is cancelled at some point in its lifecycle |
| `duplicate_probability` | `0.05` | Probability a duplicate event (same `event_id`) is emitted |
| `late_event_probability` | `0.1` | Probability an event's `ingest_time` is delayed 2–10 min behind `event_time` |
| `missing_step_probability` | `0.05` | Probability the `PICKED_UP` / `PICKED_UP_ORDER` step is skipped |
| `anomaly_probability` | `0.05` | Probability of an anomalous delivery duration (see below) |
| `courier_offline_probability` | `0.05` | Probability a courier goes offline mid-delivery or post-delivery |

---

## Data Quality Injections

| Issue | Behaviour |
|-------|-----------|
| **Duplicates** | Exact copy of an event re-emitted (same `event_id`) |
| **Late arrivals** | `ingest_time` lags `event_time` by 2–10 minutes |
| **Missing steps** | `PICKED_UP` / `PICKED_UP_ORDER` absent from the sequence |
| **Anomalies** | `delivery_minutes` is negative (time inversion), 1–5 min (impossibly fast), or 60–120 min (unrealistically slow) |
| **Cancellations** | Order cancelled after acceptance, after prep, or rarely after pickup |
| **Courier offline** | Courier goes `OFFLINE` mid-delivery or after drop-off, followed by a later `ONLINE` event |

---

## Output
```
samples/
├── json/
│   ├── order_events_sample.jsonl
│   └── courier_state_events_sample.jsonl
└── avro/
    ├── order_events_sample.avro
    └── courier_state_events_sample.avro
```

- **JSONL** — one event per line, useful for manual inspection
- **AVRO** — schema-enforced using `../schemas/order_events.avsc` and `../schemas/courier_state_events.avsc`

Events within each file are sorted by `ingest_time` to simulate broker arrival order.

---

## Event Schemas

### Order Events

| Field | Type | Description |
|-------|------|-------------|
| `schema_version` | int | Schema version number |
| `event_id` | string (UUID) | Unique event identifier |
| `event_sequence` | int | Intra-order sequence number for ordering guarantees |
| `event_type` | enum | `ORDER_CREATED` · `RESTAURANT_ACCEPTED` · `PREP_STARTED`* · `PREP_DONE` · `COURIER_ASSIGNED` · `PICKED_UP` · `DELIVERED` · `CANCELLED` |
| `order_id` | string (UUID) | Stable order identifier |
| `restaurant_id` | string | Restaurant that received the order |
| `courier_id` | string / null | Assigned courier — null before `COURIER_ASSIGNED` |
| `zone_id` | string | Geographic zone |
| `order_value` | float / null | Order value in € (lognormal, clamped to €8–€80) |
| `event_time` | long (ms) | Logical event time |
| `ingest_time` | long (ms) | Broker arrival time — may be later than `event_time` |

*`PREP_STARTED` is emitted for ~70% of orders.

### Courier State Events

| Field | Type | Description |
|-------|------|-------------|
| `schema_version` | int | Schema version number |
| `event_id` | string (UUID) | Unique event identifier |
| `event_type` | enum | `ONLINE` · `OFFLINE` · `ARRIVED_RESTAURANT` · `PICKED_UP_ORDER` · `LOCATION_UPDATE` · `ARRIVED_CUSTOMER` |
| `courier_id` | string | Courier identifier |
| `order_id` | string / null | Associated order — null for `ONLINE` / `OFFLINE` |
| `zone_id` | string | Geographic zone |
| `latitude` | float / null | Courier latitude — present on `LOCATION_UPDATE` events |
| `longitude` | float / null | Courier longitude — present on `LOCATION_UPDATE` events |
| `event_time` | long (ms) | Logical event time |
| `ingest_time` | long (ms) | Broker arrival time — may be later than `event_time` |
