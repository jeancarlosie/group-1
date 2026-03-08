# Food Delivery Real-time Analytics Pipeline

> **_NOTE:_**
> We are charlie kirk (change this depending on implementation)

## General Project Overview
On-demand food delivery platforms such as Uber Eats, Glovo, and Deliveroo operate as real-time marketplaces where the following occurs:
1) Customers place orders
2) Restaurants prepare meals
3) Couriers deliver
4) The platform continuously optimizes dispatch
5) ETAs
6) Cancellations
7) Operational health

Our project simulates the aforementioned ecosystem by utilizing two streaming event feeds
and generating synthetic **JSON + AVRO** events with realistic patterns and edge cases

The **two feeds** are designed for the following purposes:
- Event-time processing
- Late data handling
- Windowed analytics
- Forming the foundation for a real-time analytics pipeline using Spark Structured Streaming
    **Fourth point will not be present until Milestone 2*

**This current delivery for the project's first milesone will include:**
- Two distinct event streams modeling demand and supply
- AVRO schemas for both streams (with enums, optional fields, and schema_version)
- Configurable Python generator producing JSONL and AVRO samples
- Realistic streaming edge cases (late, duplicates, missing steps, anomalies)


## Team Structure
```
| Member Name | Role | Tasks & Responsibilities |
|-------|------|------------------|
| Member 1 | Tech Lead / Architect | Streaming architecture, feed separation, future Spark Structured Streaming integration |
| Gonzalo Domínguez | Schema & Streaming Engineer | AVRO schemas design & implementation, event&ingest-time semantics, streaming correctness, compatibility and evolution strategies |
| Member 3 | Data Engineer | Generator development, JSON/AVRO output, testing |
| Eva Yang Garrudo| Analytics Lead | Defines KPIs, planned streaming queries, use cases |
| Member 5 | Repository Manager | Repo structure, sample batch preparation, packaging |
| Shamil Mukhamedov | Documentation | General README, assumptions, validation of edge cases |
```

## Project Structure
```
├── generator
│   ├── README.md        # README file exclusively created for the generator, the general README.md can be found below
│   ├── config.yaml
│   ├── generate.py
│   └── requirements.txt
├── samples              # Sample JSON and AVRO batches for inspection
│   ├── avro
│   │   └── .gitkeep
│   └── json
│       └── .gitkeep
├── schemas              # .avsc files for AVRO definitions
│   ├── courier_state_events.avsc
│   └── order_events.avsc
├── docs                 # Detailed design notes and justification (missing)
└── README.md
```



## Design Notes: Feeds, Fields, Events

### Why Two Feeds?
We intentionally separate the platform into two joinable streams:
1) Order Events (Demand + lifecycle + business metrics) 
2) Courier State Events (Supply + operational signals)

This matches real-world architectures where order lifecycle and courier telemetry are produced by different services and joined downstream using stable identifiers.

**Join keys**
- `order_id` joins order lifecycle ↔ courier delivery actions
- `courier_id` links courier behavior across orders
- `zone_id` supports aggregation and partition strategies
- `restaurant_id` supports restaurant-level SLA analytics

### Feed A — `OrderEvent` (order_events.avsc)
Represents the lifecycle of an order.

**Event Types**
- `ORDER_CREATED`
- `RESTAURANT_ACCEPTED`
- `PREP_STARTED` *(schema supports it even if generator doesn’t always emit it)*
- `PREP_DONE`
- `COURIER_ASSIGNED`
- `PICKED_UP`
- `DELIVERED`
- `CANCELLED`

**Key Fields**
- `schema_version` (int): supports schema evolution over time
- `event_id` (string): unique event identifier (supports dedup downstream)
- `event_type` (enum): lifecycle stage
- `order_id` (string): stable order identifier
- `restaurant_id` (string): restaurant entity identifier
- `courier_id` (nullable string): present after courier assignment
- `zone_id` (string): geographic zone for aggregation
- `order_value` (nullable double): supports revenue KPIs
- `event_time` (timestamp-millis): event-time for analytics
- `ingest_time` (timestamp-millis): arrival time (used to simulate late/out-of-order events)

### Feed B — `CourierStateEvent` (courier_state_events.avsc)
Represents courier availability and operational delivery milestones.

**Event Types**
- `ONLINE`
- `OFFLINE`
- `LOCATION_UPDATE` *(schema supports it; generator focuses on milestones)*
- `ARRIVED_RESTAURANT`
- `PICKED_UP_ORDER`
- `ARRIVED_CUSTOMER`

**Key Fields**
- `schema_version` (int): schema evolution
- `event_id` (string): unique event identifier
- `event_type` (enum): courier status / milestone type
- `courier_id` (string): courier identifier
- `order_id` (nullable string): links milestone to an order when relevant
- `zone_id` (string): aggregation and partitioning
- `event_time` (timestamp-millis): event-time (truth)
- `ingest_time` (timestamp-millis): used to simulate late data


## Data Generation & Realism
### Realistic Distributions
The generator supports:
- Lunch and dinner peaks (multipliers)
- Zone-level skew via restaurant assignment
- Configurable number of zones/restaurants/couriers
- Randomized prep and delivery durations
- Order value distribution (uniform in current version)

### Streaming Correctness Edge Cases
To demonstrate watermarking, deduplication, and event-time correctness later, we inject:
- **Late/out-of-order events** (`late_event_probability`)
- **Duplicates** (`duplicate_probability`)
- **Missing steps** (e.g., DELIVERED without PICKED_UP) (`missing_step_probability`)
- **Impossible durations** (negative delivery time) (`anomaly_probability`)
- **Courier offline mid-delivery** (`courier_offline_probability`)
- **Cancellations** (`cancellation_probability`)

## Assumptions
- **Zones are abstract** (e.g., “zone_0..zone_4”), not real coordinates.
- Orders are assigned to a restaurant; the zone is derived from restaurant.
- Each order chooses a courier id early (for simulation convenience); lifecycle events may still omit courier_id until assignment stage.
- Late data is simulated by shifting `ingest_time` relative to `event_time` (2–10 minutes).
- Duplicates are exact copies (same event_id), enabling realistic dedup logic in streaming.
- The generator is designed for correctness demonstrations, not perfect behavioral realism.

## Planned Analytics (Far More Applicable for Second Milestone)

### Basic (Windowed KPIs)
- Orders per zone per 1–5 minute tumbling window
- Revenue per zone/restaurant window
- Cancellation rate per zone window

### Intermediate (Stateful / Session)
- Courier online sessions (ONLINE → OFFLINE session windows)
- Demand–supply health per zone:
  - orders awaiting pickup vs active couriers
- Restaurant SLA monitoring:
  - prep time percentiles per restaurant/zone (event-time)

### Advanced (Anomaly/Fraud/Forecast)
- Delivery time anomaly detection per zone (with late data)
- Fraud heuristics:
  - repeated cancellations by zone/user-device proxy (future extension)
- Surge indicator:
  - near-real-time detection of overload zones using recent trends

## How to Run (Generator)
### Install dependencies
```bash
pip install fastavro pyyaml
