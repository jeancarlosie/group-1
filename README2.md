# Food Delivery Real-time Analytics Pipeline — Milestone 2

> **Milestone 1** covered schema design, AVRO producers, and the offline generator.
> **This milestone (Milestone 2)** covers the end-to-end real-time pipeline: Azure Event Hubs ingestion, Spark Structured Streaming processing, Parquet storage on Azure Blob, and a live Streamlit dashboard.

---

## Architecture Overview

```
Python Producers (x2)
        │
        │  AVRO over Kafka protocol
        ▼
Azure Event Hubs
├── group_01_orders
└── group_01_courier_state_events
        │
        │  Spark Structured Streaming
        ▼
Spark (Google Colab)
├── AVRO deserialization (from_avro)
├── Analytical queries (windowed + stateless)
└── Blob write (Parquet, 10s trigger)
        │
        ▼
Azure Blob Storage — container: group01output
├── orders/                        ← raw order events (Parquet)
├── courier_state_events/          ← raw courier events (Parquet)
├── checkpoints/                   ← Spark offset checkpoints
└── analytics/
    ├── zone_demand/
    ├── cancellations/
    ├── avg_order_value/
    ├── orders_windowed/
    └── courier_windowed/
        │
        ▼
Streamlit Dashboard (dashboard/app.py)
└── Reads last 6 Parquet files (~60s rolling window)
    and renders live charts every 15 seconds
```

**Event Hub Namespace:** `iesstsabbadbab-grp-01-05`
**Blob Container:** `group01output` @ storage account `iesstsabbadbab`

---

## Repository Structure

```
group-1/
├── README.md
├── notebooks/
│   └── milestone_2.ipynb        # Full pipeline: producers → Spark → Blob → dashboard
├── producers/
│   ├── avro_orders_producer.py  # Standalone AVRO producer for order events
│   └── avro_courier_producer.py # Standalone AVRO producer for courier state events
├── dashboard/
│   └── app.py                   # Streamlit live dashboard (reads from Azure Blob)
├── schemas/
│   ├── order_events.avsc        # AVRO schema — OrderEvent
│   └── courier_state_events.avsc # AVRO schema — CourierStateEvent
├── generator/                   # Milestone 1 offline generator (this is unchanged from Milestone 1)
├── samples/                     # Sample JSONL and AVRO batches (this is unchanged from Milestone 1)
└── docs/                        # Design notes
```

---

## Team Structure

| Member | Role | Milestone 2 Responsibilities |
|--------|------|------------------------------|
| Paul Khairallah | Tech Lead / Architect | Spark session setup, JAR configuration, Event Hub integration |
| Gonzalo Domínguez | Schema & Streaming Engineer | AVRO deserialization, stream parsing, watermarking |
| Jean Carlos | Data Engineer | Producer scripts, Blob write queries, Colab orchestration |
| Eva Yang Garrudo | Analytics Lead | Analytical query design, windowed aggregations, KPI definitions |
| Alejandra Gómez | Repository Manager | Repo structure, file organisation, submission packaging |
| Shamil Mukhamedov | Documentation | README, architecture diagram, reflection |

---

## How to Run

### Prerequisites

The pipeline runs entirely on **Google Colab**. No local setup is required beyond opening the notebook and installing the contemplated dependencies in the running environment.

### Step 1 — Open the notebook
Open `notebooks/milestone_2.ipynb` in Google Colab.

### Step 2 — Run in order
The notebook is fully sequential. Run *all* cells top to bottom, without interrupting any cell:

| Section | What it does |
|---------|-------------|
| 0. Configuration | Sets Event Hub and Blob credentials |
| 1. Install dependencies | Installs `fastavro`, `confluent-kafka`, `pyspark` |
| 2. AVRO Schemas | Defines schemas in-memory |
| 3. Python Producers | Writes and launches both producer scripts as background processes |
| 4–5. Spark Setup & Session | Downloads JARs, creates SparkSession with Azure connectors |
| 6. Kafka Configuration | Configures Spark ↔ Event Hub connection settings |
| 7. Read & Deserialise Streams | Reads both Event Hubs, deserialises AVRO, exposes DataFrames |
| 8. Analytical Queries | Starts all streaming queries (memory sink for inspection) |
| 9. Inspect Results | Queries memory sinks with `spark.sql()` — wait ~30s for data |
| 10. Write to Blob | Starts Parquet write streams to Azure Blob Storage |
| 11. Read Back from Blob | Verifies Parquet files landed correctly |
| 12. Stop Queries | Manual stop cells (guarded with `if False`) |

> **Important:** producers run before Spark starts reading, thus the importance of running the notebook in order.

### Step 3 — Run the dashboard

The dashboard section at the bottom of the notebook:
1. Installs Streamlit and `pyngrok`
2. Writes `app.py` to disk
3. Launches Streamlit on port 8501
4. Prints a public ngrok URL, which you can open in a new tab

---

## Running the Dashboard Standalone

`dashboard/app.py` can be run independently of the notebook, as long as Spark is already writing Parquet files to Blob Storage.

```bash
pip install streamlit azure-storage-blob pandas pyarrow plotly streamlit-autorefresh
streamlit run dashboard/app.py
```

> **Important:** The dashboard reads only the **6 most recently modified Parquet files** per topic (~60 seconds of live data) and auto-refreshes every 15 seconds.

---

## Implemented Spark Streaming Queries

### Stateless aggregations

| Query name | Stream | Business question |
|---|---|---|
| `all_orders` | Orders | Raw event feed for inspection |
| `orders_by_zone` | Orders | ORDER_CREATED count per zone |
| `event_type_counts` | Orders | Lifecycle stage breakdown across all event types |
| `cancellations` | Orders | Cancelled orders stream + cancellation rate KPI |
| `avg_order_value` | Orders | Avg / min / max order value per zone |
| `all_courier_events` | Courier | Raw courier event feed |
| `courier_activity_by_zone` | Courier | Active couriers + event count per zone |

### Windowed aggregations (with watermarking)

| Query name | Stream | Window | Slide | Watermark |
|---|---|---|---|---|
| `orders_windowed` | Orders | 5 min | 1 min | 10 min |
| `courier_windowed` | Courier | 5 min | 1 min | 10 min |

Windowed queries use `withWatermark("event_time", "10 minutes")` to handle late-arriving events, which are injected by the producers at 10% probability.

### Data at rest (Azure Blob — Parquet)

| Path | Contents | Output mode |
|---|---|---|
| `orders/` | All raw order events | Append |
| `courier_state_events/` | All raw courier events | Append |
| `analytics/zone_demand/` | Order count per zone | Complete (foreachBatch overwrite) |
| `analytics/cancellations/` | Cancelled order events | Append |
| `analytics/avg_order_value/` | Avg/min/max value per zone | Complete (foreachBatch overwrite) |
| `analytics/orders_windowed/` | Windowed order volume per zone | Append |
| `analytics/courier_windowed/` | Windowed courier activity per zone | Append |

---

## Streaming Correctness & Edge Cases

The producers intentionally inject the following anomalies to stress-test the pipeline:

| Edge case | Probability | How it's handled |
|---|---|---|
| Late / out-of-order events | 10% | `ingest_time` shifted 2–10 min after `event_time`; watermarking handles late data |
| Duplicate events | 5% | Same `event_id` re-emitted; deduplicated downstream via `.dropDuplicates(["event_id"])` |
| Missing steps | 5% | e.g. `DELIVERED` without `PICKED_UP`; handled by AVRO `PERMISSIVE` mode |
| Anomalous durations | 5% | Negative, sub-1-min, or 60–120 min delivery times; visible in analytics as outliers |
| Courier offline mid-delivery | 5% | `OFFLINE` during active order; tests supply-demand imbalance query |
| Cancellations | 10% | `ORDER_CREATED` → `CANCELLED` flow; tracked as cancellation rate KPI |

---

## Dashboard — Live KPIs & Charts

The Streamlit dashboard (`dashboard/app.py`) displays a rolling 60-second window of live data:

**KPI tiles**
- Orders created, delivered, cancelled (with rates)
- Active couriers (based on last known state)

**Charts**
- Orders by zone (bar)
- Event type distribution (donut)
- Order lifecycle funnel (unique orders per stage)
- Order value by zone — avg / min / max (grouped bar)
- Courier event counts by type (bar)
- Courier events: zone × event type (heatmap)

Auto-refreshes every 15 seconds. Reads directly from Azure Blob, so there is no Spark dependency at runtime.
