# Generator

This folder contains the synthetic streaming data generator for the food delivery simulation pipeline. It produces realistic order lifecycle and courier state events — including intentional anomalies and data quality issues — to be used for downstream Kafka ingestion and Spark Structured Streaming processing.

---

## Overview

The generator simulates a food delivery platform across multiple zones and restaurants. It outputs two event streams:

- **Order Events** — lifecycle events per order (created → accepted → prepared → picked up → delivered / cancelled)
- **Courier State Events** — courier activity events (online, arrived at restaurant, picked up order, arrived at customer, offline)

Both streams are written as `.jsonl` (JSON Lines) and `.avro` files.

---

## Files

| File | Description |
|------|-------------|
| `generate.py` | Main script that runs the simulation and writes output files |
| `config.yaml` | Simulation parameters (zones, couriers, probabilities, etc.) |
| `requirements.txt` | Python dependencies |

---

## Configuration

All simulation behavior is controlled via `config.yaml`:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `zones` | `5` | Number of geographic zones |
| `restaurants_per_zone` | `20` | Restaurants per zone (100 total) |
| `couriers` | `50` | Number of active couriers |
| `simulation_minutes` | `10` | Duration of the simulation in minutes |
| `base_orders_per_minute` | `5` | Baseline order rate per minute |
| `lunch_peak_multiplier` | `2.0` | Order rate multiplier during lunch (11:00–14:00) |
| `dinner_peak_multiplier` | `2.5` | Order rate multiplier during dinner (18:00–21:00) |
| `cancellation_probability` | `0.1` | Probability an order gets cancelled after acceptance |
| `duplicate_probability` | `0.05` | Probability of a duplicate event being emitted |
| `late_event_probability` | `0.1` | Probability an event arrives with a delay (2–10 min) |
| `missing_step_probability` | `0.05` | Probability the `PICKED_UP` step is skipped |
| `anomaly_probability` | `0.05` | Probability of an impossible delivery duration (negative value) |
| `courier_offline_probability` | `0.05` | Probability a courier goes offline after delivery |

---

## Data Quality Injections

The generator deliberately injects the following data quality issues to simulate real-world streaming challenges:

- **Duplicates** — the same event may be emitted more than once
- **Late arrivals** — `ingest_time` may lag behind `event_time` by 2–10 minutes
- **Missing steps** — the `PICKED_UP` / `PICKED_UP_ORDER` step may be absent
- **Anomalies** — `delivery_minutes` can be set to `-5` (impossible value)
- **Cancellations** — orders may be cancelled after restaurant acceptance, truncating the event sequence

---

## Output

Running the script generates the following files:
