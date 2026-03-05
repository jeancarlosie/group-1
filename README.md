# Food Delivery Real-time Analytics Pipeline

> **_NOTE:_**
> We are charlie kirk (change this depending on implementation)

## General Project Overview
```
On-demand food delivery platforms such as Uber Eats, Glovo, and Deliveroo operate as real-time marketplaces where the following occurs:
1) customers place orders
2) restaurants prepare meals
3) couriers deliver
4) the platform continuously optimizes dispatch
5) ETAs
6) cancellations
7) operational health

Our project simulates the aforementioned ecosystem by utilizing two streaming event feeds
and generating synthetic JSON + AVRO events with realistic patterns and edge cases

The feeds are designed for the following purposes:
- event-time processing
- late data handling
- windowed analytics
- forming the foundation for a real-time analytics pipeline using Spark Structured Streaming
    *Fourth point will not be present until Milestone 2

This current delivery for the project's first milesone will include
- Two distinct event streams modeling demand and supply
- AVRO schemas for both streams (with enums, optional fields, and schema_version)
- Configurable Python generator producing JSONL and AVRO samples
- Realistic streaming edge cases (late, duplicates, missing steps, anomalies)
```

## Team Structure
```
| Member Name | Role | Tasks & Responsibilities |
|-------|------|------------------|
| Member 1 | Tech Lead / Architect | Overall design, schema + event model decisions |
| Member 2 | Data Engineer | Generator development, JSON/AVRO output, testing |
| Member 3 | Analytics Lead | Defines KPIs, planned streaming queries, use cases |
| Shamil Mukhamedov | Documentation | General README, assumptions, validation of edge cases |
| Member 5 | blank role name| task |
| Member 6 | role name| task/responsibilities |
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






