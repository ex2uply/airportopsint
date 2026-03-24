# Skyhaven Airport Operations Intelligence Platform

> A production-grade data engineering pipeline implementing the **Bronze → Silver → Gold medallion architecture** using PySpark and Delta Lake, designed for Databricks deployment.

## Business Objective

SkyHaven International Airport processes **~550 flights/day** across **24 gates**. This platform ingests heterogeneous operational data from six sources and produces actionable KPIs:

- **Delay Propagation Risk** — cascade impact scoring per flight
- **Baggage Misconnect Flagging** — risk scoring for tight connections
- **OTP Regulatory Reporting** — DGCA-compliant on-time performance
- **Gate Utilization Analysis** — efficiency metrics and gap detection
- **Security Staffing Recommendations** — 15-minute zone-level predictions

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                             │
│  CSV: flight_schedule, baggage_scans, aircraft_master, SLAs     │
│  JSON: gate_events (NDJSON), passenger_counters (NDJSON)        │
└─────────────────────────┬───────────────────────────────────────┘
                          │
          ┌───────────────▼───────────────────┐
          │          BRONZE LAYER             │
          │  Raw ingestion + DQ sanity checks │
          │  ─ MERGE for flight_schedule      │
          │  ─ APPEND for events/scans/IoT    │
          │  ─ Referential integrity checks   │
          └───────────────┬──────────────────┘
                          │
          ┌───────────────▼───────────────────┐
          │          SILVER LAYER             │
          │  Enriched operational views       │
          │  ─ Flight-ops join                │
          │  ─ Delay propagation scoring      │
          │  ─ Baggage journey + misconnect   │
          │  ─ Zone occupancy aggregation     │
          └───────────────┬──────────────────┘
                          │
          ┌───────────────▼───────────────────┐
          │           GOLD LAYER              │
          │  Business KPIs & Reports          │
          │  ─ OTP daily regulatory report    │
          │  ─ Gate utilization summary       │
          │  ─ Security staffing recommend.   │
          └──────────────────────────────────┘
```

## Project Structure

```
DE-5/
├── notebooks/
│   ├── bronze/
│   │   ├── Task_1_1_Flight_Schedule_Bronze.ipynb
│   │   ├── Task_1_2_Gate_Events_Baggage_Scans_Bronze.ipynb
│   │   ├── Task_1_3_IoT_Counters_Bronze.ipynb
│   │   └── Task_1_4_SLA_Aircraft_Master_Bronze.ipynb
│   ├── silver/
│   │   ├── Task_2_1_Flight_Operations.ipynb
│   │   ├── Task_2_2_Delay_Propagation_Risk.ipynb
│   │   ├── Task_2_3_Baggage_Journey_Misconnect.ipynb
│   │   └── Task_2_4_Zone_Occupancy_15min.ipynb
│   └── gold/
│       ├── Task_3_1_OTP_Daily_Report.ipynb
│       ├── Task_3_2_Gate_Utilization_Summary.ipynb
│       └── Task_3_3_Security_Staffing_Recommendation.ipynb
├── dags/
│   └── airport_ops_dag.py          # Airflow dual-tier DAG
├── tests/
│   ├── test_delay_cascade.py       # Delay cascade scenario tests
│   └── test_referential_integrity.py  # RI validation report
├── airport_analytics_data/         # Raw source data (6 sources)
│   ├── flight_schedule/
│   ├── gate_events/
│   ├── baggage_scans/
│   ├── passenger_counters/
│   ├── aircraft_master/
│   └── airline_sla/
└── README.md
```

## Data Sources

| Source | Format | Rows | Update Pattern |
|--------|--------|------|----------------|
| Flight Schedule | CSV | ~2,400 | MERGE (status changes) |
| Gate Events | NDJSON | ~10,800 | Append (immutable events) |
| Baggage Scans | CSV | ~18,000 | Append (immutable scans) |
| Passenger Counters | NDJSON | ~34,500 | Append (IoT telemetry) |
| Aircraft Master | CSV | ~60 | Overwrite (reference) |
| Airline SLA | CSV/XLSX | ~8 | Overwrite (reference) |

## Setup & Execution

### Prerequisites

- Python 3.10+
- PySpark 3.5+
- Delta Lake (`delta-spark` 3.1.0+)
- Apache Airflow 2.x (for orchestration)

### Run on Databricks

1. Upload `airport_analytics_data/` to `/FileStore/airport_analytics_data/`
2. Import all `.ipynb` files from `notebooks/` into your Databricks workspace
3. Execute notebooks in order: **Bronze → Silver → Gold**

### Run Locally

```bash
pip install pyspark delta-spark pandas openpyxl
```

Execute notebooks in sequence using `jupyter notebook` or `papermill`.

### Orchestration

Deploy `dags/airport_ops_dag.py` to your Airflow environment:

- **15-minute tier** — gate_events, baggage_scans, passenger_counters
- **Daily tier** — flight_schedule, SLA/aircraft refresh, full Silver+Gold rebuild

## Git Branching Strategy

```
main ← production-ready releases
  └── dev ← integration branch
        ├── feature/flight_schedule     (Task 1.1)
        ├── feature/gate_events         (Task 1.2)
        ├── feature/iot_counters        (Task 1.3)
        ├── feature/reference_data      (Task 1.4)
        ├── feature/silver_layer        (Tasks 2.1–2.4)
        ├── feature/gold_layer          (Tasks 3.1–3.3)
        └── feature/orchestration       (Task 4.x, tests)
```

## Testing

```bash
python tests/test_delay_cascade.py
python tests/test_referential_integrity.py
```

## License

Internal project — Skyhaven International Airport.
