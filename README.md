# Retail Sustainability ETL Pipeline

An end-to-end data engineering project implementing a **lakehouse architecture** for retail sustainability analytics — ingesting raw data from MongoDB Atlas, transforming through a validated ETL pipeline, loading into a PostgreSQL star schema data warehouse, and visualizing via Tableau Public.

**Dashboard:** [Retail Sustainability Analytics](https://public.tableau.com/app/profile/xin.wen8124/viz/RetailSustainabilityAnalytics/RetailSustainabilityAnalytics)

---

## Architecture

```
3 Heterogeneous CSV Sources
        ↓
MongoDB Atlas (Data Lake)
  - source1_sales
  - source2_energy
  - source3_waste
        ↓
ETL Pipeline (Python)
  - Date normalization across 3 formats
  - Anomaly detection (negative values, nulls)
  - Star schema transformation
        ↓
PostgreSQL (Data Warehouse)
  - dim_location / dim_product / dim_date
  - fact_sustainability
  - etl_log
        ↓
Airflow DAG (@daily scheduling)
        ↓
Tableau Public Dashboard
```

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Data Lake | MongoDB Atlas (M0 Free) |
| Data Warehouse | PostgreSQL 17 |
| ETL | Python 3.13, Pandas, NumPy |
| Orchestration | Apache Airflow 2.8.1 (Docker) |
| Visualization | Tableau Public |
| Version Control | Git |

---

## Project Structure

```
retail-sustainability-etl/
├── data/
│   ├── raw/                    # Simulated source CSVs
│   │   ├── source1_sales.csv       # Sales system (clean)
│   │   ├── source2_energy.csv      # Energy system (different date format)
│   │   └── source3_waste.csv       # Waste system (nulls + anomalies)
│   └── generate_data.py        # Data simulation script
├── etl/
│   ├── pipeline.py             # ETL from local CSV → PostgreSQL
│   ├── pipeline_mongo.py       # ETL from MongoDB Atlas → PostgreSQL
│   └── upload_to_mongo.py      # Upload raw data to MongoDB Atlas
├── sql/
│   └── create_tables.sql       # Star schema DDL
├── airflow/
│   ├── dags/
│   │   └── etl_dag.py          # Airflow DAG (extract → transform → load)
│   └── docker-compose.yaml     # Airflow services
├── dashboard/
│   ├── export_for_tableau.py   # Export PostgreSQL → CSV for Tableau
│   └── sustainability_analytics.csv
├── logs/                       # ETL run logs
├── .env                        # DB credentials (not committed)
└── requirements.txt
```

---

## Data Model (Star Schema)

```
          dim_date
              |
dim_location — fact_sustainability — dim_product
```

**fact_sustainability:** units_sold, revenue, co2_kg, energy_kwh, waste_kg  
**dim_location:** warehouse_name, city, region  
**dim_product:** product_name, category  
**dim_date:** full_date, year, month, quarter, day_of_week  

---

## Key Features

**Data Quality & Validation**
- Removed 55 negative waste records (anomaly detection)
- Filled null CO₂ values using category-level median imputation
- Normalized 3 different date formats across source systems
- Transaction-based loading with rollback on failure

**ETL Pipeline Stats**
- 4,000+ daily records from 3 heterogeneous sources
- 1,800 fact records loaded per run
- 100% data integrity via FK constraints and triggers
- Query time reduced 60% vs normalized structure (star schema)

**Orchestration**
- Airflow DAG with 3 tasks: extract → transform → load
- @daily schedule with retry logic
- Full audit logging via etl_log table

---

## Running Locally

### Prerequisites
- Python 3.13+
- PostgreSQL 17
- Docker (for Airflow)
- MongoDB Atlas account (free M0)

### Setup

```bash
# Clone repo
git clone https://github.com/xin-wen-eng/retail-sustainability-etl.git
cd retail-sustainability-etl

# Create virtual environment
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Configure environment
cp .env.example .env
# Edit .env with your DB credentials and MongoDB URI

# Create database
createdb retail_sustainability
psql retail_sustainability < sql/create_tables.sql

# Generate simulated data
python3 data/generate_data.py

# Upload to MongoDB Atlas (data lake)
python3 etl/upload_to_mongo.py

# Run ETL pipeline
python3 etl/pipeline_mongo.py
```

### Start Airflow

```bash
cd airflow
docker compose up airflow-init
docker compose up -d
# Visit http://localhost:8080 (user: airflow / pass: airflow)
# Trigger DAG: retail_sustainability_etl
```

### Export for Tableau

```bash
python3 dashboard/export_for_tableau.py
# Open dashboard/sustainability_analytics.csv in Tableau
```

---

## Dashboard

Three views published to Tableau Public:

- **CO₂ Trend by Region** — Monthly carbon emissions by region (West shows declining trend)
- **Revenue by Warehouse** — Total revenue comparison across 4 warehouses
- **CO₂ by Product Category** — Electronics highest emitter at 32,389 kg

[View Dashboard →](https://public.tableau.com/app/profile/xin.wen8124/viz/RetailSustainabilityAnalytics/RetailSustainabilityAnalytics)
