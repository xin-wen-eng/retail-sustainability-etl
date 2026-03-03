from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import psycopg2
from psycopg2.extras import execute_batch
import os
import logging

default_args = {
    "owner": "xin",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

DB_CONFIG = {
    "host": "host.docker.internal",
    "port": 5432,
    "dbname": "retail_sustainability",
    "user": "tanair",
    "password": "",
}

WAREHOUSE_META = {
    "Seattle Hub": {"city": "Seattle", "region": "West"},
    "Portland DC": {"city": "Portland", "region": "West"},
    "Chicago Center": {"city": "Chicago", "region": "Midwest"},
    "Atlanta South": {"city": "Atlanta", "region": "South"},
}

DATA_PATH = "/opt/airflow/project/data/raw"

def extract(**context):
    sales = pd.read_csv(f"{DATA_PATH}/source1_sales.csv")
    energy = pd.read_csv(f"{DATA_PATH}/source2_energy.csv")
    waste = pd.read_csv(f"{DATA_PATH}/source3_waste.csv")
    logging.info(f"Extracted: sales={len(sales)}, energy={len(energy)}, waste={len(waste)}")
    context["ti"].xcom_push(key="sales", value=sales.to_json())
    context["ti"].xcom_push(key="energy", value=energy.to_json())
    context["ti"].xcom_push(key="waste", value=waste.to_json())

def transform(**context):
    ti = context["ti"]
    sales = pd.read_json(ti.xcom_pull(key="sales"))
    energy = pd.read_json(ti.xcom_pull(key="energy"))
    waste = pd.read_json(ti.xcom_pull(key="waste"))

    sales["date"] = pd.to_datetime(sales["date"])
    energy["date"] = pd.to_datetime(energy["report_date"], dayfirst=True)
    waste["date"] = pd.to_datetime(waste["trans_date"].astype(str), format="%Y%m%d")

    anomalies = len(waste[waste["waste_kg"] < 0])
    waste = waste[waste["waste_kg"] >= 0]
    waste["co2_kg"] = waste["co2_kg"].fillna(
        waste.groupby("category")["co2_kg"].transform("median")
    )

    merged = pd.merge(
        sales, waste,
        left_on=["date", "warehouse_name", "product_name"],
        right_on=["date", "wh_name", "prod_name"],
        how="left"
    )
    merged = pd.merge(
        merged, energy[["date", "location", "energy_kwh"]],
        left_on=["date", "warehouse_name"],
        right_on=["date", "location"],
        how="left"
    )
    merged["energy_kwh"] = (merged["energy_kwh"] / 5).round(4)

    logging.info(f"Transform complete: {len(merged)} records, {anomalies} anomalies removed")
    ti.xcom_push(key="merged", value=merged.to_json())
    ti.xcom_push(key="failed", value=anomalies)

def load(**context):
    ti = context["ti"]
    merged = pd.read_json(ti.xcom_pull(key="merged"))
    failed = ti.xcom_pull(key="failed")

    merged["date"] = pd.to_datetime(merged["date"])

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    conn.autocommit = False

    try:
        # dim_location
        for wh_name, meta in WAREHOUSE_META.items():
            wh_rows = merged[merged["warehouse_name"] == wh_name]
            if len(wh_rows) > 0:
                wh_id = int(wh_rows["warehouse_id"].iloc[0])
                cur.execute("""
                    INSERT INTO dim_location (location_id, warehouse_name, city, country, region)
                    VALUES (%s, %s, %s, 'US', %s)
                    ON CONFLICT (location_id) DO NOTHING
                """, (wh_id, wh_name, meta["city"], meta["region"]))

        # dim_product
        products = merged[["product_id", "product_name", "category"]].drop_duplicates()
        for _, row in products.iterrows():
            cur.execute("""
                INSERT INTO dim_product (product_id, product_name, category)
                VALUES (%s, %s, %s)
                ON CONFLICT (product_id) DO NOTHING
            """, (int(row["product_id"]), row["product_name"], row["category"]))

        # dim_date
        for d in merged["date"].dt.date.unique():
            dt = pd.Timestamp(d)
            cur.execute("""
                INSERT INTO dim_date (full_date, year, month, quarter, day_of_week)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (full_date) DO NOTHING
            """, (d, dt.year, dt.month, dt.quarter, dt.day_name()))

        # fact table
        fact_rows = []
        for _, row in merged.iterrows():
            cur.execute("SELECT date_id FROM dim_date WHERE full_date = %s", (row["date"].date(),))
            date_id = cur.fetchone()[0]
            fact_rows.append((
                date_id,
                int(row["warehouse_id"]),
                int(row["product_id"]),
                int(row["units_sold"]),
                float(row["revenue"]),
                float(row["co2_kg"]) if pd.notna(row.get("co2_kg")) else 0,
                float(row["energy_kwh"]) if pd.notna(row.get("energy_kwh")) else 0,
                float(row["waste_kg"]) if pd.notna(row.get("waste_kg")) else 0,
            ))

        execute_batch(cur, """
            INSERT INTO fact_sustainability
            (date_id, location_id, product_id, units_sold, revenue, co2_kg, energy_kwh, waste_kg)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, fact_rows)

        cur.execute("""
            INSERT INTO etl_log (source, records_processed, records_failed, status, message)
            VALUES (%s, %s, %s, %s, %s)
        """, ("airflow_dag", len(fact_rows), failed, "SUCCESS",
              f"Loaded {len(fact_rows)} records"))

        conn.commit()
        logging.info(f"Loaded {len(fact_rows)} fact records")

    except Exception as e:
        conn.rollback()
        logging.error(f"Load failed: {e}")
        raise
    finally:
        cur.close()
        conn.close()

with DAG(
    dag_id="retail_sustainability_etl",
    default_args=default_args,
    description="Daily ETL for retail sustainability data",
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["retail", "etl", "sustainability"],
) as dag:

    t1 = PythonOperator(task_id="extract", python_callable=extract)
    t2 = PythonOperator(task_id="transform", python_callable=transform)
    t3 = PythonOperator(task_id="load", python_callable=load)

    t1 >> t2 >> t3
