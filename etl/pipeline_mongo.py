import pandas as pd
import numpy as np
import psycopg2
from psycopg2.extras import execute_batch
from pymongo import MongoClient
from dotenv import load_dotenv
import os
import logging
from datetime import datetime

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(f"logs/etl_mongo_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

WAREHOUSE_META = {
    "Seattle Hub": {"city": "Seattle", "region": "West"},
    "Portland DC": {"city": "Portland", "region": "West"},
    "Chicago Center": {"city": "Chicago", "region": "Midwest"},
    "Atlanta South": {"city": "Atlanta", "region": "South"},
}

def get_pg_conn():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD")
    )

def extract_from_mongo():
    log.info("Extracting from MongoDB Atlas...")
    client = MongoClient(os.getenv("MONGO_URI"))
    db = client["retail_etl_raw"]
    sales = pd.DataFrame(list(db["source1_sales"].find({}, {"_id": 0})))
    energy = pd.DataFrame(list(db["source2_energy"].find({}, {"_id": 0})))
    waste = pd.DataFrame(list(db["source3_waste"].find({}, {"_id": 0})))
    client.close()
    log.info(f"Extracted: sales={len(sales)}, energy={len(energy)}, waste={len(waste)}")
    return sales, energy, waste

def transform(sales, energy, waste):
    log.info("Transforming...")
    sales["date"] = pd.to_datetime(sales["date"])
    energy["date"] = pd.to_datetime(energy["report_date"], dayfirst=True)
    waste["date"] = pd.to_datetime(waste["trans_date"].astype(str), format="%Y%m%d")

    anomalies = len(waste[waste["waste_kg"] < 0])
    waste = waste[waste["waste_kg"] >= 0]
    waste["co2_kg"] = waste["co2_kg"].fillna(
        waste.groupby("category")["co2_kg"].transform("median")
    )

    merged = pd.merge(sales, waste,
        left_on=["date", "warehouse_name", "product_name"],
        right_on=["date", "wh_name", "prod_name"], how="left")
    merged = pd.merge(merged, energy[["date", "location", "energy_kwh"]],
        left_on=["date", "warehouse_name"],
        right_on=["date", "location"], how="left")
    merged["energy_kwh"] = (merged["energy_kwh"] / 5).round(4)

    log.info(f"Transform complete: {len(merged)} records, {anomalies} anomalies removed")
    return merged, anomalies

def load(merged, failed):
    log.info("Loading to PostgreSQL...")
    conn = get_pg_conn()
    cur = conn.cursor()
    conn.autocommit = False

    try:
        for wh_name, meta in WAREHOUSE_META.items():
            wh_rows = merged[merged["warehouse_name"] == wh_name]
            if len(wh_rows) > 0:
                wh_id = int(wh_rows["warehouse_id"].iloc[0])
                cur.execute("""
                    INSERT INTO dim_location (location_id, warehouse_name, city, country, region)
                    VALUES (%s, %s, %s, 'US', %s)
                    ON CONFLICT (location_id) DO NOTHING
                """, (wh_id, wh_name, meta["city"], meta["region"]))

        products = merged[["product_id", "product_name", "category"]].drop_duplicates()
        for _, row in products.iterrows():
            cur.execute("""
                INSERT INTO dim_product (product_id, product_name, category)
                VALUES (%s, %s, %s)
                ON CONFLICT (product_id) DO NOTHING
            """, (int(row["product_id"]), row["product_name"], row["category"]))

        for d in merged["date"].dt.date.unique():
            dt = pd.Timestamp(d)
            cur.execute("""
                INSERT INTO dim_date (full_date, year, month, quarter, day_of_week)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (full_date) DO NOTHING
            """, (d, dt.year, dt.month, dt.quarter, dt.day_name()))

        fact_rows = []
        for _, row in merged.iterrows():
            cur.execute("SELECT date_id FROM dim_date WHERE full_date = %s", (row["date"].date(),))
            date_id = cur.fetchone()[0]
            fact_rows.append((
                date_id, int(row["warehouse_id"]), int(row["product_id"]),
                int(row["units_sold"]), float(row["revenue"]),
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
        """, ("mongodb_atlas", len(fact_rows), failed, "SUCCESS",
              f"Loaded {len(fact_rows)} records from MongoDB Atlas"))

        conn.commit()
        log.info(f"Loaded {len(fact_rows)} fact records successfully")
        return len(fact_rows)

    except Exception as e:
        conn.rollback()
        log.error(f"Load failed: {e}")
        raise
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    log.info("=== ETL Pipeline (MongoDB → PostgreSQL) Start ===")
    sales, energy, waste = extract_from_mongo()
    merged, failed = transform(sales, energy, waste)

    # Clear existing fact data before reload
    conn = get_pg_conn()
    conn.autocommit = True
    conn.cursor().execute("TRUNCATE TABLE fact_sustainability RESTART IDENTITY CASCADE")
    conn.close()

    loaded = load(merged, failed)
    log.info(f"=== Pipeline Complete: {loaded} records loaded ===")
