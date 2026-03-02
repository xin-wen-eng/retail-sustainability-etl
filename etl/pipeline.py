import pandas as pd
import numpy as np
import psycopg2
from psycopg2.extras import execute_batch
from dotenv import load_dotenv
import os
import logging
from datetime import datetime

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(f"logs/etl_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

def get_conn():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD")
    )

# ---------- EXTRACT ----------
def extract():
    log.info("Extracting from 3 sources...")
    sales = pd.read_csv("data/raw/source1_sales.csv")
    energy = pd.read_csv("data/raw/source2_energy.csv")
    waste = pd.read_csv("data/raw/source3_waste.csv")
    log.info(f"Extracted: sales={len(sales)}, energy={len(energy)}, waste={len(waste)}")
    return sales, energy, waste

# ---------- TRANSFORM ----------
def transform(sales, energy, waste):
    log.info("Transforming...")
    failed = 0

    # Normalize dates
    sales["date"] = pd.to_datetime(sales["date"])
    energy["date"] = pd.to_datetime(energy["report_date"], dayfirst=True)
    waste["date"] = pd.to_datetime(waste["trans_date"], format="%Y%m%d")

    # Validate waste anomalies
    anomalies = waste[waste["waste_kg"] < 0]
    failed += len(anomalies)
    if len(anomalies) > 0:
        log.warning(f"Removed {len(anomalies)} negative waste records")
        waste = waste[waste["waste_kg"] >= 0]

    # Fill nulls
    waste["co2_kg"] = waste["co2_kg"].fillna(
        waste.groupby("category")["co2_kg"].transform("median")
    )

    # Merge sales + waste on date, warehouse, product
    merged = pd.merge(
        sales, waste,
        left_on=["date", "warehouse_name", "product_name"],
        right_on=["date", "wh_name", "prod_name"],
        how="left"
    )

    # Merge energy on date, warehouse
    merged = pd.merge(
        merged, energy[["date", "location", "energy_kwh"]],
        left_on=["date", "warehouse_name"],
        right_on=["date", "location"],
        how="left"
    )

    # Distribute energy per product (evenly across 5 products)
    merged["energy_kwh"] = (merged["energy_kwh"] / 5).round(4)

    log.info(f"Transform complete: {len(merged)} records, {failed} failed")
    return merged, failed

# ---------- LOAD ----------
def load(merged, failed):
    log.info("Loading to PostgreSQL...")
    conn = get_conn()
    cur = conn.cursor()
    records_loaded = 0

    try:
        conn.autocommit = False

        # Load dim_location
        WAREHOUSE_META = {
            "Seattle Hub": {"city": "Seattle", "region": "West"},
            "Portland DC": {"city": "Portland", "region": "West"},
            "Chicago Center": {"city": "Chicago", "region": "Midwest"},
            "Atlanta South": {"city": "Atlanta", "region": "South"},
        }
        locations = merged[["warehouse_id", "warehouse_name"]].drop_duplicates()
        for _, row in locations.iterrows():
            meta = WAREHOUSE_META.get(row["warehouse_name"], {"city": "Unknown", "region": "Unknown"})
            cur.execute("""
                INSERT INTO dim_location (location_id, warehouse_name, city, country, region)
                VALUES (%s, %s, %s, 'US', %s)
                ON CONFLICT (location_id) DO NOTHING
            """, (int(row["warehouse_id"]), row["warehouse_name"], meta["city"], meta["region"]))

        # Load dim_product
        products = merged[["product_id", "product_name", "category"]].drop_duplicates()
        for _, row in products.iterrows():
            cur.execute("""
                INSERT INTO dim_product (product_id, product_name, category)
                VALUES (%s, %s, %s)
                ON CONFLICT (product_id) DO NOTHING
            """, (int(row["product_id"]), row["product_name"], row["category"]))

        # Load dim_date
        dates = merged["date"].dt.date.unique()
        for d in dates:
            dt = pd.Timestamp(d)
            cur.execute("""
                INSERT INTO dim_date (full_date, year, month, quarter, day_of_week)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (full_date) DO NOTHING
            """, (d, dt.year, dt.month, dt.quarter, dt.day_name()))

        # Load fact table
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

        records_loaded = len(fact_rows)

        # Log run
        cur.execute("""
            INSERT INTO etl_log (source, records_processed, records_failed, status, message)
            VALUES (%s, %s, %s, %s, %s)
        """, ("all_sources", records_loaded, failed, "SUCCESS",
              f"Loaded {records_loaded} fact records"))

        conn.commit()
        log.info(f"Loaded {records_loaded} fact records successfully")

    except Exception as e:
        conn.rollback()
        cur.execute("""
            INSERT INTO etl_log (source, records_processed, records_failed, status, message)
            VALUES (%s, %s, %s, %s, %s)
        """, ("all_sources", 0, 0, "FAILED", str(e)))
        conn.commit()
        log.error(f"Load failed, rolled back: {e}")
        raise

    finally:
        cur.close()
        conn.close()

    return records_loaded

# ---------- MAIN ----------
if __name__ == "__main__":
    log.info("=== ETL Pipeline Start ===")
    sales, energy, waste = extract()
    merged, failed = transform(sales, energy, waste)
    loaded = load(merged, failed)
    log.info(f"=== ETL Pipeline Complete: {loaded} records loaded ===")