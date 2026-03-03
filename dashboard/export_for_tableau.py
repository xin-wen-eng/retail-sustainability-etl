import pandas as pd
import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()

conn = psycopg2.connect(
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT"),
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD")
)

# Main analytics query
df = pd.read_sql("""
    SELECT 
        d.full_date,
        d.year,
        d.month,
        d.quarter,
        l.warehouse_name,
        l.city,
        l.region,
        p.product_name,
        COALESCE(p.category, 'Electronics') as category,
        f.units_sold,
        f.revenue,
        f.co2_kg,
        f.energy_kwh,
        f.waste_kg
    FROM fact_sustainability f
    JOIN dim_date d ON f.date_id = d.date_id
    JOIN dim_location l ON f.location_id = l.location_id
    JOIN dim_product p ON f.product_id = p.product_id
    ORDER BY d.full_date, l.warehouse_name, p.product_name
""", conn)

conn.close()

df["category"] = df["category"].fillna("Electronics")
df.to_csv("dashboard/sustainability_analytics.csv", index=False)
print(f"Exported {len(df)} records to dashboard/sustainability_analytics.csv")
