import pandas as pd
import numpy as np
from faker import Faker
import random
from datetime import datetime, timedelta
import os

fake = Faker()
random.seed(42)
np.random.seed(42)

WAREHOUSES = [
    {"id": 1, "name": "Seattle Hub", "city": "Seattle", "country": "US", "region": "West"},
    {"id": 2, "name": "Portland DC", "city": "Portland", "country": "US", "region": "West"},
    {"id": 3, "name": "Chicago Center", "city": "Chicago", "country": "US", "region": "Midwest"},
    {"id": 4, "name": "Atlanta South", "city": "Atlanta", "country": "US", "region": "South"},
]

PRODUCTS = [
    {"id": 1, "name": "Organic Cotton T-Shirt", "category": "Apparel", "subcategory": "Tops"},
    {"id": 2, "name": "Recycled Plastic Bottle", "category": "Beverage", "subcategory": "Water"},
    {"id": 3, "name": "Bamboo Cutting Board", "category": "Kitchen", "subcategory": "Utensils"},
    {"id": 4, "name": "Solar Phone Charger", "category": "Electronics", "subcategory": "Accessories"},
    {"id": 5, "name": "Compostable Packaging", "category": "Packaging", "subcategory": "Eco"},
]

CO2_FACTORS = {
    "Apparel": 2.5, "Beverage": 0.3, "Kitchen": 1.2,
    "Electronics": 8.5, "Packaging": 0.8
}

def generate_dates(n=90):
    start = datetime(2025, 10, 1)
    return [start + timedelta(days=i) for i in range(n)]

dates = generate_dates()

# Source 1: Sales System (clean CSV)
def gen_source1():
    rows = []
    for date in dates:
        for wh in WAREHOUSES:
            for prod in PRODUCTS:
                units = random.randint(10, 200)
                price = round(random.uniform(5, 150), 2)
                rows.append({
                    "date": date.strftime("%Y-%m-%d"),
                    "warehouse_id": wh["id"],
                    "warehouse_name": wh["name"],
                    "product_id": prod["id"],
                    "product_name": prod["name"],
                    "units_sold": units,
                    "revenue": round(units * price, 2)
                })
    df = pd.DataFrame(rows)
    df.to_csv("data/raw/source1_sales.csv", index=False)
    print(f"Source 1: {len(df)} records")

# Source 2: Energy System (different date format, extra columns)
def gen_source2():
    rows = []
    for date in dates:
        for wh in WAREHOUSES:
            rows.append({
                "report_date": date.strftime("%d/%m/%Y"),  # different format
                "location": wh["name"],
                "city": wh["city"],
                "energy_kwh": round(random.uniform(100, 800), 2),
                "solar_pct": round(random.uniform(0, 60), 1),
                "notes": fake.sentence() if random.random() < 0.1 else ""
            })
    df = pd.DataFrame(rows)
    df.to_csv("data/raw/source2_energy.csv", index=False)
    print(f"Source 2: {len(df)} records")

# Source 3: Waste System (JSON-like CSV, has nulls and anomalies)
def gen_source3():
    rows = []
    for date in dates:
        for wh in WAREHOUSES:
            for prod in PRODUCTS:
                waste = round(random.uniform(0.5, 20), 2)
                co2 = round(waste * CO2_FACTORS[prod["category"]] * random.uniform(0.8, 1.2), 4)
                # inject anomalies
                if random.random() < 0.02:
                    waste = -waste  # negative value anomaly
                if random.random() < 0.01:
                    co2 = None  # null anomaly
                rows.append({
                    "trans_date": date.strftime("%Y%m%d"),  # yet another format
                    "wh_name": wh["name"],
                    "prod_name": prod["name"],
                    "category": prod["category"],
                    "waste_kg": waste,
                    "co2_kg": co2,
                })
    df = pd.DataFrame(rows)
    df.to_csv("data/raw/source3_waste.csv", index=False)
    print(f"Source 3: {len(df)} records")

if __name__ == "__main__":
    os.makedirs("data/raw", exist_ok=True)
    gen_source1()
    gen_source2()
    gen_source3()
    print("Data generation complete.")
