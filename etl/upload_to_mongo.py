import pandas as pd
from pymongo import MongoClient
from dotenv import load_dotenv
import os
import logging

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

def upload_raw_data():
    client = MongoClient(os.getenv("MONGO_URI"))
    db = client["retail_etl_raw"]

    # Upload each source as a collection
    sources = {
        "source1_sales": "data/raw/source1_sales.csv",
        "source2_energy": "data/raw/source2_energy.csv",
        "source3_waste": "data/raw/source3_waste.csv",
    }

    for collection_name, filepath in sources.items():
        df = pd.read_csv(filepath)
        records = df.to_dict("records")

        col = db[collection_name]
        col.drop()  # clear before reload
        col.insert_many(records)
        log.info(f"Uploaded {len(records)} records to MongoDB Atlas: {collection_name}")

    log.info("All raw data uploaded to MongoDB Atlas as data lake layer")
    client.close()

if __name__ == "__main__":
    upload_raw_data()
