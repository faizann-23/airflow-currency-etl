from datetime import datetime, timezone

import requests
from pymongo import MongoClient

try:
    from airflow.sdk import dag, task          # Airflow 3
except ImportError:
    from airflow.decorators import dag, task   # Airflow 2

API_URL = "https://api.frankfurter.dev/v2/rates"
MONGO_URI = "mongodb://admin:password123@mongo:27017/"


@dag(
    dag_id="currency_exchange_etl",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["etl", "currency"],
)
def currency_exchange_etl():

    # ---------- Task 1: Extract ----------
    @task
    def extract_rates(ti=None):
        params = {"base": "EUR", "quotes": "USD,GBP,JPY"}
        response = requests.get(API_URL, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        print("RAW API RESPONSE:", data)
        # Named XCom push
        ti.xcom_push(key="raw_rates", value=data)

    # ---------- Task 2: Transform ----------
    @task
    def transform_rates(ti=None):
        # Named XCom pull
        raw = ti.xcom_pull(task_ids="extract_rates", key="raw_rates")
        retrieved_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
        records = []

        if isinstance(raw, list):
            # v2 format: list of {date, base, quote, rate}
            for item in raw:
                records.append({
                    "base_currency": item["base"],
                    "target_currency": item["quote"],
                    "exchange_rate": item["rate"],
                    "rate_date": item["date"],
                    "source": "Frankfurter API",
                    "retrieved_at": retrieved_at,
                })
        else:
            # old format: {base, date, rates: {USD: 1.15, ...}}
            for currency, rate in raw["rates"].items():
                records.append({
                    "base_currency": raw["base"],
                    "target_currency": currency,
                    "exchange_rate": rate,
                    "rate_date": raw["date"],
                    "source": "Frankfurter API",
                    "retrieved_at": retrieved_at,
                })

        print("TRANSFORMED RECORDS:", records)
        # Named XCom push
        ti.xcom_push(key="transformed_rates", value=records)

    # ---------- Task 3: Load ----------
    @task
    def load_to_mongodb(ti=None):
        # Named XCom pull
        records = ti.xcom_pull(task_ids="transform_rates", key="transformed_rates")
        client = MongoClient(MONGO_URI)
        collection = client["currency_db"]["exchange_rates"]

        for rec in records:
            # upsert: dobara run karne par duplicate nahi banega
            collection.update_one(
                {
                    "base_currency": rec["base_currency"],
                    "target_currency": rec["target_currency"],
                    "rate_date": rec["rate_date"],
                },
                {"$set": rec},
                upsert=True,
            )
        print(f"Loaded {len(records)} records into MongoDB")
        client.close()

    extract_rates() >> transform_rates() >> load_to_mongodb()


currency_exchange_etl()