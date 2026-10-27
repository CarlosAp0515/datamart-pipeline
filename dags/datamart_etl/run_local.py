"""Prueba manual de extracción + transformación (sin Airflow)."""
import json

from datamart_etl import extract, transform

RAW = "/opt/airflow/data/raw"
STG = "/opt/airflow/data/staging"

print(extract.extract_source(RAW, STG, "data.csv", "sales_daily"))
print(extract.extract_source(RAW, STG, "online_retail_history.csv", "history"))
print(json.dumps(transform.run_transform(STG, "Europe/London"), indent=2, ensure_ascii=False))