"""
DAG diario del pipeline DataMart.

extract_sales_daily ─┐
                     ├─> transform_clean_and_split -> quality_check -> load_warehouse
extract_history ─────┘

- Reintentos automáticos ante fallos (default_args).
- Idempotente: la carga es un full refresh transaccional (ver datamart_etl/load.py).
- Parámetros operativos en Airflow Variables, leídas en tiempo de ejecución.
- Conexión al DW mediante la Airflow Connection `dw_postgres`.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from datamart_etl import extract, load, quality, transform

DW_CONN_ID = "dw_postgres"


def _extract_sales_daily():
    return extract.extract_source(
        Variable.get("data_dir"), Variable.get("staging_dir"),
        Variable.get("sales_file"), "sales_daily",
    )


def _extract_history():
    return extract.extract_source(
        Variable.get("data_dir"), Variable.get("staging_dir"),
        Variable.get("history_file"), "history",
    )


def _transform():
    return transform.run_transform(
        Variable.get("staging_dir"), Variable.get("source_timezone")
    )


def _quality_check():
    return quality.check_quality(
        Variable.get("staging_dir"), float(Variable.get("max_rejection_pct"))
    )


def _load_warehouse():
    return load.load_warehouse(Variable.get("staging_dir"), DW_CONN_ID)


default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="datamart_daily_pipeline",
    description="ETL diario DataMart: extracción, transformación, calidad y carga al DW",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["datamart", "etl"],
) as dag:

    extract_sales_daily = PythonOperator(
        task_id="extract_sales_daily", python_callable=_extract_sales_daily)

    extract_history = PythonOperator(
        task_id="extract_history", python_callable=_extract_history)

    transform_clean_and_split = PythonOperator(
        task_id="transform_clean_and_split", python_callable=_transform)

    quality_check = PythonOperator(
        task_id="quality_check", python_callable=_quality_check)

    load_warehouse = PythonOperator(
        task_id="load_warehouse", python_callable=_load_warehouse)

    [extract_sales_daily, extract_history] >> transform_clean_and_split
    transform_clean_and_split >> quality_check >> load_warehouse