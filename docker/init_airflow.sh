#!/usr/bin/env bash
set -e

echo ">>> Migrando base de metadatos de Airflow..."
airflow db migrate

echo ">>> Creando usuario administrador..."
airflow users create \
  --username "${AIRFLOW_ADMIN_USER}" \
  --password "${AIRFLOW_ADMIN_PASSWORD}" \
  --firstname Admin --lastname DataMart \
  --role Admin \
  --email "${AIRFLOW_ADMIN_EMAIL}" || echo "El usuario ya existe, continuo."

echo ">>> Configurando Connection del DW..."
airflow connections delete dw_postgres >/dev/null 2>&1 || true
airflow connections add dw_postgres \
  --conn-type postgres \
  --conn-host "${DW_HOST}" \
  --conn-port "${DW_PORT}" \
  --conn-login "${DW_USER}" \
  --conn-password "${DW_PASSWORD}" \
  --conn-schema "${DW_DB}"

echo ">>> Inicializando Variables..."
airflow variables set data_dir "/opt/airflow/data/raw"
airflow variables set staging_dir "/opt/airflow/data/staging"
airflow variables set source_timezone "Europe/London"
airflow variables set sales_file "data.csv"
airflow variables set history_file "online_retail_history.csv"
airflow variables set history_format "online_retail_ii"
airflow variables set max_rejection_pct "40"

echo ">>> Inicializacion completa."