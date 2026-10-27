"""
Carga al repositorio analítico (PostgreSQL).

Idempotencia: la carga es un "full refresh" dentro de UNA sola transacción:
TRUNCATE -> COPY de cada tabla -> reconstrucción del agregado -> verificación.
Si algo falla se hace ROLLBACK y el DW queda exactamente como estaba.
Ejecutarlo N veces con los mismos datos deja siempre el mismo resultado.
"""
import csv
import json
import logging
import os

from airflow.providers.postgres.hooks.postgres import PostgresHook

SCHEMA = "dw"

# Orden de carga: primero dimensiones (las FKs de los hechos dependen de ellas)
LOAD_PLAN = [
    ("dim_product", "clean_dim_product.csv"),
    ("dim_customer", "clean_dim_customer.csv"),
    ("fact_sales", "clean_sales.csv"),
    ("fact_returns", "clean_returns.csv"),
    ("rejected_records", "clean_rejected.csv"),
]

# Una sola sentencia TRUNCATE sobre todas las tablas evita problemas con las FKs
TRUNCATE_SQL = f"""
TRUNCATE {SCHEMA}.agg_daily_product_net,
         {SCHEMA}.fact_sales,
         {SCHEMA}.fact_returns,
         {SCHEMA}.dim_customer,
         {SCHEMA}.dim_product,
         {SCHEMA}.rejected_records
RESTART IDENTITY;
"""

# Neto = revenue bruto - monto devuelto, por producto y por día
AGG_SQL = f"""
INSERT INTO {SCHEMA}.agg_daily_product_net
    (invoice_date, product_code, units_sold, gross_revenue,
     units_returned, returned_amount, net_revenue)
SELECT COALESCE(s.invoice_date, r.invoice_date),
       COALESCE(s.product_code, r.product_code),
       COALESCE(s.units_sold, 0),
       COALESCE(s.gross_revenue, 0),
       COALESCE(r.units_returned, 0),
       COALESCE(r.returned_amount, 0),
       COALESCE(s.gross_revenue, 0) - COALESCE(r.returned_amount, 0)
FROM (SELECT invoice_date, product_code,
             SUM(quantity) AS units_sold,
             SUM(gross_revenue) AS gross_revenue
      FROM {SCHEMA}.fact_sales GROUP BY invoice_date, product_code) s
FULL OUTER JOIN
     (SELECT invoice_date, product_code,
             SUM(ABS(quantity)) AS units_returned,
             SUM(return_amount) AS returned_amount
      FROM {SCHEMA}.fact_returns GROUP BY invoice_date, product_code) r
  ON s.invoice_date = r.invoice_date AND s.product_code = r.product_code;
"""

# Tabla -> clave del transform_summary.json con el conteo esperado
EXPECTED_COUNTS = {
    "fact_sales": "sales",
    "fact_returns": "returns",
    "dim_product": "products",
    "dim_customer": "customers",
    "rejected_records": "rejected",
}


def _csv_header(path):
    """Lee los nombres de columna del CSV para no depender del orden."""
    with open(path, "r", encoding="utf-8", newline="") as f:
        return next(csv.reader(f))


def _copy_csv(cur, table, path):
    columns = ", ".join(_csv_header(path))
    sql = f"COPY {SCHEMA}.{table} ({columns}) FROM STDIN WITH (FORMAT csv, HEADER true)"
    with open(path, "r", encoding="utf-8", newline="") as f:
        cur.copy_expert(sql, f)
    cur.execute(f"SELECT COUNT(*) FROM {SCHEMA}.{table}")
    n = cur.fetchone()[0]
    logging.info("Cargada %s: %s filas", table, n)
    return n


def load_warehouse(staging_dir, conn_id="dw_postgres"):
    with open(os.path.join(staging_dir, "transform_summary.json"), encoding="utf-8") as f:
        summary = json.load(f)

    conn = PostgresHook(postgres_conn_id=conn_id).get_conn()
    counts = {}
    try:
        cur = conn.cursor()

        logging.info("Vaciando tablas del DW (full refresh transaccional)...")
        cur.execute(TRUNCATE_SQL)

        for table, filename in LOAD_PLAN:
            counts[table] = _copy_csv(cur, table, os.path.join(staging_dir, filename))

        logging.info("Construyendo agg_daily_product_net...")
        cur.execute(AGG_SQL)
        cur.execute(f"SELECT COUNT(*) FROM {SCHEMA}.agg_daily_product_net")
        counts["agg_daily_product_net"] = cur.fetchone()[0]

        # Verificación: lo cargado debe coincidir con lo que dijo la transformación
        for table, key in EXPECTED_COUNTS.items():
            if counts[table] != summary[key]:
                raise ValueError(
                    f"Conteo inconsistente en {table}: cargadas {counts[table]}, "
                    f"esperadas {summary[key]}"
                )

        conn.commit()
        logging.info("Carga confirmada (COMMIT): %s", counts)
        return counts
    except Exception:
        conn.rollback()
        logging.exception("Fallo la carga: ROLLBACK, el DW queda como estaba.")
        raise
    finally:
        conn.close()