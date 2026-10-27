"""
Transformación: aplica las reglas de negocio y deja los CSV limpios en staging.
Cada fila recibe como máximo UN motivo de rechazo (el primero que aplique).
"""
import json
import os
import pandas as pd

from datamart_etl import config as cfg


# ---------- Funciones de normalización ----------
def _normalize_code(series):
    """Mayúsculas y sin espacios (regla de negocio)."""
    return series.fillna("").str.upper().str.replace(r"\s+", "", regex=True)


def _normalize_customer(series):
    """'17850.0' -> '17850'. Vacío -> nulo."""
    s = series.str.strip().str.replace(r"\.0$", "", regex=True)
    return s.where(s.notna() & (s != ""), None)


def _parse_to_utc(series, tz_name):
    """Texto -> fecha local -> UTC. Horas inexistentes o ambiguas del cambio de
    horario se vuelven NaT y se rechazan como fecha inválida."""
    local = pd.to_datetime(series, format=cfg.DATE_FORMAT, errors="coerce")
    local = local.dt.tz_localize(tz_name, ambiguous="NaT", nonexistent="NaT")
    return local.dt.tz_convert("UTC")


def assign_category(product_name):
    for category, pattern in cfg.CATEGORY_PATTERNS:
        if pattern.search(product_name):
            return category
    return cfg.DEFAULT_CATEGORY


# ---------- Limpieza principal ----------
def clean_transactions(raw, tz_name):
    df = raw.reset_index(drop=True).copy()

    # Columnas derivadas (las originales quedan intactas para el log de rechazos)
    df["invoice_clean"] = df["invoice_no"].str.strip().str.upper()
    df["code_clean"] = _normalize_code(df["product_code"])
    df["customer_clean"] = _normalize_customer(df["customer_id"])
    df["customer_key"] = df["customer_clean"].fillna(cfg.NO_CUSTOMER_LABEL)
    df["ts_utc"] = _parse_to_utc(df["invoice_ts"], tz_name)
    df["qty"] = pd.to_numeric(df["quantity"], errors="coerce")
    df["price"] = pd.to_numeric(df["unit_price"], errors="coerce")
    df["priority"] = df["source_system"].map(cfg.SOURCE_PRIORITY)

    # ----- Reglas de rechazo, en orden de prioridad -----
    df["reject_reason"] = None

    def mark(mask, reason):
        free = df["reject_reason"].isna()
        df.loc[free & mask, "reject_reason"] = reason

    mark(df.duplicated(subset=cfg.STD_COLUMNS + ["source_system"], keep="first"),
         cfg.R_DUP_EXACT)
    mark(df["ts_utc"].isna(), cfg.R_BAD_DATE)
    mark(df["qty"].isna() | df["price"].isna(), cfg.R_NOT_NUMERIC)
    mark(~df["code_clean"].str.match(cfg.PRODUCT_CODE_REGEX), cfg.R_BAD_CODE)
    mark(df["price"] <= 0, cfg.R_BAD_PRICE)

    # Duplicados entre fuentes: gana la de menor número de prioridad
    alive = df[df["reject_reason"].isna()].sort_values("priority", kind="stable")
    is_dup = alive.duplicated(subset=cfg.DEDUP_KEY, keep="first")
    df.loc[is_dup[is_dup].index, "reject_reason"] = cfg.R_DUP_KEY

    # ----- Registros válidos -----
    valid = df[df["reject_reason"].isna()]
    out = pd.DataFrame({
        "invoice_no": valid["invoice_clean"],
        "product_code": valid["code_clean"],
        "customer_id": valid["customer_clean"],
        "country": valid["country"].str.strip(),
        "invoice_ts_utc": valid["ts_utc"],
        "invoice_date": valid["ts_utc"].dt.date,   # periodo diario = día UTC
        "quantity": valid["qty"].astype(int),
        "unit_price": valid["price"],
        "source_system": valid["source_system"],
        "description": valid["description"],
    })

    # Separación ventas / devoluciones (regla: cantidad <= 0 es devolución)
    sales = out[out["quantity"] > 0].drop(columns="description").copy()
    sales["gross_revenue"] = (sales["quantity"] * sales["unit_price"]).round(4)

    returns = out[out["quantity"] <= 0].drop(columns="description").copy()
    returns["return_amount"] = (returns["quantity"].abs() * returns["unit_price"]).round(4)

    # ----- dim_product: nombre canónico = descripción normalizada más frecuente -----
    names = out[["product_code", "description"]].dropna().copy()
    names["name_raw"] = names["description"].str.strip()
    names["name_norm"] = names["name_raw"].str.upper().str.replace(r"\s+", " ", regex=True)

    variants = names.groupby("product_code").agg(
        raw_description_variants=("name_raw", "nunique"),
        normalized_description_variants=("name_norm", "nunique"),
    )
    canonical = (
        names.groupby(["product_code", "name_norm"]).size().reset_index(name="n")
        .sort_values(["product_code", "n", "name_norm"], ascending=[True, False, True])
        .drop_duplicates("product_code")[["product_code", "name_norm"]]
        .rename(columns={"name_norm": "product_name"})
    )
    products = pd.DataFrame({"product_code": sorted(out["product_code"].unique())})
    products = (products.merge(canonical, on="product_code", how="left")
                        .merge(variants, left_on="product_code", right_index=True, how="left"))
    products["product_name"] = products["product_name"].fillna("SIN DESCRIPCION")
    for col in ("raw_description_variants", "normalized_description_variants"):
        products[col] = products[col].fillna(0).astype(int)
    products["category"] = products["product_name"].map(assign_category)

    # ----- dim_customer: país más frecuente de cada cliente -----
    with_customer = out.dropna(subset=["customer_id"])
    customers = (with_customer.groupby("customer_id")["country"]
                 .agg(lambda s: s.mode().iat[0]).reset_index())

    # ----- Log de rechazos -----
    rej = df[df["reject_reason"].notna()]
    rejected = pd.DataFrame({
        "source_system": rej["source_system"].values,
        "reason": rej["reject_reason"].values,
        "raw_data": rej[cfg.STD_COLUMNS + ["source_system"]]
                    .to_json(orient="records", lines=True, force_ascii=False)
                    .splitlines(),
    })

    by_reason = rej["reject_reason"].value_counts().to_dict()
    quality_rejected = sum(by_reason.get(r, 0) for r in cfg.QUALITY_REASONS)
    summary = {
        "rows_input": int(len(df)),
        "rows_valid": int(len(valid)),
        "sales": int(len(sales)),
        "returns": int(len(returns)),
        "rejected": int(len(rej)),
        "rejected_by_reason": {k: int(v) for k, v in by_reason.items()},
        "quality_rejected_pct": round(100 * quality_rejected / len(df), 2),
        "products": int(len(products)),
        "customers": int(len(customers)),
        "sales_without_customer": int(sales["customer_id"].isna().sum()),
    }
    return {"sales": sales, "returns": returns, "dim_product": products,
            "dim_customer": customers, "rejected": rejected, "summary": summary}


# ---------- Orquestación de la tarea ----------
def run_transform(staging_dir, tz_name):
    frames = [
        pd.read_csv(os.path.join(staging_dir, f"extracted_{source}.csv"),
                    dtype=str, keep_default_na=False, na_values=[""])
        for source in cfg.SOURCE_PRIORITY
    ]
    result = clean_transactions(pd.concat(frames, ignore_index=True), tz_name)

    for name in ("sales", "returns", "dim_product", "dim_customer", "rejected"):
        result[name].to_csv(os.path.join(staging_dir, f"clean_{name}.csv"), index=False)
    with open(os.path.join(staging_dir, "transform_summary.json"), "w", encoding="utf-8") as f:
        json.dump(result["summary"], f, indent=2, ensure_ascii=False)
    return result["summary"]