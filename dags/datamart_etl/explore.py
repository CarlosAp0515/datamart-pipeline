"""Exploración rápida de las fuentes crudas. Solo lectura, no modifica nada."""
import re
import pandas as pd

RAW_DIR = "/opt/airflow/data/raw"
FILES = ["data.csv", "online_retail_history.csv"]
CODE_PATTERN = re.compile(r"^\d{5}[A-Z]{0,2}$")

# Nombres posibles de cada columna según la fuente
CANDIDATES = {
    "invoice": ["InvoiceNo", "Invoice", "TransactionNo"],
    "code": ["StockCode", "ProductNo"],
    "description": ["Description", "ProductName"],
    "quantity": ["Quantity"],
    "date": ["InvoiceDate", "Date"],
    "price": ["UnitPrice", "Price"],
    "customer": ["CustomerID", "Customer ID", "CustomerNo"],
    "country": ["Country"],
}


def read_csv_safe(path):
    """Prueba UTF-8 y, si falla, latin-1 (data.csv viene en latin-1)."""
    for enc in ("utf-8", "latin-1"):
        try:
            return pd.read_csv(path, encoding=enc, dtype=str), enc
        except UnicodeDecodeError:
            continue
    raise ValueError(f"No se pudo leer {path}")


def pick(df, names):
    return next((c for c in names if c in df.columns), None)


def explore(filename):
    df, enc = read_csv_safe(f"{RAW_DIR}/{filename}")
    cols = {k: pick(df, v) for k, v in CANDIDATES.items()}
    print("=" * 70)
    print(f"ARCHIVO: {filename} | encoding: {enc} | filas: {len(df):,}")
    print("Columnas:", list(df.columns))
    print("Mapeo detectado:", cols)
    print("\nPrimeras 3 filas:")
    print(df.head(3).to_string())
    print("\nNulos por columna:")
    print(df.isna().sum().to_string())
    print(f"\nFilas duplicadas exactas: {df.duplicated().sum():,}")

    qty = pd.to_numeric(df[cols["quantity"]], errors="coerce")
    price = pd.to_numeric(df[cols["price"]], errors="coerce")
    print(f"Cantidad <= 0 (devoluciones/ajustes): {(qty <= 0).sum():,}")
    print(f"Precio <= 0: {(price <= 0).sum():,}")
    print(f"Cantidad no numérica: {qty.isna().sum():,} | Precio no numérico: {price.isna().sum():,}")

    cust = df[cols["customer"]]
    print(f"Sin customer ID: {cust.isna().sum():,} ({cust.isna().mean():.1%})")

    codes = df[cols["code"]].astype(str).str.strip().str.upper()
    bad = codes[~codes.str.match(CODE_PATTERN)]
    print(f"Códigos que NO siguen el patrón de catálogo: {len(bad):,}")
    print("  Más frecuentes:", bad.value_counts().head(10).to_dict())
    print(f"Códigos únicos (normalizados): {codes.nunique():,}")

    desc = df[cols["description"]].dropna().str.strip()
    norm = desc.str.upper().str.replace(r"\s+", " ", regex=True)
    tmp = pd.DataFrame({"code": codes[desc.index], "raw": desc, "norm": norm})
    v = tmp.groupby("code").agg(raw=("raw", "nunique"), norm=("norm", "nunique"))
    print(f"Códigos con >1 descripción cruda: {(v.raw > 1).sum():,}")
    print(f"Códigos con >1 descripción normalizada: {(v.norm > 1).sum():,}")

    print("\nMuestra de fechas (texto original):", df[cols["date"]].head(3).tolist())
    print("Países:", df[cols["country"]].value_counts().head(8).to_dict())


if __name__ == "__main__":
    for f in FILES:
        explore(f)