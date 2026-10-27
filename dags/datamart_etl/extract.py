"""Extracción: lee un CSV crudo, estandariza columnas y lo deja en staging."""
import os
import pandas as pd

from datamart_etl import config as cfg


def read_raw_csv(path):
    """Lee un CSV como texto. Prueba UTF-8 y si falla usa latin-1 (data.csv)."""
    if not os.path.exists(path):
        raise FileNotFoundError(f"No existe el archivo de origen: {path}")
    for encoding in ("utf-8", "latin-1"):
        try:
            return pd.read_csv(path, dtype=str, encoding=encoding,
                               keep_default_na=False, na_values=[""])
        except UnicodeDecodeError:
            continue
    raise ValueError(f"No se pudo leer {path}")


def extract_source(raw_dir, staging_dir, file_name, source_system):
    """Devuelve un resumen pequeño (apto para XCom) y escribe el CSV de staging."""
    df = read_raw_csv(os.path.join(raw_dir, file_name))

    missing = set(cfg.COLUMN_MAP) - set(df.columns)
    if missing:
        raise ValueError(f"{file_name}: faltan columnas {sorted(missing)}")

    # Se conservan solo las columnas conocidas (ej: se descarta la columna 'index')
    df = df[list(cfg.COLUMN_MAP)].rename(columns=cfg.COLUMN_MAP)
    df["source_system"] = source_system

    os.makedirs(staging_dir, exist_ok=True)
    out_path = os.path.join(staging_dir, f"extracted_{source_system}.csv")
    df.to_csv(out_path, index=False)   # se sobrescribe: idempotente
    return {"source": source_system, "rows": int(len(df)), "path": out_path}