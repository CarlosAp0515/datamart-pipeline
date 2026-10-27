import json
import os
import logging

def check_quality(staging_dir: str, max_reject_pct: float = 10.0):
    """
    Valida las métricas de calidad post-transformación.
    Falla intencionalmente si la tasa de rechazo supera el umbral.
    """
    summary_file = os.path.join(staging_dir, "transform_summary.json")
    
    if not os.path.exists(summary_file):
        raise FileNotFoundError("No se encontró el resumen de transformación.")

    with open(summary_file, "r", encoding="utf-8") as f:
        summary = json.load(f)
        
    rejected_pct = summary.get("quality_rejected_pct", 100.0)
    
    if rejected_pct > max_reject_pct:
        raise ValueError(f"Fallo de Data Quality: {rejected_pct}% de rechazos supera el límite permitido de {max_reject_pct}%.")
        
    logging.info(f"Data Quality superado. Tasa de rechazos: {rejected_pct}%")
    logging.info(f"Ventas procesadas: {summary.get('sales', 0)} | Devoluciones: {summary.get('returns', 0)}")
    
    return summary