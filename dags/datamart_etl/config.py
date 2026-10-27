"""
Configuración central del pipeline.
Las decisiones de negocio viven aquí para poder cambiarlas sin tocar la lógica.
"""
import re

# ---------- Fuentes ----------
# Prioridad ante duplicados entre fuentes: menor número = gana.
SOURCE_PRIORITY = {"sales_daily": 1, "history": 2}

# ---------- Columnas ----------
# Ambos CSV usan los mismos nombres. Si una fuente cambia, se ajusta aquí.
COLUMN_MAP = {
    "InvoiceNo": "invoice_no",
    "StockCode": "product_code",
    "Description": "description",
    "Quantity": "quantity",
    "InvoiceDate": "invoice_ts",
    "UnitPrice": "unit_price",
    "CustomerID": "customer_id",
    "Country": "country",
}
STD_COLUMNS = list(COLUMN_MAP.values())
DATE_FORMAT = "%m/%d/%Y %H:%M"          # ej: 12/1/2010 8:26

# ---------- Reglas de calidad ----------
# Código de catálogo válido: 5 dígitos + 0 a 2 letras (85123A, 15056BL, 22633)
PRODUCT_CODE_REGEX = r"^\d{5}[A-Z]{0,2}$"

# Clave compuesta para detectar el mismo registro entre fuentes
DEDUP_KEY = ["invoice_clean", "code_clean", "ts_utc", "qty", "price", "customer_key"]
NO_CUSTOMER_LABEL = "SIN_CLIENTE"       # solo se usa para comparar duplicados

# Motivos de rechazo (se guardan en dw.rejected_records)
R_DUP_EXACT = "duplicado_exacto_en_fuente"
R_BAD_DATE = "fecha_invalida"
R_NOT_NUMERIC = "cantidad_o_precio_no_numerico"
R_BAD_CODE = "codigo_no_catalogo"
R_BAD_PRICE = "precio_cero_o_negativo"
R_DUP_KEY = "duplicado_clave_compuesta"

# Rechazos por CALIDAD (los duplicados no cuentan para el umbral de calidad)
QUALITY_REASONS = [R_BAD_DATE, R_NOT_NUMERIC, R_BAD_CODE, R_BAD_PRICE]

# ---------- Categorías (heurística por palabras clave) ----------
# Se evalúa en orden: la primera categoría que coincida gana.
DEFAULT_CATEGORY = "Otros"
CATEGORY_KEYWORDS = [
    ("Deportes", ["SKIPPING", "FRISBEE", "YOGA", "FOOTBALL", "CRICKET", "BOXING",
                  "SKATE", "KITE", "BICYCLE", "DOMINOES", "GAME", "GAMES", "PLAYING"]),
    ("Ropa", ["SCARF", "APRON", "HAT", "SOCK", "SOCKS", "GLOVES", "SLIPPERS",
              "UMBRELLA", "PURSE", "WALLET", "BRACELET", "NECKLACE", "EARRINGS",
              "JEWELLERY", "BAG", "BAGS", "SHIRT", "CARDIGAN", "BROOCH", "HAIR"]),
    ("Papelería", ["CARD", "CARDS", "PAPER", "NOTEBOOK", "NOTEBOOKS", "PEN", "PENS",
                   "PENCIL", "PENCILS", "STICKER", "STICKERS", "ENVELOPE", "WRAP",
                   "RIBBON", "STATIONERY", "DIARY", "BOOK", "JOURNAL", "ERASER",
                   "RULER", "CRAYON", "POSTCARD", "TAG", "TAGS", "STAMP", "MARKER"]),
    ("Electrónica", ["RADIO", "CLOCK", "ALARM", "CALCULATOR", "TORCH", "BATTERY",
                     "USB", "LAMP", "LED", "FAIRY LIGHTS", "CHARGER", "SPEAKER"]),
    ("Hogar", ["CANDLE", "HOLDER", "MUG", "CUP", "CUSHION", "LANTERN", "FRAME", "BOX",
               "JAR", "DRAWER", "BOTTLE", "PLATE", "BOWL", "TEAPOT", "VASE", "CURTAIN",
               "TOWEL", "BASKET", "TRAY", "DOORMAT", "HOOK", "SIGN", "CAKE", "KITCHEN",
               "GARDEN", "HEART", "TEA", "TIN", "T-LIGHT"]),
]
CATEGORY_PATTERNS = [
    (cat, re.compile(r"\b(" + "|".join(re.escape(w) for w in words) + r")\b"))
    for cat, words in CATEGORY_KEYWORDS
]