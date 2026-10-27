# Documento de decisiones técnicas – DataMart S.A.S.

Este documento explica cómo se diseñó el repositorio analítico, cómo se resolvió cada caso ambiguo de la sección 5 del enunciado, cómo se garantiza la idempotencia del DAG y qué conclusiones arrojan los datos. Cada cifra indica la consulta de `sql/validation/queries.sql` (P1–P7) que la respalda.

> **Moneda:** el dataset proviene de una tienda del Reino Unido, por lo que todos los montos están en **libras esterlinas (GBP, £)**. No se aplicó conversión de moneda: el enunciado menciona monedas distintas por país, pero las fuentes entregadas solo contienen una.

---

## 1. Alcance de la solución

**Implementado**
- Entorno Docker completo: Airflow (webserver + scheduler), PostgreSQL de metadatos y PostgreSQL analítico separado, con Connection `dw_postgres`, Variables y usuario administrador creados automáticamente (`docker/init_airflow.sh`).
- DAG diario con extracción de cada fuente, transformación, validación de calidad y carga, con reintentos e idempotencia.
- Modelo dimensional con separación ventas/devoluciones, log de rechazos y agregado diario del neto.
- Consultas SQL para las siete preguntas de negocio.

**No implementado (pendiente)**
- La API interna de productos (FastAPI), que el enunciado define como opcional. Cómo se habría hecho: un servicio adicional en el `docker-compose` con dos endpoints (`/products?page=&size=` y `/products/{code}`), y una tarea extra del DAG que consuma la API con `requests` para enriquecer la categoría. En su lugar se usa la estrategia descrita en la sección 3.8, que mantiene el flujo funcionando sin API.
- Carga incremental (ver sección 5, limitaciones).

---

## 2. Diseño del repositorio analítico

Modelo dimensional simple (esquema `dw` en PostgreSQL). El diagrama está en `docs/diagrama_modelo.png` y `docs/modelo_datos.dbml`.

| Tabla | Rol | Clave | Relaciones |
|---|---|---|---|
| `dim_product` | Catálogo: nombre canónico, categoría y contadores de variantes de descripción | `product_code` | Referenciada por hechos y agregado |
| `dim_customer` | Clientes identificados y su país principal | `customer_id` | Referenciada por hechos (opcional) |
| `fact_sales` | Líneas de venta válidas (`cantidad > 0`, `precio > 0`) con `gross_revenue` | `sale_id` | FK a producto y cliente |
| `fact_returns` | Devoluciones/ajustes (`cantidad ≤ 0`) con `return_amount` | `return_id` | FK a producto y cliente |
| `agg_daily_product_net` | Bruto, devuelto y neto por producto y día | `(invoice_date, product_code)` | FK a producto |
| `rejected_records` | Registros rechazados con motivo, fuente y registro original en JSONB | `reject_id` | — |

**Por qué este diseño**
- **Ventas y devoluciones en tablas separadas:** la regla de negocio exige tratarlas distinto. Mezclarlas distorsionaría el revenue bruto y el ticket promedio, y separarlas permite calcular el neto sin ambigüedad.
- **Tabla agregada diaria:** el neto se define "por producto y por periodo diario". Precalcularlo evita cruzar las dos tablas de hechos en cada consulta de KPIs (evolución mensual, top productos, categorías).
- **Log de rechazos con JSONB:** conserva el registro original completo para auditoría sin forzar un esquema, y evita perder datos sin dejar rastro.
- **Cliente opcional en los hechos:** permite conservar las transacciones sin Customer ID (sección 3.1).
- **Simplicidad deliberada:** no se creó una dimensión de fecha ni de país porque ninguna pregunta de negocio las requiere; se pueden agregar sin cambiar los hechos.

---

## 3. Reglas de negocio y casos ambiguos

### Reglas de rechazo (orden de aplicación)

Cada fila recibe como máximo **un** motivo: el primero que cumpla. Las reglas viven en `dags/datamart_etl/config.py` y `transform.py`, por lo que son fáciles de cambiar.

| # | Motivo en `rejected_records` | Condición |
|---|---|---|
| 1 | `duplicado_exacto_en_fuente` | Fila idéntica a otra de la misma fuente |
| 2 | `fecha_invalida` | Fecha no interpretable o inexistente/ambigua por cambio de horario |
| 3 | `cantidad_o_precio_no_numerico` | Cantidad o precio no convertibles a número |
| 4 | `codigo_no_catalogo` | Código que no cumple `^\d{5}[A-Z]{0,2}$` |
| 5 | `precio_cero_o_negativo` | Precio unitario ≤ 0 |
| 6 | `duplicado_clave_compuesta` | Mismo registro en otra fuente (ver 3.3) |

Después de las reglas, las filas válidas con `cantidad ≤ 0` van a `fact_returns` y el resto a `fact_sales`.

### 3.1 Transacciones sin Customer ID

**Decisión:** se **incluyen**, con `customer_id = NULL`.
**Por qué:** representan cerca del 24.9 % de las filas crudas. Excluirlas eliminaría una parte material del revenue y sesgaría el análisis. Se pueden comparar contra los clientes identificados (pregunta 5).
**Impacto (P5):** el segmento `sin_cliente` agrupa pocas facturas (1,371) pero con un ticket promedio mucho mayor (£1,101 frente a £475 de los identificados) y menos unidades por línea (3.19 frente a 13.14). Descartarlas habría supuesto perder cerca de £1.5 M de revenue bruto.
**Alternativa descartada:** asignar un cliente genérico ("DESCONOCIDO") obligaría a una fila falsa en `dim_customer` que contaría como un cliente más.

### 3.2 Descripciones con variaciones de escritura

**Decisión:** el nombre canónico de cada código es la **descripción normalizada más frecuente** (mayúsculas, sin espacios repetidos). En caso de empate, la primera en orden alfabético.
**Por qué:** es una regla objetiva, reproducible y no depende del orden de las filas.
**Evidencia (exploración, P6a):** 3,958 códigos únicos tras normalizar; 642 con más de una descripción cruda y **638 con más de una descripción tras normalizar**. Solo 4 se explican por mayúsculas/minúsculas; el resto son textos realmente distintos (notas como "damaged" o "?"), y esos son los que contesta la pregunta 6. `dim_product` guarda ambos contadores (`raw_description_variants`, `normalized_description_variants`).

### 3.3 Solapamiento entre las dos fuentes

**Hallazgo:** el "historial" (`online_retail_history.csv`) resultó ser una copia de la fuente diaria: mismas 541,909 filas y mismos valores; solo añade una columna `index` y escribe el cliente como `17850.0`.
**Decisión (dos pasos):**
1. Dentro de cada fuente se eliminan los duplicados exactos (5,268 por fuente, una vez descartada la columna `index`).
2. Entre fuentes se deduplica con la **clave compuesta** `factura + código normalizado + fecha UTC + cantidad + precio + cliente`. Si coincide, gana la fuente diaria (`SOURCE_PRIORITY` en `config.py`).

**Por qué clave compuesta y no solo la factura:** una factura tiene muchas líneas, así que el número de factura no identifica un registro.
**Resultado:** el historial aporta 0 filas nuevas y todas sus filas quedan en `rejected_records` con su motivo. Es el comportamiento esperado, y demuestra que la regla funciona.
**Normalización previa a la comparación:** el identificador de cliente se limpia (`17850.0` → `17850`) y se descarta la columna `index`; sin ello ninguna fila coincidiría.

### 3.4 Códigos de producto fuera de catálogo

**Decisión:** se rechazan con el motivo `codigo_no_catalogo`. Un código de catálogo tiene 5 dígitos y hasta 2 letras (`85123A`, `15056BL`).
**Por qué:** los 2,995 registros crudos que no lo cumplen (por ejemplo `POST`, `DOT`, `M`, `D`, `BANK CHARGES`, `AMAZONFEE`) son cargos de envío, descuentos, comisiones y ajustes manuales, no productos vendibles. Incluirlos inflaría el catálogo y contaminaría los rankings de productos.
**Efecto:** se conserva el registro original en `rejected_records`. Los códigos totales y los de catálogo se reconcilian con las consultas P6a y P6d.

### 3.5 Precio unitario cero o negativo

**Decisión:** se rechaza (`precio_cero_o_negativo`) tanto para ventas como para devoluciones.
**Por qué:** es una regla explícita del negocio. En el caso de las devoluciones con precio 0 (o ajustes con precio negativo, como "Adjust bad debt") no tienen valor monetario calculable, así que no aportan al neto.

### 3.6 Fechas y zona horaria

**Decisión:** los dos CSV traen fecha sin zona horaria (`12/1/2010 8:26`, formato mes/día/año). Se interpretan en la zona definida por la Variable `source_timezone` (`Europe/London`, la del origen del dataset) y se convierten a **UTC** antes de cargar.
**Detalle:** las horas inexistentes o ambiguas por el cambio de horario no se pueden convertir sin riesgo, así que se rechazan como `fecha_invalida`. El "periodo diario" del neto es el día UTC.

### 3.7 Devoluciones y revenue bruto/neto

- `revenue bruto = cantidad × precio unitario` (solo ventas).
- `monto devuelto = |cantidad| × precio unitario` (devoluciones).
- `revenue neto = revenue bruto − monto devuelto`, por **producto y día UTC**, en `agg_daily_product_net`.
- Un día puede tener neto negativo si un producto se devuelve más de lo que se vendió ese día. Se conserva así porque refleja la realidad del periodo.
- Esta definición "por producto y día" hace que una venta anulada el mismo día quede neutralizada en el neto, que es lo que debe ocurrir.

### 3.8 Categorías de producto (sin API)

**Decisión:** como las fuentes no traen categoría y la API es opcional, la categoría se asigna por **palabras clave de la descripción canónica** (diccionario ordenado en `config.py`, gana la primera coincidencia): Deportes, Ropa, Papelería, Electrónica, Hogar, y `Otros` si no hay coincidencia.
**Por qué:** es simple, explicable y editable sin tocar la lógica.
**Limitación reconocida (P7c):** es una heurística. Alrededor del 40 % del catálogo (34.5 % del revenue bruto) cae en `Otros`, y algunas clasificaciones son aproximadas. Las preguntas por categoría deben leerse con esa reserva. La mejora natural es la API de catálogo del sistema legado.

### 3.9 Contexto geográfico

Aunque el enunciado habla de Colombia, México y Perú, los datos son de una tienda del Reino Unido (≈ 90.5 % de las facturas, P4). Algunos países con pocas facturas (Países Bajos £3,052, Australia £2,466) tienen un ticket promedio varias veces superior al británico, pero con un volumen pequeño: es una pista, no una conclusión.

---

## 4. Idempotencia del DAG

La carga (`load.py`) es un *full refresh* dentro de **una sola transacción**:
1. Un `TRUNCATE` multitabla con `RESTART IDENTITY` vacía todas las tablas del esquema `dw`.
2. `COPY` de cada CSV de staging, con las dimensiones primero (las FK de los hechos dependen de ellas).
3. Reconstrucción de `agg_daily_product_net` desde los hechos.
4. **Verificación:** cada conteo cargado debe coincidir con `transform_summary.json`; si no, se lanza un error.
5. `COMMIT`. Ante cualquier fallo se hace `ROLLBACK`, así que el DW queda como estaba y nunca en un estado intermedio.

Además, extracción y transformación **sobrescriben** sus archivos de staging, por lo que no acumulan estado entre ejecuciones. Resultado: ejecutar el DAG dos veces con los mismos datos deja exactamente los mismos conteos (verificado con dos corridas consecutivas).

**Limitación:** el full refresh es adecuado para este volumen. Con datos crecientes se pasaría a una carga incremental por partición de fecha (`logical_date`) con `MERGE`/`INSERT ... ON CONFLICT`.

---

## 5. Hallazgos de la exploración (fuente cruda, por archivo)

| Aspecto | Hallazgo |
|---|---|
| Filas | 541,909 |
| Descripciones nulas | 1,454 |
| Duplicados exactos | 5,268 |
| Cantidad ≤ 0 | 10,624 |
| Precio ≤ 0 | 2,517 |
| Sin customer ID | 135,080 (24.9 %) |
| Códigos fuera de catálogo | 2,995 |
| Codificación | `data.csv` en latin-1; el historial en UTF-8 |

**Resultado de la corrida completa** (ambas fuentes: 1,083,818 filas de entrada):

| Tabla | Filas |
|---|---|
| `fact_sales` | 522,502 |
| `fact_returns` | 8,668 |
| `rejected_records` | 552,648 |
| `agg_daily_product_net` | 276,995 |

Verificación de cuadre: 522,502 + 8,668 + 552,648 = 1,083,818, es decir, no se perdió ninguna fila. El desglose de rechazos por motivo está en `data/staging/transform_summary.json` y en la tabla `rejected_records` (`GROUP BY reason`).

---

## 6. Conclusiones y recomendación al equipo de producto

**Definición usada:** tasa de devolución = unidades devueltas / unidades vendidas, exigiendo al menos 100 unidades vendidas para evitar falsos positivos (P3b).

**Hallazgo principal (P7d, P7e):** las devoluciones están muy concentradas en pocos productos. Los dos principales son:

| Producto | Vendidas | Devueltas |
|---|---|---|
| `23843` (PAPER CRAFT, LITTLE BIRDIE) | 80,995 | 80,995 |
| `23166` (MEDIUM CERAMIC TOP STORAGE JAR) | 78,033 | 74,494 |

Juntos explican el **51.7 %** del dinero devuelto. Al excluirlos, la tasa global de devolución baja de **4.64 % a 2.30 %**. El patrón (una línea de venta masiva y una línea de devolución de la misma cantidad) es consistente con pedidos anulados y no con defectos del producto. El diseño por producto y día ya neutraliza ese efecto en el revenue neto, pero distorsiona la tasa de devolución.

**Recomendación concreta:**
1. **Marcar y separar en los reportes de devolución los pares venta→anulación de una misma línea** (misma cantidad, mismo producto, mismo día), y calcular la tasa de devolución "ajustada" sin ellos. Con los datos actuales, la tasa de referencia pasa de 4.64 % a 2.30 %, un número mucho más útil para decidir qué referencias revisar.
2. **Crear una alerta de validación para pedidos de volumen atípico** (por ejemplo, más de 10,000 unidades en una línea) para que se confirmen antes de procesarse.
3. **Enfocar la revisión de calidad de producto** en los artículos con alta tasa de devolución y revenue bruto relevante (P7b), una vez excluidos los pedidos anulados.

> Los datos muestran concentración y un patrón compatible con anulaciones; no permiten afirmar la causa. Confirmarla requeriría los motivos de devolución, que las fuentes no incluyen.

**Limitación de datos relacionada:** con alrededor del 40 % del catálogo en la categoría `Otros`, el análisis de rentabilidad por categoría es poco confiable. Enriquecer el catálogo legado (la API prevista) es el siguiente paso de datos.

---

## 7. Cómo cambiar una decisión

Todo está en `dags/datamart_etl/config.py`:

| Quiero cambiar... | Dónde |
|---|---|
| Prioridad entre fuentes | `SOURCE_PRIORITY` |
| Patrón de código válido | `PRODUCT_CODE_REGEX` |
| Clave de duplicados entre fuentes | `DEDUP_KEY` |
| Motivos que cuentan para el umbral de calidad | `QUALITY_REASONS` |
| Categorías y palabras clave | `CATEGORY_KEYWORDS` y `DEFAULT_CATEGORY` |
| Formato de fecha | `DATE_FORMAT` |

Parámetros operativos (zona horaria, umbral de rechazo, rutas, nombres de archivo) son **Airflow Variables**, definidas en `docker/init_airflow.sh`.