***

### `docs/DECISIONES.md`

```markdown
# Documento de Decisiones Técnicas y Analíticas

Este documento justifica la arquitectura del Data Warehouse, el manejo de ambigüedades en la limpieza de datos y las conclusiones de negocio derivadas del caso DataMart S.A.S.

## 1. Diseño del Repositorio Analítico (Data Warehouse)
Se diseñó un modelo dimensional sobre PostgreSQL estructurado para aislar flujos financieros y optimizar la reportería:

* **Separación de Hechos (Ventas vs. Devoluciones):** Las reglas de negocio dictan que transacciones con cantidad $\le 0$ son devoluciones. Mantenerlas en una misma tabla distorsionaría sumas de revenue bruto y cálculos de ticket promedio. Se crearon `dw.fact_sales` y `dw.fact_returns`.
* **Capa de Agregación (`dw.agg_daily_product_net`):** Para agilizar consultas de KPIs financieros recurrentes (evolución mensual, top productos netos), se consolidó una vista materializada/tabla diaria. Esto previene cruces masivos (JOINs) entre la tabla de ventas y devoluciones por cada consulta.
* **Trazabilidad de Calidad (`dw.rejected_records`):** Registros inválidos (precios nulos/negativos, códigos no catalogados) se insertan aquí en formato JSONB. Esto asegura la auditoría de calidad sin interrumpir el flujo del pipeline.

## 2. Resolución de Casos Ambiguos y Calidad de Datos

* **Transacciones sin Customer ID (24.9% de los datos):**
  Se decidió **incluirlas** bajo un segmento `sin_cliente`. La exploración (`Exploracion.TXT`) demostró que no son ruido: representan 135,080 líneas, agrupan solo 1,371 facturas, pero aportan un ticket promedio de $1,101 (vs $475 del cliente identificado) con pocas unidades por línea (3.19 vs 13.14). Excluirlas habría significado una pérdida ciega de ~$1.5M en el reporte de revenue bruto.
* **Inconsistencia de Descripciones:**
  Se detectaron 638 descripciones normalizadas para 3,958 códigos base. Se resolvió creando un "nombre canónico" en `dim_product` (el más frecuente o primero en aparecer), unificando el catálogo. Al sumar 32 códigos rechazados por no pertenecer al catálogo estándar, se confirma un total real de 3,837 códigos únicos en la historia.
* **Duplicidad y Solapamiento de Fuentes:**
  El dataset histórico de 541,909 filas no contenía duplicados exactos en sí mismo, pero al cruzarlo con el archivo diario se detectaron más de 531,000 colisiones exactas. Esto indica que la fuente histórica es casi una copia redundante. Se resolvió aplicando una clave compuesta en memoria (`InvoiceNo + ProductCode + Quantity + InvoiceDate`) para deduplicar antes de cargar, protegiendo la base de datos de sobrecargas.
* **Discrepancia Geográfica (Contexto de Negocio):**
  Aunque el enunciado menciona operaciones en Colombia, México y Perú, los datos operacionales pertenecen casi exclusivamente al Reino Unido (90.5% de las facturas). Curiosamente, la analítica demostró que países periféricos (Holanda con $3,052, Australia con $2,466) quintuplican el ticket promedio británico, sugiriendo una oportunidad de negocio B2B internacional.

## 3. Garantía de Idempotencia en Airflow
Para asegurar que ejecutar el DAG varias veces el mismo día produzca resultados idénticos sin duplicar data, se diseñó la tarea de carga (`load.py`) con un patrón **"Destructive Load" transaccional**:
1. Dentro de un manejador de contexto `with hook.get_conn()`, se ejecuta un `TRUNCATE TABLE ... CASCADE` previo a las tablas dimensionales y de hechos.
2. Se realiza la inserción masiva de los CSVs transformados vía el comando `COPY` (optimizado para PostgreSQL).
3. Si cualquier archivo falla, la transacción hace *rollback*, impidiendo estados intermedios inconsistentes.

## 4. Conclusiones y Recomendación para DataMart
Las consultas de validación (Sección 7) definieron la tasa de devolución como "unidades devueltas / unidades vendidas" (con un mínimo de 100 ventas para evitar falsos positivos por volumen bajo). Bajo esta métrica, se halló un sesgo crítico en la operación:

**El impacto de las Cancelaciones Masivas:**
Las altas tasas de devolución no provienen de defectos de fábrica, sino de anomalías transaccionales. Los dos productos con más "devoluciones" son:
1. `23843 (PAPER CRAFT, LITTLE BIRDIE)`: 80,995 ventas y 80,995 devoluciones.
2. `23166 (MEDIUM CERAMIC TOP STORAGE JAR)`: 78,033 ventas y 74,494 devoluciones.

Estas ocurrencias sucedieron casi en un 100% dentro de una sola factura/línea anulada. En conjunto, estos dos productos representan el **51.7% de todo el dinero devuelto en DataMart**. Si se excluyen, la tasa de devolución general de la compañía cae a la mitad (de 4.64% a 2.30%).

**Recomendación Accionable:**
> Se recomienda al equipo de Producto y Ventas implementar una regla de confirmación estricta o cobro de depósito no reembolsable para transacciones mayoristas atípicas (superiores a 10,000 unidades). El análisis de calidad comprobó que las "pérdidas" logísticas están concentradas en la anulación de 2 o 3 carritos de compra masivos, no en insatisfacción de los clientes retail.
>
> A nivel analítico, es urgente enriquecer los metadatos del catálogo legado: el 40% de los productos (equivalente al 34.5% de los ingresos brutos) recaen en la categoría "Otros", lo que inutiliza el análisis de rentabilidad segmentada.