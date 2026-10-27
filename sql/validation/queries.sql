-- =====================================================================
-- Consultas de validación - Preguntas de negocio (sección 7)
-- Ejecutar contra el DW después de correr el pipeline:
--   docker compose exec -T dw-db psql -U dw_user -d datamart_dw < sql/validation/queries.sql
-- =====================================================================

-- ---------------------------------------------------------------------
-- P1. Evolución mensual de las ventas netas (ventas - devoluciones)
-- ---------------------------------------------------------------------
SELECT date_trunc('month', invoice_date)::date AS mes,
       ROUND(SUM(gross_revenue), 2)    AS revenue_bruto,
       ROUND(SUM(returned_amount), 2)  AS devoluciones,
       ROUND(SUM(net_revenue), 2)      AS revenue_neto
FROM dw.agg_daily_product_net
GROUP BY 1
ORDER BY 1;

-- ---------------------------------------------------------------------
-- P2. Categorías con más revenue bruto y proporción de devoluciones
--     (proporción = monto devuelto / revenue bruto)
-- ---------------------------------------------------------------------
SELECT p.category,
       ROUND(SUM(a.gross_revenue), 2)   AS revenue_bruto,
       ROUND(SUM(a.returned_amount), 2) AS monto_devuelto,
       ROUND(100.0 * SUM(a.returned_amount)
             / NULLIF(SUM(a.gross_revenue), 0), 2) AS pct_devolucion
FROM dw.agg_daily_product_net a
JOIN dw.dim_product p USING (product_code)
GROUP BY p.category
ORDER BY revenue_bruto DESC;

-- ---------------------------------------------------------------------
-- P3a. Top 10 productos por revenue neto
-- ---------------------------------------------------------------------
SELECT a.product_code,
       p.product_name,
       p.category,
       ROUND(SUM(a.net_revenue), 2) AS revenue_neto
FROM dw.agg_daily_product_net a
JOIN dw.dim_product p USING (product_code)
GROUP BY a.product_code, p.product_name, p.category
ORDER BY revenue_neto DESC
LIMIT 10;

-- ---------------------------------------------------------------------
-- P3b. Top 10 productos por tasa de devolución (unidades devueltas / vendidas)
--      Se exige un mínimo de 100 unidades vendidas para evitar ruido de
--      productos con muy pocas ventas.
-- ---------------------------------------------------------------------
SELECT a.product_code,
       p.product_name,
       SUM(a.units_sold)     AS unidades_vendidas,
       SUM(a.units_returned) AS unidades_devueltas,
       ROUND(100.0 * SUM(a.units_returned) / SUM(a.units_sold), 2) AS tasa_devolucion_pct
FROM dw.agg_daily_product_net a
JOIN dw.dim_product p USING (product_code)
GROUP BY a.product_code, p.product_name
HAVING SUM(a.units_sold) >= 100
ORDER BY tasa_devolucion_pct DESC
LIMIT 10;

-- ---------------------------------------------------------------------
-- P4. Países con más transacciones y ticket promedio
--     (transacción = factura; ticket = revenue bruto / facturas)
-- ---------------------------------------------------------------------
WITH por_pais AS (
    SELECT country,
           COUNT(DISTINCT invoice_no) AS facturas,
           SUM(gross_revenue)         AS revenue
    FROM dw.fact_sales
    GROUP BY country
)
SELECT country,
       facturas,
       ROUND(100.0 * facturas / SUM(facturas) OVER (), 2) AS pct_del_total,
       ROUND(revenue / facturas, 2)                       AS ticket_promedio
FROM por_pais
ORDER BY facturas DESC;

-- ---------------------------------------------------------------------
-- P5. Clientes identificados vs. transacciones sin customer ID
-- ---------------------------------------------------------------------
SELECT CASE WHEN customer_id IS NULL THEN 'sin_cliente' ELSE 'identificado' END AS segmento,
       COUNT(*)                                     AS lineas,
       COUNT(DISTINCT invoice_no)                   AS facturas,
       ROUND(SUM(gross_revenue), 2)                 AS revenue_bruto,
       ROUND(SUM(gross_revenue)
             / COUNT(DISTINCT invoice_no), 2)       AS ticket_promedio,
       ROUND(AVG(quantity), 2)                      AS unidades_por_linea,
       ROUND(AVG(unit_price), 2)                    AS precio_unitario_promedio
FROM dw.fact_sales
GROUP BY 1;

-- ---------------------------------------------------------------------
-- P6a. Códigos únicos de producto y productos con descripción inconsistente
--      - con_variantes_de_escritura: mismo texto en mayúsculas/minúsculas distintas
--      - con_descripcion_distinta: descripciones realmente diferentes tras normalizar
-- ---------------------------------------------------------------------
SELECT COUNT(*)                                                  AS codigos_unicos_en_catalogo,
       COUNT(*) FILTER (WHERE raw_description_variants > 1)      AS con_variantes_de_escritura,
       COUNT(*) FILTER (WHERE normalized_description_variants > 1) AS con_descripcion_distinta
FROM dw.dim_product;

-- P6b. Detalle: productos con más descripciones distintas
SELECT product_code,
       product_name AS nombre_canonico,
       raw_description_variants,
       normalized_description_variants
FROM dw.dim_product
WHERE normalized_description_variants > 1
ORDER BY normalized_description_variants DESC, product_code
LIMIT 20;

-- P6c. Códigos fuera del catálogo que fueron rechazados (para el total real de códigos)
SELECT COUNT(DISTINCT UPPER(TRIM(raw_data->>'product_code'))) AS codigos_no_catalogo_rechazados
FROM dw.rejected_records
WHERE reason = 'codigo_no_catalogo';

-- ---------------------------------------------------------------------
-- P7. Soporte numérico para la recomendación al equipo de producto
-- ---------------------------------------------------------------------
-- P7a. Productos que generan pérdida (neto total negativo)
SELECT a.product_code,
       p.product_name,
       p.category,
       ROUND(SUM(a.gross_revenue), 2)   AS revenue_bruto,
       ROUND(SUM(a.returned_amount), 2) AS monto_devuelto,
       ROUND(SUM(a.net_revenue), 2)     AS revenue_neto
FROM dw.agg_daily_product_net a
JOIN dw.dim_product p USING (product_code)
GROUP BY a.product_code, p.product_name, p.category
HAVING SUM(a.net_revenue) < 0
ORDER BY revenue_neto ASC
LIMIT 15;

-- P7b. Productos relevantes (bruto >= 5.000) con mayor % de monto devuelto
SELECT a.product_code,
       p.product_name,
       p.category,
       ROUND(SUM(a.gross_revenue), 2)   AS revenue_bruto,
       ROUND(SUM(a.returned_amount), 2) AS monto_devuelto,
       ROUND(100.0 * SUM(a.returned_amount) / SUM(a.gross_revenue), 2) AS pct_devolucion
FROM dw.agg_daily_product_net a
JOIN dw.dim_product p USING (product_code)
GROUP BY a.product_code, p.product_name, p.category
HAVING SUM(a.gross_revenue) >= 5000
ORDER BY pct_devolucion DESC
LIMIT 10;

-- P7c. Cuánto del catálogo quedó en la categoría "Otros" (calidad de la clasificación)
SELECT category,
       COUNT(*) AS productos,
       ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_catalogo
FROM dw.dim_product
GROUP BY category
ORDER BY productos DESC;

SELECT r.product_code,
       p.product_name,
       ROUND(SUM(r.return_amount), 2)  AS total_devuelto,
       COUNT(*)                        AS lineas_devolucion,
       ROUND(MAX(r.return_amount), 2)  AS mayor_linea,
       ROUND(100.0 * MAX(r.return_amount) / SUM(r.return_amount), 1) AS pct_en_una_linea
FROM dw.fact_returns r
JOIN dw.dim_product p USING (product_code)
WHERE r.product_code IN ('23843','23166','84347','23113','21108','47566B','22273','22191','48185')
GROUP BY r.product_code, p.product_name
ORDER BY total_devuelto DESC;