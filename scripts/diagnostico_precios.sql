-- =============================================================================
-- Diagnostico: el chequeo `price_match` falla
--
--   price_match [finops.silver.slv_usage_priced]:
--   660 de 869 registros con precio resuelto (75.95%, minimo 98.00%)
--
-- El chequeo mide que fraccion de los registros de consumo encontro precio de
-- lista. Cuando falla hay exactamente dos causas posibles, y estas consultas
-- las distinguen:
--
--   A. El SKU no existe en `system.billing.list_prices`. Es lo normal en SKUs
--      no facturables (networking, storage incluido, creditos de prueba). No es
--      un error: hay que declararlos en `quality.checks.price_match_ignore_skus`.
--
--   B. El SKU si tiene precio pero el join no lo encuentra. Eso SI es un defecto
--      -- unidad distinta, nube distinta, o ventana de vigencia que no cubre el
--      consumo -- y subestima el gasto real. Las consultas 3 a 5 lo separan.
--
-- Ejecutar en el editor SQL con acceso a `system.billing`.
-- =============================================================================

-- 1. Que SKUs quedaron sin precio, y cuanto consumo representan.
--    Esta es la consulta principal: su salida decide A vs B.
SELECT
  sku_name,
  billing_origin_product,
  usage_unit,
  cloud,
  COUNT(*)                    AS registros,
  ROUND(SUM(usage_quantity), 4) AS cantidad_total,
  MIN(usage_date)             AS desde,
  MAX(usage_date)             AS hasta
FROM finops.silver.slv_usage_priced
WHERE price_missing
GROUP BY sku_name, billing_origin_product, usage_unit, cloud
ORDER BY registros DESC;

-- 2. Peso real del problema: los registros sin precio valorizan en 0, asi que
--    el costo que se esta perdiendo NO aparece en las tablas gold. Este es el
--    unico numero que importa para decidir si el pipeline debe bloquearse.
SELECT
  COUNT(*)                                              AS registros_totales,
  COUNT_IF(price_missing)                               AS sin_precio,
  ROUND(100.0 * COUNT_IF(price_missing) / COUNT(*), 2)  AS pct_registros_sin_precio,
  ROUND(SUM(usage_quantity), 2)                         AS cantidad_total,
  ROUND(SUM(CASE WHEN price_missing THEN usage_quantity ELSE 0 END), 2) AS cantidad_sin_precio,
  ROUND(100.0 * SUM(CASE WHEN price_missing THEN usage_quantity ELSE 0 END)
              / NULLIF(SUM(usage_quantity), 0), 2)      AS pct_cantidad_sin_precio
FROM finops.silver.slv_usage_priced;

-- 3. ¿Esos SKUs existen en list_prices? Si la columna `tramos_de_precio` es 0,
--    la causa es A. Si es > 0, la causa es B y hay que seguir con 4 y 5.
WITH sin_precio AS (
  SELECT DISTINCT sku_name, usage_unit, cloud
  FROM finops.silver.slv_usage_priced
  WHERE price_missing
)
SELECT
  s.sku_name,
  s.usage_unit           AS unidad_en_consumo,
  s.cloud                AS nube_en_consumo,
  COUNT(p.sku_name)      AS tramos_de_precio,
  COLLECT_SET(p.usage_unit)  AS unidades_en_precios,
  COLLECT_SET(p.cloud)       AS nubes_en_precios
FROM sin_precio s
LEFT JOIN finops.bronze.brz_billing_list_prices p
       ON p.sku_name = s.sku_name
GROUP BY s.sku_name, s.usage_unit, s.cloud
ORDER BY tramos_de_precio, s.sku_name;

-- 4. Causa B por vigencia: el SKU tiene precio, pero ningun tramo cubre el
--    momento del consumo. El join evalua la vigencia contra `usage_end_time`.
WITH sin_precio AS (
  SELECT sku_name, usage_unit, cloud, usage_end_time
  FROM finops.silver.slv_usage_priced
  WHERE price_missing
)
SELECT
  s.sku_name,
  MIN(s.usage_end_time)  AS consumo_desde,
  MAX(s.usage_end_time)  AS consumo_hasta,
  MIN(p.price_start_time) AS precio_vigente_desde,
  MAX(COALESCE(p.price_end_time, TIMESTAMP'2999-01-01')) AS precio_vigente_hasta
FROM sin_precio s
JOIN finops.bronze.brz_billing_list_prices p
  ON p.sku_name = s.sku_name
GROUP BY s.sku_name
HAVING MIN(s.usage_end_time) < MIN(p.price_start_time)
    OR MAX(s.usage_end_time) >= MAX(COALESCE(p.price_end_time, TIMESTAMP'2999-01-01'))
ORDER BY s.sku_name;

-- 5. Causa B por snapshot desactualizado: `brz_billing_list_prices` se
--    sobrescribe completo en cada corrida. Si aqui faltan SKUs que si estan en
--    la fuente, la ingesta de precios no corrio o fallo en silencio.
SELECT
  (SELECT COUNT(*) FROM finops.bronze.brz_billing_list_prices) AS tramos_en_bronze,
  (SELECT COUNT(*) FROM system.billing.list_prices)            AS tramos_en_origen,
  (SELECT COUNT(DISTINCT sku_name) FROM finops.bronze.brz_billing_list_prices) AS skus_en_bronze,
  (SELECT COUNT(DISTINCT sku_name) FROM system.billing.list_prices)            AS skus_en_origen;

-- 6. SKUs presentes en el origen pero ausentes del snapshot bronze.
--    Deberia devolver cero filas.
SELECT DISTINCT sku_name
FROM system.billing.list_prices
EXCEPT
SELECT DISTINCT sku_name
FROM finops.bronze.brz_billing_list_prices;
