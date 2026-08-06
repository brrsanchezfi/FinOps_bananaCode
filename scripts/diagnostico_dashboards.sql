-- =============================================================================
-- Diagnostico: "NO DATA" en los dashboards
--
-- Ejecutar en el editor SQL de Databricks, con el MISMO SQL warehouse que usan
-- los dashboards (var.warehouse_id en databricks.yml). Si aqui hay filas y el
-- dashboard sigue vacio, el problema es del widget; si aqui tampoco hay filas,
-- el problema es de datos o de permisos.
--
-- Cambiar `finops_dev` por el catalogo del entorno que se este revisando.
-- =============================================================================

-- 1. ¿Existen las tablas y tienen filas?
SELECT 'fct_cost_daily'   AS tabla, COUNT(*) AS filas, MIN(usage_date) AS desde, MAX(usage_date) AS hasta
FROM finops_dev.gold.fct_cost_daily
UNION ALL SELECT 'fct_kpi_daily', COUNT(*), MIN(usage_date), MAX(usage_date)
FROM finops_dev.gold.fct_kpi_daily
UNION ALL SELECT 'fct_budget_status', COUNT(*), MIN(as_of_date), MAX(as_of_date)
FROM finops_dev.gold.fct_budget_status
UNION ALL SELECT 'fct_recommendation', COUNT(*), MIN(analysis_date), MAX(analysis_date)
FROM finops_dev.gold.fct_recommendation
UNION ALL SELECT 'fct_cost_anomaly', COUNT(*), MIN(usage_date), MAX(usage_date)
FROM finops_dev.gold.fct_cost_anomaly
UNION ALL SELECT 'fct_cost_forecast', COUNT(*), MIN(forecast_date), MAX(forecast_date)
FROM finops_dev.gold.fct_cost_forecast
UNION ALL SELECT 'fct_chargeback_monthly', COUNT(*), NULL, NULL
FROM finops_dev.gold.fct_chargeback_monthly
ORDER BY tabla;

-- 2. La consulta exacta del contador "Costo del ultimo dia".
--    Debe devolver EXACTAMENTE una fila.
SELECT
  total_cost_usd,
  mtd_cost_usd,
  rolling_7d_avg_usd,
  usage_date
FROM finops_dev.gold.fct_kpi_daily
QUALIFY ROW_NUMBER() OVER (ORDER BY usage_date DESC) = 1;

-- 3. ¿Los filtros de ventana dejan algo fuera?
--    Casi todos los widgets filtran por los ultimos 30 dias. Si la ingesta solo
--    cubre unos pocos dias recientes esto igual debe devolver filas; si devuelve
--    cero, los datos son mas antiguos que la ventana del dashboard.
SELECT
  CURRENT_DATE()                                   AS hoy,
  MAX(usage_date)                                  AS ultimo_dia_con_datos,
  DATEDIFF(CURRENT_DATE(), MAX(usage_date))        AS dias_de_rezago,
  COUNT_IF(usage_date >= CURRENT_DATE() - INTERVAL 30 DAYS) AS filas_ultimos_30d
FROM finops_dev.gold.fct_cost_daily;

-- 4. Permisos: ¿el principal que ejecuta el dashboard puede leer el schema?
--    Si el dashboard esta publicado "con las credenciales del propietario", el
--    principal es el publicador; si no, es cada lector.
SELECT current_user() AS ejecutando_como;
SHOW GRANTS ON SCHEMA finops_dev.gold;

-- 5. Muestra de datos, para confirmar que el contenido es razonable.
SELECT usage_date, sku_group, entity_type, team, cost_center,
       ROUND(SUM(total_cost_usd), 2) AS costo_usd
FROM finops_dev.gold.fct_cost_daily
GROUP BY usage_date, sku_group, entity_type, team, cost_center
ORDER BY usage_date DESC, costo_usd DESC
LIMIT 20;
