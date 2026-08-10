-- =============================================================================
-- Inventario de etiquetas existentes
--
-- El modelo gold expone solo las SEIS dimensiones canonicas declaradas en
-- `conf/base.yml` -> `tagging.dimensions`. La capa silver resuelve esas
-- dimensiones y descarta el resto del mapa de etiquetas, asi que **el catalogo
-- no tiene hoy una tabla con las etiquetas realmente en uso**.
--
-- Estas consultas la reconstruyen desde bronze, que si conserva los mapas
-- crudos. Sirven para dos cosas:
--
--   1. Saber que se esta etiquetando de verdad, y con que ortografia.
--   2. Decidir que alias agregar a `tagging.aliases` -- una clave frecuente que
--      no este en la lista de alias es gasto que cae en SIN_ASIGNAR.
--
-- Ejecutar en el editor SQL con el warehouse de los dashboards.
-- =============================================================================

-- 1. Todas las claves de etiqueta en uso, con el costo que representan.
--    Esta es la consulta principal: el inventario propiamente dicho.
--
--    `custom_tags` viene de system.billing.usage y ya trae propagadas las
--    etiquetas del recurso, asi que es la fuente mas fiel al gasto.
WITH etiquetas AS (
  SELECT
    u.record_id,
    LOWER(TRIM(t.key))   AS clave_original,
    REGEXP_REPLACE(LOWER(TRIM(t.key)), '[^a-z0-9]', '') AS clave_normalizada,
    TRIM(t.value)        AS valor
  FROM finops.bronze.brz_billing_usage u
  LATERAL VIEW EXPLODE(u.custom_tags) t AS key, value
  WHERE u.usage_date >= CURRENT_DATE() - INTERVAL 30 DAYS
)
SELECT
  e.clave_normalizada,
  COLLECT_SET(e.clave_original)          AS ortografias_encontradas,
  COUNT(DISTINCT e.valor)                AS valores_distintos,
  COUNT(*)                               AS registros,
  ROUND(SUM(s.total_cost_usd), 2)        AS costo_usd_30d
FROM etiquetas e
LEFT JOIN finops.silver.slv_usage_priced s USING (record_id)
GROUP BY e.clave_normalizada
ORDER BY costo_usd_30d DESC NULLS LAST;

-- 2. Los valores de una clave concreta, con su costo.
--    Cambiar 'team' por la clave que interese (ya normalizada, sin _ ni -).
WITH etiquetas AS (
  SELECT
    u.record_id,
    REGEXP_REPLACE(LOWER(TRIM(t.key)), '[^a-z0-9]', '') AS clave,
    TRIM(t.value) AS valor
  FROM finops.bronze.brz_billing_usage u
  LATERAL VIEW EXPLODE(u.custom_tags) t AS key, value
  WHERE u.usage_date >= CURRENT_DATE() - INTERVAL 30 DAYS
)
SELECT
  e.valor,
  COUNT(*)                        AS registros,
  ROUND(SUM(s.total_cost_usd), 2) AS costo_usd_30d
FROM etiquetas e
LEFT JOIN finops.silver.slv_usage_priced s USING (record_id)
WHERE e.clave = 'team'
GROUP BY e.valor
ORDER BY costo_usd_30d DESC NULLS LAST;

-- 3. Claves en uso que la configuracion NO reconoce.
--    Cada una es gasto que termina en SIN_ASIGNAR pudiendo estar atribuido.
--    La lista literal de abajo son los alias de `conf/base.yml`; si se edita
--    alli, actualizar aqui tambien (o revisar contra el archivo).
WITH etiquetas AS (
  SELECT
    u.record_id,
    REGEXP_REPLACE(LOWER(TRIM(t.key)), '[^a-z0-9]', '') AS clave
  FROM finops.bronze.brz_billing_usage u
  LATERAL VIEW EXPLODE(u.custom_tags) t AS key, value
  WHERE u.usage_date >= CURRENT_DATE() - INTERVAL 30 DAYS
)
SELECT
  e.clave,
  COUNT(*)                        AS registros,
  ROUND(SUM(s.total_cost_usd), 2) AS costo_usd_30d
FROM etiquetas e
LEFT JOIN finops.silver.slv_usage_priced s USING (record_id)
WHERE e.clave NOT IN (
  -- cost_center
  'costcenter', 'cc', 'centrocosto',
  -- team
  'team', 'equipo', 'squad', 'area', 'grupo',
  -- project
  'project', 'proyecto', 'initiative', 'producto', 'product',
  -- environment
  'environment', 'env', 'ambiente', 'stage', 'entorno',
  -- application
  'application', 'app', 'aplicacion', 'sistema', 'system',
  -- owner
  'owner', 'responsable', 'dueno', 'contact', 'email'
)
GROUP BY e.clave
ORDER BY costo_usd_30d DESC NULLS LAST;

-- 4. Etiquetas definidas en los recursos, no en el consumo.
--    Un tag puede existir en el cluster o el job y no propagarse a
--    `custom_tags`; asi se ve la diferencia.
SELECT 'cluster' AS origen, LOWER(TRIM(t.key)) AS clave, COUNT(*) AS recursos
FROM finops.bronze.brz_compute_clusters
LATERAL VIEW EXPLODE(tags) t AS key, value
WHERE delete_time IS NULL
GROUP BY 1, 2
UNION ALL
SELECT 'job', LOWER(TRIM(t.key)), COUNT(*)
FROM finops.bronze.brz_lakeflow_jobs
LATERAL VIEW EXPLODE(tags) t AS key, value
WHERE delete_time IS NULL
GROUP BY 1, 2
UNION ALL
SELECT 'warehouse', LOWER(TRIM(t.key)), COUNT(*)
FROM finops.bronze.brz_compute_warehouses
LATERAL VIEW EXPLODE(tags) t AS key, value
WHERE delete_time IS NULL
GROUP BY 1, 2
ORDER BY recursos DESC;

-- 5. Cuanto gasto no tiene NINGUNA etiqueta.
--    Es el techo de lo que cualquier mejora de etiquetado puede recuperar.
SELECT
  COUNT(*)                                   AS registros,
  COUNT_IF(SIZE(COALESCE(custom_tags, MAP())) = 0) AS sin_ninguna_etiqueta,
  ROUND(100.0 * COUNT_IF(SIZE(COALESCE(custom_tags, MAP())) = 0) / COUNT(*), 2) AS pct
FROM finops.bronze.brz_billing_usage
WHERE usage_date >= CURRENT_DATE() - INTERVAL 30 DAYS;
