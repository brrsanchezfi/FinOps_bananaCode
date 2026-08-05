# Databricks notebook source
# MAGIC %md
# MAGIC # FinOps — Exploracion ad-hoc
# MAGIC
# MAGIC Notebook de consulta. **No escribe nada**: sirve para investigar un pico de
# MAGIC costo, validar una atribucion o preparar una consulta antes de llevarla a un
# MAGIC dashboard.
# MAGIC
# MAGIC Todas las consultas usan los nombres del registro de tablas, asi que
# MAGIC funcionan igual en dev, qa y prd cambiando unicamente el widget `env`.

# COMMAND ----------

import sys
from pathlib import Path

_raiz = Path.cwd()
for _candidato in [_raiz, *_raiz.parents[:4]]:
    _src = _candidato / "src"
    if (_src / "finops" / "__init__.py").exists() and str(_src) not in sys.path:
        sys.path.insert(0, str(_src))
        break

from finops.catalog import table_map  # noqa: E402
from finops.notebook import bootstrap  # noqa: E402

ctx = bootstrap()
T = table_map(ctx.cfg)
spark = ctx.spark

print(f"Entorno: {ctx.cfg.env} | catalogo: {ctx.cfg.catalog}")
for clave, fqn in sorted(T.items()):
    print(f"  {clave:<28} {fqn}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Costo diario de los ultimos 30 dias

# COMMAND ----------

spark.sql(f"""
SELECT usage_date, ROUND(SUM(total_cost_usd), 2) AS costo_usd
FROM {T['fct_cost_daily']}
WHERE usage_date >= CURRENT_DATE() - INTERVAL 30 DAYS
GROUP BY usage_date
ORDER BY usage_date
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quien gasta: top 20 por equipo y centro de costo

# COMMAND ----------

spark.sql(f"""
SELECT team, cost_center, environment,
       ROUND(SUM(total_cost_usd), 2) AS costo_usd,
       COUNT(DISTINCT entity_key) AS recursos
FROM {T['fct_cost_daily']}
WHERE usage_date >= CURRENT_DATE() - INTERVAL 30 DAYS
GROUP BY team, cost_center, environment
ORDER BY costo_usd DESC
LIMIT 20
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Investigar un dia especifico
# MAGIC
# MAGIC Cambiar la fecha para ver el desglose completo de una jornada con desviacion.

# COMMAND ----------

FECHA = ctx.cfg.max_date.isoformat()

spark.sql(f"""
SELECT entity_type, entity_name, sku_group, team, cost_center,
       ROUND(SUM(usage_quantity), 3) AS dbus,
       ROUND(SUM(total_cost_usd), 2) AS costo_usd
FROM {T['fct_cost_daily']}
WHERE usage_date = DATE'{FECHA}'
GROUP BY entity_type, entity_name, sku_group, team, cost_center
ORDER BY costo_usd DESC
LIMIT 50
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Registros de consumo sin precio resuelto
# MAGIC
# MAGIC Si esta consulta devuelve filas, hay SKUs cuyo costo se esta subestimando:
# MAGIC revisar `system.billing.list_prices` y el chequeo de calidad `price_match`.

# COMMAND ----------

spark.sql(f"""
SELECT sku_name, billing_origin_product, usage_unit,
       COUNT(*) AS registros,
       ROUND(SUM(usage_quantity), 3) AS dbus_sin_valorizar
FROM {T['slv_usage_priced']}
WHERE price_missing = true
  AND usage_date >= CURRENT_DATE() - INTERVAL 30 DAYS
GROUP BY sku_name, billing_origin_product, usage_unit
ORDER BY dbus_sin_valorizar DESC
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cobertura de etiquetado por dimension

# COMMAND ----------

spark.sql(f"""
SELECT dimension,
       ROUND(SUM(total_cost_usd), 2) AS costo_total_usd,
       ROUND(SUM(unattributed_cost_usd), 2) AS sin_atribuir_usd,
       ROUND(SUM(attributed_cost_usd) / NULLIF(SUM(total_cost_usd), 0) * 100, 2) AS cobertura_pct
FROM {T['fct_tag_coverage_daily']}
WHERE usage_date >= CURRENT_DATE() - INTERVAL 30 DAYS
GROUP BY dimension
ORDER BY cobertura_pct
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Salud del pipeline

# COMMAND ----------

spark.sql(f"""
SELECT run_id, run_date, stage, status, duration_seconds, rows_written, error_message
FROM {T['ops_run_log']}
ORDER BY run_started_at DESC, stage
LIMIT 100
""").display()
