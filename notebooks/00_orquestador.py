# Databricks notebook source
# MAGIC %md
# MAGIC # FinOps Databricks — Orquestador general
# MAGIC
# MAGIC Este notebook ejecuta el pipeline completo de analitica FinOps. **No contiene
# MAGIC logica de negocio**: toda la logica vive en el paquete `finops` (carpeta `src/`),
# MAGIC que se prueba con `pytest` sin necesidad de un cluster.
# MAGIC
# MAGIC ## Etapas
# MAGIC
# MAGIC | Etapa | Que hace |
# MAGIC |---|---|
# MAGIC | `setup` | Crea catalogo y schemas en Unity Catalog |
# MAGIC | `bronze` | Copia incremental de las system tables |
# MAGIC | `silver` | Valorizacion del consumo, entidades y etiquetas normalizadas |
# MAGIC | `gold` | Modelo dimensional (hechos, dimensiones, KPIs) |
# MAGIC | `analytics` | Anomalias, pronostico, presupuestos, recomendaciones, chargeback |
# MAGIC | `quality` | Chequeos de calidad de datos |
# MAGIC | `alerts` | Reglas de alerta, deduplicacion y despacho a canales |
# MAGIC | `maintenance` | OPTIMIZE/ZORDER y comentarios de catalogo |
# MAGIC
# MAGIC ## Parametros (widgets)
# MAGIC
# MAGIC - **env**: `dev` | `qa` | `prd`
# MAGIC - **stages**: etapas separadas por coma. Vacio = todas.
# MAGIC - **run_date**: fecha logica de la corrida (`YYYY-MM-DD`). Vacio = hoy.
# MAGIC - **full_refresh**: `true` reprocesa la ventana historica completa.
# MAGIC - **dry_run**: `true` calcula sin escribir (util para validar permisos).
# MAGIC - **overrides**: overrides de configuracion, ej. `anomaly.score_threshold=4.0;forecast.horizon_days=60`

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Preparacion
# MAGIC
# MAGIC `bootstrap()` declara los widgets, resuelve la configuracion efectiva
# MAGIC (`conf/base.yml` + `conf/<env>.yml` + overrides) y configura la sesion de Spark.

# COMMAND ----------

import sys
from pathlib import Path

# Permite ejecutar el notebook directamente desde el workspace sincronizado por el
# bundle, aun sin el wheel instalado en el cluster.
_raiz = Path.cwd()
for _candidato in [_raiz, *_raiz.parents[:4]]:
    _src = _candidato / "src"
    if (_src / "finops" / "__init__.py").exists() and str(_src) not in sys.path:
        sys.path.insert(0, str(_src))
        break

from finops.notebook import bootstrap, resumen  # noqa: E402

ctx = bootstrap()
print(ctx.cfg.describe())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Configuracion efectiva
# MAGIC
# MAGIC Se imprime redactada: nunca incluye URLs de webhook ni valores de secretos.

# COMMAND ----------

import json  # noqa: E402

print(json.dumps(ctx.cfg.redacted(), indent=2, ensure_ascii=False, default=str))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Ejecucion del pipeline

# COMMAND ----------

resultado = ctx.run(dashboard_url=ctx.params.get("dashboard_url", ""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Resumen de la corrida
# MAGIC
# MAGIC Si alguna etapa fallo, esta celda lanza una excepcion para que la tarea del
# MAGIC job quede marcada como fallida (y dispare las notificaciones del job).

# COMMAND ----------

resumen(resultado)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Vista rapida de resultados

# COMMAND ----------

from finops.catalog import (  # noqa: E402
    GOLD_BUDGET_STATUS,
    GOLD_KPI_DAILY,
    GOLD_RECOMMENDATION,
)
from finops.spark_utils import table_exists  # noqa: E402

if table_exists(ctx.spark, GOLD_KPI_DAILY.fqn(ctx.cfg)):
    print("KPIs de los ultimos 7 dias:")
    ctx.spark.table(GOLD_KPI_DAILY.fqn(ctx.cfg)).orderBy("usage_date", ascending=False).limit(7).display()

# COMMAND ----------

if table_exists(ctx.spark, GOLD_BUDGET_STATUS.fqn(ctx.cfg)):
    print("Estado de presupuestos:")
    (
        ctx.spark.table(GOLD_BUDGET_STATUS.fqn(ctx.cfg))
        .filter(f"as_of_date = DATE'{ctx.cfg.max_date.isoformat()}'")
        .select(
            "budget_name", "scope_label", "budget_amount_usd", "actual_cost_usd",
            "consumed_pct", "projected_total_usd", "status",
        )
        .orderBy("consumed_pct", ascending=False)
        .display()
    )

# COMMAND ----------

if table_exists(ctx.spark, GOLD_RECOMMENDATION.fqn(ctx.cfg)):
    print("Top 20 recomendaciones por ahorro estimado:")
    (
        ctx.spark.table(GOLD_RECOMMENDATION.fqn(ctx.cfg))
        .select(
            "rule_id", "entity_name", "team", "observed_cost_usd",
            "estimated_monthly_savings_usd", "confidence", "recommendation",
        )
        .orderBy("estimated_monthly_savings_usd", ascending=False)
        .limit(20)
        .display()
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Salida de la tarea
# MAGIC
# MAGIC Se devuelve un resumen en JSON para que otras tareas del job puedan
# MAGIC consumirlo con `dbutils.jobs.taskValues` o `run_job_task`.

# COMMAND ----------

salida = {
    "run_id": resultado.run_id,
    "environment": ctx.cfg.env,
    "window_start": ctx.cfg.min_date.isoformat(),
    "window_end": ctx.cfg.max_date.isoformat(),
    "ok": resultado.ok,
    "stages": {m.stage: m.status for m in resultado.recorder.metrics},
}
if ctx.dbutils is not None:
    ctx.dbutils.notebook.exit(json.dumps(salida))
else:
    print(json.dumps(salida, indent=2))
