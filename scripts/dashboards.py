#!/usr/bin/env python
"""Generacion y renderizado de los dashboards Lakeview de FinOps.

Dos subcomandos:

    python scripts/dashboards.py generate
        Reconstruye `dashboards/*.lvdash.json` desde las definiciones de este
        archivo. Los JSON quedan versionados con marcadores `{{clave_tabla}}`
        en lugar de nombres de tabla fijos, para que sirvan en los tres entornos.

    python scripts/dashboards.py render --env prd
        Sustituye los marcadores por los nombres completamente calificados del
        entorno y escribe el resultado en `.build/dashboards/<env>/`. Es el paso
        previo obligatorio a `databricks bundle deploy`.

Motivo del diseno: un dashboard Lakeview lleva el SQL embebido con nombres de
tabla literales. Versionar el catalogo de un entorno dentro del JSON haria el
repositorio no promocionable entre dev/qa/prd.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
DASHBOARDS_DIR = REPO_ROOT / "dashboards"
BUILD_DIR = REPO_ROOT / ".build" / "dashboards"
PLACEHOLDER = re.compile(r"\{\{([a-z0-9_]+)\}\}")

GRID_WIDTH = 6  # Lakeview usa una grilla de 6 columnas


# ---------------------------------------------------------------------------
# Constructores de widgets
# ---------------------------------------------------------------------------
def dataset(name: str, display: str, sql: str) -> dict[str, Any]:
    return {
        "name": name,
        "displayName": display,
        "queryLines": [linea + "\n" for linea in sql.strip().splitlines()],
    }


def _pos(x: int, y: int, w: int, h: int) -> dict[str, int]:
    return {"x": x, "y": y, "width": w, "height": h}


def markdown(name: str, texto: str, *, x: int, y: int, w: int = GRID_WIDTH, h: int = 2) -> dict[str, Any]:
    return {"widget": {"name": name, "textbox_spec": texto}, "position": _pos(x, y, w, h)}


def _query(name: str, ds: str, campos: list[tuple[str, str]], disagg: bool = False) -> dict[str, Any]:
    return {
        "name": f"{name}_query",
        "query": {
            "datasetName": ds,
            "fields": [{"name": alias, "expression": expr} for alias, expr in campos],
            "disaggregated": disagg,
        },
    }


def counter(
    name: str, ds: str, campo: str, titulo: str, *, x: int, y: int, w: int = 1, h: int = 3,
    formato: str = "number-currency-usd", decimales: int = 0,
) -> dict[str, Any]:
    return {
        "widget": {
            "name": name,
            "queries": [_query(name, ds, [(campo, f"`{campo}`")])],
            "spec": {
                "version": 2,
                "widgetType": "counter",
                "encodings": {
                    "value": {
                        "fieldName": campo,
                        "displayName": titulo,
                        "format": {"type": formato, "decimalPlaces": {"type": "exact", "places": decimales}},
                    }
                },
                "frame": {"title": titulo, "showTitle": True},
            },
        },
        "position": _pos(x, y, w, h),
    }


def chart(
    name: str, ds: str, tipo: str, titulo: str, *,
    x_campo: str, x_escala: str, x_titulo: str,
    y_campo: str, y_titulo: str, y_agg: str = "SUM",
    color_campo: str | None = None, color_titulo: str = "",
    x: int, y: int, w: int = 3, h: int = 6,
) -> dict[str, Any]:
    campos = [(x_campo, f"`{x_campo}`"), (f"{y_agg.lower()}({y_campo})", f"{y_agg}(`{y_campo}`)")]
    encodings: dict[str, Any] = {
        "x": {"fieldName": x_campo, "scale": {"type": x_escala}, "displayName": x_titulo},
        "y": {
            "fieldName": f"{y_agg.lower()}({y_campo})",
            "scale": {"type": "quantitative"},
            "displayName": y_titulo,
        },
    }
    if color_campo:
        campos.append((color_campo, f"`{color_campo}`"))
        encodings["color"] = {
            "fieldName": color_campo,
            "scale": {"type": "categorical"},
            "displayName": color_titulo or color_campo,
        }
    return {
        "widget": {
            "name": name,
            "queries": [_query(name, ds, campos)],
            "spec": {
                "version": 3,
                "widgetType": tipo,
                "encodings": encodings,
                "frame": {"title": titulo, "showTitle": True},
            },
        },
        "position": _pos(x, y, w, h),
    }


def table(
    name: str, ds: str, titulo: str, columnas: list[tuple[str, str]], *,
    x: int, y: int, w: int = GRID_WIDTH, h: int = 8,
) -> dict[str, Any]:
    return {
        "widget": {
            "name": name,
            "queries": [_query(name, ds, [(c, f"`{c}`") for c, _ in columnas], disagg=True)],
            "spec": {
                "version": 1,
                "widgetType": "table",
                "encodings": {
                    "columns": [
                        {"fieldName": campo, "displayName": etiqueta, "visible": True, "order": i}
                        for i, (campo, etiqueta) in enumerate(columnas)
                    ]
                },
                "frame": {"title": titulo, "showTitle": True},
            },
        },
        "position": _pos(x, y, w, h),
    }


# ---------------------------------------------------------------------------
# Dashboard 1 — Ejecutivo
# ---------------------------------------------------------------------------
def dashboard_ejecutivo() -> dict[str, Any]:
    datasets = [
        dataset(
            "kpi_actual", "KPIs del ultimo dia cerrado",
            """
SELECT
  total_cost_usd,
  mtd_cost_usd,
  rolling_7d_avg_usd,
  COALESCE(dod_change_pct, 0) * 100 AS dod_change_pct,
  COALESCE(wow_change_pct, 0) * 100 AS wow_change_pct,
  COALESCE(allocation_coverage_ratio, 0) * 100 AS allocation_coverage_pct,
  active_entities,
  usage_date
FROM {{fct_kpi_daily}}
QUALIFY ROW_NUMBER() OVER (ORDER BY usage_date DESC) = 1
            """,
        ),
        dataset(
            "serie_costo", "Costo diario y media movil",
            """
SELECT
  usage_date,
  total_cost_usd,
  rolling_7d_avg_usd,
  rolling_28d_avg_usd
FROM {{fct_kpi_daily}}
WHERE usage_date >= CURRENT_DATE() - INTERVAL 90 DAYS
ORDER BY usage_date
            """,
        ),
        dataset(
            "costo_por_equipo", "Costo por equipo (30 dias)",
            """
SELECT
  team,
  SUM(total_cost_usd) AS total_cost_usd
FROM {{fct_cost_daily}}
WHERE usage_date >= CURRENT_DATE() - INTERVAL 30 DAYS
GROUP BY team
ORDER BY total_cost_usd DESC
LIMIT 15
            """,
        ),
        dataset(
            "costo_por_sku", "Mezcla de consumo por tipo de compute (30 dias)",
            """
SELECT
  sku_group,
  SUM(total_cost_usd) AS total_cost_usd
FROM {{fct_cost_daily}}
WHERE usage_date >= CURRENT_DATE() - INTERVAL 30 DAYS
GROUP BY sku_group
ORDER BY total_cost_usd DESC
            """,
        ),
        dataset(
            "presupuestos", "Estado de presupuestos vigentes",
            """
SELECT
  budget_name,
  scope_label,
  budget_amount_usd,
  actual_cost_usd,
  consumed_pct,
  projected_total_usd,
  projected_pct,
  status,
  remaining_days,
  required_daily_cost_usd
FROM {{fct_budget_status}}
QUALIFY ROW_NUMBER() OVER (PARTITION BY budget_id ORDER BY as_of_date DESC) = 1
ORDER BY consumed_pct DESC
            """,
        ),
        dataset(
            "pronostico_org", "Ejecutado vs pronosticado",
            """
SELECT
  usage_date AS fecha,
  total_cost_usd AS costo_usd,
  'Ejecutado' AS serie
FROM {{fct_kpi_daily}}
WHERE usage_date >= CURRENT_DATE() - INTERVAL 60 DAYS

UNION ALL

SELECT
  forecast_date AS fecha,
  predicted_cost_usd AS costo_usd,
  'Pronosticado' AS serie
FROM {{fct_cost_forecast}}
WHERE dimension = 'organizacion'
            """,
        ),
        dataset(
            "anomalias", "Anomalias recientes",
            """
SELECT
  usage_date,
  dimension,
  dimension_value,
  severity,
  direction,
  actual_cost_usd,
  expected_cost_usd,
  deviation_usd,
  ROUND(pct_change * 100, 1) AS pct_change,
  score
FROM {{fct_cost_anomaly}}
WHERE usage_date >= CURRENT_DATE() - INTERVAL 14 DAYS
  AND severity IN ('high', 'critical')
ORDER BY usage_date DESC, ABS(score) DESC
LIMIT 50
            """,
        ),
        dataset(
            "top_entidades", "Recursos de mayor costo (30 dias)",
            """
SELECT
  entity_name,
  entity_type,
  team,
  cost_center,
  environment,
  SUM(total_cost_usd) AS total_cost_usd
FROM {{fct_cost_daily}}
WHERE usage_date >= CURRENT_DATE() - INTERVAL 30 DAYS
GROUP BY entity_name, entity_type, team, cost_center, environment
ORDER BY total_cost_usd DESC
LIMIT 25
            """,
        ),
    ]

    layout = [
        markdown(
            "titulo",
            "# FinOps Databricks — Vista ejecutiva\n"
            "Costo, tendencia, presupuestos y desviaciones. Los importes estan en USD y "
            "corresponden al consumo facturado de Databricks (DBU) segun las tablas de sistema.",
            x=0, y=0, h=2,
        ),
        counter("kpi_dia", "kpi_actual", "total_cost_usd", "Costo del ultimo dia", x=0, y=2),
        counter("kpi_mtd", "kpi_actual", "mtd_cost_usd", "Acumulado del mes", x=1, y=2),
        counter("kpi_7d", "kpi_actual", "rolling_7d_avg_usd", "Promedio 7 dias", x=2, y=2),
        counter("kpi_dod", "kpi_actual", "dod_change_pct", "Variacion vs dia previo (%)",
                x=3, y=2, formato="number-plain", decimales=1),
        counter("kpi_wow", "kpi_actual", "wow_change_pct", "Variacion vs semana previa (%)",
                x=4, y=2, formato="number-plain", decimales=1),
        counter("kpi_cobertura", "kpi_actual", "allocation_coverage_pct", "Cobertura de atribucion (%)",
                x=5, y=2, formato="number-plain", decimales=1),
        chart("serie", "serie_costo", "line", "Costo diario (90 dias)",
              x_campo="usage_date", x_escala="temporal", x_titulo="Fecha",
              y_campo="total_cost_usd", y_titulo="USD", x=0, y=5, w=4, h=7),
        chart("mezcla", "costo_por_sku", "pie", "Mezcla por tipo de compute",
              x_campo="sku_group", x_escala="categorical", x_titulo="Tipo",
              y_campo="total_cost_usd", y_titulo="USD", x=4, y=5, w=2, h=7),
        chart("equipos", "costo_por_equipo", "bar", "Costo por equipo (30 dias)",
              x_campo="team", x_escala="categorical", x_titulo="Equipo",
              y_campo="total_cost_usd", y_titulo="USD", x=0, y=12, w=3, h=7),
        chart("pronostico", "pronostico_org", "line", "Ejecutado vs pronostico",
              x_campo="fecha", x_escala="temporal", x_titulo="Fecha",
              y_campo="costo_usd", y_titulo="USD", color_campo="serie", color_titulo="Serie",
              x=3, y=12, w=3, h=7),
        markdown("sub_presupuestos", "## Presupuestos", x=0, y=19, h=1),
        table("tabla_presupuestos", "presupuestos", "Estado de presupuestos", [
            ("budget_name", "Presupuesto"), ("scope_label", "Ambito"),
            ("budget_amount_usd", "Presupuesto USD"), ("actual_cost_usd", "Ejecutado USD"),
            ("consumed_pct", "% consumido"), ("projected_total_usd", "Proyeccion USD"),
            ("projected_pct", "% proyectado"), ("status", "Estado"),
            ("remaining_days", "Dias restantes"), ("required_daily_cost_usd", "Gasto diario permitido"),
        ], x=0, y=20, h=7),
        markdown("sub_anomalias", "## Desviaciones y recursos de mayor costo", x=0, y=27, h=1),
        table("tabla_anomalias", "anomalias", "Anomalias de costo (14 dias)", [
            ("usage_date", "Fecha"), ("dimension", "Dimension"), ("dimension_value", "Valor"),
            ("severity", "Severidad"), ("direction", "Sentido"),
            ("actual_cost_usd", "Real USD"), ("expected_cost_usd", "Esperado USD"),
            ("deviation_usd", "Desviacion USD"), ("pct_change", "% cambio"), ("score", "Score"),
        ], x=0, y=28, h=8),
        table("tabla_entidades", "top_entidades", "Top 25 recursos por costo (30 dias)", [
            ("entity_name", "Recurso"), ("entity_type", "Tipo"), ("team", "Equipo"),
            ("cost_center", "Centro de costo"), ("environment", "Ambiente"),
            ("total_cost_usd", "Costo USD"),
        ], x=0, y=36, h=8),
    ]

    return {
        "datasets": datasets,
        "pages": [{"name": "ejecutivo", "displayName": "Vista ejecutiva", "layout": layout}],
    }


# ---------------------------------------------------------------------------
# Dashboard 2 — Costos y chargeback
# ---------------------------------------------------------------------------
def dashboard_costos() -> dict[str, Any]:
    datasets = [
        dataset(
            "chargeback_mes", "Chargeback del ultimo mes cerrado",
            """
SELECT
  unit,
  direct_cost_usd,
  allocated_shared_usd,
  allocated_unallocated_usd,
  overhead_usd,
  total_chargeback_usd,
  ROUND(pct_of_total * 100, 2) AS pct_of_total,
  entity_count
FROM {{fct_chargeback_monthly}}
WHERE period = (SELECT MAX(period) FROM {{fct_chargeback_monthly}})
ORDER BY total_chargeback_usd DESC
            """,
        ),
        dataset(
            "chargeback_tendencia", "Evolucion mensual por unidad",
            """
SELECT
  period,
  unit,
  total_chargeback_usd
FROM {{fct_chargeback_monthly}}
WHERE period >= DATE_FORMAT(ADD_MONTHS(CURRENT_DATE(), -12), 'yyyy-MM')
ORDER BY period, total_chargeback_usd DESC
            """,
        ),
        dataset(
            "costo_mensual", "Costo mensual por tipo de compute",
            """
SELECT
  year_month,
  sku_group,
  SUM(total_cost_usd) AS total_cost_usd,
  SUM(usage_quantity) AS total_dbus
FROM {{agg_cost_monthly}}
WHERE year_month >= DATE_FORMAT(ADD_MONTHS(CURRENT_DATE(), -12), 'yyyy-MM')
GROUP BY year_month, sku_group
ORDER BY year_month, total_cost_usd DESC
            """,
        ),
        dataset(
            "costo_workspace", "Costo por workspace (30 dias)",
            """
SELECT
  w.workspace_name,
  c.workspace_id,
  SUM(c.total_cost_usd) AS total_cost_usd
FROM {{fct_cost_daily}} c
LEFT JOIN {{dim_workspace}} w ON c.workspace_id = w.workspace_id
WHERE c.usage_date >= CURRENT_DATE() - INTERVAL 30 DAYS
GROUP BY w.workspace_name, c.workspace_id
ORDER BY total_cost_usd DESC
            """,
        ),
        dataset(
            "cobertura", "Cobertura de etiquetado",
            """
SELECT
  usage_date,
  dimension,
  SUM(total_cost_usd) AS total_cost_usd,
  SUM(unattributed_cost_usd) AS unattributed_cost_usd,
  ROUND(SUM(attributed_cost_usd) / NULLIF(SUM(total_cost_usd), 0) * 100, 2) AS coverage_pct
FROM {{fct_tag_coverage_daily}}
WHERE usage_date >= CURRENT_DATE() - INTERVAL 60 DAYS
GROUP BY usage_date, dimension
ORDER BY usage_date
            """,
        ),
        dataset(
            "jobs_costosos", "Jobs de mayor costo (30 dias)",
            """
SELECT
  COALESCE(job_name, CAST(job_id AS STRING)) AS job,
  team,
  cost_center,
  COUNT(*) AS ejecuciones,
  SUM(CASE WHEN is_failed THEN 1 ELSE 0 END) AS fallidas,
  ROUND(AVG(duration_minutes), 2) AS duracion_promedio_min,
  ROUND(SUM(run_cost_usd), 2) AS costo_usd,
  ROUND(SUM(run_cost_usd) / NULLIF(COUNT(*), 0), 4) AS costo_por_ejecucion_usd
FROM {{fct_job_run_cost}}
WHERE run_date >= CURRENT_DATE() - INTERVAL 30 DAYS
GROUP BY job, team, cost_center
ORDER BY costo_usd DESC
LIMIT 30
            """,
        ),
        dataset(
            "warehouses", "SQL warehouses (30 dias)",
            """
SELECT
  COALESCE(warehouse_name, warehouse_id) AS warehouse,
  warehouse_size,
  warehouse_type,
  SUM(total_cost_usd) AS costo_usd,
  SUM(query_count) AS consultas,
  ROUND(SUM(active_hours), 2) AS horas_activas,
  ROUND(SUM(total_cost_usd) / NULLIF(SUM(query_count), 0), 4) AS costo_por_consulta_usd,
  MAX(auto_stop_minutes) AS auto_stop_min
FROM {{fct_warehouse_cost_daily}}
WHERE usage_date >= CURRENT_DATE() - INTERVAL 30 DAYS
GROUP BY warehouse, warehouse_size, warehouse_type
ORDER BY costo_usd DESC
LIMIT 30
            """,
        ),
        dataset(
            "detalle_entidad", "Detalle de consumo por recurso",
            """
SELECT
  usage_date,
  entity_type,
  entity_name,
  sku_group,
  team,
  cost_center,
  project,
  environment,
  ROUND(usage_quantity, 4) AS dbus,
  ROUND(total_cost_usd, 4) AS costo_usd
FROM {{fct_cost_daily}}
WHERE usage_date >= CURRENT_DATE() - INTERVAL 7 DAYS
ORDER BY costo_usd DESC
LIMIT 500
            """,
        ),
    ]

    layout = [
        markdown(
            "titulo",
            "# Costos y chargeback\n"
            "Imputacion de costo por unidad de negocio, evolucion mensual y detalle por recurso. "
            "El chargeback incluye el prorrateo del costo compartido y del costo sin atribuir "
            "segun la estrategia definida en `conf/budgets.yml`.",
            x=0, y=0, h=2,
        ),
        chart("cb_barras", "chargeback_mes", "bar", "Chargeback del ultimo mes",
              x_campo="unit", x_escala="categorical", x_titulo="Unidad",
              y_campo="total_chargeback_usd", y_titulo="USD", x=0, y=2, w=3, h=7),
        chart("cb_tendencia", "chargeback_tendencia", "area", "Evolucion mensual por unidad",
              x_campo="period", x_escala="categorical", x_titulo="Mes",
              y_campo="total_chargeback_usd", y_titulo="USD",
              color_campo="unit", color_titulo="Unidad", x=3, y=2, w=3, h=7),
        table("cb_tabla", "chargeback_mes", "Detalle de imputacion", [
            ("unit", "Unidad"), ("direct_cost_usd", "Directo USD"),
            ("allocated_shared_usd", "Compartido asignado USD"),
            ("allocated_unallocated_usd", "No atribuido asignado USD"),
            ("overhead_usd", "Recargo USD"), ("total_chargeback_usd", "Total USD"),
            ("pct_of_total", "% del total"), ("entity_count", "Recursos"),
        ], x=0, y=9, h=7),
        markdown("sub_tendencia", "## Evolucion y distribucion", x=0, y=16, h=1),
        chart("mensual", "costo_mensual", "bar", "Costo mensual por tipo de compute",
              x_campo="year_month", x_escala="categorical", x_titulo="Mes",
              y_campo="total_cost_usd", y_titulo="USD",
              color_campo="sku_group", color_titulo="Tipo", x=0, y=17, w=3, h=7),
        chart("workspaces", "costo_workspace", "bar", "Costo por workspace (30 dias)",
              x_campo="workspace_name", x_escala="categorical", x_titulo="Workspace",
              y_campo="total_cost_usd", y_titulo="USD", x=3, y=17, w=3, h=7),
        chart("cobertura_serie", "cobertura", "line", "Cobertura de etiquetado (%)",
              x_campo="usage_date", x_escala="temporal", x_titulo="Fecha",
              y_campo="coverage_pct", y_titulo="% cobertura", y_agg="AVG",
              color_campo="dimension", color_titulo="Dimension", x=0, y=24, w=6, h=6),
        markdown("sub_recursos", "## Jobs y warehouses", x=0, y=30, h=1),
        table("tabla_jobs", "jobs_costosos", "Jobs de mayor costo (30 dias)", [
            ("job", "Job"), ("team", "Equipo"), ("cost_center", "Centro de costo"),
            ("ejecuciones", "Ejecuciones"), ("fallidas", "Fallidas"),
            ("duracion_promedio_min", "Duracion prom. (min)"),
            ("costo_usd", "Costo USD"), ("costo_por_ejecucion_usd", "USD/ejecucion"),
        ], x=0, y=31, h=8),
        table("tabla_warehouses", "warehouses", "SQL warehouses (30 dias)", [
            ("warehouse", "Warehouse"), ("warehouse_size", "Tamano"), ("warehouse_type", "Tipo"),
            ("costo_usd", "Costo USD"), ("consultas", "Consultas"),
            ("horas_activas", "Horas activas"), ("costo_por_consulta_usd", "USD/consulta"),
            ("auto_stop_min", "Auto-stop (min)"),
        ], x=0, y=39, h=8),
        markdown("sub_detalle", "## Detalle de consumo (7 dias)", x=0, y=47, h=1),
        table("tabla_detalle", "detalle_entidad", "Consumo por recurso y dia", [
            ("usage_date", "Fecha"), ("entity_type", "Tipo"), ("entity_name", "Recurso"),
            ("sku_group", "SKU"), ("team", "Equipo"), ("cost_center", "Centro de costo"),
            ("project", "Proyecto"), ("environment", "Ambiente"),
            ("dbus", "DBUs"), ("costo_usd", "Costo USD"),
        ], x=0, y=48, h=10),
    ]

    return {
        "datasets": datasets,
        "pages": [{"name": "costos", "displayName": "Costos y chargeback", "layout": layout}],
    }


# ---------------------------------------------------------------------------
# Dashboard 3 — Optimizacion y gobierno
# ---------------------------------------------------------------------------
def dashboard_optimizacion() -> dict[str, Any]:
    datasets = [
        dataset(
            "resumen_ahorro", "Potencial de ahorro identificado",
            """
SELECT
  SUM(estimated_monthly_savings_usd) AS ahorro_mensual_usd,
  SUM(CASE WHEN confidence = 'alta' THEN estimated_monthly_savings_usd ELSE 0 END) AS ahorro_alta_confianza_usd,
  COUNT(*) AS recomendaciones,
  COUNT(DISTINCT entity_key) AS recursos_afectados
FROM {{fct_recommendation}}
WHERE analysis_date = (SELECT MAX(analysis_date) FROM {{fct_recommendation}})
            """,
        ),
        dataset(
            "ahorro_por_regla", "Ahorro estimado por tipo de hallazgo",
            """
SELECT
  rule_id,
  COUNT(*) AS hallazgos,
  SUM(estimated_monthly_savings_usd) AS ahorro_mensual_usd
FROM {{fct_recommendation}}
WHERE analysis_date = (SELECT MAX(analysis_date) FROM {{fct_recommendation}})
GROUP BY rule_id
ORDER BY ahorro_mensual_usd DESC
            """,
        ),
        dataset(
            "recomendaciones", "Recomendaciones accionables",
            """
SELECT
  rule_id,
  title,
  entity_name,
  entity_type,
  team,
  cost_center,
  environment,
  observed_cost_usd,
  estimated_monthly_savings_usd,
  confidence,
  severity,
  recommendation,
  estimation_method
FROM {{fct_recommendation}}
WHERE analysis_date = (SELECT MAX(analysis_date) FROM {{fct_recommendation}})
ORDER BY estimated_monthly_savings_usd DESC
LIMIT 200
            """,
        ),
        dataset(
            "eficiencia", "Indicadores de eficiencia",
            """
SELECT
  usage_date,
  ROUND(COALESCE(serverless_share, 0) * 100, 2) AS serverless_pct,
  ROUND(COALESCE(all_purpose_share, 0) * 100, 2) AS all_purpose_pct,
  ROUND(COALESCE(allocation_coverage_ratio, 0) * 100, 2) AS cobertura_pct,
  cost_per_dbu_usd
FROM {{fct_kpi_daily}}
WHERE usage_date >= CURRENT_DATE() - INTERVAL 90 DAYS
ORDER BY usage_date
            """,
        ),
        dataset(
            "gasto_sin_atribuir", "Gasto sin atribucion por recurso (30 dias)",
            """
SELECT
  entity_name,
  entity_type,
  workspace_id,
  SUM(total_cost_usd) AS costo_usd
FROM {{fct_cost_daily}}
WHERE usage_date >= CURRENT_DATE() - INTERVAL 30 DAYS
  AND is_untagged = true
GROUP BY entity_name, entity_type, workspace_id
ORDER BY costo_usd DESC
LIMIT 50
            """,
        ),
        dataset(
            "jobs_inestables", "Jobs con mayor desperdicio por fallos (30 dias)",
            """
SELECT
  COALESCE(job_name, CAST(job_id AS STRING)) AS job,
  team,
  COUNT(*) AS ejecuciones,
  SUM(CASE WHEN is_failed THEN 1 ELSE 0 END) AS fallidas,
  ROUND(SUM(CASE WHEN is_failed THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0) * 100, 1) AS tasa_fallo_pct,
  ROUND(SUM(CASE WHEN is_failed THEN run_cost_usd ELSE 0 END), 2) AS costo_fallido_usd
FROM {{fct_job_run_cost}}
WHERE run_date >= CURRENT_DATE() - INTERVAL 30 DAYS
GROUP BY job, team
HAVING SUM(CASE WHEN is_failed THEN 1 ELSE 0 END) > 0
ORDER BY costo_fallido_usd DESC
LIMIT 30
            """,
        ),
        dataset(
            "config_clusters", "Configuracion de clusters con costo",
            """
SELECT
  e.entity_name,
  e.workspace_id,
  e.autotermination_minutes,
  e.autoscale_enabled,
  e.num_workers,
  e.spark_version,
  e.days_since_activity,
  e.lifetime_cost_usd
FROM {{dim_entity}} e
WHERE e.entity_type = 'CLUSTER'
ORDER BY e.lifetime_cost_usd DESC
LIMIT 100
            """,
        ),
        dataset(
            "salud_pipeline", "Ultimas corridas del pipeline FinOps",
            """
SELECT
  run_id,
  run_date,
  stage,
  status,
  duration_seconds,
  rows_written,
  error_message
FROM {{ops_run_log}}
WHERE run_started_at >= CURRENT_TIMESTAMP() - INTERVAL 7 DAYS
ORDER BY run_started_at DESC, stage
LIMIT 200
            """,
        ),
        dataset(
            "alertas", "Alertas recientes",
            """
SELECT
  created_at,
  rule_id,
  severity,
  title,
  scope,
  dispatch_status,
  channels,
  delivered
FROM {{ops_alert_log}}
WHERE created_at >= CURRENT_TIMESTAMP() - INTERVAL 14 DAYS
ORDER BY created_at DESC
LIMIT 200
            """,
        ),
    ]

    layout = [
        markdown(
            "titulo",
            "# Optimizacion y gobierno\n"
            "Oportunidades de ahorro, calidad del etiquetado y salud de la plataforma. "
            "**Los ahorros son estimaciones**: cada recomendacion declara su metodo de calculo "
            "y su nivel de confianza en la columna correspondiente. Validar antes de comprometer cifras.",
            x=0, y=0, h=2,
        ),
        counter("ahorro_total", "resumen_ahorro", "ahorro_mensual_usd",
                "Ahorro mensual estimado", x=0, y=2, w=2),
        counter("ahorro_confiable", "resumen_ahorro", "ahorro_alta_confianza_usd",
                "Ahorro de alta confianza", x=2, y=2, w=2),
        counter("num_recomendaciones", "resumen_ahorro", "recomendaciones",
                "Recomendaciones abiertas", x=4, y=2, w=1, formato="number-plain"),
        counter("recursos_afectados", "resumen_ahorro", "recursos_afectados",
                "Recursos involucrados", x=5, y=2, w=1, formato="number-plain"),
        chart("por_regla", "ahorro_por_regla", "bar", "Ahorro estimado por tipo de hallazgo",
              x_campo="rule_id", x_escala="categorical", x_titulo="Hallazgo",
              y_campo="ahorro_mensual_usd", y_titulo="USD/mes", x=0, y=5, w=3, h=7),
        chart("eficiencia_serie", "eficiencia", "line", "Indicadores de eficiencia (%)",
              x_campo="usage_date", x_escala="temporal", x_titulo="Fecha",
              y_campo="serverless_pct", y_titulo="% serverless", y_agg="AVG",
              x=3, y=5, w=3, h=7),
        markdown("sub_reco", "## Recomendaciones", x=0, y=12, h=1),
        table("tabla_reco", "recomendaciones", "Recomendaciones priorizadas por ahorro", [
            ("rule_id", "Hallazgo"), ("title", "Titulo"), ("entity_name", "Recurso"),
            ("entity_type", "Tipo"), ("team", "Equipo"), ("environment", "Ambiente"),
            ("observed_cost_usd", "Costo observado USD"),
            ("estimated_monthly_savings_usd", "Ahorro estimado USD/mes"),
            ("confidence", "Confianza"), ("severity", "Prioridad"),
            ("recommendation", "Accion sugerida"), ("estimation_method", "Metodo de estimacion"),
        ], x=0, y=13, h=10),
        markdown("sub_gobierno", "## Gobierno del etiquetado", x=0, y=23, h=1),
        table("tabla_sin_tag", "gasto_sin_atribuir", "Gasto sin atribucion (30 dias)", [
            ("entity_name", "Recurso"), ("entity_type", "Tipo"),
            ("workspace_id", "Workspace"), ("costo_usd", "Costo USD"),
        ], x=0, y=24, w=3, h=8),
        table("tabla_clusters", "config_clusters", "Configuracion de clusters", [
            ("entity_name", "Cluster"), ("autotermination_minutes", "Auto-term (min)"),
            ("autoscale_enabled", "Autoescalado"), ("num_workers", "Workers"),
            ("spark_version", "DBR"), ("days_since_activity", "Dias sin uso"),
            ("lifetime_cost_usd", "Costo acumulado USD"),
        ], x=3, y=24, w=3, h=8),
        markdown("sub_jobs", "## Desperdicio por fallos", x=0, y=32, h=1),
        table("tabla_jobs_malos", "jobs_inestables", "Jobs con ejecuciones fallidas (30 dias)", [
            ("job", "Job"), ("team", "Equipo"), ("ejecuciones", "Ejecuciones"),
            ("fallidas", "Fallidas"), ("tasa_fallo_pct", "% fallo"),
            ("costo_fallido_usd", "Costo desperdiciado USD"),
        ], x=0, y=33, h=8),
        markdown("sub_plataforma", "## Salud de la plataforma FinOps", x=0, y=41, h=1),
        table("tabla_corridas", "salud_pipeline", "Corridas del pipeline (7 dias)", [
            ("run_date", "Fecha"), ("stage", "Etapa"), ("status", "Estado"),
            ("duration_seconds", "Duracion (s)"), ("rows_written", "Filas"),
            ("error_message", "Error"),
        ], x=0, y=42, w=3, h=8),
        table("tabla_alertas", "alertas", "Alertas emitidas (14 dias)", [
            ("created_at", "Momento"), ("rule_id", "Regla"), ("severity", "Severidad"),
            ("title", "Titulo"), ("scope", "Ambito"), ("dispatch_status", "Estado"),
            ("channels", "Canales"), ("delivered", "Entregada"),
        ], x=3, y=42, w=3, h=8),
    ]

    return {
        "datasets": datasets,
        "pages": [{"name": "optimizacion", "displayName": "Optimizacion y gobierno", "layout": layout}],
    }


DASHBOARDS = {
    "finops_ejecutivo": dashboard_ejecutivo,
    "finops_costos": dashboard_costos,
    "finops_optimizacion": dashboard_optimizacion,
}


# ---------------------------------------------------------------------------
# Subcomandos
# ---------------------------------------------------------------------------
def cmd_generate(args: argparse.Namespace) -> int:
    DASHBOARDS_DIR.mkdir(parents=True, exist_ok=True)
    for nombre, constructor in DASHBOARDS.items():
        destino = DASHBOARDS_DIR / f"{nombre}.lvdash.json"
        destino.write_text(
            json.dumps(constructor(), indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
        )
        print(f"generado {destino.relative_to(REPO_ROOT)}")
    return 0


def cmd_render(args: argparse.Namespace) -> int:
    sys.path.insert(0, str(REPO_ROOT / "src"))
    from finops.catalog import TABLES_BY_KEY, table_map
    from finops.config import load_config

    cfg = load_config(args.env, conf_dir=REPO_ROOT / "conf", use_env_vars=False)
    mapa = table_map(cfg)

    destino_dir = BUILD_DIR / args.env
    destino_dir.mkdir(parents=True, exist_ok=True)

    faltantes: set[str] = set()

    def sustituir(match: re.Match[str]) -> str:
        clave = match.group(1)
        if clave not in TABLES_BY_KEY:
            faltantes.add(clave)
            return match.group(0)
        return mapa[clave]

    archivos = sorted(DASHBOARDS_DIR.glob("*.lvdash.json"))
    if not archivos:
        print("No hay dashboards para renderizar. Ejecuta primero 'generate'.", file=sys.stderr)
        return 1

    # Se resuelve todo primero: si algun marcador no existe se aborta sin
    # escribir nada, para no dejar un directorio de build a medias.
    renderizados = {a: PLACEHOLDER.sub(sustituir, a.read_text(encoding="utf-8")) for a in archivos}
    if faltantes:
        print(f"ERROR: marcadores sin tabla registrada: {sorted(faltantes)}", file=sys.stderr)
        return 1

    for archivo, texto in renderizados.items():
        salida = destino_dir / archivo.name
        salida.write_text(texto, encoding="utf-8")
        print(f"renderizado {salida.relative_to(REPO_ROOT)}")

    print(f"\nCatalogo objetivo: {cfg.catalog}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Dashboards Lakeview de FinOps")
    sub = parser.add_subparsers(dest="command", required=True)

    p_gen = sub.add_parser("generate", help="Reconstruye los .lvdash.json versionados")
    p_gen.set_defaults(func=cmd_generate)

    p_render = sub.add_parser("render", help="Sustituye marcadores por tablas del entorno")
    p_render.add_argument("--env", required=True, choices=["dev", "qa", "prd"])
    p_render.set_defaults(func=cmd_render)

    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    sys.exit(main())
