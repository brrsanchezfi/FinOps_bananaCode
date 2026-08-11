#!/usr/bin/env python
"""Generador de los dashboards Lakeview de FinOps.

    python scripts/dashboards.py generate   # reconstruye dashboards/*.lvdash.json
    python scripts/dashboards.py check      # verifica que esten al dia

Este archivo es la **unica fuente de verdad** de los dashboards: aqui viven el
SQL y el layout como codigo Python revisable. `generate` escribe el JSON ya
resuelto en `dashboards/*.lvdash.json`, que se versiona y es lo que despliega el
bundle.

En el SQL de este archivo las tablas se escriben como marcadores `{{clave}}` del
registro de `finops.catalog`; `generate` los sustituye por el nombre resuelto
desde la configuracion. Ningun nombre de catalogo se escribe a mano.

Como los tres entornos comparten catalogo y schemas, los nombres resueltos son
identicos y basta **un** juego de archivos. Si algun dia un entorno apuntara a
otro sitio, `check` lo detecta y avisa que hay que volver a generar por entorno.

> Los JSON generados **no se editan a mano**: el siguiente `generate` los
> sobrescribe y CI falla si difieren de lo que produce este archivo.
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
PLACEHOLDER = re.compile(r"\{\{([a-z0-9_]+)\}\}")
ENVIRONMENTS = ("dev", "qa", "prd")

#: Lakeview usa una grilla de 12 columnas. Se confirmo exportando un dashboard
#: real del workspace: contenia un widget en x=7 con width=3, imposible en una
#: grilla de 6.
GRID_COLUMNAS_LAKEVIEW = 12

#: El layout de este archivo se escribe sobre una grilla logica de 6 columnas
#: (mas comoda de leer) y `_pos` la escala a la real. Cambiar solo esto evita
#: reescribir las coordenadas de los cuarenta y tantos widgets.
GRID_WIDTH = 6
_ESCALA_GRILLA = GRID_COLUMNAS_LAKEVIEW // GRID_WIDTH

#: Valores que exige el esquema de Lakeview en cada pagina. Sin ellos el
#: dashboard se despliega pero los widgets no se enlazan con sus consultas, y
#: aparecen con el marcador "Select fields to visualize".
PAGE_TYPE = "PAGE_TYPE_CANVAS"
LAYOUT_VERSION = "GRID_V1"

#: Preferencias de presentacion del dashboard completo.
UI_SETTINGS: dict[str, Any] = {
    "theme": {"widgetHeaderAlignment": "ALIGNMENT_UNSPECIFIED"},
    "applyModeEnabled": False,
}


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
    """Traduce coordenadas de la grilla logica de 6 a la real de 12 columnas."""
    return {
        "x": x * _ESCALA_GRILLA,
        "y": y,
        "width": w * _ESCALA_GRILLA,
        "height": h,
    }


def markdown(name: str, texto: str, *, x: int, y: int, w: int = GRID_WIDTH, h: int = 2) -> dict[str, Any]:
    """Widget de texto.

    Lakeview usa `multilineTextboxSpec` con el contenido partido en lineas; cada
    una conserva su salto salvo la ultima. La clave `textbox_spec` no existe en
    el esquema y el widget quedaba en blanco.
    """
    lineas = texto.split("\n")
    return {
        "widget": {
            "name": name,
            "multilineTextboxSpec": {
                "lines": [ln + "\n" for ln in lineas[:-1]] + [lineas[-1]],
            },
        },
        "position": _pos(x, y, w, h),
    }


#: Nombre de la consulta dentro de cada widget. Es local al widget, y el `spec`
#: lo referencia en `data.queryName`.
QUERY_NAME = "main_query"


def _query(ds: str, campos: list[tuple[str, str]], disagg: bool = False) -> dict[str, Any]:
    return {
        "name": QUERY_NAME,
        "query": {
            "datasetName": ds,
            "fields": [{"name": alias, "expression": expr} for alias, expr in campos],
            "disaggregated": disagg,
        },
    }


def _spec(widget_type: str, version: int, encodings: dict[str, Any], titulo: str) -> dict[str, Any]:
    """Spec de widget con el enlace a su consulta.

    `data.queryName` es lo que ata el widget a su consulta. Sin esa clave
    Lakeview no sabe de donde salen los campos y muestra el marcador
    "Select fields to visualize", aunque el widget declare encodings validos.
    """
    return {
        "version": version,
        "widgetType": widget_type,
        "encodings": encodings,
        "frame": {"title": titulo, "showTitle": True},
        "data": {"queryName": QUERY_NAME},
    }


def counter(
    name: str, ds: str, campo: str, titulo: str, *, x: int, y: int, w: int = 1, h: int = 3,
    agg: str = "SUM",
) -> dict[str, Any]:
    """Contador de un unico valor.

    Replica la forma de un contador construido en la UI y verificado en el
    workspace: el campo se agrega (`SUM`) y la consulta va agrupada
    (`disaggregated=False`). Los datasets que alimentan contadores devuelven una
    sola fila, asi que la agregacion no altera el valor.
    """
    medida = f"{agg.lower()}({campo})"
    return {
        "widget": {
            "name": name,
            "queries": [_query(ds, [(medida, f"{agg}(`{campo}`)")], disagg=False)],
            "spec": _spec("counter", 2, {"value": {"fieldName": medida, "displayName": titulo}}, titulo),
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
    medida = f"{y_agg.lower()}({y_campo})"
    campos = [(x_campo, f"`{x_campo}`"), (medida, f"{y_agg}(`{y_campo}`)")]

    if tipo == "pie":
        # Un pie no tiene ejes: la magnitud va en `angle` y la categoria en
        # `color`. Con `x`/`y` el widget no renderiza.
        encodings: dict[str, Any] = {
            "angle": {
                "fieldName": medida,
                "scale": {"type": "quantitative"},
                "displayName": y_titulo,
            },
            "color": {
                "fieldName": x_campo,
                "scale": {"type": "categorical"},
                "displayName": x_titulo,
            },
        }
        return {
            "widget": {
                "name": name,
                "queries": [_query(ds, campos)],
                "spec": _spec("pie", 3, encodings, titulo),
            },
            "position": _pos(x, y, w, h),
        }

    encodings = {
        "x": {"fieldName": x_campo, "scale": {"type": x_escala}, "displayName": x_titulo},
        "y": {
            "fieldName": medida,
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
            "queries": [_query(ds, campos)],
            "spec": _spec(tipo, 3, encodings, titulo),
        },
        "position": _pos(x, y, w, h),
    }


# Aqui vivia `_tipo_columna`, que deducia `type` y `displayAs` por el nombre de
# la columna. Esos metadatos pertenecen al formato de tabla version 1 y son
# justamente lo que rompia el widget en la version 2; ver `table` mas abajo.


def table(
    name: str, ds: str, titulo: str, columnas: list[tuple[str, str]], *,
    x: int, y: int, w: int = GRID_WIDTH, h: int = 8,
) -> dict[str, Any]:
    """Tabla de filas crudas.

    La forma esta verificada contra un widget reparado en la UI del workspace
    (ver docs/04-dashboards.md). Dos cosas que no son negociables:

      * `version` es **2**. Con 1 el widget renderiza
        "Visualization has no fields selected".
      * cada columna lleva **solo** `fieldName`. Los metadatos por columna
        (`type`, `displayAs`, `booleanValues`, `alignContent`, `order`…) son del
        formato anterior; en la version 2 invalidan la lista completa y el
        widget queda vacio.

    Por eso `columnas` conserva la etiqueta legible aunque no se emita: es el
    encabezado que corresponde a cada columna, y documenta la intencion para
    cuando el esquema permita declararlo. Hoy Lakeview muestra el nombre crudo
    de la columna.
    """
    return {
        "widget": {
            "name": name,
            "queries": [_query(ds, [(c, f"`{c}`") for c, _ in columnas], disagg=True)],
            "spec": _spec("table", 2, {"columns": [{"fieldName": c} for c, _ in columnas]}, titulo),
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
                x=3, y=2),
        counter("kpi_wow", "kpi_actual", "wow_change_pct", "Variacion vs semana previa (%)",
                x=4, y=2),
        counter("kpi_cobertura", "kpi_actual", "allocation_coverage_pct", "Cobertura de atribucion (%)",
                x=5, y=2),
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
        "pages": [
            {
                "name": "ejecutivo",
                "displayName": "Vista ejecutiva",
                "layout": layout,
                "pageType": PAGE_TYPE,
                "layoutVersion": LAYOUT_VERSION,
            }
        ],
        "uiSettings": UI_SETTINGS,
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
        "pages": [
            {
                "name": "costos",
                "displayName": "Costos y chargeback",
                "layout": layout,
                "pageType": PAGE_TYPE,
                "layoutVersion": LAYOUT_VERSION,
            }
        ],
        "uiSettings": UI_SETTINGS,
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
                "Recomendaciones abiertas", x=4, y=2, w=1),
        counter("recursos_afectados", "resumen_ahorro", "recursos_afectados",
                "Recursos involucrados", x=5, y=2, w=1),
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
        "pages": [
            {
                "name": "optimizacion",
                "displayName": "Optimizacion y gobierno",
                "layout": layout,
                "pageType": PAGE_TYPE,
                "layoutVersion": LAYOUT_VERSION,
            }
        ],
        "uiSettings": UI_SETTINGS,
    }


# ---------------------------------------------------------------------------
# Dashboard 4 — Gobierno de etiquetado (en vivo)
# ---------------------------------------------------------------------------
def dashboard_etiquetado() -> dict[str, Any]:
    """Monitoreo del etiquetado, sobre vistas que leen las system tables.

    A diferencia de los otros tres, este tablero NO depende de que el pipeline
    haya corrido: sus datasets consultan `vw_*_live`, que leen
    `system.billing.usage` directamente. Quien corrige el etiquetado cambiando
    una policy ve el efecto al recargar, no al dia siguiente.

    El costo esta a precio de lista (las vistas no aplican descuentos), asi que
    las cifras pueden no cuadrar con las de los otros tableros. El encabezado
    lo advierte de forma visible.
    """
    datasets = [
        dataset(
            "cobertura_hoy", "Cobertura de etiquetado por dimension (ultimos 30 dias)",
            """
SELECT
  dimension,
  ROUND(SUM(attributed_cost_usd), 2) AS costo_atribuido_usd,
  ROUND(SUM(unattributed_cost_usd), 2) AS costo_sin_atribuir_usd,
  ROUND(100.0 * SUM(attributed_cost_usd) / NULLIF(SUM(total_cost_usd), 0), 1) AS cobertura_pct
FROM {{vw_tag_coverage_live}}
WHERE usage_date >= CURRENT_DATE() - INTERVAL 30 DAYS
GROUP BY dimension
ORDER BY cobertura_pct
            """,
        ),
        dataset(
            "cobertura_serie", "Evolucion de la cobertura por dimension",
            """
SELECT
  usage_date,
  dimension,
  ROUND(100.0 * SUM(attributed_cost_usd) / NULLIF(SUM(total_cost_usd), 0), 2) AS cobertura_pct
FROM {{vw_tag_coverage_live}}
WHERE usage_date >= CURRENT_DATE() - INTERVAL 60 DAYS
GROUP BY usage_date, dimension
ORDER BY usage_date
            """,
        ),
        dataset(
            "resumen_global", "Gasto total y gasto sin etiquetar",
            """
SELECT
  ROUND(SUM(list_cost_usd), 2) AS costo_total_usd,
  ROUND(SUM(CASE WHEN SIZE(COALESCE(custom_tags, MAP())) = 0
                 THEN list_cost_usd ELSE 0 END), 2) AS costo_sin_etiquetas_usd,
  COUNT(DISTINCT entity_id) AS recursos
FROM {{vw_usage_live}}
WHERE usage_date >= CURRENT_DATE() - INTERVAL 30 DAYS
            """,
        ),
        dataset(
            "claves_no_reconocidas", "Claves en uso que la configuracion no reconoce",
            """
SELECT
  tag_key AS clave,
  distinct_values AS valores_distintos,
  records AS registros,
  list_cost_usd AS costo_usd
FROM {{vw_tag_inventory_live}}
WHERE NOT is_recognized
ORDER BY list_cost_usd DESC
LIMIT 30
            """,
        ),
        dataset(
            "inventario", "Inventario de claves de etiqueta en uso",
            """
SELECT
  tag_key AS clave,
  COALESCE(maps_to_dimension, 'NO RECONOCIDA') AS dimension,
  distinct_values AS valores_distintos,
  records AS registros,
  list_cost_usd AS costo_usd
FROM {{vw_tag_inventory_live}}
ORDER BY list_cost_usd DESC
LIMIT 50
            """,
        ),
        dataset(
            "sin_etiquetar", "Recursos con costo y sin ninguna dimension resoluble",
            """
SELECT
  entity_type,
  entity_id,
  sku_name,
  billing_origin_product,
  records,
  first_seen,
  last_seen,
  list_cost_usd
FROM {{vw_untagged_spend_live}}
ORDER BY list_cost_usd DESC
LIMIT 100
            """,
        ),
        dataset(
            "sin_etiquetar_por_tipo", "Gasto sin atribuir por tipo de recurso",
            """
SELECT
  entity_type,
  ROUND(SUM(list_cost_usd), 2) AS costo_usd
FROM {{vw_untagged_spend_live}}
GROUP BY entity_type
ORDER BY costo_usd DESC
            """,
        ),
    ]

    layout = [
        markdown(
            "titulo",
            "# FinOps Databricks — Gobierno de etiquetado\n"
            "**Datos en vivo**: este tablero lee las tablas de sistema directamente, "
            "no depende de la ejecucion del pipeline. Los importes son a **precio de "
            "lista** (sin descuentos negociados), asi que pueden diferir de los otros "
            "tableros; sirven para comparar entre si, no como cifra de facturacion.",
            x=0, y=0, w=6, h=2,
        ),
        counter("kpi_costo_total", "resumen_global", "costo_total_usd",
                "Costo total 30 dias (lista)", x=0, y=2, w=2, h=3),
        counter("kpi_sin_etiquetas", "resumen_global", "costo_sin_etiquetas_usd",
                "Sin ninguna etiqueta (USD)", x=2, y=2, w=2, h=3),
        counter("kpi_recursos", "resumen_global", "recursos",
                "Recursos con consumo", x=4, y=2, w=2, h=3),

        chart("cobertura_barras", "cobertura_hoy", "bar", "Cobertura por dimension (30 dias, %)",
              x_campo="dimension", x_escala="categorical", x_titulo="Dimension",
              y_campo="cobertura_pct", y_titulo="% cubierto",
              x=0, y=5, w=3, h=7),
        chart("cobertura_evolucion", "cobertura_serie", "line", "Evolucion de la cobertura (60 dias)",
              x_campo="usage_date", x_escala="temporal", x_titulo="Fecha",
              y_campo="cobertura_pct", y_titulo="% cubierto",
              color_campo="dimension", color_titulo="Dimension",
              x=3, y=5, w=3, h=7),

        markdown("sub_brechas", "## Donde esta la brecha", x=0, y=12, w=6, h=1),

        chart("brecha_por_tipo", "sin_etiquetar_por_tipo", "bar", "Gasto sin atribuir por tipo de recurso",
              x_campo="entity_type", x_escala="categorical", x_titulo="Tipo",
              y_campo="costo_usd", y_titulo="USD",
              x=0, y=13, w=3, h=7),
        table("tabla_no_reconocidas", "claves_no_reconocidas",
              "Claves en uso que la configuracion NO reconoce", [
                  ("clave", "Clave"), ("valores_distintos", "Valores"),
                  ("registros", "Registros"), ("costo_usd", "Costo USD"),
              ], x=3, y=13, w=3, h=7),

        markdown("sub_inventario", "## Inventario de etiquetas", x=0, y=20, w=6, h=1),
        table("tabla_inventario", "inventario", "Todas las claves en uso", [
            ("clave", "Clave"), ("dimension", "Dimension"),
            ("valores_distintos", "Valores"), ("registros", "Registros"),
            ("costo_usd", "Costo USD"),
        ], x=0, y=21, w=6, h=8),

        markdown("sub_recursos", "## Recursos sin atribucion", x=0, y=29, w=6, h=1),
        table("tabla_sin_etiquetar", "sin_etiquetar",
              "Recursos con costo y sin ninguna dimension resoluble", [
                  ("entity_type", "Tipo"), ("entity_id", "Recurso"),
                  ("sku_name", "SKU"), ("billing_origin_product", "Producto"),
                  ("records", "Registros"), ("first_seen", "Desde"),
                  ("last_seen", "Hasta"), ("list_cost_usd", "Costo USD"),
              ], x=0, y=30, w=6, h=8),
    ]

    return {
        "datasets": datasets,
        "pages": [
            {
                "name": "etiquetado",
                "displayName": "Gobierno de etiquetado",
                "layout": layout,
                "pageType": PAGE_TYPE,
                "layoutVersion": LAYOUT_VERSION,
            }
        ],
        "uiSettings": UI_SETTINGS,
    }


DASHBOARDS = {
    "finops_ejecutivo": dashboard_ejecutivo,
    "finops_costos": dashboard_costos,
    "finops_optimizacion": dashboard_optimizacion,
    "finops_etiquetado": dashboard_etiquetado,
}


# ---------------------------------------------------------------------------
# Subcomandos
# ---------------------------------------------------------------------------
def render_env(env: str) -> dict[str, str]:
    """Genera el JSON de cada dashboard con las tablas resueltas para un entorno.

    Devuelve {nombre_archivo: contenido}. Lanza si algun marcador no corresponde
    a una tabla del registro: es la guarda que impide desplegar un dashboard que
    consulte una tabla inexistente.
    """
    sys.path.insert(0, str(REPO_ROOT / "src"))
    from finops.catalog import table_map
    from finops.config import load_config

    cfg = load_config(env, conf_dir=REPO_ROOT / "conf", use_env_vars=False)
    # `table_map` incluye tablas y vistas: los dashboards referencian ambas.
    mapa = table_map(cfg)
    faltantes: set[str] = set()

    def sustituir(match: re.Match[str]) -> str:
        clave = match.group(1)
        if clave not in mapa:
            faltantes.add(clave)
            return match.group(0)
        return mapa[clave]

    salida: dict[str, str] = {}
    for nombre, constructor in DASHBOARDS.items():
        crudo = json.dumps(constructor(), indent=2, ensure_ascii=False) + "\n"
        salida[f"{nombre}.lvdash.json"] = PLACEHOLDER.sub(sustituir, crudo)

    if faltantes:
        raise SystemExit(
            "ERROR: marcadores sin tabla ni vista registrada en finops.catalog: "
            f"{sorted(faltantes)}"
        )
    return salida


def verificar_entornos_coinciden() -> None:
    """Confirma que los tres entornos resuelven a los mismos nombres de tabla.

    Es la condicion que permite versionar UN solo juego de dashboards. Si alguien
    hace que un entorno apunte a otro catalogo o schema, esto falla y explica que
    hay que volver a generar por entorno.
    """
    por_entorno = {env: render_env(env) for env in ENVIRONMENTS}
    referencia = por_entorno[ENVIRONMENTS[0]]
    distintos = [env for env, r in por_entorno.items() if r != referencia]
    if distintos:
        raise SystemExit(
            f"ERROR: los entornos {distintos} resuelven a tablas distintas de "
            f"'{ENVIRONMENTS[0]}'.\n"
            "Un solo juego de dashboards deja de ser valido: habria que generarlos\n"
            "por entorno y apuntar resources/dashboards.yml a\n"
            "dashboards/${bundle.target}/."
        )


def cmd_generate(args: argparse.Namespace) -> int:
    verificar_entornos_coinciden()
    DASHBOARDS_DIR.mkdir(parents=True, exist_ok=True)
    for nombre, contenido in render_env(ENVIRONMENTS[0]).items():
        destino = DASHBOARDS_DIR / nombre
        destino.write_text(contenido, encoding="utf-8")
        print(f"generado {destino.relative_to(REPO_ROOT).as_posix()}")
    return 0


def cmd_check(args: argparse.Namespace) -> int:
    """Verifica que los archivos versionados coincidan con lo que produce este script."""
    verificar_entornos_coinciden()
    desactualizados = [
        nombre
        for nombre, contenido in render_env(ENVIRONMENTS[0]).items()
        if not (DASHBOARDS_DIR / nombre).exists()
        or (DASHBOARDS_DIR / nombre).read_text(encoding="utf-8") != contenido
    ]
    if desactualizados:
        print("Dashboards desactualizados:", file=sys.stderr)
        for nombre in desactualizados:
            print(f"  - dashboards/{nombre}", file=sys.stderr)
        print("\nEjecuta: python scripts/dashboards.py generate", file=sys.stderr)
        return 1
    print(f"Dashboards al dia ({len(DASHBOARDS)} archivos).")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Dashboards Lakeview de FinOps")
    sub = parser.add_subparsers(dest="command", required=True)

    p_gen = sub.add_parser("generate", help="Genera dashboards/*.lvdash.json")
    p_gen.set_defaults(func=cmd_generate)

    p_check = sub.add_parser("check", help="Verifica que los archivos versionados esten al dia")
    p_check.set_defaults(func=cmd_check)

    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    sys.exit(main())
