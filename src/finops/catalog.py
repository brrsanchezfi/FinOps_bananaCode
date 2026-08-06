"""Registro central de tablas del modelo FinOps y bootstrap de Unity Catalog.

Toda referencia a una tabla del modelo debe pasar por aqui. Los nombres logicos
(`BRZ_USAGE`, `GOLD_COST_DAILY`, ...) son la unica fuente de verdad y se usan
tanto en el codigo Python como al generar el SQL de los dashboards.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from .config import FinOpsConfig
from .logging_utils import get_logger

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import SparkSession

log = get_logger("catalog")


@dataclass(frozen=True)
class TableDef:
    """Definicion logica de una tabla del modelo."""

    key: str
    layer: str
    name: str
    description: str
    partition_by: tuple[str, ...] = ()
    zorder_by: tuple[str, ...] = ()

    def fqn(self, cfg: FinOpsConfig) -> str:
        return cfg.table(self.layer, self.name)


# ---------------------------------------------------------------------------
# BRONZE — copia incremental de las system tables
# ---------------------------------------------------------------------------
BRZ_USAGE = TableDef(
    "brz_usage", "bronze", "brz_billing_usage",
    "Copia incremental de system.billing.usage acotada a la ventana de proceso.",
    partition_by=("usage_date",),
)
BRZ_LIST_PRICES = TableDef(
    "brz_list_prices", "bronze", "brz_billing_list_prices",
    "Snapshot de system.billing.list_prices (precios de lista vigentes e historicos).",
)
BRZ_CLUSTERS = TableDef(
    "brz_clusters", "bronze", "brz_compute_clusters",
    "Snapshot de configuracion de clusters (system.compute.clusters).",
)
BRZ_NODE_TYPES = TableDef(
    "brz_node_types", "bronze", "brz_compute_node_types",
    "Catalogo de tipos de nodo disponibles (system.compute.node_types).",
)
BRZ_WAREHOUSES = TableDef(
    "brz_warehouses", "bronze", "brz_compute_warehouses",
    "Snapshot de SQL warehouses (system.compute.warehouses).",
)
BRZ_JOBS = TableDef(
    "brz_jobs", "bronze", "brz_lakeflow_jobs",
    "Snapshot de definiciones de jobs (system.lakeflow.jobs).",
)
BRZ_JOB_RUNS = TableDef(
    "brz_job_runs", "bronze", "brz_lakeflow_job_run_timeline",
    "Linea de tiempo de ejecuciones de jobs.",
    partition_by=("run_date",),
)
BRZ_JOB_TASK_RUNS = TableDef(
    "brz_job_task_runs", "bronze", "brz_lakeflow_job_task_run_timeline",
    "Linea de tiempo de ejecuciones de tareas de jobs.",
    partition_by=("run_date",),
)
BRZ_QUERY_HISTORY = TableDef(
    "brz_query_history", "bronze", "brz_query_history",
    "Historial de consultas SQL (system.query.history).",
    partition_by=("query_date",),
)
BRZ_WORKSPACES = TableDef(
    "brz_workspaces", "bronze", "brz_workspaces",
    "Catalogo de workspaces de la cuenta.",
)

# ---------------------------------------------------------------------------
# SILVER — normalizado y enriquecido
# ---------------------------------------------------------------------------
SLV_USAGE_PRICED = TableDef(
    "slv_usage_priced", "silver", "slv_usage_priced",
    "Consumo valorizado: DBU x precio de lista, con descuentos, etiquetas "
    "normalizadas y entidad resuelta. Grano = registro de system.billing.usage.",
    partition_by=("usage_date",),
    zorder_by=("workspace_id", "sku_group"),
)
SLV_JOB_RUNS = TableDef(
    "slv_job_runs", "silver", "slv_job_runs",
    "Ejecuciones de jobs con duracion, resultado y metadata del job.",
    partition_by=("run_date",),
)
SLV_CLUSTERS = TableDef(
    "slv_clusters", "silver", "slv_clusters",
    "Ultima configuracion conocida de cada cluster con banderas de eficiencia.",
)
SLV_WAREHOUSES = TableDef(
    "slv_warehouses", "silver", "slv_warehouses",
    "Ultima configuracion conocida de cada SQL warehouse.",
)
SLV_QUERIES = TableDef(
    "slv_queries", "silver", "slv_queries",
    "Consultas SQL agregadas por warehouse/usuario/dia.",
    partition_by=("query_date",),
)

# ---------------------------------------------------------------------------
# GOLD — modelo de consumo analitico
# ---------------------------------------------------------------------------
GOLD_DIM_DATE = TableDef("dim_date", "gold", "dim_date", "Dimension calendario.")
GOLD_DIM_SKU = TableDef("dim_sku", "gold", "dim_sku", "Dimension de SKU con agrupacion y familia de producto.")
GOLD_DIM_ENTITY = TableDef(
    "dim_entity", "gold", "dim_entity",
    "Dimension unificada de entidades de consumo (cluster, warehouse, job, pipeline, endpoint).",
)
GOLD_DIM_WORKSPACE = TableDef("dim_workspace", "gold", "dim_workspace", "Dimension de workspace.")

GOLD_COST_DAILY = TableDef(
    "fct_cost_daily", "gold", "fct_cost_daily",
    "Hecho central de costo diario. Grano = fecha x workspace x sku x entidad x "
    "dimensiones de etiquetado.",
    partition_by=("usage_date",),
    zorder_by=("team", "cost_center", "sku_group"),
)
GOLD_COST_MONTHLY = TableDef(
    "agg_cost_monthly", "gold", "agg_cost_monthly",
    "Agregado mensual por todas las dimensiones de atribucion.",
)
GOLD_JOB_RUN_COST = TableDef(
    "fct_job_run_cost", "gold", "fct_job_run_cost",
    "Costo imputado por ejecucion de job, con estado y duracion.",
    partition_by=("run_date",),
)
GOLD_WAREHOUSE_COST = TableDef(
    "fct_warehouse_cost_daily", "gold", "fct_warehouse_cost_daily",
    "Costo diario por SQL warehouse con metricas de actividad de consultas.",
    partition_by=("usage_date",),
)
GOLD_TAG_COVERAGE = TableDef(
    "fct_tag_coverage_daily", "gold", "fct_tag_coverage_daily",
    "Cobertura de etiquetado por dia y dimension: costo atribuido vs no atribuido.",
    partition_by=("usage_date",),
)
GOLD_BUDGET_STATUS = TableDef(
    "fct_budget_status", "gold", "fct_budget_status",
    "Estado de cada presupuesto por periodo: consumido, proyectado, ritmo y semaforo.",
)
GOLD_ANOMALY = TableDef(
    "fct_cost_anomaly", "gold", "fct_cost_anomaly",
    "Anomalias de costo detectadas por serie y dia.",
)
GOLD_FORECAST = TableDef(
    "fct_cost_forecast", "gold", "fct_cost_forecast",
    "Pronostico de costo diario por dimension con intervalo de confianza.",
)
GOLD_RECOMMENDATION = TableDef(
    "fct_recommendation", "gold", "fct_recommendation",
    "Recomendaciones de optimizacion con ahorro estimado y evidencia.",
)
GOLD_CHARGEBACK = TableDef(
    "fct_chargeback_monthly", "gold", "fct_chargeback_monthly",
    "Imputacion mensual de costo por unidad de negocio, incluyendo prorrateo de "
    "costo compartido y no atribuido.",
)
GOLD_KPI_DAILY = TableDef(
    "fct_kpi_daily", "gold", "fct_kpi_daily",
    "KPIs FinOps diarios de la organizacion (costo, variacion, cobertura, eficiencia).",
)

# ---------------------------------------------------------------------------
# OPS — operacion de la plataforma
# ---------------------------------------------------------------------------
# La clave logica coincide siempre con el nombre fisico: es lo que permite
# escribir `{{ops_alert_log}}` en el SQL de un dashboard sin ambiguedad.
OPS_RUN_LOG = TableDef("ops_run_log", "gold", "ops_run_log", "Bitacora de ejecucion del pipeline por etapa.")
OPS_QUALITY = TableDef("ops_data_quality", "gold", "ops_data_quality", "Resultados de los chequeos de calidad.")
OPS_ALERTS = TableDef("ops_alert_log", "gold", "ops_alert_log", "Alertas generadas y su estado de despacho.")
OPS_WATERMARK = TableDef("ops_watermark", "bronze", "ops_watermark", "Marcas de agua de ingesta por fuente.")


ALL_TABLES: tuple[TableDef, ...] = (
    BRZ_USAGE, BRZ_LIST_PRICES, BRZ_CLUSTERS, BRZ_NODE_TYPES, BRZ_WAREHOUSES,
    BRZ_JOBS, BRZ_JOB_RUNS, BRZ_JOB_TASK_RUNS, BRZ_QUERY_HISTORY, BRZ_WORKSPACES,
    SLV_USAGE_PRICED, SLV_JOB_RUNS, SLV_CLUSTERS, SLV_WAREHOUSES, SLV_QUERIES,
    GOLD_DIM_DATE, GOLD_DIM_SKU, GOLD_DIM_ENTITY, GOLD_DIM_WORKSPACE,
    GOLD_COST_DAILY, GOLD_COST_MONTHLY, GOLD_JOB_RUN_COST, GOLD_WAREHOUSE_COST,
    GOLD_TAG_COVERAGE, GOLD_BUDGET_STATUS, GOLD_ANOMALY, GOLD_FORECAST,
    GOLD_RECOMMENDATION, GOLD_CHARGEBACK, GOLD_KPI_DAILY,
    OPS_RUN_LOG, OPS_QUALITY, OPS_ALERTS, OPS_WATERMARK,
)

TABLES_BY_KEY: dict[str, TableDef] = {t.key: t for t in ALL_TABLES}


def table_map(cfg: FinOpsConfig) -> dict[str, str]:
    """Mapa clave logica -> nombre completamente calificado.

    Se usa para sustituir marcadores `{{clave}}` en el SQL de los dashboards.
    """
    return {t.key: t.fqn(cfg) for t in ALL_TABLES}


# ---------------------------------------------------------------------------
# Bootstrap
# ---------------------------------------------------------------------------
def bootstrap(spark: SparkSession, cfg: FinOpsConfig) -> list[str]:
    """Crea catalogo, schemas y aplica comentarios. Idempotente.

    Las tablas se crean al primer write (schema-on-write de Delta); aqui solo se
    garantiza la existencia de los contenedores.
    """
    from .spark_utils import ensure_catalog, ensure_schema

    storage_root = cfg.get("catalog.storage_root")
    managed_location = cfg.get("catalog.managed_location")
    crear_catalogo = bool(cfg.get("catalog.create_if_missing", True))

    # El catalogo se resuelve una sola vez, antes de los schemas: si falla, el
    # mensaje debe ser sobre el catalogo y no repetirse tres veces.
    ensure_catalog(spark, cfg.catalog, managed_location=managed_location, create_if_missing=crear_catalogo)

    creados: list[str] = []
    for capa in ("bronze", "silver", "gold"):
        schema = cfg.schema(capa)
        ensure_schema(
            spark, cfg.catalog, schema, storage_root,
            managed_location=managed_location, create_catalog=crear_catalogo,
        )
        creados.append(f"{cfg.catalog}.{schema}")
        try:
            spark.sql(
                f"COMMENT ON SCHEMA {cfg.catalog}.{schema} IS "
                f"'FinOps {capa} — generado por finops-bananacode'"
            )
        except Exception as exc:  # noqa: BLE001
            log.debug("No se pudo comentar el schema %s: %s", schema, exc)
    log.info("Schemas listos: %s", ", ".join(creados))
    ensure_analytics_tables(spark, cfg)
    return creados


def ensure_analytics_tables(spark: SparkSession, cfg: FinOpsConfig) -> int:
    """Crea vacias las tablas que se escriben desde Python, si no existen.

    Varias solo reciben filas cuando hay resultados, y algunos resultados tardan
    semanas en aparecer: la deteccion de anomalias y el pronostico necesitan
    historia suficiente. Crearlas vacias evita que un dashboard falle con
    TABLE_OR_VIEW_NOT_FOUND durante los primeros dias de operacion.
    """
    from .schemas import tablas_con_esquema
    from .spark_utils import create_table_if_missing

    propiedades = cfg.get("catalog.table_properties", {}) or {}
    creadas = 0
    for tabla, esquema in tablas_con_esquema():
        if create_table_if_missing(
            spark, tabla.fqn(cfg), esquema,
            partition_by=list(tabla.partition_by) or None,
            properties=propiedades, dry_run=cfg.dry_run,
        ):
            creadas += 1
    if creadas:
        log.info("Tablas vacias creadas: %s", creadas)
    return creadas


def apply_comments(spark: SparkSession, cfg: FinOpsConfig) -> int:
    """Aplica el `description` de cada TableDef como COMMENT en Unity Catalog."""
    from .spark_utils import table_exists

    aplicados = 0
    for tabla in ALL_TABLES:
        fqn = tabla.fqn(cfg)
        if not table_exists(spark, fqn):
            continue
        texto = tabla.description.replace("'", "''")
        try:
            spark.sql(f"COMMENT ON TABLE {fqn} IS '{texto}'")
            aplicados += 1
        except Exception as exc:  # noqa: BLE001
            log.debug("No se pudo comentar %s: %s", fqn, exc)
    log.info("Comentarios aplicados a %s tablas", aplicados)
    return aplicados


def maintenance(spark: SparkSession, cfg: FinOpsConfig) -> int:
    """OPTIMIZE + Z-ORDER de las tablas que lo declaran."""
    from .spark_utils import optimize_table, table_exists

    optimizadas = 0
    for tabla in ALL_TABLES:
        if not tabla.zorder_by:
            continue
        fqn = tabla.fqn(cfg)
        if table_exists(spark, fqn):
            optimize_table(spark, fqn, list(tabla.zorder_by))
            optimizadas += 1
    return optimizadas
