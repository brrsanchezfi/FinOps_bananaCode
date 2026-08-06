"""Ingesta de system tables -> bronze.

Principios:

  * **Tolerancia a esquema**: las system tables evolucionan (columnas nuevas,
    renombramientos como system.workflow.* -> system.lakeflow.*). Se seleccionan
    solo las columnas que existen y se registra lo que falta, en lugar de fallar.
  * **Tolerancia a disponibilidad**: las fuentes marcadas `optional: true` en la
    configuracion se omiten con advertencia si el principal no tiene permisos.
  * **Idempotencia**: los hechos con fecha se reescriben por rango; los catalogos
    (precios, clusters, jobs) se sobrescriben como snapshot completo.
"""

from __future__ import annotations

from datetime import date
from typing import TYPE_CHECKING, Any

from ..catalog import (
    BRZ_CLUSTERS,
    BRZ_JOB_RUNS,
    BRZ_JOB_TASK_RUNS,
    BRZ_JOBS,
    BRZ_LIST_PRICES,
    BRZ_NODE_TYPES,
    BRZ_QUERY_HISTORY,
    BRZ_USAGE,
    BRZ_WAREHOUSES,
    BRZ_WORKSPACES,
    TableDef,
)
from ..config import FinOpsConfig
from ..logging_utils import get_logger
from ..spark_utils import overwrite_table, read_source, replace_date_range, with_audit_columns

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import DataFrame, SparkSession

log = get_logger("ingestion")


# ---------------------------------------------------------------------------
# Utilidades de esquema
# ---------------------------------------------------------------------------
def select_existing(df: DataFrame, columns: list[str], *, context: str = "") -> DataFrame:
    """Proyecta solo las columnas presentes, avisando de las ausentes."""
    disponibles = set(df.columns)
    presentes = [c for c in columns if c in disponibles]
    faltantes = [c for c in columns if c not in disponibles]
    if faltantes:
        log.warning("%s: columnas ausentes en el origen, se omiten: %s", context or "fuente", faltantes)
    return df.select(*presentes) if presentes else df


def _properties(cfg: FinOpsConfig) -> dict[str, str]:
    return cfg.get("catalog.table_properties", {}) or {}


def _exclude_workspaces(df: DataFrame, cfg: FinOpsConfig) -> DataFrame:
    from pyspark.sql import functions as F

    excluidos = [str(w) for w in (cfg.get("ingestion.exclude_workspace_ids") or [])]
    if not excluidos or "workspace_id" not in df.columns:
        return df
    return df.filter(~F.col("workspace_id").cast("string").isin(excluidos))


# ---------------------------------------------------------------------------
# Fuentes con fecha (escritura incremental por rango)
# ---------------------------------------------------------------------------
USAGE_COLUMNS = [
    "record_id", "account_id", "workspace_id", "sku_name", "cloud",
    "usage_start_time", "usage_end_time", "usage_date", "custom_tags",
    "usage_unit", "usage_quantity", "usage_metadata", "identity_metadata",
    "record_type", "ingestion_date", "billing_origin_product", "product_features",
    "usage_type",
]


def ingest_usage(
    spark: SparkSession, cfg: FinOpsConfig, run_id: str, min_date: date, max_date: date
) -> int:
    """Ingesta system.billing.usage acotada a la ventana de proceso."""
    from pyspark.sql import functions as F

    origen = read_source(spark, cfg.source("billing_usage"))
    if origen is None:
        return 0

    df = (
        origen.filter(F.col("usage_date").between(min_date, max_date))
        .transform(lambda d: select_existing(d, USAGE_COLUMNS, context="billing.usage"))
        .transform(lambda d: _exclude_workspaces(d, cfg))
    )
    # Los registros de tipo corregido/retroactivo se conservan: forman parte del
    # costo real y el reemplazo por rango los incorpora sin duplicar.
    df = with_audit_columns(df, run_id, "system.billing.usage")
    return replace_date_range(
        spark, df, BRZ_USAGE.fqn(cfg),
        date_column="usage_date", min_date=min_date, max_date=max_date,
        partition_by=list(BRZ_USAGE.partition_by), properties=_properties(cfg), dry_run=cfg.dry_run,
    )


JOB_RUN_COLUMNS = [
    "account_id", "workspace_id", "job_id", "run_id", "period_start_time", "period_end_time",
    "trigger_type", "run_type", "run_name", "compute_ids", "result_state", "termination_code",
    "job_parameters",
]


def ingest_job_runs(
    spark: SparkSession, cfg: FinOpsConfig, run_id: str, min_date: date, max_date: date
) -> int:
    """Ingesta la linea de tiempo de ejecuciones de jobs."""
    from pyspark.sql import functions as F

    origen = read_source(spark, cfg.source("lakeflow_job_run_timeline"))
    if origen is None:
        return 0

    df = (
        origen.withColumn("run_date", F.to_date("period_start_time"))
        .filter(F.col("run_date").between(min_date, max_date))
        .transform(lambda d: select_existing(d, [*JOB_RUN_COLUMNS, "run_date"], context="job_run_timeline"))
        .transform(lambda d: _exclude_workspaces(d, cfg))
    )
    df = with_audit_columns(df, run_id, "system.lakeflow.job_run_timeline")
    return replace_date_range(
        spark, df, BRZ_JOB_RUNS.fqn(cfg),
        date_column="run_date", min_date=min_date, max_date=max_date,
        partition_by=list(BRZ_JOB_RUNS.partition_by), properties=_properties(cfg), dry_run=cfg.dry_run,
    )


TASK_RUN_COLUMNS = [
    "account_id", "workspace_id", "job_id", "run_id", "parent_run_id", "task_key",
    "period_start_time", "period_end_time", "compute_ids", "result_state", "termination_code",
]


def ingest_task_runs(
    spark: SparkSession, cfg: FinOpsConfig, run_id: str, min_date: date, max_date: date
) -> int:
    """Ingesta la linea de tiempo de tareas (grano fino para diagnosticar jobs lentos)."""
    from pyspark.sql import functions as F

    origen = read_source(spark, cfg.source("lakeflow_job_task_run_timeline"))
    if origen is None:
        return 0

    df = (
        origen.withColumn("run_date", F.to_date("period_start_time"))
        .filter(F.col("run_date").between(min_date, max_date))
        .transform(lambda d: select_existing(d, [*TASK_RUN_COLUMNS, "run_date"], context="job_task_run_timeline"))
        .transform(lambda d: _exclude_workspaces(d, cfg))
    )
    df = with_audit_columns(df, run_id, "system.lakeflow.job_task_run_timeline")
    return replace_date_range(
        spark, df, BRZ_JOB_TASK_RUNS.fqn(cfg),
        date_column="run_date", min_date=min_date, max_date=max_date,
        partition_by=list(BRZ_JOB_TASK_RUNS.partition_by), properties=_properties(cfg), dry_run=cfg.dry_run,
    )


QUERY_COLUMNS = [
    "account_id", "workspace_id", "statement_id", "executed_by", "executed_by_user_id",
    "statement_type", "execution_status", "compute", "client_application", "start_time", "end_time",
    "total_duration_ms", "execution_duration_ms", "compilation_duration_ms",
    "waiting_for_compute_duration_ms", "read_bytes", "read_rows", "produced_rows",
    "written_bytes", "spilled_local_bytes",
]


def ingest_query_history(
    spark: SparkSession, cfg: FinOpsConfig, run_id: str, min_date: date, max_date: date
) -> int:
    """Ingesta el historial de consultas SQL (base del analisis de warehouses)."""
    from pyspark.sql import functions as F

    origen = read_source(spark, cfg.source("query_history"))
    if origen is None:
        return 0

    df = (
        origen.withColumn("query_date", F.to_date("start_time"))
        .filter(F.col("query_date").between(min_date, max_date))
        .transform(lambda d: select_existing(d, [*QUERY_COLUMNS, "query_date"], context="query.history"))
        .transform(lambda d: _exclude_workspaces(d, cfg))
    )
    df = with_audit_columns(df, run_id, "system.query.history")
    return replace_date_range(
        spark, df, BRZ_QUERY_HISTORY.fqn(cfg),
        date_column="query_date", min_date=min_date, max_date=max_date,
        partition_by=list(BRZ_QUERY_HISTORY.partition_by), properties=_properties(cfg), dry_run=cfg.dry_run,
    )


# ---------------------------------------------------------------------------
# Fuentes de catalogo (snapshot completo)
# ---------------------------------------------------------------------------
def _ingest_snapshot(
    spark: SparkSession,
    cfg: FinOpsConfig,
    run_id: str,
    source_key: str,
    target: TableDef,
    columns: list[str] | None = None,
) -> int:
    """Sobrescribe una tabla bronze con el snapshot completo de la fuente."""
    definicion = cfg.source(source_key)
    origen = read_source(spark, definicion)
    if origen is None:
        return 0
    df = select_existing(origen, columns, context=source_key) if columns else origen
    df = _exclude_workspaces(df, cfg)
    df = with_audit_columns(df, run_id, definicion["table"])
    return overwrite_table(spark, df, target.fqn(cfg), properties=_properties(cfg), dry_run=cfg.dry_run)


PRICE_COLUMNS = [
    "price_start_time", "price_end_time", "account_id", "sku_name", "cloud",
    "currency_code", "usage_unit", "pricing",
]
CLUSTER_COLUMNS = [
    "account_id", "workspace_id", "cluster_id", "cluster_name", "owned_by", "create_time",
    "delete_time", "driver_node_type", "worker_node_type", "worker_count",
    "min_autoscale_workers", "max_autoscale_workers", "auto_termination_minutes",
    "enable_elastic_disk", "tags", "cluster_source", "init_scripts", "azure_attributes",
    "aws_attributes", "gcp_attributes", "driver_instance_pool_id", "worker_instance_pool_id",
    "dbr_version", "change_time", "change_date", "data_security_mode", "policy_id",
]
WAREHOUSE_COLUMNS = [
    "account_id", "workspace_id", "warehouse_id", "warehouse_name", "warehouse_type",
    "warehouse_channel", "warehouse_size", "min_clusters", "max_clusters", "auto_stop_minutes",
    "tags", "change_time", "delete_time",
]
JOB_COLUMNS = [
    "account_id", "workspace_id", "job_id", "name", "description", "creator_id", "tags",
    "change_time", "delete_time", "run_as",
]
NODE_TYPE_COLUMNS = ["account_id", "node_type", "core_count", "cpu_name", "memory_mb", "gpu_count", "gpu_name"]
WORKSPACE_COLUMNS = ["account_id", "workspace_id", "workspace_name", "workspace_url", "create_time", "status"]


def ingest_list_prices(spark: SparkSession, cfg: FinOpsConfig, run_id: str) -> int:
    return _ingest_snapshot(spark, cfg, run_id, "billing_list_prices", BRZ_LIST_PRICES, PRICE_COLUMNS)


def ingest_clusters(spark: SparkSession, cfg: FinOpsConfig, run_id: str) -> int:
    return _ingest_snapshot(spark, cfg, run_id, "compute_clusters", BRZ_CLUSTERS, CLUSTER_COLUMNS)


def ingest_warehouses(spark: SparkSession, cfg: FinOpsConfig, run_id: str) -> int:
    return _ingest_snapshot(spark, cfg, run_id, "compute_warehouses", BRZ_WAREHOUSES, WAREHOUSE_COLUMNS)


def ingest_jobs(spark: SparkSession, cfg: FinOpsConfig, run_id: str) -> int:
    return _ingest_snapshot(spark, cfg, run_id, "lakeflow_jobs", BRZ_JOBS, JOB_COLUMNS)


def ingest_node_types(spark: SparkSession, cfg: FinOpsConfig, run_id: str) -> int:
    return _ingest_snapshot(spark, cfg, run_id, "compute_node_types", BRZ_NODE_TYPES, NODE_TYPE_COLUMNS)


def ingest_workspaces(spark: SparkSession, cfg: FinOpsConfig, run_id: str) -> int:
    return _ingest_snapshot(spark, cfg, run_id, "access_workspaces_latest", BRZ_WORKSPACES, WORKSPACE_COLUMNS)


# ---------------------------------------------------------------------------
# Orquestacion de la capa bronze
# ---------------------------------------------------------------------------
#: (nombre_etapa, funcion, requiere_ventana)
INGESTORS: tuple[tuple[str, Any, bool], ...] = (
    ("bronze.usage", ingest_usage, True),
    ("bronze.list_prices", ingest_list_prices, False),
    ("bronze.clusters", ingest_clusters, False),
    ("bronze.warehouses", ingest_warehouses, False),
    ("bronze.jobs", ingest_jobs, False),
    ("bronze.node_types", ingest_node_types, False),
    ("bronze.workspaces", ingest_workspaces, False),
    ("bronze.job_runs", ingest_job_runs, True),
    ("bronze.task_runs", ingest_task_runs, True),
    ("bronze.query_history", ingest_query_history, True),
)


def run_bronze(
    spark: SparkSession,
    cfg: FinOpsConfig,
    run_id: str,
    *,
    recorder: Any = None,
    min_date: date | None = None,
    max_date: date | None = None,
) -> dict[str, int]:
    """Ejecuta toda la ingesta bronze. Devuelve filas escritas por etapa."""
    from ..logging_utils import stage

    desde = min_date or cfg.min_date
    hasta = max_date or cfg.max_date
    log.info("Ingesta bronze ventana [%s .. %s]", desde, hasta)

    resultados: dict[str, int] = {}
    for nombre, funcion, usa_ventana in INGESTORS:
        with stage(nombre, recorder) as metrica:
            filas = (
                funcion(spark, cfg, run_id, desde, hasta) if usa_ventana else funcion(spark, cfg, run_id)
            )
            metrica.rows = filas
            if filas == 0:
                metrica.status = "skipped"
            resultados[nombre] = filas
    return resultados
