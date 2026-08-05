"""Capa gold: modelo dimensional listo para dashboards y analitica.

Hecho central: `fct_cost_daily`, con grano

    usage_date x workspace_id x sku_name x entity_key x (dimensiones de etiqueta)

Sobre el se construyen los agregados mensuales, la vista de jobs, la de
warehouses, la cobertura de etiquetado y los KPIs diarios. Las tablas derivadas
de analitica (anomalias, pronostico, presupuestos, recomendaciones, chargeback)
las produce `finops.pipeline` a partir de estas.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import TYPE_CHECKING, Any

from ..catalog import (
    BRZ_WORKSPACES,
    GOLD_COST_DAILY,
    GOLD_COST_MONTHLY,
    GOLD_DIM_DATE,
    GOLD_DIM_ENTITY,
    GOLD_DIM_SKU,
    GOLD_DIM_WORKSPACE,
    GOLD_JOB_RUN_COST,
    GOLD_KPI_DAILY,
    GOLD_TAG_COVERAGE,
    GOLD_WAREHOUSE_COST,
    SLV_CLUSTERS,
    SLV_JOB_RUNS,
    SLV_QUERIES,
    SLV_USAGE_PRICED,
    SLV_WAREHOUSES,
)
from ..config import FinOpsConfig
from ..logging_utils import get_logger, stage
from ..spark_utils import overwrite_table, replace_date_range, table_exists

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import DataFrame, SparkSession

log = get_logger("gold")

#: Metricas de costo que se agregan en todos los niveles.
COST_MEASURES = (
    "list_cost_usd",
    "discount_amount_usd",
    "effective_cost_usd",
    "estimated_infra_cost_usd",
    "total_cost_usd",
)


def _dimensions(cfg: FinOpsConfig) -> list[str]:
    return list(cfg.get("tagging.dimensions", []) or [])


def _sum_measures(alias_prefix: str = "") -> list[Any]:
    from pyspark.sql import functions as F

    return [F.round(F.sum(F.coalesce(F.col(m), F.lit(0.0))), 6).alias(f"{alias_prefix}{m}") for m in COST_MEASURES]


# ---------------------------------------------------------------------------
# Dimensiones
# ---------------------------------------------------------------------------
def build_dim_date(spark: SparkSession, cfg: FinOpsConfig, *, extra_future_days: int = 90) -> DataFrame:
    """Calendario que cubre la historia cargada mas el horizonte de pronostico."""
    from pyspark.sql import functions as F

    limites = spark.table(SLV_USAGE_PRICED.fqn(cfg)).agg(
        F.min("usage_date").alias("min_d"), F.max("usage_date").alias("max_d")
    ).collect()[0]
    inicio: date = limites["min_d"] or (cfg.max_date - timedelta(days=365))
    fin: date = (limites["max_d"] or cfg.max_date) + timedelta(days=extra_future_days)

    return (
        spark.sql(
            f"SELECT explode(sequence(DATE'{inicio.isoformat()}', DATE'{fin.isoformat()}', "
            f"INTERVAL 1 DAY)) AS date_key"
        )
        .withColumn("year", F.year("date_key"))
        .withColumn("quarter", F.quarter("date_key"))
        .withColumn("month", F.month("date_key"))
        .withColumn("day", F.dayofmonth("date_key"))
        .withColumn("year_month", F.date_format("date_key", "yyyy-MM"))
        .withColumn("year_quarter", F.concat_ws("-Q", F.year("date_key"), F.quarter("date_key")))
        .withColumn("week_of_year", F.weekofyear("date_key"))
        .withColumn("day_of_week", F.dayofweek("date_key"))
        .withColumn("day_name", F.date_format("date_key", "EEEE"))
        .withColumn("is_weekend", F.dayofweek("date_key").isin(1, 7))
        .withColumn("month_start", F.trunc("date_key", "month"))
        .withColumn("month_end", F.last_day("date_key"))
        .withColumn("is_future", F.col("date_key") > F.lit(cfg.max_date))
    )


def build_dim_sku(spark: SparkSession, cfg: FinOpsConfig) -> DataFrame:
    """Catalogo de SKU observados con su clasificacion y precio vigente."""
    from pyspark.sql import functions as F

    return (
        spark.table(SLV_USAGE_PRICED.fqn(cfg))
        .groupBy("sku_name", "sku_group", "compute_family", "is_serverless", "is_photon", "usage_unit")
        .agg(
            F.max("unit_price").alias("latest_unit_price_usd"),
            F.min("usage_date").alias("first_seen_date"),
            F.max("usage_date").alias("last_seen_date"),
            F.round(F.sum("total_cost_usd"), 4).alias("lifetime_cost_usd"),
            F.max("billing_origin_product").alias("billing_origin_product"),
        )
    )


def build_dim_workspace(spark: SparkSession, cfg: FinOpsConfig) -> DataFrame:
    """Dimension de workspace, enriquecida con el catalogo de cuenta si existe."""
    from pyspark.sql import functions as F

    base = (
        spark.table(SLV_USAGE_PRICED.fqn(cfg))
        .groupBy("account_id", "workspace_id", "cloud")
        .agg(
            F.min("usage_date").alias("first_seen_date"),
            F.max("usage_date").alias("last_seen_date"),
            F.round(F.sum("total_cost_usd"), 4).alias("lifetime_cost_usd"),
        )
    )
    if table_exists(spark, BRZ_WORKSPACES.fqn(cfg)):
        catalogo = spark.table(BRZ_WORKSPACES.fqn(cfg)).select(
            F.col("workspace_id").alias("_w_id"),
            F.col("workspace_name").alias("workspace_name"),
            F.col("workspace_url").alias("workspace_url"),
            F.col("status").alias("workspace_status"),
        )
        base = base.join(F.broadcast(catalogo), base["workspace_id"] == catalogo["_w_id"], "left").drop("_w_id")
    else:
        base = (
            base.withColumn("workspace_name", F.concat(F.lit("ws-"), F.col("workspace_id")))
            .withColumn("workspace_url", F.lit(None).cast("string"))
            .withColumn("workspace_status", F.lit(None).cast("string"))
        )
    return base


def build_dim_entity(spark: SparkSession, cfg: FinOpsConfig) -> DataFrame:
    """Dimension unificada de entidades de consumo con su configuracion."""
    from pyspark.sql import functions as F

    dims = _dimensions(cfg)
    base = (
        spark.table(SLV_USAGE_PRICED.fqn(cfg))
        .groupBy("entity_key", "entity_type", "entity_id", "workspace_id")
        .agg(
            F.max("entity_name").alias("entity_name"),
            F.max("owner_resolved").alias("owner_resolved"),
            F.min("usage_date").alias("first_seen_date"),
            F.max("usage_date").alias("last_seen_date"),
            F.round(F.sum("total_cost_usd"), 4).alias("lifetime_cost_usd"),
            F.countDistinct("usage_date").alias("active_days"),
            F.max("sku_group").alias("primary_sku_group"),
            F.max("is_serverless").alias("is_serverless"),
            F.max("is_photon").alias("is_photon"),
            *[F.max(d).alias(d) for d in dims],
        )
    )

    if table_exists(spark, SLV_CLUSTERS.fqn(cfg)):
        clusters = spark.table(SLV_CLUSTERS.fqn(cfg)).select(
            F.concat(F.lit("CLUSTER:"), F.col("cluster_id")).alias("_e_key"),
            "autotermination_minutes", "autoscale_enabled", "num_workers", "min_workers",
            "max_workers", "spark_version", "driver_node_type", "worker_node_type",
            "is_single_node", "cluster_source", "policy_id",
        )
        base = base.join(F.broadcast(clusters), base["entity_key"] == clusters["_e_key"], "left").drop("_e_key")

    if table_exists(spark, SLV_WAREHOUSES.fqn(cfg)):
        warehouses = spark.table(SLV_WAREHOUSES.fqn(cfg)).select(
            F.concat(F.lit("WAREHOUSE:"), F.col("warehouse_id")).alias("_w_key"),
            "warehouse_size", "warehouse_type", "warehouse_channel",
            "auto_stop_minutes", "min_clusters", "max_clusters",
        )
        base = base.join(F.broadcast(warehouses), base["entity_key"] == warehouses["_w_key"], "left").drop("_w_key")

    return base.withColumn("days_since_activity", F.datediff(F.lit(cfg.max_date), F.col("last_seen_date")))


# ---------------------------------------------------------------------------
# Hechos
# ---------------------------------------------------------------------------
def build_cost_daily(spark: SparkSession, cfg: FinOpsConfig) -> DataFrame:
    """Hecho de costo diario. Grano: fecha x workspace x sku x entidad x etiquetas."""
    from pyspark.sql import functions as F

    dims = _dimensions(cfg)
    silver = spark.table(SLV_USAGE_PRICED.fqn(cfg)).filter(
        F.col("usage_date").between(cfg.min_date, cfg.max_date)
    )

    claves = [
        "usage_date", "account_id", "workspace_id", "cloud", "sku_name", "sku_group",
        "compute_family", "is_serverless", "is_photon",
        "entity_key", "entity_type", "entity_id", *dims,
    ]
    return (
        silver.groupBy(*claves)
        .agg(
            *_sum_measures(),
            F.round(F.sum(F.coalesce(F.col("usage_quantity"), F.lit(0.0))), 6).alias("usage_quantity"),
            F.max("usage_unit").alias("usage_unit"),
            F.max("entity_name").alias("entity_name"),
            F.max("owner_resolved").alias("owner_resolved"),
            F.count("*").alias("usage_record_count"),
            F.sum(F.when(F.col("price_missing"), 1).otherwise(0)).alias("records_without_price"),
            F.max("is_untagged").alias("is_untagged"),
            F.max("is_fully_tagged").alias("is_fully_tagged"),
        )
        .withColumn("year_month", F.date_format("usage_date", "yyyy-MM"))
    )


def build_cost_monthly(spark: SparkSession, cfg: FinOpsConfig) -> DataFrame:
    """Agregado mensual completo (se reconstruye entero: es barato y evita huecos)."""
    from pyspark.sql import functions as F

    dims = _dimensions(cfg)
    diario = spark.table(GOLD_COST_DAILY.fqn(cfg))
    return (
        diario.groupBy(
            "year_month", "account_id", "workspace_id", "sku_group", "compute_family", "entity_type", *dims
        )
        .agg(
            *_sum_measures(),
            F.round(F.sum("usage_quantity"), 6).alias("usage_quantity"),
            F.countDistinct("entity_key").alias("entity_count"),
            F.countDistinct("usage_date").alias("active_days"),
        )
        .withColumn("month_start", F.to_date(F.concat(F.col("year_month"), F.lit("-01"))))
    )


def build_job_run_cost(spark: SparkSession, cfg: FinOpsConfig) -> DataFrame:
    """Costo imputado por ejecucion de job.

    El costo se atribuye por `job_run_id` cuando `usage_metadata` lo expone. Las
    ejecuciones sin costo asociado (por ejemplo las que corrieron en compute
    compartido) quedan con costo 0 y `has_cost = false`, para no inventar cifras.
    """
    from pyspark.sql import functions as F

    costo = (
        spark.table(SLV_USAGE_PRICED.fqn(cfg))
        .filter(F.col("usage_date").between(cfg.min_date, cfg.max_date))
        .filter(F.col("job_run_id").isNotNull())
        .groupBy("workspace_id", "job_id", "job_run_id")
        .agg(
            F.round(F.sum("total_cost_usd"), 6).alias("run_cost_usd"),
            F.round(F.sum("usage_quantity"), 6).alias("run_dbus"),
            F.max("sku_group").alias("sku_group"),
            F.max("is_serverless").alias("is_serverless"),
        )
    )

    runs = spark.table(SLV_JOB_RUNS.fqn(cfg)).filter(F.col("run_date").between(cfg.min_date, cfg.max_date))
    dims = _dimensions(cfg)
    etiquetas = (
        spark.table(SLV_USAGE_PRICED.fqn(cfg))
        .filter(F.col("job_id").isNotNull())
        .groupBy("workspace_id", "job_id")
        .agg(*[F.max(d).alias(d) for d in dims])
    )

    unido = (
        runs.join(
            costo,
            (runs["workspace_id"] == costo["workspace_id"])
            & (runs["run_id"].cast("string") == costo["job_run_id"]),
            "left",
        )
        .drop(costo["workspace_id"])
        .drop(costo["job_id"])
    )
    unido = unido.join(
        F.broadcast(etiquetas),
        (unido["workspace_id"] == etiquetas["workspace_id"]) & (unido["job_id"] == etiquetas["job_id"]),
        "left",
    ).drop(etiquetas["workspace_id"]).drop(etiquetas["job_id"])

    return (
        unido.withColumn("run_cost_usd", F.coalesce(F.col("run_cost_usd"), F.lit(0.0)))
        .withColumn("has_cost", F.col("run_dbus").isNotNull())
        .withColumn(
            "cost_per_minute_usd",
            F.when(F.col("duration_minutes") > 0, F.round(F.col("run_cost_usd") / F.col("duration_minutes"), 6)),
        )
        .withColumn("entity_key", F.concat(F.lit("JOB:"), F.col("job_id").cast("string")))
    )


def build_warehouse_cost_daily(spark: SparkSession, cfg: FinOpsConfig) -> DataFrame:
    """Costo diario por SQL warehouse cruzado con la actividad de consultas."""
    from pyspark.sql import functions as F

    dims = _dimensions(cfg)
    costo = (
        spark.table(SLV_USAGE_PRICED.fqn(cfg))
        .filter(F.col("usage_date").between(cfg.min_date, cfg.max_date))
        .filter(F.col("warehouse_id").isNotNull())
        .groupBy("usage_date", "workspace_id", "warehouse_id", *dims)
        .agg(
            *_sum_measures(),
            F.round(F.sum("usage_quantity"), 6).alias("usage_quantity"),
            F.max("sku_group").alias("sku_group"),
            F.max("is_serverless").alias("is_serverless"),
        )
    )

    if table_exists(spark, SLV_QUERIES.fqn(cfg)):
        actividad = (
            spark.table(SLV_QUERIES.fqn(cfg))
            .groupBy("query_date", "workspace_id", "warehouse_id")
            .agg(
                F.sum("query_count").alias("query_count"),
                F.sum("failed_query_count").alias("failed_query_count"),
                F.sum("active_hours").alias("active_hours"),
                F.avg("avg_duration_ms").alias("avg_duration_ms"),
                F.countDistinct("executed_by").alias("distinct_users"),
                F.sum("read_bytes").alias("read_bytes"),
            )
        )
        costo = costo.join(
            actividad,
            (costo["usage_date"] == actividad["query_date"])
            & (costo["workspace_id"] == actividad["workspace_id"])
            & (costo["warehouse_id"] == actividad["warehouse_id"]),
            "left",
        ).drop(actividad["workspace_id"]).drop(actividad["warehouse_id"]).drop("query_date")
    else:
        for columna in ("query_count", "failed_query_count", "active_hours", "distinct_users", "read_bytes"):
            costo = costo.withColumn(columna, F.lit(None).cast("double"))
        costo = costo.withColumn("avg_duration_ms", F.lit(None).cast("double"))

    if table_exists(spark, SLV_WAREHOUSES.fqn(cfg)):
        catalogo = spark.table(SLV_WAREHOUSES.fqn(cfg)).select(
            F.col("warehouse_id").alias("_w_id"), "warehouse_name", "warehouse_size",
            "warehouse_type", "auto_stop_minutes", "min_clusters", "max_clusters",
        )
        costo = costo.join(F.broadcast(catalogo), costo["warehouse_id"] == catalogo["_w_id"], "left").drop("_w_id")

    return costo.withColumn(
        "cost_per_query_usd",
        F.when(F.col("query_count") > 0, F.round(F.col("total_cost_usd") / F.col("query_count"), 6)),
    ).withColumn("entity_key", F.concat(F.lit("WAREHOUSE:"), F.col("warehouse_id")))


def build_tag_coverage_daily(spark: SparkSession, cfg: FinOpsConfig) -> DataFrame:
    """Cobertura de etiquetado diaria por dimension, ponderada por costo."""
    from functools import reduce

    from pyspark.sql import functions as F

    dims = _dimensions(cfg)
    sin_asignar = str(cfg.get("tagging.unallocated_value", "SIN_ASIGNAR"))
    diario = spark.table(GOLD_COST_DAILY.fqn(cfg)).filter(
        F.col("usage_date").between(cfg.min_date, cfg.max_date)
    )

    partes = []
    for dimension in dims:
        partes.append(
            diario.groupBy("usage_date", "workspace_id")
            .agg(
                F.lit(dimension).alias("dimension"),
                F.round(F.sum("total_cost_usd"), 6).alias("total_cost_usd"),
                F.round(
                    F.sum(F.when(F.col(dimension) != sin_asignar, F.col("total_cost_usd")).otherwise(0.0)), 6
                ).alias("attributed_cost_usd"),
                F.countDistinct(F.when(F.col(dimension) != sin_asignar, F.col("entity_key"))).alias(
                    "attributed_entities"
                ),
                F.countDistinct("entity_key").alias("total_entities"),
            )
            .select("usage_date", "workspace_id", "dimension", "total_cost_usd", "attributed_cost_usd",
                    "attributed_entities", "total_entities")
        )

    if not partes:
        return spark.createDataFrame([], schema="usage_date date, workspace_id string, dimension string")

    union = reduce(lambda a, b: a.unionByName(b), partes)
    return (
        union.withColumn(
            "unattributed_cost_usd", F.round(F.col("total_cost_usd") - F.col("attributed_cost_usd"), 6)
        )
        .withColumn(
            "coverage_ratio",
            F.when(F.col("total_cost_usd") > 0, F.round(F.col("attributed_cost_usd") / F.col("total_cost_usd"), 6))
            .otherwise(F.lit(1.0)),
        )
    )


def build_kpi_daily(spark: SparkSession, cfg: FinOpsConfig) -> DataFrame:
    """KPIs diarios de la organizacion: costo, variacion, mezcla y eficiencia."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    diario = spark.table(GOLD_COST_DAILY.fqn(cfg))
    sin_asignar = str(cfg.get("tagging.unallocated_value", "SIN_ASIGNAR"))

    base = diario.groupBy("usage_date").agg(
        F.round(F.sum("total_cost_usd"), 4).alias("total_cost_usd"),
        F.round(F.sum("effective_cost_usd"), 4).alias("effective_cost_usd"),
        F.round(F.sum("estimated_infra_cost_usd"), 4).alias("estimated_infra_cost_usd"),
        F.round(F.sum("usage_quantity"), 4).alias("total_dbus"),
        F.countDistinct("entity_key").alias("active_entities"),
        F.countDistinct("workspace_id").alias("active_workspaces"),
        F.round(F.sum(F.when(F.col("sku_group") == "ALL_PURPOSE", F.col("total_cost_usd")).otherwise(0.0)), 4).alias(
            "all_purpose_cost_usd"
        ),
        F.round(F.sum(F.when(F.col("sku_group").startswith("SERVERLESS"), F.col("total_cost_usd")).otherwise(0.0)), 4)
        .alias("serverless_cost_usd"),
        F.round(F.sum(F.when(F.col("is_photon"), F.col("total_cost_usd")).otherwise(0.0)), 4).alias(
            "photon_cost_usd"
        ),
        F.round(
            F.sum(F.when(F.col("cost_center") == sin_asignar, F.col("total_cost_usd")).otherwise(0.0)), 4
        ).alias("unallocated_cost_usd")
        if "cost_center" in diario.columns
        else F.lit(0.0).alias("unallocated_cost_usd"),
    )

    ventana_orden = Window.orderBy("usage_date")
    ventana_7 = ventana_orden.rowsBetween(-6, 0)
    ventana_28 = ventana_orden.rowsBetween(-27, 0)

    return (
        base.withColumn("cost_prev_day_usd", F.lag("total_cost_usd", 1).over(ventana_orden))
        .withColumn("cost_prev_week_usd", F.lag("total_cost_usd", 7).over(ventana_orden))
        .withColumn("rolling_7d_avg_usd", F.round(F.avg("total_cost_usd").over(ventana_7), 4))
        .withColumn("rolling_28d_avg_usd", F.round(F.avg("total_cost_usd").over(ventana_28), 4))
        .withColumn("mtd_cost_usd", F.round(F.sum("total_cost_usd").over(
            Window.partitionBy(F.date_format("usage_date", "yyyy-MM")).orderBy("usage_date")
        ), 4))
        .withColumn(
            "dod_change_pct",
            F.when(F.col("cost_prev_day_usd") > 0,
                   F.round((F.col("total_cost_usd") - F.col("cost_prev_day_usd")) / F.col("cost_prev_day_usd"), 6)),
        )
        .withColumn(
            "wow_change_pct",
            F.when(F.col("cost_prev_week_usd") > 0,
                   F.round((F.col("total_cost_usd") - F.col("cost_prev_week_usd")) / F.col("cost_prev_week_usd"), 6)),
        )
        .withColumn(
            "cost_per_dbu_usd",
            F.when(F.col("total_dbus") > 0, F.round(F.col("total_cost_usd") / F.col("total_dbus"), 6)),
        )
        .withColumn(
            "allocation_coverage_ratio",
            F.when(
                F.col("total_cost_usd") > 0,
                F.round(1.0 - (F.col("unallocated_cost_usd") / F.col("total_cost_usd")), 6),
            ).otherwise(F.lit(1.0)),
        )
        .withColumn(
            "serverless_share",
            F.when(F.col("total_cost_usd") > 0, F.round(F.col("serverless_cost_usd") / F.col("total_cost_usd"), 6)),
        )
        .withColumn(
            "all_purpose_share",
            F.when(F.col("total_cost_usd") > 0, F.round(F.col("all_purpose_cost_usd") / F.col("total_cost_usd"), 6)),
        )
    )


# ---------------------------------------------------------------------------
# Perfiles de entidad para el motor de optimizacion
# ---------------------------------------------------------------------------
def build_entity_profiles(spark: SparkSession, cfg: FinOpsConfig, *, lookback_days: int = 30) -> DataFrame:
    """Construye el insumo del motor de recomendaciones.

    Una fila por entidad con su costo en la ventana, su configuracion y las
    metricas de actividad (ejecuciones, fallos, duracion, consultas).
    """
    from pyspark.sql import functions as F

    desde = cfg.max_date - timedelta(days=lookback_days)
    dims = _dimensions(cfg)

    costo = (
        spark.table(GOLD_COST_DAILY.fqn(cfg))
        .filter(F.col("usage_date").between(desde, cfg.max_date))
        .groupBy("entity_key")
        .agg(
            F.round(F.sum("total_cost_usd"), 4).alias("cost_usd"),
            F.max("entity_type").alias("entity_type"),
            F.max("entity_id").alias("entity_id"),
            F.max("entity_name").alias("entity_name"),
            F.max("workspace_id").alias("workspace_id"),
            F.max("sku_group").alias("sku_group"),
            F.max("is_serverless").alias("is_serverless"),
            F.max("is_photon").alias("is_photon"),
            F.max("owner_resolved").alias("owner"),
            F.max("usage_date").alias("last_activity_date"),
            F.round(
                F.sum(F.when(F.col("is_untagged"), F.col("total_cost_usd")).otherwise(0.0)), 4
            ).alias("untagged_cost_usd"),
            *[F.max(d).alias(d) for d in dims],
        )
    )

    entidad = spark.table(GOLD_DIM_ENTITY.fqn(cfg))
    columnas_config = [
        c for c in (
            "autotermination_minutes", "autoscale_enabled", "num_workers", "min_workers", "max_workers",
            "spark_version", "is_single_node", "warehouse_size", "warehouse_type", "auto_stop_minutes",
        )
        if c in entidad.columns
    ]
    if columnas_config:
        config = entidad.select(F.col("entity_key").alias("_e_key"), *columnas_config)
        costo = costo.join(F.broadcast(config), costo["entity_key"] == config["_e_key"], "left").drop("_e_key")

    if table_exists(spark, GOLD_JOB_RUN_COST.fqn(cfg)):
        jobs = (
            spark.table(GOLD_JOB_RUN_COST.fqn(cfg))
            .filter(F.col("run_date").between(desde, cfg.max_date))
            .groupBy("entity_key")
            .agg(
                F.count("*").alias("run_count"),
                F.sum(F.when(F.col("is_failed"), 1).otherwise(0)).alias("failed_run_count"),
                F.round(F.avg("duration_minutes"), 4).alias("avg_duration_minutes"),
                F.max("run_date").alias("last_run_date"),
            )
            .withColumnRenamed("entity_key", "_j_key")
        )
        costo = costo.join(jobs, costo["entity_key"] == jobs["_j_key"], "left").drop("_j_key")

    if table_exists(spark, GOLD_WAREHOUSE_COST.fqn(cfg)):
        warehouses = (
            spark.table(GOLD_WAREHOUSE_COST.fqn(cfg))
            .filter(F.col("usage_date").between(desde, cfg.max_date))
            .groupBy("entity_key")
            .agg(
                F.sum("query_count").alias("query_count"),
                F.sum("active_hours").alias("active_hours"),
            )
            .withColumnRenamed("entity_key", "_w_key")
        )
        costo = costo.join(warehouses, costo["entity_key"] == warehouses["_w_key"], "left").drop("_w_key")

    return costo.withColumn(
        "days_since_activity", F.datediff(F.lit(cfg.max_date), F.col("last_activity_date"))
    ).withColumn("analysis_days", F.lit(lookback_days))


# ---------------------------------------------------------------------------
# Orquestacion de la capa gold
# ---------------------------------------------------------------------------
def run_gold(spark: SparkSession, cfg: FinOpsConfig, run_id: str, *, recorder: Any = None) -> dict[str, int]:
    """Construye toda la capa gold en el orden de dependencias correcto."""
    propiedades = cfg.get("catalog.table_properties", {}) or {}
    resultados: dict[str, int] = {}

    # 1) Hecho central (incremental por rango)
    with stage("gold.cost_daily", recorder) as metrica:
        metrica.rows = replace_date_range(
            spark, build_cost_daily(spark, cfg), GOLD_COST_DAILY.fqn(cfg),
            date_column="usage_date", min_date=cfg.min_date, max_date=cfg.max_date,
            partition_by=list(GOLD_COST_DAILY.partition_by), properties=propiedades, dry_run=cfg.dry_run,
        )
        resultados["gold.cost_daily"] = metrica.rows

    # 2) Hechos derivados con ventana
    derivados = (
        ("gold.job_run_cost", GOLD_JOB_RUN_COST, build_job_run_cost, "run_date", SLV_JOB_RUNS),
        ("gold.warehouse_cost", GOLD_WAREHOUSE_COST, build_warehouse_cost_daily, "usage_date", None),
        ("gold.tag_coverage", GOLD_TAG_COVERAGE, build_tag_coverage_daily, "usage_date", None),
    )
    for nombre, destino, constructor, columna_fecha, requerida in derivados:
        with stage(nombre, recorder) as metrica:
            if requerida is not None and not table_exists(spark, requerida.fqn(cfg)):
                metrica.status = "skipped"
                metrica.details["motivo"] = f"{requerida.fqn(cfg)} no disponible"
                resultados[nombre] = 0
                continue
            metrica.rows = replace_date_range(
                spark, constructor(spark, cfg), destino.fqn(cfg),
                date_column=columna_fecha, min_date=cfg.min_date, max_date=cfg.max_date,
                partition_by=list(destino.partition_by), properties=propiedades, dry_run=cfg.dry_run,
            )
            resultados[nombre] = metrica.rows

    # 3) Dimensiones y agregados (snapshot completo)
    snapshots = (
        ("gold.dim_date", GOLD_DIM_DATE, build_dim_date),
        ("gold.dim_sku", GOLD_DIM_SKU, build_dim_sku),
        ("gold.dim_workspace", GOLD_DIM_WORKSPACE, build_dim_workspace),
        ("gold.dim_entity", GOLD_DIM_ENTITY, build_dim_entity),
        ("gold.cost_monthly", GOLD_COST_MONTHLY, build_cost_monthly),
        ("gold.kpi_daily", GOLD_KPI_DAILY, build_kpi_daily),
    )
    for nombre, destino, constructor in snapshots:
        with stage(nombre, recorder) as metrica:
            metrica.rows = overwrite_table(
                spark, constructor(spark, cfg), destino.fqn(cfg),
                properties=propiedades, dry_run=cfg.dry_run,
            )
            resultados[nombre] = metrica.rows

    return resultados
