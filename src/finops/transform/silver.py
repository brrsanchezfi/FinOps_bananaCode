"""Capa silver: consumo valorizado, entidades resueltas y etiquetas normalizadas.

La tabla central es `slv_usage_priced`, con el mismo grano que
`system.billing.usage` (un registro por intervalo de consumo) enriquecido con:

  * precio de lista vigente al momento del consumo -> costo en USD
  * descuento aplicado segun las reglas de configuracion
  * grupo de SKU, banderas serverless / photon
  * entidad de consumo resuelta (job, cluster, warehouse, pipeline, endpoint)
  * dimensiones de atribucion resueltas por cadena de precedencia de etiquetas

La semantica de clasificacion y valorizacion es exactamente la de
`finops.transform.pricing`; aqui se expresa en operaciones de Spark para poder
aplicarla sobre el volumen completo, y las pruebas comparan ambos caminos.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from ..catalog import (
    BRZ_CLUSTERS,
    BRZ_JOB_RUNS,
    BRZ_JOBS,
    BRZ_LIST_PRICES,
    BRZ_QUERY_HISTORY,
    BRZ_USAGE,
    BRZ_WAREHOUSES,
    SLV_CLUSTERS,
    SLV_JOB_RUNS,
    SLV_QUERIES,
    SLV_USAGE_PRICED,
    SLV_WAREHOUSES,
)
from ..config import FinOpsConfig
from ..logging_utils import get_logger, stage
from ..spark_utils import overwrite_table, replace_date_range, table_exists
from . import pricing as P
from .tags import DEFAULT_SOURCE_ORDER, build_alias_index, normalize_key

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import Column, DataFrame, SparkSession

log = get_logger("silver")

_NORM_REGEX = "[^a-z0-9]"


# ---------------------------------------------------------------------------
# Helpers de columnas
# ---------------------------------------------------------------------------
def _norm_col(col: Column) -> Column:
    """Normaliza un texto igual que `tags.normalize_key` pero en Spark."""
    from pyspark.sql import functions as F

    return F.regexp_replace(F.lower(F.trim(col.cast("string"))), _NORM_REGEX, "")


def _blank_to_null(col: Column) -> Column:
    from pyspark.sql import functions as F

    limpio = F.trim(col.cast("string"))
    return F.when(
        limpio.isNull() | (limpio == "") | F.lower(limpio).isin("null", "none", "n/a", "na", "-", "undefined"),
        F.lit(None).cast("string"),
    ).otherwise(limpio)


def _normalized_tag_map(col: Column) -> Column:
    """Devuelve el mapa de etiquetas con las claves normalizadas.

    Permite resolver 'Cost-Center', 'cost_center' y 'COSTCENTER' con una unica
    busqueda por clave, sin explotar el mapa ni hacer joins adicionales.
    """
    from pyspark.sql import functions as F

    return F.when(
        col.isNull(), F.create_map().cast("map<string,string>")
    ).otherwise(
        F.map_from_entries(
            F.transform(
                F.map_entries(col),
                lambda e: F.struct(
                    F.regexp_replace(F.lower(F.trim(e["key"])), _NORM_REGEX, "").alias("key"),
                    e["value"].cast("string").alias("value"),
                ),
            )
        )
    )


def _struct_field(df: DataFrame, struct_name: str, field: str) -> Column:
    """Accede a un campo de struct devolviendo NULL si el campo no existe."""
    from pyspark.sql import functions as F
    from pyspark.sql.types import StructType

    if struct_name not in df.columns:
        return F.lit(None).cast("string")
    tipo = df.schema[struct_name].dataType
    if isinstance(tipo, StructType) and field in tipo.fieldNames():
        return F.col(f"{struct_name}.{field}").cast("string")
    return F.lit(None).cast("string")


def _map_literal(mapping: dict[str, Any]) -> Column:
    """Construye un literal map<string,string> a partir de un dict de Python."""
    from pyspark.sql import functions as F

    if not mapping:
        return F.create_map().cast("map<string,string>")
    pares: list[Column] = []
    for clave, valor in mapping.items():
        pares.extend([F.lit(str(clave)), F.lit(str(valor))])
    return F.create_map(*pares)


# ---------------------------------------------------------------------------
# Clasificacion de SKU en Spark (espejo de pricing.classify_sku)
# ---------------------------------------------------------------------------
def sku_group_expr(sku_col: str = "sku_name", product_col: str = "billing_origin_product") -> Column:
    from pyspark.sql import functions as F

    sku = F.upper(F.coalesce(F.col(sku_col).cast("string"), F.lit("")))
    producto = F.upper(F.trim(F.coalesce(F.col(product_col).cast("string"), F.lit(""))))
    serverless = sku.rlike("(?i)SERVERLESS") | producto.rlike("(?i)SERVERLESS")

    # 1) billing_origin_product (campo controlado)
    expr = F.lit(None).cast("string")
    for producto_valor, grupo in P.PRODUCT_TO_GROUP.items():
        if grupo in {"SQL", "JOBS", "DLT"}:
            destino = F.when(serverless, F.lit(f"SERVERLESS_{grupo}")).otherwise(F.lit(grupo))
        else:
            destino = F.lit(grupo)
        expr = F.when(producto == F.lit(producto_valor), destino).otherwise(expr)

    # 2) patrones sobre sku_name, en el mismo orden que la version Python
    patron_expr = F.lit(None).cast("string")
    for patron, grupo in reversed(P.SKU_PATTERN_SPECS):
        patron_expr = F.when(sku.rlike(f"(?i){patron}"), F.lit(grupo)).otherwise(patron_expr)

    # 3) respaldo
    respaldo = F.when(serverless, F.lit("SERVERLESS_JOBS")).otherwise(F.lit("OTHER"))
    return F.coalesce(expr, patron_expr, respaldo)


def discount_expr(discount_rules: list[dict[str, Any]] | None) -> tuple[Column, Column]:
    """Construye (descuento, nombre_regla) evaluando las reglas en orden."""
    from pyspark.sql import functions as F

    contexto = {
        "workspace_id": F.col("workspace_id").cast("string"),
        "account_id": F.col("account_id").cast("string"),
        "sku_name": F.upper(F.col("sku_name").cast("string")),
        "sku_group": F.col("sku_group"),
        "billing_origin_product": F.upper(F.coalesce(F.col("billing_origin_product").cast("string"), F.lit(""))),
        "cloud": F.upper(F.coalesce(F.col("cloud").cast("string"), F.lit(""))),
    }

    descuento = F.lit(0.0)
    nombre = F.lit("sin_descuento")
    # Se construye de la ultima a la primera para que la primera regla que
    # coincida sea la que quede en el `when` mas externo.
    for regla in reversed(discount_rules or []):
        if not isinstance(regla, dict):
            continue
        condicion = F.lit(True)
        for clave, esperado in (regla.get("match") or {}).items():
            columna = contexto.get(clave)
            if columna is None:
                condicion = F.lit(False)
                break
            candidatos = esperado if isinstance(esperado, (list, tuple)) else [esperado]
            sub = F.lit(False)
            for candidato in candidatos:
                sub = sub | columna.like(str(candidato).upper().replace("*", "%"))
            condicion = condicion & sub
        pct = max(0.0, min(float(regla.get("discount_pct", 0.0) or 0.0), 0.999))
        descuento = F.when(condicion, F.lit(pct)).otherwise(descuento)
        nombre = F.when(condicion, F.lit(str(regla.get("name", "sin_nombre")))).otherwise(nombre)
    return descuento, nombre


# ---------------------------------------------------------------------------
# Etiquetas
# ---------------------------------------------------------------------------
def _aliases_by_dimension(cfg: FinOpsConfig) -> dict[str, list[str]]:
    """Alias normalizados por dimension, respetando el indice de precedencia."""
    aliases = cfg.get("tagging.aliases", {}) or {}
    indice = build_alias_index(aliases)
    salida: dict[str, list[str]] = {}
    for clave_norm, dimension in indice.items():
        salida.setdefault(dimension, []).append(clave_norm)
    return salida


def resolve_tag_columns(df: DataFrame, cfg: FinOpsConfig, tag_map_columns: dict[str, str]) -> DataFrame:
    """Resuelve las dimensiones canonicas sobre un DataFrame.

    Args:
        tag_map_columns: nombre_fuente -> nombre de la columna map ya normalizada.
            El orden de `DEFAULT_SOURCE_ORDER` define la precedencia.
    """
    from pyspark.sql import functions as F

    dimensiones = list(cfg.get("tagging.dimensions", []) or [])
    alias_por_dim = _aliases_by_dimension(cfg)
    value_map = cfg.get("tagging.value_map", {}) or {}
    sin_asignar = str(cfg.get("tagging.unallocated_value", "SIN_ASIGNAR"))
    defaults_ws = cfg.get("tagging.workspace_defaults", {}) or {}

    orden = [s for s in DEFAULT_SOURCE_ORDER if s in tag_map_columns]
    orden += [s for s in tag_map_columns if s not in orden]

    salida = df
    for dimension in dimensiones:
        alias = alias_por_dim.get(dimension, [normalize_key(dimension)])

        candidatos: list[Column] = []
        fuentes: list[tuple[str, Column]] = []
        for nombre_fuente in orden:
            columna_map = tag_map_columns[nombre_fuente]
            if columna_map not in salida.columns:
                continue
            for clave in alias:
                valor = _blank_to_null(F.element_at(F.col(columna_map), F.lit(clave)))
                candidatos.append(valor)
                fuentes.append((nombre_fuente, valor))

        crudo = F.coalesce(*candidatos) if candidatos else F.lit(None).cast("string")

        # Origen de la resolucion (para auditoria de gobierno).
        origen = F.lit("default")
        for nombre_fuente, valor in reversed(fuentes):
            origen = F.when(valor.isNotNull(), F.lit(nombre_fuente)).otherwise(origen)

        # Canonizacion de valores y valor por defecto del workspace.
        mapa_valores = {normalize_key(k): v for k, v in (value_map.get(dimension) or {}).items()}
        canonico = F.coalesce(F.element_at(_map_literal(mapa_valores), _norm_col(crudo)), crudo)

        defaults_dim = {
            str(ws): str(valores.get(dimension))
            for ws, valores in defaults_ws.items()
            if isinstance(valores, dict) and valores.get(dimension)
        }
        por_workspace = F.element_at(_map_literal(defaults_dim), F.col("workspace_id").cast("string"))

        salida = salida.withColumn(dimension, F.coalesce(canonico, por_workspace, F.lit(sin_asignar)))
        salida = salida.withColumn(
            f"tag_source_{dimension}",
            F.when(canonico.isNotNull(), origen)
            .when(por_workspace.isNotNull(), F.lit("workspace_defaults"))
            .otherwise(F.lit("default")),
        )

    resueltas = sum(
        (F.when(F.col(d) != F.lit(sin_asignar), F.lit(1)).otherwise(F.lit(0)) for d in dimensiones),
        F.lit(0),
    )
    salida = salida.withColumn("tags_resolved", resueltas)
    salida = salida.withColumn("tags_expected", F.lit(len(dimensiones)))
    salida = salida.withColumn("is_fully_tagged", F.col("tags_resolved") == F.lit(len(dimensiones)))
    salida = salida.withColumn("is_untagged", F.col("tags_resolved") == F.lit(0))
    return salida


# ---------------------------------------------------------------------------
# Precios
# ---------------------------------------------------------------------------
def _unit_price_column(prices: DataFrame) -> Column:
    """Extrae el precio unitario del struct `pricing`, con respaldos."""
    from pyspark.sql import functions as F
    from pyspark.sql.types import StructType

    if "pricing" not in prices.columns:
        return F.lit(None).cast("double")
    tipo = prices.schema["pricing"].dataType
    if not isinstance(tipo, StructType):
        return F.col("pricing").cast("double")

    campos = tipo.fieldNames()
    candidatos: list[Column] = []
    # `effective_list.default` refleja el precio realmente aplicable cuando hay
    # promociones vigentes; `default` es el precio de lista puro.
    if "effective_list" in campos:
        sub = tipo["effective_list"].dataType
        if isinstance(sub, StructType) and "default" in sub.fieldNames():
            candidatos.append(F.col("pricing.effective_list.default").cast("double"))
    if "default" in campos:
        candidatos.append(F.col("pricing.default").cast("double"))
    if "promotional" in campos:
        sub = tipo["promotional"].dataType
        if isinstance(sub, StructType) and "default" in sub.fieldNames():
            candidatos.append(F.col("pricing.promotional.default").cast("double"))
    return F.coalesce(*candidatos) if candidatos else F.lit(None).cast("double")


def join_prices(usage: DataFrame, prices: DataFrame) -> DataFrame:
    """Une consumo con el precio de lista vigente en el momento del consumo.

    La vigencia se evalua contra `usage_end_time`: un cambio de tarifa a mitad
    de dia debe aplicarse a los intervalos posteriores, no a todo el dia.
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    precios = prices.select(
        F.col("sku_name").alias("p_sku_name"),
        F.upper(F.coalesce(F.col("cloud").cast("string"), F.lit(""))).alias("p_cloud"),
        F.col("usage_unit").alias("p_usage_unit"),
        F.col("currency_code").alias("p_currency"),
        F.col("price_start_time").alias("p_start"),
        F.col("price_end_time").alias("p_end"),
        _unit_price_column(prices).alias("p_unit_price"),
    )

    condicion = (
        (usage["sku_name"] == precios["p_sku_name"])
        & (F.coalesce(usage["usage_unit"], F.lit("")) == F.coalesce(precios["p_usage_unit"], F.lit("")))
        & (
            (F.upper(F.coalesce(usage["cloud"].cast("string"), F.lit(""))) == precios["p_cloud"])
            | (precios["p_cloud"] == F.lit(""))
        )
        & (usage["usage_end_time"] >= precios["p_start"])
        & (precios["p_end"].isNull() | (usage["usage_end_time"] < precios["p_end"]))
    )

    unido = usage.join(F.broadcast(precios), condicion, "left")

    # Un registro podria empatar con mas de un tramo de precio (bordes de
    # vigencia solapados); se conserva el tramo mas reciente.
    ventana = Window.partitionBy("record_id").orderBy(F.col("p_start").desc_nulls_last())
    return (
        unido.withColumn("_rn", F.row_number().over(ventana))
        .filter(F.col("_rn") == 1)
        .drop("_rn", "p_sku_name", "p_cloud", "p_usage_unit", "p_start", "p_end")
        .withColumnRenamed("p_unit_price", "unit_price")
        .withColumnRenamed("p_currency", "price_currency")
    )


# ---------------------------------------------------------------------------
# slv_usage_priced
# ---------------------------------------------------------------------------
def build_usage_priced(spark: SparkSession, cfg: FinOpsConfig) -> DataFrame:
    """Construye el DataFrame de consumo valorizado para la ventana de proceso."""
    from pyspark.sql import functions as F

    usage = spark.table(BRZ_USAGE.fqn(cfg)).filter(
        F.col("usage_date").between(cfg.min_date, cfg.max_date)
    )
    prices = spark.table(BRZ_LIST_PRICES.fqn(cfg))

    # --- entidad de consumo ---
    metadata_cols = {
        campo: _struct_field(usage, "usage_metadata", campo)
        for campo in (
            "job_id", "job_run_id", "cluster_id", "warehouse_id", "dlt_pipeline_id",
            "endpoint_id", "instance_pool_id", "notebook_id", "app_id", "metastore_id",
        )
    }
    df = usage
    for campo, columna in metadata_cols.items():
        df = df.withColumn(campo, _blank_to_null(columna))
    df = df.withColumn(
        "run_as",
        _blank_to_null(
            F.coalesce(
                _struct_field(usage, "identity_metadata", "run_as"),
                _struct_field(usage, "identity_metadata", "owned_by"),
                _struct_field(usage, "usage_metadata", "run_as"),
            )
        ),
    )

    tipo_entidad = F.lit("UNKNOWN")
    id_entidad = F.lit(None).cast("string")
    # Se recorre la prioridad al reves para que la primera coincidencia quede
    # en el `when` mas externo (mismo criterio que tags.ENTITY_PRIORITY).
    from .tags import ENTITY_PRIORITY

    for clave, etiqueta in reversed(ENTITY_PRIORITY):
        if clave not in df.columns:
            continue
        tipo_entidad = F.when(F.col(clave).isNotNull(), F.lit(etiqueta)).otherwise(tipo_entidad)
        id_entidad = F.when(F.col(clave).isNotNull(), F.col(clave)).otherwise(id_entidad)

    df = (
        df.withColumn("entity_type", tipo_entidad)
        .withColumn("entity_id", id_entidad)
        .withColumn(
            "entity_key",
            F.concat_ws(":", F.col("entity_type"), F.coalesce(F.col("entity_id"), F.lit("SIN_ID"))),
        )
    )

    # --- clasificacion de SKU ---
    df = (
        df.withColumn("sku_group", sku_group_expr())
        .withColumn(
            "compute_family",
            F.when(F.col("sku_group").startswith("SERVERLESS"), F.lit("SERVERLESS"))
            .when(F.col("sku_group").isin("ALL_PURPOSE", "JOBS", "DLT", "SQL"), F.col("sku_group"))
            .otherwise(F.lit("OTHER")),
        )
        .withColumn(
            "is_serverless",
            F.upper(F.coalesce(F.col("sku_name"), F.lit(""))).rlike("(?i)SERVERLESS")
            | F.upper(F.coalesce(F.col("billing_origin_product").cast("string"), F.lit(""))).rlike("(?i)SERVERLESS"),
        )
        .withColumn("is_photon", F.upper(F.coalesce(F.col("sku_name"), F.lit(""))).rlike("(?i)PHOTON"))
    )

    # --- etiquetas: custom_tags del consumo + tags del cluster + tags del job ---
    df = df.withColumn(
        "_tags_usage",
        _normalized_tag_map(F.col("custom_tags")) if "custom_tags" in df.columns else _map_literal({}),
    )

    if table_exists(spark, BRZ_CLUSTERS.fqn(cfg)):
        clusters = _latest_by(spark.table(BRZ_CLUSTERS.fqn(cfg)), ["cluster_id"], "change_time")
        clusters = clusters.select(
            F.col("cluster_id").alias("_c_cluster_id"),
            _normalized_tag_map(F.col("tags")).alias("_tags_cluster"),
            F.col("cluster_name").alias("cluster_name"),
            F.col("owned_by").alias("cluster_owner"),
        )
        df = df.join(F.broadcast(clusters), df["cluster_id"] == clusters["_c_cluster_id"], "left").drop(
            "_c_cluster_id"
        )
    else:
        df = df.withColumn("_tags_cluster", _map_literal({}))
        df = df.withColumn("cluster_name", F.lit(None).cast("string"))
        df = df.withColumn("cluster_owner", F.lit(None).cast("string"))

    if table_exists(spark, BRZ_JOBS.fqn(cfg)):
        jobs = _latest_by(spark.table(BRZ_JOBS.fqn(cfg)), ["workspace_id", "job_id"], "change_time")
        jobs = jobs.select(
            F.col("workspace_id").alias("_j_ws"),
            F.col("job_id").cast("string").alias("_j_job_id"),
            _normalized_tag_map(F.col("tags")).alias("_tags_job"),
            F.col("name").alias("job_name"),
            F.col("run_as").alias("job_run_as"),
        )
        df = df.join(
            F.broadcast(jobs),
            (df["workspace_id"] == jobs["_j_ws"]) & (df["job_id"] == jobs["_j_job_id"]),
            "left",
        ).drop("_j_ws", "_j_job_id")
    else:
        df = df.withColumn("_tags_job", _map_literal({}))
        df = df.withColumn("job_name", F.lit(None).cast("string"))
        df = df.withColumn("job_run_as", F.lit(None).cast("string"))

    df = resolve_tag_columns(
        df,
        cfg,
        {"custom_tags": "_tags_usage", "cluster_tags": "_tags_cluster", "job_tags": "_tags_job"},
    )

    # --- nombre legible de la entidad ---
    df = df.withColumn(
        "entity_name",
        F.coalesce(
            F.when(F.col("entity_type") == "JOB", F.col("job_name")),
            F.when(F.col("entity_type") == "CLUSTER", F.col("cluster_name")),
            F.col("entity_id"),
        ),
    ).withColumn(
        "owner_resolved",
        F.coalesce(F.col("run_as"), F.col("job_run_as"), F.col("cluster_owner")),
    )

    # --- precios y costo ---
    df = join_prices(df, prices)
    descuento, regla = discount_expr(cfg.get("pricing.discounts"))
    infra_cfg = cfg.get("pricing.infra_estimate", {}) or {}
    factores = {
        familia: float((infra_cfg.get("factor_by_compute") or {}).get(familia, 0.0) or 0.0)
        for familia in P.COMPUTE_FAMILIES
    }
    infra_habilitado = bool(infra_cfg.get("enabled", False))

    factor_col = F.lit(0.0)
    if infra_habilitado:
        factor_col = F.coalesce(
            F.element_at(_map_literal({k: str(v) for k, v in factores.items()}), F.col("compute_family")).cast("double"),
            F.lit(0.0),
        )

    df = (
        df.withColumn("discount_pct", descuento)
        .withColumn("discount_rule", regla)
        .withColumn("infra_factor", factor_col)
        .withColumn("price_missing", F.col("unit_price").isNull())
        .withColumn(
            "list_cost_usd",
            F.round(F.coalesce(F.col("usage_quantity"), F.lit(0.0)) * F.coalesce(F.col("unit_price"), F.lit(0.0)), 6),
        )
        .withColumn("discount_amount_usd", F.round(F.col("list_cost_usd") * F.col("discount_pct"), 6))
        .withColumn("effective_cost_usd", F.round(F.col("list_cost_usd") - F.col("discount_amount_usd"), 6))
        .withColumn("estimated_infra_cost_usd", F.round(F.col("effective_cost_usd") * F.col("infra_factor"), 6))
        .withColumn(
            "total_cost_usd", F.round(F.col("effective_cost_usd") + F.col("estimated_infra_cost_usd"), 6)
        )
    )

    dimensiones = list(cfg.get("tagging.dimensions", []) or [])
    columnas = [
        "record_id", "account_id", "workspace_id", "cloud", "sku_name", "usage_date",
        "usage_start_time", "usage_end_time", "usage_unit", "usage_quantity",
        "billing_origin_product", "record_type", "usage_type",
        "sku_group", "compute_family", "is_serverless", "is_photon",
        "entity_type", "entity_id", "entity_key", "entity_name",
        "job_id", "job_run_id", "cluster_id", "warehouse_id", "dlt_pipeline_id",
        "endpoint_id", "instance_pool_id", "owner_resolved",
        "unit_price", "price_currency", "price_missing",
        "discount_pct", "discount_rule", "infra_factor",
        "list_cost_usd", "discount_amount_usd", "effective_cost_usd",
        "estimated_infra_cost_usd", "total_cost_usd",
        *dimensiones,
        *[f"tag_source_{d}" for d in dimensiones],
        "tags_resolved", "tags_expected", "is_fully_tagged", "is_untagged",
        "_run_id",
    ]
    disponibles = [c for c in columnas if c in df.columns]
    return df.select(*disponibles)


def _latest_by(df: DataFrame, keys: list[str], order_col: str) -> DataFrame:
    """Ultima version de cada clave segun una columna de tiempo de cambio."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    if order_col not in df.columns:
        return df.dropDuplicates(keys)
    ventana = Window.partitionBy(*keys).orderBy(F.col(order_col).desc_nulls_last())
    return df.withColumn("_rn", F.row_number().over(ventana)).filter(F.col("_rn") == 1).drop("_rn")


# ---------------------------------------------------------------------------
# Dimensiones operativas de silver
# ---------------------------------------------------------------------------
def build_clusters(spark: SparkSession, cfg: FinOpsConfig) -> DataFrame:
    """Ultima configuracion de cada cluster con banderas de eficiencia."""
    from pyspark.sql import functions as F

    base = _latest_by(spark.table(BRZ_CLUSTERS.fqn(cfg)), ["workspace_id", "cluster_id"], "change_time")
    return base.select(
        "account_id", "workspace_id", "cluster_id", "cluster_name",
        F.col("owned_by").alias("cluster_owner"),
        "create_time", "delete_time", "cluster_source", "policy_id", "data_security_mode",
        F.col("driver_node_type").alias("driver_node_type"),
        F.col("worker_node_type").alias("worker_node_type"),
        F.col("worker_count").cast("int").alias("num_workers"),
        F.col("min_autoscale_workers").cast("int").alias("min_workers"),
        F.col("max_autoscale_workers").cast("int").alias("max_workers"),
        F.col("auto_termination_minutes").cast("int").alias("autotermination_minutes"),
        F.col("dbr_version").alias("spark_version"),
        F.col("tags").alias("cluster_tags"),
        (F.col("max_autoscale_workers").isNotNull() & (F.col("max_autoscale_workers") > F.col("min_autoscale_workers")))
        .alias("autoscale_enabled"),
        (F.coalesce(F.col("worker_count"), F.lit(0)) == 0).alias("is_single_node"),
        F.col("delete_time").isNotNull().alias("is_deleted"),
    )


def build_warehouses(spark: SparkSession, cfg: FinOpsConfig) -> DataFrame:
    """Ultima configuracion de cada SQL warehouse."""
    from pyspark.sql import functions as F

    base = _latest_by(spark.table(BRZ_WAREHOUSES.fqn(cfg)), ["workspace_id", "warehouse_id"], "change_time")
    return base.select(
        "account_id", "workspace_id", "warehouse_id", "warehouse_name",
        "warehouse_type", "warehouse_channel", "warehouse_size",
        F.col("min_clusters").cast("int").alias("min_clusters"),
        F.col("max_clusters").cast("int").alias("max_clusters"),
        F.col("auto_stop_minutes").cast("int").alias("auto_stop_minutes"),
        F.col("tags").alias("warehouse_tags"),
        F.col("delete_time").isNotNull().alias("is_deleted"),
        F.upper(F.coalesce(F.col("warehouse_type"), F.lit(""))).contains("SERVERLESS").alias("is_serverless"),
    )


def build_job_runs(spark: SparkSession, cfg: FinOpsConfig) -> DataFrame:
    """Consolida la linea de tiempo en una fila por ejecucion de job."""
    from pyspark.sql import functions as F

    runs = spark.table(BRZ_JOB_RUNS.fqn(cfg)).filter(
        F.col("run_date").between(cfg.min_date, cfg.max_date)
    )
    agregado = runs.groupBy("workspace_id", "job_id", "run_id").agg(
        F.min("period_start_time").alias("run_start_time"),
        F.max("period_end_time").alias("run_end_time"),
        F.min("run_date").alias("run_date"),
        F.max("run_name").alias("run_name"),
        F.max("trigger_type").alias("trigger_type"),
        F.max("run_type").alias("run_type"),
        F.max("result_state").alias("result_state"),
        F.max("termination_code").alias("termination_code"),
        F.first("compute_ids", ignorenulls=True).alias("compute_ids"),
    )

    resultado = agregado.withColumn(
        "duration_minutes",
        F.round(
            (F.col("run_end_time").cast("double") - F.col("run_start_time").cast("double")) / 60.0, 4
        ),
    ).withColumn(
        "is_failed",
        F.upper(F.coalesce(F.col("result_state"), F.lit(""))).isin(
            "FAILED", "ERROR", "TIMEDOUT", "TIMED_OUT", "INTERNAL_ERROR", "UPSTREAM_FAILED"
        ),
    ).withColumn(
        "is_success", F.upper(F.coalesce(F.col("result_state"), F.lit(""))).isin("SUCCEEDED", "SUCCESS")
    ).withColumn(
        "is_cancelled", F.upper(F.coalesce(F.col("result_state"), F.lit(""))).isin("CANCELED", "CANCELLED")
    )

    if table_exists(spark, BRZ_JOBS.fqn(cfg)):
        jobs = _latest_by(spark.table(BRZ_JOBS.fqn(cfg)), ["workspace_id", "job_id"], "change_time").select(
            F.col("workspace_id").alias("_j_ws"),
            F.col("job_id").cast("string").alias("_j_job_id"),
            F.col("name").alias("job_name"),
            F.col("run_as").alias("job_run_as"),
            F.col("creator_id").alias("job_creator_id"),
        )
        resultado = resultado.join(
            F.broadcast(jobs),
            (resultado["workspace_id"] == jobs["_j_ws"])
            & (resultado["job_id"].cast("string") == jobs["_j_job_id"]),
            "left",
        ).drop("_j_ws", "_j_job_id")
    return resultado


def build_queries(spark: SparkSession, cfg: FinOpsConfig) -> DataFrame:
    """Agrega el historial de consultas por dia / warehouse / usuario."""
    from pyspark.sql import functions as F

    historial = spark.table(BRZ_QUERY_HISTORY.fqn(cfg)).filter(
        F.col("query_date").between(cfg.min_date, cfg.max_date)
    )
    warehouse_id = _struct_field(historial, "compute", "warehouse_id")
    cluster_id = _struct_field(historial, "compute", "cluster_id")

    return (
        historial.withColumn("warehouse_id", warehouse_id)
        .withColumn("compute_cluster_id", cluster_id)
        .groupBy("query_date", "workspace_id", "warehouse_id", "executed_by")
        .agg(
            F.count("*").alias("query_count"),
            F.sum(F.coalesce(F.col("total_duration_ms"), F.lit(0))).alias("total_duration_ms"),
            F.avg(F.col("total_duration_ms")).alias("avg_duration_ms"),
            F.expr("percentile_approx(total_duration_ms, 0.95)").alias("p95_duration_ms"),
            F.sum(F.coalesce(F.col("waiting_for_compute_duration_ms"), F.lit(0))).alias("queue_ms"),
            F.sum(F.coalesce(F.col("read_bytes"), F.lit(0))).alias("read_bytes"),
            F.sum(F.coalesce(F.col("produced_rows"), F.lit(0))).alias("produced_rows"),
            F.sum(
                F.when(F.upper(F.coalesce(F.col("execution_status"), F.lit(""))).contains("FAIL"), 1).otherwise(0)
            ).alias("failed_query_count"),
            F.countDistinct("statement_id").alias("distinct_statements"),
        )
        .withColumn("active_hours", F.round(F.col("total_duration_ms") / 3_600_000.0, 4))
    )


# ---------------------------------------------------------------------------
# Orquestacion de la capa silver
# ---------------------------------------------------------------------------
def run_silver(spark: SparkSession, cfg: FinOpsConfig, run_id: str, *, recorder: Any = None) -> dict[str, int]:
    """Construye toda la capa silver."""
    propiedades = cfg.get("catalog.table_properties", {}) or {}
    resultados: dict[str, int] = {}

    with stage("silver.usage_priced", recorder) as metrica:
        df = build_usage_priced(spark, cfg)
        metrica.rows = replace_date_range(
            spark, df, SLV_USAGE_PRICED.fqn(cfg),
            date_column="usage_date", min_date=cfg.min_date, max_date=cfg.max_date,
            partition_by=list(SLV_USAGE_PRICED.partition_by), properties=propiedades, dry_run=cfg.dry_run,
        )
        resultados["silver.usage_priced"] = metrica.rows

    opcionales = (
        ("silver.clusters", BRZ_CLUSTERS, SLV_CLUSTERS, build_clusters, None),
        ("silver.warehouses", BRZ_WAREHOUSES, SLV_WAREHOUSES, build_warehouses, None),
        ("silver.job_runs", BRZ_JOB_RUNS, SLV_JOB_RUNS, build_job_runs, "run_date"),
        ("silver.queries", BRZ_QUERY_HISTORY, SLV_QUERIES, build_queries, "query_date"),
    )
    for nombre, origen, destino, constructor, columna_fecha in opcionales:
        with stage(nombre, recorder) as metrica:
            if not table_exists(spark, origen.fqn(cfg)):
                metrica.status = "skipped"
                metrica.details["motivo"] = f"{origen.fqn(cfg)} no disponible"
                resultados[nombre] = 0
                continue
            df = constructor(spark, cfg)
            if columna_fecha:
                metrica.rows = replace_date_range(
                    spark, df, destino.fqn(cfg),
                    date_column=columna_fecha, min_date=cfg.min_date, max_date=cfg.max_date,
                    partition_by=list(destino.partition_by), properties=propiedades, dry_run=cfg.dry_run,
                )
            else:
                metrica.rows = overwrite_table(
                    spark, df, destino.fqn(cfg), properties=propiedades, dry_run=cfg.dry_run
                )
            resultados[nombre] = metrica.rows
    return resultados
