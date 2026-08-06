"""Adaptadores de Spark: sesion, lectura tolerante, escritura idempotente.

Este modulo concentra TODA la interaccion con Spark/Delta para que el resto del
paquete permanezca testeable sin cluster. Las importaciones de pyspark son
perezosas: importar `finops.spark_utils` no requiere pyspark instalado.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import TYPE_CHECKING, Any

from .errors import ConfigError, SourceUnavailableError
from .logging_utils import get_logger

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import DataFrame, SparkSession

log = get_logger("spark")


# ---------------------------------------------------------------------------
# Sesion
# ---------------------------------------------------------------------------
def get_spark(app_name: str = "finops") -> SparkSession:
    """Devuelve la SparkSession activa o crea una nueva."""
    from pyspark.sql import SparkSession

    activa = SparkSession.getActiveSession()
    if activa is not None:
        return activa
    return SparkSession.builder.appName(app_name).getOrCreate()


#: Modo de sobrescritura de particiones que exige la plataforma.
#
# El patron incremental de este pipeline es DELETE explicito del rango + append
# (`replace_date_range`), nunca sobrescritura dinamica de particiones. Y las
# tablas de snapshot se reescriben enteras con `overwriteSchema`, que Delta
# **rechaza** si el modo dinamico esta activo:
#
#   [DELTA_OVERWRITE_SCHEMA_WITH_DYNAMIC_PARTITION_OVERWRITE]
#   'overwriteSchema' cannot be used in dynamic partition overwrite mode.
#
# Se fija explicitamente en 'static' para no depender de la configuracion del
# cluster, que puede traerlo en dinamico.
PARTITION_OVERWRITE_MODE = "static"


def configure_session(spark: SparkSession, shuffle_partitions: Any = "auto") -> None:
    """Aplica ajustes de rendimiento razonables para cargas FinOps."""
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", PARTITION_OVERWRITE_MODE)
    if shuffle_partitions and str(shuffle_partitions).lower() != "auto":
        spark.conf.set("spark.sql.shuffle.partitions", str(shuffle_partitions))


def current_user(spark: SparkSession) -> str:
    try:
        return spark.sql("SELECT current_user() AS u").collect()[0]["u"]
    except Exception:  # noqa: BLE001 - informativo
        return "desconocido"


# ---------------------------------------------------------------------------
# Lectura
# ---------------------------------------------------------------------------
def table_exists(spark: SparkSession, fqn: str) -> bool:
    """True si la tabla existe y es legible por el principal actual."""
    try:
        spark.sql(f"DESCRIBE TABLE {fqn}").limit(1).collect()
        return True
    except Exception:  # noqa: BLE001 - cualquier fallo se interpreta como no disponible
        return False


def read_table(spark: SparkSession, fqn: str) -> DataFrame:
    """Lee una tabla lanzando SourceUnavailableError con contexto util."""
    try:
        return spark.table(fqn)
    except Exception as exc:  # noqa: BLE001
        raise SourceUnavailableError(fqn, str(exc)) from exc


def read_source(spark: SparkSession, definition: dict[str, Any]) -> DataFrame | None:
    """Lee una fuente segun su definicion en `sources`.

    Intenta la tabla principal y luego los `fallback_tables` (util para el
    renombramiento system.workflow.* -> system.lakeflow.*). Devuelve None si la
    fuente es opcional y ninguna variante esta disponible.
    """
    candidatas = [definition["table"], *(definition.get("fallback_tables") or [])]
    for fqn in candidatas:
        if table_exists(spark, fqn):
            if fqn != definition["table"]:
                log.warning("Usando tabla alterna '%s' en lugar de '%s'", fqn, definition["table"])
            return spark.table(fqn)
    if definition.get("optional", False):
        log.warning("Fuente opcional no disponible, se omite: %s", candidatas)
        return None
    raise SourceUnavailableError(definition["table"], "no existe o sin permisos de lectura")


# ---------------------------------------------------------------------------
# Escritura
# ---------------------------------------------------------------------------
def build_create_catalog_sql(catalog: str, managed_location: str | None = None) -> str:
    """SQL de creacion de catalogo, con ubicacion gestionada si se configuro.

    Funcion pura para poder probarla sin Spark.
    """
    sql = f"CREATE CATALOG IF NOT EXISTS {catalog}"
    if managed_location:
        sql += f" MANAGED LOCATION '{managed_location.rstrip('/')}'"
    return sql


def build_create_schema_sql(catalog: str, schema: str, storage_root: str | None = None) -> str:
    """SQL de creacion de schema. Funcion pura."""
    sql = f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}"
    if storage_root:
        sql += f" MANAGED LOCATION '{storage_root.rstrip('/')}/{schema}'"
    return sql


def catalog_exists(spark: SparkSession, catalog: str) -> bool:
    """True si el catalogo existe y es visible para el principal actual."""
    try:
        spark.sql(f"DESCRIBE CATALOG {catalog}").limit(1).collect()
        return True
    except Exception:  # noqa: BLE001
        return False


def ensure_catalog(
    spark: SparkSession,
    catalog: str,
    *,
    managed_location: str | None = None,
    create_if_missing: bool = True,
) -> bool:
    """Garantiza que el catalogo exista. Devuelve True si lo creo esta corrida.

    Si el catalogo ya existe no se intenta crear: `CREATE CATALOG IF NOT EXISTS`
    no es inocuo cuando el metastore no tiene storage root, porque falla en vez
    de ser un no-op.

    Crear un catalogo es una operacion de administracion. Si no se puede, se
    lanza un error con las dos salidas posibles en lugar de un mensaje opaco de
    Unity Catalog.
    """
    if catalog_exists(spark, catalog):
        log.debug("El catalogo '%s' ya existe", catalog)
        return False

    if not create_if_missing:
        raise ConfigError(
            f"El catalogo '{catalog}' no existe y 'catalog.create_if_missing' es false.\n"
            f"Crealo con un administrador de Unity Catalog, o pon create_if_missing en true."
        )

    try:
        spark.sql(build_create_catalog_sql(catalog, managed_location))
        log.info("Catalogo '%s' creado%s", catalog, f" en {managed_location}" if managed_location else "")
        return True
    except Exception as exc:  # noqa: BLE001
        raise ConfigError(
            f"No se pudo crear el catalogo '{catalog}': {exc}\n\n"
            "Causa habitual: el metastore no tiene storage root definido (cuentas con "
            "Default Storage habilitado), asi que 'CREATE CATALOG' necesita una ubicacion "
            "explicita.\n\n"
            "Dos salidas:\n"
            f"  1. Crear el catalogo una sola vez, con un administrador:\n"
            f"       CREATE CATALOG {catalog};                      -- si hay Default Storage\n"
            f"       CREATE CATALOG {catalog} MANAGED LOCATION 'abfss://<contenedor>@<cuenta>"
            f".dfs.core.windows.net/<ruta>';\n"
            "     (o desde Catalog Explorer > Create catalog)\n"
            f"  2. Configurar la ubicacion en conf/<env>.yml para que el pipeline lo cree:\n"
            "       catalog:\n"
            "         managed_location: 'abfss://<contenedor>@<cuenta>.dfs.core.windows.net/<ruta>'\n\n"
            "La opcion 1 es la recomendada: crear catalogos no deberia ser responsabilidad "
            "de un pipeline de datos."
        ) from exc


def ensure_schema(
    spark: SparkSession,
    catalog: str,
    schema: str,
    storage_root: str | None = None,
    *,
    managed_location: str | None = None,
    create_catalog: bool = True,
) -> None:
    """Garantiza catalogo y schema."""
    ensure_catalog(spark, catalog, managed_location=managed_location, create_if_missing=create_catalog)
    spark.sql(build_create_schema_sql(catalog, schema, storage_root))


def apply_table_properties(spark: SparkSession, fqn: str, properties: dict[str, str] | None) -> None:
    if not properties:
        return
    pares = ", ".join(f"'{k}' = '{v}'" for k, v in properties.items())
    try:
        spark.sql(f"ALTER TABLE {fqn} SET TBLPROPERTIES ({pares})")
    except Exception as exc:  # noqa: BLE001 - no es fatal
        log.warning("No se pudieron aplicar TBLPROPERTIES en %s: %s", fqn, exc)


def with_audit_columns(df: DataFrame, run_id: str, source: str = "") -> DataFrame:
    """Agrega columnas de auditoria estandar a cualquier DataFrame."""
    from pyspark.sql import functions as F

    salida = df.withColumn("_ingested_at", F.current_timestamp()).withColumn("_run_id", F.lit(run_id))
    if source:
        salida = salida.withColumn("_source", F.lit(source))
    return salida


def overwrite_table(
    spark: SparkSession,
    df: DataFrame,
    fqn: str,
    *,
    partition_by: list[str] | None = None,
    properties: dict[str, str] | None = None,
    dry_run: bool = False,
) -> int:
    """Sobrescribe una tabla completa (snapshot). Devuelve filas escritas."""
    filas = df.count()
    if dry_run:
        log.info("[dry-run] overwrite %s (%s filas)", fqn, f"{filas:,}")
        return filas
    writer = (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option("partitionOverwriteMode", PARTITION_OVERWRITE_MODE)
    )
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.saveAsTable(fqn)
    apply_table_properties(spark, fqn, properties)
    log.info("overwrite %s -> %s filas", fqn, f"{filas:,}")
    return filas


def replace_date_range(
    spark: SparkSession,
    df: DataFrame,
    fqn: str,
    *,
    date_column: str,
    min_date: date,
    max_date: date,
    partition_by: list[str] | None = None,
    properties: dict[str, str] | None = None,
    dry_run: bool = False,
) -> int:
    """Escritura incremental idempotente por rango de fechas.

    Borra el rango [min_date, max_date] en el destino y agrega el nuevo lote.
    Es el patron correcto para billing: los registros de un dia pueden llegar
    tarde y deben reemplazar por completo lo que ya se habia calculado.
    """
    filas = df.count()
    if dry_run:
        log.info("[dry-run] replace %s rango [%s..%s] (%s filas)", fqn, min_date, max_date, f"{filas:,}")
        return filas

    if not table_exists(spark, fqn):
        writer = (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .option("partitionOverwriteMode", PARTITION_OVERWRITE_MODE)
        )
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        writer.saveAsTable(fqn)
        apply_table_properties(spark, fqn, properties)
        log.info("crear %s -> %s filas", fqn, f"{filas:,}")
        return filas

    spark.sql(
        f"DELETE FROM {fqn} WHERE {date_column} >= DATE'{min_date.isoformat()}' "
        f"AND {date_column} <= DATE'{max_date.isoformat()}'"
    )
    df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(fqn)
    apply_table_properties(spark, fqn, properties)
    log.info("replace %s rango [%s..%s] -> %s filas", fqn, min_date, max_date, f"{filas:,}")
    return filas


def delete_date_range(
    spark: SparkSession,
    fqn: str,
    *,
    date_column: str,
    min_date: date,
    max_date: date,
    dry_run: bool = False,
) -> None:
    """Borra un rango de fechas de una tabla, si existe.

    Se usa cuando una corrida re-evalua un rango y no produce filas: lo que se
    habia escrito antes para ese rango debe desaparecer (por ejemplo, una
    anomalia detectada ayer que hoy ya no lo es).
    """
    if not table_exists(spark, fqn):
        return
    if dry_run:
        log.info("[dry-run] delete %s rango [%s..%s]", fqn, min_date, max_date)
        return
    spark.sql(
        f"DELETE FROM {fqn} WHERE {date_column} >= DATE'{min_date.isoformat()}' "
        f"AND {date_column} <= DATE'{max_date.isoformat()}'"
    )
    log.info("delete %s rango [%s..%s]", fqn, min_date, max_date)


def merge_table(
    spark: SparkSession,
    df: DataFrame,
    fqn: str,
    *,
    keys: list[str],
    partition_by: list[str] | None = None,
    properties: dict[str, str] | None = None,
    update: bool = True,
    dry_run: bool = False,
) -> int:
    """MERGE upsert por clave de negocio. Crea la tabla si no existe."""
    filas = df.count()
    if dry_run:
        log.info("[dry-run] merge %s por %s (%s filas)", fqn, keys, f"{filas:,}")
        return filas

    if not table_exists(spark, fqn):
        writer = (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .option("partitionOverwriteMode", PARTITION_OVERWRITE_MODE)
        )
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        writer.saveAsTable(fqn)
        apply_table_properties(spark, fqn, properties)
        log.info("crear %s -> %s filas", fqn, f"{filas:,}")
        return filas

    vista = f"_stg_{fqn.replace('.', '_')}"
    df.createOrReplaceTempView(vista)
    condicion = " AND ".join(f"t.{k} <=> s.{k}" for k in keys)
    accion_update = "WHEN MATCHED THEN UPDATE SET *" if update else ""
    spark.sql(
        f"""
        MERGE INTO {fqn} AS t
        USING {vista} AS s
        ON {condicion}
        {accion_update}
        WHEN NOT MATCHED THEN INSERT *
        """
    )
    apply_table_properties(spark, fqn, properties)
    log.info("merge %s por %s -> %s filas de origen", fqn, keys, f"{filas:,}")
    return filas


def append_rows(
    spark: SparkSession,
    rows: list[dict[str, Any]],
    fqn: str,
    schema: Any = None,
    *,
    dry_run: bool = False,
) -> int:
    """Agrega filas Python a una tabla Delta (logs de corrida, alertas)."""
    if not rows:
        return 0
    if dry_run:
        log.info("[dry-run] append %s filas a %s", len(rows), fqn)
        return len(rows)
    df = spark.createDataFrame(rows, schema=schema) if schema else spark.createDataFrame(rows)
    df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(fqn)
    return len(rows)


def optimize_table(spark: SparkSession, fqn: str, zorder_by: list[str] | None = None) -> None:
    """OPTIMIZE + Z-ORDER tolerante a fallos (no aplica en todos los tiers)."""
    try:
        sql = f"OPTIMIZE {fqn}"
        if zorder_by:
            sql += f" ZORDER BY ({', '.join(zorder_by)})"
        spark.sql(sql)
    except Exception as exc:  # noqa: BLE001
        log.warning("OPTIMIZE fallo en %s (no es bloqueante): %s", fqn, exc)


def vacuum_table(spark: SparkSession, fqn: str, retain_hours: int = 168) -> None:
    try:
        spark.sql(f"VACUUM {fqn} RETAIN {retain_hours} HOURS")
    except Exception as exc:  # noqa: BLE001
        log.warning("VACUUM fallo en %s: %s", fqn, exc)


# ---------------------------------------------------------------------------
# Conversion Spark -> Python (para la capa de analitica pura)
# ---------------------------------------------------------------------------
def rows_to_dicts(df: DataFrame, limit: int | None = None) -> list[dict[str, Any]]:
    """Materializa un DataFrame como lista de dicts nativos de Python.

    Se usa para pasar agregados pequenos (series diarias por dimension) a los
    algoritmos puros de `finops.analytics`. No usar sobre datos crudos.
    """
    filas = df.limit(limit).collect() if limit else df.collect()
    salida: list[dict[str, Any]] = []
    for fila in filas:
        registro = fila.asDict(recursive=True)
        salida.append({k: _normalize_value(v) for k, v in registro.items()})
    return salida


def _normalize_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.replace(tzinfo=value.tzinfo or timezone.utc)
    return value
