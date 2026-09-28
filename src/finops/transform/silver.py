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
# Clasificacion de SKU en Spark: compila pricing.CASCADA_DE_SKU
# ---------------------------------------------------------------------------
def sku_group_expr(sku_col: str = "sku_name", product_col: str = "billing_origin_product") -> Column:
    """Compila `pricing.CASCADA_DE_SKU` a una expresion CASE WHEN.

    No reimplementa la clasificacion: recorre la MISMA cascada que
    `pricing.classify_sku`. Antes eran dos implementaciones de la misma regla --
    los patrones se compartian, pero la precedencia estaba escrita dos veces --
    y la de Spark, que es la que corre en el pipeline, no tenia ni una prueba.

    La cascada se recorre AL REVES para que el `otherwise` de cada paso envuelva
    al siguiente: asi el primer paso queda mas afuera y gana, que es el orden en
    que la evalua Python.
    """
    from pyspark.sql import functions as F

    sku = F.upper(F.coalesce(F.col(sku_col).cast("string"), F.lit("")))
    producto = F.upper(F.trim(F.coalesce(F.col(product_col).cast("string"), F.lit(""))))
    serverless = sku.rlike(f"(?i){P.PATRON_SERVERLESS}") | producto.rlike(f"(?i){P.PATRON_SERVERLESS}")

    columnas = {"producto": producto, "sku": sku}

    expr = F.when(serverless, F.lit(P.GRUPO_POR_DEFECTO_SERVERLESS)).otherwise(
        F.lit(P.GRUPO_POR_DEFECTO)
    )
    for paso in reversed(P.CASCADA_DE_SKU):
        columna = columnas[paso.campo]
        if paso.operador == "igual":
            condicion = columna == F.lit(paso.valor)
        elif paso.operador == "regex":
            condicion = columna.rlike(f"(?i){paso.valor}")
        else:  # pragma: no cover - `test_pricing` falla antes si aparece uno nuevo
            raise ValueError(f"Operador sin traduccion a Spark: '{paso.operador}'")

        if paso.variante_serverless:
            destino = F.when(serverless, F.lit(f"SERVERLESS_{paso.grupo}")).otherwise(
                F.lit(paso.grupo)
            )
        else:
            destino = F.lit(paso.grupo)

        expr = F.when(condicion, destino).otherwise(expr)
    return expr


def discount_expr(discount_rules: list[dict[str, Any]] | None) -> tuple[Column, Column]:
    """Compila las reglas de descuento a (descuento, nombre_regla).

    No reimplementa la regla: usa las piezas de `pricing` que usa la version
    Python -- `CLAVES_DE_DESCUENTO`, `glob_a_like`, `pct_de_regla` y
    `nombre_de_regla` -- para que las dos no puedan divergir.

    Divergian, y de forma que pegaba en la factura. Probado contra el motor
    real de Databricks:

      - Un descuento por `account_id` (un UUID en minusculas) NUNCA se aplicaba:
        el patron se pasaba a mayusculas y la columna no, y LIKE distingue
        mayusculas. Python lo aplicaba. El tablero reportaba precio de lista,
        por encima de la factura, sin ningun error.
      - `_` en un patron es comodin en LIKE y literal en glob.
      - Con `*`, un valor NULL coincidia en Spark (se convertia en '' y
        `'' LIKE '%'` es verdadero) y no en Python.

    Ahora toda columna del contexto se pasa a mayusculas, el patron se traduce
    con `glob_a_like`, y un NULL no coincide con nada (`IS NOT NULL`).

    La cascada se recorre al reves para que la PRIMERA regla que coincida quede
    en el `when` mas externo: es el orden en que la evalua Python.
    """
    from pyspark.sql import functions as F

    contexto = {clave: F.upper(F.col(clave).cast("string")) for clave in P.CLAVES_DE_DESCUENTO}

    descuento = F.lit(0.0)
    nombre = F.lit("sin_descuento")
    for regla in reversed(discount_rules or []):
        if not isinstance(regla, dict):
            continue
        condicion = F.lit(True)
        for clave, esperado in (regla.get("match") or {}).items():
            columna = contexto.get(clave)
            if columna is None:
                # Clave que el contexto no conoce: tampoco coincide en Python.
                condicion = F.lit(False)
                break
            candidatos = esperado if isinstance(esperado, (list, tuple)) else [esperado]
            alguno = F.lit(False)
            for candidato in candidatos:
                alguno = alguno | columna.like(P.glob_a_like(candidato))
            condicion = condicion & columna.isNotNull() & alguno
        descuento = F.when(condicion, F.lit(P.pct_de_regla(regla))).otherwise(descuento)
        nombre = F.when(condicion, F.lit(P.nombre_de_regla(regla))).otherwise(nombre)
    return descuento, nombre


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
    """Precio unitario del struct `pricing`, segun `pricing.CAMPOS_DE_PRECIO`.

    La decision de que campos leer vive en `campos_de_precio_disponibles`, que
    es pura; aqui solo se traduce a columnas.
    """
    from pyspark.sql import functions as F
    from pyspark.sql.types import StructType

    if "pricing" not in prices.columns:
        return F.lit(None).cast("double")
    tipo = prices.schema["pricing"].dataType
    if not isinstance(tipo, StructType):
        return F.col("pricing").cast("double")

    estructura = {
        campo.name: (campo.dataType.fieldNames() if isinstance(campo.dataType, StructType) else None)
        for campo in tipo.fields
    }
    candidatos = [
        F.col(f"pricing.{ruta}").cast("double")
        for ruta in P.campos_de_precio_disponibles(estructura)
    ]
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
# ---------------------------------------------------------------------------
# Consumo valorizado (slv_usage_priced), paso a paso
#
# Antes era UNA funcion de 215 lineas que hacia siete cosas distintas y en dos
# de ellas reimplementaba a mano logica que ya existia en `pricing`. Ahora cada
# paso es una funcion DataFrame -> DataFrame con nombre, que se puede ubicar,
# leer y probar por separado. `build_usage_priced` solo los encadena.
#
#     extraer_entidad      que recurso genero el consumo (job, cluster, ...)
#     clasificar_sku       grupo, familia de computo, serverless, photon
#     adjuntar_etiquetas   tags del consumo, del cluster y del job -> dimensiones
#     adjuntar_nombres     nombre legible de la entidad y responsable
#     valorizar            precio, descuento, infraestructura y costo
#     columnas_de_salida   el contrato de columnas de la tabla
#
# Cada paso tiene sus pruebas en tests/test_silver_pasos.py, en una clase con
# el mismo nombre que la funcion.
# ---------------------------------------------------------------------------

#: Campos de `usage_metadata` que identifican el recurso que genero el consumo.
CAMPOS_DE_ENTIDAD = (
    "job_id", "job_run_id", "cluster_id", "warehouse_id", "dlt_pipeline_id",
    "endpoint_id", "instance_pool_id", "notebook_id", "app_id", "metastore_id",
)


def extraer_entidad(df: DataFrame) -> DataFrame:
    """Recurso que genero cada registro: `entity_type`, `entity_id`, `entity_key`.

    La prioridad sale de `tags.ENTITY_PRIORITY`, la misma que usa la version
    Python. Se recorre al reves para que la primera coincidencia quede en el
    `when` mas externo.
    """
    from pyspark.sql import functions as F

    from .tags import ENTITY_PRIORITY

    for campo in CAMPOS_DE_ENTIDAD:
        df = df.withColumn(campo, _blank_to_null(_struct_field(df, "usage_metadata", campo)))
    df = df.withColumn(
        "run_as",
        _blank_to_null(
            F.coalesce(
                _struct_field(df, "identity_metadata", "run_as"),
                _struct_field(df, "identity_metadata", "owned_by"),
                _struct_field(df, "usage_metadata", "run_as"),
            )
        ),
    )

    tipo_entidad = F.lit("UNKNOWN")
    id_entidad = F.lit(None).cast("string")
    for clave, etiqueta in reversed(ENTITY_PRIORITY):
        if clave not in df.columns:
            continue
        tipo_entidad = F.when(F.col(clave).isNotNull(), F.lit(etiqueta)).otherwise(tipo_entidad)
        id_entidad = F.when(F.col(clave).isNotNull(), F.col(clave)).otherwise(id_entidad)

    return (
        df.withColumn("entity_type", tipo_entidad)
        .withColumn("entity_id", id_entidad)
        .withColumn(
            "entity_key",
            F.concat_ws(":", F.col("entity_type"), F.coalesce(F.col("entity_id"), F.lit("SIN_ID"))),
        )
    )


def clasificar_sku(df: DataFrame) -> DataFrame:
    """`sku_group`, `compute_family`, `is_serverless` e `is_photon`.

    Todo sale de `pricing`: la cascada de grupos, los grupos con familia propia
    y los patrones de serverless y photon. Antes las tres ultimas columnas se
    escribian aqui a mano; hoy coincidian con Python, pero nada lo garantizaba.
    """
    from pyspark.sql import functions as F

    sku = F.upper(F.coalesce(F.col("sku_name"), F.lit("")))
    producto = F.upper(F.coalesce(F.col("billing_origin_product").cast("string"), F.lit("")))
    return (
        df.withColumn("sku_group", sku_group_expr())
        .withColumn(
            "compute_family",
            F.when(F.col("sku_group").startswith(P.PATRON_SERVERLESS), F.lit("SERVERLESS"))
            .when(F.col("sku_group").isin(*sorted(P.GRUPOS_CON_FAMILIA_PROPIA)), F.col("sku_group"))
            .otherwise(F.lit("OTHER")),
        )
        .withColumn(
            "is_serverless",
            sku.rlike(f"(?i){P.PATRON_SERVERLESS}") | producto.rlike(f"(?i){P.PATRON_SERVERLESS}"),
        )
        .withColumn("is_photon", sku.rlike(f"(?i){P.PATRON_PHOTON}"))
    )


def _adjuntar_ultima_version(
    df: DataFrame,
    spark: SparkSession,
    fqn: str,
    claves: list[tuple[str, str]],
    columnas: dict[str, Column],
    vacias: dict[str, Column],
) -> DataFrame:
    """Une la ultima version de una tabla de catalogo, o rellena si no existe.

    Los catalogos de clusters, jobs y warehouses son fuentes OPCIONALES: el
    principal puede no tener permiso, o la system table puede no existir en la
    cuenta. Su ausencia no rompe el pipeline; deja esas columnas vacias.

    Antes el mismo bloque "si existe, une la ultima version; si no, rellena con
    nulos" estaba copiado tres veces, una por catalogo.

    `claves` son pares (columna en df, columna en el catalogo).
    """
    from pyspark.sql import functions as F

    if not table_exists(spark, fqn):
        for nombre, valor in vacias.items():
            df = df.withColumn(nombre, valor)
        return df

    catalogo = _latest_by(spark.table(fqn), [c for _, c in claves], "change_time")
    alias = {c: f"_k_{c}" for _, c in claves}
    catalogo = catalogo.select(
        *[F.col(c).cast("string").alias(alias[c]) for _, c in claves],
        *[col.alias(nombre) for nombre, col in columnas.items()],
    )
    condicion = None
    for propia, ajena in claves:
        parte = df[propia].cast("string") == catalogo[alias[ajena]]
        condicion = parte if condicion is None else condicion & parte
    return df.join(F.broadcast(catalogo), condicion, "left").drop(*alias.values())


def adjuntar_etiquetas(spark: SparkSession, cfg: FinOpsConfig, df: DataFrame) -> DataFrame:
    """Etiquetas del consumo, del cluster y del job -> dimensiones canonicas."""
    from pyspark.sql import functions as F

    df = df.withColumn(
        "_tags_usage",
        _normalized_tag_map(F.col("custom_tags")) if "custom_tags" in df.columns else _map_literal({}),
    )
    nulo = F.lit(None).cast("string")
    df = _adjuntar_ultima_version(
        df, spark, BRZ_CLUSTERS.fqn(cfg),
        claves=[("cluster_id", "cluster_id")],
        columnas={
            "_tags_cluster": _normalized_tag_map(F.col("tags")),
            "cluster_name": F.col("cluster_name"),
            "cluster_owner": F.col("owned_by"),
        },
        vacias={"_tags_cluster": _map_literal({}), "cluster_name": nulo, "cluster_owner": nulo},
    )
    df = _adjuntar_ultima_version(
        df, spark, BRZ_JOBS.fqn(cfg),
        claves=[("workspace_id", "workspace_id"), ("job_id", "job_id")],
        columnas={
            "_tags_job": _normalized_tag_map(F.col("tags")),
            "job_name": F.col("name"),
            "job_run_as": F.col("run_as"),
        },
        vacias={"_tags_job": _map_literal({}), "job_name": nulo, "job_run_as": nulo},
    )
    return resolve_tag_columns(
        df, cfg,
        {"custom_tags": "_tags_usage", "cluster_tags": "_tags_cluster", "job_tags": "_tags_job"},
    )


def adjuntar_nombres(spark: SparkSession, cfg: FinOpsConfig, df: DataFrame) -> DataFrame:
    """Nombre legible de la entidad y su responsable.

    El nombre es solo para mostrar: agrupar siempre por entity_key/entity_id,
    porque un nombre puede repetirse o cambiar. Cada tipo toma su nombre de
    donde exista; si no hay, queda el id.
    """
    from pyspark.sql import functions as F

    df = _adjuntar_ultima_version(
        df, spark, BRZ_WAREHOUSES.fqn(cfg),
        claves=[("workspace_id", "workspace_id"), ("warehouse_id", "warehouse_id")],
        columnas={"_warehouse_name": F.col("warehouse_name")},
        vacias={"_warehouse_name": F.lit(None).cast("string")},
    )

    def metadato(campo: str) -> Column:
        return _blank_to_null(_struct_field(df, "usage_metadata", campo))

    tipo = F.col("entity_type")
    return df.withColumn(
        "entity_name",
        F.coalesce(
            F.when(tipo == "JOB", F.coalesce(F.col("job_name"), metadato("job_name"))),
            F.when(tipo == "CLUSTER", F.col("cluster_name")),
            F.when(tipo == "WAREHOUSE", F.col("_warehouse_name")),
            F.when(tipo == "APP", metadato("app_name")),
            F.when(tipo == "MODEL_ENDPOINT", metadato("endpoint_name")),
            F.when(tipo == "NOTEBOOK", metadato("notebook_path")),
            F.col("entity_id"),
        ),
    ).withColumn(
        "owner_resolved",
        F.coalesce(F.col("run_as"), F.col("job_run_as"), F.col("cluster_owner")),
    )


def factor_de_infraestructura(cfg: FinOpsConfig) -> Column:
    """Factor del estimador de infraestructura segun la familia de computo.

    Acotado en 0 por abajo, como `pricing.price_record`: un factor negativo
    restaria costo del total. Antes Spark no lo acotaba y Python si, y
    `validate_config` no lo revisaba, asi que un factor negativo pasaba.
    """
    from pyspark.sql import functions as F

    infra = cfg.get("pricing.infra_estimate", {}) or {}
    if not bool(infra.get("enabled", False)):
        return F.lit(0.0)
    factores = {
        familia: str(float((infra.get("factor_by_compute") or {}).get(familia, 0.0) or 0.0))
        for familia in P.COMPUTE_FAMILIES
    }
    bruto = F.element_at(_map_literal(factores), F.col("compute_family")).cast("double")
    return F.greatest(F.coalesce(bruto, F.lit(0.0)), F.lit(0.0))


def valorizar(df: DataFrame, prices: DataFrame, cfg: FinOpsConfig) -> DataFrame:
    """Precio de lista vigente, descuento, estimacion de infraestructura y costo.

    La aritmetica es `pricing.componentes_de_costo`, la MISMA funcion que evalua
    Python, aplicada sobre Columns en vez de floats. Antes estaba reescrita aqui
    a mano y redondeaba en otro orden que Python.
    """
    from pyspark.sql import functions as F

    df = join_prices(df, prices)
    descuento, regla = discount_expr(cfg.get("pricing.discounts"))
    df = (
        df.withColumn("discount_pct", descuento)
        .withColumn("discount_rule", regla)
        .withColumn("infra_factor", factor_de_infraestructura(cfg))
        .withColumn("price_missing", F.col("unit_price").isNull())
    )
    costos = P.componentes_de_costo(
        cantidad=F.coalesce(F.col("usage_quantity"), F.lit(0.0)),
        precio=F.coalesce(F.col("unit_price"), F.lit(0.0)),
        descuento=F.col("discount_pct"),
        factor=F.col("infra_factor"),
        redondear=lambda c: F.round(c, P.DECIMALES_DE_COSTO),
    )
    for nombre, columna in costos.items():
        df = df.withColumn(nombre, columna)
    return df


def columnas_de_salida(df: DataFrame, cfg: FinOpsConfig) -> DataFrame:
    """El contrato de columnas de slv_usage_priced, en su orden.

    Se proyectan solo las que existen: las fuentes opcionales pueden faltar y
    su ausencia no rompe la tabla.
    """
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
        *P.COMPONENTES_DE_COSTO,
        *dimensiones,
        *[f"tag_source_{d}" for d in dimensiones],
        "tags_resolved", "tags_expected", "is_fully_tagged", "is_untagged",
        "_run_id",
    ]
    return df.select(*[c for c in columnas if c in df.columns])


def build_usage_priced(spark: SparkSession, cfg: FinOpsConfig) -> DataFrame:
    """Consumo valorizado de la ventana de proceso. Solo encadena los pasos."""
    from pyspark.sql import functions as F

    uso = spark.table(BRZ_USAGE.fqn(cfg)).filter(
        F.col("usage_date").between(cfg.min_date, cfg.max_date)
    )
    precios = spark.table(BRZ_LIST_PRICES.fqn(cfg))

    df = extraer_entidad(uso)
    df = clasificar_sku(df)
    df = adjuntar_etiquetas(spark, cfg, df)
    df = adjuntar_nombres(spark, cfg, df)
    df = valorizar(df, precios, cfg)
    return columnas_de_salida(df, cfg)


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
