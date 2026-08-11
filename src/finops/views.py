"""Vistas de gobierno de etiquetado, en vivo sobre las system tables.

A diferencia del resto del modelo, estas vistas **no dependen de que el pipeline
haya corrido**: leen `system.billing.usage` directamente, asi que un tablero
construido sobre ellas muestra el estado actual del etiquetado cada vez que se
abre. Lo unico que el pipeline hace es crearlas (DDL) en la etapa `setup`.

Ese es justamente el punto: el etiquetado es un problema de gobierno que se
corrige configurando policies, y quien lo corrige necesita ver el efecto de
inmediato, no al dia siguiente.

Dos consecuencias que hay que tener presentes:

  * **El costo es a precio de lista.** Los descuentos negociados viven en
    `conf/*.yml` y los aplica la capa silver, no estas vistas. Si hay descuento
    configurado, las cifras de aqui seran mayores que las de gold. Por eso las
    columnas se llaman `list_cost_usd` y no `total_cost_usd`.
  * **Quien consulte las vistas necesita permiso sobre `system.billing`.** Para
    los analistas, la via es publicar el tablero con credenciales embebidas.

Las funciones que construyen el SQL son puras y se prueban sin Spark; el unico
adaptador es `ensure_views`.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from .catalog import TableDef
from .config import FinOpsConfig
from .logging_utils import get_logger
from .transform.tags import build_alias_index, normalize_key

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import SparkSession

log = get_logger("views")

#: Expresion que normaliza una clave de etiqueta igual que `tags.normalize_key`.
_NORM = "REGEXP_REPLACE(LOWER(TRIM({expr})), '[^a-z0-9]', '')"

#: Valores textuales que se tratan como "sin valor" al resolver una etiqueta.
_VACIOS = ("null", "none", "n/a", "na", "-", "undefined")


VIEW_USAGE_LIVE = TableDef(
    "vw_usage_live", "gold", "vw_usage_live",
    "Consumo en vivo desde system.billing.usage, valorizado a precio de lista.",
)
VIEW_TAG_INVENTORY = TableDef(
    "vw_tag_inventory_live", "gold", "vw_tag_inventory_live",
    "Inventario en vivo de claves de etiqueta en uso, con su costo y si la configuracion las reconoce.",
)
VIEW_TAG_COVERAGE = TableDef(
    "vw_tag_coverage_live", "gold", "vw_tag_coverage_live",
    "Cobertura de etiquetado en vivo por dimension canonica y dia, ponderada por costo.",
)
VIEW_UNTAGGED = TableDef(
    "vw_untagged_spend_live", "gold", "vw_untagged_spend_live",
    "Recursos en vivo que generan costo sin ninguna dimension resoluble.",
)

ALL_VIEWS: tuple[TableDef, ...] = (
    VIEW_USAGE_LIVE,
    VIEW_TAG_INVENTORY,
    VIEW_TAG_COVERAGE,
    VIEW_UNTAGGED,
)


# ---------------------------------------------------------------------------
# Helpers puros
# ---------------------------------------------------------------------------
def _sql_literal_list(valores: list[str]) -> str:
    """Lista de literales SQL, con las comillas simples escapadas."""
    return ", ".join("'" + str(v).replace("'", "''") + "'" for v in valores)


def _valor_util(expr: str) -> str:
    """Expresion que devuelve el valor de la etiqueta o NULL si es un relleno.

    Un tag con valor 'null' o '-' esta tan sin resolver como uno ausente, y
    contarlo como atribuido inflaria la cobertura.
    """
    limpio = f"TRIM({expr})"
    return (
        f"CASE WHEN {limpio} IS NULL OR {limpio} = '' "
        f"OR LOWER({limpio}) IN ({_sql_literal_list(list(_VACIOS))}) "
        f"THEN NULL ELSE {limpio} END"
    )


def _expr_dimension(alias: list[str]) -> str:
    """Primer alias de la dimension que resuelva, sobre el mapa normalizado."""
    if not alias:
        return "CAST(NULL AS STRING)"
    candidatos = [_valor_util(f"tags_norm['{a}']") for a in alias]
    if len(candidatos) == 1:
        return candidatos[0]
    return "COALESCE(" + ", ".join(candidatos) + ")"


def alias_por_dimension(cfg: FinOpsConfig) -> dict[str, list[str]]:
    """Alias normalizados por dimension, en el orden declarado en la config."""
    dimensiones = list(cfg.get("tagging.dimensions", []) or [])
    indice = build_alias_index(cfg.get("tagging.aliases", {}) or {})
    salida: dict[str, list[str]] = {d: [] for d in dimensiones}
    for clave_norm, dimension in indice.items():
        if dimension in salida:
            salida[dimension].append(clave_norm)
    # Una dimension sin alias declarados se resuelve por su propio nombre.
    for dimension, alias in salida.items():
        if not alias:
            alias.append(normalize_key(dimension))
    return salida


# ---------------------------------------------------------------------------
# Constructores de SQL
# ---------------------------------------------------------------------------
def build_usage_live_sql(cfg: FinOpsConfig) -> str:
    """Vista base: consumo del periodo con precio de lista y tags normalizados."""
    fqn = VIEW_USAGE_LIVE.fqn(cfg)
    dias = int(cfg.get("governance.live_window_days", 90))
    origen = str(cfg.get("sources.billing_usage.table", "system.billing.usage"))
    precios = str(cfg.get("sources.billing_list_prices.table", "system.billing.list_prices"))
    clave_norm = _NORM.format(expr="e.key")

    return f"""
CREATE OR REPLACE VIEW {fqn} AS
WITH precios AS (
  SELECT
    sku_name,
    usage_unit,
    UPPER(COALESCE(CAST(cloud AS STRING), '')) AS cloud,
    price_start_time,
    price_end_time,
    COALESCE(pricing.effective_list.default, pricing.default) AS unit_price
  FROM {precios}
)
SELECT
  u.record_id,
  u.account_id,
  u.workspace_id,
  u.cloud,
  u.sku_name,
  u.billing_origin_product,
  u.usage_date,
  u.usage_unit,
  u.usage_quantity,
  u.custom_tags,
  -- Mapa de etiquetas con las claves normalizadas: permite resolver
  -- 'Cost-Center', 'cost_center' y 'COSTCENTER' con una sola busqueda.
  MAP_FROM_ENTRIES(
    TRANSFORM(
      MAP_ENTRIES(COALESCE(u.custom_tags, MAP())),
      e -> STRUCT({clave_norm} AS key, CAST(e.value AS STRING) AS value)
    )
  ) AS tags_norm,
  COALESCE(u.usage_metadata.job_id, u.usage_metadata.dlt_pipeline_id,
           u.usage_metadata.warehouse_id, u.usage_metadata.cluster_id,
           u.usage_metadata.endpoint_id)                       AS entity_id,
  CASE
    WHEN u.usage_metadata.job_id          IS NOT NULL THEN 'JOB'
    WHEN u.usage_metadata.dlt_pipeline_id IS NOT NULL THEN 'PIPELINE'
    WHEN u.usage_metadata.warehouse_id    IS NOT NULL THEN 'WAREHOUSE'
    WHEN u.usage_metadata.cluster_id      IS NOT NULL THEN 'CLUSTER'
    WHEN u.usage_metadata.endpoint_id     IS NOT NULL THEN 'ENDPOINT'
    ELSE 'UNKNOWN'
  END                                                          AS entity_type,
  ROUND(u.usage_quantity * COALESCE(p.unit_price, 0), 6)       AS list_cost_usd,
  p.unit_price IS NULL                                         AS price_missing
FROM {origen} u
LEFT JOIN precios p
       ON p.sku_name = u.sku_name
      AND COALESCE(p.usage_unit, '') = COALESCE(u.usage_unit, '')
      AND (p.cloud = UPPER(COALESCE(CAST(u.cloud AS STRING), '')) OR p.cloud = '')
      AND u.usage_end_time >= p.price_start_time
      AND (p.price_end_time IS NULL OR u.usage_end_time < p.price_end_time)
WHERE u.usage_date >= CURRENT_DATE() - INTERVAL {dias} DAYS
-- Un registro puede empatar con mas de un tramo de precio en los bordes de
-- vigencia; se conserva el mas reciente, igual que en la capa silver.
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY u.record_id ORDER BY p.price_start_time DESC NULLS LAST
) = 1
""".strip()


def build_tag_inventory_sql(cfg: FinOpsConfig) -> str:
    """Inventario de claves de etiqueta en uso, reconocidas y no reconocidas."""
    fqn = VIEW_TAG_INVENTORY.fqn(cfg)
    base = VIEW_USAGE_LIVE.fqn(cfg)
    alias = alias_por_dimension(cfg)

    ramas = "\n".join(
        f"    WHEN tag_key IN ({_sql_literal_list(claves)}) THEN '{dimension}'"
        for dimension, claves in alias.items()
        if claves
    )
    caso = f"CASE\n{ramas}\n    ELSE NULL\n  END" if ramas else "CAST(NULL AS STRING)"

    return f"""
CREATE OR REPLACE VIEW {fqn} AS
WITH explotado AS (
  SELECT
    {_NORM.format(expr="t.key")} AS tag_key,
    TRIM(t.key)                  AS tag_key_original,
    TRIM(t.value)                AS tag_value,
    u.list_cost_usd
  FROM {base} u
  LATERAL VIEW EXPLODE(COALESCE(u.custom_tags, MAP())) t AS key, value
)
SELECT
  tag_key,
  {caso}                                  AS maps_to_dimension,
  {caso} IS NOT NULL                      AS is_recognized,
  COLLECT_SET(tag_key_original)           AS spellings,
  COUNT(DISTINCT tag_value)               AS distinct_values,
  COUNT(*)                                AS records,
  ROUND(SUM(list_cost_usd), 2)            AS list_cost_usd
FROM explotado
GROUP BY tag_key
""".strip()


def build_tag_coverage_sql(cfg: FinOpsConfig) -> str:
    """Cobertura por dimension canonica y dia, ponderada por costo."""
    fqn = VIEW_TAG_COVERAGE.fqn(cfg)
    base = VIEW_USAGE_LIVE.fqn(cfg)
    alias = alias_por_dimension(cfg)

    bloques = []
    for dimension, claves in alias.items():
        expr = _expr_dimension(claves)
        bloques.append(
            f"""SELECT
  usage_date,
  workspace_id,
  '{dimension}' AS dimension,
  ROUND(SUM(list_cost_usd), 2)                                              AS total_cost_usd,
  ROUND(SUM(CASE WHEN {expr} IS NOT NULL THEN list_cost_usd ELSE 0 END), 2) AS attributed_cost_usd,
  COUNT(DISTINCT CASE WHEN {expr} IS NOT NULL THEN entity_id END)           AS attributed_entities,
  COUNT(DISTINCT entity_id)                                                 AS total_entities
FROM {base}
GROUP BY usage_date, workspace_id"""
        )

    union = "\nUNION ALL\n".join(bloques)
    return f"""
CREATE OR REPLACE VIEW {fqn} AS
WITH por_dimension AS (
{union}
)
SELECT
  usage_date,
  workspace_id,
  dimension,
  total_cost_usd,
  attributed_cost_usd,
  ROUND(total_cost_usd - attributed_cost_usd, 2) AS unattributed_cost_usd,
  CASE WHEN total_cost_usd > 0
       THEN ROUND(100.0 * attributed_cost_usd / total_cost_usd, 2)
       ELSE 100.0 END                            AS coverage_pct,
  attributed_entities,
  total_entities
FROM por_dimension
""".strip()


def build_untagged_spend_sql(cfg: FinOpsConfig) -> str:
    """Recursos que generan costo sin resolver ninguna dimension."""
    fqn = VIEW_UNTAGGED.fqn(cfg)
    base = VIEW_USAGE_LIVE.fqn(cfg)
    alias = alias_por_dimension(cfg)

    resueltas = [f"({_expr_dimension(claves)} IS NOT NULL)" for claves in alias.values() if claves]
    ninguna = " AND ".join(f"NOT {r}" for r in resueltas) if resueltas else "TRUE"

    return f"""
CREATE OR REPLACE VIEW {fqn} AS
SELECT
  entity_type,
  entity_id,
  workspace_id,
  sku_name,
  billing_origin_product,
  COUNT(*)                          AS records,
  MIN(usage_date)                   AS first_seen,
  MAX(usage_date)                   AS last_seen,
  ROUND(SUM(list_cost_usd), 2)      AS list_cost_usd,
  SIZE(MAX(COALESCE(custom_tags, MAP()))) AS tag_count
FROM {base}
WHERE {ninguna}
GROUP BY entity_type, entity_id, workspace_id, sku_name, billing_origin_product
""".strip()


#: (TableDef, constructor). El orden importa: las demas dependen de la base.
VIEW_BUILDERS = (
    (VIEW_USAGE_LIVE, build_usage_live_sql),
    (VIEW_TAG_INVENTORY, build_tag_inventory_sql),
    (VIEW_TAG_COVERAGE, build_tag_coverage_sql),
    (VIEW_UNTAGGED, build_untagged_spend_sql),
)


def all_view_sql(cfg: FinOpsConfig) -> list[tuple[str, str]]:
    """[(fqn, DDL)] de todas las vistas, en orden de dependencia."""
    return [(vista.fqn(cfg), constructor(cfg)) for vista, constructor in VIEW_BUILDERS]


# ---------------------------------------------------------------------------
# Adaptador Spark
# ---------------------------------------------------------------------------
def ensure_views(spark: SparkSession, cfg: FinOpsConfig) -> list[str]:
    """Crea o reemplaza las vistas de gobierno. Idempotente.

    Una vista que falle no aborta el resto ni la etapa: son un complemento de
    gobierno, y perder el tablero de etiquetado no justifica tumbar el pipeline
    que produce las cifras de costo. El motivo queda en el log.
    """
    creadas: list[str] = []
    for fqn, ddl in all_view_sql(cfg):
        if cfg.dry_run:
            log.info("[dry-run] crear vista %s", fqn)
            creadas.append(fqn)
            continue
        try:
            spark.sql(ddl)
            creadas.append(fqn)
            log.info("vista %s lista", fqn)
        except Exception as exc:  # noqa: BLE001
            log.warning(
                "No se pudo crear la vista %s: %s. "
                "Suele ser falta de permiso SELECT sobre system.billing.",
                fqn, exc,
            )
    return creadas
