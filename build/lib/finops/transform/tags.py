"""Normalizacion de etiquetas y atribucion de costo.

El costo solo es accionable si se puede imputar a un responsable. En Databricks
las etiquetas llegan por varias rutas y con nombres inconsistentes:

  * `usage.custom_tags`      etiquetas propagadas del recurso (cluster/warehouse/job)
  * `usage.usage_metadata`   metadata estructurada (job_id, cluster_id, warehouse_id, ...)
  * tags del cluster / del job definidos por cada equipo

Este modulo resuelve, para cada registro, un valor por dimension canonica
(cost_center, team, project, environment, application, owner) aplicando:

  1. indice de alias  (costcenter / cc / centro_costo -> cost_center)
  2. cadena de precedencia de fuentes (la primera que resuelva gana)
  3. normalizacion de valores (prod/produccion/production -> PRD)
  4. valor por defecto del workspace
  5. valor 'SIN_ASIGNAR'

Funciones puras, sin dependencia de Spark.
"""

from __future__ import annotations

import re
from typing import Any

_NON_ALNUM = re.compile(r"[^a-z0-9]+")

#: Orden de precedencia por defecto de las fuentes de etiquetas.
DEFAULT_SOURCE_ORDER = ("custom_tags", "cluster_tags", "job_tags", "workspace_defaults")


def normalize_key(key: str | None) -> str:
    """Normaliza una clave de etiqueta para comparacion tolerante.

    'Cost-Center', 'cost_center', 'COSTCENTER ' -> 'costcenter'
    """
    if key is None:
        return ""
    return _NON_ALNUM.sub("", str(key).strip().lower())


def normalize_value(value: Any) -> str | None:
    """Limpia el valor de una etiqueta. Devuelve None si queda vacio."""
    if value is None:
        return None
    texto = str(value).strip()
    if not texto or texto.lower() in {"null", "none", "n/a", "na", "-", "undefined"}:
        return None
    return texto


def build_alias_index(aliases: dict[str, list[str]] | None) -> dict[str, str]:
    """Construye el indice clave_normalizada -> dimension canonica.

    Si dos dimensiones declaran el mismo alias, gana la primera en orden de
    declaracion (comportamiento determinista y documentado).
    """
    indice: dict[str, str] = {}
    for dimension, variantes in (aliases or {}).items():
        candidatos = [dimension, *(variantes or [])]
        for candidato in candidatos:
            clave = normalize_key(candidato)
            if clave and clave not in indice:
                indice[clave] = dimension
    return indice


def canonicalize_value(dimension: str, value: str | None, value_map: dict[str, dict[str, str]] | None) -> str | None:
    """Aplica el mapa de valores canonicos de una dimension."""
    if value is None:
        return None
    mapa = (value_map or {}).get(dimension) or {}
    if not mapa:
        return value
    # Comparacion insensible a mayusculas y separadores.
    indice = {normalize_key(k): v for k, v in mapa.items()}
    return indice.get(normalize_key(value), value)


def extract_dimensions(
    tag_map: dict[str, Any] | None,
    alias_index: dict[str, str],
    dimensions: list[str],
) -> dict[str, str]:
    """Proyecta un mapa de etiquetas crudo sobre las dimensiones canonicas."""
    salida: dict[str, str] = {}
    for clave_cruda, valor_crudo in (tag_map or {}).items():
        dimension = alias_index.get(normalize_key(clave_cruda))
        if dimension is None or dimension not in dimensions:
            continue
        valor = normalize_value(valor_crudo)
        if valor is not None and dimension not in salida:
            salida[dimension] = valor
    return salida


def resolve_tags(
    sources: dict[str, dict[str, Any] | None],
    *,
    dimensions: list[str],
    alias_index: dict[str, str],
    value_map: dict[str, dict[str, str]] | None = None,
    unallocated_value: str = "SIN_ASIGNAR",
    source_order: tuple[str, ...] | list[str] = DEFAULT_SOURCE_ORDER,
) -> dict[str, Any]:
    """Resuelve todas las dimensiones para un registro.

    Args:
        sources: mapa nombre_fuente -> mapa de etiquetas. Ej:
            {"custom_tags": {...}, "cluster_tags": {...}, "workspace_defaults": {...}}
        dimensions: dimensiones canonicas a resolver.
        alias_index: salida de `build_alias_index`.
        value_map: normalizacion de valores por dimension.
        unallocated_value: valor cuando ninguna fuente resuelve la dimension.
        source_order: precedencia de fuentes.

    Returns:
        dict con una clave por dimension, mas:
            tag_source_<dim>   nombre de la fuente que resolvio la dimension
            tags_resolved      cantidad de dimensiones resueltas
            is_fully_tagged    True si todas las dimensiones se resolvieron
            is_untagged        True si ninguna se resolvio
    """
    resuelto: dict[str, str] = {}
    origen: dict[str, str] = {}

    for nombre_fuente in source_order:
        mapa = sources.get(nombre_fuente)
        if not mapa:
            continue
        encontrados = extract_dimensions(mapa, alias_index, dimensions)
        for dimension, valor in encontrados.items():
            if dimension not in resuelto:
                resuelto[dimension] = valor
                origen[dimension] = nombre_fuente

    salida: dict[str, Any] = {}
    for dimension in dimensions:
        bruto = resuelto.get(dimension)
        canonico = canonicalize_value(dimension, bruto, value_map)
        salida[dimension] = canonico if canonico is not None else unallocated_value
        salida[f"tag_source_{dimension}"] = origen.get(dimension, "default")

    resueltas = sum(1 for d in dimensions if salida[d] != unallocated_value)
    salida["tags_resolved"] = resueltas
    salida["tags_expected"] = len(dimensions)
    salida["is_fully_tagged"] = resueltas == len(dimensions)
    salida["is_untagged"] = resueltas == 0
    return salida


# ---------------------------------------------------------------------------
# Resolucion de entidad de consumo
# ---------------------------------------------------------------------------
#: Prioridad de resolucion: la primera clave presente en usage_metadata define
#: el tipo de entidad. El orden refleja especificidad (una tarea de job en un
#: cluster de job debe imputarse al job, no al cluster).
ENTITY_PRIORITY: tuple[tuple[str, str], ...] = (
    ("job_id", "JOB"),
    ("dlt_pipeline_id", "PIPELINE"),
    ("warehouse_id", "WAREHOUSE"),
    ("endpoint_id", "MODEL_ENDPOINT"),
    ("instance_pool_id", "INSTANCE_POOL"),
    ("cluster_id", "CLUSTER"),
    ("notebook_id", "NOTEBOOK"),
    ("app_id", "APP"),
    ("metastore_id", "METASTORE"),
)


def resolve_entity(usage_metadata: dict[str, Any] | None) -> dict[str, Any]:
    """Determina la entidad de consumo a partir de `usage_metadata`.

    Devuelve entity_type, entity_id y entity_key ('<TIPO>:<id>'), ademas de los
    identificadores individuales para poder cruzar con las dimensiones.
    """
    metadata = usage_metadata or {}
    tipo, identificador = "UNKNOWN", None
    for clave, etiqueta in ENTITY_PRIORITY:
        valor = normalize_value(metadata.get(clave))
        if valor is not None:
            tipo, identificador = etiqueta, valor
            break

    return {
        "entity_type": tipo,
        "entity_id": identificador,
        "entity_key": f"{tipo}:{identificador}" if identificador else f"{tipo}:SIN_ID",
        "job_id": normalize_value(metadata.get("job_id")),
        "job_run_id": normalize_value(metadata.get("job_run_id")),
        "cluster_id": normalize_value(metadata.get("cluster_id")),
        "warehouse_id": normalize_value(metadata.get("warehouse_id")),
        "dlt_pipeline_id": normalize_value(metadata.get("dlt_pipeline_id")),
        "endpoint_id": normalize_value(metadata.get("endpoint_id")),
        "instance_pool_id": normalize_value(metadata.get("instance_pool_id")),
        "run_as": normalize_value(metadata.get("run_as") or metadata.get("owner")),
    }


def tag_coverage(records: list[dict[str, Any]], dimension: str, unallocated_value: str = "SIN_ASIGNAR") -> dict[str, Any]:
    """Calcula la cobertura de etiquetado ponderada por costo.

    Args:
        records: lista de dicts con al menos `dimension` y 'total_cost_usd'.

    Returns:
        costo total, costo atribuido, costo sin atribuir y ratio de cobertura.
    """
    total = 0.0
    atribuido = 0.0
    for registro in records:
        costo = float(registro.get("total_cost_usd") or 0.0)
        total += costo
        if registro.get(dimension) not in (None, "", unallocated_value):
            atribuido += costo
    ratio = (atribuido / total) if total > 0 else 1.0
    return {
        "dimension": dimension,
        "total_cost_usd": round(total, 4),
        "attributed_cost_usd": round(atribuido, 4),
        "unattributed_cost_usd": round(total - atribuido, 4),
        "coverage_ratio": round(ratio, 6),
    }
