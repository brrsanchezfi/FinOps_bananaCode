"""Modelo de costos: clasificacion de SKU, descuentos y valorizacion.

`system.billing.usage` entrega cantidad de DBUs consumidos; `system.billing.list_prices`
entrega el precio de lista vigente por SKU en una ventana temporal. El costo
efectivo es:

    costo_lista     = usage_quantity * precio_unitario
    costo_efectivo  = costo_lista * (1 - descuento)
    costo_infra_est = costo_efectivo * factor_infra   (opcional, estimacion)
    costo_total     = costo_efectivo + costo_infra_est

Todas las funciones de este modulo son puras: reciben y devuelven tipos nativos.
Los adaptadores de Spark viven en `finops.transform.silver`.
"""

from __future__ import annotations

import fnmatch
import re
from typing import Any

# ---------------------------------------------------------------------------
# Clasificacion de SKU
# ---------------------------------------------------------------------------

#: Grupos canonicos de SKU expuestos en el modelo gold.
SKU_GROUPS = (
    "ALL_PURPOSE",     # clusters interactivos / notebooks
    "JOBS",            # compute de jobs
    "DLT",             # Delta Live Tables / Lakeflow Declarative Pipelines
    "SQL",             # SQL warehouses (classic y pro)
    "SERVERLESS_SQL",  # SQL serverless
    "SERVERLESS_JOBS", # jobs serverless
    "SERVERLESS_DLT",  # pipelines serverless
    "MODEL_SERVING",   # inferencia / endpoints
    "AI_TRAINING",     # fine tuning, model training
    "STORAGE_OPS",     # predictive optimization, lakehouse monitoring
    "OTHER",
)

#: Familias de precio para el estimador de infraestructura.
COMPUTE_FAMILIES = ("ALL_PURPOSE", "JOBS", "DLT", "SQL", "SERVERLESS", "OTHER")

_SERVERLESS_TOKEN = re.compile(r"SERVERLESS", re.IGNORECASE)
_PHOTON_TOKEN = re.compile(r"PHOTON", re.IGNORECASE)

# Orden importa: la primera coincidencia gana. Se declaran como cadenas para
# poder reutilizar exactamente los mismos patrones en el motor de Spark (rlike),
# garantizando que la clasificacion en Python y en SQL no divergan.
SKU_PATTERN_SPECS: tuple[tuple[str, str], ...] = (
    (r"SERVERLESS.*(SQL|DBSQL)", "SERVERLESS_SQL"),
    (r"(SQL|DBSQL).*SERVERLESS", "SERVERLESS_SQL"),
    (r"SERVERLESS.*(JOB|WORKFLOW|TASK)", "SERVERLESS_JOBS"),
    (r"(JOB|WORKFLOW).*SERVERLESS", "SERVERLESS_JOBS"),
    (r"SERVERLESS.*(DLT|PIPELINE)", "SERVERLESS_DLT"),
    (r"(DLT|PIPELINE).*SERVERLESS", "SERVERLESS_DLT"),
    (r"(REAL_TIME_INFERENCE|MODEL_SERVING|INFERENCE|VECTOR_SEARCH)", "MODEL_SERVING"),
    (r"(FINE_TUNING|MODEL_TRAINING|GPU.*TRAINING|MOSAIC)", "AI_TRAINING"),
    (r"(PREDICTIVE_OPTIMIZATION|LAKEHOUSE_MONITORING|MANAGED_STORAGE|ONLINE_TABLE)", "STORAGE_OPS"),
    (r"DLT|DELTA_LIVE|PIPELINE", "DLT"),
    (r"ALL_PURPOSE|INTERACTIVE|NOTEBOOK", "ALL_PURPOSE"),
    # `\b` no sirve aqui: en 'PREMIUM_JOBS_COMPUTE' el guion bajo es caracter de
    # palabra, asi que no hay frontera entre '_' y 'J'. Se delimita explicitamente.
    (r"(^|_)JOBS?(_|$)|AUTOMATED", "JOBS"),
    (r"SQL", "SQL"),
)

_SKU_PATTERNS: tuple[tuple[re.Pattern[str], str], ...] = tuple(
    (re.compile(patron, re.I), grupo) for patron, grupo in SKU_PATTERN_SPECS
)

#: Mapeo desde `billing_origin_product` (mas confiable que el nombre del SKU).
PRODUCT_TO_GROUP = {
    "ALL_PURPOSE": "ALL_PURPOSE",
    "INTERACTIVE": "ALL_PURPOSE",
    "NOTEBOOKS": "ALL_PURPOSE",
    "JOBS": "JOBS",
    "WORKFLOWS": "JOBS",
    "DLT": "DLT",
    "LAKEFLOW_PIPELINES": "DLT",
    "SQL": "SQL",
    "DBSQL": "SQL",
    "MODEL_SERVING": "MODEL_SERVING",
    "REAL_TIME_INFERENCE": "MODEL_SERVING",
    "VECTOR_SEARCH": "MODEL_SERVING",
    "AGENT_EVALUATION": "MODEL_SERVING",
    "FINE_TUNING": "AI_TRAINING",
    "MODEL_TRAINING": "AI_TRAINING",
    "PREDICTIVE_OPTIMIZATION": "STORAGE_OPS",
    "LAKEHOUSE_MONITORING": "STORAGE_OPS",
    "ONLINE_TABLES": "STORAGE_OPS",
}


def is_serverless(sku_name: str | None, billing_origin_product: str | None = None) -> bool:
    """True si el SKU corresponde a compute serverless."""
    return any(
        texto and _SERVERLESS_TOKEN.search(str(texto))
        for texto in (sku_name, billing_origin_product)
    )


def is_photon(sku_name: str | None) -> bool:
    """True si el SKU factura la tarifa Photon."""
    return bool(sku_name and _PHOTON_TOKEN.search(str(sku_name)))


def classify_sku(sku_name: str | None, billing_origin_product: str | None = None) -> str:
    """Devuelve el grupo canonico de un SKU.

    Prioriza `billing_origin_product` cuando esta disponible porque es un campo
    controlado; cae al reconocimiento por patron sobre `sku_name` en otro caso.
    """
    sku = (sku_name or "").upper()
    producto = (billing_origin_product or "").upper().strip()
    serverless = is_serverless(sku, producto)

    base = PRODUCT_TO_GROUP.get(producto)
    if base:
        if serverless and base in {"SQL", "JOBS", "DLT"}:
            return f"SERVERLESS_{base}"
        return base

    for patron, grupo in _SKU_PATTERNS:
        if patron.search(sku):
            return grupo

    if serverless:
        return "SERVERLESS_JOBS"
    return "OTHER"


def compute_family(sku_group: str) -> str:
    """Familia usada por el estimador de costo de infraestructura."""
    if sku_group.startswith("SERVERLESS"):
        return "SERVERLESS"
    if sku_group in {"ALL_PURPOSE", "JOBS", "DLT", "SQL"}:
        return sku_group
    return "OTHER"


# ---------------------------------------------------------------------------
# Descuentos
# ---------------------------------------------------------------------------
def _matches_rule(match: dict[str, Any], context: dict[str, Any]) -> bool:
    """Evalua el bloque `match` de una regla de descuento contra un contexto.

    Un `match` vacio siempre coincide (regla por defecto). Los valores admiten
    comodines estilo glob y listas (OR).
    """
    for clave, esperado in (match or {}).items():
        actual = context.get(clave)
        if actual is None:
            return False
        actual_txt = str(actual)
        candidatos = esperado if isinstance(esperado, (list, tuple)) else [esperado]
        if not any(fnmatch.fnmatch(actual_txt.upper(), str(c).upper()) for c in candidatos):
            return False
    return True


def resolve_discount(rules: list[dict[str, Any]] | None, context: dict[str, Any]) -> tuple[float, str]:
    """Devuelve (descuento, nombre_regla) para un registro de consumo.

    Se aplica la PRIMERA regla que coincida, por lo que el orden en la
    configuracion define la precedencia.
    """
    for regla in rules or []:
        if not isinstance(regla, dict):
            continue
        if _matches_rule(regla.get("match", {}), context):
            pct = float(regla.get("discount_pct", 0.0) or 0.0)
            return max(0.0, min(pct, 0.999)), str(regla.get("name", "sin_nombre"))
    return 0.0, "sin_descuento"


# ---------------------------------------------------------------------------
# Valorizacion
# ---------------------------------------------------------------------------
def price_record(
    *,
    usage_quantity: float | None,
    unit_price: float | None,
    discount_pct: float = 0.0,
    infra_factor: float = 0.0,
) -> dict[str, float]:
    """Calcula los componentes de costo de un registro de consumo.

    Devuelve list_cost_usd, discount_amount_usd, effective_cost_usd,
    estimated_infra_cost_usd y total_cost_usd. Los None se tratan como 0 para
    que un precio faltante no propague nulos al modelo (queda visible via
    `price_missing`, que calcula la capa silver).
    """
    cantidad = float(usage_quantity or 0.0)
    precio = float(unit_price or 0.0)
    descuento = max(0.0, min(float(discount_pct or 0.0), 0.999))
    factor = max(0.0, float(infra_factor or 0.0))

    costo_lista = cantidad * precio
    monto_descuento = costo_lista * descuento
    costo_efectivo = costo_lista - monto_descuento
    costo_infra = costo_efectivo * factor

    return {
        "list_cost_usd": round(costo_lista, 6),
        "discount_amount_usd": round(monto_descuento, 6),
        "effective_cost_usd": round(costo_efectivo, 6),
        "estimated_infra_cost_usd": round(costo_infra, 6),
        "total_cost_usd": round(costo_efectivo + costo_infra, 6),
    }


def infra_factor_for(cfg_infra: dict[str, Any] | None, sku_group: str) -> float:
    """Factor de estimacion de infraestructura para un grupo de SKU."""
    if not cfg_infra or not cfg_infra.get("enabled", False):
        return 0.0
    factores = cfg_infra.get("factor_by_compute", {}) or {}
    familia = compute_family(sku_group)
    return float(factores.get(familia, factores.get("OTHER", 0.0)) or 0.0)


def build_pricing_context(record: dict[str, Any]) -> dict[str, Any]:
    """Extrae del registro crudo las claves que pueden usarse en `match`."""
    return {
        "workspace_id": record.get("workspace_id"),
        "account_id": record.get("account_id"),
        "sku_name": record.get("sku_name"),
        "sku_group": record.get("sku_group") or classify_sku(
            record.get("sku_name"), record.get("billing_origin_product")
        ),
        "billing_origin_product": record.get("billing_origin_product"),
        "cloud": record.get("cloud"),
    }


def enrich_usage_record(record: dict[str, Any], pricing_cfg: dict[str, Any]) -> dict[str, Any]:
    """Aplica clasificacion + descuento + valorizacion a un registro de consumo.

    Es la version pura de la transformacion silver; la implementacion Spark en
    `silver.py` replica exactamente esta semantica y se contrasta contra esta
    funcion en las pruebas.
    """
    grupo = classify_sku(record.get("sku_name"), record.get("billing_origin_product"))
    contexto = build_pricing_context({**record, "sku_group": grupo})
    descuento, nombre_regla = resolve_discount(pricing_cfg.get("discounts"), contexto)
    factor = infra_factor_for(pricing_cfg.get("infra_estimate"), grupo)

    costos = price_record(
        usage_quantity=record.get("usage_quantity"),
        unit_price=record.get("unit_price"),
        discount_pct=descuento,
        infra_factor=factor,
    )

    return {
        **record,
        "sku_group": grupo,
        "compute_family": compute_family(grupo),
        "is_serverless": is_serverless(record.get("sku_name"), record.get("billing_origin_product")),
        "is_photon": is_photon(record.get("sku_name")),
        "discount_pct": descuento,
        "discount_rule": nombre_regla,
        "infra_factor": factor,
        "price_missing": record.get("unit_price") is None,
        **costos,
    }
