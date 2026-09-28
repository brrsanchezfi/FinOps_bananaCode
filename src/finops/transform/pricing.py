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
from dataclasses import dataclass
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

#: Patrones que marcan un SKU como serverless o Photon. Los comparten la version
#: Python (`is_serverless`, `is_photon`) y la de Spark (`silver.clasificar_sku`,
#: `silver.sku_group_expr`), que antes escribian cada una el suyo.
PATRON_SERVERLESS = "SERVERLESS"
PATRON_PHOTON = "PHOTON"

_SERVERLESS_TOKEN = re.compile(PATRON_SERVERLESS, re.IGNORECASE)
_PHOTON_TOKEN = re.compile(PATRON_PHOTON, re.IGNORECASE)

#: Grupos de SKU que son su propia familia de computo. Todo SERVERLESS_* cae en
#: SERVERLESS y el resto en OTHER.
GRUPOS_CON_FAMILIA_PROPIA = frozenset({"ALL_PURPOSE", "JOBS", "DLT", "SQL"})

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

#: Grupos que cambian de nombre cuando el consumo es serverless.
_GRUPOS_CON_VARIANTE_SERVERLESS = frozenset({"SQL", "JOBS", "DLT"})

#: Grupo cuando nada coincide.
GRUPO_POR_DEFECTO = "OTHER"
GRUPO_POR_DEFECTO_SERVERLESS = "SERVERLESS_JOBS"

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


@dataclass(frozen=True)
class PasoDeClasificacion:
    """Un paso de la cascada que clasifica un SKU en su grupo canonico.

    Existe para que la clasificacion tenga UNA sola definicion. Antes habia dos
    implementaciones de la misma regla: `classify_sku` en Python (probada) y
    `silver.sku_group_expr` en Spark (sin una sola prueba, y la que de verdad
    corre en el pipeline). Compartian los patrones, pero la PRECEDENCIA estaba
    escrita dos veces, y nada impedia que una cambiara sin la otra.

    Ahora las dos recorren esta misma cascada: Python evaluandola y Spark
    compilandola a una expresion `CASE WHEN`.

    Campos
    ------
    campo    'producto' (billing_origin_product) o 'sku' (sku_name).
    operador 'igual' (exacto) o 'regex'.
    valor    el valor exacto o el patron, segun el operador.
    grupo    grupo canonico que se asigna al coincidir.
    variante_serverless  si True y el consumo es serverless, el grupo se emite
             como SERVERLESS_<grupo>. Solo aplica a SQL, JOBS y DLT.
    """

    campo: str
    operador: str
    valor: str
    grupo: str
    variante_serverless: bool = False

    def coincide(self, texto: str) -> bool:
        """Evaluacion en Python. El compilador de Spark hace lo equivalente."""
        if self.operador == "igual":
            return texto == self.valor
        if self.operador == "regex":
            return bool(re.search(self.valor, texto, re.IGNORECASE))
        raise ValueError(f"Operador de clasificacion desconocido: '{self.operador}'")

    def resultado(self, serverless: bool) -> str:
        if self.variante_serverless and serverless:
            return f"SERVERLESS_{self.grupo}"
        return self.grupo


def _construir_cascada() -> tuple[PasoDeClasificacion, ...]:
    """Orden = precedencia. `billing_origin_product` va primero porque es un
    campo controlado; el reconocimiento por patron sobre el nombre del SKU es el
    recurso siguiente."""
    pasos = [
        PasoDeClasificacion(
            "producto", "igual", producto, grupo,
            variante_serverless=grupo in _GRUPOS_CON_VARIANTE_SERVERLESS,
        )
        for producto, grupo in PRODUCT_TO_GROUP.items()
    ]
    pasos += [
        PasoDeClasificacion("sku", "regex", patron, grupo)
        for patron, grupo in SKU_PATTERN_SPECS
    ]
    return tuple(pasos)


#: Fuente de verdad de la clasificacion, para Python y para Spark.
CASCADA_DE_SKU: tuple[PasoDeClasificacion, ...] = _construir_cascada()

#: Operadores que el compilador de Spark tiene que saber traducir. Una prueba
#: falla si la cascada usa uno que el compilador no cubre: de lo contrario, un
#: operador nuevo se clasificaria distinto en Python que en Spark.
OPERADORES = frozenset({"igual", "regex"})
CAMPOS = frozenset({"producto", "sku"})


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

    for paso in CASCADA_DE_SKU:
        texto = producto if paso.campo == "producto" else sku
        if paso.coincide(texto):
            return paso.resultado(serverless)

    return GRUPO_POR_DEFECTO_SERVERLESS if serverless else GRUPO_POR_DEFECTO


def compute_family(sku_group: str) -> str:
    """Familia usada por el estimador de costo de infraestructura."""
    if sku_group.startswith(PATRON_SERVERLESS):
        return "SERVERLESS"
    if sku_group in GRUPOS_CON_FAMILIA_PROPIA:
        return sku_group
    return "OTHER"


# ---------------------------------------------------------------------------
# Descuentos
# ---------------------------------------------------------------------------
#: Claves por las que una regla de descuento puede discriminar.
#:
#: Es UNA lista para las dos implementaciones: `build_pricing_context` (Python)
#: y `silver.discount_expr` (Spark, la que corre en el pipeline). Antes cada una
#: declaraba las suyas, y aunque hoy coincidian, nada impedia que divergieran.
CLAVES_DE_DESCUENTO: tuple[str, ...] = (
    "workspace_id", "account_id", "sku_name", "sku_group", "billing_origin_product", "cloud",
)

#: Descuento maximo aplicable. Un 100 % convertiria el consumo en gratis y
#: taparia errores de configuracion.
_DESCUENTO_MAXIMO = 0.999


def glob_a_like(patron: str) -> str:
    """Traduce un patron glob de una regla de descuento a SQL LIKE.

    Es la pieza que hace que Python (fnmatch) y Spark (LIKE) coincidan, y no
    coincidian. Los dos lenguajes de comodines no son el mismo:

        glob   *  cualquier cosa     ?  un caracter     _ %  literales
        LIKE   %  cualquier cosa     _  un caracter

    Antes solo se traducia `*` -> `%`. El `_` -- que abunda en los nombres de SKU
    -- quedaba como comodin de LIKE: `PREMIUM_JOBS_*` coincidia en Spark con
    `PREMIUMXJOBSXCOMPUTE` y en Python no. Y `?` quedaba literal en Spark.

    Se devuelve en MAYUSCULAS: la comparacion es insensible a mayusculas en los
    dos lados, y en Spark la columna tambien se pasa a mayusculas.

    Los rangos `[...]` de glob no tienen equivalente en LIKE: se rechazan (ver
    `validate_config`) en vez de traducirlos a algo que diverja en silencio.
    """
    texto = str(patron).upper()
    if "[" in texto or "]" in texto:
        raise ValueError(
            f"Patron de descuento '{patron}' con rango [...]: no tiene equivalente en "
            "SQL LIKE y se clasificaria distinto en Python y en Spark. Usa '*' o '?'."
        )
    salida = []
    for caracter in texto:
        if caracter == "*":
            salida.append("%")
        elif caracter == "?":
            salida.append("_")
        elif caracter in "%_\\":
            salida.append("\\" + caracter)
        else:
            salida.append(caracter)
    return "".join(salida)


def pct_de_regla(regla: dict[str, Any]) -> float:
    """Descuento de una regla, acotado a [0, 0.999]. Compartido con Spark."""
    return max(0.0, min(float(regla.get("discount_pct", 0.0) or 0.0), _DESCUENTO_MAXIMO))


def nombre_de_regla(regla: dict[str, Any]) -> str:
    return str(regla.get("name", "sin_nombre"))


def _matches_rule(match: dict[str, Any], context: dict[str, Any]) -> bool:
    """Evalua el bloque `match` de una regla de descuento contra un contexto.

    Un `match` vacio siempre coincide (regla por defecto). Los valores admiten
    comodines estilo glob y listas (OR). Un valor ausente NO coincide con nada,
    ni siquiera con `*`: el compilador de Spark hace lo mismo con `IS NOT NULL`.

    `fnmatchcase` y no `fnmatch`: `fnmatch` pasa por `os.path.normcase`, que en
    Windows cambia la barra por barra invertida, asi que el resultado dependia del sistema
    operativo -- y las pruebas corren en Windows en local y en Linux en CI.
    """
    for clave, esperado in (match or {}).items():
        actual = context.get(clave)
        if actual is None:
            return False
        actual_txt = str(actual).upper()
        candidatos = esperado if isinstance(esperado, (list, tuple)) else [esperado]
        if not any(fnmatch.fnmatchcase(actual_txt, str(c).upper()) for c in candidatos):
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
            return pct_de_regla(regla), nombre_de_regla(regla)
    return 0.0, "sin_descuento"


# ---------------------------------------------------------------------------
# Valorizacion
# ---------------------------------------------------------------------------

#: Campos del struct `pricing` de system.billing.list_prices, en orden de
#: precedencia: se toma el primero que no sea nulo.
#:
#: `effective_list.default` va primero porque refleja el precio realmente
#: aplicable cuando hay promociones vigentes; `default` es el precio de lista
#: puro. En una cuenta real difieren en 300 de 1.067 tramos, asi que el orden
#: no es cosmetico.
#:
#: Lo consumen las DOS valorizaciones del producto: la capa silver (tablas gold)
#: y la vista `vw_usage_live` (tablero de gobierno). Antes cada una tenia su
#: lista y la de la vista no traia `promotional`. Medido sobre esa cuenta, esa
#: diferencia no movia ninguna cifra -- ningun tramo carece de precio principal
#: -- pero era la unica diferencia de definicion entre dos joins sincronizados a
#: mano, y nada impedia que crecieran otras.
CAMPOS_DE_PRECIO: tuple[str, ...] = ("effective_list.default", "default", "promotional.default")


def campos_de_precio_disponibles(campos_del_struct: dict[str, list[str] | None]) -> list[str]:
    """Campos de `CAMPOS_DE_PRECIO` que existen en el esquema real de `pricing`.

    `campos_del_struct` mapea cada campo de primer nivel a sus subcampos, o a
    None si no es un struct. Funcion pura: el esquema de las system tables
    cambia, y decidir que columnas leer no deberia requerir una SparkSession
    para probarse.
    """
    salida = []
    for ruta in CAMPOS_DE_PRECIO:
        partes = ruta.split(".")
        if partes[0] not in campos_del_struct:
            continue
        if len(partes) == 1 or partes[1] in (campos_del_struct[partes[0]] or []):
            salida.append(ruta)
    return salida


#: Decimales de todo importe del modelo.
DECIMALES_DE_COSTO = 6

#: Columnas de costo, en orden. Son las claves que devuelve
#: `componentes_de_costo`, y forman parte del contrato de slv_usage_priced.
COMPONENTES_DE_COSTO: tuple[str, ...] = (
    "list_cost_usd", "discount_amount_usd", "effective_cost_usd",
    "estimated_infra_cost_usd", "total_cost_usd",
)


def componentes_de_costo(cantidad, precio, descuento, factor, redondear) -> dict[str, Any]:
    """LA formula de costo. Se evalua sobre floats (Python) o sobre Columns (Spark).

    Esta escrita una sola vez y se le inyecta como redondear: `round(x, 6)` en
    Python, `F.round(c, 6)` en Spark. Los operadores `*`, `-` y `+` funcionan
    igual sobre floats que sobre Columns, asi que las dos implementaciones son
    la MISMA funcion y no pueden divergir.

    Divergian. Python redondeaba solo al final y Spark en cada paso, arrastrando
    el valor redondeado. Medido sobre 9.502 registros reales con 15 % de
    descuento y factor 0,85: el 30 % daba distinto, aunque en plata la
    diferencia era de $0,000157 en un mes.

    Se eligio la version de Spark -- redondear en cada paso -- no por exactitud
    sino porque CUADRA: cada componente sale de los ya redondeados, asi que
    `lista - descuento == efectivo` y `efectivo + infra == total` exactos. Con
    el redondeo al final, el 42 % de los registros no cuadraba. En un reporte
    financiero, columnas que no suman se notan antes que un micro-dolar.
    """
    lista = redondear(cantidad * precio)
    monto_descuento = redondear(lista * descuento)
    efectivo = redondear(lista - monto_descuento)
    infra = redondear(efectivo * factor)
    return {
        "list_cost_usd": lista,
        "discount_amount_usd": monto_descuento,
        "effective_cost_usd": efectivo,
        "estimated_infra_cost_usd": infra,
        "total_cost_usd": redondear(efectivo + infra),
    }


def price_record(
    *,
    usage_quantity: float | None,
    unit_price: float | None,
    discount_pct: float = 0.0,
    infra_factor: float = 0.0,
) -> dict[str, float]:
    """Componentes de costo de un registro de consumo (version Python).

    Los None se tratan como 0 para que un precio faltante no propague nulos al
    modelo (queda visible via `price_missing`, que calcula la capa silver). La
    formula es `componentes_de_costo`, la misma que evalua Spark.
    """
    return componentes_de_costo(
        cantidad=float(usage_quantity or 0.0),
        precio=float(unit_price or 0.0),
        descuento=max(0.0, min(float(discount_pct or 0.0), _DESCUENTO_MAXIMO)),
        factor=max(0.0, float(infra_factor or 0.0)),
        redondear=lambda x: round(x, DECIMALES_DE_COSTO),
    )


def infra_factor_for(cfg_infra: dict[str, Any] | None, sku_group: str) -> float:
    """Factor de estimacion de infraestructura para un grupo de SKU."""
    if not cfg_infra or not cfg_infra.get("enabled", False):
        return 0.0
    factores = cfg_infra.get("factor_by_compute", {}) or {}
    familia = compute_family(sku_group)
    return float(factores.get(familia, factores.get("OTHER", 0.0)) or 0.0)


def build_pricing_context(record: dict[str, Any]) -> dict[str, Any]:
    """Extrae del registro crudo las claves que pueden usarse en `match`."""
    contexto = {clave: record.get(clave) for clave in CLAVES_DE_DESCUENTO}
    contexto["sku_group"] = contexto["sku_group"] or classify_sku(
        record.get("sku_name"), record.get("billing_origin_product")
    )
    return contexto


def enrich_usage_record(record: dict[str, Any], pricing_cfg: dict[str, Any]) -> dict[str, Any]:
    """Aplica clasificacion + descuento + valorizacion a un registro de consumo.

    Es la version pura de la transformacion silver. Las piezas que deciden algo
    -- `CASCADA_DE_SKU`, `CLAVES_DE_DESCUENTO`, `glob_a_like`, `pct_de_regla` --
    las comparte con la implementacion Spark de `silver.py`, en vez de que cada
    lado tenga la suya.

    Esta docstring afirmaba antes que la version Spark "se contrasta contra esta
    funcion en las pruebas". No era cierto: `silver.py` estaba al 0 % de
    cobertura, y la divergencia que habia -- un descuento por `account_id` que
    Spark nunca aplicaba -- paso inadvertida justamente por eso.
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
