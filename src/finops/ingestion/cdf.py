"""Ventana de proceso derivada del Change Data Feed de las system tables.

El pipeline rebarre a ciegas una ventana de `lookback_days` en cada corrida,
porque los registros de facturacion llegan tarde y no hay forma de saber cuales.
El CDF si lo dice: `table_changes()` devuelve exactamente las filas que
aparecieron desde un instante dado, y de ahi salen los `usage_date` que hay que
reprocesar.

La diferencia es de orden de magnitud. Medido sobre una cuenta con ~100
workspaces: 296 filas cambiaron en 24 horas, contra las decenas de miles que
tiene una ventana de 7 dias. Eso abarata la corrida lo suficiente para pasar de
diaria a horaria, que es de donde sale la frescura de los tableros.

Lo que el CDF NO arregla: `system.billing.usage` publica con su propio rezago
(medido 3.5 h, hasta 12 h segun la documentacion). La frescura del tablero nunca
baja de ahi, corra el pipeline cuando corra.

**El respaldo no es opcional.** La historia del feed se poda: pedir cambios desde
un instante que ya no existe falla con "Earliest available version is N". Pasa
solo con que el pipeline no corra un par de dias, que es un escenario que el
runbook ya contempla. Cuando el CDF no sirve, se vuelve a la ventana por
`lookback_days` sin que la corrida falle.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import TYPE_CHECKING

from ..config import FinOpsConfig
from ..logging_utils import get_logger

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import SparkSession

log = get_logger("cdf")

#: Margen que se resta al instante de la ultima corrida al pedir los cambios.
#: Cubre el desfase de reloj entre el cluster y el servidor de sharing: pedir
#: exactamente desde la ultima corrida puede perderse un commit que aterrizo en
#: el mismo segundo, y perder una fila de costo es peor que releer unas pocas.
MARGEN_SOLAPE = timedelta(minutes=10)


# ---------------------------------------------------------------------------
# Funciones puras
# ---------------------------------------------------------------------------
def build_changes_sql(table: str, since: datetime, *, date_column: str = "usage_date") -> str:
    """SQL que lista los valores de `date_column` tocados desde `since`.

    Se agrega en el origen a proposito: interesa el conjunto de fechas, no las
    filas. Traer las filas para despues hacer un distinct local mueve miles de
    registros por la red para nada.
    """
    marca = since.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    return (
        f"SELECT DISTINCT {date_column} AS fecha "
        f"FROM table_changes('{table}', '{marca}') "
        f"WHERE {date_column} IS NOT NULL"
    )


def window_from_changed_dates(
    changed: set[date] | list[date] | None, fallback: tuple[date, date]
) -> tuple[date, date]:
    """Ventana [min, max] que cubre las fechas cambiadas.

    `None` significa "el CDF no se pudo usar" y devuelve `fallback`. Un conjunto
    VACIO es distinto: significa "no cambio nada", y devuelve una ventana de un
    solo dia sobre el maximo del respaldo. Colapsarla a nada haria que una
    corrida sin novedades dejara las tablas sin tocar, y con ellas los chequeos
    de frescura, que empezarian a fallar por falta de datos nuevos.
    """
    if changed is None:
        return fallback
    fechas = sorted(changed)
    if not fechas:
        return fallback[1], fallback[1]
    return fechas[0], max(fechas[-1], fallback[1])


def cdf_enabled(cfg: FinOpsConfig) -> bool:
    return bool(cfg.get("ingestion.cdf.enabled", False))


def since_for(cfg: FinOpsConfig, last_run_at: datetime | None) -> datetime | None:
    """Desde cuando pedir cambios, o None si conviene no usar el CDF.

    Sin corrida previa no hay punto de partida. Y si la ultima corrida quedo mas
    atras que `max_lookback_hours`, tampoco se intenta: lo mas probable es que la
    historia del feed ya no la cubra, y la ventana por `lookback_days` es mas
    barata que descubrirlo con una consulta que falla.
    """
    if last_run_at is None:
        return None
    horas_max = int(cfg.get("ingestion.cdf.max_lookback_hours", 48))
    limite = datetime.now(timezone.utc) - timedelta(hours=horas_max)
    if last_run_at.tzinfo is None:
        last_run_at = last_run_at.replace(tzinfo=timezone.utc)
    if last_run_at < limite:
        log.info(
            "Ultima corrida (%s) fuera de la ventana de CDF (%s h); se usa lookback_days",
            last_run_at, horas_max,
        )
        return None
    return last_run_at - MARGEN_SOLAPE


# ---------------------------------------------------------------------------
# Adaptador Spark
# ---------------------------------------------------------------------------
def changed_usage_dates(
    spark: SparkSession, cfg: FinOpsConfig, since: datetime
) -> set[date] | None:
    """Fechas de consumo tocadas desde `since`, o None si el CDF no sirve.

    Devuelve None -- y NO lanza -- ante cualquier fallo: feed no disponible,
    historia podada, permisos. Es informacion para acotar trabajo, no un dato
    del modelo, asi que jamas debe tumbar una corrida que el camino de siempre
    podria completar.
    """
    tabla = str(cfg.get("sources.billing_usage.table", "system.billing.usage"))
    consulta = build_changes_sql(tabla, since)
    try:
        filas = spark.sql(consulta).collect()
    except Exception as exc:  # noqa: BLE001 - cualquier fallo degrada a lookback
        log.warning("CDF no disponible sobre %s (%s); se usa lookback_days", tabla, exc)
        return None
    fechas = {r["fecha"] for r in filas if r["fecha"] is not None}
    log.info("CDF: %s fecha(s) de consumo cambiadas desde %s", len(fechas), since)
    return fechas
