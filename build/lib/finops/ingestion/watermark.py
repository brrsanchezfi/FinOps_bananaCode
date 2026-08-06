"""Marcas de agua de ingesta.

La ventana de proceso normal es "ultimos N dias" (los registros de facturacion
llegan con retraso y deben reprocesarse), pero conviene registrar hasta donde se
avanzo en cada fuente para diagnostico y para el primer backfill.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import TYPE_CHECKING, Any

from ..catalog import OPS_WATERMARK
from ..config import FinOpsConfig
from ..logging_utils import get_logger

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import SparkSession

log = get_logger("watermark")


def compute_window(cfg: FinOpsConfig, last_watermark: date | None = None) -> tuple[date, date]:
    """Calcula la ventana [min, max] a procesar.

    Reglas:
      * full_refresh: se ignora la marca de agua y se usa `initial_load_days`.
      * sin marca de agua previa: primera carga, se usa `initial_load_days`.
      * con marca de agua: se retrocede `lookback_days` desde ella para
        recapturar los registros que llegaron tarde, sin reprocesar todo.
    """
    from datetime import timedelta

    maximo = cfg.max_date
    if bool(cfg.get("ingestion.full_refresh", False)) or last_watermark is None:
        dias = int(cfg.get("ingestion.initial_load_days", 400))
        return maximo - timedelta(days=dias), maximo

    lookback = int(cfg.get("ingestion.lookback_days", 7))
    minimo = min(last_watermark, maximo) - timedelta(days=lookback)
    return minimo, maximo


def read_watermarks(spark: SparkSession, cfg: FinOpsConfig) -> dict[str, date]:
    """Lee la ultima marca de agua por fuente. Devuelve {} si no hay tabla."""
    from pyspark.sql import functions as F

    from ..spark_utils import table_exists

    fqn = OPS_WATERMARK.fqn(cfg)
    if not table_exists(spark, fqn):
        return {}
    filas = (
        spark.table(fqn)
        .groupBy("source_key")
        .agg(F.max("watermark_date").alias("watermark_date"))
        .collect()
    )
    return {r["source_key"]: r["watermark_date"] for r in filas if r["watermark_date"] is not None}


def write_watermark(
    spark: SparkSession,
    cfg: FinOpsConfig,
    source_key: str,
    watermark_date: date,
    *,
    run_id: str,
    rows: int = 0,
    details: dict[str, Any] | None = None,
) -> None:
    """Registra la marca de agua alcanzada por una fuente."""
    from ..spark_utils import append_rows

    fila = {
        "source_key": source_key,
        "watermark_date": watermark_date,
        "rows_ingested": int(rows),
        "run_id": run_id,
        "environment": cfg.env,
        "updated_at": datetime.now(timezone.utc),
        "details": {k: str(v) for k, v in (details or {}).items()},
    }
    append_rows(spark, [fila], OPS_WATERMARK.fqn(cfg), dry_run=cfg.dry_run)
    log.debug("Marca de agua %s -> %s", source_key, watermark_date)
