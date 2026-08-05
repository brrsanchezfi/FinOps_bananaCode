"""Chequeos de calidad sobre el modelo FinOps.

La evaluacion se separa en dos capas:

  * `evaluate_*`  funciones puras que reciben metricas ya calculadas y deciden
    si el chequeo pasa. Se prueban sin Spark.
  * `run_checks`  adaptador que calcula esas metricas con Spark y aplica las
    funciones puras.

Severidades: ``error`` bloquea el pipeline (si `fail_pipeline_on_error`),
``warning`` solo se registra.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from typing import TYPE_CHECKING, Any

from ..config import FinOpsConfig
from ..errors import DataQualityError
from ..logging_utils import get_logger

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import SparkSession

log = get_logger("quality")

SEVERITY_ERROR = "error"
SEVERITY_WARNING = "warning"


@dataclass
class CheckResult:
    """Resultado de un chequeo individual."""

    check: str
    table: str
    passed: bool
    severity: str
    observed: float | None
    threshold: float | None
    message: str
    checked_at: datetime = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        if self.checked_at is None:
            self.checked_at = datetime.now(timezone.utc)

    def to_row(self) -> dict[str, Any]:
        return asdict(self)


# ---------------------------------------------------------------------------
# Evaluadores puros
# ---------------------------------------------------------------------------
def evaluate_freshness(
    max_usage_date: date | None, as_of: date, max_lag_hours: int, table: str = ""
) -> CheckResult:
    """La ventana de datos debe estar dentro del retraso aceptable.

    `system.billing.usage` publica con horas de retraso; un rezago mayor al
    esperado indica un problema de ingesta o de permisos, no un dia sin consumo.
    """
    if max_usage_date is None:
        return CheckResult(
            "freshness", table, False, SEVERITY_ERROR, None, float(max_lag_hours),
            "No hay datos de consumo en la tabla",
        )
    rezago_horas = (as_of - max_usage_date).days * 24
    ok = rezago_horas <= max_lag_hours
    return CheckResult(
        "freshness", table, ok, SEVERITY_ERROR, float(rezago_horas), float(max_lag_hours),
        f"Ultimo dia con datos {max_usage_date.isoformat()} ({rezago_horas}h de rezago, maximo {max_lag_hours}h)",
    )


def evaluate_null_ratio(
    null_rows: int, total_rows: int, max_ratio: float, column: str, table: str = ""
) -> CheckResult:
    ratio = (null_rows / total_rows) if total_rows > 0 else 0.0
    ok = ratio <= max_ratio
    return CheckResult(
        f"null_ratio.{column}", table, ok, SEVERITY_ERROR, round(ratio, 6), max_ratio,
        f"{null_rows:,} de {total_rows:,} filas con '{column}' nulo ({ratio:.4%}, maximo {max_ratio:.4%})",
    )


def evaluate_row_count(total_rows: int, min_rows: int, table: str = "") -> CheckResult:
    ok = total_rows >= min_rows
    return CheckResult(
        "row_count", table, ok, SEVERITY_ERROR, float(total_rows), float(min_rows),
        f"{total_rows:,} filas en la ventana procesada (minimo {min_rows:,})",
    )


def evaluate_negative_cost(negative_rows: int, max_rows: int, table: str = "") -> CheckResult:
    ok = negative_rows <= max_rows
    return CheckResult(
        "negative_cost", table, ok, SEVERITY_ERROR, float(negative_rows), float(max_rows),
        f"{negative_rows:,} filas con costo negativo (maximo tolerado {max_rows:,})",
    )


def evaluate_price_match(
    priced_rows: int, total_rows: int, min_ratio: float, table: str = ""
) -> CheckResult:
    """Fraccion de registros de consumo que encontraron precio de lista.

    Un SKU nuevo sin precio publicado produce costo 0 y subestima el gasto; por
    eso este chequeo es de severidad error y no solo informativo.
    """
    ratio = (priced_rows / total_rows) if total_rows > 0 else 1.0
    ok = ratio >= min_ratio
    return CheckResult(
        "price_match", table, ok, SEVERITY_ERROR, round(ratio, 6), min_ratio,
        f"{priced_rows:,} de {total_rows:,} registros con precio resuelto "
        f"({ratio:.2%}, minimo {min_ratio:.2%})",
    )


def evaluate_tag_coverage(coverage_ratio: float, min_ratio: float, dimension: str, table: str = "") -> CheckResult:
    ok = coverage_ratio >= min_ratio
    return CheckResult(
        f"tag_coverage.{dimension}", table, ok, SEVERITY_WARNING, round(coverage_ratio, 6), min_ratio,
        f"Cobertura de '{dimension}': {coverage_ratio:.2%} (minimo esperado {min_ratio:.2%})",
    )


def evaluate_duplicates(duplicate_keys: int, table: str = "", key_desc: str = "clave") -> CheckResult:
    ok = duplicate_keys == 0
    return CheckResult(
        "duplicates", table, ok, SEVERITY_ERROR, float(duplicate_keys), 0.0,
        f"{duplicate_keys:,} {key_desc}(s) duplicada(s)",
    )


def summarize(results: list[CheckResult]) -> dict[str, Any]:
    fallidos = [r for r in results if not r.passed]
    bloqueantes = [r for r in fallidos if r.severity == SEVERITY_ERROR]
    return {
        "total": len(results),
        "passed": len(results) - len(fallidos),
        "failed": len(fallidos),
        "blocking": len(bloqueantes),
        "failed_checks": [r.check for r in fallidos],
    }


def enforce(results: list[CheckResult], fail_on_error: bool) -> None:
    """Lanza DataQualityError si hay chequeos bloqueantes fallidos."""
    bloqueantes = [r for r in results if not r.passed and r.severity == SEVERITY_ERROR]
    if not bloqueantes:
        return
    mensajes = [f"{r.check} [{r.table}]: {r.message}" for r in bloqueantes]
    if fail_on_error:
        raise DataQualityError(mensajes)
    for mensaje in mensajes:
        log.warning("Calidad (no bloqueante): %s", mensaje)


# ---------------------------------------------------------------------------
# Adaptador Spark
# ---------------------------------------------------------------------------
def run_checks(spark: SparkSession, cfg: FinOpsConfig) -> list[CheckResult]:
    """Ejecuta la bateria de chequeos sobre silver y gold."""
    from pyspark.sql import functions as F

    from ..catalog import GOLD_COST_DAILY, SLV_USAGE_PRICED
    from ..spark_utils import table_exists

    checks_cfg = cfg.get("quality.checks", {}) or {}
    resultados: list[CheckResult] = []

    silver_fqn = SLV_USAGE_PRICED.fqn(cfg)
    if not table_exists(spark, silver_fqn):
        return [
            CheckResult(
                "table_exists", silver_fqn, False, SEVERITY_ERROR, 0.0, 1.0,
                "La tabla silver de consumo valorizado no existe",
            )
        ]

    ventana = spark.table(silver_fqn).filter(F.col("usage_date").between(cfg.min_date, cfg.max_date))
    agregado = ventana.agg(
        F.count("*").alias("total"),
        F.max("usage_date").alias("max_date"),
        F.sum(F.when(F.col("total_cost_usd").isNull(), 1).otherwise(0)).alias("null_cost"),
        F.sum(F.when(F.col("total_cost_usd") < 0, 1).otherwise(0)).alias("negative_cost"),
        F.sum(F.when(~F.col("price_missing"), 1).otherwise(0)).alias("priced"),
    ).collect()[0]

    total = int(agregado["total"] or 0)
    resultados.append(evaluate_row_count(total, int(checks_cfg.get("min_daily_rows", 1)), silver_fqn))
    resultados.append(
        evaluate_freshness(
            agregado["max_date"], cfg.max_date, int(checks_cfg.get("freshness_max_lag_hours", 36)), silver_fqn
        )
    )
    resultados.append(
        evaluate_null_ratio(
            int(agregado["null_cost"] or 0), total,
            float(checks_cfg.get("max_null_ratio_cost", 0.001)), "total_cost_usd", silver_fqn,
        )
    )
    resultados.append(
        evaluate_negative_cost(
            int(agregado["negative_cost"] or 0), int(checks_cfg.get("max_negative_cost_rows", 0)), silver_fqn
        )
    )
    resultados.append(
        evaluate_price_match(
            int(agregado["priced"] or 0), total, float(checks_cfg.get("price_match_min_ratio", 0.98)), silver_fqn
        )
    )

    gold_fqn = GOLD_COST_DAILY.fqn(cfg)
    if table_exists(spark, gold_fqn):
        gold = spark.table(gold_fqn).filter(F.col("usage_date").between(cfg.min_date, cfg.max_date))
        sin_asignar = str(cfg.get("tagging.unallocated_value", "SIN_ASIGNAR"))
        minimo_cobertura = float(cfg.get("tagging.min_coverage_ratio", 0.80))
        for dimension in ("cost_center", "team"):
            if dimension not in gold.columns:
                continue
            fila = gold.agg(
                F.sum("total_cost_usd").alias("total"),
                F.sum(F.when(F.col(dimension) != sin_asignar, F.col("total_cost_usd")).otherwise(0.0)).alias("attr"),
            ).collect()[0]
            total_costo = float(fila["total"] or 0.0)
            ratio = (float(fila["attr"] or 0.0) / total_costo) if total_costo > 0 else 1.0
            resultados.append(evaluate_tag_coverage(ratio, minimo_cobertura, dimension, gold_fqn))

        duplicados = (
            gold.groupBy("usage_date", "workspace_id", "sku_name", "entity_key")
            .count()
            .filter(F.col("count") > 1)
            .count()
        )
        resultados.append(evaluate_duplicates(duplicados, gold_fqn, "combinacion fecha/workspace/sku/entidad"))

    resumen = summarize(resultados)
    log.info(
        "Calidad de datos: %s/%s aprobados, %s bloqueantes fallidos",
        resumen["passed"], resumen["total"], resumen["blocking"],
    )
    return resultados
