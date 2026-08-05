"""Logging estructurado y medicion de etapas.

Emite lineas legibles en consola (visible en los logs de driver de Databricks) y
mantiene un registro en memoria de las metricas de cada etapa para poder
persistirlas al final de la corrida.
"""

from __future__ import annotations

import logging
import sys
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any

_LOGGER_NAME = "finops"
_CONFIGURED = False


@dataclass
class StageMetric:
    """Metrica de una etapa del pipeline."""

    stage: str
    status: str = "running"
    duration_seconds: float = 0.0
    rows: int | None = None
    details: dict[str, Any] = field(default_factory=dict)
    error: str | None = None


class RunRecorder:
    """Acumula las metricas de todas las etapas de una corrida."""

    def __init__(self) -> None:
        self.metrics: list[StageMetric] = []

    def add(self, metric: StageMetric) -> None:
        self.metrics.append(metric)

    def as_rows(self, run_id: str, env: str) -> list[dict[str, Any]]:
        return [
            {
                "run_id": run_id,
                "environment": env,
                "stage": m.stage,
                "status": m.status,
                "duration_seconds": round(m.duration_seconds, 3),
                "rows_written": m.rows,
                "details": {k: str(v) for k, v in m.details.items()},
                "error_message": m.error,
            }
            for m in self.metrics
        ]

    @property
    def failed(self) -> list[StageMetric]:
        return [m for m in self.metrics if m.status == "error"]

    def summary(self) -> str:
        lineas = []
        for m in self.metrics:
            filas = f"{m.rows:,}" if m.rows is not None else "-"
            marca = {"ok": "OK ", "error": "ERR", "skipped": "SKP"}.get(m.status, "???")
            lineas.append(f"  [{marca}] {m.stage:<38} {m.duration_seconds:7.1f}s  filas={filas}")
        return "\n".join(lineas)


def configure_logging(level: str = "INFO") -> logging.Logger:
    """Configura el logger raiz del paquete. Idempotente."""
    global _CONFIGURED
    logger = logging.getLogger(_LOGGER_NAME)
    nivel = getattr(logging, str(level).upper(), logging.INFO)
    logger.setLevel(nivel)
    if not _CONFIGURED:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(
            logging.Formatter(
                fmt="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
        )
        logger.addHandler(handler)
        logger.propagate = False
        _CONFIGURED = True
    else:
        for h in logger.handlers:
            h.setLevel(nivel)
    return logger


def get_logger(name: str | None = None) -> logging.Logger:
    """Devuelve un logger hijo del paquete."""
    if not _CONFIGURED:
        configure_logging()
    return logging.getLogger(_LOGGER_NAME if not name else f"{_LOGGER_NAME}.{name}")


@contextmanager
def stage(name: str, recorder: RunRecorder | None = None, **details: Any) -> Iterator[StageMetric]:
    """Context manager que cronometra una etapa y registra su resultado.

    Uso:
        with stage("silver.usage_priced", recorder) as m:
            m.rows = escribir(...)
    """
    log = get_logger("stage")
    metric = StageMetric(stage=name, details=dict(details))
    inicio = time.perf_counter()
    log.info("-> inicia %s", name)
    try:
        yield metric
    except Exception as exc:  # noqa: BLE001 - se re-lanza tras registrar
        metric.status = "error"
        metric.error = f"{type(exc).__name__}: {exc}"
        metric.duration_seconds = time.perf_counter() - inicio
        if recorder:
            recorder.add(metric)
        log.error("<- FALLA %s (%.1fs): %s", name, metric.duration_seconds, metric.error)
        raise
    else:
        if metric.status == "running":
            metric.status = "ok"
        metric.duration_seconds = time.perf_counter() - inicio
        if recorder:
            recorder.add(metric)
        filas = f" filas={metric.rows:,}" if metric.rows is not None else ""
        log.info("<- %s %s (%.1fs)%s", metric.status.upper(), name, metric.duration_seconds, filas)
