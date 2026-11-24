"""Logging y medicion de etapas.

El logging lo pone DKOps (loguru, configurado por su `AppLogger`): consola
visible en los logs de driver de Databricks y, una vez que el Launcher crea la
sesion, archivo en `runtime.log_dir`. Este modulo agrega lo propio de FinOps:
el registro en memoria de las metricas de cada etapa, que se persiste al final
de la corrida en `ops_run_log`.
"""

from __future__ import annotations

import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any

from DKOps.logger_config import AppLogger
from loguru import logger

_LOGGER_NAME = "finops"


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
                "pipeline_environment": env,
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


def configure_logging(level: str = "INFO") -> None:
    """Activa el logger de DKOps en consola. Idempotente: gana la primera llamada.

    El Launcher (ver `finops.governance.start_launcher`) le agrega despues el
    archivo de log; aqui solo se fija el nivel, que sale de `conf/*.yml`.
    """
    AppLogger.setup({"LOG_LEVEL": str(level).upper()}, log_filename=_LOGGER_NAME)


class _Logger:
    """Logger de DKOps (loguru) con la interfaz de `logging` que usa el paquete.

    El codigo de FinOps formatea al estilo `logging` (`log.info("x %s", y)`) y
    loguru lo hace con `{}`; este adaptador resuelve los `%` antes de delegar,
    para no reescribir cada llamada. `class_name` es el contexto que muestra el
    formato de DKOps (`finops.silver.funcion | mensaje`).
    """

    def __init__(self, name: str) -> None:
        self._log = logger.bind(class_name=name)

    def _emit(self, level: str, msg: str, args: tuple[Any, ...], exception: bool = False) -> None:
        self._log.opt(depth=2, exception=exception).log(level, str(msg) % args if args else str(msg))

    def debug(self, msg: str, *args: Any) -> None:
        self._emit("DEBUG", msg, args)

    def info(self, msg: str, *args: Any) -> None:
        self._emit("INFO", msg, args)

    def warning(self, msg: str, *args: Any) -> None:
        self._emit("WARNING", msg, args)

    def error(self, msg: str, *args: Any) -> None:
        self._emit("ERROR", msg, args)

    def exception(self, msg: str, *args: Any) -> None:
        self._emit("ERROR", msg, args, exception=True)


def get_logger(name: str | None = None) -> _Logger:
    """Devuelve un logger del paquete, con `name` como contexto."""
    return _Logger(_LOGGER_NAME if not name else f"{_LOGGER_NAME}.{name}")


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
