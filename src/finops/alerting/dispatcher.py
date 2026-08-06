"""Despacho de alertas: deduplicacion, limites y enrutamiento a canales.

Flujo:

    alertas generadas
        -> filtro de severidad global
        -> deduplicacion por huella + periodo de enfriamiento
        -> tope maximo por corrida (con resumen si se excede)
        -> enrutamiento a cada canal que cumpla su severidad minima
        -> persistencia en ops_alert_log

La deduplicacion es pura (`filter_new`), asi que se puede probar sin Spark: se
le pasa el historico de (huella, timestamp) ya leido de la tabla.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

from ..logging_utils import get_logger
from .notifier import Channel, DeliveryResult, format_digest, should_route
from .rules import Alert, meets_severity

log = get_logger("dispatcher")


@dataclass
class DispatchReport:
    """Resultado de una corrida de despacho."""

    generated: int = 0
    suppressed_severity: int = 0
    suppressed_cooldown: int = 0
    suppressed_limit: int = 0
    dispatched: int = 0
    deliveries: list[DeliveryResult] = field(default_factory=list)
    alerts: list[Alert] = field(default_factory=list)

    @property
    def failed_deliveries(self) -> list[DeliveryResult]:
        return [d for d in self.deliveries if not d.delivered]

    def summary(self) -> str:
        return (
            f"generadas={self.generated} despachadas={self.dispatched} "
            f"suprimidas(severidad={self.suppressed_severity}, "
            f"enfriamiento={self.suppressed_cooldown}, tope={self.suppressed_limit}) "
            f"entregas_fallidas={len(self.failed_deliveries)}"
        )


def filter_new(
    alerts: list[Alert],
    history: dict[str, datetime] | None,
    *,
    cooldown_hours: int = 24,
    now: datetime | None = None,
) -> tuple[list[Alert], list[Alert]]:
    """Separa alertas nuevas de las que siguen en periodo de enfriamiento.

    Args:
        history: huella -> ultimo despacho (timezone-aware o naive UTC).

    Returns:
        (nuevas, suprimidas)
    """
    ahora = now or datetime.now(timezone.utc)
    umbral = timedelta(hours=max(0, cooldown_hours))
    historico = history or {}

    nuevas: list[Alert] = []
    suprimidas: list[Alert] = []
    vistas_en_esta_corrida: set[str] = set()

    for alerta in alerts:
        if alerta.fingerprint in vistas_en_esta_corrida:
            suprimidas.append(alerta)  # duplicado dentro del mismo lote
            continue
        ultimo = historico.get(alerta.fingerprint)
        if ultimo is not None:
            if ultimo.tzinfo is None:
                ultimo = ultimo.replace(tzinfo=timezone.utc)
            if ahora - ultimo < umbral:
                suprimidas.append(alerta)
                continue
        nuevas.append(alerta)
        vistas_en_esta_corrida.add(alerta.fingerprint)

    return nuevas, suprimidas


def apply_limit(alerts: list[Alert], max_alerts: int) -> tuple[list[Alert], list[Alert]]:
    """Aplica el tope por corrida conservando las mas severas."""
    if max_alerts <= 0 or len(alerts) <= max_alerts:
        return alerts, []
    ordenadas = sorted(alerts, key=lambda a: (-a.severity_rank, a.rule_id))
    return ordenadas[:max_alerts], ordenadas[max_alerts:]


def dispatch(
    alerts: list[Alert],
    channels: list[Channel],
    *,
    history: dict[str, datetime] | None = None,
    config: dict[str, Any] | None = None,
    now: datetime | None = None,
) -> DispatchReport:
    """Ejecuta el flujo completo de despacho."""
    cfg = config or {}
    reporte = DispatchReport(generated=len(alerts))

    if not cfg.get("enabled", True):
        log.info("Alertamiento deshabilitado por configuracion; %s alertas no se despachan", len(alerts))
        reporte.suppressed_severity = len(alerts)
        return reporte

    severidad_minima = str(cfg.get("min_severity", "medium"))
    candidatas = [a for a in alerts if meets_severity(a.severity, severidad_minima)]
    reporte.suppressed_severity = len(alerts) - len(candidatas)

    nuevas, en_enfriamiento = filter_new(
        candidatas, history, cooldown_hours=int(cfg.get("cooldown_hours", 24)), now=now
    )
    reporte.suppressed_cooldown = len(en_enfriamiento)

    a_despachar, excedentes = apply_limit(nuevas, int(cfg.get("max_alerts_per_run", 50)))
    reporte.suppressed_limit = len(excedentes)
    reporte.alerts = a_despachar

    for alerta in a_despachar:
        for canal in channels:
            if not should_route(alerta, canal):
                continue
            reporte.deliveries.append(canal.send(alerta))
    reporte.dispatched = len(a_despachar)

    if excedentes:
        log.warning(
            "Se supero el tope de %s alertas por corrida; %s quedaron sin notificar (quedan en la tabla).\n%s",
            cfg.get("max_alerts_per_run", 50),
            len(excedentes),
            format_digest(excedentes),
        )

    log.info("Despacho de alertas: %s", reporte.summary())
    return reporte


def alerts_to_rows(
    alerts: list[Alert],
    deliveries: list[DeliveryResult],
    *,
    run_id: str,
    env: str,
    suppressed: list[Alert] | None = None,
) -> list[dict[str, Any]]:
    """Convierte alertas y entregas en filas para ops_alert_log."""
    por_huella: dict[str, list[DeliveryResult]] = {}
    for entrega in deliveries:
        por_huella.setdefault(entrega.fingerprint, []).append(entrega)

    filas: list[dict[str, Any]] = []
    for alerta in alerts:
        entregas = por_huella.get(alerta.fingerprint, [])
        filas.append(
            {
                **alerta.to_row(),
                "run_id": run_id,
                "pipeline_environment": env,
                "dispatch_status": "dispatched",
                "channels": ",".join(sorted({e.channel for e in entregas})),
                "delivered": all(e.delivered for e in entregas) if entregas else False,
                "delivery_detail": "; ".join(f"{e.channel}={e.detail}" for e in entregas),
            }
        )
    for alerta in suppressed or []:
        filas.append(
            {
                **alerta.to_row(),
                "run_id": run_id,
                "pipeline_environment": env,
                "dispatch_status": "suppressed",
                "channels": "",
                "delivered": False,
                "delivery_detail": "suprimida por enfriamiento o tope de corrida",
            }
        )
    return filas
