"""Canales de notificacion y formateo de mensajes.

El formateo (funciones `format_*`) es puro y probado unitariamente. El envio
(`WebhookChannel.send`) usa `requests`, disponible en el runtime de Databricks.

Ningun secreto se escribe en logs ni en la tabla de alertas: la URL del webhook
se resuelve desde un scope de secretos de Databricks en el momento del envio.
"""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Any, Protocol

from ..errors import AlertDeliveryError
from ..logging_utils import get_logger
from .rules import Alert, meets_severity

log = get_logger("alerting")

_SEVERITY_COLOR = {
    "critical": "D13438",
    "high": "F7630C",
    "medium": "FFB900",
    "low": "0078D4",
}
_SEVERITY_EMOJI = {
    "critical": ":rotating_light:",
    "high": ":warning:",
    "medium": ":large_yellow_circle:",
    "low": ":information_source:",
}
_SEVERITY_ES = {
    "critical": "CRITICA",
    "high": "ALTA",
    "medium": "MEDIA",
    "low": "BAJA",
}


# ---------------------------------------------------------------------------
# Formateo
# ---------------------------------------------------------------------------
def format_plain(alert: Alert, *, env: str = "", workspace_url: str = "") -> str:
    """Texto plano, usado por el canal de correo y como respaldo."""
    lineas = [
        f"[{_SEVERITY_ES.get(alert.severity, alert.severity.upper())}] {alert.title}",
        "",
        alert.message,
        "",
        f"Regla: {alert.rule_id}",
        f"Ambito: {alert.scope}",
    ]
    if alert.event_date:
        lineas.append(f"Fecha: {alert.event_date.isoformat()}")
    if env:
        lineas.append(f"Entorno: {env.upper()}")
    if workspace_url:
        lineas.append(f"Workspace: {workspace_url}")
    return "\n".join(lineas)


def format_teams(alert: Alert, *, env: str = "", dashboard_url: str = "") -> dict[str, Any]:
    """Payload MessageCard para un webhook entrante de Microsoft Teams."""
    hechos = [
        {"name": "Severidad", "value": _SEVERITY_ES.get(alert.severity, alert.severity)},
        {"name": "Regla", "value": alert.rule_id},
        {"name": "Ambito", "value": alert.scope},
    ]
    if alert.event_date:
        hechos.append({"name": "Fecha", "value": alert.event_date.isoformat()})
    if alert.metric_value is not None:
        hechos.append({"name": "Valor", "value": f"{alert.metric_value:,.2f}"})
    if alert.threshold_value is not None:
        hechos.append({"name": "Umbral", "value": f"{alert.threshold_value:,.2f}"})
    if env:
        hechos.append({"name": "Entorno", "value": env.upper()})

    tarjeta: dict[str, Any] = {
        "@type": "MessageCard",
        "@context": "https://schema.org/extensions",
        "summary": alert.title,
        "themeColor": _SEVERITY_COLOR.get(alert.severity, "0078D4"),
        "title": alert.title,
        "sections": [{"activityTitle": "FinOps Databricks", "text": alert.message, "facts": hechos}],
    }
    if dashboard_url:
        tarjeta["potentialAction"] = [
            {
                "@type": "OpenUri",
                "name": "Abrir dashboard FinOps",
                "targets": [{"os": "default", "uri": dashboard_url}],
            }
        ]
    return tarjeta


def format_slack(alert: Alert, *, env: str = "", dashboard_url: str = "") -> dict[str, Any]:
    """Payload Block Kit para un webhook entrante de Slack."""
    campos = [
        {"type": "mrkdwn", "text": f"*Severidad:*\n{_SEVERITY_ES.get(alert.severity, alert.severity)}"},
        {"type": "mrkdwn", "text": f"*Regla:*\n{alert.rule_id}"},
        {"type": "mrkdwn", "text": f"*Ambito:*\n{alert.scope}"},
    ]
    if alert.event_date:
        campos.append({"type": "mrkdwn", "text": f"*Fecha:*\n{alert.event_date.isoformat()}"})
    if env:
        campos.append({"type": "mrkdwn", "text": f"*Entorno:*\n{env.upper()}"})

    bloques: list[dict[str, Any]] = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"{_SEVERITY_EMOJI.get(alert.severity, '')} {alert.title}"[:150],
                "emoji": True,
            },
        },
        {"type": "section", "text": {"type": "mrkdwn", "text": alert.message}},
        {"type": "section", "fields": campos[:10]},
    ]
    if dashboard_url:
        bloques.append(
            {
                "type": "actions",
                "elements": [
                    {
                        "type": "button",
                        "text": {"type": "plain_text", "text": "Abrir dashboard FinOps"},
                        "url": dashboard_url,
                    }
                ],
            }
        )
    return {"text": alert.title, "blocks": bloques}


def format_digest(alerts: list[Alert], *, env: str = "") -> str:
    """Resumen agrupado, util cuando se supera el limite de alertas por corrida."""
    por_severidad: dict[str, int] = {}
    for alerta in alerts:
        por_severidad[alerta.severity] = por_severidad.get(alerta.severity, 0) + 1
    conteo = ", ".join(
        f"{_SEVERITY_ES.get(s, s)}: {n}" for s, n in sorted(por_severidad.items(), key=lambda kv: -len(kv[0]))
    )
    lineas = [f"Resumen FinOps{f' [{env.upper()}]' if env else ''}: {len(alerts)} alertas ({conteo})", ""]
    lineas.extend(f"- [{_SEVERITY_ES.get(a.severity, a.severity)}] {a.title}" for a in alerts[:25])
    if len(alerts) > 25:
        lineas.append(f"... y {len(alerts) - 25} mas. Ver la tabla ops_alert_log.")
    return "\n".join(lineas)


FORMATTERS = {"teams": format_teams, "slack": format_slack, "plain": format_plain}


# ---------------------------------------------------------------------------
# Canales
# ---------------------------------------------------------------------------
@dataclass
class DeliveryResult:
    """Resultado del intento de entrega de una alerta por un canal."""

    channel: str
    fingerprint: str
    delivered: bool
    detail: str = ""


class Channel(Protocol):
    """Contrato de un canal de notificacion."""

    name: str
    min_severity: str

    def send(self, alert: Alert) -> DeliveryResult: ...


class NoopChannel:
    """Canal que registra en log sin enviar nada. Util en dev y dry-run."""

    def __init__(self, name: str = "noop", min_severity: str = "low") -> None:
        self.name = name
        self.min_severity = min_severity

    def send(self, alert: Alert) -> DeliveryResult:
        log.info("[noop:%s] %s | %s", self.name, alert.severity.upper(), alert.title)
        return DeliveryResult(self.name, alert.fingerprint, True, "no enviado (canal noop)")


class WebhookChannel:
    """Canal webhook (Teams / Slack / generico) con reintentos exponenciales."""

    def __init__(
        self,
        name: str,
        url: str,
        *,
        fmt: str = "teams",
        min_severity: str = "high",
        env: str = "",
        dashboard_url: str = "",
        timeout: int = 15,
        max_retries: int = 3,
    ) -> None:
        if not url:
            raise AlertDeliveryError(f"Canal '{name}' sin URL resuelta")
        self.name = name
        self._url = url
        self.fmt = fmt
        self.min_severity = min_severity
        self.env = env
        self.dashboard_url = dashboard_url
        self.timeout = timeout
        self.max_retries = max_retries

    def build_payload(self, alert: Alert) -> dict[str, Any]:
        if self.fmt == "slack":
            return format_slack(alert, env=self.env, dashboard_url=self.dashboard_url)
        if self.fmt == "teams":
            return format_teams(alert, env=self.env, dashboard_url=self.dashboard_url)
        return {"text": format_plain(alert, env=self.env)}

    def send(self, alert: Alert) -> DeliveryResult:
        cuerpo = json.dumps(self.build_payload(alert)).encode("utf-8")
        ultimo_error = ""
        for intento in range(1, self.max_retries + 1):
            try:
                peticion = urllib.request.Request(  # noqa: S310 - URL de webhook interna configurada
                    self._url,
                    data=cuerpo,
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                with urllib.request.urlopen(peticion, timeout=self.timeout) as respuesta:  # noqa: S310
                    codigo = respuesta.status
                if 200 <= codigo < 300:
                    return DeliveryResult(self.name, alert.fingerprint, True, f"HTTP {codigo}")
                ultimo_error = f"HTTP {codigo}"
            except urllib.error.HTTPError as exc:
                ultimo_error = f"HTTP {exc.code}"
                if 400 <= exc.code < 500 and exc.code != 429:
                    break  # error de cliente: reintentar no ayuda
            except Exception as exc:  # noqa: BLE001
                ultimo_error = f"{type(exc).__name__}: {exc}"
            if intento < self.max_retries:
                time.sleep(2**intento)
        log.warning("Canal '%s' no pudo entregar '%s': %s", self.name, alert.title, ultimo_error)
        return DeliveryResult(self.name, alert.fingerprint, False, ultimo_error)


class TableChannel:
    """Canal que solo persiste la alerta (el escritor real es el dispatcher)."""

    def __init__(self, name: str = "tabla", min_severity: str = "low") -> None:
        self.name = name
        self.min_severity = min_severity

    def send(self, alert: Alert) -> DeliveryResult:
        return DeliveryResult(self.name, alert.fingerprint, True, "persistida en ops_alert_log")


def should_route(alert: Alert, channel: Channel) -> bool:
    """True si la alerta cumple la severidad minima del canal."""
    return meets_severity(alert.severity, getattr(channel, "min_severity", "low"))


def build_channels(
    channels_cfg: list[dict[str, Any]] | None,
    *,
    secret_resolver: Any = None,
    env: str = "",
    dashboard_url: str = "",
    dry_run: bool = False,
) -> list[Channel]:
    """Instancia los canales habilitados a partir de la configuracion.

    Args:
        secret_resolver: callable(scope, key) -> str. En Databricks se pasa
            `dbutils.secrets.get`. Si un secreto no resuelve, el canal se omite
            con una advertencia en lugar de tumbar la corrida.
    """
    canales: list[Channel] = []
    for definicion in channels_cfg or []:
        if not isinstance(definicion, dict) or not definicion.get("enabled", False):
            continue
        nombre = str(definicion.get("name", "sin_nombre"))
        tipo = str(definicion.get("type", "noop"))
        min_sev = str(definicion.get("min_severity", "low"))

        if dry_run:
            canales.append(NoopChannel(nombre, min_sev))
            continue
        if tipo == "table":
            canales.append(TableChannel(nombre, min_sev))
            continue
        if tipo == "noop":
            canales.append(NoopChannel(nombre, min_sev))
            continue
        if tipo == "webhook":
            url = definicion.get("url")
            if not url and secret_resolver is not None:
                try:
                    url = secret_resolver(definicion.get("secret_scope"), definicion.get("secret_key"))
                except Exception as exc:  # noqa: BLE001
                    log.warning("Canal '%s' omitido: no se pudo leer el secreto (%s)", nombre, exc)
                    continue
            if not url:
                log.warning("Canal '%s' omitido: sin URL ni secreto disponible", nombre)
                continue
            canales.append(
                WebhookChannel(
                    nombre,
                    str(url),
                    fmt=str(definicion.get("format", "teams")),
                    min_severity=min_sev,
                    env=env,
                    dashboard_url=dashboard_url,
                )
            )
            continue
        log.warning("Tipo de canal no soportado '%s' en '%s'", tipo, nombre)
    return canales
