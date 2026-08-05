"""Reglas de alerta: convierten hechos del modelo gold en eventos accionables.

Cada regla produce objetos `Alert` con:

  * `fingerprint`  huella estable que identifica *la misma condicion* a lo largo
    del tiempo. Es la base de la deduplicacion: mientras la condicion persista,
    no se vuelve a notificar dentro del periodo de enfriamiento.
  * `severity`     low | medium | high | critical
  * `message`      texto listo para humanos, con la cifra y la accion sugerida.

Funciones puras: reciben listas de dicts / dataclasses ya materializadas.
"""

from __future__ import annotations

import hashlib
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timezone
from typing import Any

SEVERITY_ORDER = {"low": 0, "medium": 1, "high": 2, "critical": 3}


@dataclass
class Alert:
    """Evento de alerta listo para despacho."""

    rule_id: str
    severity: str
    title: str
    message: str
    fingerprint: str
    scope: str
    entity_key: str | None = None
    metric_value: float | None = None
    threshold_value: float | None = None
    owner_email: str | None = None
    event_date: date | None = None
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    context: dict[str, Any] = field(default_factory=dict)

    def to_row(self) -> dict[str, Any]:
        fila = asdict(self)
        fila["context"] = {k: str(v) for k, v in self.context.items()}
        return fila

    @property
    def severity_rank(self) -> int:
        return SEVERITY_ORDER.get(self.severity, 0)


def fingerprint(*parts: Any) -> str:
    """Huella estable de 16 caracteres a partir de las partes que definen la condicion."""
    crudo = "|".join("" if p is None else str(p) for p in parts)
    return hashlib.sha256(crudo.encode("utf-8")).hexdigest()[:16]


def meets_severity(severity: str, minimum: str) -> bool:
    return SEVERITY_ORDER.get(severity, 0) >= SEVERITY_ORDER.get(minimum, 0)


def _money(value: float) -> str:
    return f"USD {value:,.2f}"


# ---------------------------------------------------------------------------
# Presupuestos
# ---------------------------------------------------------------------------
def budget_alerts(
    statuses: list[Any], cfg: dict[str, Any] | None = None, *, as_of: date | None = None
) -> list[Alert]:
    """Alerta cuando un presupuesto cruza uno de sus umbrales configurados.

    La huella incluye el periodo y el umbral alcanzado, de forma que cruzar el
    80% notifica una vez y cruzar luego el 100% vuelve a notificar.
    """
    reglas = (cfg or {}).get("budget_threshold", {}) or {}
    if not reglas.get("enabled", True):
        return []
    umbrales_default = [float(t) for t in (reglas.get("thresholds_pct") or [50, 80, 90, 100])]

    salida: list[Alert] = []
    for estado in statuses:
        alcanzado = getattr(estado, "threshold_reached_pct", None)
        if alcanzado is None:
            continue
        configurados = estado.details.get("thresholds_pct") or umbrales_default
        if float(alcanzado) not in [float(t) for t in configurados]:
            continue

        consumido = float(estado.consumed_pct)
        if consumido >= 100:
            severidad = "critical"
        elif consumido >= 90:
            severidad = "high"
        elif consumido >= 80:
            severidad = "medium"
        else:
            severidad = "low"

        etiqueta_periodo = estado.details.get("period_label", str(estado.period_start))
        salida.append(
            Alert(
                rule_id="BUDGET_THRESHOLD",
                severity=severidad,
                title=f"Presupuesto '{estado.budget_name}' al {consumido:.0f}%",
                message=(
                    f"El presupuesto '{estado.budget_name}' ({estado.scope_label}) del periodo "
                    f"{etiqueta_periodo} alcanzo el {consumido:.1f}% "
                    f"({_money(estado.actual_cost_usd)} de {_money(estado.budget_amount_usd)}). "
                    f"Proyeccion de cierre: {_money(estado.projected_total_usd)} "
                    f"({estado.projected_pct:.1f}%). Estado: {estado.status}."
                ),
                fingerprint=fingerprint("BUDGET_THRESHOLD", estado.budget_id, etiqueta_periodo, alcanzado),
                scope=estado.scope_label,
                metric_value=round(consumido, 2),
                threshold_value=float(alcanzado),
                owner_email=estado.owner_email,
                event_date=as_of or estado.as_of_date,
                context={
                    "budget_id": estado.budget_id,
                    "period": etiqueta_periodo,
                    "actual_cost_usd": estado.actual_cost_usd,
                    "budget_amount_usd": estado.budget_amount_usd,
                    "projected_total_usd": estado.projected_total_usd,
                    "status": estado.status,
                },
            )
        )
    return salida


def forecast_overrun_alerts(
    statuses: list[Any], cfg: dict[str, Any] | None = None, *, as_of: date | None = None
) -> list[Alert]:
    """Alerta preventiva: el pronostico supera el presupuesto aunque aun no se haya excedido."""
    regla = (cfg or {}).get("forecast_overrun", {}) or {}
    if not regla.get("enabled", True):
        return []
    exceso_min = float(regla.get("pct_over_budget", 0.05))

    salida: list[Alert] = []
    for estado in statuses:
        if estado.budget_amount_usd <= 0 or estado.consumed_pct >= 100:
            continue  # ya excedido: lo cubre BUDGET_THRESHOLD
        exceso = (estado.projected_total_usd / estado.budget_amount_usd) - 1.0
        if exceso < exceso_min:
            continue
        etiqueta_periodo = estado.details.get("period_label", str(estado.period_start))
        salida.append(
            Alert(
                rule_id="FORECAST_OVERRUN",
                severity="high" if exceso >= 0.20 else "medium",
                title=f"Proyeccion excede el presupuesto '{estado.budget_name}' en {exceso:.0%}",
                message=(
                    f"Con el consumo actual, '{estado.budget_name}' ({estado.scope_label}) cerrara el periodo "
                    f"{etiqueta_periodo} en {_money(estado.projected_total_usd)} frente a un presupuesto de "
                    f"{_money(estado.budget_amount_usd)} ({exceso:+.1%}). "
                    f"Para no excederlo el gasto diario debe bajar a {_money(estado.required_daily_cost_usd)} "
                    f"(hoy: {_money(estado.avg_daily_cost_usd)})."
                ),
                fingerprint=fingerprint("FORECAST_OVERRUN", estado.budget_id, etiqueta_periodo),
                scope=estado.scope_label,
                metric_value=round(estado.projected_total_usd, 2),
                threshold_value=round(estado.budget_amount_usd, 2),
                owner_email=estado.owner_email,
                event_date=as_of or estado.as_of_date,
                context={
                    "budget_id": estado.budget_id,
                    "period": etiqueta_periodo,
                    "projected_pct": estado.projected_pct,
                    "required_daily_cost_usd": estado.required_daily_cost_usd,
                },
            )
        )
    return salida


# ---------------------------------------------------------------------------
# Anomalias
# ---------------------------------------------------------------------------
def anomaly_alerts(anomalies: list[Any], cfg: dict[str, Any] | None = None) -> list[Alert]:
    """Convierte anomalias detectadas en alertas, filtrando por severidad minima."""
    regla = (cfg or {}).get("cost_anomaly", {}) or {}
    if not regla.get("enabled", True):
        return []
    minima = str(regla.get("min_severity", "high"))

    salida: list[Alert] = []
    for anomalia in anomalies:
        if not meets_severity(anomalia.severity, minima):
            continue
        if anomalia.direction != "SPIKE":
            continue  # las caidas se registran en la tabla pero no se notifican
        salida.append(
            Alert(
                rule_id="COST_ANOMALY",
                severity=anomalia.severity,
                title=f"Costo anomalo en {anomalia.dimension}={anomalia.dimension_value}",
                message=(
                    f"El {anomalia.usage_date.isoformat()} el costo de {anomalia.dimension}="
                    f"{anomalia.dimension_value} fue {_money(anomalia.actual_cost_usd)}, "
                    f"{anomalia.pct_change:+.0%} respecto a lo esperado "
                    f"({_money(anomalia.expected_cost_usd)}). Desviacion: "
                    f"{_money(anomalia.deviation_usd)}, score {anomalia.score:.1f}."
                ),
                fingerprint=fingerprint(
                    "COST_ANOMALY", anomalia.dimension, anomalia.dimension_value, anomalia.usage_date
                ),
                scope=anomalia.series_key,
                entity_key=anomalia.dimension_value if anomalia.dimension == "entity_key" else None,
                metric_value=anomalia.actual_cost_usd,
                threshold_value=anomalia.expected_cost_usd,
                event_date=anomalia.usage_date,
                context={
                    "score": anomalia.score,
                    "pct_change": anomalia.pct_change,
                    "method": anomalia.method,
                    "baseline_points": anomalia.baseline_points,
                },
            )
        )
    return salida


# ---------------------------------------------------------------------------
# Series de costo
# ---------------------------------------------------------------------------
def daily_spike_alerts(
    daily_totals: list[tuple[date, float]], cfg: dict[str, Any] | None = None, *, scope: str = "ORGANIZACION"
) -> list[Alert]:
    """Alerta por salto del gasto total diario respecto al promedio de la semana previa."""
    regla = (cfg or {}).get("daily_spend_spike", {}) or {}
    if not regla.get("enabled", True) or len(daily_totals) < 8:
        return []
    incremento_min = float(regla.get("pct_increase", 0.35))
    costo_min = float(regla.get("min_cost_usd", 200.0))

    ordenados = sorted(daily_totals, key=lambda p: p[0])
    fecha, valor = ordenados[-1]
    if valor < costo_min:
        return []
    previos = [v for _, v in ordenados[-8:-1]]
    base = sum(previos) / len(previos) if previos else 0.0
    if base <= 0:
        return []
    variacion = (valor - base) / base
    if variacion < incremento_min:
        return []

    return [
        Alert(
            rule_id="DAILY_SPEND_SPIKE",
            severity="critical" if variacion >= 1.0 else "high",
            title=f"Gasto diario {variacion:+.0%} sobre la semana previa",
            message=(
                f"El gasto del {fecha.isoformat()} fue {_money(valor)}, {variacion:+.0%} frente al promedio "
                f"de los 7 dias previos ({_money(base)}). Revisar el detalle por equipo y entidad."
            ),
            fingerprint=fingerprint("DAILY_SPEND_SPIKE", scope, fecha),
            scope=scope,
            metric_value=round(valor, 2),
            threshold_value=round(base * (1 + incremento_min), 2),
            event_date=fecha,
            context={"baseline_7d_avg_usd": round(base, 2), "pct_increase": round(variacion, 4)},
        )
    ]


def new_expensive_entity_alerts(
    entities: list[dict[str, Any]], cfg: dict[str, Any] | None = None, *, as_of: date | None = None
) -> list[Alert]:
    """Alerta por entidades nuevas que ya representan un costo relevante.

    Espera dicts con: entity_key, entity_name, entity_type, cost_usd,
    first_seen_date, team, owner.
    """
    regla = (cfg or {}).get("new_expensive_entity", {}) or {}
    if not regla.get("enabled", True):
        return []
    costo_min = float(regla.get("min_cost_usd", 150.0))
    ventana = int(regla.get("lookback_days", 7))
    referencia = as_of or date.today()

    salida: list[Alert] = []
    for entidad in entities:
        primera_vez = entidad.get("first_seen_date")
        if not isinstance(primera_vez, date):
            continue
        if (referencia - primera_vez).days > ventana:
            continue
        costo = float(entidad.get("cost_usd") or 0.0)
        if costo < costo_min:
            continue
        nombre = entidad.get("entity_name") or entidad.get("entity_key")
        salida.append(
            Alert(
                rule_id="NEW_EXPENSIVE_ENTITY",
                severity="high" if costo >= costo_min * 3 else "medium",
                title=f"Nuevo recurso costoso: {nombre}",
                message=(
                    f"El recurso {nombre} ({entidad.get('entity_type')}) aparecio el "
                    f"{primera_vez.isoformat()} y ya acumula {_money(costo)}. "
                    f"Responsable: {entidad.get('owner') or entidad.get('team') or 'sin identificar'}. "
                    "Verificar que sea un consumo previsto y que tenga etiquetas de atribucion."
                ),
                fingerprint=fingerprint("NEW_EXPENSIVE_ENTITY", entidad.get("entity_key"), primera_vez),
                scope=str(entidad.get("team") or "ORGANIZACION"),
                entity_key=str(entidad.get("entity_key")),
                metric_value=round(costo, 2),
                threshold_value=costo_min,
                owner_email=entidad.get("owner"),
                event_date=referencia,
                context={"first_seen_date": primera_vez, "entity_type": entidad.get("entity_type")},
            )
        )
    return salida


# ---------------------------------------------------------------------------
# Gobierno y salud de la plataforma
# ---------------------------------------------------------------------------
def tag_coverage_alerts(
    coverage: dict[str, Any], cfg: dict[str, Any] | None = None, *, as_of: date | None = None
) -> list[Alert]:
    """Alerta cuando la cobertura de etiquetado cae por debajo del minimo."""
    regla = (cfg or {}).get("tag_coverage_drop", {}) or {}
    if not regla.get("enabled", True):
        return []
    minimo = float(regla.get("min_coverage_ratio", 0.80))
    ratio = float(coverage.get("coverage_ratio", 1.0))
    if ratio >= minimo:
        return []

    dimension = coverage.get("dimension", "cost_center")
    sin_atribuir = float(coverage.get("unattributed_cost_usd") or 0.0)
    referencia = as_of or date.today()
    return [
        Alert(
            rule_id="TAG_COVERAGE_DROP",
            severity="high" if ratio < minimo * 0.75 else "medium",
            title=f"Cobertura de '{dimension}' en {ratio:.0%}",
            message=(
                f"Solo el {ratio:.1%} del costo tiene la dimension '{dimension}' resuelta "
                f"(minimo esperado {minimo:.0%}). Costo sin atribuir: {_money(sin_atribuir)}. "
                "Revisar politicas de cluster y plantillas de job para forzar el etiquetado."
            ),
            fingerprint=fingerprint("TAG_COVERAGE_DROP", dimension, referencia.strftime("%Y-%m")),
            scope=f"dimension={dimension}",
            metric_value=round(ratio, 4),
            threshold_value=minimo,
            event_date=referencia,
            context={"unattributed_cost_usd": round(sin_atribuir, 2), "dimension": dimension},
        )
    ]


def pipeline_health_alerts(
    run_metrics: list[dict[str, Any]], cfg: dict[str, Any] | None = None, *, run_id: str = ""
) -> list[Alert]:
    """Alerta si alguna etapa del propio pipeline FinOps fallo."""
    regla = (cfg or {}).get("pipeline_health", {}) or {}
    if not regla.get("enabled", True):
        return []
    fallidas = [m for m in run_metrics if m.get("status") == "error"]
    if not fallidas:
        return []
    nombres = ", ".join(str(m.get("stage")) for m in fallidas)
    return [
        Alert(
            rule_id="PIPELINE_HEALTH",
            severity="critical",
            title=f"Pipeline FinOps con {len(fallidas)} etapa(s) fallida(s)",
            message=(
                f"La corrida {run_id} fallo en: {nombres}. "
                f"Primer error: {fallidas[0].get('error_message')}"
            ),
            fingerprint=fingerprint("PIPELINE_HEALTH", run_id, nombres),
            scope="PLATAFORMA",
            metric_value=float(len(fallidas)),
            event_date=date.today(),
            context={"run_id": run_id, "failed_stages": nombres},
        )
    ]


def quality_alerts(failures: list[dict[str, Any]], *, run_id: str = "") -> list[Alert]:
    """Alerta por chequeos de calidad de datos fallidos."""
    if not failures:
        return []
    criticos = [f for f in failures if str(f.get("severity", "error")) == "error"]
    if not criticos:
        return []
    nombres = ", ".join(str(f.get("check")) for f in criticos)
    return [
        Alert(
            rule_id="DATA_QUALITY",
            severity="high",
            title=f"{len(criticos)} chequeo(s) de calidad fallidos",
            message=(
                f"Los siguientes chequeos fallaron en la corrida {run_id}: {nombres}. "
                "Las cifras del dashboard pueden estar incompletas hasta resolverlos."
            ),
            fingerprint=fingerprint("DATA_QUALITY", run_id, nombres),
            scope="PLATAFORMA",
            metric_value=float(len(criticos)),
            event_date=date.today(),
            context={"run_id": run_id, "failed_checks": nombres},
        )
    ]


# ---------------------------------------------------------------------------
# Orquestacion de reglas
# ---------------------------------------------------------------------------
def build_all(
    *,
    budget_statuses: list[Any] | None = None,
    anomalies: list[Any] | None = None,
    daily_totals: list[tuple[date, float]] | None = None,
    new_entities: list[dict[str, Any]] | None = None,
    coverage: dict[str, Any] | None = None,
    run_metrics: list[dict[str, Any]] | None = None,
    quality_failures: list[dict[str, Any]] | None = None,
    alerting_cfg: dict[str, Any] | None = None,
    as_of: date | None = None,
    run_id: str = "",
) -> list[Alert]:
    """Ejecuta todas las reglas habilitadas y devuelve las alertas ordenadas."""
    reglas_cfg = (alerting_cfg or {}).get("rules", {}) or {}
    alertas: list[Alert] = []

    if budget_statuses:
        alertas += budget_alerts(budget_statuses, reglas_cfg, as_of=as_of)
        alertas += forecast_overrun_alerts(budget_statuses, reglas_cfg, as_of=as_of)
    if anomalies:
        alertas += anomaly_alerts(anomalies, reglas_cfg)
    if daily_totals:
        alertas += daily_spike_alerts(daily_totals, reglas_cfg)
    if new_entities:
        alertas += new_expensive_entity_alerts(new_entities, reglas_cfg, as_of=as_of)
    if coverage:
        alertas += tag_coverage_alerts(coverage, reglas_cfg, as_of=as_of)
    if run_metrics:
        alertas += pipeline_health_alerts(run_metrics, reglas_cfg, run_id=run_id)
    if quality_failures:
        alertas += quality_alerts(quality_failures, run_id=run_id)

    alertas.sort(key=lambda a: (-a.severity_rank, a.rule_id))
    return alertas
