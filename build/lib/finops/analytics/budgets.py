"""Motor de presupuestos: consumo, ritmo, proyeccion y semaforo.

Un presupuesto define un ambito (`scope`) sobre `fct_cost_daily`, un periodo y
un monto. Para cada periodo vigente el motor calcula:

  * consumido a la fecha y % del presupuesto
  * ritmo diario y ritmo requerido para no excederlo
  * proyeccion de cierre (usando el pronostico si existe)
  * umbral alcanzado y estado: OK / WARNING / CRITICAL / EXCEEDED
  * dias estimados hasta agotar el presupuesto

Funciones puras: reciben la lista de registros de costo ya materializada.
"""

from __future__ import annotations

import fnmatch
from calendar import monthrange
from dataclasses import asdict, dataclass, field
from datetime import date, timedelta
from typing import Any

from .forecast import ForecastPoint, project_period_total

PERIODS = ("monthly", "quarterly", "yearly")

STATUS_OK = "OK"
STATUS_WARNING = "WARNING"
STATUS_CRITICAL = "CRITICAL"
STATUS_EXCEEDED = "EXCEEDED"


@dataclass
class BudgetStatus:
    """Estado calculado de un presupuesto en un periodo."""

    budget_id: str
    budget_name: str
    period: str
    period_start: date
    period_end: date
    as_of_date: date
    scope: dict[str, str]
    scope_label: str
    owner_email: str | None
    budget_amount_usd: float
    actual_cost_usd: float
    consumed_pct: float
    forecast_remaining_usd: float
    projected_total_usd: float
    projected_pct: float
    variance_usd: float
    avg_daily_cost_usd: float
    required_daily_cost_usd: float
    elapsed_days: int
    remaining_days: int
    period_progress_pct: float
    days_to_exhaustion: int | None
    threshold_reached_pct: float | None
    status: str
    is_on_track: bool
    details: dict[str, Any] = field(default_factory=dict)

    def to_row(self) -> dict[str, Any]:
        fila = asdict(self)
        fila["scope"] = {k: str(v) for k, v in self.scope.items()}
        fila["details"] = {k: str(v) for k, v in self.details.items()}
        return fila


# ---------------------------------------------------------------------------
# Periodos
# ---------------------------------------------------------------------------
def period_bounds(period: str, reference: date) -> tuple[date, date]:
    """Limites [inicio, fin] del periodo que contiene `reference`."""
    periodo = (period or "monthly").lower()
    if periodo == "yearly":
        return date(reference.year, 1, 1), date(reference.year, 12, 31)
    if periodo == "quarterly":
        trimestre = (reference.month - 1) // 3
        mes_inicio = trimestre * 3 + 1
        mes_fin = mes_inicio + 2
        return (
            date(reference.year, mes_inicio, 1),
            date(reference.year, mes_fin, monthrange(reference.year, mes_fin)[1]),
        )
    return (
        date(reference.year, reference.month, 1),
        date(reference.year, reference.month, monthrange(reference.year, reference.month)[1]),
    )


def period_label(period: str, start: date) -> str:
    periodo = (period or "monthly").lower()
    if periodo == "yearly":
        return str(start.year)
    if periodo == "quarterly":
        return f"{start.year}-Q{(start.month - 1) // 3 + 1}"
    return start.strftime("%Y-%m")


# ---------------------------------------------------------------------------
# Ambito
# ---------------------------------------------------------------------------
def scope_label(scope: dict[str, Any] | None) -> str:
    if not scope:
        return "ORGANIZACION"
    return " & ".join(f"{k}={v}" for k, v in sorted(scope.items()))


def matches_scope(record: dict[str, Any], scope: dict[str, Any] | None) -> bool:
    """True si el registro de costo pertenece al ambito del presupuesto.

    Los valores admiten comodines glob y listas (OR). Un scope vacio abarca todo.
    """
    for clave, esperado in (scope or {}).items():
        actual = record.get(clave)
        if actual is None:
            return False
        actual_txt = str(actual).upper()
        candidatos = esperado if isinstance(esperado, (list, tuple)) else [esperado]
        if not any(fnmatch.fnmatch(actual_txt, str(c).upper()) for c in candidatos):
            return False
    return True


def is_active(budget: dict[str, Any], reference: date) -> bool:
    """Respeta effective_from / effective_to si estan definidos."""
    desde = budget.get("effective_from")
    hasta = budget.get("effective_to")
    if desde and reference < _as_date(desde):
        return False
    return not (hasta and reference > _as_date(hasta))


def _as_date(value: Any) -> date:
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


# ---------------------------------------------------------------------------
# Evaluacion
# ---------------------------------------------------------------------------
def _highest_threshold_reached(pct: float, thresholds: list[float] | None) -> float | None:
    alcanzados = [float(t) for t in (thresholds or []) if pct >= float(t)]
    return max(alcanzados) if alcanzados else None


def _status_from(consumed_pct: float, projected_pct: float, thresholds: list[float] | None) -> str:
    if consumed_pct >= 100.0:
        return STATUS_EXCEEDED
    ordenados = sorted(float(t) for t in (thresholds or [80.0, 100.0]))
    critico = ordenados[-2] if len(ordenados) >= 2 else 90.0
    advertencia = ordenados[0] if ordenados else 80.0
    if consumed_pct >= critico or projected_pct >= 100.0:
        return STATUS_CRITICAL
    if consumed_pct >= advertencia or projected_pct >= critico:
        return STATUS_WARNING
    return STATUS_OK


def evaluate_budget(
    budget: dict[str, Any],
    cost_records: list[dict[str, Any]],
    *,
    as_of: date,
    forecast_points: list[ForecastPoint] | None = None,
    date_field: str = "usage_date",
    value_field: str = "total_cost_usd",
) -> BudgetStatus:
    """Evalua un presupuesto contra los registros de costo del periodo vigente."""
    periodo = str(budget.get("period", "monthly")).lower()
    inicio, fin = period_bounds(periodo, as_of)
    ambito = budget.get("scope") or {}
    monto = float(budget.get("amount_usd", 0.0) or 0.0)
    umbrales = [float(t) for t in (budget.get("thresholds_pct") or [80, 100])]

    puntos: dict[date, float] = {}
    for registro in cost_records:
        fecha = registro.get(date_field)
        if not isinstance(fecha, date) or not (inicio <= fecha <= fin):
            continue
        if not matches_scope(registro, ambito):
            continue
        puntos[fecha] = puntos.get(fecha, 0.0) + float(registro.get(value_field) or 0.0)

    serie = sorted(puntos.items())
    proyeccion = project_period_total(serie, inicio, fin, forecast_points)

    ejecutado = proyeccion["actual_cost_usd"]
    proyectado = proyeccion["projected_total_usd"]
    consumido_pct = (ejecutado / monto * 100.0) if monto > 0 else 0.0
    proyectado_pct = (proyectado / monto * 100.0) if monto > 0 else 0.0

    dias_totales = (fin - inicio).days + 1
    dias_transcurridos = max(0, min((as_of - inicio).days + 1, dias_totales))
    avance_periodo_pct = (dias_transcurridos / dias_totales * 100.0) if dias_totales else 0.0

    restante = max(0.0, monto - ejecutado)
    dias_restantes = proyeccion["remaining_days"]
    requerido_diario = (restante / dias_restantes) if dias_restantes > 0 else 0.0
    ritmo = proyeccion["avg_daily_cost_usd"]
    dias_para_agotar = int(restante // ritmo) if ritmo > 0 and restante > 0 else (0 if restante <= 0 else None)

    estado = _status_from(consumido_pct, proyectado_pct, umbrales)

    return BudgetStatus(
        budget_id=str(budget.get("id", "sin_id")),
        budget_name=str(budget.get("name", budget.get("id", "sin_nombre"))),
        period=periodo,
        period_start=inicio,
        period_end=fin,
        as_of_date=as_of,
        scope={k: str(v) for k, v in ambito.items()},
        scope_label=scope_label(ambito),
        owner_email=budget.get("owner_email"),
        budget_amount_usd=round(monto, 2),
        actual_cost_usd=ejecutado,
        consumed_pct=round(consumido_pct, 4),
        forecast_remaining_usd=proyeccion["forecast_remaining_usd"],
        projected_total_usd=proyectado,
        projected_pct=round(proyectado_pct, 4),
        variance_usd=round(monto - proyectado, 4),
        avg_daily_cost_usd=ritmo,
        required_daily_cost_usd=round(requerido_diario, 4),
        elapsed_days=dias_transcurridos,
        remaining_days=dias_restantes,
        period_progress_pct=round(avance_periodo_pct, 4),
        days_to_exhaustion=dias_para_agotar,
        threshold_reached_pct=_highest_threshold_reached(consumido_pct, umbrales),
        status=estado,
        is_on_track=proyectado_pct <= 100.0,
        details={
            "period_label": period_label(periodo, inicio),
            "thresholds_pct": umbrales,
            "forecast_used": bool(forecast_points),
        },
    )


def evaluate_all(
    budgets_config: dict[str, Any],
    cost_records: list[dict[str, Any]],
    *,
    as_of: date,
    forecasts_by_scope: dict[str, list[ForecastPoint]] | None = None,
) -> list[BudgetStatus]:
    """Evalua todos los presupuestos activos definidos en conf/budgets.yml."""
    salida: list[BudgetStatus] = []
    for presupuesto in (budgets_config or {}).get("budgets", []) or []:
        if not isinstance(presupuesto, dict) or not is_active(presupuesto, as_of):
            continue
        clave = str(presupuesto.get("id"))
        salida.append(
            evaluate_budget(
                presupuesto,
                cost_records,
                as_of=as_of,
                forecast_points=(forecasts_by_scope or {}).get(clave),
            )
        )
    salida.sort(key=lambda b: -b.consumed_pct)
    return salida


def burn_rate_summary(status: BudgetStatus) -> str:
    """Frase legible del ritmo de consumo, para el cuerpo de las alertas."""
    if status.budget_amount_usd <= 0:
        return "Presupuesto sin monto definido."
    if status.status == STATUS_EXCEEDED:
        exceso = status.actual_cost_usd - status.budget_amount_usd
        return f"Presupuesto excedido en USD {exceso:,.2f} ({status.consumed_pct:.1f}% consumido)."
    if status.days_to_exhaustion is not None and status.days_to_exhaustion <= status.remaining_days:
        return (
            f"Al ritmo actual (USD {status.avg_daily_cost_usd:,.2f}/dia) el presupuesto se agota "
            f"en {status.days_to_exhaustion} dias, con {status.remaining_days} dias de periodo restantes."
        )
    return (
        f"Consumido {status.consumed_pct:.1f}% con {status.period_progress_pct:.1f}% del periodo transcurrido. "
        f"Proyeccion de cierre: USD {status.projected_total_usd:,.2f} ({status.projected_pct:.1f}%)."
    )


def next_period_start(period: str, reference: date) -> date:
    _, fin = period_bounds(period, reference)
    return fin + timedelta(days=1)
