"""Pronostico de costo diario.

Metodos disponibles:

  * ``holt``            suavizado exponencial doble con tendencia amortiguada
                        (Holt-damped). Es el mejor compromiso para series de
                        costo cloud: capta la tendencia sin extrapolarla de
                        forma explosiva a 30-45 dias.
  * ``seasonal_naive``  repite la ultima semana observada.
  * ``moving_average``  promedio movil de la ventana reciente.

Sobre cualquiera de ellos se aplica, opcionalmente, un indice de estacionalidad
semanal multiplicativo estimado sobre la ventana de entrenamiento. El intervalo
de prediccion se deriva de la desviacion de los residuales del ajuste, ampliado
con la raiz del horizonte (comportamiento tipico de un random walk).
"""

from __future__ import annotations

import math
import statistics
from dataclasses import asdict, dataclass
from datetime import date, timedelta
from typing import Any

_Z_95 = 1.96


@dataclass
class ForecastPoint:
    """Un dia pronosticado."""

    forecast_date: date
    predicted_cost_usd: float
    lower_bound_usd: float
    upper_bound_usd: float
    horizon_day: int

    def to_row(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class ForecastResult:
    """Resultado completo de un pronostico de serie."""

    series_key: str
    dimension: str
    dimension_value: str
    method: str
    generated_for_date: date
    history_days: int
    mape: float | None
    points: list[ForecastPoint]

    def total(self) -> float:
        return round(sum(p.predicted_cost_usd for p in self.points), 4)

    def to_rows(self) -> list[dict[str, Any]]:
        return [
            {
                "series_key": self.series_key,
                "dimension": self.dimension,
                "dimension_value": self.dimension_value,
                "method": self.method,
                "generated_for_date": self.generated_for_date,
                "history_days": self.history_days,
                "mape": self.mape,
                **p.to_row(),
            }
            for p in self.points
        ]


# ---------------------------------------------------------------------------
# Preparacion de la serie
# ---------------------------------------------------------------------------
def fill_missing_days(points: list[tuple[date, float]], fill_value: float = 0.0) -> list[tuple[date, float]]:
    """Rellena los dias sin registro para obtener una serie contigua.

    Un dia sin consumo es informacion valida (costo 0), no un hueco: omitirlo
    distorsiona tendencia y estacionalidad.
    """
    if not points:
        return []
    ordenados = sorted(points, key=lambda p: p[0])
    indice = {f: float(v or 0.0) for f, v in ordenados}
    inicio, fin = ordenados[0][0], ordenados[-1][0]
    salida: list[tuple[date, float]] = []
    cursor = inicio
    while cursor <= fin:
        salida.append((cursor, indice.get(cursor, fill_value)))
        cursor += timedelta(days=1)
    return salida


def weekly_indices(points: list[tuple[date, float]], min_per_day: int = 2) -> dict[int, float]:
    """Indice estacional multiplicativo por dia de la semana (0=lunes).

    Devuelve 1.0 para los dias sin evidencia suficiente. Los indices se acotan a
    [0.2, 3.0] para que un dia atipico no domine el pronostico.
    """
    valores = [v for _, v in points]
    promedio_global = statistics.fmean(valores) if valores else 0.0
    if promedio_global <= 0:
        return dict.fromkeys(range(7), 1.0)

    por_dia: dict[int, list[float]] = {d: [] for d in range(7)}
    for fecha, valor in points:
        por_dia[fecha.weekday()].append(valor)

    indices: dict[int, float] = {}
    for dia, muestras in por_dia.items():
        if len(muestras) < min_per_day:
            indices[dia] = 1.0
            continue
        indices[dia] = max(0.2, min(3.0, statistics.fmean(muestras) / promedio_global))
    return indices


def deseasonalize(points: list[tuple[date, float]], indices: dict[int, float]) -> list[tuple[date, float]]:
    return [(f, v / indices.get(f.weekday(), 1.0) if indices.get(f.weekday(), 1.0) > 0 else v) for f, v in points]


# ---------------------------------------------------------------------------
# Modelos
# ---------------------------------------------------------------------------
def holt_damped_fit(
    values: list[float], alpha: float = 0.35, beta: float = 0.12, phi: float = 0.92
) -> tuple[float, float, list[float]]:
    """Ajusta Holt con tendencia amortiguada. Devuelve (nivel, tendencia, ajustados)."""
    if not values:
        return 0.0, 0.0, []
    if len(values) == 1:
        return float(values[0]), 0.0, [float(values[0])]

    nivel = float(values[0])
    tendencia = float(values[1] - values[0])
    ajustados: list[float] = [nivel]

    for observado in values[1:]:
        prediccion = nivel + phi * tendencia
        ajustados.append(prediccion)
        nivel_previo = nivel
        nivel = alpha * observado + (1 - alpha) * prediccion
        tendencia = beta * (nivel - nivel_previo) + (1 - beta) * phi * tendencia
    return nivel, tendencia, ajustados


def _holt_forecast(values: list[float], horizon: int, alpha: float, beta: float, phi: float) -> list[float]:
    nivel, tendencia, _ = holt_damped_fit(values, alpha, beta, phi)
    salida: list[float] = []
    acumulado_phi = 0.0
    for h in range(1, horizon + 1):
        acumulado_phi += phi**h
        salida.append(max(0.0, nivel + acumulado_phi * tendencia))
    return salida


def _seasonal_naive_forecast(values: list[float], horizon: int, period: int = 7) -> list[float]:
    if not values:
        return [0.0] * horizon
    ventana = values[-period:] if len(values) >= period else values
    return [max(0.0, ventana[i % len(ventana)]) for i in range(horizon)]


def _moving_average_forecast(values: list[float], horizon: int, window: int = 7) -> list[float]:
    if not values:
        return [0.0] * horizon
    ventana = values[-window:]
    promedio = max(0.0, statistics.fmean(ventana))
    return [promedio] * horizon


def _residual_sigma(values: list[float], fitted: list[float]) -> float:
    residuales = [v - f for v, f in zip(values, fitted, strict=False)]
    if len(residuales) < 2:
        return 0.0
    return statistics.pstdev(residuales)


def _mape(values: list[float], fitted: list[float]) -> float | None:
    pares = [(v, f) for v, f in zip(values, fitted, strict=False) if v > 0]
    if len(pares) < 3:
        return None
    return round(statistics.fmean([abs(v - f) / v for v, f in pares]), 6)


# ---------------------------------------------------------------------------
# API principal
# ---------------------------------------------------------------------------
def forecast_series(
    points: list[tuple[date, float]],
    *,
    horizon_days: int = 45,
    method: str = "holt",
    alpha: float = 0.35,
    beta: float = 0.12,
    damping: float = 0.92,
    weekly_seasonality: bool = True,
    min_history_days: int = 21,
    train_window_days: int = 120,
    series_key: str = "",
    dimension: str = "",
    dimension_value: str = "",
    as_of: date | None = None,
) -> ForecastResult | None:
    """Pronostica una serie diaria. Devuelve None si no hay historia suficiente."""
    if not points:
        return None

    serie = fill_missing_days(points)
    if len(serie) < min_history_days:
        return None
    if train_window_days > 0:
        serie = serie[-train_window_days:]

    ultima_fecha = serie[-1][0]
    origen = as_of or ultima_fecha

    indices = weekly_indices(serie) if weekly_seasonality else dict.fromkeys(range(7), 1.0)
    serie_ajustada = deseasonalize(serie, indices) if weekly_seasonality else serie
    valores = [v for _, v in serie_ajustada]

    metodo = method.lower()
    if metodo == "seasonal_naive":
        base = _seasonal_naive_forecast(valores, horizon_days)
        ajustados = valores
    elif metodo == "moving_average":
        base = _moving_average_forecast(valores, horizon_days)
        ajustados = valores
    else:
        metodo = "holt"
        base = _holt_forecast(valores, horizon_days, alpha, beta, damping)
        _, _, ajustados = holt_damped_fit(valores, alpha, beta, damping)

    sigma = _residual_sigma(valores, ajustados)
    error_medio = _mape(valores, ajustados)

    puntos: list[ForecastPoint] = []
    for h in range(1, horizon_days + 1):
        fecha_futura = ultima_fecha + timedelta(days=h)
        factor = indices.get(fecha_futura.weekday(), 1.0) if weekly_seasonality else 1.0
        estimado = max(0.0, base[h - 1] * factor)
        margen = _Z_95 * sigma * math.sqrt(h) * factor
        puntos.append(
            ForecastPoint(
                forecast_date=fecha_futura,
                predicted_cost_usd=round(estimado, 4),
                lower_bound_usd=round(max(0.0, estimado - margen), 4),
                upper_bound_usd=round(estimado + margen, 4),
                horizon_day=h,
            )
        )

    return ForecastResult(
        series_key=series_key,
        dimension=dimension,
        dimension_value=dimension_value,
        method=metodo,
        generated_for_date=origen,
        history_days=len(serie),
        mape=error_medio,
        points=puntos,
    )


def forecast_many(
    series: dict[str, list[tuple[date, float]]],
    *,
    dimension: str = "",
    config: dict[str, Any] | None = None,
    as_of: date | None = None,
) -> list[ForecastResult]:
    """Pronostica un conjunto de series usando el bloque `forecast` de la config."""
    cfg = config or {}
    salida: list[ForecastResult] = []
    for valor_dimension, puntos in series.items():
        resultado = forecast_series(
            puntos,
            horizon_days=int(cfg.get("horizon_days", 45)),
            method=str(cfg.get("method", "holt")),
            alpha=float(cfg.get("alpha", 0.35)),
            beta=float(cfg.get("beta", 0.12)),
            damping=float(cfg.get("damping", 0.92)),
            weekly_seasonality=bool(cfg.get("weekly_seasonality", True)),
            min_history_days=int(cfg.get("min_history_days", 21)),
            train_window_days=int(cfg.get("train_window_days", 120)),
            series_key=f"{dimension}={valor_dimension}",
            dimension=dimension,
            dimension_value=str(valor_dimension),
            as_of=as_of,
        )
        if resultado is not None:
            salida.append(resultado)
    return salida


# ---------------------------------------------------------------------------
# Proyeccion de periodo (usada por presupuestos)
# ---------------------------------------------------------------------------
def project_period_total(
    actual_points: list[tuple[date, float]],
    period_start: date,
    period_end: date,
    forecast_points: list[ForecastPoint] | None = None,
    *,
    fallback_daily_rate: float | None = None,
) -> dict[str, Any]:
    """Proyecta el costo total de un periodo combinando ejecutado + pronostico.

    Si no hay pronostico disponible se extrapola con el ritmo diario promedio de
    lo ya ejecutado (o con `fallback_daily_rate`).
    """
    ejecutado = sum(v for f, v in actual_points if period_start <= f <= period_end)
    dias_ejecutados = len({f for f, _ in actual_points if period_start <= f <= period_end})
    ultimo_dia_real = max((f for f, _ in actual_points if period_start <= f <= period_end), default=None)

    inicio_restante = (ultimo_dia_real + timedelta(days=1)) if ultimo_dia_real else period_start
    dias_restantes = max(0, (period_end - inicio_restante).days + 1) if inicio_restante <= period_end else 0

    if forecast_points:
        indice_fc = {p.forecast_date: p.predicted_cost_usd for p in forecast_points}
        pronosticado = sum(v for f, v in indice_fc.items() if inicio_restante <= f <= period_end)
        dias_cubiertos = sum(1 for f in indice_fc if inicio_restante <= f <= period_end)
        if dias_cubiertos < dias_restantes and dias_cubiertos > 0:
            # El horizonte no cubre todo el periodo: se extiende con el promedio.
            promedio_fc = pronosticado / dias_cubiertos
            pronosticado += promedio_fc * (dias_restantes - dias_cubiertos)
    else:
        ritmo = fallback_daily_rate
        if ritmo is None:
            ritmo = (ejecutado / dias_ejecutados) if dias_ejecutados > 0 else 0.0
        pronosticado = ritmo * dias_restantes

    return {
        "actual_cost_usd": round(ejecutado, 4),
        "forecast_remaining_usd": round(pronosticado, 4),
        "projected_total_usd": round(ejecutado + pronosticado, 4),
        "elapsed_days": dias_ejecutados,
        "remaining_days": dias_restantes,
        "avg_daily_cost_usd": round(ejecutado / dias_ejecutados, 4) if dias_ejecutados else 0.0,
    }
