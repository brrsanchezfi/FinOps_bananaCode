"""Deteccion de anomalias de costo sobre series diarias.

Metodo por defecto: **z-score modificado** (mediana + MAD), robusto frente a los
propios picos que se quieren detectar — a diferencia de media/desviacion, un
solo dia atipico no infla la referencia y enmascara los siguientes.

    score = 0.6745 * (x - mediana) / MAD

Con `day_of_week_aware` la referencia se construye solo con los mismos dias de
la semana, lo que evita marcar como anomalia la caida natural de los fines de
semana en cargas empresariales. En ese modo un punto solo se evalua si existen
al menos 3 observaciones previas del mismo dia de la semana dentro de la
ventana: con menos evidencia se prefiere no reportar antes que comparar contra
una base mezclada (es decir, se necesitan ~3 semanas de historia para que el
modo estacional empiece a producir detecciones).

Una serie se reporta como anomalia si simultaneamente:
  * |score| >= score_threshold
  * |cambio relativo vs esperado| >= min_pct_change
  * el costo promedio de la serie >= min_avg_cost_usd  (evita ruido irrelevante)
"""

from __future__ import annotations

import math
import statistics
from dataclasses import asdict, dataclass
from datetime import date, timedelta
from typing import Any

_MAD_SCALE = 0.6745  # constante de consistencia con la normal


@dataclass
class AnomalyResult:
    """Anomalia detectada en un punto de una serie."""

    series_key: str
    dimension: str
    dimension_value: str
    usage_date: date
    actual_cost_usd: float
    expected_cost_usd: float
    deviation_usd: float
    pct_change: float
    score: float
    direction: str          # SPIKE | DROP
    severity: str           # low | medium | high | critical
    method: str
    baseline_points: int
    baseline_window_days: int

    def to_row(self) -> dict[str, Any]:
        return asdict(self)


# ---------------------------------------------------------------------------
# Estadistica robusta
# ---------------------------------------------------------------------------
def median_absolute_deviation(values: list[float], center: float | None = None) -> float:
    """MAD = mediana(|x - mediana(x)|)."""
    if not values:
        return 0.0
    centro = statistics.median(values) if center is None else center
    return statistics.median([abs(v - centro) for v in values])


def robust_score(value: float, baseline: list[float], method: str = "mad") -> tuple[float, float, float]:
    """Devuelve (score, esperado, dispersion) para un valor contra su base.

    Si el metodo es 'mad' y la MAD es cero (serie plana o casi), se degrada a la
    desviacion absoluta media y luego a la desviacion estandar, para no dividir
    por cero ni declarar anomalia todo cambio minimo.
    """
    if not baseline:
        return 0.0, value, 0.0

    if method == "zscore":
        esperado = statistics.fmean(baseline)
        dispersion = statistics.pstdev(baseline) if len(baseline) > 1 else 0.0
        if dispersion <= 0:
            return 0.0, esperado, 0.0
        return (value - esperado) / dispersion, esperado, dispersion

    esperado = statistics.median(baseline)
    mad = median_absolute_deviation(baseline, esperado)
    if mad > 0:
        return _MAD_SCALE * (value - esperado) / mad, esperado, mad

    desv_media = statistics.fmean([abs(v - esperado) for v in baseline]) if baseline else 0.0
    if desv_media > 0:
        return _MAD_SCALE * (value - esperado) / desv_media, esperado, desv_media

    desv_std = statistics.pstdev(baseline) if len(baseline) > 1 else 0.0
    if desv_std > 0:
        return (value - esperado) / desv_std, esperado, desv_std

    # Serie perfectamente constante: cualquier cambio es "infinito"; se acota.
    if math.isclose(value, esperado, rel_tol=1e-9, abs_tol=1e-9):
        return 0.0, esperado, 0.0
    return math.copysign(99.0, value - esperado), esperado, 0.0


def classify_severity(score: float, thresholds: dict[str, float] | None) -> str:
    """Traduce un score a severidad usando los umbrales configurados."""
    umbrales = thresholds or {}
    magnitud = abs(score)
    if magnitud >= float(umbrales.get("critical", 8.0)):
        return "critical"
    if magnitud >= float(umbrales.get("high", 6.0)):
        return "high"
    if magnitud >= float(umbrales.get("medium", 4.5)):
        return "medium"
    return "low"


# ---------------------------------------------------------------------------
# Deteccion sobre una serie
# ---------------------------------------------------------------------------
def _build_baseline(
    puntos: list[tuple[date, float]],
    indice: int,
    window_days: int,
    day_of_week_aware: bool,
    min_history: int,
) -> list[float]:
    """Construye la base de comparacion para el punto `indice`."""
    fecha_actual = puntos[indice][0]
    inicio_ventana = fecha_actual - timedelta(days=window_days)

    historicos = [(f, v) for f, v in puntos[:indice] if inicio_ventana <= f < fecha_actual]
    if not historicos:
        return []

    if day_of_week_aware:
        mismo_dia = [v for f, v in historicos if f.weekday() == fecha_actual.weekday()]
        # Se exige un minimo de observaciones del mismo dia para que la referencia
        # estacional sea significativa. Si no las hay, se devuelve una base vacia
        # y el punto NO se evalua: caer a la ventana completa reintroduciria
        # justamente el falso positivo de fin de semana que este modo evita.
        return mismo_dia if len(mismo_dia) >= 3 else []
    return [v for _, v in historicos] if len(historicos) >= min_history else []


def detect_series(
    points: list[tuple[date, float]],
    *,
    series_key: str = "",
    dimension: str = "",
    dimension_value: str = "",
    window_days: int = 28,
    min_history_days: int = 14,
    method: str = "mad",
    score_threshold: float = 3.5,
    min_avg_cost_usd: float = 20.0,
    min_pct_change: float = 0.30,
    day_of_week_aware: bool = True,
    severity_thresholds: dict[str, float] | None = None,
    evaluate_last_n_days: int | None = None,
) -> list[AnomalyResult]:
    """Detecta anomalias en una serie diaria ordenada.

    Args:
        points: [(fecha, costo)] ordenables por fecha. Se ordenan internamente.
        evaluate_last_n_days: si se indica, solo se evaluan los ultimos N puntos
            (el resto sirve unicamente como historia). Util en corridas diarias.

    Returns:
        Lista de anomalias detectadas, ordenada por fecha.
    """
    if not points:
        return []

    ordenados = sorted(points, key=lambda p: p[0])
    valores = [float(v or 0.0) for _, v in ordenados]

    if len(ordenados) < min_history_days + 1:
        return []
    if statistics.fmean(valores) < min_avg_cost_usd:
        return []

    primer_indice = min_history_days
    if evaluate_last_n_days is not None:
        primer_indice = max(primer_indice, len(ordenados) - int(evaluate_last_n_days))

    resultados: list[AnomalyResult] = []
    for i in range(primer_indice, len(ordenados)):
        fecha, valor_crudo = ordenados[i]
        valor = float(valor_crudo or 0.0)
        base = _build_baseline(ordenados, i, window_days, day_of_week_aware, min_history_days)
        if len(base) < 3:
            continue

        score, esperado, _ = robust_score(valor, base, method)
        if abs(score) < score_threshold:
            continue

        cambio_pct = ((valor - esperado) / esperado) if esperado > 0 else (1.0 if valor > 0 else 0.0)
        if abs(cambio_pct) < min_pct_change:
            continue

        resultados.append(
            AnomalyResult(
                series_key=series_key,
                dimension=dimension,
                dimension_value=dimension_value,
                usage_date=fecha,
                actual_cost_usd=round(valor, 4),
                expected_cost_usd=round(esperado, 4),
                deviation_usd=round(valor - esperado, 4),
                pct_change=round(cambio_pct, 6),
                score=round(score, 4),
                direction="SPIKE" if valor > esperado else "DROP",
                severity=classify_severity(score, severity_thresholds),
                method=method,
                baseline_points=len(base),
                baseline_window_days=window_days,
            )
        )
    return resultados


def detect_many(
    series: dict[str, list[tuple[date, float]]],
    *,
    dimension: str = "",
    config: dict[str, Any] | None = None,
    evaluate_last_n_days: int | None = None,
) -> list[AnomalyResult]:
    """Aplica `detect_series` a un conjunto de series.

    Args:
        series: mapa valor_de_dimension -> puntos.
        config: bloque `anomaly` de la configuracion.
    """
    cfg = config or {}
    salida: list[AnomalyResult] = []
    for valor_dimension, puntos in series.items():
        salida.extend(
            detect_series(
                puntos,
                series_key=f"{dimension}={valor_dimension}",
                dimension=dimension,
                dimension_value=str(valor_dimension),
                window_days=int(cfg.get("window_days", 28)),
                min_history_days=int(cfg.get("min_history_days", 14)),
                method=str(cfg.get("method", "mad")).lower(),
                score_threshold=float(cfg.get("score_threshold", 3.5)),
                min_avg_cost_usd=float(cfg.get("min_avg_cost_usd", 20.0)),
                min_pct_change=float(cfg.get("min_pct_change", 0.30)),
                day_of_week_aware=bool(cfg.get("day_of_week_aware", True)),
                severity_thresholds=cfg.get("severity"),
                evaluate_last_n_days=evaluate_last_n_days,
            )
        )
    salida.sort(key=lambda a: (a.usage_date, -abs(a.score)))
    return salida


def group_points(
    records: list[dict[str, Any]],
    *,
    key_field: str,
    date_field: str = "usage_date",
    value_field: str = "total_cost_usd",
) -> dict[str, list[tuple[date, float]]]:
    """Agrupa registros planos en series por dimension, sumando duplicados."""
    acumulado: dict[str, dict[date, float]] = {}
    for registro in records:
        clave = registro.get(key_field)
        fecha = registro.get(date_field)
        if clave is None or fecha is None:
            continue
        if not isinstance(fecha, date):
            continue
        serie = acumulado.setdefault(str(clave), {})
        serie[fecha] = serie.get(fecha, 0.0) + float(registro.get(value_field) or 0.0)
    return {clave: sorted(puntos.items()) for clave, puntos in acumulado.items()}
