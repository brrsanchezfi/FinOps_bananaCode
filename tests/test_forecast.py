"""Pruebas del motor de pronostico."""

from __future__ import annotations

from datetime import date, timedelta

import pytest
from tests.conftest import serie_estable

from finops.analytics.forecast import (
    ForecastPoint,
    deseasonalize,
    fill_missing_days,
    forecast_many,
    forecast_series,
    holt_damped_fit,
    project_period_total,
    weekly_indices,
)


def serie_con_tendencia(dias: int, base: float, pendiente: float) -> list[tuple[date, float]]:
    inicio = date(2026, 1, 1)
    return [(inicio + timedelta(days=i), base + pendiente * i) for i in range(dias)]


class TestPreparacion:
    def test_rellena_dias_faltantes(self):
        puntos = [(date(2026, 1, 1), 10.0), (date(2026, 1, 4), 40.0)]
        completa = fill_missing_days(puntos)
        assert len(completa) == 4
        assert completa[1] == (date(2026, 1, 2), 0.0)
        assert completa[3] == (date(2026, 1, 4), 40.0)

    def test_serie_vacia(self):
        assert fill_missing_days([]) == []

    def test_indices_semanales_de_serie_plana_son_uno(self):
        indices = weekly_indices(serie_estable(28, 100.0))
        assert all(v == pytest.approx(1.0) for v in indices.values())

    def test_indices_capturan_caida_de_fin_de_semana(self):
        inicio = date(2026, 1, 5)  # lunes
        puntos = [
            (inicio + timedelta(days=i), 30.0 if (inicio + timedelta(days=i)).weekday() >= 5 else 300.0)
            for i in range(28)
        ]
        indices = weekly_indices(puntos)
        assert indices[5] < 0.5   # sabado
        assert indices[0] > 1.0   # lunes

    def test_desestacionalizar_aplana_la_serie(self):
        # Estacionalidad realista: el fin de semana rinde ~40% de un dia habil.
        inicio = date(2026, 1, 5)
        puntos = [
            (inicio + timedelta(days=i), 120.0 if (inicio + timedelta(days=i)).weekday() >= 5 else 300.0)
            for i in range(28)
        ]
        indices = weekly_indices(puntos)
        valores = [v for _, v in deseasonalize(puntos, indices)]
        assert max(valores) / min(valores) < 1.05

    def test_indices_se_acotan(self):
        puntos = [(date(2026, 1, 5) + timedelta(days=i), 1.0 if i % 7 else 10_000.0) for i in range(28)]
        assert all(0.2 <= v <= 3.0 for v in weekly_indices(puntos).values())

    def test_el_acote_limita_la_desestacionalizacion_extrema(self):
        # Con una relacion 10x el indice del fin de semana se acota en 0.2, asi
        # que la serie no queda completamente plana: es el precio de proteger el
        # pronostico frente a un dia atipico. Comportamiento documentado.
        inicio = date(2026, 1, 5)
        puntos = [
            (inicio + timedelta(days=i), 30.0 if (inicio + timedelta(days=i)).weekday() >= 5 else 300.0)
            for i in range(28)
        ]
        indices = weekly_indices(puntos)
        assert indices[5] == pytest.approx(0.2)
        valores = [v for _, v in deseasonalize(puntos, indices)]
        assert max(valores) / min(valores) > 1.05


class TestHolt:
    def test_serie_plana_mantiene_el_nivel(self):
        nivel, tendencia, _ = holt_damped_fit([100.0] * 30)
        assert nivel == pytest.approx(100.0, abs=0.5)
        assert tendencia == pytest.approx(0.0, abs=0.5)

    def test_captura_tendencia_creciente(self):
        nivel, tendencia, _ = holt_damped_fit([100.0 + 5 * i for i in range(30)])
        assert tendencia > 0
        assert nivel > 200

    def test_serie_vacia_y_de_un_punto(self):
        assert holt_damped_fit([]) == (0.0, 0.0, [])
        assert holt_damped_fit([7.0]) == (7.0, 0.0, [7.0])


class TestForecastSeries:
    def test_serie_plana_pronostica_el_mismo_nivel(self):
        resultado = forecast_series(serie_estable(60, 100.0), horizon_days=10, weekly_seasonality=False)
        assert resultado is not None
        assert all(p.predicted_cost_usd == pytest.approx(100.0, abs=2.0) for p in resultado.points)

    def test_historia_insuficiente_devuelve_none(self):
        assert forecast_series(serie_estable(5, 100.0), min_history_days=21) is None

    def test_serie_vacia(self):
        assert forecast_series([]) is None

    def test_horizonte_correcto(self):
        resultado = forecast_series(serie_estable(60, 100.0), horizon_days=45)
        assert len(resultado.points) == 45
        assert resultado.points[0].horizon_day == 1
        assert resultado.points[-1].forecast_date == date(2026, 1, 1) + timedelta(days=59 + 45)

    def test_pronostico_nunca_es_negativo(self):
        # Tendencia fuertemente decreciente: la extrapolacion debe truncarse en 0.
        resultado = forecast_series(
            serie_con_tendencia(60, 500.0, -8.0), horizon_days=45, weekly_seasonality=False
        )
        assert all(p.predicted_cost_usd >= 0 for p in resultado.points)
        assert all(p.lower_bound_usd >= 0 for p in resultado.points)

    def test_intervalo_se_ensancha_con_el_horizonte(self):
        from tests.conftest import serie_con_ruido

        resultado = forecast_series(serie_con_ruido(60, 100.0, 15.0), horizon_days=30, weekly_seasonality=False)
        primero = resultado.points[0]
        ultimo = resultado.points[-1]
        ancho_primero = primero.upper_bound_usd - primero.lower_bound_usd
        ancho_ultimo = ultimo.upper_bound_usd - ultimo.lower_bound_usd
        assert ancho_ultimo > ancho_primero

    def test_tendencia_amortiguada_no_explota(self):
        resultado = forecast_series(
            serie_con_tendencia(60, 100.0, 10.0), horizon_days=45, weekly_seasonality=False
        )
        ultimo_observado = 100.0 + 10.0 * 59
        # Con amortiguacion phi=0.92 el crecimiento acumulado es finito.
        assert resultado.points[-1].predicted_cost_usd < ultimo_observado * 3

    @pytest.mark.parametrize("metodo", ["holt", "seasonal_naive", "moving_average"])
    def test_todos_los_metodos_producen_puntos(self, metodo):
        resultado = forecast_series(serie_estable(60, 100.0), horizon_days=10, method=metodo)
        assert resultado is not None
        assert resultado.method == metodo
        assert len(resultado.points) == 10

    def test_metodo_desconocido_cae_a_holt(self):
        resultado = forecast_series(serie_estable(60, 100.0), horizon_days=5, method="magia")
        assert resultado.method == "holt"

    def test_total_y_filas(self):
        resultado = forecast_series(
            serie_estable(60, 100.0), horizon_days=10, weekly_seasonality=False,
            dimension="team", dimension_value="Datos", series_key="team=Datos",
        )
        assert resultado.total() == pytest.approx(1000.0, abs=50)
        filas = resultado.to_rows()
        assert len(filas) == 10
        assert filas[0]["dimension"] == "team"
        assert "predicted_cost_usd" in filas[0]

    def test_mape_de_serie_plana_es_casi_cero(self):
        resultado = forecast_series(serie_estable(60, 100.0), horizon_days=5, weekly_seasonality=False)
        assert resultado.mape is not None
        assert resultado.mape < 0.05


class TestForecastMany:
    def test_omite_series_sin_historia(self):
        resultados = forecast_many(
            {"largo": serie_estable(60, 100.0), "corto": serie_estable(3, 100.0)},
            dimension="team",
            config={"min_history_days": 21, "horizon_days": 5},
        )
        assert [r.dimension_value for r in resultados] == ["largo"]


class TestProyeccionDePeriodo:
    def test_usa_el_pronostico_cuando_existe(self):
        inicio, fin = date(2026, 3, 1), date(2026, 3, 31)
        reales = [(date(2026, 3, d), 100.0) for d in range(1, 16)]
        pronostico = [
            ForecastPoint(date(2026, 3, 15) + timedelta(days=h), 120.0, 100.0, 140.0, h) for h in range(1, 17)
        ]
        salida = project_period_total(reales, inicio, fin, pronostico)
        assert salida["actual_cost_usd"] == pytest.approx(1500.0)
        assert salida["forecast_remaining_usd"] == pytest.approx(120.0 * 16)
        assert salida["remaining_days"] == 16

    def test_extrapola_con_el_ritmo_si_no_hay_pronostico(self):
        inicio, fin = date(2026, 3, 1), date(2026, 3, 31)
        reales = [(date(2026, 3, d), 100.0) for d in range(1, 11)]
        salida = project_period_total(reales, inicio, fin, None)
        assert salida["avg_daily_cost_usd"] == pytest.approx(100.0)
        assert salida["projected_total_usd"] == pytest.approx(3100.0)

    def test_horizonte_corto_se_extiende_con_el_promedio(self):
        inicio, fin = date(2026, 3, 1), date(2026, 3, 31)
        reales = [(date(2026, 3, d), 100.0) for d in range(1, 11)]
        pronostico = [ForecastPoint(date(2026, 3, 10) + timedelta(days=h), 50.0, 0.0, 100.0, h) for h in range(1, 6)]
        salida = project_period_total(reales, inicio, fin, pronostico)
        # 21 dias restantes, 5 cubiertos a 50 y 16 extendidos al mismo promedio.
        assert salida["remaining_days"] == 21
        assert salida["forecast_remaining_usd"] == pytest.approx(50.0 * 21)

    def test_periodo_sin_datos(self):
        salida = project_period_total([], date(2026, 3, 1), date(2026, 3, 31), None)
        assert salida["actual_cost_usd"] == 0.0
        assert salida["projected_total_usd"] == 0.0
