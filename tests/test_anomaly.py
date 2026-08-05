"""Pruebas de deteccion de anomalias de costo."""

from __future__ import annotations

from datetime import date, timedelta

import pytest
from tests.conftest import serie_con_ruido, serie_estable

from finops.analytics.anomaly import (
    classify_severity,
    detect_many,
    detect_series,
    group_points,
    median_absolute_deviation,
    robust_score,
)


class TestEstadisticaRobusta:
    def test_mad_de_serie_conocida(self):
        assert median_absolute_deviation([1, 2, 3, 4, 5]) == 1.0

    def test_mad_vacia(self):
        assert median_absolute_deviation([]) == 0.0

    def test_score_mad_detecta_pico(self):
        base = [100.0] * 10 + [110.0, 90.0, 105.0, 95.0]
        score, esperado, _ = robust_score(400.0, base, "mad")
        assert score > 10
        assert esperado == pytest.approx(100.0, abs=5)

    def test_score_zscore(self):
        base = [10.0, 12.0, 11.0, 9.0, 10.0]
        score, esperado, dispersion = robust_score(30.0, base, "zscore")
        assert score > 3
        assert dispersion > 0
        assert esperado == pytest.approx(10.4, abs=0.1)

    def test_serie_constante_no_marca_el_mismo_valor(self):
        score, _, _ = robust_score(100.0, [100.0] * 20, "mad")
        assert score == 0.0

    def test_serie_constante_marca_cambio(self):
        score, _, _ = robust_score(500.0, [100.0] * 20, "mad")
        assert score == pytest.approx(99.0)

    def test_base_vacia(self):
        assert robust_score(10.0, [], "mad") == (0.0, 10.0, 0.0)

    @pytest.mark.parametrize(
        ("score", "esperado"),
        [(9.0, "critical"), (6.5, "high"), (5.0, "medium"), (3.0, "low"), (-9.0, "critical")],
    )
    def test_severidad(self, score, esperado):
        assert classify_severity(score, {"critical": 8.0, "high": 6.0, "medium": 4.5}) == esperado


class TestDetectSeries:
    def _serie_con_pico(self, dias=40, base=100.0, pico=500.0):
        puntos = serie_con_ruido(dias, base, 3.0)
        puntos[-1] = (puntos[-1][0], pico)
        return puntos

    def test_detecta_pico_al_final(self):
        resultados = detect_series(
            self._serie_con_pico(), min_avg_cost_usd=10.0, day_of_week_aware=False
        )
        assert len(resultados) == 1
        anomalia = resultados[0]
        assert anomalia.direction == "SPIKE"
        assert anomalia.actual_cost_usd == 500.0
        assert anomalia.pct_change > 3.0
        assert anomalia.severity in {"high", "critical"}

    def test_serie_estable_no_produce_anomalias(self):
        assert detect_series(serie_estable(60, 100.0), min_avg_cost_usd=10.0) == []

    def test_historia_insuficiente(self):
        assert detect_series(serie_estable(5, 100.0), min_history_days=14) == []

    def test_serie_de_bajo_costo_se_ignora(self):
        puntos = self._serie_con_pico(base=2.0, pico=20.0)
        assert detect_series(puntos, min_avg_cost_usd=50.0) == []

    def test_cambio_relativo_minimo_filtra_ruido(self):
        # Un pico grande en score pero pequeno en porcentaje no debe reportarse.
        puntos = serie_estable(40, 1000.0)
        puntos[-1] = (puntos[-1][0], 1050.0)
        resultados = detect_series(
            puntos, min_avg_cost_usd=10.0, min_pct_change=0.30, day_of_week_aware=False
        )
        assert resultados == []

    def test_detecta_caida(self):
        puntos = serie_con_ruido(40, 200.0, 4.0)
        puntos[-1] = (puntos[-1][0], 10.0)
        resultados = detect_series(puntos, min_avg_cost_usd=10.0, day_of_week_aware=False)
        assert len(resultados) == 1
        assert resultados[0].direction == "DROP"
        assert resultados[0].deviation_usd < 0

    def test_evaluate_last_n_days_acota_la_evaluacion(self):
        puntos = serie_con_ruido(60, 100.0, 3.0)
        puntos[20] = (puntos[20][0], 900.0)   # pico antiguo
        puntos[-1] = (puntos[-1][0], 800.0)   # pico reciente
        resultados = detect_series(
            puntos, min_avg_cost_usd=10.0, day_of_week_aware=False, evaluate_last_n_days=2
        )
        assert len(resultados) == 1
        assert resultados[0].usage_date == puntos[-1][0]

    def test_estacionalidad_semanal_evita_falso_positivo(self):
        # Lunes a viernes alto, fin de semana bajo: el sabado no es una anomalia.
        inicio = date(2026, 1, 5)  # lunes
        puntos = []
        for i in range(42):
            fecha = inicio + timedelta(days=i)
            puntos.append((fecha, 30.0 if fecha.weekday() >= 5 else 300.0))

        # Con umbral laxo el contraste es visible: sin conciencia de dia de la
        # semana los fines de semana se marcan; con ella, no.
        con_estacionalidad = detect_series(
            puntos, min_avg_cost_usd=10.0, day_of_week_aware=True, score_threshold=1.5
        )
        sin_estacionalidad = detect_series(
            puntos, min_avg_cost_usd=10.0, day_of_week_aware=False, score_threshold=1.5
        )
        assert con_estacionalidad == []
        assert len(sin_estacionalidad) > 0
        assert all(a.direction == "DROP" for a in sin_estacionalidad)

    def test_puntos_vacios(self):
        assert detect_series([]) == []

    def test_metadatos_de_la_serie(self):
        resultados = detect_series(
            self._serie_con_pico(), series_key="team=Datos", dimension="team",
            dimension_value="Datos", min_avg_cost_usd=10.0, day_of_week_aware=False,
        )
        assert resultados[0].dimension == "team"
        assert resultados[0].dimension_value == "Datos"
        assert resultados[0].baseline_points > 0

    def test_to_row_es_serializable(self):
        resultados = detect_series(self._serie_con_pico(), min_avg_cost_usd=10.0, day_of_week_aware=False)
        fila = resultados[0].to_row()
        assert set(fila) >= {"usage_date", "actual_cost_usd", "expected_cost_usd", "score", "severity"}


class TestDetectMany:
    def test_procesa_varias_series(self):
        estable = serie_estable(40, 100.0)
        con_pico = serie_con_ruido(40, 100.0, 3.0)
        con_pico[-1] = (con_pico[-1][0], 600.0)

        resultados = detect_many(
            {"equipoA": estable, "equipoB": con_pico},
            dimension="team",
            config={"min_avg_cost_usd": 10.0, "day_of_week_aware": False, "min_history_days": 14},
        )
        assert [r.dimension_value for r in resultados] == ["equipoB"]

    def test_config_vacia_usa_valores_por_defecto(self):
        assert detect_many({}, dimension="team", config=None) == []


class TestGroupPoints:
    def test_agrupa_y_suma_duplicados(self):
        registros = [
            {"dim": "A", "usage_date": date(2026, 1, 1), "total_cost_usd": 10.0},
            {"dim": "A", "usage_date": date(2026, 1, 1), "total_cost_usd": 5.0},
            {"dim": "B", "usage_date": date(2026, 1, 2), "total_cost_usd": 7.0},
        ]
        series = group_points(registros, key_field="dim")
        assert series["A"] == [(date(2026, 1, 1), 15.0)]
        assert series["B"] == [(date(2026, 1, 2), 7.0)]

    def test_ignora_filas_incompletas(self):
        registros = [
            {"dim": None, "usage_date": date(2026, 1, 1), "total_cost_usd": 1.0},
            {"dim": "A", "usage_date": None, "total_cost_usd": 1.0},
            {"dim": "A", "usage_date": "2026-01-01", "total_cost_usd": 1.0},
        ]
        assert group_points(registros, key_field="dim") == {}

    def test_ordena_por_fecha(self):
        registros = [
            {"dim": "A", "usage_date": date(2026, 1, 3), "total_cost_usd": 3.0},
            {"dim": "A", "usage_date": date(2026, 1, 1), "total_cost_usd": 1.0},
        ]
        fechas = [f for f, _ in group_points(registros, key_field="dim")["A"]]
        assert fechas == sorted(fechas)
