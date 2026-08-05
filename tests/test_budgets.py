"""Pruebas del motor de presupuestos."""

from __future__ import annotations

from datetime import date

import pytest

from finops.analytics.budgets import (
    STATUS_CRITICAL,
    STATUS_EXCEEDED,
    STATUS_OK,
    STATUS_WARNING,
    burn_rate_summary,
    evaluate_all,
    evaluate_budget,
    is_active,
    matches_scope,
    next_period_start,
    period_bounds,
    period_label,
    scope_label,
)
from finops.analytics.forecast import ForecastPoint


def registros(dias: int, costo_diario: float, mes: int = 3, **extra) -> list[dict]:
    return [
        {"usage_date": date(2026, mes, d), "total_cost_usd": costo_diario, **extra}
        for d in range(1, dias + 1)
    ]


class TestPeriodos:
    def test_mensual(self):
        assert period_bounds("monthly", date(2026, 2, 14)) == (date(2026, 2, 1), date(2026, 2, 28))

    def test_mensual_en_ano_bisiesto(self):
        assert period_bounds("monthly", date(2028, 2, 14))[1] == date(2028, 2, 29)

    def test_trimestral(self):
        assert period_bounds("quarterly", date(2026, 5, 20)) == (date(2026, 4, 1), date(2026, 6, 30))

    def test_anual(self):
        assert period_bounds("yearly", date(2026, 7, 1)) == (date(2026, 1, 1), date(2026, 12, 31))

    def test_etiquetas(self):
        assert period_label("monthly", date(2026, 3, 1)) == "2026-03"
        assert period_label("quarterly", date(2026, 4, 1)) == "2026-Q2"
        assert period_label("yearly", date(2026, 1, 1)) == "2026"

    def test_inicio_del_siguiente_periodo(self):
        assert next_period_start("monthly", date(2026, 3, 15)) == date(2026, 4, 1)


class TestAmbito:
    def test_ambito_vacio_abarca_todo(self):
        assert matches_scope({"team": "X"}, {}) is True
        assert matches_scope({"team": "X"}, None) is True

    def test_coincidencia_simple(self):
        assert matches_scope({"team": "Datos"}, {"team": "Datos"}) is True
        assert matches_scope({"team": "Otro"}, {"team": "Datos"}) is False

    def test_comodin(self):
        assert matches_scope({"sku_group": "SERVERLESS_SQL"}, {"sku_group": "SERVERLESS*"}) is True

    def test_lista_es_or(self):
        assert matches_scope({"environment": "QA"}, {"environment": ["PRD", "QA"]}) is True

    def test_clave_ausente_no_coincide(self):
        assert matches_scope({"team": "X"}, {"cost_center": "CC-1"}) is False

    def test_multiples_claves_es_and(self):
        registro = {"team": "Datos", "environment": "PRD"}
        assert matches_scope(registro, {"team": "Datos", "environment": "PRD"}) is True
        assert matches_scope(registro, {"team": "Datos", "environment": "DEV"}) is False

    def test_etiqueta_legible(self):
        assert scope_label({}) == "ORGANIZACION"
        assert scope_label({"team": "Datos", "environment": "PRD"}) == "environment=PRD & team=Datos"


class TestVigencia:
    def test_sin_limites_siempre_activo(self):
        assert is_active({}, date(2026, 3, 1)) is True

    def test_respeta_effective_from(self):
        presupuesto = {"effective_from": "2026-04-01"}
        assert is_active(presupuesto, date(2026, 3, 1)) is False
        assert is_active(presupuesto, date(2026, 4, 5)) is True

    def test_respeta_effective_to(self):
        presupuesto = {"effective_to": "2026-02-28"}
        assert is_active(presupuesto, date(2026, 3, 1)) is False


class TestEvaluacion:
    PRESUPUESTO = {
        "id": "p1", "name": "Prueba", "scope": {}, "period": "monthly",
        "amount_usd": 3000.0, "thresholds_pct": [50, 80, 100], "owner_email": "a@b.c",
    }

    def test_consumo_a_mitad_de_mes(self):
        estado = evaluate_budget(self.PRESUPUESTO, registros(15, 100.0), as_of=date(2026, 3, 15))
        assert estado.actual_cost_usd == pytest.approx(1500.0)
        assert estado.consumed_pct == pytest.approx(50.0)
        assert estado.elapsed_days == 15
        assert estado.remaining_days == 16
        assert estado.avg_daily_cost_usd == pytest.approx(100.0)

    def test_proyeccion_sin_pronostico(self):
        estado = evaluate_budget(self.PRESUPUESTO, registros(15, 100.0), as_of=date(2026, 3, 15))
        assert estado.projected_total_usd == pytest.approx(3100.0)
        assert estado.is_on_track is False

    def test_proyeccion_con_pronostico(self):
        pronostico = [
            ForecastPoint(date(2026, 3, 15 + h), 50.0, 40.0, 60.0, h) for h in range(1, 17)
        ]
        estado = evaluate_budget(
            self.PRESUPUESTO, registros(15, 100.0), as_of=date(2026, 3, 15), forecast_points=pronostico
        )
        assert estado.forecast_remaining_usd == pytest.approx(800.0)
        assert estado.projected_total_usd == pytest.approx(2300.0)
        assert estado.is_on_track is True
        assert estado.details["forecast_used"] is True

    def test_estado_excedido(self):
        estado = evaluate_budget(self.PRESUPUESTO, registros(31, 120.0), as_of=date(2026, 3, 31))
        assert estado.consumed_pct > 100
        assert estado.status == STATUS_EXCEEDED

    def test_estado_ok(self):
        estado = evaluate_budget(self.PRESUPUESTO, registros(10, 20.0), as_of=date(2026, 3, 10))
        assert estado.status == STATUS_OK

    def test_estado_advertencia_al_cruzar_el_primer_umbral(self):
        estado = evaluate_budget(
            {**self.PRESUPUESTO, "thresholds_pct": [50, 90, 100]},
            registros(10, 165.0),  # 1650 de 3000 = 55%
            as_of=date(2026, 3, 10),
        )
        assert estado.consumed_pct == pytest.approx(55.0)
        assert estado.threshold_reached_pct == 50.0
        assert estado.status in {STATUS_WARNING, STATUS_CRITICAL}

    def test_umbral_alcanzado_es_el_mayor_cruzado(self):
        estado = evaluate_budget(self.PRESUPUESTO, registros(29, 100.0), as_of=date(2026, 3, 29))
        assert estado.consumed_pct == pytest.approx(96.667, abs=0.01)
        assert estado.threshold_reached_pct == 80.0

    def test_filtra_por_ambito(self):
        datos = [
            *registros(10, 100.0, team="Datos"),
            *registros(10, 500.0, team="Otro"),
        ]
        presupuesto = {**self.PRESUPUESTO, "scope": {"team": "Datos"}}
        estado = evaluate_budget(presupuesto, datos, as_of=date(2026, 3, 10))
        assert estado.actual_cost_usd == pytest.approx(1000.0)
        assert estado.scope_label == "team=Datos"

    def test_ignora_registros_fuera_del_periodo(self):
        datos = [*registros(10, 100.0, mes=3), *registros(10, 999.0, mes=2)]
        estado = evaluate_budget(self.PRESUPUESTO, datos, as_of=date(2026, 3, 10))
        assert estado.actual_cost_usd == pytest.approx(1000.0)

    def test_dias_para_agotar(self):
        estado = evaluate_budget(self.PRESUPUESTO, registros(10, 100.0), as_of=date(2026, 3, 10))
        # Quedan 2000 USD a un ritmo de 100/dia.
        assert estado.days_to_exhaustion == 20

    def test_gasto_requerido_para_no_exceder(self):
        estado = evaluate_budget(self.PRESUPUESTO, registros(15, 100.0), as_of=date(2026, 3, 15))
        # 1500 restantes en 16 dias.
        assert estado.required_daily_cost_usd == pytest.approx(93.75)

    def test_sin_consumo(self):
        estado = evaluate_budget(self.PRESUPUESTO, [], as_of=date(2026, 3, 15))
        assert estado.actual_cost_usd == 0.0
        assert estado.consumed_pct == 0.0
        assert estado.status == STATUS_OK

    def test_fila_serializable(self):
        estado = evaluate_budget(self.PRESUPUESTO, registros(10, 100.0), as_of=date(2026, 3, 10))
        fila = estado.to_row()
        assert isinstance(fila["scope"], dict)
        assert all(isinstance(v, str) for v in fila["details"].values())


class TestEvaluateAll:
    def test_ordena_por_consumo_descendente(self):
        config = {
            "budgets": [
                {"id": "chico", "name": "chico", "scope": {"team": "A"}, "period": "monthly", "amount_usd": 10_000},
                {"id": "grande", "name": "grande", "scope": {"team": "B"}, "period": "monthly", "amount_usd": 1_000},
            ]
        }
        datos = [*registros(10, 100.0, team="A"), *registros(10, 90.0, team="B")]
        estados = evaluate_all(config, datos, as_of=date(2026, 3, 10))
        assert [e.budget_id for e in estados] == ["grande", "chico"]

    def test_omite_presupuestos_fuera_de_vigencia(self):
        config = {
            "budgets": [
                {"id": "viejo", "scope": {}, "period": "monthly", "amount_usd": 100, "effective_to": "2026-01-31"},
                {"id": "vigente", "scope": {}, "period": "monthly", "amount_usd": 100},
            ]
        }
        estados = evaluate_all(config, registros(5, 10.0), as_of=date(2026, 3, 5))
        assert [e.budget_id for e in estados] == ["vigente"]

    def test_configuracion_vacia(self):
        assert evaluate_all({}, [], as_of=date(2026, 3, 1)) == []


class TestResumenDeRitmo:
    def test_menciona_el_exceso(self):
        estado = evaluate_budget(
            {"id": "p", "name": "P", "scope": {}, "period": "monthly", "amount_usd": 1000},
            registros(31, 100.0), as_of=date(2026, 3, 31),
        )
        texto = burn_rate_summary(estado)
        assert "excedido" in texto.lower()

    def test_menciona_dias_para_agotar(self):
        estado = evaluate_budget(
            {"id": "p", "name": "P", "scope": {}, "period": "monthly", "amount_usd": 1200},
            registros(10, 100.0), as_of=date(2026, 3, 10),
        )
        assert "agota" in burn_rate_summary(estado)
