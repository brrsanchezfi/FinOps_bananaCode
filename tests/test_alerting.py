"""Pruebas de reglas de alerta, formateo y despacho."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pytest

from finops.alerting.dispatcher import DispatchReport, alerts_to_rows, apply_limit, dispatch, filter_new
from finops.alerting.notifier import (
    DeliveryResult,
    NoopChannel,
    TableChannel,
    build_channels,
    format_digest,
    format_plain,
    format_slack,
    format_teams,
    should_route,
)
from finops.alerting.rules import (
    Alert,
    anomaly_alerts,
    budget_alerts,
    build_all,
    daily_spike_alerts,
    fingerprint,
    forecast_overrun_alerts,
    meets_severity,
    new_expensive_entity_alerts,
    pipeline_health_alerts,
    quality_alerts,
    tag_coverage_alerts,
)
from finops.analytics.anomaly import AnomalyResult
from finops.analytics.budgets import evaluate_budget


def alerta(severidad="high", huella="abc", rule_id="TEST") -> Alert:
    return Alert(
        rule_id=rule_id, severity=severidad, title="titulo", message="mensaje",
        fingerprint=huella, scope="ORGANIZACION", event_date=date(2026, 3, 1),
    )


class TestHuella:
    def test_es_estable(self):
        assert fingerprint("A", 1, None) == fingerprint("A", 1, None)

    def test_cambia_con_las_partes(self):
        assert fingerprint("A", 1) != fingerprint("A", 2)

    def test_longitud_fija(self):
        assert len(fingerprint("x")) == 16


class TestSeveridad:
    @pytest.mark.parametrize(
        ("sev", "minimo", "esperado"),
        [("critical", "high", True), ("low", "medium", False), ("medium", "medium", True)],
    )
    def test_comparacion(self, sev, minimo, esperado):
        assert meets_severity(sev, minimo) is esperado


class TestAlertasDePresupuesto:
    def _estado(self, gastado, monto=1000.0, umbrales=(50, 80, 100)):
        registros = [{"usage_date": date(2026, 3, d), "total_cost_usd": gastado / 10} for d in range(1, 11)]
        return evaluate_budget(
            {"id": "p1", "name": "Presupuesto", "scope": {}, "period": "monthly",
             "amount_usd": monto, "thresholds_pct": list(umbrales), "owner_email": "a@b.c"},
            registros, as_of=date(2026, 3, 10),
        )

    def test_genera_alerta_al_cruzar_umbral(self):
        alertas = budget_alerts([self._estado(800.0)], {"budget_threshold": {"enabled": True}})
        assert len(alertas) == 1
        assert alertas[0].rule_id == "BUDGET_THRESHOLD"
        assert alertas[0].severity == "medium"
        assert alertas[0].threshold_value == 80.0

    def test_severidad_critica_al_exceder(self):
        alertas = budget_alerts([self._estado(1100.0)], {"budget_threshold": {"enabled": True}})
        assert alertas[0].severity == "critical"

    def test_sin_alerta_bajo_los_umbrales(self):
        assert budget_alerts([self._estado(100.0)], {"budget_threshold": {"enabled": True}}) == []

    def test_regla_deshabilitada(self):
        assert budget_alerts([self._estado(1100.0)], {"budget_threshold": {"enabled": False}}) == []

    def test_huella_distingue_umbral(self):
        a80 = budget_alerts([self._estado(800.0)], {})[0]
        a100 = budget_alerts([self._estado(1100.0)], {})[0]
        assert a80.fingerprint != a100.fingerprint

    def test_incluye_destinatario(self):
        assert budget_alerts([self._estado(800.0)], {})[0].owner_email == "a@b.c"


class TestForecastOverrun:
    def _estado_con_proyeccion(self, diario, monto):
        registros = [{"usage_date": date(2026, 3, d), "total_cost_usd": diario} for d in range(1, 11)]
        return evaluate_budget(
            {"id": "p1", "name": "P", "scope": {}, "period": "monthly", "amount_usd": monto},
            registros, as_of=date(2026, 3, 10),
        )

    def test_alerta_preventiva(self):
        # 10 dias a 100 = 1000 ejecutado; proyeccion 3100 sobre presupuesto 2000.
        alertas = forecast_overrun_alerts([self._estado_con_proyeccion(100.0, 2000.0)], {})
        assert len(alertas) == 1
        assert alertas[0].rule_id == "FORECAST_OVERRUN"
        assert alertas[0].severity == "high"

    def test_no_duplica_cuando_ya_esta_excedido(self):
        assert forecast_overrun_alerts([self._estado_con_proyeccion(100.0, 500.0)], {}) == []

    def test_sin_alerta_si_va_en_linea(self):
        assert forecast_overrun_alerts([self._estado_con_proyeccion(100.0, 10_000.0)], {}) == []


class TestAlertasDeAnomalia:
    def _anomalia(self, severidad="high", direccion="SPIKE"):
        return AnomalyResult(
            series_key="team=Datos", dimension="team", dimension_value="Datos",
            usage_date=date(2026, 3, 10), actual_cost_usd=500.0, expected_cost_usd=100.0,
            deviation_usd=400.0, pct_change=4.0, score=7.5, direction=direccion,
            severity=severidad, method="mad", baseline_points=20, baseline_window_days=28,
        )

    def test_genera_alerta_para_pico(self):
        alertas = anomaly_alerts([self._anomalia()], {"cost_anomaly": {"min_severity": "high"}})
        assert len(alertas) == 1
        assert alertas[0].metric_value == 500.0

    def test_filtra_por_severidad(self):
        assert anomaly_alerts([self._anomalia("medium")], {"cost_anomaly": {"min_severity": "high"}}) == []

    def test_las_caidas_no_se_notifican(self):
        assert anomaly_alerts([self._anomalia(direccion="DROP")], {}) == []


class TestAlertasDeSerie:
    def test_pico_diario(self):
        totales = [(date(2026, 3, d), 100.0) for d in range(1, 9)]
        totales[-1] = (date(2026, 3, 8), 400.0)
        alertas = daily_spike_alerts(totales, {"daily_spend_spike": {"pct_increase": 0.35, "min_cost_usd": 100.0}})
        assert len(alertas) == 1
        assert alertas[0].severity == "critical"

    def test_serie_corta_no_alerta(self):
        assert daily_spike_alerts([(date(2026, 3, 1), 100.0)], {}) == []

    def test_costo_bajo_no_alerta(self):
        totales = [(date(2026, 3, d), 1.0) for d in range(1, 9)]
        totales[-1] = (date(2026, 3, 8), 50.0)
        assert daily_spike_alerts(totales, {"daily_spend_spike": {"min_cost_usd": 200.0}}) == []

    def test_variacion_pequena_no_alerta(self):
        totales = [(date(2026, 3, d), 1000.0) for d in range(1, 9)]
        totales[-1] = (date(2026, 3, 8), 1050.0)
        assert daily_spike_alerts(totales, {"daily_spend_spike": {"pct_increase": 0.35, "min_cost_usd": 100.0}}) == []


class TestNuevasEntidades:
    def test_detecta_recurso_nuevo_y_costoso(self):
        entidades = [{
            "entity_key": "JOB:9", "entity_name": "etl-nuevo", "entity_type": "JOB",
            "first_seen_date": date(2026, 3, 8), "cost_usd": 600.0, "team": "Datos",
        }]
        alertas = new_expensive_entity_alerts(
            entidades, {"new_expensive_entity": {"min_cost_usd": 150.0, "lookback_days": 7}},
            as_of=date(2026, 3, 10),
        )
        assert len(alertas) == 1
        assert alertas[0].severity == "high"

    def test_ignora_entidades_antiguas(self):
        entidades = [{"entity_key": "JOB:1", "first_seen_date": date(2026, 1, 1), "cost_usd": 5000.0}]
        assert new_expensive_entity_alerts(entidades, {}, as_of=date(2026, 3, 10)) == []

    def test_ignora_costo_bajo(self):
        entidades = [{"entity_key": "JOB:1", "first_seen_date": date(2026, 3, 9), "cost_usd": 5.0}]
        assert new_expensive_entity_alerts(entidades, {}, as_of=date(2026, 3, 10)) == []


class TestGobiernoYSalud:
    def test_cobertura_baja(self):
        cobertura = {"dimension": "cost_center", "coverage_ratio": 0.5, "unattributed_cost_usd": 1000.0}
        alertas = tag_coverage_alerts(cobertura, {"tag_coverage_drop": {"min_coverage_ratio": 0.8}})
        assert len(alertas) == 1
        assert alertas[0].severity == "high"

    def test_cobertura_suficiente(self):
        cobertura = {"dimension": "cost_center", "coverage_ratio": 0.95}
        assert tag_coverage_alerts(cobertura, {"tag_coverage_drop": {"min_coverage_ratio": 0.8}}) == []

    def test_pipeline_fallido(self):
        metricas = [{"stage": "bronze.usage", "status": "error", "error_message": "boom"}]
        alertas = pipeline_health_alerts(metricas, {}, run_id="r1")
        assert alertas[0].severity == "critical"
        assert "bronze.usage" in alertas[0].message

    def test_pipeline_sano(self):
        assert pipeline_health_alerts([{"stage": "x", "status": "ok"}], {}) == []

    def test_calidad_fallida(self):
        alertas = quality_alerts([{"check": "freshness", "severity": "error"}], run_id="r1")
        assert alertas[0].rule_id == "DATA_QUALITY"

    def test_calidad_solo_advertencias(self):
        assert quality_alerts([{"check": "x", "severity": "warning"}]) == []


class TestBuildAll:
    def test_ordena_por_severidad(self):
        anomalia = AnomalyResult(
            "s", "team", "Datos", date(2026, 3, 10), 500.0, 100.0, 400.0, 4.0, 9.0,
            "SPIKE", "critical", "mad", 20, 28,
        )
        alertas = build_all(
            anomalies=[anomalia],
            coverage={"dimension": "cost_center", "coverage_ratio": 0.7, "unattributed_cost_usd": 10.0},
            alerting_cfg={"rules": {}},
            as_of=date(2026, 3, 10),
        )
        severidades = [a.severity_rank for a in alertas]
        assert severidades == sorted(severidades, reverse=True)

    def test_sin_insumos_no_genera_alertas(self):
        assert build_all(alerting_cfg={"rules": {}}) == []


class TestFormateo:
    def test_texto_plano(self):
        texto = format_plain(alerta("critical"), env="prd")
        assert "CRITICA" in texto
        assert "PRD" in texto

    def test_teams_es_message_card(self):
        payload = format_teams(alerta(), env="prd", dashboard_url="https://x")
        assert payload["@type"] == "MessageCard"
        assert payload["themeColor"] == "F7630C"
        assert payload["potentialAction"][0]["targets"][0]["uri"] == "https://x"

    def test_slack_es_block_kit(self):
        payload = format_slack(alerta("critical"), env="dev")
        assert payload["blocks"][0]["type"] == "header"
        assert any(b["type"] == "section" for b in payload["blocks"])

    def test_slack_sin_boton_si_no_hay_url(self):
        payload = format_slack(alerta())
        assert all(b["type"] != "actions" for b in payload["blocks"])

    def test_resumen_agrupa(self):
        alertas = [alerta("high", f"h{i}") for i in range(30)]
        texto = format_digest(alertas, env="prd")
        assert "30 alertas" in texto
        assert "y 5 mas" in texto


class TestCanales:
    def test_noop_siempre_entrega(self):
        canal = NoopChannel()
        assert canal.send(alerta()).delivered is True

    def test_enrutamiento_por_severidad(self):
        canal = TableChannel("tabla", "high")
        assert should_route(alerta("critical"), canal) is True
        assert should_route(alerta("low"), canal) is False

    def test_build_channels_omite_deshabilitados(self):
        canales = build_channels([{"name": "x", "type": "table", "enabled": False}])
        assert canales == []

    def test_build_channels_dry_run_convierte_a_noop(self):
        canales = build_channels(
            [{"name": "t", "type": "webhook", "enabled": True, "url": "https://x"}], dry_run=True
        )
        assert isinstance(canales[0], NoopChannel)

    def test_webhook_sin_secreto_se_omite(self):
        canales = build_channels(
            [{"name": "t", "type": "webhook", "enabled": True, "secret_scope": "s", "secret_key": "k"}]
        )
        assert canales == []

    def test_webhook_usa_el_resolvedor_de_secretos(self):
        canales = build_channels(
            [{"name": "t", "type": "webhook", "enabled": True, "secret_scope": "s", "secret_key": "k"}],
            secret_resolver=lambda scope, key: f"https://hook/{scope}/{key}",
        )
        assert len(canales) == 1
        assert canales[0].name == "t"

    def test_resolvedor_que_falla_no_rompe(self):
        def resolver_roto(scope, key):
            raise RuntimeError("sin permisos")

        canales = build_channels(
            [{"name": "t", "type": "webhook", "enabled": True, "secret_scope": "s", "secret_key": "k"}],
            secret_resolver=resolver_roto,
        )
        assert canales == []

    def test_la_url_no_se_expone_en_el_objeto_publico(self):
        canales = build_channels(
            [{"name": "t", "type": "webhook", "enabled": True, "url": "https://secreto"}]
        )
        assert not any("secreto" in str(v) for k, v in vars(canales[0]).items() if not k.startswith("_"))


class TestDeduplicacion:
    def test_alerta_nueva_pasa(self):
        nuevas, suprimidas = filter_new([alerta()], {}, cooldown_hours=24)
        assert len(nuevas) == 1
        assert suprimidas == []

    def test_alerta_en_enfriamiento_se_suprime(self):
        ahora = datetime(2026, 3, 10, 12, 0, tzinfo=timezone.utc)
        historial = {"abc": ahora - timedelta(hours=2)}
        nuevas, suprimidas = filter_new([alerta()], historial, cooldown_hours=24, now=ahora)
        assert nuevas == []
        assert len(suprimidas) == 1

    def test_alerta_fuera_del_enfriamiento_pasa(self):
        ahora = datetime(2026, 3, 10, 12, 0, tzinfo=timezone.utc)
        historial = {"abc": ahora - timedelta(hours=48)}
        nuevas, _ = filter_new([alerta()], historial, cooldown_hours=24, now=ahora)
        assert len(nuevas) == 1

    def test_duplicados_en_el_mismo_lote(self):
        nuevas, suprimidas = filter_new([alerta(), alerta()], {}, cooldown_hours=24)
        assert len(nuevas) == 1
        assert len(suprimidas) == 1

    def test_historial_naive_se_interpreta_como_utc(self):
        ahora = datetime(2026, 3, 10, 12, 0, tzinfo=timezone.utc)
        historial = {"abc": datetime(2026, 3, 10, 11, 0)}
        nuevas, _ = filter_new([alerta()], historial, cooldown_hours=24, now=ahora)
        assert nuevas == []


class TestLimite:
    def test_conserva_las_mas_severas(self):
        alertas = [alerta("low", "a"), alerta("critical", "b"), alerta("medium", "c")]
        conservadas, excedentes = apply_limit(alertas, 1)
        assert conservadas[0].severity == "critical"
        assert len(excedentes) == 2

    def test_sin_limite(self):
        alertas = [alerta("low", f"h{i}") for i in range(5)]
        conservadas, excedentes = apply_limit(alertas, 0)
        assert len(conservadas) == 5
        assert excedentes == []


class TestDispatch:
    def test_flujo_completo(self):
        canal = NoopChannel("n", "low")
        reporte = dispatch(
            [alerta("critical", "a"), alerta("low", "b")],
            [canal],
            history={},
            config={"enabled": True, "min_severity": "medium", "cooldown_hours": 24, "max_alerts_per_run": 10},
        )
        assert reporte.generated == 2
        assert reporte.dispatched == 1
        assert reporte.suppressed_severity == 1
        assert len(reporte.deliveries) == 1

    def test_deshabilitado_no_despacha(self):
        reporte = dispatch([alerta()], [NoopChannel()], config={"enabled": False})
        assert reporte.dispatched == 0
        assert reporte.suppressed_severity == 1

    def test_respeta_severidad_del_canal(self):
        canal = NoopChannel("solo_criticas", "critical")
        reporte = dispatch([alerta("high", "a")], [canal], config={"min_severity": "low"})
        assert reporte.dispatched == 1
        assert reporte.deliveries == []   # ningun canal acepto la alerta

    def test_resumen_legible(self):
        reporte = dispatch([alerta()], [NoopChannel()], config={"min_severity": "low"})
        assert "generadas=1" in reporte.summary()


class TestFilasDeAlerta:
    def test_incluye_despachadas_y_suprimidas(self):
        despachada = alerta("high", "a")
        suprimida = alerta("high", "b")
        entregas = [DeliveryResult("tabla", "a", True, "ok")]
        filas = alerts_to_rows([despachada], entregas, run_id="r1", env="dev", suppressed=[suprimida])
        estados = {f["fingerprint"]: f["dispatch_status"] for f in filas}
        assert estados == {"a": "dispatched", "b": "suppressed"}
        assert filas[0]["channels"] == "tabla"
        assert filas[0]["delivered"] is True

    def test_contexto_serializado_a_texto(self):
        a = alerta()
        a.context = {"n": 5, "f": 1.5}
        filas = alerts_to_rows([a], [], run_id="r", env="dev")
        assert all(isinstance(v, str) for v in filas[0]["context"].values())

    def test_reporte_vacio(self):
        assert DispatchReport().summary().startswith("generadas=0")
