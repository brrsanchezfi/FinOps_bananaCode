"""Pruebas del motor de recomendaciones de optimizacion."""

from __future__ import annotations

from datetime import date

import pytest

from finops.analytics.optimization import (
    RULES,
    build_profile_from_rows,
    evaluate_all,
    evaluate_profile,
    rule_all_purpose_for_jobs,
    rule_failed_run_waste,
    rule_legacy_runtime,
    rule_no_autoscale,
    rule_no_autotermination,
    rule_oversized_warehouse,
    rule_photon_opportunity,
    rule_serverless_candidate,
    rule_untagged_spend,
    rule_zombie_entity,
    savings_summary,
)

CFG_COMPLETA = {
    "enabled": True,
    "min_monthly_savings_usd": 25.0,
    "lookback_days": 30,
    "rules": {
        "no_autotermination": {"enabled": True, "max_autotermination_minutes": 60, "assumed_waste_ratio": 0.15},
        "all_purpose_for_jobs": {"enabled": True, "min_runs": 5, "assumed_savings_ratio": 0.45},
        "legacy_runtime": {"enabled": True, "min_dbr_major": 14, "assumed_savings_ratio": 0.10},
        "failed_run_waste": {"enabled": True, "min_failed_runs": 3, "min_failure_ratio": 0.15},
        "zombie_entity": {"enabled": True, "idle_days": 14},
        "no_autoscale": {"enabled": True, "min_workers_fixed": 4, "assumed_savings_ratio": 0.15},
        "oversized_warehouse": {"enabled": True, "max_queries_per_hour": 5, "assumed_savings_ratio": 0.40},
        "serverless_candidate": {"enabled": True, "max_avg_runtime_minutes": 12, "min_runs": 20,
                                 "assumed_savings_ratio": 0.20},
        "untagged_spend": {"enabled": True, "min_untagged_cost_usd": 100.0},
        "photon_opportunity": {"enabled": True, "min_monthly_cost_usd": 500.0, "assumed_savings_ratio": 0.12},
    },
}


def perfil(**kwargs) -> dict:
    base = {
        "entity_key": "CLUSTER:c-1", "entity_type": "CLUSTER", "entity_id": "c-1",
        "entity_name": "cluster-analitica", "workspace_id": "111",
        "cost_usd": 3000.0, "analysis_days": 30, "sku_group": "ALL_PURPOSE",
    }
    base.update(kwargs)
    return base


class TestNoAutotermination:
    def test_sin_autoterminacion_penaliza_doble(self):
        rec = rule_no_autotermination(perfil(autotermination_minutes=0), CFG_COMPLETA["rules"]["no_autotermination"])
        assert rec is not None
        assert rec.rule_id == "NO_AUTOTERMINATION"
        assert rec.estimated_monthly_savings_usd == pytest.approx(3000 * 0.15 * 2)

    def test_umbral_alto(self):
        rec = rule_no_autotermination(perfil(autotermination_minutes=180), CFG_COMPLETA["rules"]["no_autotermination"])
        assert rec.estimated_monthly_savings_usd == pytest.approx(3000 * 0.15)

    def test_configuracion_correcta_no_genera_recomendacion(self):
        assert rule_no_autotermination(
            perfil(autotermination_minutes=30), CFG_COMPLETA["rules"]["no_autotermination"]
        ) is None

    def test_no_aplica_a_serverless(self):
        assert rule_no_autotermination(
            perfil(autotermination_minutes=0, is_serverless=True), CFG_COMPLETA["rules"]["no_autotermination"]
        ) is None

    def test_no_aplica_a_otros_tipos(self):
        assert rule_no_autotermination(
            perfil(entity_type="WAREHOUSE", autotermination_minutes=0), CFG_COMPLETA["rules"]["no_autotermination"]
        ) is None

    def test_sin_dato_de_configuracion(self):
        assert rule_no_autotermination(perfil(), CFG_COMPLETA["rules"]["no_autotermination"]) is None


class TestAllPurposeForJobs:
    def test_detecta_carga_automatizada(self):
        rec = rule_all_purpose_for_jobs(
            perfil(run_count=40, sku_group="ALL_PURPOSE"), CFG_COMPLETA["rules"]["all_purpose_for_jobs"]
        )
        assert rec.estimated_monthly_savings_usd == pytest.approx(1350.0)
        assert rec.confidence == "alta"

    def test_pocas_corridas_no_aplica(self):
        assert rule_all_purpose_for_jobs(
            perfil(run_count=2), CFG_COMPLETA["rules"]["all_purpose_for_jobs"]
        ) is None

    def test_no_aplica_si_ya_usa_jobs_compute(self):
        assert rule_all_purpose_for_jobs(
            perfil(sku_group="JOBS", run_count=40), CFG_COMPLETA["rules"]["all_purpose_for_jobs"]
        ) is None


class TestLegacyRuntime:
    def test_dbr_antiguo(self):
        rec = rule_legacy_runtime(perfil(spark_version="11.3.x-scala2.12"), CFG_COMPLETA["rules"]["legacy_runtime"])
        assert rec is not None
        assert rec.evidence["dbr_major"] == 11

    def test_dbr_actual_no_aplica(self):
        assert rule_legacy_runtime(
            perfil(spark_version="15.4.x-scala2.12"), CFG_COMPLETA["rules"]["legacy_runtime"]
        ) is None

    def test_version_no_parseable(self):
        assert rule_legacy_runtime(perfil(spark_version="custom"), CFG_COMPLETA["rules"]["legacy_runtime"]) is None

    def test_sin_version(self):
        assert rule_legacy_runtime(perfil(), CFG_COMPLETA["rules"]["legacy_runtime"]) is None


class TestFailedRunWaste:
    def test_alta_tasa_de_fallo(self):
        rec = rule_failed_run_waste(
            perfil(run_count=20, failed_run_count=8), CFG_COMPLETA["rules"]["failed_run_waste"]
        )
        assert rec.estimated_monthly_savings_usd == pytest.approx(3000 * 0.4)
        assert rec.evidence["failure_ratio"] == pytest.approx(0.4)

    def test_pocos_fallos_absolutos(self):
        assert rule_failed_run_waste(
            perfil(run_count=4, failed_run_count=2), CFG_COMPLETA["rules"]["failed_run_waste"]
        ) is None

    def test_tasa_baja(self):
        assert rule_failed_run_waste(
            perfil(run_count=100, failed_run_count=5), CFG_COMPLETA["rules"]["failed_run_waste"]
        ) is None

    def test_sin_corridas(self):
        assert rule_failed_run_waste(perfil(), CFG_COMPLETA["rules"]["failed_run_waste"]) is None


class TestZombieEntity:
    def test_recurso_inactivo(self):
        rec = rule_zombie_entity(perfil(days_since_activity=30), CFG_COMPLETA["rules"]["zombie_entity"])
        assert rec.estimated_monthly_savings_usd == pytest.approx(3000.0)
        assert rec.confidence == "alta"

    def test_recurso_activo(self):
        assert rule_zombie_entity(perfil(days_since_activity=2), CFG_COMPLETA["rules"]["zombie_entity"]) is None

    def test_sin_costo_no_hay_ahorro(self):
        assert rule_zombie_entity(
            perfil(days_since_activity=30, cost_usd=0.0), CFG_COMPLETA["rules"]["zombie_entity"]
        ) is None


class TestNoAutoscale:
    def test_cluster_fijo_grande(self):
        rec = rule_no_autoscale(
            perfil(autoscale_enabled=False, num_workers=10), CFG_COMPLETA["rules"]["no_autoscale"]
        )
        assert rec.estimated_monthly_savings_usd == pytest.approx(450.0)

    def test_con_autoescalado(self):
        assert rule_no_autoscale(
            perfil(autoscale_enabled=True, num_workers=10), CFG_COMPLETA["rules"]["no_autoscale"]
        ) is None

    def test_cluster_pequeno(self):
        assert rule_no_autoscale(
            perfil(autoscale_enabled=False, num_workers=2), CFG_COMPLETA["rules"]["no_autoscale"]
        ) is None


class TestOversizedWarehouse:
    def test_poco_trafico(self):
        rec = rule_oversized_warehouse(
            perfil(entity_type="WAREHOUSE", query_count=100, active_hours=100.0, warehouse_size="LARGE"),
            CFG_COMPLETA["rules"]["oversized_warehouse"],
        )
        assert rec.evidence["queries_per_hour"] == pytest.approx(1.0)
        assert rec.estimated_monthly_savings_usd == pytest.approx(1200.0)

    def test_trafico_alto_no_aplica(self):
        assert rule_oversized_warehouse(
            perfil(entity_type="WAREHOUSE", query_count=5000, active_hours=100.0),
            CFG_COMPLETA["rules"]["oversized_warehouse"],
        ) is None

    def test_sin_horas_activas(self):
        assert rule_oversized_warehouse(
            perfil(entity_type="WAREHOUSE", query_count=10, active_hours=0.0),
            CFG_COMPLETA["rules"]["oversized_warehouse"],
        ) is None


class TestServerlessCandidate:
    def test_job_corto_y_frecuente(self):
        rec = rule_serverless_candidate(
            perfil(entity_type="JOB", run_count=100, avg_duration_minutes=5.0),
            CFG_COMPLETA["rules"]["serverless_candidate"],
        )
        assert rec.estimated_monthly_savings_usd == pytest.approx(600.0)

    def test_job_largo_no_aplica(self):
        assert rule_serverless_candidate(
            perfil(entity_type="JOB", run_count=100, avg_duration_minutes=90.0),
            CFG_COMPLETA["rules"]["serverless_candidate"],
        ) is None

    def test_ya_serverless(self):
        assert rule_serverless_candidate(
            perfil(is_serverless=True, run_count=100, avg_duration_minutes=5.0),
            CFG_COMPLETA["rules"]["serverless_candidate"],
        ) is None


class TestUntaggedSpend:
    def test_es_hallazgo_de_gobierno_sin_ahorro(self):
        rec = rule_untagged_spend(perfil(untagged_cost_usd=500.0), CFG_COMPLETA["rules"]["untagged_spend"])
        assert rec.estimated_monthly_savings_usd == 0.0
        assert "gobierno" in rec.estimation_method

    def test_bajo_el_umbral(self):
        assert rule_untagged_spend(
            perfil(untagged_cost_usd=10.0), CFG_COMPLETA["rules"]["untagged_spend"]
        ) is None


class TestPhotonOpportunity:
    def test_carga_grande_sin_photon(self):
        rec = rule_photon_opportunity(
            perfil(sku_group="JOBS", is_photon=False, cost_usd=3000.0),
            CFG_COMPLETA["rules"]["photon_opportunity"],
        )
        assert rec.estimated_monthly_savings_usd == pytest.approx(360.0)
        assert rec.confidence == "baja"

    def test_ya_usa_photon(self):
        assert rule_photon_opportunity(
            perfil(sku_group="JOBS", is_photon=True), CFG_COMPLETA["rules"]["photon_opportunity"]
        ) is None

    def test_costo_bajo(self):
        assert rule_photon_opportunity(
            perfil(sku_group="JOBS", cost_usd=100.0), CFG_COMPLETA["rules"]["photon_opportunity"]
        ) is None

    def test_grupo_no_aplicable(self):
        assert rule_photon_opportunity(
            perfil(sku_group="ALL_PURPOSE", cost_usd=3000.0), CFG_COMPLETA["rules"]["photon_opportunity"]
        ) is None


class TestEvaluateProfile:
    def test_aplica_varias_reglas(self):
        recomendaciones = evaluate_profile(
            perfil(autotermination_minutes=0, autoscale_enabled=False, num_workers=8,
                   spark_version="10.4.x-scala2.12", run_count=50),
            CFG_COMPLETA,
        )
        ids = {r.rule_id for r in recomendaciones}
        assert {"NO_AUTOTERMINATION", "NO_AUTOSCALE", "LEGACY_RUNTIME", "ALL_PURPOSE_FOR_JOBS"} <= ids

    def test_filtra_por_ahorro_minimo(self):
        cfg = {**CFG_COMPLETA, "min_monthly_savings_usd": 100_000.0}
        assert evaluate_profile(perfil(autotermination_minutes=0), cfg) == []

    def test_gobierno_no_se_filtra_por_ahorro(self):
        cfg = {**CFG_COMPLETA, "min_monthly_savings_usd": 100_000.0}
        recomendaciones = evaluate_profile(perfil(untagged_cost_usd=500.0), cfg)
        assert [r.rule_id for r in recomendaciones] == ["UNTAGGED_SPEND"]

    def test_regla_deshabilitada(self):
        cfg = {
            **CFG_COMPLETA,
            "rules": {**CFG_COMPLETA["rules"], "no_autotermination": {"enabled": False}},
        }
        ids = {r.rule_id for r in evaluate_profile(perfil(autotermination_minutes=0), cfg)}
        assert "NO_AUTOTERMINATION" not in ids

    def test_perfil_incompleto_no_rompe(self):
        assert evaluate_profile({"entity_key": "X"}, CFG_COMPLETA) == []

    def test_severidad_escala_con_el_ahorro(self):
        alta = evaluate_profile(perfil(cost_usd=50_000.0, days_since_activity=30), CFG_COMPLETA)
        assert any(r.severity == "critical" for r in alta)


class TestEvaluateAll:
    def test_ordena_por_ahorro(self):
        perfiles = [
            perfil(entity_key="A", cost_usd=1000.0, days_since_activity=30),
            perfil(entity_key="B", cost_usd=9000.0, days_since_activity=30),
        ]
        recomendaciones = evaluate_all(perfiles, CFG_COMPLETA)
        assert recomendaciones[0].entity_key == "B"

    def test_deshabilitado_globalmente(self):
        assert evaluate_all([perfil(days_since_activity=30)], {**CFG_COMPLETA, "enabled": False}) == []


class TestResumen:
    def test_agrega_por_regla(self):
        perfiles = [perfil(entity_key=f"C:{i}", days_since_activity=30, cost_usd=1000.0) for i in range(3)]
        resumen = savings_summary(evaluate_all(perfiles, CFG_COMPLETA))
        assert resumen["total_recommendations"] == 3
        assert resumen["by_rule"]["ZOMBIE_ENTITY"]["count"] == 3
        assert resumen["total_monthly_savings_usd"] == pytest.approx(3000.0)
        assert resumen["high_confidence_savings_usd"] == pytest.approx(3000.0)


class TestPerfil:
    def test_calcula_dias_de_inactividad(self):
        salida = build_profile_from_rows(
            {"entity_key": "X", "last_activity_date": date(2026, 3, 1)},
            analysis_days=30, as_of=date(2026, 3, 20),
        )
        assert salida["days_since_activity"] == 19
        assert salida["analysis_days"] == 30


class TestRegistroDeReglas:
    def test_todas_las_reglas_tienen_configuracion_de_ejemplo(self):
        for _rule_id, (clave, _) in RULES.items():
            assert clave in CFG_COMPLETA["rules"], f"falta configuracion de prueba para '{clave}'"

    def test_las_recomendaciones_declaran_su_metodo(self):
        recomendaciones = evaluate_all([perfil(days_since_activity=30)], CFG_COMPLETA)
        assert all(r.estimation_method for r in recomendaciones)
        assert all(r.recommendation for r in recomendaciones)
