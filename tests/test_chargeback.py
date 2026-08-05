"""Pruebas de imputacion de costo (chargeback / showback)."""

from __future__ import annotations

import pytest

from finops.analytics.chargeback import (
    SHARED_UNIT,
    UNALLOCATED_UNIT,
    allocate,
    is_shared_entity,
    reconcile,
    showback_report,
)

CFG_PROPORCIONAL = {
    "allocation_dimension": "cost_center",
    "unallocated_strategy": "proportional",
    "shared_cost_strategy": "proportional",
    "shared_entity_patterns": ["*finops*", "*plataforma*"],
    "overhead_pct": 0.0,
}


def registro(unidad, costo, entidad="JOB:1", nombre="job-normal"):
    return {"cost_center": unidad, "total_cost_usd": costo, "entity_key": entidad, "entity_name": nombre}


class TestEntidadCompartida:
    def test_reconoce_por_nombre(self):
        assert is_shared_entity({"entity_name": "job-finops-daily"}, ["*finops*"]) is True

    def test_no_coincide(self):
        assert is_shared_entity({"entity_name": "etl-ventas"}, ["*finops*"]) is False

    def test_sin_patrones(self):
        assert is_shared_entity({"entity_name": "x"}, None) is False

    def test_busca_en_varios_campos(self):
        assert is_shared_entity({"entity_key": "JOB:1", "project": "Plataforma"}, ["*plataforma*"]) is True


class TestAsignacionProporcional:
    def test_reparte_no_atribuido_segun_costo_directo(self):
        registros = [
            registro("CC-A", 600.0),
            registro("CC-B", 400.0),
            registro("SIN_ASIGNAR", 100.0),
        ]
        lineas = allocate(registros, period="2026-03", config=CFG_PROPORCIONAL)
        por_unidad = {line.unit: line for line in lineas}
        assert por_unidad["CC-A"].allocated_unallocated_usd == pytest.approx(60.0)
        assert por_unidad["CC-B"].allocated_unallocated_usd == pytest.approx(40.0)
        assert por_unidad["CC-A"].total_chargeback_usd == pytest.approx(660.0)

    def test_reparte_costo_compartido(self):
        registros = [
            registro("CC-A", 750.0),
            registro("CC-B", 250.0),
            registro("CC-A", 200.0, "JOB:9", "job-finops-daily"),  # compartido
        ]
        lineas = allocate(registros, period="2026-03", config=CFG_PROPORCIONAL)
        por_unidad = {line.unit: line for line in lineas}
        assert por_unidad["CC-A"].allocated_shared_usd == pytest.approx(150.0)
        assert por_unidad["CC-B"].allocated_shared_usd == pytest.approx(50.0)

    def test_suma_igual_al_total(self):
        registros = [registro("CC-A", 600.0), registro("CC-B", 400.0), registro("SIN_ASIGNAR", 250.0)]
        lineas = allocate(registros, period="2026-03", config=CFG_PROPORCIONAL)
        assert sum(line.total_chargeback_usd for line in lineas) == pytest.approx(1250.0)

    def test_porcentajes_suman_uno(self):
        registros = [registro("CC-A", 600.0), registro("CC-B", 400.0)]
        lineas = allocate(registros, period="2026-03", config=CFG_PROPORCIONAL)
        assert sum(line.pct_of_total for line in lineas) == pytest.approx(1.0, abs=1e-6)


class TestOtrasEstrategias:
    def test_reparto_equitativo(self):
        cfg = {**CFG_PROPORCIONAL, "unallocated_strategy": "even"}
        registros = [registro("CC-A", 900.0), registro("CC-B", 100.0), registro("SIN_ASIGNAR", 200.0)]
        lineas = {line.unit: line for line in allocate(registros, period="2026-03", config=cfg)}
        assert lineas["CC-A"].allocated_unallocated_usd == pytest.approx(100.0)
        assert lineas["CC-B"].allocated_unallocated_usd == pytest.approx(100.0)

    def test_estrategia_none_deja_unidad_residual(self):
        cfg = {**CFG_PROPORCIONAL, "unallocated_strategy": "none"}
        registros = [registro("CC-A", 900.0), registro("SIN_ASIGNAR", 200.0)]
        lineas = {line.unit: line for line in allocate(registros, period="2026-03", config=cfg)}
        assert UNALLOCATED_UNIT in lineas
        assert lineas[UNALLOCATED_UNIT].total_chargeback_usd == pytest.approx(200.0)
        assert lineas["CC-A"].allocated_unallocated_usd == 0.0

    def test_compartido_none_deja_unidad_residual(self):
        cfg = {**CFG_PROPORCIONAL, "shared_cost_strategy": "none"}
        registros = [registro("CC-A", 900.0), registro("CC-A", 100.0, "JOB:9", "plataforma-core")]
        lineas = {line.unit: line for line in allocate(registros, period="2026-03", config=cfg)}
        assert lineas[SHARED_UNIT].total_chargeback_usd == pytest.approx(100.0)

    def test_recargo_administrativo(self):
        cfg = {**CFG_PROPORCIONAL, "overhead_pct": 0.10}
        lineas = allocate([registro("CC-A", 1000.0)], period="2026-03", config=cfg)
        assert lineas[0].overhead_usd == pytest.approx(100.0)
        assert lineas[0].total_chargeback_usd == pytest.approx(1100.0)


class TestCasosLimite:
    def test_sin_registros(self):
        assert allocate([], period="2026-03", config=CFG_PROPORCIONAL) == []

    def test_todo_sin_atribuir_cae_a_unidad_residual(self):
        lineas = allocate([registro("SIN_ASIGNAR", 500.0)], period="2026-03", config=CFG_PROPORCIONAL)
        assert [line.unit for line in lineas] == [UNALLOCATED_UNIT]
        assert lineas[0].total_chargeback_usd == pytest.approx(500.0)

    def test_registros_en_cero_se_ignoran(self):
        lineas = allocate([registro("CC-A", 0.0), registro("CC-B", 10.0)], period="2026-03",
                          config=CFG_PROPORCIONAL)
        assert [line.unit for line in lineas] == ["CC-B"]

    def test_cuenta_entidades_distintas(self):
        registros = [
            registro("CC-A", 10.0, "JOB:1"), registro("CC-A", 20.0, "JOB:2"), registro("CC-A", 5.0, "JOB:1"),
        ]
        lineas = allocate(registros, period="2026-03", config=CFG_PROPORCIONAL)
        assert lineas[0].entity_count == 2

    def test_dimension_alternativa(self):
        cfg = {**CFG_PROPORCIONAL, "allocation_dimension": "team"}
        registros = [{"team": "Datos", "total_cost_usd": 100.0, "entity_key": "JOB:1"}]
        lineas = allocate(registros, period="2026-03", config=cfg)
        assert lineas[0].unit == "Datos"
        assert lineas[0].allocation_dimension == "team"


class TestConciliacion:
    def test_concilia_sin_recargo(self):
        registros = [registro("CC-A", 600.0), registro("CC-B", 400.0), registro("SIN_ASIGNAR", 100.0)]
        lineas = allocate(registros, period="2026-03", config=CFG_PROPORCIONAL)
        resultado = reconcile(registros, lineas)
        assert resultado["reconciled"] is True
        assert resultado["allocated_total_usd"] == pytest.approx(1100.0)

    def test_concilia_con_recargo(self):
        cfg = {**CFG_PROPORCIONAL, "overhead_pct": 0.05}
        registros = [registro("CC-A", 1000.0), registro("SIN_ASIGNAR", 200.0)]
        lineas = allocate(registros, period="2026-03", config=cfg)
        resultado = reconcile(registros, lineas, overhead_pct=0.05)
        assert resultado["reconciled"] is True

    def test_detecta_descuadre(self):
        registros = [registro("CC-A", 1000.0)]
        lineas = allocate(registros, period="2026-03", config=CFG_PROPORCIONAL)
        lineas[0].total_chargeback_usd = 999.0
        assert reconcile(registros, lineas)["reconciled"] is False


class TestReporte:
    def test_resumen_ejecutivo(self):
        registros = [registro("CC-A", 600.0), registro("CC-B", 400.0), registro("SIN_ASIGNAR", 100.0)]
        lineas = allocate(registros, period="2026-03", config=CFG_PROPORCIONAL)
        reporte = showback_report(lineas, top_n=1)
        assert reporte["period"] == "2026-03"
        assert reporte["total_usd"] == pytest.approx(1100.0)
        assert reporte["unit_count"] == 2
        assert len(reporte["top_units"]) == 1
        assert reporte["top_units"][0]["unit"] == "CC-A"
        assert reporte["unallocated_allocated_usd"] == pytest.approx(100.0)
