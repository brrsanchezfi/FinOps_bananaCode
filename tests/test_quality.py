"""Pruebas de los evaluadores de calidad de datos."""

from __future__ import annotations

from datetime import date

import pytest

from finops.errors import DataQualityError
from finops.quality.checks import (
    MAX_SKUS_EN_MENSAJE,
    SEVERITY_ERROR,
    SEVERITY_WARNING,
    enforce,
    evaluate_duplicates,
    evaluate_freshness,
    evaluate_negative_cost,
    evaluate_null_ratio,
    evaluate_price_match,
    evaluate_row_count,
    evaluate_tag_coverage,
    sku_is_ignored,
    summarize,
)


class TestFrescura:
    def test_datos_del_dia_pasan(self):
        resultado = evaluate_freshness(date(2026, 3, 10), date(2026, 3, 10), 36)
        assert resultado.passed is True
        assert resultado.observed == 0.0

    def test_rezago_de_un_dia_pasa_con_36h(self):
        assert evaluate_freshness(date(2026, 3, 9), date(2026, 3, 10), 36).passed is True

    def test_rezago_de_tres_dias_falla(self):
        resultado = evaluate_freshness(date(2026, 3, 7), date(2026, 3, 10), 36)
        assert resultado.passed is False
        assert resultado.observed == 72.0

    def test_sin_datos_falla(self):
        resultado = evaluate_freshness(None, date(2026, 3, 10), 36)
        assert resultado.passed is False
        assert resultado.severity == SEVERITY_ERROR


class TestNulos:
    def test_bajo_el_umbral(self):
        assert evaluate_null_ratio(1, 10_000, 0.001, "total_cost_usd").passed is True

    def test_sobre_el_umbral(self):
        resultado = evaluate_null_ratio(50, 1000, 0.001, "total_cost_usd")
        assert resultado.passed is False
        assert resultado.observed == pytest.approx(0.05)

    def test_tabla_vacia_no_falla_por_nulos(self):
        assert evaluate_null_ratio(0, 0, 0.001, "x").passed is True


class TestConteoYSignos:
    def test_conteo_minimo(self):
        assert evaluate_row_count(100, 1).passed is True
        assert evaluate_row_count(0, 1).passed is False

    def test_costo_negativo(self):
        assert evaluate_negative_cost(0, 0).passed is True
        assert evaluate_negative_cost(3, 0).passed is False


class TestCoincidenciaDePrecios:
    def test_alta_cobertura(self):
        assert evaluate_price_match(9900, 10_000, 0.98).passed is True

    def test_baja_cobertura_falla(self):
        resultado = evaluate_price_match(9000, 10_000, 0.98)
        assert resultado.passed is False
        assert resultado.severity == SEVERITY_ERROR

    def test_sin_filas_se_considera_completo(self):
        assert evaluate_price_match(0, 0, 0.98).passed is True

    def test_la_falla_nombra_los_skus_sin_precio(self):
        """Un porcentaje solo no permite decidir si es un SKU no facturable o un
        defecto del join; el mensaje debe nombrar el SKU para poder actuar."""
        resultado = evaluate_price_match(
            660, 869, 0.98,
            unpriced_skus=[("PREMIUM_JOBS_COMPUTE", 150), ("NETWORKING_EGRESS", 59)],
        )
        assert resultado.passed is False
        assert "PREMIUM_JOBS_COMPUTE (150)" in resultado.message
        assert "NETWORKING_EGRESS (59)" in resultado.message
        assert "diagnostico_precios.sql" in resultado.message

    def test_el_detalle_se_resume_cuando_hay_muchos_skus(self):
        muchos = [(f"SKU_{i}", 100 - i) for i in range(MAX_SKUS_EN_MENSAJE + 3)]
        mensaje = evaluate_price_match(1, 100, 0.98, unpriced_skus=muchos).message
        assert "SKU_0 (100)" in mensaje
        assert f"SKU_{MAX_SKUS_EN_MENSAJE}" not in mensaje
        assert "y 3 SKU(s) mas" in mensaje

    def test_un_chequeo_sano_no_lista_skus(self):
        """El detalle es ruido cuando no hay nada que investigar."""
        mensaje = evaluate_price_match(
            9900, 10_000, 0.98, unpriced_skus=[("SKU_RARO", 100)]
        ).message
        assert "SKU_RARO" not in mensaje

    def test_las_exclusiones_quedan_visibles(self):
        """Excluir SKUs deja de vigilar gasto: no puede ser silencioso."""
        mensaje = evaluate_price_match(869, 869, 0.98, ignored_rows=42).message
        assert "42" in mensaje and "price_match_ignore_skus" in mensaje


class TestSkusIgnorados:
    def test_coincidencia_exacta_e_insensible_a_mayusculas(self):
        assert sku_is_ignored("networking_egress", ["NETWORKING_EGRESS"]) is True

    def test_comodin_glob(self):
        assert sku_is_ignored("PREMIUM_TRIAL_FREE", ["*_FREE"]) is True
        assert sku_is_ignored("PREMIUM_JOBS_COMPUTE", ["*_FREE"]) is False

    def test_sin_patrones_no_ignora_nada(self):
        assert sku_is_ignored("CUALQUIERA", []) is False
        assert sku_is_ignored("CUALQUIERA", None) is False

    def test_sku_nulo(self):
        assert sku_is_ignored(None, ["*"]) is False


class TestCobertura:
    def test_es_advertencia_no_error(self):
        resultado = evaluate_tag_coverage(0.5, 0.8, "cost_center")
        assert resultado.passed is False
        assert resultado.severity == SEVERITY_WARNING


class TestDuplicados:
    def test_sin_duplicados(self):
        assert evaluate_duplicates(0).passed is True

    def test_con_duplicados(self):
        assert evaluate_duplicates(5).passed is False


class TestResumenYEnforce:
    def _resultados(self):
        return [
            evaluate_row_count(100, 1),
            evaluate_price_match(5000, 10_000, 0.98),      # falla, error
            evaluate_tag_coverage(0.5, 0.8, "team"),       # falla, advertencia
        ]

    def test_resumen(self):
        resumen = summarize(self._resultados())
        assert resumen["total"] == 3
        assert resumen["failed"] == 2
        assert resumen["blocking"] == 1

    def test_enforce_lanza_con_bloqueantes(self):
        with pytest.raises(DataQualityError) as exc:
            enforce(self._resultados(), fail_on_error=True)
        assert "price_match" in str(exc.value)

    def test_enforce_no_lanza_si_esta_desactivado(self):
        enforce(self._resultados(), fail_on_error=False)

    def test_enforce_ignora_advertencias(self):
        enforce([evaluate_tag_coverage(0.1, 0.8, "team")], fail_on_error=True)

    def test_fila_serializable(self):
        fila = evaluate_row_count(10, 1).to_row()
        assert set(fila) >= {"check", "passed", "severity", "observed", "threshold", "message"}
