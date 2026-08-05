"""Pruebas del modelo de costos: clasificacion de SKU, descuentos y valorizacion."""

from __future__ import annotations

import pytest

from finops.transform.pricing import (
    classify_sku,
    compute_family,
    enrich_usage_record,
    infra_factor_for,
    is_photon,
    is_serverless,
    price_record,
    resolve_discount,
)


class TestClasificacionSku:
    @pytest.mark.parametrize(
        ("sku", "producto", "esperado"),
        [
            ("PREMIUM_ALL_PURPOSE_COMPUTE", None, "ALL_PURPOSE"),
            ("PREMIUM_ALL_PURPOSE_COMPUTE_(PHOTON)", None, "ALL_PURPOSE"),
            ("PREMIUM_JOBS_COMPUTE", None, "JOBS"),
            ("PREMIUM_JOBS_COMPUTE_(PHOTON)", None, "JOBS"),
            ("PREMIUM_DLT_ADVANCED_COMPUTE", None, "DLT"),
            ("PREMIUM_SQL_PRO_COMPUTE_US_EAST", None, "SQL"),
            ("PREMIUM_SERVERLESS_SQL_COMPUTE", None, "SERVERLESS_SQL"),
            ("PREMIUM_SERVERLESS_REAL_TIME_INFERENCE", None, "MODEL_SERVING"),
            ("MODEL_TRAINING_GPU", None, "AI_TRAINING"),
            ("PREDICTIVE_OPTIMIZATION", None, "STORAGE_OPS"),
            ("ALGO_DESCONOCIDO", None, "OTHER"),
        ],
    )
    def test_por_nombre_de_sku(self, sku, producto, esperado):
        assert classify_sku(sku, producto) == esperado

    def test_billing_origin_product_tiene_prioridad(self):
        # El nombre del SKU sugiere ALL_PURPOSE pero el producto controlado dice JOBS.
        assert classify_sku("PREMIUM_ALL_PURPOSE_COMPUTE", "JOBS") == "JOBS"

    def test_serverless_transforma_el_grupo_base(self):
        assert classify_sku("PREMIUM_SERVERLESS_COMPUTE", "SQL") == "SERVERLESS_SQL"
        assert classify_sku("PREMIUM_SERVERLESS_COMPUTE", "JOBS") == "SERVERLESS_JOBS"

    def test_serverless_no_altera_productos_no_compute(self):
        assert classify_sku("SERVERLESS_REAL_TIME_INFERENCE", "MODEL_SERVING") == "MODEL_SERVING"

    def test_valores_nulos(self):
        assert classify_sku(None, None) == "OTHER"
        assert classify_sku("", "") == "OTHER"

    def test_banderas(self):
        assert is_photon("PREMIUM_JOBS_COMPUTE_(PHOTON)") is True
        assert is_photon("PREMIUM_JOBS_COMPUTE") is False
        assert is_serverless("PREMIUM_SERVERLESS_SQL", None) is True
        assert is_serverless(None, "SERVERLESS_JOBS") is True
        assert is_serverless("PREMIUM_JOBS_COMPUTE", "JOBS") is False

    @pytest.mark.parametrize(
        ("grupo", "familia"),
        [
            ("ALL_PURPOSE", "ALL_PURPOSE"), ("JOBS", "JOBS"), ("SQL", "SQL"),
            ("SERVERLESS_SQL", "SERVERLESS"), ("SERVERLESS_JOBS", "SERVERLESS"),
            ("MODEL_SERVING", "OTHER"), ("OTHER", "OTHER"),
        ],
    )
    def test_familia_de_compute(self, grupo, familia):
        assert compute_family(grupo) == familia


class TestDescuentos:
    def test_primera_regla_que_coincide_gana(self, pricing_cfg):
        pct, nombre = resolve_discount(pricing_cfg["discounts"], {"sku_group": "SQL"})
        assert (pct, nombre) == (0.25, "acuerdo_sql")

    def test_regla_por_defecto_cuando_no_hay_match_especifico(self, pricing_cfg):
        pct, nombre = resolve_discount(pricing_cfg["discounts"], {"sku_group": "JOBS"})
        assert (pct, nombre) == (0.10, "acuerdo_general")

    def test_sin_reglas(self):
        assert resolve_discount([], {"sku_group": "JOBS"}) == (0.0, "sin_descuento")
        assert resolve_discount(None, {}) == (0.0, "sin_descuento")

    def test_comodines(self):
        reglas = [{"name": "sql_todos", "match": {"sku_name": "*SQL*"}, "discount_pct": 0.3}]
        assert resolve_discount(reglas, {"sku_name": "PREMIUM_SQL_PRO"})[0] == 0.3
        assert resolve_discount(reglas, {"sku_name": "PREMIUM_JOBS"})[0] == 0.0

    def test_lista_de_valores_es_or(self):
        reglas = [{"name": "ws", "match": {"workspace_id": ["111", "222"]}, "discount_pct": 0.15}]
        assert resolve_discount(reglas, {"workspace_id": "222"})[0] == 0.15
        assert resolve_discount(reglas, {"workspace_id": "333"})[0] == 0.0

    def test_clave_ausente_en_el_contexto_no_coincide(self):
        reglas = [{"name": "x", "match": {"cloud": "AZURE"}, "discount_pct": 0.5}]
        assert resolve_discount(reglas, {"sku_group": "JOBS"})[0] == 0.0

    def test_descuento_se_acota(self):
        reglas = [{"name": "extremo", "match": {}, "discount_pct": 5.0}]
        assert resolve_discount(reglas, {})[0] == 0.999


class TestValorizacion:
    def test_calculo_basico(self):
        costos = price_record(usage_quantity=10.0, unit_price=0.55, discount_pct=0.0, infra_factor=0.0)
        assert costos["list_cost_usd"] == pytest.approx(5.5)
        assert costos["effective_cost_usd"] == pytest.approx(5.5)
        assert costos["total_cost_usd"] == pytest.approx(5.5)

    def test_descuento_e_infra(self):
        costos = price_record(usage_quantity=100.0, unit_price=1.0, discount_pct=0.20, infra_factor=0.5)
        assert costos["list_cost_usd"] == pytest.approx(100.0)
        assert costos["discount_amount_usd"] == pytest.approx(20.0)
        assert costos["effective_cost_usd"] == pytest.approx(80.0)
        assert costos["estimated_infra_cost_usd"] == pytest.approx(40.0)
        assert costos["total_cost_usd"] == pytest.approx(120.0)

    def test_precio_faltante_no_propaga_nulos(self):
        costos = price_record(usage_quantity=10.0, unit_price=None)
        assert costos["total_cost_usd"] == 0.0

    def test_cantidad_nula(self):
        assert price_record(usage_quantity=None, unit_price=1.0)["total_cost_usd"] == 0.0

    def test_identidad_de_componentes(self):
        costos = price_record(usage_quantity=37.3, unit_price=0.723, discount_pct=0.13, infra_factor=0.25)
        assert costos["list_cost_usd"] - costos["discount_amount_usd"] == pytest.approx(
            costos["effective_cost_usd"], abs=1e-6
        )
        assert costos["effective_cost_usd"] + costos["estimated_infra_cost_usd"] == pytest.approx(
            costos["total_cost_usd"], abs=1e-6
        )


class TestFactorInfra:
    def test_deshabilitado_devuelve_cero(self):
        assert infra_factor_for({"enabled": False, "factor_by_compute": {"JOBS": 0.9}}, "JOBS") == 0.0

    def test_habilitado_usa_la_familia(self, pricing_cfg):
        assert infra_factor_for(pricing_cfg["infra_estimate"], "JOBS") == 0.5
        assert infra_factor_for(pricing_cfg["infra_estimate"], "SERVERLESS_SQL") == 0.0

    def test_familia_desconocida_usa_other(self, pricing_cfg):
        assert infra_factor_for(pricing_cfg["infra_estimate"], "MODEL_SERVING") == 0.0


class TestEnriquecimiento:
    def test_registro_completo(self, pricing_cfg):
        registro = {
            "record_id": "r1",
            "workspace_id": "123",
            "sku_name": "PREMIUM_SQL_PRO_COMPUTE",
            "billing_origin_product": "SQL",
            "usage_quantity": 20.0,
            "unit_price": 0.70,
            "cloud": "AZURE",
        }
        salida = enrich_usage_record(registro, pricing_cfg)
        assert salida["sku_group"] == "SQL"
        assert salida["discount_rule"] == "acuerdo_sql"
        assert salida["discount_pct"] == 0.25
        assert salida["list_cost_usd"] == pytest.approx(14.0)
        assert salida["effective_cost_usd"] == pytest.approx(10.5)
        assert salida["estimated_infra_cost_usd"] == pytest.approx(6.3)
        assert salida["total_cost_usd"] == pytest.approx(16.8)
        assert salida["price_missing"] is False

    def test_precio_faltante_se_marca(self, pricing_cfg):
        salida = enrich_usage_record(
            {"sku_name": "SKU_NUEVO", "usage_quantity": 5.0, "unit_price": None}, pricing_cfg
        )
        assert salida["price_missing"] is True
        assert salida["total_cost_usd"] == 0.0

    def test_no_muta_el_registro_original(self, pricing_cfg):
        registro = {"sku_name": "PREMIUM_JOBS_COMPUTE", "usage_quantity": 1.0, "unit_price": 1.0}
        enrich_usage_record(registro, pricing_cfg)
        assert "sku_group" not in registro
