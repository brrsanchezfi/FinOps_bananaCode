"""Pruebas de normalizacion de etiquetas y resolucion de entidades."""

from __future__ import annotations

import pytest

from finops.transform.tags import (
    build_alias_index,
    canonicalize_value,
    extract_dimensions,
    normalize_key,
    normalize_value,
    resolve_entity,
    resolve_tags,
    tag_coverage,
)


class TestNormalizacion:
    @pytest.mark.parametrize(
        ("crudo", "esperado"),
        [
            ("Cost-Center", "costcenter"), ("cost_center", "costcenter"),
            ("  COSTCENTER ", "costcenter"), ("Centro Costo", "centrocosto"),
            (None, ""), ("", ""),
        ],
    )
    def test_normalize_key(self, crudo, esperado):
        assert normalize_key(crudo) == esperado

    @pytest.mark.parametrize(
        ("crudo", "esperado"),
        [("  Datos ", "Datos"), ("", None), (None, None), ("null", None), ("N/A", None), ("-", None)],
    )
    def test_normalize_value(self, crudo, esperado):
        assert normalize_value(crudo) == esperado


class TestIndiceDeAlias:
    def test_incluye_la_dimension_y_sus_alias(self, tagging_cfg):
        indice = build_alias_index(tagging_cfg["aliases"])
        assert indice["costcenter"] == "cost_center"
        assert indice["cc"] == "cost_center"
        assert indice["centrocosto"] == "cost_center"
        assert indice["equipo"] == "team"

    def test_alias_en_conflicto_gana_la_primera_dimension(self):
        indice = build_alias_index({"team": ["grupo"], "project": ["grupo"]})
        assert indice["grupo"] == "team"


class TestExtraccion:
    def test_proyecta_solo_dimensiones_conocidas(self, tagging_cfg):
        indice = build_alias_index(tagging_cfg["aliases"])
        salida = extract_dimensions(
            {"CC": "CC-100", "equipo": "Datos", "irrelevante": "x"}, indice, tagging_cfg["dimensions"]
        )
        assert salida == {"cost_center": "CC-100", "team": "Datos"}

    def test_ignora_valores_vacios(self, tagging_cfg):
        indice = build_alias_index(tagging_cfg["aliases"])
        assert extract_dimensions({"cc": "  "}, indice, tagging_cfg["dimensions"]) == {}

    def test_mapa_nulo(self, tagging_cfg):
        indice = build_alias_index(tagging_cfg["aliases"])
        assert extract_dimensions(None, indice, tagging_cfg["dimensions"]) == {}


class TestCanonizacionDeValores:
    def test_mapea_variantes(self, tagging_cfg):
        assert canonicalize_value("environment", "produccion", tagging_cfg["value_map"]) == "PRD"
        assert canonicalize_value("environment", "PROD", tagging_cfg["value_map"]) == "PRD"

    def test_valor_sin_mapeo_se_conserva(self, tagging_cfg):
        assert canonicalize_value("environment", "uat", tagging_cfg["value_map"]) == "uat"

    def test_dimension_sin_mapa(self, tagging_cfg):
        assert canonicalize_value("team", "Datos", tagging_cfg["value_map"]) == "Datos"


class TestResolveTags:
    def _resolver(self, fuentes, tagging_cfg, **kwargs):
        return resolve_tags(
            fuentes,
            dimensions=tagging_cfg["dimensions"],
            alias_index=build_alias_index(tagging_cfg["aliases"]),
            value_map=tagging_cfg["value_map"],
            unallocated_value=tagging_cfg["unallocated_value"],
            **kwargs,
        )

    def test_precedencia_custom_tags_sobre_cluster(self, tagging_cfg):
        salida = self._resolver(
            {"custom_tags": {"team": "Datos"}, "cluster_tags": {"team": "Plataforma"}}, tagging_cfg
        )
        assert salida["team"] == "Datos"
        assert salida["tag_source_team"] == "custom_tags"

    def test_fallback_a_tags_del_job(self, tagging_cfg):
        salida = self._resolver(
            {"custom_tags": {}, "cluster_tags": None, "job_tags": {"cc": "CC-9"}}, tagging_cfg
        )
        assert salida["cost_center"] == "CC-9"
        assert salida["tag_source_cost_center"] == "job_tags"

    def test_sin_asignar_cuando_no_hay_nada(self, tagging_cfg):
        salida = self._resolver({}, tagging_cfg)
        assert salida["cost_center"] == "SIN_ASIGNAR"
        assert salida["is_untagged"] is True
        assert salida["tags_resolved"] == 0

    def test_totalmente_etiquetado(self, tagging_cfg):
        salida = self._resolver(
            {"custom_tags": {"cc": "CC-1", "equipo": "Datos", "ambiente": "prod"}}, tagging_cfg
        )
        assert salida["environment"] == "PRD"
        assert salida["is_fully_tagged"] is True
        assert salida["tags_resolved"] == 3

    def test_orden_de_fuentes_personalizado(self, tagging_cfg):
        salida = self._resolver(
            {"custom_tags": {"team": "A"}, "cluster_tags": {"team": "B"}},
            tagging_cfg,
            source_order=("cluster_tags", "custom_tags"),
        )
        assert salida["team"] == "B"

    def test_expone_el_conteo_esperado(self, tagging_cfg):
        salida = self._resolver({"custom_tags": {"cc": "X"}}, tagging_cfg)
        assert salida["tags_expected"] == 3
        assert salida["tags_resolved"] == 1


class TestResolveEntity:
    def test_prioridad_job_sobre_cluster(self):
        salida = resolve_entity({"job_id": "42", "cluster_id": "c-1"})
        assert salida["entity_type"] == "JOB"
        assert salida["entity_key"] == "JOB:42"
        # El cluster sigue disponible para el analisis de configuracion.
        assert salida["cluster_id"] == "c-1"

    def test_warehouse(self):
        salida = resolve_entity({"warehouse_id": "w-9"})
        assert salida["entity_type"] == "WAREHOUSE"
        assert salida["entity_key"] == "WAREHOUSE:w-9"

    def test_pipeline_antes_que_cluster(self):
        assert resolve_entity({"dlt_pipeline_id": "p1", "cluster_id": "c1"})["entity_type"] == "PIPELINE"

    def test_metadata_vacia(self):
        salida = resolve_entity(None)
        assert salida["entity_type"] == "UNKNOWN"
        assert salida["entity_key"] == "UNKNOWN:SIN_ID"

    def test_valores_vacios_se_ignoran(self):
        assert resolve_entity({"job_id": "  ", "cluster_id": "c1"})["entity_type"] == "CLUSTER"


class TestCobertura:
    def test_ponderada_por_costo(self):
        registros = [
            {"team": "A", "total_cost_usd": 80.0},
            {"team": "SIN_ASIGNAR", "total_cost_usd": 20.0},
        ]
        salida = tag_coverage(registros, "team")
        assert salida["coverage_ratio"] == pytest.approx(0.8)
        assert salida["unattributed_cost_usd"] == pytest.approx(20.0)

    def test_sin_costo_es_cobertura_total(self):
        assert tag_coverage([], "team")["coverage_ratio"] == 1.0
