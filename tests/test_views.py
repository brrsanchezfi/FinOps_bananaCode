"""Pruebas de las vistas de gobierno de etiquetado.

El SQL se construye a partir de la configuracion, asi que se puede verificar sin
Spark: lo que importa es que los alias declarados en `conf/` lleguen al SQL y que
no quede nada escrito a mano.
"""

from __future__ import annotations

import pytest

from finops.views import (
    ALL_VIEWS,
    VIEW_TAG_COVERAGE,
    VIEW_TAG_INVENTORY,
    VIEW_UNTAGGED,
    VIEW_USAGE_LIVE,
    _sql_literal_list,
    alias_por_dimension,
    all_view_sql,
    build_tag_coverage_sql,
    build_tag_inventory_sql,
    build_untagged_spend_sql,
    build_usage_live_sql,
)


@pytest.fixture
def cfg(cfg_dev):
    return cfg_dev


class TestRegistro:
    def test_las_vistas_viven_en_gold(self):
        assert {v.layer for v in ALL_VIEWS} == {"gold"}

    def test_el_nombre_declara_que_son_en_vivo(self):
        """El sufijo `_live` avisa que la cifra no viene del pipeline."""
        assert all(v.name.startswith("vw_") and v.name.endswith("_live") for v in ALL_VIEWS)

    def test_todas_tienen_descripcion_util(self):
        assert all(len(v.description.strip()) >= 20 for v in ALL_VIEWS)


class TestAliasDesdeLaConfiguracion:
    def test_toma_las_dimensiones_declaradas(self, cfg):
        alias = alias_por_dimension(cfg)
        assert set(alias) == set(cfg.get("tagging.dimensions"))

    def test_incluye_los_alias_normalizados(self, cfg):
        alias = alias_por_dimension(cfg)
        # 'cost_center' y 'cost-center' normalizan ambos a 'costcenter'.
        assert "costcenter" in alias["cost_center"]
        assert "equipo" in alias["team"]

    def test_una_dimension_sin_alias_usa_su_propio_nombre(self, cfg):
        cfg.data["tagging"]["dimensions"] = [*cfg.get("tagging.dimensions"), "negocio"]
        alias = alias_por_dimension(cfg)
        assert alias["negocio"] == ["negocio"]


class TestSqlGenerado:
    def test_la_vista_base_lee_las_system_tables(self, cfg):
        sql = build_usage_live_sql(cfg)
        assert "system.billing.usage" in sql
        assert "system.billing.list_prices" in sql

    def test_la_vista_base_deduplica_por_registro(self, cfg):
        """Un consumo puede empatar con dos tramos de precio en el borde."""
        sql = build_usage_live_sql(cfg)
        assert "QUALIFY ROW_NUMBER()" in sql
        assert "PARTITION BY u.record_id" in sql

    def test_la_ventana_sale_de_la_configuracion(self, cfg):
        cfg.data.setdefault("governance", {})["live_window_days"] = 45
        assert "INTERVAL 45 DAYS" in build_usage_live_sql(cfg)

    def test_el_costo_se_llama_de_lista(self, cfg):
        """No es `total_cost_usd`: estas vistas no aplican descuentos."""
        sql = build_usage_live_sql(cfg)
        assert "list_cost_usd" in sql
        assert "total_cost_usd" not in sql

    def test_el_inventario_clasifica_por_los_alias_declarados(self, cfg):
        sql = build_tag_inventory_sql(cfg)
        assert "'costcenter'" in sql
        assert "THEN 'cost_center'" in sql
        assert "is_recognized" in sql

    def test_la_cobertura_cubre_todas_las_dimensiones(self, cfg):
        sql = build_tag_coverage_sql(cfg)
        for dimension in cfg.get("tagging.dimensions"):
            assert f"'{dimension}' AS dimension" in sql

    def test_las_dimensiones_nuevas_llegan_al_sql(self, cfg):
        """Agregar una dimension en conf/ debe bastar: sin tocar codigo."""
        cfg.data["tagging"]["dimensions"] = [*cfg.get("tagging.dimensions"), "negocio"]
        cfg.data["tagging"]["aliases"]["negocio"] = ["negocio", "business_unit"]
        sql = build_tag_coverage_sql(cfg)
        assert "'negocio' AS dimension" in sql
        assert "tags_norm['businessunit']" in sql

    def test_los_valores_de_relleno_no_cuentan_como_atribuidos(self, cfg):
        """Un tag con valor 'null' esta tan sin resolver como uno ausente."""
        sql = build_tag_coverage_sql(cfg)
        assert "'undefined'" in sql and "'n/a'" in sql

    def test_sin_atribuir_exige_que_ninguna_dimension_resuelva(self, cfg):
        sql = build_untagged_spend_sql(cfg)
        assert sql.count("NOT (") == len(cfg.get("tagging.dimensions"))

    def test_ningun_catalogo_escrito_a_mano(self, cfg):
        """Igual que en los dashboards: el catalogo sale de la configuracion."""
        for fqn, sql in all_view_sql(cfg):
            assert fqn.startswith(f"{cfg.catalog}.")
            cuerpo = sql.replace(f"{cfg.catalog}.", "")
            assert "finops_dev" not in cuerpo and "finops_qa" not in cuerpo

    def test_toda_vista_se_crea_con_replace(self, cfg):
        """`ensure_views` corre en cada setup: no puede fallar si ya existen."""
        for _, sql in all_view_sql(cfg):
            assert sql.startswith("CREATE OR REPLACE VIEW ")

    def test_el_orden_respeta_las_dependencias(self, cfg):
        """La vista base debe crearse antes de las que la consultan."""
        fqns = [fqn for fqn, _ in all_view_sql(cfg)]
        base = VIEW_USAGE_LIVE.fqn(cfg)
        assert fqns[0] == base
        for vista in (VIEW_TAG_INVENTORY, VIEW_TAG_COVERAGE, VIEW_UNTAGGED):
            assert fqns.index(vista.fqn(cfg)) > 0


class TestEscapado:
    def test_las_comillas_se_escapan(self):
        """Un alias con apostrofo no puede romper el DDL."""
        assert _sql_literal_list(["o'brien"]) == "'o''brien'"

    def test_lista_vacia(self):
        assert _sql_literal_list([]) == ""
