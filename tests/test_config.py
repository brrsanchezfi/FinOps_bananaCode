"""Pruebas de carga, fusion y validacion de configuracion."""

from __future__ import annotations

from datetime import date, timedelta

import pytest

from finops.config import (
    coerce_scalar,
    deep_merge,
    flatten_overrides,
    get_by_path,
    load_config,
    overrides_from_env,
    set_by_path,
    validate_config,
)
from finops.errors import ConfigError


class TestDeepMerge:
    def test_fusiona_niveles_anidados(self):
        base = {"a": {"b": 1, "c": 2}, "d": 3}
        override = {"a": {"c": 99, "e": 5}}
        assert deep_merge(base, override) == {"a": {"b": 1, "c": 99, "e": 5}, "d": 3}

    def test_las_listas_se_reemplazan_completas(self):
        # Un entorno debe poder redefinir la lista de canales sin heredar la base.
        base = {"channels": [{"name": "teams"}, {"name": "slack"}]}
        override = {"channels": [{"name": "tabla"}]}
        assert deep_merge(base, override)["channels"] == [{"name": "tabla"}]

    def test_no_muta_los_originales(self):
        base = {"a": {"b": 1}}
        deep_merge(base, {"a": {"b": 2}})
        assert base == {"a": {"b": 1}}


class TestRutasPunteadas:
    def test_set_and_get(self):
        destino: dict = {}
        set_by_path(destino, "x.y.z", 42)
        assert destino == {"x": {"y": {"z": 42}}}
        assert get_by_path(destino, "x.y.z") == 42

    def test_get_devuelve_default_si_falta(self):
        assert get_by_path({"a": 1}, "a.b.c", "sin") == "sin"

    def test_set_sobre_escalar_reemplaza_nivel(self):
        destino = {"x": 5}
        set_by_path(destino, "x.y", 1)
        assert destino == {"x": {"y": 1}}

    def test_ruta_vacia_falla(self):
        with pytest.raises(ConfigError):
            set_by_path({}, "", 1)


class TestCoerceScalar:
    @pytest.mark.parametrize(
        ("crudo", "esperado"),
        [
            ("true", True), ("FALSE", False), ("null", None), ("", None),
            ("42", 42), ("3.5", 3.5), ("texto", "texto"),
            ('["a","b"]', ["a", "b"]), ('{"k":1}', {"k": 1}),
        ],
    )
    def test_conversion(self, crudo, esperado):
        assert coerce_scalar(crudo) == esperado


class TestOverrides:
    def test_desde_variables_de_entorno(self):
        entorno = {"FINOPS__CATALOG__CATALOG": "finops_sandbox", "OTRA": "x"}
        assert overrides_from_env(entorno) == {"catalog": {"catalog": "finops_sandbox"}}

    def test_parametros_planos_se_anidan(self):
        assert flatten_overrides({"anomaly.window_days": "14", "sin_punto": "x"}) == {
            "anomaly": {"window_days": 14}
        }


class TestLoadConfig:
    def test_carga_dev(self, conf_dir):
        cfg = load_config("dev", conf_dir=conf_dir, use_env_vars=False)
        assert cfg.env == "dev"
        assert cfg.catalog == "finops"
        assert cfg.table("gold", "fct_cost_daily") == "finops.gold.fct_cost_daily"

    @pytest.mark.parametrize("env", ["dev", "qa", "prd"])
    def test_los_tres_entornos_comparten_catalogo(self, conf_dir, env):
        """El modelo FinOps describe el consumo de la CUENTA, no de un ambiente.

        Los tres entornos leen las mismas system tables y producen las mismas
        cifras, asi que comparten destino. Lo que los separa es donde corre el
        codigo y que jobs estan programados.
        """
        cfg = load_config(env, conf_dir=conf_dir, use_env_vars=False)
        assert cfg.catalog == "finops"
        assert cfg.schema("gold") == "gold"

    def test_overlay_de_entorno_gana_sobre_base(self, conf_dir):
        base = load_config("prd", conf_dir=conf_dir, use_env_vars=False)
        dev = load_config("dev", conf_dir=conf_dir, use_env_vars=False)
        assert base.get("ingestion.lookback_days") == 7
        assert dev.get("ingestion.lookback_days") == 3

    def test_param_overrides_ganan_sobre_entorno(self, conf_dir):
        cfg = load_config(
            "dev", conf_dir=conf_dir, use_env_vars=False,
            param_overrides={"ingestion.lookback_days": "30"},
        )
        assert cfg.get("ingestion.lookback_days") == 30

    def test_ventana_respeta_lookback(self, conf_dir):
        cfg = load_config("dev", conf_dir=conf_dir, use_env_vars=False, run_date="2026-07-15")
        assert cfg.max_date == date(2026, 7, 15)
        assert cfg.min_date == date(2026, 7, 12)  # dev usa lookback 3

    def test_full_refresh_amplia_la_ventana(self, conf_dir):
        cfg = load_config(
            "dev", conf_dir=conf_dir, use_env_vars=False, run_date="2026-07-15",
            param_overrides={"ingestion.full_refresh": "true"},
        )
        # dev define initial_load_days = 90
        assert cfg.lookback_days == 90
        assert cfg.min_date == date(2026, 7, 15) - timedelta(days=90)

    def test_entorno_invalido(self, conf_dir):
        with pytest.raises(ConfigError, match="invalido"):
            load_config("staging", conf_dir=conf_dir, use_env_vars=False)

    def test_require_falla_si_no_existe(self, cfg_dev):
        with pytest.raises(ConfigError, match="obligatoria"):
            cfg_dev.require("clave.que.no.existe")

    def test_capa_desconocida(self, cfg_dev):
        with pytest.raises(ConfigError, match="Capa desconocida"):
            cfg_dev.schema("platinum")


class TestValidacion:
    def _cfg_valida(self, conf_dir):
        return load_config("dev", conf_dir=conf_dir, use_env_vars=False)

    def test_descuento_fuera_de_rango(self, conf_dir):
        cfg = self._cfg_valida(conf_dir)
        cfg.data["pricing"]["discounts"] = [{"name": "malo", "match": {}, "discount_pct": 1.5}]
        with pytest.raises(ConfigError, match="discount_pct"):
            validate_config(cfg)

    def test_dimension_sin_alias(self, conf_dir):
        cfg = self._cfg_valida(conf_dir)
        cfg.data["tagging"]["dimensions"] = ["cost_center", "inexistente"]
        with pytest.raises(ConfigError, match="alias"):
            validate_config(cfg)

    def test_canal_duplicado(self, conf_dir):
        cfg = self._cfg_valida(conf_dir)
        cfg.data["alerting"]["channels"] = [
            {"name": "x", "type": "table"},
            {"name": "x", "type": "table"},
        ]
        with pytest.raises(ConfigError, match="duplicado"):
            validate_config(cfg)

    def test_webhook_habilitado_sin_secreto(self, conf_dir):
        cfg = self._cfg_valida(conf_dir)
        cfg.data["alerting"]["channels"] = [{"name": "t", "type": "webhook", "enabled": True}]
        with pytest.raises(ConfigError, match="secret_key"):
            validate_config(cfg)

    def test_presupuesto_sin_monto(self, conf_dir):
        cfg = self._cfg_valida(conf_dir)
        cfg.budgets["budgets"] = [{"id": "x", "period": "monthly", "amount_usd": 0}]
        with pytest.raises(ConfigError, match="amount_usd"):
            validate_config(cfg)

    def test_metodo_de_anomalia_invalido(self, conf_dir):
        cfg = self._cfg_valida(conf_dir)
        cfg.data["anomaly"]["method"] = "magia"
        with pytest.raises(ConfigError, match="anomaly.method"):
            validate_config(cfg)


class TestConfiguracionDelRepositorio:
    """Los tres entornos versionados deben ser validos en todo momento."""

    @pytest.mark.parametrize("env", ["dev", "qa", "prd"])
    def test_entornos_validos(self, conf_dir, env):
        cfg = load_config(env, conf_dir=conf_dir, use_env_vars=False)
        assert cfg.catalog
        assert cfg.get("tagging.dimensions")

    def test_los_presupuestos_del_repo_son_validos(self, conf_dir):
        cfg = load_config("prd", conf_dir=conf_dir, use_env_vars=False)
        ids = [b["id"] for b in cfg.budgets["budgets"]]
        assert len(ids) == len(set(ids))
