"""Pruebas de la ventana derivada del Change Data Feed.

Todo lo que decide algo aqui es puro: que fechas reprocesar, desde cuando pedir
cambios y cuando NO usar el feed. El unico adaptador de Spark
(`changed_usage_dates`) no decide nada -- devuelve fechas o None.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pytest

from finops.ingestion.cdf import (
    MARGEN_SOLAPE,
    build_changes_sql,
    cdf_enabled,
    since_for,
    window_from_changed_dates,
)

RESPALDO = (date(2026, 9, 14), date(2026, 9, 21))


class TestConsultaDeCambios:
    def test_agrega_en_el_origen(self):
        """Interesa el conjunto de fechas, no las filas: el DISTINCT va alla."""
        sql = build_changes_sql("system.billing.usage", datetime(2026, 9, 21, 3, 0, tzinfo=timezone.utc))
        assert "SELECT DISTINCT usage_date" in sql
        assert "table_changes('system.billing.usage', '2026-09-21 03:00:00')" in sql

    def test_la_marca_va_en_utc(self):
        """El servidor de sharing interpreta la marca en UTC."""
        bogota = timezone(timedelta(hours=-5))
        sql = build_changes_sql("t", datetime(2026, 9, 21, 0, 0, tzinfo=bogota))
        assert "'2026-09-21 05:00:00'" in sql

    def test_descarta_fechas_nulas(self):
        assert "usage_date IS NOT NULL" in build_changes_sql("t", datetime.now(timezone.utc))


class TestVentanaDesdeLosCambios:
    def test_cubre_desde_la_fecha_mas_vieja_tocada(self):
        """Los registros tardios llegan fechados dias atras: la ventana los cubre."""
        cambiadas = {date(2026, 9, 19), date(2026, 9, 21), date(2026, 9, 20)}
        assert window_from_changed_dates(cambiadas, RESPALDO) == (date(2026, 9, 19), date(2026, 9, 21))

    def test_acota_de_verdad_frente_al_respaldo(self):
        """Es el punto del ejercicio: 3 dias en vez de 7."""
        desde, hasta = window_from_changed_dates({date(2026, 9, 21)}, RESPALDO)
        assert (hasta - desde).days < (RESPALDO[1] - RESPALDO[0]).days

    def test_nunca_se_queda_corta_por_arriba(self):
        """El maximo del respaldo manda: es la fecha logica de la corrida."""
        _, hasta = window_from_changed_dates({date(2026, 9, 10)}, RESPALDO)
        assert hasta == RESPALDO[1]

    def test_sin_cdf_devuelve_el_respaldo(self):
        """None = el feed no se pudo usar. No es lo mismo que 'no cambio nada'."""
        assert window_from_changed_dates(None, RESPALDO) == RESPALDO

    def test_sin_cambios_procesa_el_ultimo_dia(self):
        """Colapsar a nada dejaria las tablas sin tocar.

        Y con ellas el chequeo de frescura, que empezaria a fallar por falta de
        datos nuevos justo cuando el pipeline esta sano.
        """
        assert window_from_changed_dates(set(), RESPALDO) == (RESPALDO[1], RESPALDO[1])


class TestCuandoNoUsarElFeed:
    """Degradar a `lookback_days` es parte del diseno, no un caso de borde."""

    def _cfg(self, **overrides):
        from finops.config import FinOpsConfig

        datos = {"ingestion": {"cdf": {"enabled": True, "max_lookback_hours": 48}, **overrides}}
        return FinOpsConfig(env="dev", data=datos)

    def test_sin_corrida_previa_no_hay_punto_de_partida(self):
        assert since_for(self._cfg(), None) is None

    def test_una_corrida_reciente_sirve(self):
        hace_una_hora = datetime.now(timezone.utc) - timedelta(hours=1)
        assert since_for(self._cfg(), hace_una_hora) is not None

    def test_una_corrida_vieja_se_descarta_sin_consultar(self):
        """La historia del feed se poda; descubrirlo con una consulta que falla
        es mas caro que asumirlo."""
        hace_tres_dias = datetime.now(timezone.utc) - timedelta(days=3)
        assert since_for(self._cfg(), hace_tres_dias) is None

    def test_se_resta_un_margen_de_solape(self):
        """Perder un commit del mismo segundo es perder costo."""
        ahora = datetime.now(timezone.utc)
        assert since_for(self._cfg(), ahora) == ahora - MARGEN_SOLAPE

    def test_una_marca_sin_zona_se_asume_utc(self):
        """`updated_at` puede volver de Delta sin tzinfo."""
        ingenua = datetime.now(timezone.utc).replace(tzinfo=None)
        assert since_for(self._cfg(), ingenua) is not None

    @pytest.mark.parametrize("valor", [False, None])
    def test_llega_desactivado(self, valor):
        from finops.config import FinOpsConfig

        cfg = FinOpsConfig(env="dev", data={"ingestion": {"cdf": {"enabled": valor}}})
        assert not cdf_enabled(cfg)

    def test_el_repositorio_lo_distribuye_apagado(self, conf_dir):
        """Rinde al subir la frecuencia del schedule; activarlo es una decision."""
        from finops.config import load_config

        cfg = load_config("dev", conf_dir=conf_dir, use_env_vars=False, use_local_overlay=False)
        assert cfg.get("ingestion.cdf.enabled") is False


class TestLaVentanaFluyeAlPipeline:
    """Acotar solo bronze no sirve de nada: silver y gold leen de la config."""

    def _cfg(self):
        from finops.config import FinOpsConfig

        return FinOpsConfig(
            env="dev",
            data={"ingestion": {"lookback_days": 7}},
            run_date=date(2026, 9, 21),
        )

    def test_sin_override_manda_lookback_days(self):
        cfg = self._cfg()
        assert cfg.min_date == date(2026, 9, 14)
        assert cfg.max_date == date(2026, 9, 21)

    def test_con_override_manda_la_ventana_del_cdf(self):
        cfg = self._cfg()
        cfg.window_override = (date(2026, 9, 20), date(2026, 9, 21))
        assert cfg.min_date == date(2026, 9, 20)
        assert cfg.max_date == date(2026, 9, 21)

    def test_el_override_gana_sobre_max_usage_date(self):
        """Si no ganara, bronze y gold procesarian rangos distintos."""
        from finops.config import FinOpsConfig

        cfg = FinOpsConfig(
            env="dev",
            data={"ingestion": {"lookback_days": 7, "max_usage_date": "2026-08-01"}},
            run_date=date(2026, 9, 21),
        )
        assert cfg.max_date == date(2026, 8, 1)
        cfg.window_override = (date(2026, 9, 20), date(2026, 9, 21))
        assert cfg.max_date == date(2026, 9, 21)
