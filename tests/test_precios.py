"""Precio unitario: una sola precedencia para las dos valorizaciones.

El producto valoriza el consumo en dos sitios: la capa silver (que alimenta las
tablas gold) y la vista `vw_usage_live` (que alimenta el tablero de gobierno).
Si eligen precios distintos, los dos tableros muestran costos distintos para el
mismo consumo.

Sus joins estan sincronizados a mano y coinciden. La unica diferencia de
definicion era esta: silver recurria a `promotional.default` como ultimo
respaldo y la vista no. Medido sobre una cuenta real, eso no movia ninguna cifra
-- de 1.067 tramos, ninguno carece de precio principal -- pero era el primer
paso de una divergencia. Ahora las dos leen `pricing.CAMPOS_DE_PRECIO`.

Sin cluster: `StructType` es Python puro, asi que el esquema de
`system.billing.list_prices` se puede construir aqui.
"""

from __future__ import annotations

import pytest

from finops.transform import pricing as P


class TestLaPrecedencia:
    def test_effective_list_manda(self):
        """Refleja el precio realmente aplicable cuando hay promociones. En una
        cuenta real difiere de `default` en 300 de 1.067 tramos."""
        assert P.CAMPOS_DE_PRECIO[0] == "effective_list.default"
        assert P.CAMPOS_DE_PRECIO.index("default") < P.CAMPOS_DE_PRECIO.index("promotional.default")


class TestCamposDisponibles:
    """La decision de que leer es pura: el esquema de origen cambia."""

    def test_esquema_completo(self):
        estructura = {"effective_list": ["default"], "default": None, "promotional": ["default"]}
        assert P.campos_de_precio_disponibles(estructura) == list(P.CAMPOS_DE_PRECIO)

    def test_se_omite_lo_que_no_existe(self):
        """Tolerancia a esquema: una columna ausente se salta, no rompe."""
        assert P.campos_de_precio_disponibles({"default": None}) == ["default"]

    def test_un_struct_sin_el_subcampo_se_omite(self):
        estructura = {"effective_list": ["otra_cosa"], "default": None}
        assert P.campos_de_precio_disponibles(estructura) == ["default"]

    def test_esquema_vacio(self):
        assert P.campos_de_precio_disponibles({}) == []

    def test_respeta_el_orden_de_precedencia_y_no_el_del_esquema(self):
        """Si el struct llega con otro orden, el precio elegido no puede cambiar."""
        estructura = {"promotional": ["default"], "default": None, "effective_list": ["default"]}
        assert P.campos_de_precio_disponibles(estructura) == list(P.CAMPOS_DE_PRECIO)


class TestElAdaptadorSpark:
    """`silver._unit_price_column` se prueba con el esquema real, sin sesion."""

    def _precios(self, campos_pricing):
        """DataFrame falso: `_unit_price_column` solo mira columnas y esquema."""
        from pyspark.sql.types import StructField, StructType

        esquema = StructType([StructField("pricing", StructType(campos_pricing))])

        class _Falso:
            columns = ["pricing"]
            schema = esquema

        return _Falso()

    def test_con_el_esquema_de_system_billing_list_prices(self):
        from pyspark.sql.types import DoubleType, StructField, StructType

        from finops.transform.silver import _unit_price_column

        con_default = StructType([StructField("default", DoubleType())])
        columna = _unit_price_column(self._precios([
            StructField("default", DoubleType()),
            StructField("effective_list", con_default),
            StructField("promotional", con_default),
        ]))
        sql = str(columna)
        posiciones = [sql.index(f"pricing.{c}") for c in P.CAMPOS_DE_PRECIO]
        assert posiciones == sorted(posiciones), (
            f"el COALESCE no sigue CAMPOS_DE_PRECIO: {sql}"
        )

    def test_tolera_que_falte_un_campo(self):
        from pyspark.sql.types import DoubleType, StructField

        from finops.transform.silver import _unit_price_column

        sql = str(_unit_price_column(self._precios([StructField("default", DoubleType())])))
        assert "pricing.default" in sql
        assert "effective_list" not in sql


class TestLasDosValorizacionesCoinciden:
    def test_la_vista_usa_la_misma_precedencia(self, conf_dir):
        from finops.config import load_config
        from finops.views import build_usage_live_sql

        cfg = load_config("finops", conf_dir=conf_dir, use_env_vars=False, use_local_overlay=False)
        esperado = "COALESCE(" + ", ".join(f"pricing.{c}" for c in P.CAMPOS_DE_PRECIO) + ")"
        assert esperado in build_usage_live_sql(cfg)

    @pytest.mark.parametrize("modulo", ["finops.views", "finops.transform.silver"])
    def test_nadie_declara_su_propia_precedencia(self, modulo):
        """Si una de las dos vuelve a escribir los campos a mano, reaparece la
        posibilidad de que el tablero de gobierno y gold valoricen distinto."""
        import importlib
        import inspect

        fuente = inspect.getsource(importlib.import_module(modulo))
        assert "pricing.effective_list.default" not in fuente, (
            f"{modulo} volvio a declarar los campos de precio a mano"
        )
