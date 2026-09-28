"""Descuentos: una sola regla, dos evaluadores.

`pricing.resolve_discount` la evalua en Python y `silver.discount_expr` la
compila a SQL para el pipeline. Comparten `CLAVES_DE_DESCUENTO`, `glob_a_like`,
`pct_de_regla` y `nombre_de_regla`.

Antes cada lado tenia su version, y divergian de forma que pegaba en la
factura. Probado contra el motor real de Databricks, sobre 100 combinaciones de
valor y patron, la traduccion anterior divergia de Python en 8:

  - 3 por mayusculas. `account_id` es un UUID en minusculas: el patron se pasaba
    a mayusculas y la columna no, y LIKE distingue mayusculas. Un descuento
    negociado por cuenta NUNCA se aplicaba en el pipeline, y el tablero
    reportaba precio de lista, por encima de la factura, sin error alguno.
  - 4 por el comodin `_`, literal en glob y comodin en LIKE.
  - 1 por NULL con `*`: Spark lo volvia '' y `'' LIKE '%'` es verdadero.

Con la traduccion actual, las 100 coinciden. Estas pruebas fijan esa propiedad
sin cluster: la expresion Spark se construye sin SparkSession y su `str()` es el
SQL generado.
"""

from __future__ import annotations

import inspect

import pytest

from finops.transform import pricing as P


class TestTraduccionDeGlobALike:
    """El puente entre fnmatch (Python) y LIKE (Spark)."""

    @pytest.mark.parametrize(
        ("glob", "like"),
        [
            ("*SQL*", "%SQL%"),
            ("A?C", "A_C"),
            # `_` y `%` son literales en glob y comodines en LIKE: se escapan.
            ("PREMIUM_JOBS_*", "PREMIUM\\_JOBS\\_%"),
            ("100%", "100\\%"),
            # La barra invertida es el caracter de escape de LIKE: tambien.
            ("A\\B", "A\\\\B"),
            # Todo en mayusculas: la comparacion es insensible en los dos lados.
            ("cc975f4a-*", "CC975F4A-%"),
        ],
    )
    def test_traduce(self, glob: str, like: str):
        assert P.glob_a_like(glob) == like

    @pytest.mark.parametrize("patron", ["[AB]C", "SKU_[0-9]*", "X]"])
    def test_rechaza_los_rangos(self, patron: str):
        """Un rango [...] no tiene equivalente en LIKE. Traducirlo a algo
        aproximado seria volver a divergir en silencio."""
        with pytest.raises(ValueError, match="rango"):
            P.glob_a_like(patron)


class TestPythonYSparkCoincidenEnLosCasosQueDivergian:
    """Los tres bugs, fijados del lado Python y del lado del SQL generado."""

    CUENTA = "cc975f4a-ea39-438b-8de1-48cf26be6b8f"

    def _sql(self, reglas) -> str:
        from finops.transform.silver import discount_expr

        descuento, _ = discount_expr(reglas)
        return str(descuento)

    def test_descuento_por_cuenta_en_minusculas(self):
        """El bug que pegaba en la factura."""
        regla = [{"name": "acuerdo", "match": {"account_id": self.CUENTA}, "discount_pct": 0.15}]
        contexto = P.build_pricing_context({"account_id": self.CUENTA, "sku_name": "X"})
        assert P.resolve_discount(regla, contexto) == (0.15, "acuerdo")

        sql = self._sql(regla)
        assert "like(upper(CAST(account_id AS STRING))" in sql, (
            "la columna tiene que pasarse a mayusculas, como el patron: si no, "
            "LIKE nunca coincide con un UUID en minusculas"
        )

    def test_el_guion_bajo_es_literal(self):
        regla = [{"name": "jobs", "match": {"sku_name": "PREMIUM_JOBS_*"}, "discount_pct": 0.1}]
        assert P.resolve_discount(regla, {"sku_name": "PREMIUMXJOBSXCOMPUTE"}) == (0.0, "sin_descuento")
        assert "PREMIUM\\_JOBS\\_%" in self._sql(regla)

    def test_un_valor_nulo_no_coincide_ni_con_asterisco(self):
        regla = [{"name": "todo", "match": {"cloud": "*"}, "discount_pct": 0.1}]
        assert P.resolve_discount(regla, {"cloud": None}) == (0.0, "sin_descuento")
        assert "isnotnull(upper(CAST(cloud AS STRING)))" in self._sql(regla)


class TestUnaSolaDefinicion:
    def test_toda_clave_del_contexto_se_pasa_a_mayusculas_en_spark(self):
        """El bug de account_id vino de pasar a mayusculas SOLO algunas columnas."""
        reglas = [
            {"name": f"r_{clave}", "match": {clave: "x"}, "discount_pct": 0.1}
            for clave in P.CLAVES_DE_DESCUENTO
        ]
        from finops.transform.silver import discount_expr

        sql = str(discount_expr(reglas)[0])
        for clave in P.CLAVES_DE_DESCUENTO:
            assert f"upper(CAST({clave} AS STRING))" in sql, f"'{clave}' no se pasa a mayusculas"

    def test_python_arma_el_contexto_con_las_mismas_claves(self):
        assert tuple(P.build_pricing_context({})) == P.CLAVES_DE_DESCUENTO

    def test_spark_no_declara_su_propio_contexto(self):
        """LA prueba de este archivo: si vuelve a aparecer una segunda lista de
        claves, o una traduccion de comodines propia, reaparece la divergencia."""
        from finops.transform.silver import discount_expr

        fuente = inspect.getsource(discount_expr)
        assert "CLAVES_DE_DESCUENTO" in fuente
        assert "glob_a_like" in fuente
        assert '.replace("*"' not in fuente, "discount_expr volvio a traducir comodines por su cuenta"
        assert '"account_id":' not in fuente, "discount_expr volvio a declarar su propio contexto"

    def test_el_tope_y_el_nombre_son_los_mismos(self):
        regla = {"name": "exagerado", "discount_pct": 5.0}
        assert P.pct_de_regla(regla) == 0.999
        assert P.nombre_de_regla({}) == "sin_nombre"
        assert P.resolve_discount([regla], {}) == (0.999, "exagerado")

    def test_la_primera_regla_que_coincide_gana(self):
        """Python recorre en orden; Spark compila al reves para que la primera
        quede en el `when` mas externo."""
        reglas = [
            {"name": "especifica", "match": {"sku_group": "SQL"}, "discount_pct": 0.25},
            {"name": "general", "match": {}, "discount_pct": 0.10},
        ]
        assert P.resolve_discount(reglas, {"sku_group": "SQL"}) == (0.25, "especifica")

        from finops.transform.silver import discount_expr

        _, nombre = discount_expr(reglas)
        sql = str(nombre)
        assert sql.index("especifica") < sql.index("general")


class TestLaConfiguracionRechazaLosRangos:
    def test_validate_config_lo_detecta(self, conf_dir):
        """Falla en `finops validate` y en CI, no en silencio en la factura."""
        from finops.config import load_config
        from finops.errors import ConfigError

        with pytest.raises(ConfigError, match="rango"):
            load_config(
                "finops", conf_dir=conf_dir, use_env_vars=False, use_local_overlay=False,
                overrides={"pricing": {"discounts": [
                    {"name": "mala", "match": {"sku_name": "SKU_[0-9]*"}, "discount_pct": 0.1},
                ]}},
            )
