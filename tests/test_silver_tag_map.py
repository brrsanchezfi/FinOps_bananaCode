"""Pruebas del mapa de etiquetas normalizado de la capa silver.

`_normalized_tag_map` normaliza las claves (minusculas, sin simbolos). Dos
etiquetas que colisionan tras normalizar, o claves que quedan vacias, hacian
fallar la etapa silver con DUPLICATED_MAP_KEY.
"""

from __future__ import annotations

import pytest


@pytest.mark.spark
class TestNormalizedTagMap:
    def _mapa(self, spark, etiquetas):
        from pyspark.sql import functions as F

        from finops.transform.silver import _normalized_tag_map

        df = spark.createDataFrame([(etiquetas,)], "tags map<string,string>")
        return df.select(_normalized_tag_map(F.col("tags")).alias("m")).collect()[0]["m"]

    def test_colisiones_y_claves_vacias_no_fallan(self, spark):
        mapa = self._mapa(spark, {"Cost-Center": "a", "cost_center": "b", "ñ": "x", "-": "y"})
        # Las claves vacias se descartan y la colision se resuelve de forma
        # determinista: gana la menor clave original ('Cost-Center' < 'cost_center').
        assert mapa == {"costcenter": "a"}

    def test_claves_sin_colision_se_conservan(self, spark):
        mapa = self._mapa(spark, {"Team": "data", "Cost Center": "cc1"})
        assert mapa == {"team": "data", "costcenter": "cc1"}

    def test_mapa_nulo_devuelve_mapa_vacio(self, spark):
        assert self._mapa(spark, None) == {}

    def test_solo_claves_vacias_devuelve_mapa_vacio(self, spark):
        assert self._mapa(spark, {"-": "y", "ñ": "x"}) == {}
