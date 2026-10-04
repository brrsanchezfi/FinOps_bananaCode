"""Clasificacion de SKU: una sola regla, dos evaluadores.

`pricing.classify_sku` la evalua en Python y `silver.sku_group_expr` la compila
a una expresion Spark. Las dos recorren `pricing.CASCADA_DE_SKU`.

Antes eran dos implementaciones separadas de la misma regla. Compartian los
patrones, pero la PRECEDENCIA estaba escrita dos veces, y la version de Spark --
la que de verdad corre en el pipeline -- no tenia ninguna prueba: `silver.py`
estaba al 0 % de cobertura. Una divergencia entre ambas habria cambiado el grupo
de un SKU, y con el, la cifra de todos los tableros que agrupan por `sku_group`.

Estas pruebas cuidan la propiedad que hace segura esa unificacion: que no vuelva
a aparecer una segunda implementacion.

Las expresiones Spark necesitan una SparkSession activa para construirse (pyspark
3.5 lo exige hasta para `F.lit`), asi que esas pruebas usan la fixture `spark`
--el Launcher de DKOps en local-- y se omiten si pyspark no esta instalado.
`TestParidadPythonSpark` va mas alla del texto de la expresion: la EVALUA sobre
un DataFrame y compara fila por fila contra `classify_sku`.
"""

from __future__ import annotations

import inspect

import pytest

from finops.transform import pricing as P


class TestLaCascadaEsLaFuenteDeVerdad:
    def test_cubre_productos_y_patrones(self):
        productos = {p.valor for p in P.CASCADA_DE_SKU if p.campo == "producto"}
        patrones = {p.valor for p in P.CASCADA_DE_SKU if p.campo == "sku"}
        assert productos == set(P.PRODUCT_TO_GROUP)
        assert patrones == {patron for patron, _ in P.SKU_PATTERN_SPECS}

    def test_el_producto_manda_sobre_el_nombre_del_sku(self):
        """`billing_origin_product` es un campo controlado; el nombre del SKU es
        reconocimiento por patron. Si se invierte el orden, un SKU cuyo nombre
        sugiere otra cosa se clasifica mal."""
        campos = [p.campo for p in P.CASCADA_DE_SKU]
        assert campos.index("sku") > max(i for i, c in enumerate(campos) if c == "producto")

    @pytest.mark.parametrize("paso", P.CASCADA_DE_SKU, ids=lambda p: f"{p.campo}:{p.valor[:28]}")
    def test_todo_paso_usa_vocabulario_conocido(self, paso):
        """El compilador de Spark traduce campo por campo y operador por
        operador. Uno nuevo que el compilador no conozca clasificaria distinto
        en Python que en Spark, en silencio."""
        assert paso.campo in P.CAMPOS
        assert paso.operador in P.OPERADORES
        assert paso.grupo in P.SKU_GROUPS

    def test_solo_sql_jobs_y_dlt_tienen_variante_serverless(self):
        con_variante = {p.grupo for p in P.CASCADA_DE_SKU if p.variante_serverless}
        assert con_variante == {"SQL", "JOBS", "DLT"}
        for grupo in con_variante:
            assert f"SERVERLESS_{grupo}" in P.SKU_GROUPS


@pytest.mark.spark
class TestElCompiladorDeSparkCubreLaCascada:
    """Se inspecciona el texto de la expresion generada."""

    @pytest.fixture(scope="class")
    def expresion(self, spark) -> str:
        from finops.transform.silver import sku_group_expr

        return str(sku_group_expr())

    def test_genera_un_case_por_paso(self, expresion: str):
        variantes = sum(1 for p in P.CASCADA_DE_SKU if p.variante_serverless)
        esperados = len(P.CASCADA_DE_SKU) + variantes + 1  # +1: el respaldo
        assert expresion.count("CASE WHEN") == esperados

    @pytest.mark.parametrize("grupo", sorted(set(P.PRODUCT_TO_GROUP.values())))
    def test_todo_grupo_aparece_en_la_expresion(self, expresion: str, grupo: str):
        assert grupo in expresion

    def test_incluye_el_respaldo(self, expresion: str):
        assert P.GRUPO_POR_DEFECTO in expresion
        assert P.GRUPO_POR_DEFECTO_SERVERLESS in expresion

    def test_no_reimplementa_la_cascada(self):
        """LA prueba de este archivo.

        Si `sku_group_expr` vuelve a recorrer `PRODUCT_TO_GROUP` o
        `SKU_PATTERN_SPECS` por su cuenta, reaparece la segunda implementacion
        que este trabajo elimino: los datos seguirian compartidos, pero la
        precedencia volveria a estar escrita dos veces.
        """
        from finops.transform.silver import sku_group_expr

        fuente = inspect.getsource(sku_group_expr)
        assert "CASCADA_DE_SKU" in fuente
        for prohibido in ("PRODUCT_TO_GROUP", "SKU_PATTERN_SPECS", "_SKU_PATTERNS"):
            assert prohibido not in fuente, (
                f"sku_group_expr volvio a leer '{prohibido}' en vez de la cascada"
            )


class TestEquivalenciaDeReglas:
    """Casos donde Python y Spark tienen que coincidir por construccion.

    No se evaluan contra un DataFrame (requeriria cluster): se comprueba que el
    grupo que decide la cascada esta declarado en la expresion compilada.
    """

    CASOS = [
        ("PREMIUM_ALL_PURPOSE_COMPUTE", "", "ALL_PURPOSE"),
        ("PREMIUM_JOBS_COMPUTE", "", "JOBS"),
        ("PREMIUM_SQL_PRO_COMPUTE", "", "SQL"),
        ("PREMIUM_JOBS_SERVERLESS_COMPUTE_US_WEST_3", "", "SERVERLESS_JOBS"),
        ("PREMIUM_SERVERLESS_SQL_COMPUTE", "", "SERVERLESS_SQL"),
        ("PREMIUM_DLT_ADVANCED_COMPUTE", "", "DLT"),
        ("CUALQUIER_COSA_RARA", "", "OTHER"),
        # `billing_origin_product` es coincidencia EXACTA, no por patron: manda
        # sobre el nombre del SKU cuando esta en el mapa.
        ("LO_QUE_SEA", "REAL_TIME_INFERENCE", "MODEL_SERVING"),
        ("PREMIUM_SQL_PRO_COMPUTE", "JOBS", "JOBS"),
        # Un producto FUERA del mapa cae al reconocimiento por nombre de SKU, y
        # si tampoco hay patron, al respaldo. Es el caso de NETWORKING, APPS y
        # LAKEBASE en una cuenta real.
        ("PRIVATE_CONNECTIVITY_ENDPOINT", "NETWORKING", "OTHER"),
    ]

    @pytest.mark.parametrize(("sku", "producto", "esperado"), CASOS)
    def test_python_clasifica_como_se_espera(self, sku, producto, esperado):
        assert P.classify_sku(sku, producto) == esperado

    @pytest.mark.spark
    @pytest.mark.parametrize(("sku", "producto", "esperado"), CASOS)
    def test_el_grupo_esperado_existe_en_la_expresion_spark(self, spark, sku, producto, esperado):
        from finops.transform.silver import sku_group_expr

        assert esperado in str(sku_group_expr())


@pytest.mark.spark
class TestParidadPythonSpark:
    """La expresion Spark, evaluada, clasifica igual que `classify_sku`.

    Es la prueba que faltaba: las anteriores solo miran el TEXTO de la
    expresion. Aqui se corre sobre un DataFrame con todos los casos de
    `TestEquivalenciaDeReglas` mas uno por cada paso de la cascada.
    """

    @staticmethod
    def _casos() -> list[tuple[str, str]]:
        casos = [(sku, producto) for sku, producto, _ in TestEquivalenciaDeReglas.CASOS]
        for paso in P.CASCADA_DE_SKU:
            if paso.campo == "producto":
                casos.append(("SKU_SIN_PATRON", paso.valor))
                casos.append(("PREMIUM_SERVERLESS_X", paso.valor))
        casos += [("", ""), ("premium_jobs_compute", "")]
        return casos

    def test_grupo_identico_fila_por_fila(self, spark):
        from finops.transform.silver import sku_group_expr

        df = spark.createDataFrame(self._casos(), "sku_name string, billing_origin_product string")
        filas = df.withColumn("sku_group", sku_group_expr()).collect()

        distintos = [
            (f["sku_name"], f["billing_origin_product"], f["sku_group"],
             P.classify_sku(f["sku_name"], f["billing_origin_product"]))
            for f in filas
            if f["sku_group"] != P.classify_sku(f["sku_name"], f["billing_origin_product"])
        ]
        assert distintos == [], f"(sku, producto, spark, python) que divergen: {distintos}"

    def test_nulos_caen_al_respaldo(self, spark):
        from finops.transform.silver import sku_group_expr

        df = spark.createDataFrame([(None, None)], "sku_name string, billing_origin_product string")
        assert df.select(sku_group_expr().alias("g")).first()["g"] == P.classify_sku(None, None)
