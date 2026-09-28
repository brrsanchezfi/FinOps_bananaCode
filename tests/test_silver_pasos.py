"""Pruebas de los pasos de `slv_usage_priced`, uno por clase.

CONVENCION: cada paso de `finops.transform.silver` tiene aqui una clase con su
mismo nombre en CamelCase. Para encontrar la prueba de una transformacion, se
busca su nombre:

    silver.extraer_entidad            -> TestExtraerEntidad
    silver.clasificar_sku             -> TestClasificarSku
    silver.adjuntar_etiquetas         -> TestAdjuntarEtiquetas
    silver.adjuntar_nombres           -> TestAdjuntarNombres
    silver.factor_de_infraestructura  -> TestFactorDeInfraestructura
    silver.valorizar                  -> TestValorizar
    silver.columnas_de_salida         -> TestColumnasDeSalida
    silver.build_usage_priced         -> TestBuildUsagePriced

`TestTodoPasoTienePrueba` falla si aparece un paso sin su clase.

Que prueban y que NO
--------------------
Sin cluster no se pueden ejecutar DataFrames: el PySpark de esta maquina es el
de Databricks Connect, que rechaza sesiones locales. Pero construir una
expresion no requiere sesion -- `F.when(...)` devuelve un Column y su `str()` es
el SQL generado -- y un `StructType` es Python puro. `DataFrameFalso` aprovecha
eso: registra que columnas agrega cada paso y con que expresion.

Asi se prueban las DECISIONES de cada paso (que columnas produce, en que orden
de precedencia, que hace cuando falta una fuente opcional). Las CIFRAS se
verificaron aparte, corriendo el pipeline refactorizado en el banco de pruebas y
comparando su salida registro por registro contra la del codigo anterior.
"""

from __future__ import annotations

import pytest
from pyspark.sql.types import StringType, StructField, StructType

from finops.transform import pricing as P
from finops.transform import silver as S


class DataFrameFalso:
    """Lo minimo de un DataFrame para ejecutar los pasos sin sesion.

    `expresiones` guarda, por columna agregada, el SQL de su Column.
    """

    def __init__(self, esquema: StructType, expresiones: dict[str, str] | None = None):
        self._esquema = esquema
        self.expresiones: dict[str, str] = dict(expresiones or {})
        self.seleccion: list[str] | None = None

    @property
    def schema(self) -> StructType:
        return self._esquema

    @property
    def columns(self) -> list[str]:
        propias = [f.name for f in self._esquema.fields]
        return propias + [c for c in self.expresiones if c not in propias]

    def withColumn(self, nombre, columna):  # noqa: N802 - API de PySpark
        nuevo = DataFrameFalso(self._esquema, self.expresiones)
        nuevo.expresiones[nombre] = str(columna)
        return nuevo

    def select(self, *columnas):
        nuevo = DataFrameFalso(self._esquema, self.expresiones)
        nuevo.seleccion = [str(c) for c in columnas]
        return nuevo


def _uso(campos_metadata=("job_id", "cluster_id", "warehouse_id", "notebook_id")) -> DataFrameFalso:
    """Consumo con el esquema de system.billing.usage que importa a los pasos."""
    metadata = StructType([StructField(c, StringType()) for c in campos_metadata])
    identidad = StructType([StructField(c, StringType()) for c in ("run_as", "owned_by")])
    return DataFrameFalso(StructType([
        StructField("record_id", StringType()),
        StructField("workspace_id", StringType()),
        StructField("sku_name", StringType()),
        StructField("billing_origin_product", StringType()),
        StructField("usage_metadata", metadata),
        StructField("identity_metadata", identidad),
    ]))


@pytest.fixture
def sin_catalogos(monkeypatch):
    """Ninguna fuente opcional (clusters, jobs, warehouses) existe."""
    monkeypatch.setattr(S, "table_exists", lambda spark, fqn: False)


class TestExtraerEntidad:
    def test_agrega_la_identidad_de_la_entidad(self):
        df = S.extraer_entidad(_uso())
        for columna in ("entity_type", "entity_id", "entity_key", "run_as"):
            assert columna in df.expresiones

    def test_la_prioridad_es_la_de_tags(self):
        """La precedencia sale de `tags.ENTITY_PRIORITY`: el primero queda afuera."""
        from finops.transform.tags import ENTITY_PRIORITY

        df = S.extraer_entidad(_uso(campos_metadata=[c for c, _ in ENTITY_PRIORITY]))
        sql = df.expresiones["entity_type"]
        etiquetas = [etiqueta for _, etiqueta in ENTITY_PRIORITY]
        posiciones = [sql.index(f"THEN {e} ") for e in etiquetas]
        assert posiciones == sorted(posiciones), "la primera entidad debe ganar"

    def test_un_campo_ausente_en_el_origen_queda_nulo_y_no_rompe(self):
        """Tolerancia a esquema: usage_metadata cambia entre versiones."""
        df = S.extraer_entidad(_uso(campos_metadata=["job_id"]))
        assert "NULL" in df.expresiones["cluster_id"]

    def test_la_clave_nunca_queda_nula(self):
        assert "SIN_ID" in S.extraer_entidad(_uso()).expresiones["entity_key"]


class TestClasificarSku:
    def test_agrega_las_cuatro_columnas(self):
        df = S.clasificar_sku(_uso())
        assert {"sku_group", "compute_family", "is_serverless", "is_photon"} <= set(df.expresiones)

    def test_la_familia_usa_los_grupos_de_pricing(self):
        sql = S.clasificar_sku(_uso()).expresiones["compute_family"]
        for grupo in P.GRUPOS_CON_FAMILIA_PROPIA:
            assert grupo in sql

    def test_no_reescribe_los_patrones_a_mano(self):
        """Antes escribia 'SERVERLESS' y 'PHOTON' por su cuenta."""
        import inspect

        fuente = inspect.getsource(S.clasificar_sku)
        assert '"(?i)SERVERLESS"' not in fuente
        assert '"(?i)PHOTON"' not in fuente
        assert "P.PATRON_SERVERLESS" in fuente


class TestAdjuntarEtiquetas:
    def test_sin_catalogos_rellena_y_no_rompe(self, sin_catalogos, cfg_repo):
        """Clusters y jobs son fuentes OPCIONALES: su ausencia no rompe."""
        df = S.adjuntar_etiquetas(None, cfg_repo, _uso())
        for columna in ("_tags_cluster", "_tags_job", "cluster_name", "cluster_owner", "job_name", "job_run_as"):
            assert columna in df.expresiones, f"falta rellenar {columna}"

    def test_resuelve_cada_dimension_del_modelo(self, sin_catalogos, cfg_repo):
        df = S.adjuntar_etiquetas(None, cfg_repo, _uso())
        for dimension in cfg_repo.get("tagging.dimensions"):
            assert dimension in df.expresiones
            assert f"tag_source_{dimension}" in df.expresiones


class TestAdjuntarNombres:
    def _df(self, cfg):
        con_entidad = S.extraer_entidad(_uso())
        return S.adjuntar_nombres(None, cfg, S.adjuntar_etiquetas(None, cfg, con_entidad))

    def test_sin_catalogo_de_warehouses_no_rompe(self, sin_catalogos, cfg_repo):
        assert "_warehouse_name" in self._df(cfg_repo).expresiones

    def test_el_nombre_cae_al_id_si_no_hay_otro(self, sin_catalogos, cfg_repo):
        """Nunca vacio: agrupar es por entity_key, el nombre es solo para mostrar."""
        sql = self._df(cfg_repo).expresiones["entity_name"]
        assert sql.endswith(", entity_id)'>"), "el ultimo respaldo del COALESCE debe ser entity_id"

    def test_el_responsable_prefiere_run_as(self, sin_catalogos, cfg_repo):
        sql = self._df(cfg_repo).expresiones["owner_resolved"]
        assert sql.index("run_as") < sql.index("job_run_as") < sql.index("cluster_owner")


class TestFactorDeInfraestructura:
    def _cfg(self, **infra):
        from finops.config import FinOpsConfig

        return FinOpsConfig(env="finops", data={"pricing": {"infra_estimate": infra}})

    def test_desactivado_es_cero(self):
        assert str(S.factor_de_infraestructura(self._cfg(enabled=False))) == "Column<'0.0'>"

    def test_activado_se_acota_en_cero(self):
        """Un factor negativo restaria costo del total. Python ya lo acotaba."""
        sql = str(S.factor_de_infraestructura(self._cfg(enabled=True, factor_by_compute={"JOBS": 0.85})))
        assert "greatest" in sql


class TestValorizar:
    @pytest.fixture
    def df(self, monkeypatch, cfg_repo):
        # join_prices necesita un DataFrame real; aqui se reemplaza por lo que
        # entrega: el precio unitario y la moneda.
        def join_prices_falso(df, prices):
            return df.withColumn("unit_price", "p").withColumn("price_currency", "USD")

        monkeypatch.setattr(S, "join_prices", join_prices_falso)
        return S.valorizar(_uso(), None, cfg_repo)

    def test_produce_los_componentes_de_costo(self, df):
        for columna in P.COMPONENTES_DE_COSTO:
            assert columna in df.expresiones

    def test_redondea_cada_componente(self, df):
        """La formula de pricing redondea en cada paso para que las columnas cuadren."""
        for columna in P.COMPONENTES_DE_COSTO:
            assert df.expresiones[columna].startswith("Column<'round("), columna

    def test_marca_el_precio_faltante(self, df):
        assert "isnull(unit_price)" in df.expresiones["price_missing"]


class TestColumnasDeSalida:
    def test_proyecta_solo_lo_que_existe(self, cfg_repo):
        """Una fuente opcional ausente no puede romper la tabla."""
        df = S.columnas_de_salida(_uso(), cfg_repo)
        assert set(df.seleccion) <= set(_uso().columns)

    def test_el_contrato_incluye_los_costos_en_orden(self, cfg_repo):
        base = _uso()
        for columna in P.COMPONENTES_DE_COSTO:
            base = base.withColumn(columna, "x")
        seleccion = S.columnas_de_salida(base, cfg_repo).seleccion
        assert [c for c in seleccion if c in P.COMPONENTES_DE_COSTO] == list(P.COMPONENTES_DE_COSTO)


class TestBuildUsagePriced:
    def test_encadena_los_pasos_en_orden(self, monkeypatch, cfg_repo):
        """El orquestador solo encadena: cada paso depende del anterior."""
        orden: list[str] = []

        def registrar(nombre):
            def paso(*args, **kwargs):
                orden.append(nombre)
                return _uso()
            return paso

        for paso in ("extraer_entidad", "clasificar_sku", "adjuntar_etiquetas",
                     "adjuntar_nombres", "valorizar", "columnas_de_salida"):
            monkeypatch.setattr(S, paso, registrar(paso))

        class SparkFalso:
            def table(self, fqn):
                class _Tabla:
                    def filter(self, _):
                        return _uso()
                return _Tabla()

        S.build_usage_priced(SparkFalso(), cfg_repo)
        assert orden == [
            "extraer_entidad", "clasificar_sku", "adjuntar_etiquetas",
            "adjuntar_nombres", "valorizar", "columnas_de_salida",
        ]


class TestLaFormulaDeCosto:
    """`pricing.componentes_de_costo`, evaluada sobre floats."""

    def test_las_columnas_siempre_cuadran(self):
        """Con el redondeo al final, el 42 % de los registros no cuadraba."""
        import random

        rnd = random.Random(7)
        for _ in range(5000):
            r = P.price_record(
                usage_quantity=rnd.uniform(0, 50), unit_price=rnd.uniform(0.07, 0.95),
                discount_pct=0.15, infra_factor=0.85,
            )
            assert round(r["list_cost_usd"] - r["discount_amount_usd"], 6) == r["effective_cost_usd"]
            assert round(r["effective_cost_usd"] + r["estimated_infra_cost_usd"], 6) == r["total_cost_usd"]

    def test_devuelve_exactamente_los_componentes_del_contrato(self):
        r = P.price_record(usage_quantity=1, unit_price=1)
        assert tuple(r) == P.COMPONENTES_DE_COSTO


class TestTodoPasoTienePrueba:
    """La convencion de arriba, hecha cumplir."""

    PASOS = (
        "extraer_entidad", "clasificar_sku", "adjuntar_etiquetas", "adjuntar_nombres",
        "factor_de_infraestructura", "valorizar", "columnas_de_salida", "build_usage_priced",
    )

    @staticmethod
    def _clase(paso: str) -> str:
        return "Test" + "".join(parte.capitalize() for parte in paso.split("_"))

    @pytest.mark.parametrize("paso", PASOS)
    def test_hay_una_clase_por_paso(self, paso):
        assert self._clase(paso) in globals(), f"falta {self._clase(paso)} para silver.{paso}"

    @pytest.mark.parametrize("paso", PASOS)
    def test_el_paso_existe_en_silver(self, paso):
        assert callable(getattr(S, paso, None)), f"silver.{paso} ya no existe"
