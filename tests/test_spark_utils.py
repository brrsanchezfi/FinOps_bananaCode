"""Pruebas de los adaptadores de Spark que se pueden verificar sin cluster.

Se usa una SparkSession simulada que registra las llamadas: alcanza para fijar
invariantes de configuracion y de construccion de DDL, que es donde se han
producido fallos reales en despliegue.
"""

from __future__ import annotations

from datetime import date

import pytest

from finops.errors import ConfigError
from finops.spark_utils import (
    PARTITION_OVERWRITE_MODE,
    build_create_catalog_sql,
    build_create_schema_sql,
    catalog_exists,
    configure_session,
    delete_date_range,
    ensure_catalog,
)


class ConfFalsa:
    def __init__(self) -> None:
        self.valores: dict[str, str] = {}

    def set(self, clave: str, valor: str) -> None:
        self.valores[clave] = valor

    def get(self, clave: str, default=None):
        return self.valores.get(clave, default)


class SparkFalso:
    """Sesion simulada: registra el SQL ejecutado y permite forzar fallos.

    `existentes` son los objetos (catalogos o tablas) que `DESCRIBE` encuentra;
    para el resto se lanza, que es como el codigo real detecta la ausencia.
    """

    def __init__(self, *, falla_en: str | None = None, existentes: set[str] | None = None) -> None:
        self.conf = ConfFalsa()
        self.sql_ejecutado: list[str] = []
        self._falla_en = falla_en
        self._existentes = existentes or set()

    def sql(self, consulta: str):
        self.sql_ejecutado.append(consulta)
        if self._falla_en and self._falla_en in consulta:
            raise RuntimeError("fallo simulado de Unity Catalog")
        es_describe = consulta.startswith(("DESCRIBE CATALOG", "DESCRIBE TABLE"))
        if es_describe and consulta.split()[-1] not in self._existentes:
            raise RuntimeError("objeto inexistente")
        return self

    def limit(self, _n):
        return self

    def collect(self):
        return []


class TestConfiguracionDeSesion:
    def test_modo_de_particion_es_estatico(self):
        """Regresion: en modo dinamico, Delta rechaza 'overwriteSchema'.

        [DELTA_OVERWRITE_SCHEMA_WITH_DYNAMIC_PARTITION_OVERWRITE]
        El pipeline reescribe tablas de snapshot con overwriteSchema, asi que el
        modo dinamico rompe la etapa bronze.
        """
        spark = SparkFalso()
        configure_session(spark)
        assert spark.conf.get("spark.sql.sources.partitionOverwriteMode") == "static"
        assert PARTITION_OVERWRITE_MODE == "static"

    def test_ajustes_de_rendimiento(self):
        spark = SparkFalso()
        configure_session(spark)
        assert spark.conf.get("spark.sql.adaptive.enabled") == "true"
        assert spark.conf.get("spark.databricks.delta.schema.autoMerge.enabled") == "true"

    def test_shuffle_auto_no_fija_particiones(self):
        spark = SparkFalso()
        configure_session(spark, "auto")
        assert spark.conf.get("spark.sql.shuffle.partitions") is None

    def test_shuffle_explicito(self):
        spark = SparkFalso()
        configure_session(spark, 64)
        assert spark.conf.get("spark.sql.shuffle.partitions") == "64"


class TestEnsureCatalog:
    def test_no_ejecuta_ddl_si_ya_existe(self):
        """CREATE CATALOG IF NOT EXISTS falla si el metastore no tiene storage root,
        asi que no puede usarse como no-op."""
        spark = SparkFalso(existentes={"finops"})
        creado = ensure_catalog(spark, "finops")
        assert creado is False
        assert not any(s.startswith("CREATE CATALOG") for s in spark.sql_ejecutado)

    def test_crea_si_falta(self):
        spark = SparkFalso()
        assert ensure_catalog(spark, "finops") is True
        assert "CREATE CATALOG IF NOT EXISTS finops" in spark.sql_ejecutado

    def test_crea_con_ubicacion_gestionada(self):
        spark = SparkFalso()
        ensure_catalog(spark, "finops", managed_location="abfss://c@a/uc")
        assert any("MANAGED LOCATION 'abfss://c@a/uc'" in s for s in spark.sql_ejecutado)

    def test_no_crea_si_esta_deshabilitado(self):
        spark = SparkFalso()
        with pytest.raises(ConfigError, match="create_if_missing"):
            ensure_catalog(spark, "finops", create_if_missing=False)
        assert not any(s.startswith("CREATE CATALOG") for s in spark.sql_ejecutado)

    def test_error_de_creacion_es_accionable(self):
        spark = SparkFalso(falla_en="CREATE CATALOG")
        with pytest.raises(ConfigError) as exc:
            ensure_catalog(spark, "finops")
        mensaje = str(exc.value)
        assert "Default Storage" in mensaje
        assert "MANAGED LOCATION" in mensaje
        assert "managed_location" in mensaje


class TestDdl:
    def test_catalogo(self):
        assert build_create_catalog_sql("c") == "CREATE CATALOG IF NOT EXISTS c"

    def test_schema_con_raiz(self):
        assert build_create_schema_sql("c", "gold", "abfss://x/r").endswith("'abfss://x/r/gold'")

    def test_catalog_exists_es_tolerante(self):
        assert catalog_exists(SparkFalso(), "no_existe") is False
        assert catalog_exists(SparkFalso(existentes={"si_existe"}), "si_existe") is True


class TestDeleteDateRange:
    def test_no_hace_nada_si_la_tabla_no_existe(self):
        spark = SparkFalso()
        delete_date_range(
            spark, "c.s.t", date_column="d", min_date=date(2026, 1, 1), max_date=date(2026, 1, 5)
        )
        assert not any(s.startswith("DELETE") for s in spark.sql_ejecutado)

    def test_dry_run_no_borra(self):
        spark = SparkFalso(existentes={"c.s.t"})
        delete_date_range(
            spark, "c.s.t", date_column="d",
            min_date=date(2026, 1, 1), max_date=date(2026, 1, 5), dry_run=True,
        )
        assert not any(s.startswith("DELETE") for s in spark.sql_ejecutado)


class TestNormalizacionDeTipos:
    """Regresion: Spark no puede inferir un esquema con LongType y DoubleType
    en la misma columna.

        [CANNOT_MERGE_TYPE] Can not merge type `DoubleType` and `LongType`

    Es facil de provocar: `sum()` de una secuencia vacia devuelve el entero 0 y
    `round(0, 2)` sigue siendo entero, asi que un presupuesto sin consumo
    cambiaba el tipo de la columna respecto a los que si tenian.
    """

    def test_promueve_enteros_a_decimales(self):
        from finops.spark_utils import normalize_row_types

        filas = [{"costo": 0}, {"costo": 12.5}]
        assert normalize_row_types(filas) == [{"costo": 0.0}, {"costo": 12.5}]
        assert all(isinstance(f["costo"], float) for f in normalize_row_types(filas))

    def test_no_toca_columnas_enteras(self):
        from finops.spark_utils import normalize_row_types

        filas = [{"dias": 0}, {"dias": 31}]
        assert normalize_row_types(filas) == filas
        assert all(isinstance(f["dias"], int) for f in normalize_row_types(filas))

    def test_los_booleanos_no_se_convierten(self):
        """bool es subclase de int en Python, pero su tipo en Spark es BooleanType."""
        from finops.spark_utils import normalize_row_types

        filas = [{"activo": True, "costo": 1}, {"activo": False, "costo": 2.5}]
        salida = normalize_row_types(filas)
        assert salida[0]["activo"] is True
        assert isinstance(salida[0]["costo"], float)

    def test_respeta_los_nulos(self):
        from finops.spark_utils import normalize_row_types

        filas = [{"costo": None}, {"costo": 3.0}]
        assert normalize_row_types(filas) == [{"costo": None}, {"costo": 3.0}]

    def test_claves_ausentes_en_algunas_filas(self):
        from finops.spark_utils import normalize_row_types

        filas = [{"a": 1}, {"a": 2.0, "b": 5}]
        salida = normalize_row_types(filas)
        assert isinstance(salida[0]["a"], float)
        assert "b" not in salida[0]

    def test_lista_vacia(self):
        from finops.spark_utils import normalize_row_types

        assert normalize_row_types([]) == []

    def test_no_muta_la_entrada(self):
        from finops.spark_utils import normalize_row_types

        filas = [{"costo": 0}, {"costo": 1.5}]
        normalize_row_types(filas)
        assert isinstance(filas[0]["costo"], int)
