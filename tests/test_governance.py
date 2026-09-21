"""Pruebas del puente con DKOps para el gobierno de la capa bronze.

El riesgo de adoptar DKOps por capas es tener DOS registros de tablas que
divergen: `catalog.py` (Python) y los contratos JSON. La prueba central de este
archivo es justamente esa -- que ambos resuelven al mismo nombre calificado --
porque si divergen, el pipeline escribe en un sitio y el contrato valida otro.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
CONTRATOS_BRONZE = REPO_ROOT / "contracts" / "tables" / "bronze"

pytest.importorskip("DKOps", reason="DKOps no esta instalado")


def _cfg(conf_dir):
    from finops.config import load_config

    return load_config("dev", conf_dir=conf_dir, use_env_vars=False, use_local_overlay=False)


def _tablas_bronze():
    from finops.catalog import ALL_TABLES

    return [t for t in ALL_TABLES if t.layer == "bronze"]


class TestAmbienteDerivadoDeFinOps:
    """La configuracion la manda FinOps; DKOps solo la consume."""

    def test_las_tres_capas_comparten_catalogo(self, conf_dir):
        from finops.governance import build_environment_dict

        entorno = build_environment_dict(_cfg(conf_dir))
        catalogos = entorno["environments"]["finops"]["catalogs"]
        assert set(catalogos) == {"bronze", "silver", "gold"}
        assert len(set(catalogos.values())) == 1, (
            "FinOps usa UN catalogo con tres schemas (docs/adr/0005)"
        )

    def test_los_schemas_salen_de_la_configuracion(self, conf_dir):
        from finops.governance import build_environment_dict

        cfg = _cfg(conf_dir)
        paths = build_environment_dict(cfg)["environments"]["finops"]["paths"]
        for capa in ("bronze", "silver", "gold"):
            assert paths[f"schema_{capa}"] == cfg.schema(capa)

    def test_el_ambiente_se_resuelve_por_nombre_no_por_workspace(self, conf_dir):
        """El contrato no puede depender de en que workspace corre.

        Es lo que sacamos del repositorio al volverlo producto: si DKOps
        resolviera por workspace_id, cada instalacion tendria que editar los
        contratos con el id de su cuenta.
        """
        from finops.governance import build_environment_dict

        entorno = build_environment_dict(_cfg(conf_dir))
        assert entorno["DATABRICKS_TARGET"] == "dev"
        assert list(entorno["environments"]) == ["finops"]

    def test_el_catalogo_sigue_al_de_finops(self):
        """Cambiar el catalogo en conf/ debe moverse solo a los contratos."""
        from finops.config import FinOpsConfig
        from finops.governance import build_environment_dict

        cfg = FinOpsConfig(
            env="dev",
            data={"catalog": {
                "catalog": "otro_catalogo",
                "bronze_schema": "b", "silver_schema": "s", "gold_schema": "g",
            }},
        )
        entorno = build_environment_dict(cfg)["environments"]["finops"]
        assert set(entorno["catalogs"].values()) == {"otro_catalogo"}
        assert entorno["paths"]["schema_bronze"] == "b"


class TestLosDosRegistrosNoDivergen:
    """`catalog.py` y los contratos JSON describen las mismas tablas."""

    def test_cada_tabla_bronze_tiene_contrato(self):
        esperados = {t.name for t in _tablas_bronze()}
        existentes = {p.stem for p in CONTRATOS_BRONZE.glob("*.json")}
        assert esperados - existentes == set(), "tablas bronze sin contrato"

    def test_no_hay_contratos_huerfanos(self):
        esperados = {t.name for t in _tablas_bronze()}
        existentes = {p.stem for p in CONTRATOS_BRONZE.glob("*.json")}
        assert existentes - esperados == set(), "contratos sin tabla en el registro"

    def test_el_contrato_resuelve_al_mismo_nombre_que_el_registro(self, conf_dir):
        """LA prueba de este archivo.

        Si divergen, el pipeline escribe en una tabla y el contrato valida otra,
        y nada lo delata hasta que los numeros no cuadran.
        """
        from finops.governance import load_table_contract

        cfg = _cfg(conf_dir)
        for tabla in _tablas_bronze():
            contrato = load_table_contract(cfg, "bronze", tabla.name)
            assert contrato.full_name == tabla.fqn(cfg), (
                f"'{tabla.name}': contrato dice '{contrato.full_name}' "
                f"y el registro '{tabla.fqn(cfg)}'"
            )

    def test_las_particiones_coinciden_con_el_registro(self, conf_dir):
        from finops.governance import load_table_contract

        cfg = _cfg(conf_dir)
        for tabla in _tablas_bronze():
            contrato = load_table_contract(cfg, "bronze", tabla.name)
            assert tuple(contrato.partitions) == tuple(tabla.partition_by), tabla.name


class TestToleranciaDelContratoDeBronze:
    """Bronze copia la system table: el contrato la tolera, no la disciplina."""

    @pytest.mark.parametrize(
        "ruta", sorted(CONTRATOS_BRONZE.glob("*.json")), ids=lambda p: p.stem
    )
    def test_toda_columna_es_nullable(self, ruta: Path):
        """`nullable: false` sobre un origen ajeno convierte un dato faltante
        en una corrida rota."""
        contrato = json.loads(ruta.read_text(encoding="utf-8"))
        obligatorias = [c["name"] for c in contrato["columns"] if not c.get("nullable", True)]
        assert obligatorias == [], f"{ruta.stem} declara columnas obligatorias: {obligatorias}"

    @pytest.mark.parametrize(
        "ruta", sorted(CONTRATOS_BRONZE.glob("*.json")), ids=lambda p: p.stem
    )
    def test_admite_columnas_nuevas(self, ruta: Path):
        """Las system tables ganan columnas sin aviso; sin merge_schema, la
        primera columna nueva rompe la ingesta en vez de incorporarse."""
        contrato = json.loads(ruta.read_text(encoding="utf-8"))
        assert contrato["properties"]["merge_schema"] is True

    @pytest.mark.parametrize(
        "ruta", sorted(CONTRATOS_BRONZE.glob("*.json")), ids=lambda p: p.stem
    )
    def test_los_tipos_complejos_conservan_su_forma_en_el_comentario(self, ruta: Path):
        """DKOps valida la CLASE del tipo, no su interior.

        `usage_metadata` es un STRUCT de 50+ campos: el contrato no puede
        validarlos, pero guardar el tipo completo en el comentario hace que un
        cambio de Databricks aparezca en el diff del pull request. Sin esto, la
        deriva dentro de un struct seria invisible.
        """
        contrato = json.loads(ruta.read_text(encoding="utf-8"))
        for columna in contrato["columns"]:
            if columna["type"] in {"STRUCT", "MAP", "ARRAY", "DECIMAL"}:
                assert columna.get("comment"), (
                    f"{ruta.stem}.{columna['name']} es {columna['type']} y no guarda su forma"
                )


class TestTraduccionDeTipos:
    """Spark reporta BIGINT/INT; el vocabulario de DKOps es LONG/INTEGER."""

    def _tipo_base(self):
        sys.path.insert(0, str(REPO_ROOT / "scripts"))
        from contratos import tipo_base

        return tipo_base

    @pytest.mark.parametrize(
        ("spark", "dkops"),
        [
            ("BIGINT", "LONG"),
            ("INT", "INTEGER"),
            ("STRING", "STRING"),
            ("MAP<STRING,STRING>", "MAP"),
            ("DECIMAL(38,18)", "DECIMAL"),
            ("STRUCT<A:STRING,B:INT>", "STRUCT"),
            ("ARRAY<STRING>", "ARRAY"),
            ("TIMESTAMP_NTZ", "TIMESTAMP"),
        ],
    )
    def test_traduce_al_vocabulario_de_dkops(self, spark: str, dkops: str):
        assert self._tipo_base()(spark) == dkops
