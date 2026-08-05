"""Pruebas del registro de tablas y de la coherencia del modelo."""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from finops.catalog import ALL_TABLES, TABLES_BY_KEY, table_map

REPO_ROOT = Path(__file__).resolve().parents[1]
DASHBOARDS_DIR = REPO_ROOT / "dashboards"


class TestRegistro:
    def test_claves_unicas(self):
        claves = [t.key for t in ALL_TABLES]
        assert len(claves) == len(set(claves))

    def test_nombres_unicos_por_capa(self):
        pares = [(t.layer, t.name) for t in ALL_TABLES]
        assert len(pares) == len(set(pares))

    def test_todas_tienen_descripcion(self):
        sin_descripcion = [t.key for t in ALL_TABLES if len(t.description.strip()) < 20]
        assert sin_descripcion == []

    def test_capas_validas(self):
        assert {t.layer for t in ALL_TABLES} <= {"bronze", "silver", "gold"}

    def test_fqn_usa_la_configuracion(self, cfg_dev):
        assert TABLES_BY_KEY["fct_cost_daily"].fqn(cfg_dev) == "finops_dev.gold.fct_cost_daily"

    def test_table_map_cubre_todas(self, cfg_dev):
        mapa = table_map(cfg_dev)
        assert len(mapa) == len(ALL_TABLES)
        assert all(v.count(".") == 2 for v in mapa.values())


class TestCoherenciaConDashboards:
    """Los dashboards versionados solo pueden referenciar tablas del registro."""

    def _archivos(self):
        return sorted(DASHBOARDS_DIR.glob("*.lvdash.json"))

    def test_existen_dashboards(self):
        assert self._archivos(), "no hay dashboards versionados en dashboards/"

    @pytest.mark.parametrize("nombre", ["finops_ejecutivo", "finops_costos", "finops_optimizacion"])
    def test_json_valido(self, nombre):
        ruta = DASHBOARDS_DIR / f"{nombre}.lvdash.json"
        assert ruta.exists(), f"falta {ruta.name}"
        contenido = json.loads(ruta.read_text(encoding="utf-8"))
        assert contenido.get("datasets"), f"{ruta.name} no define datasets"
        assert contenido.get("pages"), f"{ruta.name} no define paginas"

    def test_los_marcadores_de_tabla_existen_en_el_registro(self):
        # El SQL de los dashboards referencia tablas con marcadores {{clave}}
        # que el desplegador sustituye por el FQN del entorno.
        patron = re.compile(r"\{\{([a-z0-9_]+)\}\}")
        desconocidos: set[str] = set()
        for archivo in self._archivos():
            for clave in patron.findall(archivo.read_text(encoding="utf-8")):
                if clave not in TABLES_BY_KEY:
                    desconocidos.add(f"{archivo.name}:{clave}")
        assert desconocidos == set(), f"marcadores sin tabla registrada: {sorted(desconocidos)}"

    def test_no_hay_nombres_de_catalogo_incrustados(self):
        # Un FQN fijo rompe el despliegue multi-entorno.
        for archivo in self._archivos():
            texto = archivo.read_text(encoding="utf-8")
            assert "finops_dev." not in texto, f"{archivo.name} tiene el catalogo de dev incrustado"
            assert "finops_qa." not in texto, f"{archivo.name} tiene el catalogo de qa incrustado"
