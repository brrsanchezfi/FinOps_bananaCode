"""Pruebas del registro de tablas y de la coherencia del modelo."""

from __future__ import annotations

import json
import re
import sys
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


ENTORNOS = ("dev", "qa", "prd")
NOMBRES = ("finops_ejecutivo", "finops_costos", "finops_optimizacion")


def _config(env: str):
    from finops.config import load_config

    return load_config(env, conf_dir=REPO_ROOT / "conf", use_env_vars=False)


class TestCoherenciaConDashboards:
    """Los dashboards versionados solo pueden consultar tablas del registro.

    Se generan ya resueltos por entorno (`dashboards/<env>/`), asi que la
    verificacion es que cada FQN referenciado exista en el registro para ESE
    entorno, y que no quede ningun marcador sin sustituir.
    """

    @pytest.mark.parametrize("env", ENTORNOS)
    @pytest.mark.parametrize("nombre", NOMBRES)
    def test_json_valido(self, env, nombre):
        ruta = DASHBOARDS_DIR / env / f"{nombre}.lvdash.json"
        assert ruta.exists(), f"falta {ruta}"
        contenido = json.loads(ruta.read_text(encoding="utf-8"))
        assert contenido.get("datasets"), f"{ruta} no define datasets"
        assert contenido.get("pages"), f"{ruta} no define paginas"

    @pytest.mark.parametrize("env", ENTORNOS)
    def test_no_quedan_marcadores_sin_sustituir(self, env):
        patron = re.compile(r"\{\{[a-z0-9_]+\}\}")
        pendientes = {
            f"{archivo.name}:{m}"
            for archivo in sorted((DASHBOARDS_DIR / env).glob("*.lvdash.json"))
            for m in patron.findall(archivo.read_text(encoding="utf-8"))
        }
        assert pendientes == set(), f"marcadores sin resolver en {env}: {sorted(pendientes)}"

    @pytest.mark.parametrize("env", ENTORNOS)
    def test_las_tablas_referenciadas_existen_en_el_registro(self, env):
        cfg = _config(env)
        conocidas = set(table_map(cfg).values())
        # Captura cualquier FQN de tres partes que empiece por el catalogo del
        # entorno; `system.*` y otros catalogos no aplican aqui.
        patron = re.compile(rf"\b{re.escape(cfg.catalog)}\.[a-z0-9_]+\.[a-z0-9_]+\b")
        desconocidas: set[str] = set()
        for archivo in sorted((DASHBOARDS_DIR / env).glob("*.lvdash.json")):
            for fqn in patron.findall(archivo.read_text(encoding="utf-8")):
                if fqn not in conocidas:
                    desconocidas.add(f"{archivo.name}:{fqn}")
        assert desconocidas == set(), f"tablas fuera del registro: {sorted(desconocidas)}"

    @pytest.mark.parametrize("env", ENTORNOS)
    def test_cada_entorno_usa_solo_su_catalogo(self, env):
        """Un dashboard de qa no puede consultar el catalogo de prd."""
        cfg = _config(env)
        ajenos = {e: _config(e).catalog for e in ENTORNOS if e != env}
        for archivo in sorted((DASHBOARDS_DIR / env).glob("*.lvdash.json")):
            texto = archivo.read_text(encoding="utf-8")
            for otro_env, catalogo in ajenos.items():
                assert f"{catalogo}." not in texto, (
                    f"{env}/{archivo.name} referencia el catalogo de {otro_env} ({catalogo})"
                )
            assert f"{cfg.catalog}." in texto, f"{env}/{archivo.name} no referencia {cfg.catalog}"

    def test_no_quedan_dashboards_sin_entorno(self):
        """El layout viejo (dashboards/*.lvdash.json en la raiz) ya no aplica."""
        sueltos = sorted(DASHBOARDS_DIR.glob("*.lvdash.json"))
        assert sueltos == [], f"dashboards sin entorno: {[p.name for p in sueltos]}"

    def test_los_versionados_coinciden_con_el_generador(self):
        """Falla si alguien edito un JSON a mano sin regenerar."""
        sys.path.insert(0, str(REPO_ROOT / "scripts"))
        import dashboards as generador

        for env in ENTORNOS:
            for nombre, contenido in generador.render_env(env).items():
                archivo = DASHBOARDS_DIR / env / nombre
                assert archivo.read_text(encoding="utf-8") == contenido, (
                    f"dashboards/{env}/{nombre} difiere del generador. "
                    "Ejecuta: python scripts/dashboards.py generate"
                )
