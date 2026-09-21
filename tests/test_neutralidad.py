"""El repositorio distribuye PRODUCTO, no instalaciones.

Nada de lo que viaja aqui puede identificar a un cliente concreto: ni su
workspace, ni sus unidades de negocio, ni sus montos de presupuesto, ni los
correos de su gente. Todo eso vive en `conf/local.yml` y `conf/budgets.local.yml`,
que git ignora.

Estas pruebas son el guardarrail: la fuga anterior entro sola, editando
`conf/base.yml` durante una implantacion, y nada la detuvo hasta la revision
manual del zip.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]

#: Directorios y archivos versionados donde podria colarse configuracion de un
#: cliente. `dashboards/` entra porque su SQL se edita en la UI del workspace y
#: se exporta de vuelta al repositorio.
VERSIONADOS = [
    *sorted(
        c
        for c in (REPO_ROOT / "conf").glob("*.yml")
        # `local.yml` y `*.local.yml` son de la instalacion y git los ignora:
        # si se revisaran aqui, tener un cliente configurado al lado pondria la
        # suite en rojo por hacer exactamente lo que se espera.
        if c.name != "local.yml" and not c.name.endswith(".local.yml")
    ),
    *sorted((REPO_ROOT / "dashboards").glob("*.lvdash.json")),
    *sorted((REPO_ROOT / "resources").glob("*.yml")),
    REPO_ROOT / "databricks.yml",
    REPO_ROOT / "README.md",
]

#: Dominios admitidos en un correo de ejemplo. Cualquier otro es el de alguien real.
DOMINIOS_DE_EJEMPLO = {"example.com", "example.org", "example.net", "tuempresa.com"}

#: URL de un workspace de Azure Databricks: adb-<workspace_id>.<numero>.azuredatabricks.net
HOST_WORKSPACE = re.compile(r"adb-\d{10,}\.\d+\.azuredatabricks\.net", re.I)

#: Correo electronico, suficientemente laxo para lo que aparece en YAML y JSON.
CORREO = re.compile(r"[\w.+-]+@[\w-]+\.[\w.-]+")


def _archivos_existentes() -> list[Path]:
    return [p for p in VERSIONADOS if p.is_file()]


def _texto(p: Path) -> str:
    return p.read_text(encoding="utf-8")


class TestSinRastrosDeCliente:
    @pytest.mark.parametrize("ruta", _archivos_existentes(), ids=lambda p: p.name)
    def test_ningun_host_de_workspace_escrito(self, ruta: Path):
        """El workspace destino sale del perfil del CLI, nunca del repositorio."""
        hallazgos = HOST_WORKSPACE.findall(_texto(ruta))
        assert hallazgos == [], (
            f"{ruta.relative_to(REPO_ROOT)} nombra workspaces concretos: {sorted(set(hallazgos))}. "
            "El host va en el perfil del CLI (databricks auth login -p ...), no aqui."
        )

    @pytest.mark.parametrize("ruta", _archivos_existentes(), ids=lambda p: p.name)
    def test_todo_correo_es_de_ejemplo(self, ruta: Path):
        """Un correo real delata una implantacion y ademas recibe sus alertas."""
        texto = _texto(ruta)
        reales = {
            m.group(0)
            for m in CORREO.finditer(texto)
            if m.group(0).rsplit("@", 1)[1].lower() not in DOMINIOS_DE_EJEMPLO
            # `abfss://cuenta@almacen.dfs.core.windows.net/...` tiene la forma de
            # un correo y no lo es.
            and texto[max(0, m.start() - 3):m.start()] != "://"
        }
        assert reales == set(), (
            f"{ruta.relative_to(REPO_ROOT)} trae correos reales: {sorted(reales)}. "
            f"Usa un dominio de ejemplo ({', '.join(sorted(DOMINIOS_DE_EJEMPLO))}) y "
            "pon el real en conf/local.yml."
        )


class TestBaseNeutra:
    """`conf/base.yml` y `conf/budgets.yml` son el punto de partida de CUALQUIER
    instalacion: lo especifico de una sola no puede vivir ahi."""

    @pytest.fixture(scope="class")
    def base(self) -> dict:
        return yaml.safe_load(_texto(REPO_ROOT / "conf" / "base.yml"))

    def test_sin_workspaces_mapeados(self, base: dict):
        defaults = base["tagging"].get("workspace_defaults") or {}
        assert defaults == {}, (
            f"conf/base.yml mapea workspaces concretos: {sorted(defaults)}. "
            "Ese mapa va en conf/local.yml (plantilla: conf/local.example.yml)."
        )

    def test_sin_unidades_de_negocio(self, base: dict):
        centros = (base["tagging"].get("value_map") or {}).get("cost_center") or {}
        assert centros == {}, (
            f"conf/base.yml normaliza unidades de negocio de un cliente: {sorted(centros)}. "
            "Ese mapa va en conf/local.yml."
        )

    def test_los_presupuestos_versionados_son_ejemplos(self):
        """No son las cifras de nadie: las reales van en conf/budgets.local.yml."""
        presupuestos = yaml.safe_load(_texto(REPO_ROOT / "conf" / "budgets.yml"))["budgets"]
        assert presupuestos, "conf/budgets.yml debe traer ejemplos que documenten el formato"
        for presupuesto in presupuestos:
            dominio = presupuesto["owner_email"].rsplit("@", 1)[1].lower()
            assert dominio in DOMINIOS_DE_EJEMPLO, (
                f"presupuesto '{presupuesto['id']}' apunta a {presupuesto['owner_email']}"
            )


class TestLaConfiguracionLocalLlegaAlWorkspace:
    """El overlay de la instalacion esta ignorado por git, y el CLI de Databricks
    excluye del bundle lo que git ignora. Si nadie lo fuerza de vuelta, el
    pipeline corre en el workspace con la configuracion neutra del repositorio:
    sin presupuestos y con todo el costo en SIN_ASIGNAR, sin ningun error."""

    @pytest.fixture(scope="class")
    def bundle(self) -> dict:
        return yaml.safe_load(_texto(REPO_ROOT / "databricks.yml"))

    @pytest.mark.parametrize("archivo", ["conf/local.yml", "conf/budgets.local.yml"])
    def test_esta_ignorado_por_git(self, archivo: str):
        gitignore = _texto(REPO_ROOT / ".gitignore")
        patrones = {
            linea.strip()
            for linea in gitignore.splitlines()
            if linea.strip() and not linea.startswith("#")
        }
        cubierto = archivo in patrones or "conf/*.local.yml" in patrones
        assert cubierto, f"{archivo} no esta cubierto por .gitignore"

    @pytest.mark.parametrize("archivo", ["conf/local.yml", "conf/budgets.local.yml"])
    def test_el_bundle_lo_sincroniza(self, bundle: dict, archivo: str):
        incluidos = (bundle.get("sync") or {}).get("include") or []
        assert archivo in incluidos, (
            f"{archivo} no esta en sync.include de databricks.yml. Sin el, el bundle "
            "no lo sube al workspace porque git lo ignora."
        )

    def test_los_dashboards_se_despliegan_desde_el_render(self, bundle: dict):
        """Apuntar a `dashboards/` desplegaria el catalogo neutro sin fallar."""
        recursos = yaml.safe_load(_texto(REPO_ROOT / "resources" / "dashboards.yml"))
        rutas = [
            d["file_path"] for d in recursos["resources"]["dashboards"].values()
        ]
        assert rutas, "resources/dashboards.yml no declara ningun dashboard"
        for ruta in rutas:
            assert ruta.startswith("../build/dashboards/"), (
                f"{ruta} apunta a los JSON versionados, resueltos contra el catalogo "
                "neutro. Debe salir de `python scripts/dashboards.py render`."
            )
        incluidos = (bundle.get("sync") or {}).get("include") or []
        assert "build/dashboards/*.lvdash.json" in incluidos, (
            "build/ esta en .gitignore: sin esta entrada en sync.include el deploy "
            "falla con 'no such file or directory' sobre el primer dashboard."
        )

    def test_hay_plantilla_para_cada_archivo_local(self):
        for plantilla in ("conf/local.example.yml", "conf/budgets.local.example.yml"):
            assert (REPO_ROOT / plantilla).is_file(), f"falta la plantilla {plantilla}"
