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
        assert TABLES_BY_KEY["fct_cost_daily"].fqn(cfg_dev) == "finops.gold.fct_cost_daily"

    def test_table_map_cubre_tablas_y_vistas(self, cfg_dev):
        """Los dashboards referencian vistas ademas de tablas; ambas resuelven."""
        from finops.views import ALL_VIEWS

        mapa = table_map(cfg_dev)
        assert len(mapa) == len(ALL_TABLES) + len(ALL_VIEWS)
        assert all(v.count(".") == 2 for v in mapa.values())

    def test_no_hay_colision_de_claves_entre_tablas_y_vistas(self):
        """Una clave repetida haria que un marcador resolviera al objeto equivocado."""
        from finops.views import ALL_VIEWS

        claves_tabla = {t.key for t in ALL_TABLES}
        claves_vista = {v.key for v in ALL_VIEWS}
        assert not (claves_tabla & claves_vista)


ENTORNOS = ("dev", "qa", "prd")
NOMBRES = ("finops_ejecutivo", "finops_costos", "finops_optimizacion", "finops_etiquetado")


def _config(env: str):
    from finops.config import load_config

    return load_config(env, conf_dir=REPO_ROOT / "conf", use_env_vars=False)


class TestCoherenciaConDashboards:
    """Los dashboards versionados solo pueden consultar tablas del registro.

    Los tres entornos comparten catalogo y schemas, asi que basta UN juego de
    archivos en `dashboards/`. Los nombres de tabla salen de la configuracion via
    marcadores `{{clave}}`; ninguno se escribe a mano.
    """

    @pytest.mark.parametrize("nombre", NOMBRES)
    def test_json_valido(self, nombre):
        ruta = DASHBOARDS_DIR / f"{nombre}.lvdash.json"
        assert ruta.exists(), f"falta {ruta}"
        contenido = json.loads(ruta.read_text(encoding="utf-8"))
        assert contenido.get("datasets"), f"{ruta} no define datasets"
        assert contenido.get("pages"), f"{ruta} no define paginas"

    def test_no_quedan_marcadores_sin_sustituir(self):
        patron = re.compile(r"\{\{[a-z0-9_]+\}\}")
        pendientes = {
            f"{archivo.name}:{m}"
            for archivo in sorted(DASHBOARDS_DIR.glob("*.lvdash.json"))
            for m in patron.findall(archivo.read_text(encoding="utf-8"))
        }
        assert pendientes == set(), f"marcadores sin resolver: {sorted(pendientes)}"

    def test_las_tablas_referenciadas_existen_en_el_registro(self):
        cfg = _config("prd")
        conocidas = set(table_map(cfg).values())
        # Cualquier FQN de tres partes bajo el catalogo del modelo. `system.*` y
        # otros catalogos no aplican aqui.
        patron = re.compile(rf"\b{re.escape(cfg.catalog)}\.[a-z0-9_]+\.[a-z0-9_]+\b")
        desconocidas: set[str] = set()
        for archivo in sorted(DASHBOARDS_DIR.glob("*.lvdash.json")):
            for fqn in patron.findall(archivo.read_text(encoding="utf-8")):
                if fqn not in conocidas:
                    desconocidas.add(f"{archivo.name}:{fqn}")
        assert desconocidas == set(), f"tablas fuera del registro: {sorted(desconocidas)}"

    def test_los_tres_entornos_resuelven_a_las_mismas_tablas(self):
        """Es la condicion que permite versionar un solo juego de dashboards.

        Si algun entorno vuelve a apuntar a otro catalogo o schema, hay que
        generar por entorno otra vez y apuntar `resources/dashboards.yml` a
        `dashboards/${bundle.target}/`.
        """
        resueltos = {env: table_map(_config(env)) for env in ENTORNOS}
        referencia = resueltos[ENTORNOS[0]]
        distintos = {env: m for env, m in resueltos.items() if m != referencia}
        assert distintos == {}, (
            f"estos entornos resuelven a tablas distintas de '{ENTORNOS[0]}': {sorted(distintos)}"
        )

    def test_ningun_catalogo_escrito_a_mano(self):
        """Los nombres deben salir de la configuracion, no estar quemados.

        Un catalogo de un entorno concreto en el JSON delataria que el generador
        dejo de resolver desde `conf/`.
        """
        catalogos_de_entorno = {"finops_dev", "finops_qa", "finops_prd"}
        for archivo in sorted(DASHBOARDS_DIR.glob("*.lvdash.json")):
            texto = archivo.read_text(encoding="utf-8")
            for catalogo in catalogos_de_entorno:
                assert f"{catalogo}." not in texto, (
                    f"{archivo.name} tiene el catalogo '{catalogo}' escrito a mano"
                )

    def test_no_quedan_subdirectorios_por_entorno(self):
        """El layout anterior (dashboards/<env>/) ya no aplica."""
        sobrantes = [d.name for d in DASHBOARDS_DIR.iterdir() if d.is_dir()]
        assert sobrantes == [], f"subdirectorios sobrantes en dashboards/: {sobrantes}"

    def test_los_versionados_coinciden_con_el_generador(self):
        """Falla si alguien edito un JSON a mano sin regenerar."""
        sys.path.insert(0, str(REPO_ROOT / "scripts"))
        import dashboards as generador

        for nombre, contenido in generador.render_env(ENTORNOS[0]).items():
            archivo = DASHBOARDS_DIR / nombre
            assert archivo.read_text(encoding="utf-8") == contenido, (
                f"dashboards/{nombre} difiere del generador. "
                "Ejecuta: python scripts/dashboards.py generate"
            )


class TestSqlDeCreacion:
    """Constructores de DDL: puros, probables sin Spark."""

    def test_catalogo_sin_ubicacion(self):
        from finops.spark_utils import build_create_catalog_sql

        assert build_create_catalog_sql("finops") == "CREATE CATALOG IF NOT EXISTS finops"

    def test_catalogo_con_ubicacion_gestionada(self):
        from finops.spark_utils import build_create_catalog_sql

        sql = build_create_catalog_sql("finops", "abfss://c@a.dfs.core.windows.net/uc")
        assert sql == (
            "CREATE CATALOG IF NOT EXISTS finops "
            "MANAGED LOCATION 'abfss://c@a.dfs.core.windows.net/uc'"
        )

    def test_se_normaliza_la_barra_final(self):
        from finops.spark_utils import build_create_catalog_sql

        assert build_create_catalog_sql("f", "abfss://c@a/uc/").endswith("'abfss://c@a/uc'")

    def test_schema_sin_ubicacion(self):
        from finops.spark_utils import build_create_schema_sql

        assert build_create_schema_sql("finops", "gold") == "CREATE SCHEMA IF NOT EXISTS finops.gold"

    def test_schema_cuelga_del_storage_root(self):
        from finops.spark_utils import build_create_schema_sql

        sql = build_create_schema_sql("finops", "gold", "abfss://c@a/raiz/")
        assert sql.endswith("MANAGED LOCATION 'abfss://c@a/raiz/gold'")


class TestPoliticaDeCreacionDeCatalogo:
    def test_dev_permite_crear(self, conf_dir):
        from finops.config import load_config

        cfg = load_config("dev", conf_dir=conf_dir, use_env_vars=False)
        assert cfg.get("catalog.create_if_missing") is True

    @pytest.mark.parametrize("env", ["qa", "prd"])
    def test_entornos_gobernados_no_crean_catalogo(self, conf_dir, env):
        """En qa/prd crear el catalogo es tarea de un administrador, no del pipeline."""
        from finops.config import load_config

        cfg = load_config(env, conf_dir=conf_dir, use_env_vars=False)
        assert cfg.get("catalog.create_if_missing") is False


class TestWidgetsDeDashboard:
    """Invariantes de forma de los widgets Lakeview.

    El JSON se autoro sin un workspace donde validarlo visualmente, asi que
    estas pruebas fijan lo que ya se detecto que estaba mal.
    """

    def _widgets(self, tipo: str):
        for archivo in sorted(DASHBOARDS_DIR.glob("*.lvdash.json")):
            contenido = json.loads(archivo.read_text(encoding="utf-8"))
            for pagina in contenido["pages"]:
                for elemento in pagina["layout"]:
                    spec = elemento["widget"].get("spec") or {}
                    if spec.get("widgetType") == tipo:
                        yield archivo.name, elemento["widget"]["name"], spec

    def test_el_pie_usa_angle_y_color_no_ejes(self):
        """Un pie no tiene ejes: con `x`/`y` el widget no renderiza."""
        vistos = 0
        for archivo, nombre, spec in self._widgets("pie"):
            encodings = spec["encodings"]
            assert set(encodings) == {"angle", "color"}, f"{archivo}:{nombre} usa {set(encodings)}"
            assert encodings["angle"]["scale"]["type"] == "quantitative"
            assert encodings["color"]["scale"]["type"] == "categorical"
            vistos += 1
        assert vistos > 0, "no se genero ningun pie"

    def test_los_ejes_solo_en_graficos_cartesianos(self):
        for tipo in ("line", "bar", "area"):
            for archivo, nombre, spec in self._widgets(tipo):
                assert {"x", "y"} <= set(spec["encodings"]), f"{archivo}:{nombre} sin ejes"

    # NOTA: aqui hubo dos pruebas que exigian `type`, `displayAs` y
    # `alignContent` en cada columna de tabla, y alineacion a la derecha en los
    # importes. Ambas afirmaban una suposicion, no una forma observada: esos
    # metadatos son del formato de tabla version 1 y son exactamente lo que
    # dejaba el widget en "Visualization has no fields selected". Las reemplazan
    # `test_las_tablas_usan_la_version_2` y
    # `test_las_columnas_de_tabla_solo_llevan_fieldName`.

    def test_cada_widget_consulta_un_dataset_declarado(self):
        for archivo in sorted(DASHBOARDS_DIR.glob("*.lvdash.json")):
            contenido = json.loads(archivo.read_text(encoding="utf-8"))
            declarados = {d["name"] for d in contenido["datasets"]}
            for pagina in contenido["pages"]:
                for elemento in pagina["layout"]:
                    for consulta in elemento["widget"].get("queries", []):
                        usado = consulta["query"]["datasetName"]
                        assert usado in declarados, (
                            f"{archivo.name}: el widget '{elemento['widget']['name']}' "
                            f"usa el dataset '{usado}', que no esta declarado"
                        )

    def test_los_campos_del_widget_existen_en_su_encoding(self):
        """Todo fieldName referenciado debe estar en los campos de la consulta."""
        for archivo, nombre, spec in self._widgets("table"):
            widget = next(
                w["widget"]
                for f in [DASHBOARDS_DIR / archivo]
                for p in json.loads(f.read_text(encoding="utf-8"))["pages"]
                for w in p["layout"]
                if w["widget"]["name"] == nombre
            )
            disponibles = {c["name"] for c in widget["queries"][0]["query"]["fields"]}
            for columna in spec["encodings"]["columns"]:
                assert columna["fieldName"] in disponibles, (
                    f"{archivo}:{nombre}: la columna '{columna['fieldName']}' no se consulta"
                )

    def test_toda_consulta_agrega_o_esta_desagregada(self):
        """Regresion: un campo sin agregar con `disaggregated=false` da "NO DATA".

        Con `disaggregated=false` Lakeview construye una consulta agrupada y
        espera expresiones de agregacion. Una columna suelta ahi no devuelve
        filas, y el widget aparece vacio sin mensaje de error.
        """
        for archivo in sorted(DASHBOARDS_DIR.glob("*.lvdash.json")):
            contenido = json.loads(archivo.read_text(encoding="utf-8"))
            for pagina in contenido["pages"]:
                for elemento in pagina["layout"]:
                    for consulta in elemento["widget"].get("queries", []):
                        query = consulta["query"]
                        if query["disaggregated"]:
                            continue
                        agregados = [c for c in query["fields"] if "(" in c["expression"]]
                        assert agregados, (
                            f"{archivo.name}: el widget '{elemento['widget']['name']}' no agrega "
                            "ningun campo y no esta desagregado; devolvera NO DATA"
                        )

    # NOTA: aqui hubo una prueba que exigia `disaggregated=True` en los
    # contadores. Era una suposicion mia y resulto equivocada: un contador
    # construido en la UI y verificado en el workspace usa una agregacion con
    # `disaggregated=False`. La reemplaza `test_los_contadores_agregan_su_campo`.

    def test_las_paginas_declaran_tipo_y_version_de_layout(self):
        """Confirmado exportando un dashboard real del workspace.

        Sin `pageType` y `layoutVersion` el dashboard se despliega pero los
        widgets no se enlazan con sus consultas: aparecen con el marcador
        "Select fields to visualize".
        """
        for archivo in sorted(DASHBOARDS_DIR.glob("*.lvdash.json")):
            contenido = json.loads(archivo.read_text(encoding="utf-8"))
            assert "uiSettings" in contenido, f"{archivo.name} sin uiSettings"
            for pagina in contenido["pages"]:
                assert pagina.get("pageType") == "PAGE_TYPE_CANVAS", archivo.name
                assert pagina.get("layoutVersion") == "GRID_V1", archivo.name

    def test_el_layout_usa_la_grilla_de_doce_columnas(self):
        """La grilla de Lakeview tiene 12 columnas, no 6.

        Se dedujo de un export real: contenia un widget en x=7 con width=3,
        imposible en una grilla de 6.
        """
        for archivo in sorted(DASHBOARDS_DIR.glob("*.lvdash.json")):
            contenido = json.loads(archivo.read_text(encoding="utf-8"))
            for pagina in contenido["pages"]:
                for elemento in pagina["layout"]:
                    pos = elemento["position"]
                    fin = pos["x"] + pos["width"]
                    assert fin <= 12, (
                        f"{archivo.name}:{elemento['widget']['name']} termina en {fin}, "
                        "fuera de la grilla de 12"
                    )
                # Al menos un widget debe aprovechar el ancho completo, o el
                # layout quedaria confinado a la mitad izquierda del tablero.
                anchos = [e["position"]["x"] + e["position"]["width"] for e in pagina["layout"]]
                assert max(anchos) == 12, f"{archivo.name} no ocupa el ancho completo"

    def test_los_widgets_no_se_solapan(self):
        for archivo in sorted(DASHBOARDS_DIR.glob("*.lvdash.json")):
            contenido = json.loads(archivo.read_text(encoding="utf-8"))
            for pagina in contenido["pages"]:
                ocupadas: dict[tuple[int, int], str] = {}
                for elemento in pagina["layout"]:
                    pos = elemento["position"]
                    nombre = elemento["widget"]["name"]
                    for fila in range(pos["y"], pos["y"] + pos["height"]):
                        for col in range(pos["x"], pos["x"] + pos["width"]):
                            previo = ocupadas.get((fila, col))
                            assert previo is None, (
                                f"{archivo.name}: '{nombre}' se solapa con '{previo}' "
                                f"en (fila {fila}, columna {col})"
                            )
                            ocupadas[(fila, col)] = nombre

    def test_todo_widget_de_datos_declara_su_consulta(self):
        """`spec.data.queryName` es lo que ata el widget a su consulta.

        Sin esa clave Lakeview no sabe de donde salen los campos y muestra
        "Select fields to visualize", aunque los encodings sean validos.
        Verificado contra un widget reparado en la UI del workspace.
        """
        for archivo in sorted(DASHBOARDS_DIR.glob("*.lvdash.json")):
            contenido = json.loads(archivo.read_text(encoding="utf-8"))
            for pagina in contenido["pages"]:
                for elemento in pagina["layout"]:
                    widget = elemento["widget"]
                    if "queries" not in widget:
                        continue  # widget de texto
                    nombres = {q["name"] for q in widget["queries"]}
                    declarada = widget["spec"].get("data", {}).get("queryName")
                    assert declarada, (
                        f"{archivo.name}: '{widget['name']}' no declara spec.data.queryName"
                    )
                    assert declarada in nombres, (
                        f"{archivo.name}: '{widget['name']}' apunta a la consulta "
                        f"'{declarada}', que no existe en el widget"
                    )

    def test_los_widgets_de_texto_usan_multiline_textbox(self):
        """`textbox_spec` no existe en el esquema: el widget queda en blanco."""
        vistos = 0
        for archivo in sorted(DASHBOARDS_DIR.glob("*.lvdash.json")):
            contenido = json.loads(archivo.read_text(encoding="utf-8"))
            for pagina in contenido["pages"]:
                for elemento in pagina["layout"]:
                    widget = elemento["widget"]
                    if "queries" in widget:
                        continue
                    assert "textbox_spec" not in widget, f"{archivo.name}:{widget['name']}"
                    lineas = widget.get("multilineTextboxSpec", {}).get("lines")
                    assert lineas, f"{archivo.name}:{widget['name']} sin lineas de texto"
                    vistos += 1
        assert vistos > 0

    def test_los_contadores_agregan_su_campo(self):
        """Forma verificada en el workspace: agregacion + consulta agrupada."""
        for archivo, nombre, spec in self._widgets("counter"):
            widget = next(
                w["widget"]
                for f in [DASHBOARDS_DIR / archivo]
                for p in json.loads(f.read_text(encoding="utf-8"))["pages"]
                for w in p["layout"]
                if w["widget"]["name"] == nombre
            )
            query = widget["queries"][0]["query"]
            assert query["disaggregated"] is False, f"{archivo}:{nombre}"
            campo = query["fields"][0]
            assert "(" in campo["expression"], f"{archivo}:{nombre} no agrega el campo"
            assert spec["encodings"]["value"]["fieldName"] == campo["name"]

    def test_las_tablas_usan_la_version_2(self):
        """Regresion: con `version: 1` la tabla muestra "no fields selected".

        Forma verificada contra un widget reparado en la UI del workspace.
        """
        vistos = 0
        for archivo, nombre, spec in self._widgets("table"):
            assert spec["version"] == 2, (
                f"{archivo}:{nombre} usa version {spec['version']}; "
                "el formato de tabla vigente es la 2"
            )
            vistos += 1
        assert vistos > 0, "ninguna tabla revisada"

    def test_las_columnas_de_tabla_solo_llevan_fieldName(self):
        """Regresion: los metadatos por columna son del formato version 1.

        `type`, `displayAs`, `booleanValues`, `alignContent` y `order` invalidan
        la lista completa de columnas en la version 2, y el widget queda vacio.
        """
        for archivo, nombre, spec in self._widgets("table"):
            columnas = spec["encodings"]["columns"]
            assert columnas, f"{archivo}:{nombre} sin columnas"
            for columna in columnas:
                assert set(columna) == {"fieldName"}, (
                    f"{archivo}:{nombre} columna '{columna.get('fieldName')}' "
                    f"lleva claves de mas: {sorted(set(columna) - {'fieldName'})}"
                )

    def test_las_columnas_de_tabla_coinciden_con_los_campos(self):
        """Una columna que no exista entre los campos de la consulta sale vacia."""
        for archivo in DASHBOARDS_DIR.glob("*.lvdash.json"):
            contenido = json.loads(archivo.read_text(encoding="utf-8"))
            for pagina in contenido["pages"]:
                for elemento in pagina["layout"]:
                    widget = elemento["widget"]
                    spec = widget.get("spec", {})
                    if spec.get("widgetType") != "table":
                        continue
                    campos = [f["name"] for f in widget["queries"][0]["query"]["fields"]]
                    columnas = [c["fieldName"] for c in spec["encodings"]["columns"]]
                    assert columnas == campos, f"{archivo.name}:{widget['name']}"
