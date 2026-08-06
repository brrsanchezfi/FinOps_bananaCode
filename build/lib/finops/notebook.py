"""Puente entre los notebooks de Databricks y el paquete `finops`.

Los notebooks del repositorio son orquestadores delgados: leen parametros,
llaman a `bootstrap()` y delegan en `finops.pipeline`. Toda la logica vive en
modulos importables y probados, de modo que un notebook nunca contiene reglas
de negocio.

Uso tipico dentro de un notebook:

    from finops.notebook import bootstrap, resumen
    ctx = bootstrap()
    resultado = ctx.run(["bronze", "silver", "gold"])
    resumen(resultado)
"""

from __future__ import annotations

import contextlib
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .config import FinOpsConfig, load_config
from .logging_utils import configure_logging, get_logger

log = get_logger("notebook")

#: Parametros que el notebook expone como widgets.
WIDGETS: tuple[tuple[str, str, str], ...] = (
    ("env", "dev", "Entorno (dev|qa|prd)"),
    ("stages", "", "Etapas separadas por coma (vacio = todas)"),
    ("run_date", "", "Fecha logica YYYY-MM-DD (vacio = hoy)"),
    ("full_refresh", "false", "Reprocesar ventana historica completa"),
    ("dry_run", "false", "Calcular sin escribir"),
    ("overrides", "", "Overrides ruta.clave=valor separados por ; "),
)


def get_dbutils(spark: Any = None) -> Any:
    """Obtiene `dbutils` desde cualquier contexto de ejecucion de Databricks."""
    try:  # notebook interactivo
        import IPython

        shell = IPython.get_ipython()
        if shell is not None and "dbutils" in shell.user_ns:
            return shell.user_ns["dbutils"]
    except Exception:  # noqa: BLE001
        pass
    try:  # job / cluster
        from pyspark.dbutils import DBUtils  # type: ignore[import-not-found]

        from .spark_utils import get_spark

        return DBUtils(spark or get_spark())
    except Exception as exc:  # noqa: BLE001
        log.warning("dbutils no esta disponible en este contexto: %s", exc)
        return None


def ensure_src_on_path() -> None:
    """Agrega `src/` al sys.path cuando el wheel no esta instalado.

    Permite ejecutar los notebooks directamente desde el workspace sincronizado
    por el bundle, sin tener que construir e instalar la libreria primero.
    """
    aqui = Path(__file__).resolve()
    for candidato in (aqui.parents[1], *(p / "src" for p in aqui.parents[:5])):
        if (candidato / "finops" / "__init__.py").exists() and str(candidato) not in sys.path:
            sys.path.insert(0, str(candidato))
            return


def read_params(dbutils: Any) -> dict[str, str]:
    """Declara los widgets y devuelve sus valores actuales."""
    if dbutils is None:
        return {nombre: defecto for nombre, defecto, _ in WIDGETS}
    valores: dict[str, str] = {}
    for nombre, defecto, etiqueta in WIDGETS:
        with contextlib.suppress(Exception):  # el widget ya existe
            dbutils.widgets.text(nombre, defecto, etiqueta)
        try:
            valores[nombre] = dbutils.widgets.get(nombre)
        except Exception:  # noqa: BLE001
            valores[nombre] = defecto
    return valores


def parse_overrides(texto: str) -> dict[str, str]:
    """Convierte 'a.b=1;c.d=2' en {'a.b': '1', 'c.d': '2'}."""
    salida: dict[str, str] = {}
    for parte in (texto or "").split(";"):
        parte = parte.strip()
        if not parte or "=" not in parte:
            continue
        clave, valor = parte.split("=", 1)
        salida[clave.strip()] = valor.strip()
    return salida


@dataclass
class NotebookContext:
    """Contexto listo para ejecutar el pipeline desde un notebook."""

    cfg: FinOpsConfig
    spark: Any
    dbutils: Any = None
    params: dict[str, str] = field(default_factory=dict)

    @property
    def secret_resolver(self):
        """Resolvedor de secretos para los canales de alerta (o None)."""
        if self.dbutils is None:
            return None
        return lambda scope, key: self.dbutils.secrets.get(scope=scope, key=key)

    @property
    def workspace_url(self) -> str:
        try:
            return "https://" + self.spark.conf.get("spark.databricks.workspaceUrl", "")
        except Exception:  # noqa: BLE001
            return ""

    def stages(self, default: list[str] | None = None) -> list[str]:
        """Etapas solicitadas por parametro, o el valor por defecto."""
        from .pipeline import ALL_STAGES

        crudo = (self.params.get("stages") or "").strip()
        if crudo:
            return [s.strip() for s in crudo.split(",") if s.strip()]
        return list(default or ALL_STAGES)

    def run(self, stages: list[str] | None = None, *, dashboard_url: str = "", **kwargs):
        """Ejecuta el pipeline con el contexto del notebook."""
        from .pipeline import run as run_pipeline

        return run_pipeline(
            self.cfg,
            spark=self.spark,
            stages=stages or self.stages(),
            secret_resolver=self.secret_resolver,
            dashboard_url=dashboard_url,
            **kwargs,
        )


def bootstrap(default_env: str = "dev", conf_dir: str | Path | None = None) -> NotebookContext:
    """Prepara sesion, parametros y configuracion. Punto de entrada del notebook."""
    ensure_src_on_path()
    from .spark_utils import configure_session, get_spark

    spark = get_spark("finops")
    dbutils = get_dbutils(spark)
    params = read_params(dbutils)

    overrides = parse_overrides(params.get("overrides", ""))
    if params.get("full_refresh", "").lower() in {"true", "1", "yes"}:
        overrides["ingestion.full_refresh"] = "true"
    if params.get("dry_run", "").lower() in {"true", "1", "yes"}:
        overrides["runtime.dry_run"] = "true"

    cfg = load_config(
        params.get("env") or default_env,
        conf_dir=conf_dir,
        param_overrides=overrides,
        run_date=params.get("run_date") or None,
    )
    configure_logging(str(cfg.get("runtime.log_level", "INFO")))
    configure_session(spark, cfg.get("runtime.shuffle_partitions", "auto"))

    log.info("Notebook listo | %s", cfg.describe())
    return NotebookContext(cfg=cfg, spark=spark, dbutils=dbutils, params=params)


def resumen(result: Any, *, exit_on_failure: bool = True) -> str:
    """Imprime el resumen de la corrida y falla la tarea si hubo errores."""
    texto = result.summary()
    print(texto)
    if exit_on_failure and not result.ok:
        etapas = ", ".join(m.stage for m in result.recorder.failed)
        raise RuntimeError(f"El pipeline fallo en: {etapas}")
    return texto
