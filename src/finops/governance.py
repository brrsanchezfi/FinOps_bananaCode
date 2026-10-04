"""Puente entre FinOps y DKOps: sesion Spark, logging y escritura de tablas.

DKOps pone la infraestructura del pipeline:

  - el `Launcher` crea la SparkSession (cluster de Databricks, PC local o
    Databricks Connect) y configura el logger;
  - los writers de `table_governance` escriben TODAS las tablas del modelo
    contra su contrato JSON versionado, validando el DataFrame antes de tocar
    la tabla.

Lo que NO gobierna DKOps es la configuracion: la fuente de verdad sigue siendo
`conf/*.yml`, con su capa por instalacion (`conf/local.yml`). Este modulo
traduce de una a la otra y expone las cuatro escrituras que usa el pipeline.

La alternativa era dejar que DKOps leyera su propio `config.json`, y eso pone
dos sistemas de configuracion en el mismo repositorio: uno diciendo que catalogo
usar para las tablas y otro para todo lo demas. Dos fuentes de verdad sobre el
mismo dato terminan divergiendo, y la que identifica la instalacion del cliente
es la de FinOps.

Alcance: las tres capas. Bronze fue la primera (es donde la deriva de esquema
de las system tables mas duele); silver, gold y las tablas de operacion
escriben igual, cada una con su contrato en `finops/contracts/tables/<capa>/`.

Nota sobre el placeholder del schema
------------------------------------
DKOps resuelve `{catalog.<capa>}`, `{path.<nombre>}` y `{env}`. No tiene un
placeholder para SCHEMA, porque asume una capa por catalogo. FinOps pone las
tres capas en UN catalogo separadas por schema (docs/adr/0005), asi que el
nombre del schema viaja por el espacio de `paths` como `{path.schema_bronze}`.
Es un prestamo de nomenclatura, no un descuido: esta aqui para que el nombre del
schema siga saliendo de `conf/*.yml` y no quede quemado en los contratos.
"""

from __future__ import annotations

import json
import os
import tempfile
from datetime import date, datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import TYPE_CHECKING, Any

from .config import FinOpsConfig
from .errors import ConfigError
from .logging_utils import get_logger

if TYPE_CHECKING:  # pragma: no cover
    from DKOps.launcher import Launcher
    from DKOps.table_governance.contracts.loader import TableContract
    from pyspark.sql import DataFrame

    from .catalog import TableDef

log = get_logger("governance")

#: Los contratos viajan dentro del paquete: el wheel que se instala en el
#: cluster los lleva consigo.
CONTRACTS_DIR = Path(__file__).resolve().parent / "contracts" / "tables"

#: Clave sintetica del unico "ambiente" que se le declara a DKOps. No es un
#: workspace_id real: la resolucion se fuerza por nombre (DATABRICKS_TARGET)
#: para que el contrato no dependa de en que workspace corre, que es
#: precisamente lo que FinOps saca del repositorio.
_CLAVE_AMBIENTE = "finops"


def build_environment_dict(cfg: FinOpsConfig) -> dict[str, Any]:
    """Config de ambiente de DKOps derivada de `conf/*.yml`. Funcion pura.

    Las tres capas apuntan al MISMO catalogo a proposito: es el modelo de
    FinOps, donde lo que separa las capas es el schema (ver docs/adr/0005). Si
    algun dia se separan por catalogo, este es el unico sitio que cambia.
    """
    catalogo = cfg.catalog
    return {
        # Resolucion por nombre, no por workspace_id: ver `_CLAVE_AMBIENTE`.
        "DATABRICKS_TARGET": cfg.env,
        "environments": {
            _CLAVE_AMBIENTE: {
                "env": cfg.env,
                "env_short": cfg.env[0],
                "catalogs": dict.fromkeys(("bronze", "silver", "gold"), catalogo),
                "paths": {
                    f"schema_{capa}": cfg.schema(capa) for capa in ("bronze", "silver", "gold")
                },
            }
        },
    }


def build_environment(cfg: FinOpsConfig):
    """`EnvironmentConfig` de DKOps alimentado por la configuracion de FinOps.

    Se construye desde un dict, no desde un `config.json`: DKOps lo permite y es
    lo que mantiene una sola fuente de verdad.
    """
    from DKOps.environment_config import EnvironmentConfig

    return EnvironmentConfig(build_environment_dict(cfg), is_databricks=False)


def contract_path(layer: str, table_name: str, *, base_dir: Path | None = None) -> Path:
    return (base_dir or CONTRACTS_DIR) / layer / f"{table_name}.json"


def load_table_contract(
    cfg: FinOpsConfig, layer: str, table_name: str, *, base_dir: Path | None = None
) -> TableContract:
    """Contrato de una tabla, con catalogo y schema ya resueltos por FinOps."""
    from DKOps.table_governance.contracts.loader import load_contract

    ruta = contract_path(layer, table_name, base_dir=base_dir)
    if not ruta.is_file():
        raise ConfigError(
            f"No existe el contrato de '{table_name}' en {ruta}. "
            "Generalo con: python scripts/contratos.py bootstrap --schemas <archivo>"
        )
    return load_contract(ruta, env=build_environment(cfg))


def load_layer_contracts(
    cfg: FinOpsConfig, layer: str, *, base_dir: Path | None = None
) -> dict[str, TableContract]:
    """Todos los contratos de una capa, indexados por nombre de tabla."""
    directorio = (base_dir or CONTRACTS_DIR) / layer
    if not directorio.is_dir():
        log.warning("No hay contratos para la capa '%s' en %s", layer, directorio)
        return {}
    contratos = {
        ruta.stem: load_table_contract(cfg, layer, ruta.stem, base_dir=base_dir)
        for ruta in sorted(directorio.glob("*.json"))
    }
    log.info("Contratos cargados para '%s': %s", layer, len(contratos))
    return contratos


# ---------------------------------------------------------------------------
# Sesion: el Launcher de DKOps
# ---------------------------------------------------------------------------
#: Respaldo del log cuando el volumen no existe todavia (primera corrida, antes
#: de `setup`) o cuando no se corre en Databricks.
LOG_DIR_FALLBACK = "/tmp/finops/logs"

#: Volumen de Unity Catalog para los logs, en el schema de gold, junto a
#: `ops_run_log`. Lo crea la etapa `setup` (`catalog.bootstrap`).
LOG_VOLUME = "logs"


def log_volume_dir(cfg: FinOpsConfig) -> str:
    return f"/Volumes/{cfg.catalog}/{cfg.schema('gold')}/{LOG_VOLUME}"


def log_dir(cfg: FinOpsConfig) -> str:
    """Directorio del log de DKOps.

    Por defecto un volumen de Unity Catalog, no la carpeta del bundle: esa es
    codigo, y cada `bundle deploy` la sincroniza. El volumen sobrevive al
    cluster y lo gobiernan los permisos de UC. Si su raiz no esta montada
    (volumen aun sin crear, o fuera de Databricks) se usa /tmp.
    """
    configurado = cfg.get("runtime.log_dir")
    if configurado:
        return str(configurado)
    volumen = log_volume_dir(cfg)
    if Path(volumen).is_dir():
        return volumen
    log.warning("El volumen de logs %s no existe todavia; el log va a %s", volumen, LOG_DIR_FALLBACK)
    return LOG_DIR_FALLBACK


def launcher_config(cfg: FinOpsConfig) -> dict[str, Any]:
    """`config.json` del Launcher derivado de `conf/*.yml`. Funcion pura.

    `runtime.launcher` deja pasar claves propias del Launcher (por ejemplo
    `EXECUTION_ENVIRONMENT: databricks` y `CLUSTER_ID` para Databricks Connect)
    sin que este modulo tenga que conocerlas.
    """
    return {
        "EXECUTION_ENVIRONMENT": "local",
        "SPARK_APP_NAME": f"finops-{cfg.env}",
        "LOG_LEVEL": str(cfg.get("runtime.log_level", "INFO")).upper(),
        "LOG_DIR": log_dir(cfg),
        **(cfg.get("runtime.launcher") or {}),
        **build_environment_dict(cfg),
    }


def start_launcher(cfg: FinOpsConfig) -> Launcher:
    """Launcher activo del proceso; lo crea la primera vez. Idempotente.

    En un notebook o job de Databricks el Launcher reutiliza la SparkSession
    del cluster. El `config.json` que exige se escribe en un temporal: la
    configuracion sigue saliendo de `conf/*.yml`.
    """
    from DKOps.launcher import Launcher

    try:
        return Launcher.current()
    except RuntimeError:
        pass
    ruta = Path(tempfile.gettempdir()) / f"finops-launcher-{cfg.env}.json"
    ruta.write_text(json.dumps(launcher_config(cfg)), encoding="utf-8")
    # La variable de entorno le gana al config.json en DKOps: se fija para que
    # un DATABRICKS_TARGET heredado del entorno no resuelva otro ambiente.
    os.environ["DATABRICKS_TARGET"] = cfg.env
    # Un archivo por corrida: los volumenes de UC no admiten reabrir un archivo
    # para agregarle lineas, solo escribirlo de corrido.
    sello = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    return Launcher(str(ruta), log_filename=f"finops-{cfg.env}-{sello}")


# ---------------------------------------------------------------------------
# Escritura: los writers de DKOps contra el contrato de cada tabla
# ---------------------------------------------------------------------------
@lru_cache(maxsize=1)
def _overwrite_writer() -> type:
    """Writer de sobrescritura completa (snapshot) sobre la maquinaria de DKOps.

    `TableWriter.overwrite` de DKOps 0.3.x emite antes un `CREATE OR REPLACE
    TABLE` con los tipos del contrato, y el contrato solo guarda la clase de los
    tipos complejos (`MAP`, `STRUCT`, `ARRAY`): ese DDL no es SQL valido para
    las tablas que los tienen. Este writer hace lo mismo que la carga full pero
    deja que el esquema lo ponga el DataFrame ya validado (`overwriteSchema`).
    Se construye perezosamente porque los writers de DKOps importan pyspark.
    """
    from DKOps.logger_config import log_operation
    from DKOps.table_governance.writers.base_writer import BaseWriter

    class OverwriteWriter(BaseWriter):
        @log_operation("overwrite")
        def write(self, df: DataFrame, **kwargs: Any) -> None:
            self._validate(df)
            df = self._reorder_columns(self._apply_defaults(df))
            if self._dry_run:
                self._log_dry_run("overwrite")
                return
            self._write_df(df, mode="overwrite", overwrite_schema=True)
            self.apply_contract_metadata()

    return OverwriteWriter


def _contract(cfg: FinOpsConfig, tabla: TableDef) -> TableContract:
    return load_table_contract(cfg, tabla.layer, tabla.name)


def _after_write(cfg: FinOpsConfig, contrato: TableContract, df: DataFrame) -> int:
    """Aplica las TBLPROPERTIES de la instalacion y devuelve las filas escritas.

    Las filas salen del historial de Delta y no de un `count()`: los writers de
    DKOps ya cuentan una vez y contar otra recomputaria el DataFrame entero. El
    `count()` queda solo de respaldo, si el historial no esta disponible.
    """
    from DKOps.launcher import Launcher

    from .spark_utils import apply_table_properties

    spark = Launcher.current().spark
    nombre = contrato.effective_name
    apply_table_properties(spark, nombre, cfg.get("catalog.table_properties", {}) or {})
    # Despues de escribir vienen versiones de solo metadata (comentarios,
    # TBLPROPERTIES): la cifra es la de la ultima version que escribio datos.
    try:
        historial = spark.sql(f"DESCRIBE HISTORY {nombre} LIMIT 10").collect()
        metricas = [h["operationMetrics"] or {} for h in historial]
        return next(int(m["numOutputRows"]) for m in metricas if "numOutputRows" in m)
    except Exception:  # noqa: BLE001 - la cifra es informativa
        return df.count()


def _table_exists(contrato: TableContract) -> bool:
    from DKOps.launcher import Launcher

    from .spark_utils import table_exists

    return table_exists(Launcher.current().spark, contrato.effective_name)


def _range_condition(date_column: str, min_date: date, max_date: date) -> str:
    return f"{date_column} >= DATE'{min_date.isoformat()}' AND {date_column} <= DATE'{max_date.isoformat()}'"


def overwrite(cfg: FinOpsConfig, tabla: TableDef, df: DataFrame) -> int:
    """Reemplaza la tabla completa (snapshot)."""
    contrato = _contract(cfg, tabla)
    if cfg.dry_run:
        return df.count()
    _overwrite_writer()(contrato).write(df)
    return _after_write(cfg, contrato, df)


def append(cfg: FinOpsConfig, tabla: TableDef, df: DataFrame) -> int:
    """Agrega filas sin tocar las existentes (bitacoras, alertas, marcas de agua)."""
    from DKOps.table_governance.writers.table_writer import TableWriter

    contrato = _contract(cfg, tabla)
    filas = df.count()
    TableWriter(contrato, dry_run=cfg.dry_run).append(df)
    if not cfg.dry_run:
        _after_write(cfg, contrato, df)
    return filas


def upsert(cfg: FinOpsConfig, tabla: TableDef, df: DataFrame, *, keys: list[str]) -> int:
    """MERGE por clave de negocio. Crea la tabla si no existe."""
    from DKOps.table_governance.writers.table_writer import TableWriter

    contrato = _contract(cfg, tabla)
    filas = df.count()
    TableWriter(contrato, dry_run=cfg.dry_run).upsert(df, keys=keys)
    if not cfg.dry_run:
        _after_write(cfg, contrato, df)
    return filas


def delete_range(cfg: FinOpsConfig, tabla: TableDef, *, date_column: str, min_date: date, max_date: date) -> None:
    """Borra un rango de fechas, si la tabla existe.

    Se usa cuando una corrida re-evalua un rango y no produce filas: lo que se
    habia escrito antes para ese rango debe desaparecer.
    """
    from DKOps.table_governance.writers.table_writer import TableWriter

    contrato = _contract(cfg, tabla)
    if _table_exists(contrato):
        TableWriter(contrato, dry_run=cfg.dry_run).delete(_range_condition(date_column, min_date, max_date))


def replace_range(
    cfg: FinOpsConfig, tabla: TableDef, df: DataFrame, *, date_column: str, min_date: date, max_date: date
) -> int:
    """Escritura incremental idempotente por rango de fechas (ver docs/adr/0002).

    Borra [min_date, max_date] en el destino y agrega el lote nuevo: los
    registros de billing llegan tarde y deben reemplazar por completo lo que ya
    se habia calculado para esos dias.
    """
    contrato = _contract(cfg, tabla)
    if cfg.dry_run:
        return df.count()
    if not _table_exists(contrato):
        return overwrite(cfg, tabla, df)

    from DKOps.table_governance.contracts.validator import SchemaValidator
    from DKOps.table_governance.writers.table_writer import TableWriter

    # Se valida ANTES de borrar: si el lote no cumple el contrato, el rango
    # anterior debe quedar intacto en vez de quedar vacio.
    SchemaValidator(contrato).validate(df).raise_if_critical()
    writer = TableWriter(contrato)
    writer.delete(_range_condition(date_column, min_date, max_date))
    writer.append(df)
    return _after_write(cfg, contrato, df)


def create_if_missing(cfg: FinOpsConfig, tabla: TableDef, schema: Any) -> bool:
    """Crea vacia una tabla con el esquema dado. Devuelve True si la creo.

    Varias tablas de analitica solo reciben filas cuando hay resultados, y hay
    resultados que tardan semanas en aparecer. Sin esto un dashboard falla con
    TABLE_OR_VIEW_NOT_FOUND en vez de mostrar "sin datos".
    """
    from DKOps.launcher import Launcher

    contrato = _contract(cfg, tabla)
    if _table_exists(contrato):
        return False
    if cfg.dry_run:
        log.info("[dry-run] crear tabla vacia %s", contrato.full_name)
        return True
    overwrite(cfg, tabla, Launcher.current().spark.createDataFrame([], schema))
    return True


def apply_metadata(cfg: FinOpsConfig, tablas: Any) -> int:
    """Reaplica comentarios del contrato a las tablas que ya existen."""
    from DKOps.table_governance.writers.table_writer import TableWriter

    aplicadas = 0
    for tabla in tablas:
        contrato = _contract(cfg, tabla)
        if _table_exists(contrato):
            TableWriter(contrato, dry_run=cfg.dry_run).apply_contract_metadata()
            aplicadas += 1
    return aplicadas
