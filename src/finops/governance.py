"""Puente entre la configuracion de FinOps y el gobierno de tablas de DKOps.

DKOps gobierna las tablas: contratos JSON versionados, validacion antes de
escribir y `SafeMigrator` para la evolucion de esquema. Lo que NO gobierna aqui
es la configuracion: la fuente de verdad sigue siendo `conf/*.yml`, con su capa
por instalacion (`conf/local.yml`). Este modulo traduce de una a la otra.

La alternativa era dejar que DKOps leyera su propio `config.json`, y eso pone
dos sistemas de configuracion en el mismo repositorio: uno diciendo que catalogo
usar para las tablas y otro para todo lo demas. Dos fuentes de verdad sobre el
mismo dato terminan divergiendo, y la que identifica la instalacion del cliente
es la de FinOps.

Alcance actual: **solo bronze**. Silver y gold siguen con `catalog.py` y
`spark_utils.py`. Bronze es la capa mas simple -- proyecciones directas de las
system tables -- y a la vez donde el riesgo de deriva de esquema es mayor, que
es justo lo que los contratos atacan.

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

from pathlib import Path
from typing import TYPE_CHECKING, Any

from .config import FinOpsConfig
from .errors import ConfigError
from .logging_utils import get_logger

if TYPE_CHECKING:  # pragma: no cover
    from DKOps.table_governance.contracts.loader import TableContract

log = get_logger("governance")

#: Los contratos viven en el repositorio, junto al codigo que los usa.
CONTRACTS_DIR = Path(__file__).resolve().parents[2] / "contracts" / "tables"

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
