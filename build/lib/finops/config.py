"""Carga, fusion y validacion de la configuracion FinOps.

Precedencia (de menor a mayor):
    1. conf/base.yml
    2. conf/<env>.yml
    3. overrides explicitos (parametros de job / widgets del notebook)
    4. variables de entorno con prefijo FINOPS__ (doble guion bajo = nivel)
       ej: FINOPS__CATALOG__CATALOG=finops_sandbox

La configuracion se expone como un objeto `FinOpsConfig` con acceso por ruta
punteada (`cfg.get("anomaly.window_days")`) y helpers para nombres de tabla.
"""

from __future__ import annotations

import copy
import json
import os
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import yaml

from .errors import ConfigError

VALID_ENVIRONMENTS = ("dev", "qa", "prd")
_ENV_PREFIX = "FINOPS__"

# Capas del modelo y su clave de schema en la configuracion.
_LAYER_SCHEMA_KEY = {
    "bronze": "catalog.bronze_schema",
    "silver": "catalog.silver_schema",
    "gold": "catalog.gold_schema",
}


# ---------------------------------------------------------------------------
# Utilidades puras
# ---------------------------------------------------------------------------
def deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    """Fusiona `override` sobre `base` recursivamente, sin mutar los originales.

    Las listas se reemplazan completas (no se concatenan): asi un entorno puede
    redefinir la lista de canales de alerta sin heredar los de base.
    """
    result = copy.deepcopy(base)
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = deep_merge(result[key], value)
        else:
            result[key] = copy.deepcopy(value)
    return result


def set_by_path(target: dict[str, Any], path: str, value: Any) -> None:
    """Asigna `value` en `target` siguiendo una ruta punteada, creando niveles."""
    partes = [p for p in path.split(".") if p]
    if not partes:
        raise ConfigError("Ruta de configuracion vacia")
    nodo = target
    for parte in partes[:-1]:
        siguiente = nodo.get(parte)
        if not isinstance(siguiente, dict):
            siguiente = {}
            nodo[parte] = siguiente
        nodo = siguiente
    nodo[partes[-1]] = value


def get_by_path(source: dict[str, Any], path: str, default: Any = None) -> Any:
    """Lee una ruta punteada devolviendo `default` si algun nivel no existe."""
    nodo: Any = source
    for parte in path.split("."):
        if not isinstance(nodo, dict) or parte not in nodo:
            return default
        nodo = nodo[parte]
    return nodo


def coerce_scalar(raw: str) -> Any:
    """Convierte un string de variable de entorno al tipo Python mas probable."""
    texto = raw.strip()
    bajo = texto.lower()
    if bajo in {"true", "yes", "on"}:
        return True
    if bajo in {"false", "no", "off"}:
        return False
    if bajo in {"null", "none", ""}:
        return None
    if texto.startswith(("[", "{")):
        try:
            return json.loads(texto)
        except json.JSONDecodeError:
            return texto
    try:
        return int(texto)
    except ValueError:
        pass
    try:
        return float(texto)
    except ValueError:
        pass
    return texto


def overrides_from_env(environ: dict[str, str] | None = None) -> dict[str, Any]:
    """Construye un dict de overrides desde variables FINOPS__A__B=valor."""
    environ = environ if environ is not None else dict(os.environ)
    salida: dict[str, Any] = {}
    for clave, valor in environ.items():
        if not clave.startswith(_ENV_PREFIX):
            continue
        ruta = clave[len(_ENV_PREFIX) :].lower().replace("__", ".")
        if not ruta:
            continue
        set_by_path(salida, ruta, coerce_scalar(valor))
    return salida


def flatten_overrides(params: dict[str, Any]) -> dict[str, Any]:
    """Convierte parametros planos con rutas punteadas en un dict anidado.

    Los parametros de job de Databricks llegan como strings planos:
        {"anomaly.window_days": "14", "ingestion.full_refresh": "true"}
    """
    salida: dict[str, Any] = {}
    for clave, valor in params.items():
        if valor is None:
            continue
        if "." not in clave:
            continue
        set_by_path(salida, clave, coerce_scalar(valor) if isinstance(valor, str) else valor)
    return salida


# ---------------------------------------------------------------------------
# Objeto de configuracion
# ---------------------------------------------------------------------------
@dataclass
class FinOpsConfig:
    """Configuracion efectiva de una corrida."""

    env: str
    data: dict[str, Any] = field(default_factory=dict)
    budgets: dict[str, Any] = field(default_factory=dict)
    run_date: date = field(default_factory=date.today)
    conf_dir: Path | None = None

    # -- acceso generico ----------------------------------------------------
    def get(self, path: str, default: Any = None) -> Any:
        return get_by_path(self.data, path, default)

    def require(self, path: str) -> Any:
        valor = self.get(path, _MISSING)
        if valor is _MISSING:
            raise ConfigError(f"Falta la clave de configuracion obligatoria '{path}'")
        return valor

    def __getitem__(self, path: str) -> Any:
        return self.require(path)

    # -- nombres de objetos -------------------------------------------------
    @property
    def catalog(self) -> str:
        return str(self.require("catalog.catalog"))

    def schema(self, layer: str) -> str:
        clave = _LAYER_SCHEMA_KEY.get(layer)
        if clave is None:
            raise ConfigError(f"Capa desconocida '{layer}'. Validas: {sorted(_LAYER_SCHEMA_KEY)}")
        return str(self.require(clave))

    def schema_fqn(self, layer: str) -> str:
        return f"{self.catalog}.{self.schema(layer)}"

    def table(self, layer: str, name: str) -> str:
        """Nombre completamente calificado de una tabla del modelo."""
        return f"{self.schema_fqn(layer)}.{name}"

    def source(self, key: str) -> dict[str, Any]:
        """Definicion de una tabla de sistema origen."""
        definicion = self.get(f"sources.{key}")
        if not isinstance(definicion, dict) or "table" not in definicion:
            raise ConfigError(f"Fuente '{key}' no definida o sin clave 'table' en sources")
        return definicion

    @property
    def source_keys(self) -> list[str]:
        fuentes = self.get("sources", {}) or {}
        return sorted(fuentes.keys())

    # -- ventanas de tiempo -------------------------------------------------
    @property
    def max_date(self) -> date:
        raw = self.get("ingestion.max_usage_date")
        if raw:
            return _parse_date(raw)
        return self.run_date

    @property
    def lookback_days(self) -> int:
        if bool(self.get("ingestion.full_refresh", False)):
            return int(self.get("ingestion.initial_load_days", 400))
        return int(self.get("ingestion.lookback_days", 7))

    @property
    def min_date(self) -> date:
        return self.max_date - timedelta(days=max(self.lookback_days, 0))

    @property
    def dry_run(self) -> bool:
        return bool(self.get("runtime.dry_run", False))

    # -- serializacion ------------------------------------------------------
    def to_dict(self) -> dict[str, Any]:
        return copy.deepcopy(self.data)

    def redacted(self) -> dict[str, Any]:
        """Copia apta para logging: no incluye valores de secretos resueltos."""
        copia = self.to_dict()
        for canal in copia.get("alerting", {}).get("channels", []) or []:
            if isinstance(canal, dict):
                canal.pop("resolved_url", None)
                canal.pop("url", None)
        return copia

    def describe(self) -> str:
        return (
            f"env={self.env} catalogo={self.catalog} "
            f"ventana=[{self.min_date.isoformat()} .. {self.max_date.isoformat()}] "
            f"full_refresh={self.get('ingestion.full_refresh')} dry_run={self.dry_run}"
        )


class _Missing:
    def __repr__(self) -> str:  # pragma: no cover - solo cosmetico
        return "<missing>"


_MISSING = _Missing()


def _parse_date(raw: Any) -> date:
    if isinstance(raw, date) and not isinstance(raw, datetime):
        return raw
    if isinstance(raw, datetime):
        return raw.date()
    texto = str(raw).strip()
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%d-%m-%Y"):
        try:
            return datetime.strptime(texto, fmt).date()
        except ValueError:
            continue
    raise ConfigError(f"Fecha invalida '{raw}'. Formato esperado YYYY-MM-DD")


# ---------------------------------------------------------------------------
# Carga
# ---------------------------------------------------------------------------
def _read_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise ConfigError(f"No existe el archivo de configuracion: {path}")
    with path.open("r", encoding="utf-8") as fh:
        contenido = yaml.safe_load(fh) or {}
    if not isinstance(contenido, dict):
        raise ConfigError(f"El archivo {path} debe contener un mapeo en la raiz")
    return contenido


def default_conf_dir() -> Path:
    """Localiza el directorio conf/ tanto en repo local como en workspace/bundle."""
    candidatos = []
    env_dir = os.environ.get("FINOPS_CONF_DIR")
    if env_dir:
        candidatos.append(Path(env_dir))
    aqui = Path(__file__).resolve()
    # src/finops/config.py -> raiz del repo
    candidatos.append(aqui.parents[2] / "conf")
    # instalado como wheel: buscar hacia arriba desde el cwd
    cwd = Path.cwd().resolve()
    candidatos.extend([cwd / "conf", *[p / "conf" for p in cwd.parents[:4]]])
    for c in candidatos:
        if c.is_dir() and (c / "base.yml").exists():
            return c
    raise ConfigError(
        "No se encontro el directorio 'conf/'. Define FINOPS_CONF_DIR o pasa conf_dir explicitamente."
    )


def load_config(
    env: str,
    *,
    conf_dir: str | Path | None = None,
    overrides: dict[str, Any] | None = None,
    param_overrides: dict[str, Any] | None = None,
    run_date: date | str | None = None,
    use_env_vars: bool = True,
) -> FinOpsConfig:
    """Construye la configuracion efectiva para un entorno.

    Args:
        env: uno de dev | qa | prd.
        conf_dir: directorio con base.yml y <env>.yml. Autodetectado si es None.
        overrides: dict anidado con overrides de mayor precedencia que <env>.yml.
        param_overrides: dict plano con rutas punteadas (parametros de job).
        run_date: fecha logica de la corrida (por defecto hoy).
        use_env_vars: aplica variables FINOPS__*.
    """
    env_norm = str(env).strip().lower()
    if env_norm not in VALID_ENVIRONMENTS:
        raise ConfigError(f"Entorno '{env}' invalido. Validos: {VALID_ENVIRONMENTS}")

    directorio = Path(conf_dir) if conf_dir else default_conf_dir()
    datos = _read_yaml(directorio / "base.yml")
    datos = deep_merge(datos, _read_yaml(directorio / f"{env_norm}.yml"))

    if overrides:
        datos = deep_merge(datos, overrides)
    if param_overrides:
        datos = deep_merge(datos, flatten_overrides(param_overrides))
    if use_env_vars:
        datos = deep_merge(datos, overrides_from_env())

    presupuestos_path = directorio / "budgets.yml"
    presupuestos = _read_yaml(presupuestos_path) if presupuestos_path.exists() else {}

    cfg = FinOpsConfig(
        env=env_norm,
        data=datos,
        budgets=presupuestos,
        run_date=_parse_date(run_date) if run_date else date.today(),
        conf_dir=directorio,
    )
    validate_config(cfg)
    return cfg


# ---------------------------------------------------------------------------
# Validacion
# ---------------------------------------------------------------------------
def validate_config(cfg: FinOpsConfig) -> None:
    """Valida invariantes de la configuracion. Lanza ConfigError si algo no cuadra."""
    errores: list[str] = []

    for clave in ("catalog.catalog", "catalog.bronze_schema", "catalog.silver_schema", "catalog.gold_schema"):
        if not cfg.get(clave):
            errores.append(f"'{clave}' es obligatorio")

    for obligatoria in ("billing_usage", "billing_list_prices"):
        definicion = cfg.get(f"sources.{obligatoria}")
        if not isinstance(definicion, dict) or not definicion.get("table"):
            errores.append(f"sources.{obligatoria}.table es obligatorio")

    lookback = cfg.get("ingestion.lookback_days", 7)
    if not isinstance(lookback, int) or lookback < 0:
        errores.append("ingestion.lookback_days debe ser un entero >= 0")

    for regla in cfg.get("pricing.discounts", []) or []:
        pct = regla.get("discount_pct", 0.0) if isinstance(regla, dict) else None
        if pct is None or not (0.0 <= float(pct) < 1.0):
            errores.append(f"pricing.discounts['{regla}'].discount_pct debe estar en [0, 1)")

    dims = cfg.get("tagging.dimensions", []) or []
    if not dims:
        errores.append("tagging.dimensions no puede estar vacio")
    alias = cfg.get("tagging.aliases", {}) or {}
    for dim in dims:
        if dim not in alias:
            errores.append(f"tagging.aliases no define alias para la dimension '{dim}'")

    metodo = str(cfg.get("anomaly.method", "mad")).lower()
    if metodo not in {"mad", "zscore"}:
        errores.append("anomaly.method debe ser 'mad' o 'zscore'")

    metodo_fc = str(cfg.get("forecast.method", "holt")).lower()
    if metodo_fc not in {"holt", "seasonal_naive", "moving_average"}:
        errores.append("forecast.method debe ser holt | seasonal_naive | moving_average")

    severidades = {"low", "medium", "high", "critical"}
    min_sev = str(cfg.get("alerting.min_severity", "medium")).lower()
    if min_sev not in severidades:
        errores.append(f"alerting.min_severity debe ser uno de {sorted(severidades)}")

    nombres_canal: set[str] = set()
    for canal in cfg.get("alerting.channels", []) or []:
        if not isinstance(canal, dict):
            errores.append("Cada elemento de alerting.channels debe ser un mapeo")
            continue
        nombre = canal.get("name")
        if not nombre:
            errores.append("Cada canal de alerta requiere 'name'")
        elif nombre in nombres_canal:
            errores.append(f"Canal de alerta duplicado '{nombre}'")
        else:
            nombres_canal.add(nombre)
        if canal.get("type") not in {"webhook", "table", "email", "noop"}:
            errores.append(f"Canal '{nombre}': type debe ser webhook | table | email | noop")
        if canal.get("type") == "webhook" and canal.get("enabled") and not canal.get("secret_key"):
            errores.append(f"Canal webhook '{nombre}' habilitado sin secret_key")

    ids_presupuesto: set[str] = set()
    for presupuesto in cfg.budgets.get("budgets", []) or []:
        pid = presupuesto.get("id")
        if not pid:
            errores.append("Cada presupuesto requiere 'id'")
            continue
        if pid in ids_presupuesto:
            errores.append(f"Presupuesto duplicado '{pid}'")
        ids_presupuesto.add(pid)
        if float(presupuesto.get("amount_usd", 0)) <= 0:
            errores.append(f"Presupuesto '{pid}': amount_usd debe ser > 0")
        if presupuesto.get("period") not in {"monthly", "quarterly", "yearly"}:
            errores.append(f"Presupuesto '{pid}': period debe ser monthly | quarterly | yearly")

    if errores:
        raise ConfigError("Configuracion invalida:\n  - " + "\n  - ".join(errores))
