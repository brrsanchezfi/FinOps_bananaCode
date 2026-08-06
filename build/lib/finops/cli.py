"""Interfaz de linea de comandos de la plataforma FinOps.

Uso tipico:

    finops validate --env dev              # valida la configuracion sin Spark
    finops plan --env prd                  # muestra la ventana y las tablas destino
    finops run --env dev --stages bronze,silver,gold
    finops run --env prd --set anomaly.score_threshold=4.0

`validate` y `plan` no requieren Spark ni conexion al workspace, por lo que se
ejecutan en CI para detectar errores de configuracion antes de desplegar.
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any

from .catalog import ALL_TABLES, table_map
from .config import VALID_ENVIRONMENTS, load_config
from .errors import FinOpsError
from .logging_utils import configure_logging, get_logger
from .pipeline import ALL_STAGES

log = get_logger("cli")


def _parse_overrides(pares: list[str] | None) -> dict[str, Any]:
    """Convierte ['a.b=1', 'c=true'] en un dict plano para `param_overrides`."""
    salida: dict[str, Any] = {}
    for par in pares or []:
        if "=" not in par:
            raise FinOpsError(f"Override invalido '{par}'. Formato esperado ruta.clave=valor")
        clave, valor = par.split("=", 1)
        salida[clave.strip()] = valor.strip()
    return salida


def _build_config(args: argparse.Namespace):
    overrides = _parse_overrides(getattr(args, "set", None))
    if getattr(args, "full_refresh", False):
        overrides["ingestion.full_refresh"] = "true"
    if getattr(args, "dry_run", False):
        overrides["runtime.dry_run"] = "true"
    return load_config(
        args.env,
        conf_dir=getattr(args, "conf_dir", None),
        param_overrides=overrides,
        run_date=getattr(args, "run_date", None),
    )


def cmd_validate(args: argparse.Namespace) -> int:
    cfg = _build_config(args)
    print(f"Configuracion valida para el entorno '{cfg.env}'.")
    print(f"  {cfg.describe()}")
    print(f"  presupuestos definidos: {len(cfg.budgets.get('budgets', []) or [])}")
    canales = [c.get("name") for c in (cfg.get("alerting.channels") or []) if c.get("enabled")]
    print(f"  canales de alerta habilitados: {', '.join(canales) or 'ninguno'}")
    if args.show:
        print(json.dumps(cfg.redacted(), indent=2, default=str, ensure_ascii=False))
    return 0


def cmd_plan(args: argparse.Namespace) -> int:
    cfg = _build_config(args)
    print(f"Plan de ejecucion — entorno {cfg.env}")
    print(f"  ventana de proceso : {cfg.min_date} .. {cfg.max_date}")
    print(f"  catalogo destino   : {cfg.catalog}")
    print(f"  etapas             : {', '.join(args.stages.split(',')) if args.stages else ', '.join(ALL_STAGES)}")
    print(f"  dry-run            : {cfg.dry_run}")
    print("\nTablas del modelo:")
    mapa = table_map(cfg)
    for tabla in ALL_TABLES:
        print(f"  [{tabla.layer:<6}] {mapa[tabla.key]}")
    print("\nFuentes configuradas:")
    for clave in cfg.source_keys:
        definicion = cfg.source(clave)
        marca = "opcional" if definicion.get("optional") else "requerida"
        print(f"  {definicion['table']:<45} ({marca})")
    return 0


def cmd_run(args: argparse.Namespace) -> int:
    from .pipeline import run as run_pipeline

    cfg = _build_config(args)
    etapas = tuple(s.strip() for s in args.stages.split(",")) if args.stages else ALL_STAGES
    resultado = run_pipeline(cfg, stages=etapas, raise_on_error=not args.continue_on_error)
    print(resultado.summary())
    return 0 if resultado.ok else 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="finops", description="Plataforma de analitica FinOps para Databricks")
    parser.add_argument("--log-level", default="INFO", help="Nivel de log (DEBUG, INFO, WARNING, ERROR)")
    sub = parser.add_subparsers(dest="command", required=True)

    def comunes(p: argparse.ArgumentParser) -> None:
        p.add_argument("--env", required=True, choices=VALID_ENVIRONMENTS, help="Entorno objetivo")
        p.add_argument("--conf-dir", default=None, help="Directorio de configuracion (autodetectado si se omite)")
        p.add_argument("--run-date", default=None, help="Fecha logica de la corrida (YYYY-MM-DD)")
        p.add_argument("--set", action="append", metavar="RUTA=VALOR", help="Override de configuracion (repetible)")

    p_validate = sub.add_parser("validate", help="Valida la configuracion sin ejecutar nada")
    comunes(p_validate)
    p_validate.add_argument("--show", action="store_true", help="Imprime la configuracion efectiva")
    p_validate.set_defaults(func=cmd_validate)

    p_plan = sub.add_parser("plan", help="Muestra ventana, tablas destino y fuentes")
    comunes(p_plan)
    p_plan.add_argument("--stages", default=None, help="Etapas separadas por coma")
    p_plan.add_argument("--full-refresh", action="store_true")
    p_plan.add_argument("--dry-run", action="store_true")
    p_plan.set_defaults(func=cmd_plan)

    p_run = sub.add_parser("run", help="Ejecuta el pipeline (requiere Spark)")
    comunes(p_run)
    p_run.add_argument("--stages", default=None, help=f"Etapas separadas por coma. Validas: {','.join(ALL_STAGES)}")
    p_run.add_argument("--full-refresh", action="store_true", help="Reprocesa la ventana historica completa")
    p_run.add_argument("--dry-run", action="store_true", help="Calcula sin escribir")
    p_run.add_argument("--continue-on-error", action="store_true", help="No aborta al fallar una etapa")
    p_run.set_defaults(func=cmd_run)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    configure_logging(args.log_level)
    try:
        return int(args.func(args))
    except FinOpsError as exc:
        log.error("%s", exc)
        return 2
    except KeyboardInterrupt:  # pragma: no cover
        log.warning("Interrumpido por el usuario")
        return 130


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
