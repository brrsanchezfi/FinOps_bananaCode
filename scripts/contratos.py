"""Contratos de tabla DKOps para la capa bronze.

    python scripts/contratos.py bootstrap --schemas esquemas.json
    python scripts/contratos.py check

Por que existen estos contratos
-------------------------------
Hoy el esquema de bronze no esta escrito en ninguna parte: las tablas nacen de
proyectar lo que traiga la system table de turno. Eso hace que un cambio de
esquema en el origen entre sin que nadie lo revise, y las system tables cambian
(este repo ya carga con el renombre `system.workflow.*` -> `system.lakeflow.*`).
Un contrato JSON versionado convierte ese cambio en un diff de pull request.

`bootstrap` es una herramienta de ARRANQUE, no parte del pipeline: toma los
esquemas reales de un workspace ya cargado y escribe los contratos por primera
vez. A partir de ahi los contratos son fuente de verdad y se editan a mano; si
se regeneran a ciegas contra el workspace, se pierde justamente la revision que
justifica tenerlos.

No se conecta a Databricks: recibe un archivo con el resultado de esta consulta,
para no arrastrar plomeria de autenticacion a una herramienta que se usa una vez.

    SELECT table_name, column_name, full_data_type, is_nullable
    FROM <catalogo>.information_schema.columns
    WHERE table_schema = 'bronze'
    ORDER BY table_name, ordinal_position

Sobre los placeholders
----------------------
`{catalog.bronze}` lo resuelve DKOps. El SCHEMA va como `{path.schema_bronze}`
porque el espacio de placeholders de DKOps es catalogo/path/env y no tiene uno
para schema: FinOps pone las tres capas en UN catalogo separadas por schema (ver
docs/adr/0005), asi que el nombre del schema tiene que viajar por algun lado.
`finops.governance` es quien lo inyecta desde `conf/*.yml`.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
CONTRACTS_DIR = REPO_ROOT / "contracts" / "tables" / "bronze"

#: Columnas que agrega el propio pipeline, no el origen (ver `with_audit_columns`).
COLUMNAS_DE_AUDITORIA = {"_ingested_at", "_source_table", "_run_id"}


def _tablas_bronze() -> dict[str, object]:
    """TableDefs de bronze, indexados por nombre fisico."""
    sys.path.insert(0, str(REPO_ROOT / "src"))
    from finops.catalog import ALL_TABLES

    return {t.name: t for t in ALL_TABLES if t.layer == "bronze"}


#: Nombre SQL de Spark -> nombre que entiende DKOps. Spark reporta `BIGINT` e
#: `INT` en information_schema; el vocabulario de contratos de DKOps es `LONG` e
#: `INTEGER`. Sin esta traduccion el contrato no carga.
EQUIVALENCIAS_DE_TIPO = {
    "BIGINT": "LONG",
    "INT": "INTEGER",
    "SMALLINT": "INTEGER",
    "TINYINT": "INTEGER",
    "REAL": "FLOAT",
    "VARCHAR": "STRING",
    "CHAR": "STRING",
    "TIMESTAMP_NTZ": "TIMESTAMP",
}


def tipo_base(tipo_completo: str) -> str:
    """Tipo que entiende DKOps, a partir del tipo completo de Spark.

    DKOps valida contra nombres SIN parametrizar (MAP, ARRAY, STRUCT): comprueba
    la CLASE del tipo, no su forma interna. `MAP<STRING,STRING>` se declara
    entonces como `MAP`.

    Es una limitacion real y conviene tenerla presente: el contrato detecta que
    una columna cambie de nombre, desaparezca o cambie de clase, pero NO que a
    `usage_metadata` le agreguen o le quiten un campo por dentro. Por eso el
    tipo completo se conserva en el comentario de la columna -- ahi si aparece
    en el diff del pull request, aunque DKOps no lo valide.
    """
    crudo = tipo_completo.upper().split("<", 1)[0].split("(", 1)[0].strip()
    return EQUIVALENCIAS_DE_TIPO.get(crudo, crudo)


def agrupar_columnas(filas: list[dict]) -> dict[str, list[dict]]:
    """Agrupa el resultado de information_schema por tabla. Funcion pura."""
    salida: dict[str, list[dict]] = {}
    for fila in filas:
        completo = fila["full_data_type"].upper()
        base = tipo_base(completo)
        salida.setdefault(fila["table_name"], []).append(
            {
                "name": fila["column_name"],
                "type": base,
                "nullable": fila["is_nullable"] == "YES",
                # Solo cuando aporta: para STRING o DATE repetirlo es ruido.
                **({"comment": completo} if completo != base else {}),
            }
        )
    return salida


def construir_contrato(tabla, columnas: list[dict]) -> dict:
    """Contrato de una tabla bronze. Funcion pura: se prueba sin Spark."""
    return {
        "_doc": f"Bronze — {tabla.description}",
        "catalog": "{catalog.bronze}",
        "schema": "{path.schema_bronze}",
        "name": tabla.name,
        "type": "MANAGED",
        "format": "DELTA",
        "comment": tabla.description,
        "owner": "finops",
        # Bronze copia la system table tal cual: TODA columna es nullable a
        # proposito. Declarar `nullable: false` sobre un origen que no
        # controlamos convierte un dato faltante en una corrida rota, y el
        # principio de esta capa es tolerar el origen, no disciplinarlo.
        "columns": [{**c, "nullable": True} for c in columnas],
        "partitions": list(tabla.partition_by),
        "properties": {
            "layer": "bronze",
            "quality": "raw",
            # Las system tables ganan columnas sin aviso. Sin esto, la primera
            # columna nueva rompe la ingesta en vez de incorporarse.
            "merge_schema": True,
        },
    }


def cmd_bootstrap(args: argparse.Namespace) -> int:
    tablas = _tablas_bronze()
    columnas = agrupar_columnas(json.loads(Path(args.schemas).read_text(encoding="utf-8")))

    CONTRACTS_DIR.mkdir(parents=True, exist_ok=True)
    faltantes = sorted(set(tablas) - set(columnas))
    if faltantes:
        print(f"ATENCION: sin esquema en el workspace, se omiten: {faltantes}", file=sys.stderr)

    for nombre, tabla in sorted(tablas.items()):
        if nombre not in columnas:
            continue
        contrato = construir_contrato(tabla, columnas[nombre])
        destino = CONTRACTS_DIR / f"{nombre}.json"
        destino.write_text(json.dumps(contrato, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        print(f"escrito {destino.relative_to(REPO_ROOT).as_posix()} ({len(contrato['columns'])} columnas)")
    return 0


def cmd_check(args: argparse.Namespace) -> int:  # noqa: ARG001
    """Valida que cada tabla bronze del registro tenga contrato, y viceversa."""
    tablas = set(_tablas_bronze())
    contratos = {p.stem for p in CONTRACTS_DIR.glob("*.json")} if CONTRACTS_DIR.is_dir() else set()

    sin_contrato = sorted(tablas - contratos)
    huerfanos = sorted(contratos - tablas)
    if sin_contrato:
        print(f"Tablas bronze sin contrato: {sin_contrato}", file=sys.stderr)
    if huerfanos:
        print(f"Contratos sin tabla en el registro: {huerfanos}", file=sys.stderr)
    if sin_contrato or huerfanos:
        return 1
    print(f"Contratos al dia ({len(contratos)} tablas bronze).")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Contratos de tabla DKOps (bronze)")
    sub = parser.add_subparsers(dest="command", required=True)

    p_boot = sub.add_parser("bootstrap", help="Genera los contratos desde esquemas ya extraidos")
    p_boot.add_argument(
        "--schemas", required=True,
        help="JSON con el resultado de la consulta a information_schema (ver el docstring)",
    )
    p_boot.set_defaults(func=cmd_bootstrap)

    p_check = sub.add_parser("check", help="Verifica la correspondencia registro <-> contratos")
    p_check.set_defaults(func=cmd_check)

    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    sys.exit(main())
