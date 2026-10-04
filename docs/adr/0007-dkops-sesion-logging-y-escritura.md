# ADR 0007 — DKOps pone la sesion, el logging y la escritura de todas las capas

- **Estado:** aceptada
- **Fecha:** 2026-10-04

## Contexto

DKOps entro al repositorio solo para los contratos de bronze (commit 40b96d8),
como prueba acotada. El resto de la infraestructura seguia siendo propia:
`spark_utils.get_spark` para la sesion, `logging` de la libreria estandar para
el log y siete funciones de escritura en `spark_utils` (`overwrite_table`,
`replace_date_range`, `merge_table`, `append_rows`, ...), cada una con su propio
manejo de "la tabla no existe", `dry_run`, particiones y propiedades. Y los
contratos de bronze no se usaban al escribir: nada los validaba en una corrida.

DKOps ya resuelve todo eso: el `Launcher` crea la sesion correcta en Databricks,
en un PC o por Databricks Connect y configura el logger; los writers de
`table_governance` escriben contra un contrato y validan el DataFrame antes de
tocar la tabla.

## Decision

`finops.governance` es el unico puente con DKOps y expone lo que usa el
pipeline:

- `start_launcher(cfg)`: el `Launcher` alimentado desde `conf/*.yml`. El
  `config.json` que exige se genera en un temporal; la configuracion sigue
  teniendo una sola fuente de verdad.
- `overwrite`, `replace_range`, `delete_range`, `upsert`, `append` y
  `create_if_missing`: las escrituras, todas con los writers de DKOps contra el
  contrato de la tabla.

Las 34 tablas del registro tienen contrato en `finops/contracts/tables/<capa>/`,
dentro del paquete para que el wheel del cluster los lleve. `logging_utils`
delega en el logger de DKOps y conserva solo lo propio de FinOps: las metricas
por etapa que van a `ops_run_log`.

La sobrescritura completa usa un writer propio de una docena de lineas sobre
`BaseWriter` de DKOps, en vez de `TableWriter.overwrite`: este emite un
`CREATE OR REPLACE TABLE` con los tipos del contrato, y el contrato solo guarda
la clase de los tipos complejos (`MAP`, `STRUCT`), asi que ese DDL no es SQL
valido para las tablas que los tienen.

## Consecuencias

**A favor**

- Una sola forma de escribir. `spark_utils` pierde cerca de 250 lineas de
  escritura y sesion.
- Toda escritura se valida contra su contrato. Un cambio de esquema, en una
  system table o en una transformacion, falla antes de escribir o aparece como
  diff de un contrato.
- `replace_range` valida el lote antes de borrar el rango: un lote invalido ya
  no deja dias vacios.
- La misma sesion y el mismo log en Databricks, en un PC y por Databricks
  Connect (`runtime.launcher` en `conf/local.yml`). El log de cada corrida
  queda en el volumen `/Volumes/<catalogo>/<gold>/logs`, no en la carpeta del
  bundle, que cada `bundle deploy` sincroniza.

**En contra**

- Cambiar el esquema de una tabla ahora exige actualizar su contrato
  (`python scripts/contratos.py bootstrap --schemas <archivo>`). Una columna
  nueva no bloquea (`merge_schema` y WARNING), pero una que desaparece o cambia
  de clase si.
- Dependencia mas profunda de DKOps 0.3.x, incluido un metodo interno de
  `BaseWriter`. Cuando DKOps acepte tipos complejos completos en el contrato,
  el writer propio de sobrescritura sobra.
- En un PC los writers de DKOps registran las tablas como `schema.tabla` (sin
  catalogo). En Databricks el nombre es el de siempre.
- El log es mas verboso: DKOps registra cada contrato que carga y cada
  validacion.
