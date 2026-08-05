# ADR 0002 — Escritura idempotente por reemplazo de rango de fechas

- **Estado:** aceptada
- **Fecha:** 2026-08-05

## Contexto

`system.billing.usage` publica registros con retraso: el consumo de un dia puede
llegar horas o incluso dos o tres dias despues, y Databricks tambien emite
registros correctivos. Un pipeline que procese "solo lo nuevo" pierde esos datos
tardios y subestima el costo de forma permanente.

Las opciones de escritura eran:

1. `append` de la ventana reciente — duplica en cada corrida.
2. `overwrite` completo de la tabla — correcto pero costoso: recalcular anos de
   historia cada dia.
3. `MERGE` por clave de negocio — correcto, pero `system.billing.usage` no expone
   una clave estable y barata para todos los hechos derivados, y el MERGE sobre
   tablas grandes es caro.
4. **Reemplazo del rango de fechas procesado.**

## Decision

Se adopta la opcion 4, implementada en `spark_utils.replace_date_range`:

```
DELETE FROM destino WHERE fecha BETWEEN min_date AND max_date;
INSERT (append) del lote nuevo;
```

La ventana por defecto son 7 dias hacia atras (`ingestion.lookback_days`), lo que
cubre con margen la latencia observada de publicacion.

Los catalogos sin fecha (precios, clusters, jobs, warehouses) se escriben como
snapshot completo (`overwrite`), porque son pequenos y solo interesa su estado
mas reciente.

Las tablas de analitica que representan "el estado vigente" (pronostico,
recomendaciones) se sobrescriben enteras. Las que llevan historia
(`fct_budget_status`, `fct_chargeback_monthly`) usan `MERGE` por su clave
natural, que ahi si es estable y de baja cardinalidad.

## Consecuencias

**A favor**

- **Idempotencia real:** correr el pipeline N veces sobre la misma ventana
  produce exactamente el mismo resultado. Un reintento tras un fallo es seguro.
- Los datos tardios se incorporan automaticamente sin logica especial.
- El costo de proceso es proporcional a la ventana, no al historico.
- Reprocesar un rango arbitrario es trivial:
  `--params run_date=2026-07-31,overrides="ingestion.lookback_days=31"`.

**En contra**

- Requiere que las tablas esten particionadas por la columna de fecha para que el
  `DELETE` sea eficiente. Se cumple: `usage_date`, `run_date` y `query_date` son
  claves de particion.
- Dos corridas simultaneas sobre rangos solapados compiten. Se mitiga con
  `max_concurrent_runs: 1` en los jobs.
- El `DELETE` genera archivos tombstone; se compensa con `OPTIMIZE` en la etapa
  de mantenimiento y el `VACUUM` estandar de Delta.

## Notas

La marca de agua (`ops_watermark`) se registra para diagnostico y para decidir la
ventana del primer backfill, pero **no** se usa para avanzar incrementalmente: la
ventana movil hacia atras es lo que garantiza capturar los datos tardios. Una
marca de agua estricta reintroduciria el problema que esta decision resuelve.
