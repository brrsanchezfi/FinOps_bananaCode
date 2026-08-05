# 01 — Arquitectura

## Vision general

La plataforma es un pipeline medallion sobre Unity Catalog, con una capa de
analitica y una de alertamiento encima. Todo se despliega como un Databricks
Asset Bundle.

```
                       ┌──────────────────────────────────────────────┐
 system.billing.usage  │                                              │
 system.billing.list_prices                                           │
 system.compute.clusters      ──► BRONZE ──► SILVER ──► GOLD ──► ┌────┴─────┐
 system.compute.warehouses        copia      valorizado  modelo  │ Lakeview │
 system.lakeflow.jobs             incremental y atribuido dimensional       │
 system.lakeflow.job_run_timeline                              └────┬─────┘
 system.query.history  │                     │                      │
                       └─────────────────────┼──────────────────────┘
                                             ▼
                                    ANALYTICS  ──►  ALERTING
                                    anomalias      Teams / Slack
                                    pronostico     ops_alert_log
                                    presupuestos
                                    recomendaciones
                                    chargeback
```

## Capas

### Bronze — `finops.bronze`

Copia fiel de las system tables, acotada a la ventana de proceso. No transforma
nada; solo agrega columnas de auditoria (`_ingested_at`, `_run_id`, `_source`).

**Por que copiar si las system tables ya existen:** las system tables tienen
retencion limitada (365 dias en `billing.usage`, menos en otras), no se pueden
particionar ni optimizar, y consultarlas repetidamente desde dashboards es lento
y caro. La copia da control sobre retencion, layout e historia.

**Tolerancia a la evolucion del esquema.** Las system tables cambian: columnas
nuevas, y renombramientos como `system.workflow.*` → `system.lakeflow.*`. La
ingesta proyecta solo las columnas que existen (`select_existing`) y admite
`fallback_tables` en la configuracion. Las fuentes marcadas `optional: true` se
omiten con advertencia si el principal no tiene permisos, en lugar de tumbar la
corrida — un workspace sin acceso a `system.query.history` igual obtiene todo el
analisis de costo.

### Silver — `finops.silver`

Tabla central: **`slv_usage_priced`**, mismo grano que `system.billing.usage`
(un registro por intervalo de consumo), enriquecido con:

- **Precio**: join con `list_prices` por SKU + unidad + nube, respetando la
  vigencia contra `usage_end_time` (un cambio de tarifa a mitad de dia aplica
  solo a los intervalos posteriores).
- **Clasificacion de SKU**: grupo canonico (`ALL_PURPOSE`, `JOBS`, `DLT`, `SQL`,
  `SERVERLESS_*`, `MODEL_SERVING`, ...), banderas serverless y Photon.
- **Entidad de consumo**: job, pipeline, warehouse, endpoint, pool, cluster —
  resuelta por prioridad desde `usage_metadata`.
- **Dimensiones de atribucion**: `cost_center`, `team`, `project`, `environment`,
  `application`, `owner`, resueltas por cadena de precedencia de etiquetas.

### Gold — `finops.gold`

Modelo dimensional. Hecho central **`fct_cost_daily`**:

```
grano = usage_date × workspace_id × sku_name × entity_key × dimensiones de etiqueta
```

Sobre el se derivan agregados mensuales, costo por ejecucion de job, costo por
warehouse, cobertura de etiquetado y KPIs diarios. Las tablas de analitica
(`fct_cost_anomaly`, `fct_cost_forecast`, `fct_budget_status`,
`fct_recommendation`, `fct_chargeback_monthly`) se producen en la etapa
`analytics`.

Detalle completo en [02 — Modelo de datos](02-modelo-datos.md).

---

## Decisiones de diseno

### 1. La logica de negocio no vive en los notebooks

Un notebook no se puede probar en CI, no se puede refactorizar con seguridad y
no se puede reutilizar. Por eso todo lo que decide algo vive en `src/finops/` y
los notebooks son de ~30 lineas.

La consecuencia practica: la clasificacion de un SKU, la deteccion de una
anomalia o la evaluacion de un presupuesto se prueban con `pytest` en segundos,
sin cluster. La suite completa corre sin `pyspark` instalado.

### 2. Funciones puras separadas de los adaptadores de Spark

Los algoritmos (`analytics/`, `transform/pricing.py`, `transform/tags.py`,
`alerting/rules.py`, `quality/checks.py`) reciben y devuelven `dict`, `list` y
dataclasses. Spark aparece unicamente en `spark_utils.py`, `ingestion/`,
`transform/silver.py` y `transform/gold.py`.

Donde una misma regla debe existir en ambos mundos —la clasificacion de SKU se
necesita fila a fila en Spark y tambien en pruebas— los patrones se declaran una
sola vez (`pricing.SKU_PATTERN_SPECS`) y ambos caminos los consumen, para que no
puedan divergir.

### 3. Escritura idempotente por rango de fechas

Los registros de facturacion llegan con retraso: un dia puede recibir datos dos
o tres dias despues. Escribir en modo `append` duplicaria; escribir en
`overwrite` completo seria carisimo.

El patron es `replace_date_range`: se borra el rango `[min_date, max_date]` en el
destino y se agrega el lote nuevo. Reprocesar la misma ventana N veces produce
exactamente el mismo resultado. La ventana por defecto es de 7 dias hacia atras
(`ingestion.lookback_days`), suficiente para el SLA de publicacion de Databricks.

### 4. Registro central de tablas

`catalog.py` es la unica fuente de verdad de los nombres. Ni el codigo ni el SQL
de los dashboards escriben `finops.gold.fct_cost_daily` a mano: usan la clave
logica. Los dashboards versionados llevan marcadores `{{fct_cost_daily}}` que se
sustituyen por el FQN del entorno al desplegar, de modo que el mismo repositorio
promociona de dev a prd sin editar nada.

Una prueba (`tests/test_catalog.py`) verifica que ningun dashboard referencie una
tabla que no exista en el registro, y que ninguno tenga un catalogo incrustado.

### 5. Etapas independientes

`pipeline.run(stages=[...])` permite ejecutar cualquier subconjunto. Esto habilita:

- Un job diario multi-tarea, con una tarea por etapa: cada una tiene su duracion,
  sus reintentos y su estado en la UI de Workflows.
- Un job de **alertamiento** que corre tres veces al dia sobre el gold ya
  construido, sin repetir la ingesta.
- Reprocesar solo la analitica tras cambiar un umbral, sin volver a ingerir.

Para que el segundo caso funcione, la etapa `alerts` lee sus insumos de las
tablas gold cuando la etapa `analytics` no corrio en el mismo proceso.

### 6. La configuracion es datos, no codigo

Umbrales, reglas de descuento, alias de etiquetas, presupuestos, canales y
severidades viven en YAML. Cambiar el umbral de anomalias o agregar un
presupuesto no requiere tocar Python ni volver a construir el wheel.

Precedencia:

```
conf/base.yml  <  conf/<env>.yml  <  parametros de job  <  variables FINOPS__*
```

`validate_config` verifica invariantes al cargar (descuentos en rango, alias
definidos para cada dimension, canales sin duplicar, presupuestos con monto
positivo). CI valida los tres entornos en cada PR, asi que un YAML mal formado
no llega a produccion.

---

## Flujo de una corrida

```
setup        crea catalogo y schemas
  │
bronze       lee system tables en la ventana → brz_*
  │          registra marca de agua
silver       valoriza, resuelve entidades y etiquetas → slv_*
  │
gold         hecho diario, dimensiones, agregados, KPIs → fct_*/dim_*/agg_*
  │
  ├── quality      chequeos sobre silver y gold → ops_data_quality
  │                (falla el pipeline si hay errores bloqueantes)
  │
  └── analytics    anomalias, pronostico, presupuestos,
      │            recomendaciones, chargeback → fct_*
      │
    alerts         reglas → deduplicacion → canales → ops_alert_log
      │
    maintenance    OPTIMIZE / ZORDER / comentarios de catalogo
```

Cada etapa se cronometra y se registra en `ops_run_log`, incluida la que falla.
La bitacora se persiste incluso cuando el pipeline aborta, para poder diagnosticar.

---

## Manejo de fallos

| Situacion | Comportamiento |
|---|---|
| Fuente opcional sin permisos | Advertencia, se omite esa tabla, el resto continua |
| Tabla de sistema renombrada | Se prueba `fallback_tables` antes de fallar |
| Columna nueva o eliminada | Se proyectan solo las columnas presentes |
| SKU sin precio de lista | Costo 0, `price_missing = true`, lo detecta el chequeo `price_match` |
| Chequeo de calidad bloqueante | Aborta el pipeline si `quality.fail_pipeline_on_error` (en dev es `false`) |
| Etapa del pipeline falla | Se registra en `ops_run_log` y genera alerta `PIPELINE_HEALTH` |
| Canal de alerta caido | Reintentos exponenciales; la alerta igual queda en `ops_alert_log` |
| Secreto de webhook ausente | El canal se omite con advertencia, no tumba la corrida |

---

## Que **no** hace la plataforma

- No lee la factura de Azure. Solo el consumo de Databricks (DBU).
  Ver [06 — Modelo de costos](06-modelo-costos.md).
- No aplica cambios sobre los recursos: las recomendaciones son informativas, no
  se ejecuta ninguna accion correctiva automatica.
- No reemplaza las politicas de cluster. Detecta gasto sin etiquetar, pero forzar
  el etiquetado es responsabilidad de las cluster policies del workspace.
