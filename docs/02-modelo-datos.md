# 02 — Modelo de datos

Todos los nombres se resuelven desde `src/finops/catalog.py`. En este documento
se usan sin catalogo: el prefijo real es `finops_dev.`, `finops_qa.` o `finops.`
segun el entorno.

---

## Bronze — copia de las system tables

| Tabla | Origen | Grano | Particion |
|---|---|---|---|
| `brz_billing_usage` | `system.billing.usage` | registro de consumo | `usage_date` |
| `brz_billing_list_prices` | `system.billing.list_prices` | SKU × vigencia | — |
| `brz_compute_clusters` | `system.compute.clusters` | cluster × cambio | — |
| `brz_compute_node_types` | `system.compute.node_types` | tipo de nodo | — |
| `brz_compute_warehouses` | `system.compute.warehouses` | warehouse × cambio | — |
| `brz_lakeflow_jobs` | `system.lakeflow.jobs` | job × cambio | — |
| `brz_lakeflow_job_run_timeline` | `system.lakeflow.job_run_timeline` | intervalo de ejecucion | `run_date` |
| `brz_lakeflow_job_task_run_timeline` | `system.lakeflow.job_task_run_timeline` | intervalo de tarea | `run_date` |
| `brz_query_history` | `system.query.history` | consulta SQL | `query_date` |
| `brz_workspaces` | `system.access.workspaces_latest` | workspace | — |
| `ops_watermark` | interno | fuente × corrida | — |

Las tablas con fecha se escriben con reemplazo de rango; las de catalogo, como
snapshot completo.

Columnas de auditoria en todas: `_ingested_at`, `_run_id`, `_source`.

---

## Silver

### `slv_usage_priced` — consumo valorizado

**Grano:** un registro de `system.billing.usage`. Es la tabla mas importante del
modelo: todo lo demas se deriva de ella.

**Particion:** `usage_date`. **Z-ORDER:** `workspace_id`, `sku_group`.

| Grupo | Columnas |
|---|---|
| Identificacion | `record_id`, `account_id`, `workspace_id`, `cloud` |
| Tiempo | `usage_date`, `usage_start_time`, `usage_end_time` |
| Consumo | `sku_name`, `usage_unit`, `usage_quantity`, `billing_origin_product`, `record_type`, `usage_type` |
| Clasificacion | `sku_group`, `compute_family`, `is_serverless`, `is_photon` |
| Entidad | `entity_type`, `entity_id`, `entity_key`, `entity_name`, `job_id`, `job_run_id`, `cluster_id`, `warehouse_id`, `dlt_pipeline_id`, `endpoint_id`, `instance_pool_id`, `owner_resolved` |
| Precio | `unit_price`, `price_currency`, `price_missing` |
| Descuento | `discount_pct`, `discount_rule` |
| Costo | `list_cost_usd`, `discount_amount_usd`, `effective_cost_usd`, `estimated_infra_cost_usd`, `total_cost_usd`, `infra_factor` |
| Atribucion | `cost_center`, `team`, `project`, `environment`, `application`, `owner` |
| Trazabilidad | `tag_source_<dimension>` (que fuente resolvio cada etiqueta), `tags_resolved`, `tags_expected`, `is_fully_tagged`, `is_untagged` |

**`entity_key`** tiene el formato `TIPO:id` (`JOB:412`, `WAREHOUSE:abc123`,
`CLUSTER:0710-...`). Es la clave de union con `dim_entity` y con los hechos
derivados.

**Prioridad de resolucion de entidad** (`transform/tags.py:ENTITY_PRIORITY`):

```
job_id → dlt_pipeline_id → warehouse_id → endpoint_id
       → instance_pool_id → cluster_id → notebook_id → app_id → metastore_id
```

Una tarea de job que corre en un job cluster se imputa al **job**, no al cluster:
el job es la unidad de negocio, el cluster es un detalle de implementacion. El
`cluster_id` sigue disponible como columna para el analisis de configuracion.

### Resto de silver

| Tabla | Grano | Para que sirve |
|---|---|---|
| `slv_job_runs` | ejecucion de job | duracion, resultado, fallos por job |
| `slv_clusters` | cluster (ultima version) | banderas de eficiencia: `autotermination_minutes`, `autoscale_enabled`, `num_workers`, `spark_version`, `is_single_node` |
| `slv_warehouses` | warehouse (ultima version) | `warehouse_size`, `auto_stop_minutes`, `min/max_clusters` |
| `slv_queries` | dia × workspace × warehouse × usuario | `query_count`, `active_hours`, duraciones, `failed_query_count` |

---

## Gold — dimensiones

| Tabla | Grano | Notas |
|---|---|---|
| `dim_date` | dia | Cubre la historia cargada + 90 dias (para el pronostico). `is_future` marca el tramo proyectado |
| `dim_sku` | SKU | Clasificacion, ultimo precio unitario, costo acumulado |
| `dim_workspace` | workspace | Nombre y URL si `system.access.workspaces_latest` esta disponible |
| `dim_entity` | `entity_key` | Dimension unificada: cluster, warehouse, job, pipeline, endpoint. Incluye la configuracion (auto-terminacion, autoescalado, DBR, tamano de warehouse) y `first_seen_date` / `last_seen_date` / `days_since_activity` |

`dim_entity` es lo que alimenta el motor de recomendaciones: es donde se cruza
*cuanto cuesta* con *como esta configurado*.

---

## Gold — hechos

### `fct_cost_daily` — hecho central

**Grano:** `usage_date × account_id × workspace_id × cloud × sku_name × sku_group
× compute_family × is_serverless × is_photon × entity_key × entity_type ×
entity_id × cost_center × team × project × environment × application × owner`

**Particion:** `usage_date`. **Z-ORDER:** `team`, `cost_center`, `sku_group`.

Medidas: las cinco de costo (`list_cost_usd` … `total_cost_usd`),
`usage_quantity`, `usage_record_count`, `records_without_price`,
`is_untagged`, `is_fully_tagged`. Mas `year_month` para agregar por mes sin
recalcular.

> **Cual medida usar:** `total_cost_usd` para reportar (incluye la estimacion de
> infraestructura si esta habilitada), `effective_cost_usd` para conciliar
> contra la factura de Databricks, `list_cost_usd` para ver el efecto del
> descuento negociado.

### Hechos derivados

| Tabla | Grano | Contenido |
|---|---|---|
| `agg_cost_monthly` | mes × workspace × sku_group × dimensiones | Agregado mensual, `entity_count`, `active_days` |
| `fct_job_run_cost` | ejecucion de job | Costo imputado por corrida, duracion, `is_failed`, `cost_per_minute_usd`, `has_cost` |
| `fct_warehouse_cost_daily` | dia × warehouse | Costo cruzado con `query_count`, `active_hours`, `cost_per_query_usd`, configuracion del warehouse |
| `fct_tag_coverage_daily` | dia × workspace × dimension | `total_cost_usd`, `attributed_cost_usd`, `unattributed_cost_usd`, `coverage_ratio` |
| `fct_kpi_daily` | dia | KPIs de la organizacion (ver abajo) |

**`fct_job_run_cost.has_cost`**: hay ejecuciones sin costo atribuible (corrieron
en compute compartido que `usage_metadata` no desglosa por `job_run_id`). Esas
filas quedan con `run_cost_usd = 0` y `has_cost = false` en lugar de repartir el
costo con un supuesto. Filtrar por `has_cost = true` cuando se calcule costo
promedio por ejecucion.

### `fct_kpi_daily`

Una fila por dia con: `total_cost_usd`, `effective_cost_usd`, `total_dbus`,
`active_entities`, `active_workspaces`, `mtd_cost_usd`, `rolling_7d_avg_usd`,
`rolling_28d_avg_usd`, `dod_change_pct`, `wow_change_pct`, `cost_per_dbu_usd`,
`allocation_coverage_ratio`, `serverless_share`, `all_purpose_share`,
`photon_cost_usd`, `unallocated_cost_usd`.

Es la tabla que alimenta los contadores de la vista ejecutiva. Consultarla es
barato: una fila por dia.

---

## Gold — analitica

| Tabla | Grano | Escritura |
|---|---|---|
| `fct_cost_anomaly` | dimension × valor × dia | Reemplazo de rango sobre la ventana evaluada |
| `fct_cost_forecast` | dimension × valor × dia futuro | Sobrescritura completa (el pronostico siempre es "el vigente") |
| `fct_budget_status` | presupuesto × periodo × `as_of_date` | MERGE — conserva la historia diaria de cada presupuesto |
| `fct_recommendation` | regla × entidad | Sobrescritura completa (foto de las recomendaciones abiertas) |
| `fct_chargeback_monthly` | mes × dimension × unidad | MERGE por `(period, allocation_dimension, unit)` |

`fct_budget_status` guarda el estado de cada dia, no solo el ultimo: permite ver
como evoluciono el consumo de un presupuesto a lo largo del mes. Para el estado
vigente:

```sql
SELECT * FROM finops.gold.fct_budget_status
QUALIFY ROW_NUMBER() OVER (PARTITION BY budget_id ORDER BY as_of_date DESC) = 1
```

### Columnas clave de `fct_recommendation`

`rule_id`, `title`, `entity_key`, `entity_name`, `observed_cost_usd`,
`estimated_monthly_savings_usd`, `savings_pct`, **`confidence`** (alta/media/baja),
`severity`, **`estimation_method`**, `recommendation`, `evidence` (map).

`estimation_method` explica de donde sale la cifra de ahorro. Es obligatorio en
toda regla: una recomendacion sin metodo declarado no es accionable.

### Columnas clave de `fct_chargeback_monthly`

`unit`, `direct_cost_usd`, `allocated_shared_usd`, `allocated_unallocated_usd`,
`overhead_usd`, `total_chargeback_usd`, `pct_of_total`, `direct_pct_of_unit`,
`entity_count`.

**Invariante:** la suma de `total_chargeback_usd` de un periodo equivale al costo
total del periodo × (1 + `overhead_pct`). El pipeline lo verifica con
`chargeback.reconcile` y registra una advertencia si no cuadra.

---

## Tablas operativas

| Tabla | Contenido |
|---|---|
| `ops_run_log` | Una fila por etapa y corrida: `status`, `duration_seconds`, `rows_written`, `error_message` |
| `ops_data_quality` | Resultado de cada chequeo: `check`, `passed`, `severity`, `observed`, `threshold` |
| `ops_alert_log` | Alertas generadas: `fingerprint`, `severity`, `dispatch_status` (`dispatched`/`suppressed`), `channels`, `delivered` |
| `ops_watermark` | Marca de agua de ingesta por fuente |

`ops_alert_log` es tambien la memoria de la deduplicacion: el despachador lee de
ahi las huellas ya notificadas para respetar el periodo de enfriamiento.

---

## Consultas frecuentes

**Costo del mes por equipo:**

```sql
SELECT team, ROUND(SUM(total_cost_usd), 2) AS costo_usd
FROM finops.gold.fct_cost_daily
WHERE year_month = DATE_FORMAT(CURRENT_DATE(), 'yyyy-MM')
GROUP BY team
ORDER BY costo_usd DESC
```

**Jobs mas caros por ejecucion (solo los que tienen costo atribuido):**

```sql
SELECT job_name,
       COUNT(*) AS ejecuciones,
       ROUND(AVG(run_cost_usd), 4) AS costo_promedio_usd,
       ROUND(AVG(duration_minutes), 2) AS duracion_promedio_min
FROM finops.gold.fct_job_run_cost
WHERE run_date >= CURRENT_DATE() - INTERVAL 30 DAYS
  AND has_cost = true
GROUP BY job_name
ORDER BY costo_promedio_usd DESC
LIMIT 20
```

**Gasto sin responsable, por workspace:**

```sql
SELECT workspace_id,
       ROUND(SUM(total_cost_usd), 2) AS sin_atribuir_usd
FROM finops.gold.fct_cost_daily
WHERE usage_date >= CURRENT_DATE() - INTERVAL 30 DAYS
  AND cost_center = 'SIN_ASIGNAR'
GROUP BY workspace_id
ORDER BY sin_atribuir_usd DESC
```

**De donde salio una etiqueta (auditoria de atribucion):**

```sql
SELECT team, tag_source_team, COUNT(*) AS registros,
       ROUND(SUM(total_cost_usd), 2) AS costo_usd
FROM finops.silver.slv_usage_priced
WHERE usage_date >= CURRENT_DATE() - INTERVAL 7 DAYS
GROUP BY team, tag_source_team
ORDER BY costo_usd DESC
```
