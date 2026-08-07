# 07 — Runbook operativo

## Verificacion diaria (2 minutos)

```sql
-- 1. ¿Corrio el pipeline y termino bien?
SELECT run_id, run_date, stage, status, duration_seconds, rows_written, error_message
FROM finops.gold.ops_run_log
WHERE run_date >= CURRENT_DATE() - INTERVAL 2 DAYS
ORDER BY run_started_at DESC, stage;

-- 2. ¿Los datos estan frescos?
SELECT MAX(usage_date) AS ultimo_dia,
       DATEDIFF(CURRENT_DATE(), MAX(usage_date)) AS dias_de_rezago
FROM finops.silver.slv_usage_priced;

-- 3. ¿Fallo algun chequeo de calidad?
SELECT check, passed, severity, observed, threshold, message
FROM finops.gold.ops_data_quality
WHERE checked_at >= CURRENT_TIMESTAMP() - INTERVAL 1 DAY
  AND passed = false;

-- 4. ¿Que se alerto?
SELECT created_at, rule_id, severity, title, dispatch_status, delivered
FROM finops.gold.ops_alert_log
WHERE created_at >= CURRENT_TIMESTAMP() - INTERVAL 1 DAY
ORDER BY created_at DESC;
```

Un rezago de 1 dia es normal. De 2 o mas, investigar.

---

## Diagnostico

### El pipeline fallo

```sql
SELECT stage, error_message, duration_seconds
FROM finops.gold.ops_run_log
WHERE run_id = '<run_id>' AND status = 'error';
```

| Error contiene | Causa probable | Accion |
|---|---|---|
| `SourceUnavailableError` | Falta permiso o el schema de sistema no esta habilitado | Ver [03 — Despliegue](03-despliegue.md#permisos-en-databricks) |
| `DataQualityError` | Chequeo bloqueante fallido | Ver la seccion de calidad abajo |
| `TABLE_OR_VIEW_NOT_FOUND` | Primera corrida sin `setup`, o schema borrado | Correr con `stages=setup,bronze,silver,gold` |
| `AnalysisException ... column` | Cambio de esquema en una system table | Ver "cambio de esquema" abajo |
| `PERMISSION_DENIED` sobre el catalogo | Falta `CREATE SCHEMA` / `USE CATALOG` | Conceder permisos al principal |

Reintentar una sola etapa:

```bash
databricks bundle run finops_pipeline_diario -t prd --params stages=silver
```

### Los datos no estan frescos

```sql
-- ¿El origen tiene los datos?
SELECT MAX(usage_date) FROM system.billing.usage;

-- ¿Bronze los trajo?
SELECT MAX(usage_date) FROM finops.bronze.brz_billing_usage;

-- ¿Hasta donde avanzo la marca de agua?
SELECT source_key, MAX(watermark_date) AS marca, MAX(updated_at) AS actualizada
FROM finops.bronze.ops_watermark GROUP BY source_key;
```

- Origen desactualizado → es latencia de Databricks, esperar.
- Origen al dia y bronze atrasado → el job no corrio o fallo la ingesta.
- Bronze al dia y silver atrasado → fallo la etapa `silver`.

### El costo no cuadra con la factura

Ver [06 — Modelo de costos](06-modelo-costos.md#conciliar-con-la-factura-de-databricks).
Recordar: conciliar con `effective_cost_usd`, **no** con `total_cost_usd`.

```sql
-- SKU sin precio (subestiman el costo)
SELECT sku_name, COUNT(*) AS registros, ROUND(SUM(usage_quantity), 3) AS dbus
FROM finops.silver.slv_usage_priced
WHERE price_missing = true AND usage_date >= CURRENT_DATE() - INTERVAL 30 DAYS
GROUP BY sku_name ORDER BY dbus DESC;
```

### Un pico de costo

```sql
-- 1. Que dia y cuanto
SELECT usage_date, total_cost_usd, rolling_7d_avg_usd, dod_change_pct
FROM finops.gold.fct_kpi_daily
ORDER BY usage_date DESC LIMIT 14;

-- 2. Que dimension explica el salto
SELECT dimension, dimension_value, actual_cost_usd, expected_cost_usd,
       deviation_usd, severity
FROM finops.gold.fct_cost_anomaly
WHERE usage_date = DATE'AAAA-MM-DD'
ORDER BY ABS(deviation_usd) DESC;

-- 3. Que recurso concreto, comparado con el dia anterior
SELECT entity_type, entity_name, sku_group, team,
       ROUND(SUM(CASE WHEN usage_date = DATE'AAAA-MM-DD' THEN total_cost_usd END), 2) AS dia_pico,
       ROUND(SUM(CASE WHEN usage_date = DATE'AAAA-MM-DD' - 1 THEN total_cost_usd END), 2) AS dia_previo
FROM finops.gold.fct_cost_daily
WHERE usage_date BETWEEN DATE'AAAA-MM-DD' - 1 AND DATE'AAAA-MM-DD'
GROUP BY entity_type, entity_name, sku_group, team
ORDER BY dia_pico DESC LIMIT 20;
```

El notebook `90_exploracion` tiene estas consultas listas y parametrizadas.

### Cambio de esquema en una system table

Sintoma: `AnalysisException` mencionando una columna en la etapa `bronze` o
`silver`.

La ingesta ya proyecta solo las columnas presentes, asi que una columna
**eliminada** se maneja sola (con advertencia en el log). Los casos que requieren
intervencion:

- **Tabla renombrada**: agregar el nombre anterior a `fallback_tables` en
  `conf/base.yml`.
- **Columna renombrada**: actualizar la lista correspondiente en
  `src/finops/ingestion/system_tables.py` (`USAGE_COLUMNS`, `CLUSTER_COLUMNS`, …).
- **Campo de struct movido**: `_struct_field` devuelve NULL si el campo no
  existe, asi que no rompe, pero el dato se pierde. Ajustar en
  `transform/silver.py`.

Buscar en el log las advertencias `columnas ausentes en el origen`.

---

## Chequeos de calidad

| Chequeo | Severidad | Que significa si falla |
|---|---|---|
| `freshness` | error | Rezago mayor al esperado: problema de ingesta o permisos |
| `row_count` | error | La ventana no trajo filas |
| `null_ratio.total_cost_usd` | error | Nulos en la medida de costo |
| `negative_cost` | error | Costo negativo (creditos o error de precio) |
| `price_match` | error | Menos del 98% de registros con precio resuelto |
| `tag_coverage.<dim>` | **warning** | Cobertura de etiquetado baja: no bloquea |
| `duplicates` | error | Grano de `fct_cost_daily` violado |

En `dev`, `quality.fail_pipeline_on_error: false` — los chequeos se registran
pero no abortan. En `qa` y `prd` si abortan.

Para desbloquear una corrida en produccion mientras se resuelve la causa raiz:

```bash
databricks bundle run finops_pipeline_diario -t prd \
  --params overrides="quality.fail_pipeline_on_error=false"
```

Es una medida temporal: deja constancia en `ops_data_quality` de que se corrio
con chequeos fallidos.

---

## Tareas periodicas

### Semanal

- Revisar el dashboard **Optimizacion** y convertir las recomendaciones de alta
  confianza en tickets.
- Revisar `TAG_COVERAGE_DROP`: si la cobertura baja de forma sostenida, revisar
  las politicas de cluster.

### Mensual

- Conciliar `effective_cost_usd` contra la factura de Databricks.
- Revisar `fct_chargeback_monthly` del mes cerrado con las areas.
- Actualizar `conf/budgets.yml` con los presupuestos del periodo siguiente.
- Verificar que no aparezcan SKU nuevos clasificados como `OTHER`:

```sql
SELECT sku_name, billing_origin_product, ROUND(SUM(total_cost_usd), 2) AS costo_usd
FROM finops.gold.fct_cost_daily
WHERE sku_group = 'OTHER' AND usage_date >= CURRENT_DATE() - INTERVAL 30 DAYS
GROUP BY sku_name, billing_origin_product
ORDER BY costo_usd DESC;
```

Si alguno tiene costo relevante, agregar su patron en
`transform/pricing.py:SKU_PATTERN_SPECS` con su prueba.

### Trimestral

- Revisar los umbrales de `anomaly` y `optimization` contra la tasa de falsos
  positivos observada.
- Revisar los factores de `pricing.infra_estimate` si esta habilitado.
- Revisar la retencion: `runtime.gold_retention_days`.

---

## Operaciones comunes

**Reprocesar un rango de fechas:**

```bash
databricks bundle run finops_pipeline_diario -t prd \
  --params run_date=2026-07-31,overrides="ingestion.lookback_days=31"
```

Procesa del 30 de junio al 31 de julio. La escritura por rango es idempotente:
repetirlo no duplica.

**Recalcular solo la analitica tras cambiar un umbral:**

```bash
databricks bundle run finops_pipeline_diario -t prd --params stages=analytics
```

**Probar sin escribir:**

```bash
databricks bundle run finops_pipeline_diario -t dev --params dry_run=true
```

Calcula todo y reporta las filas que escribiria, sin tocar ninguna tabla. Los
canales de alerta pasan automaticamente a `noop`.

**Silenciar temporalmente las alertas externas:**

```bash
databricks bundle run finops_alertas -t prd \
  --params overrides="alerting.min_severity=critical"
```

O poner `enabled: false` en el canal y redesplegar.

**Backfill completo desde cero:**

```bash
databricks bundle run finops_backfill -t prd
```

---

## Incidentes

### El pipeline lleva varios dias sin correr

1. No se perdio nada: `system.billing.usage` retiene 365 dias.
2. Correr con una ventana que cubra el hueco:

```bash
databricks bundle run finops_pipeline_diario -t prd \
  --params overrides="ingestion.lookback_days=15"
```

3. Verificar la continuidad de la serie:

```sql
SELECT usage_date, total_cost_usd FROM finops.gold.fct_kpi_daily
WHERE usage_date >= CURRENT_DATE() - INTERVAL 20 DAYS ORDER BY usage_date;
```

### Tormenta de alertas

El tope `max_alerts_per_run` (50) ya limita el envio. Si aun asi es excesivo,
subir temporalmente `alerting.min_severity` a `critical` y atacar la causa: casi
siempre es un cambio masivo de etiquetado o una anomalia sistemica que dispara
muchas series a la vez.

### `dashboard has been modified remotely` al desplegar

```
Error: dashboard "finops_optimizacion" has been modified remotely
```

No es un fallo: es una salvaguarda del CLI. Alguien edito el dashboard en la UI
y esos cambios no estan en el repositorio, asi que el CLI se niega a
sobrescribirlos en silencio.

**Antes de forzar, decidir que version vale.** El contenido de los dashboards se
gestiona desde `scripts/dashboards.py`; una edicion hecha en la UI se pierde en
el siguiente deploy por diseno.

- Si el cambio remoto no importa (o ya se traslado al generador):

  ```bash
  databricks bundle deploy -t dev --force
  ```

- Si el cambio remoto vale la pena conservar, primero exportarlo y trasladarlo
  al generador:

  ```bash
  databricks bundle generate dashboard --existing-path "/Workspace/Users/<usuario>/<dashboard>" -t dev
  ```

  Luego ajustar `scripts/dashboards.py`, `generate`, commitear y desplegar sin
  `--force`.

Publicar un dashboard (boton *Publish*) no deberia disparar este aviso; editarlo
si.

### Un dashboard muestra "NO DATA" o TABLE_OR_VIEW_NOT_FOUND

Primero distinguir las dos cosas, porque tienen causas distintas:

| Sintoma | Causa |
|---|---|
| El widget dice **NO DATA** | La consulta corrio y devolvio cero filas |
| El widget da **TABLE_OR_VIEW_NOT_FOUND** | La tabla no existe |

**Tabla inexistente.** Las tablas de analitica solo reciben filas cuando hay
resultados, y algunos tardan: la deteccion de anomalias necesita al menos 14
dias de historia y el pronostico 21. Desde la etapa `setup` se crean vacias con
su esquema, asi que esto no deberia ocurrir; si ocurre, correr:

```bash
databricks bundle run finops_pipeline_diario -t dev --params stages=setup
```

**Cero filas.** Con pocos dias de datos es lo esperado en los paneles de
anomalias y pronostico. Verificar con:

```bash
# Ejecutar en el editor SQL, con el MISMO warehouse que usan los dashboards
scripts/diagnostico_dashboards.sql
```

Si `fct_cost_daily` tiene filas pero el dashboard sigue vacio, revisar que el
warehouse del dashboard este en el mismo workspace donde corrio el pipeline.

### `DELTA_FAILED_TO_MERGE_FIELDS` al escribir una tabla ops

Sintoma: el pipeline falla al escribir `ops_watermark`, `ops_run_log`,
`ops_data_quality` u `ops_alert_log` con *Failed to merge fields 'details'*.

Causa: la tabla se creo con una version anterior que dejaba inferir el esquema.
El runtime de Databricks infiere los diccionarios anidados como `struct`, y hoy
esas columnas se escriben como `map` — que es lo correcto, porque con `struct`
cada clave nueva en `details` cambiaria el esquema de la tabla.

Solucion: recrear la tabla. Son bitacoras operativas, no se pierde informacion
de negocio.

```sql
DROP TABLE IF EXISTS finops_dev.bronze.ops_watermark;
DROP TABLE IF EXISTS finops_dev.gold.ops_run_log;
DROP TABLE IF EXISTS finops_dev.gold.ops_data_quality;
DROP TABLE IF EXISTS finops_dev.gold.ops_alert_log;
```

El pipeline las recrea en la siguiente corrida con el esquema explicito. El
error ya viene con estas instrucciones incluidas (`SchemaMismatchError`).

### Datos de costo evidentemente erroneos

1. Revisar `ops_data_quality` de la corrida.
2. Revisar `price_missing` y `discount_rule`.
3. Si el error esta en silver/gold, reprocesar el rango afectado — no hace falta
   borrar nada, el reemplazo por rango sobrescribe.
4. Si hay que rehacer todo desde bronze, correr `finops_backfill`.

Las tablas son Delta, asi que siempre se puede inspeccionar una version anterior:

```sql
DESCRIBE HISTORY finops.gold.fct_cost_daily;
SELECT * FROM finops.gold.fct_cost_daily VERSION AS OF 42 LIMIT 100;
```
