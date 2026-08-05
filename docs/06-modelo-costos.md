# 06 — Modelo de costos

Este documento explica **exactamente** que representa cada cifra de la
plataforma, para que nadie construya un caso de negocio sobre una interpretacion
equivocada.

---

## La formula

```
costo_lista        = usage_quantity × unit_price
descuento          = costo_lista × discount_pct
costo_efectivo     = costo_lista − descuento
costo_infra_estim  = costo_efectivo × infra_factor      (opcional, desactivado)
costo_total        = costo_efectivo + costo_infra_estim
```

En el modelo: `list_cost_usd`, `discount_amount_usd`, `effective_cost_usd`,
`estimated_infra_cost_usd`, `total_cost_usd`.

---

## Que **si** mide

`system.billing.usage` registra los **DBU** consumidos por cada recurso, en
intervalos. `system.billing.list_prices` da el precio de lista por DBU de cada
SKU, con vigencia temporal. El producto de ambos es el costo de Databricks.

Es la misma base sobre la que Databricks emite su factura, asi que
`effective_cost_usd` **debe conciliar** con el consumo facturado de Databricks
(salvo por descuentos y creditos que no esten reflejados en la configuracion).

---

## Que **no** mide

### 1. El costo de la infraestructura de Azure

Cuando un job corre en un cluster de 4 nodos `Standard_D4ds_v5`, se pagan dos
cosas:

- los DBU a Databricks → **esto es lo que mide la plataforma**
- las maquinas virtuales a Azure → **esto no aparece en `system.billing.usage`**

Segun el tipo de compute, la VM puede representar entre el 30% y el 50% del costo
total. Ignorarlo subestima el gasto real; asumir un factor fijo inventa precision
que no se tiene.

**La decision tomada:** el estimador esta **desactivado por defecto**.

```yaml
pricing:
  infra_estimate:
    enabled: false                # <- por defecto
    factor_by_compute:
      ALL_PURPOSE: 0.85
      JOBS: 0.85
      DLT: 0.70
      SQL: 0.60
      SERVERLESS: 0.0             # serverless ya incluye la infra en el DBU
      OTHER: 0.60
```

Con `enabled: false`, `estimated_infra_cost_usd = 0` y
`total_cost_usd = effective_cost_usd`. Las cifras son entonces estrictamente el
costo de Databricks, y son exactas.

Si se habilita, los factores son aproximaciones burdas y hay que calibrarlos
contra la factura real de Azure antes de darles credibilidad. **Serverless usa
factor 0** porque en ese modelo la infraestructura ya viene incluida en el precio
del DBU; aplicarle un factor lo contaria dos veces.

**La alternativa correcta**, si se necesita el costo total real: exportar Azure
Cost Management a una tabla e integrarla como fuente adicional, cruzando por las
etiquetas de recurso. La plataforma no lo hace hoy; es el punto de extension
natural.

### 2. Almacenamiento, red y otros servicios de Azure

ADLS, transferencia de datos, Key Vault, VNet, Private Link — nada de eso pasa
por Databricks y no aparece aqui.

### 3. Descuentos que no esten configurados

Por defecto se usa **precio de lista**. Si la organizacion tiene un compromiso de
consumo, un descuento por volumen o creditos, hay que declararlo:

```yaml
pricing:
  discounts:
    - name: compromiso_anual
      match: {}                    # aplica a todo
      discount_pct: 0.22
    - name: promo_sql
      match: {sku_group: "SQL"}
      discount_pct: 0.30
```

**Se aplica la primera regla que coincida**, asi que las mas especificas van
primero. Las claves validas en `match` son `workspace_id`, `account_id`,
`sku_name`, `sku_group`, `billing_origin_product` y `cloud`; los valores admiten
comodines (`*SQL*`) y listas (OR).

Sin configurar descuentos, `list_cost_usd == effective_cost_usd` y las cifras
seran mayores que la factura real.

---

## Vigencia de precios

El join contra `list_prices` evalua la vigencia contra **`usage_end_time`**, no
contra `usage_date`:

```sql
usage_end_time >= price_start_time
AND (price_end_time IS NULL OR usage_end_time < price_end_time)
```

Asi, un cambio de tarifa a mitad del dia aplica solo a los intervalos posteriores.
Si un registro empata con mas de un tramo (bordes de vigencia solapados), se
conserva el tramo mas reciente.

Del struct `pricing` se toma, en orden: `effective_list.default` (refleja
promociones vigentes), luego `default`, luego `promotional.default`.

### SKU sin precio

Cuando un SKU nuevo aun no tiene precio publicado, el registro queda con
`unit_price = NULL`, `price_missing = true` y costo 0. Eso **subestima** el gasto,
por lo que existe un chequeo de calidad bloqueante:

```yaml
quality:
  checks:
    price_match_min_ratio: 0.98   # al menos el 98% de registros con precio
```

Para ver que SKU estan sin valorizar:

```sql
SELECT sku_name, billing_origin_product, COUNT(*) AS registros,
       ROUND(SUM(usage_quantity), 3) AS dbus_sin_valorizar
FROM finops.silver.slv_usage_priced
WHERE price_missing = true
  AND usage_date >= CURRENT_DATE() - INTERVAL 30 DAYS
GROUP BY sku_name, billing_origin_product
ORDER BY dbus_sin_valorizar DESC
```

---

## Clasificacion de SKU

Cada registro recibe un `sku_group` canonico:

| Grupo | Que agrupa |
|---|---|
| `ALL_PURPOSE` | Clusters interactivos, notebooks |
| `JOBS` | Compute de jobs |
| `DLT` | Delta Live Tables / Lakeflow Pipelines |
| `SQL` | SQL warehouses classic y pro |
| `SERVERLESS_SQL` / `SERVERLESS_JOBS` / `SERVERLESS_DLT` | Equivalentes serverless |
| `MODEL_SERVING` | Endpoints de inferencia, vector search |
| `AI_TRAINING` | Fine tuning, model training |
| `STORAGE_OPS` | Predictive optimization, lakehouse monitoring |
| `OTHER` | Todo lo demas |

**Prioridad:** primero `billing_origin_product` (campo controlado por Databricks,
mas confiable), y solo si no resuelve, reconocimiento por patron sobre
`sku_name`. Cuando el producto es `SQL`, `JOBS` o `DLT` y el SKU o el producto
contienen "SERVERLESS", el grupo se convierte en la variante serverless.

Los patrones se declaran una sola vez en `pricing.SKU_PATTERN_SPECS` y los
consumen tanto la version Python como la version Spark, de modo que no puedan
divergir. Si aparece un SKU nuevo que cae en `OTHER`, agregar el patron ahi y una
prueba en `tests/test_pricing.py`.

---

## Atribucion del costo

El costo se imputa por **cadena de precedencia de etiquetas**:

```
custom_tags del consumo  →  tags del cluster  →  tags del job  →  default del workspace  →  SIN_ASIGNAR
```

Las claves se normalizan quitando mayusculas, guiones, guiones bajos y espacios,
asi que `Cost-Center`, `cost_center` y `COSTCENTER` resuelven igual. Los alias
aceptados se configuran por dimension:

```yaml
tagging:
  aliases:
    cost_center: [cost_center, costcenter, cc, centro_costo]
    team: [team, equipo, squad, area]
```

Los valores tambien se canonizan (`produccion`, `prod`, `production` → `PRD`).

La columna `tag_source_<dimension>` registra **que fuente** resolvio cada
dimension, lo que permite auditar la atribucion:

```sql
SELECT team, tag_source_team, ROUND(SUM(total_cost_usd), 2) AS costo_usd
FROM finops.silver.slv_usage_priced
WHERE usage_date >= CURRENT_DATE() - INTERVAL 7 DAYS
GROUP BY team, tag_source_team
```

### Entidad de consumo

Un registro de consumo se imputa a una entidad, con esta prioridad:

```
job → pipeline → warehouse → endpoint → pool → cluster → notebook → app → metastore
```

Una tarea de job que corre en un job cluster se imputa al **job**: es la unidad
de negocio. El `cluster_id` queda disponible como columna aparte para analizar la
configuracion.

---

## Chargeback

El costo del periodo se descompone en tres bloques:

- **directo** — imputable a una unidad por sus etiquetas
- **compartido** — plataforma, gobierno, monitoreo (entidades que hacen match con
  `shared_entity_patterns`)
- **no atribuido** — sin etiqueta resoluble

Los dos ultimos se reparten entre las unidades segun la estrategia configurada:

| Estrategia | Efecto |
|---|---|
| `proportional` | En proporcion al costo directo de cada unidad (por defecto) |
| `even` | Partes iguales |
| `none` | Se deja como una unidad propia (`COSTO_COMPARTIDO` / `SIN_ASIGNAR`) |

`proportional` es el default porque castiga a quien mas consume, que suele ser
quien mas se beneficia de la plataforma compartida. `none` es util cuando se
quiere hacer visible el problema del gasto sin atribuir en lugar de diluirlo.

**Invariante verificado:** la suma del chargeback de todas las unidades equivale
al costo total del periodo × (1 + `overhead_pct`). El pipeline lo comprueba con
`chargeback.reconcile()` y deja una advertencia en el log si no cuadra.

---

## Conciliar con la factura de Databricks

```sql
SELECT DATE_FORMAT(usage_date, 'yyyy-MM') AS mes,
       ROUND(SUM(list_cost_usd), 2)      AS precio_lista_usd,
       ROUND(SUM(discount_amount_usd), 2) AS descuento_usd,
       ROUND(SUM(effective_cost_usd), 2)  AS facturable_usd
FROM finops.gold.fct_cost_daily
GROUP BY mes
ORDER BY mes DESC
```

`facturable_usd` es lo que debe compararse contra la factura de Databricks.
**No usar `total_cost_usd`** para conciliar si el estimador de infraestructura
esta habilitado: esa columna incluye una estimacion que no esta en ninguna
factura.

Si hay diferencia, revisar en este orden:

1. `price_missing` — SKU sin precio publicado
2. `discount_rule` — descuentos mal configurados o ausentes
3. `ingestion.exclude_workspace_ids` — workspaces excluidos deliberadamente
4. La ventana de datos: `SELECT MAX(usage_date) FROM finops.silver.slv_usage_priced`
5. Creditos y ajustes que Databricks aplica fuera de `system.billing.usage`
