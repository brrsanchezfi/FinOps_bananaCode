# 04 — Dashboards

Cuatro dashboards Lakeview, versionados en `dashboards/*.lvdash.json` y
desplegados por el bundle. Los tres primeros leen el modelo gold; el cuarto lee
las system tables en vivo.

---

## 1. Vista ejecutiva — `finops_ejecutivo`

**Para:** direccion, gerencia de tecnologia, revision semanal de costo.

| Bloque | Contenido |
|---|---|
| Contadores | Costo del ultimo dia, acumulado del mes, promedio 7 dias, variacion dia/dia y semana/semana, cobertura de atribucion |
| Costo diario (90 dias) | Serie con medias moviles de 7 y 28 dias |
| Mezcla por tipo de compute | Distribucion entre all-purpose, jobs, DLT, SQL, serverless |
| Costo por equipo | Top 15 de los ultimos 30 dias |
| Ejecutado vs pronostico | Serie combinada: 60 dias de historia + horizonte proyectado |
| Presupuestos | Estado de cada presupuesto: consumido, proyectado, dias restantes, gasto diario permitido |
| Anomalias | Desviaciones de severidad alta/critica de los ultimos 14 dias |
| Top recursos | 25 recursos de mayor costo con su atribucion |

Fuentes: `fct_kpi_daily`, `fct_cost_daily`, `fct_budget_status`,
`fct_cost_forecast`, `fct_cost_anomaly`.

---

## 2. Costos y chargeback — `finops_costos`

**Para:** FinOps, controlaria, lideres de area.

| Bloque | Contenido |
|---|---|
| Chargeback del mes | Imputacion por unidad: directo, compartido asignado, no atribuido asignado, recargo, total |
| Evolucion mensual | Doce meses de chargeback por unidad |
| Costo mensual por tipo | Composicion del gasto a lo largo del ano |
| Costo por workspace | Distribucion entre workspaces de la cuenta |
| Cobertura de etiquetado | Serie de 60 dias por dimension |
| Jobs de mayor costo | 30 jobs con ejecuciones, fallos, duracion y costo por ejecucion |
| SQL warehouses | Costo, consultas, horas activas, costo por consulta, auto-stop |
| Detalle de consumo | 500 filas de los ultimos 7 dias, grano recurso × dia |

Fuentes: `fct_chargeback_monthly`, `agg_cost_monthly`, `fct_cost_daily`,
`fct_tag_coverage_daily`, `fct_job_run_cost`, `fct_warehouse_cost_daily`,
`dim_workspace`.

---

## 3. Optimizacion y gobierno — `finops_optimizacion`

**Para:** plataforma, ingenieria de datos, gobierno.

| Bloque | Contenido |
|---|---|
| Contadores | Ahorro mensual estimado, ahorro de alta confianza, recomendaciones abiertas, recursos involucrados |
| Ahorro por tipo de hallazgo | Barras por `rule_id` |
| Indicadores de eficiencia | Series de % serverless, % all-purpose, cobertura, costo por DBU |
| Recomendaciones | Tabla priorizada con **metodo de estimacion** y **confianza** visibles |
| Gasto sin atribucion | Recursos que generan costo sin etiquetas resolubles |
| Configuracion de clusters | Auto-terminacion, autoescalado, workers, DBR, dias sin uso |
| Desperdicio por fallos | Jobs con ejecuciones fallidas y su costo |
| Salud de la plataforma | Corridas del pipeline y alertas emitidas |

Fuentes: `fct_recommendation`, `fct_kpi_daily`, `fct_cost_daily`,
`fct_job_run_cost`, `dim_entity`, `ops_run_log`, `ops_alert_log`.

> Las columnas `confidence` y `estimation_method` estan en la tabla de
> recomendaciones a proposito: quien lee el dashboard debe poder juzgar cuanto
> peso darle a cada cifra de ahorro antes de comprometerla.

---

## 4. Gobierno de etiquetado — `finops_etiquetado`

**Para:** plataforma, gobierno, quien administra las cluster policies.

**Es el unico tablero que no depende de la ejecucion del pipeline.** Sus datasets
consultan las vistas `vw_*_live`, que leen `system.billing.usage` directamente,
asi que muestra el estado actual del etiquetado cada vez que se abre. Esa fue la
razon de disenarlo asi: el etiquetado se corrige cambiando una policy, y quien la
cambia necesita ver el efecto al recargar, no al dia siguiente.

| Bloque | Contenido |
|---|---|
| Contadores | Costo total 30 dias, gasto sin ninguna etiqueta, recursos con consumo |
| Cobertura por dimension | Barras del % cubierto de cada dimension canonica |
| Evolucion | Serie de 60 dias de la cobertura, una linea por dimension |
| Gasto sin atribuir por tipo | Donde esta la brecha: jobs, clusters, warehouses, serverless |
| Claves no reconocidas | Etiquetas en uso que la configuracion no mapea a ninguna dimension |
| Inventario | Todas las claves en uso, su dimension y el costo que representan |
| Recursos sin atribucion | Lista accionable de recursos con costo y sin ninguna dimension resoluble |

Fuentes: `vw_tag_coverage_live`, `vw_tag_inventory_live`, `vw_untagged_spend_live`,
`vw_usage_live`.

> **Los importes son a precio de lista.** Los descuentos negociados viven en
> `conf/*.yml` y los aplica la capa silver; las vistas no. Si hay descuento
> configurado, las cifras de este tablero seran mayores que las de los otros. Por
> eso las columnas se llaman `list_cost_usd` y no `total_cost_usd`: sirven para
> comparar entre si, no como cifra de facturacion.

> **Permisos.** Quien consulte las vistas necesita `SELECT` sobre
> `system.billing`, que normalmente solo tiene el service principal. Para que lo
> vean los analistas, publicar el tablero con credenciales embebidas.

Las vistas se crean en la etapa `setup` del pipeline (`finops.views.ensure_views`).
Es la unica dependencia con el pipeline, y es de DDL: los datos siempre son
actuales. Si una vista falla al crearse — casi siempre por permisos — se registra
una advertencia y el pipeline continua: perder el tablero de etiquetado no
justifica tumbar la produccion de las cifras de costo.

El SQL de las vistas se construye desde `conf/`: agregar una dimension a
`tagging.dimensions` la incorpora al tablero en el siguiente `setup`, sin tocar
codigo.

---

## Como se construyen y despliegan

Los dashboards **no se editan a mano**. La unica fuente de verdad es
`scripts/dashboards.py`, donde el SQL y el layout viven como codigo Python
legible y revisable en un diff.

```bash
python scripts/dashboards.py generate   # reconstruye dashboards/*.lvdash.json
python scripts/dashboards.py check      # verifica que esten al dia
```

`generate` escribe **tres archivos versionados** en `dashboards/`, uno por
tablero. Los nombres de tabla salen de la configuracion: en el generador se
escriben como marcadores de la clave logica del registro,

```sql
FROM {{fct_cost_daily}}
```

y `generate` los sustituye por el nombre completamente calificado. **Ningun
catalogo se escribe a mano.**

`resources/dashboards.yml` apunta directamente a `dashboards/`, asi que
**`databricks bundle deploy` funciona sobre el repositorio**: no hay paso de
build previo.

### Por que basta un solo juego de archivos

Un dashboard Lakeview lleva el SQL embebido con nombres de tabla **literales**,
asi que necesita tantas versiones como destinos distintos haya. Como los tres
entornos comparten catalogo y schemas ([ADR 0005](adr/0005-un-solo-catalogo.md)),
el destino es uno solo.

Si algun entorno volviera a apuntar a otro sitio, `generate` y `check` lo
detectan y avisan que hay que generar por entorno otra vez; una prueba
(`test_los_tres_entornos_resuelven_a_las_mismas_tablas`) lo cubre.

> **Por que se versionan archivos generados.** El CLI de Databricks excluye del
> arbol del bundle todo lo que git ignora. Un dashboard generado en una ruta
> ignorada (`.build/`, `dist/`, …) produce al desplegar:
> `failed to read serialized dashboard from file_path ...: no such file or directory`.
> Ver [ADR 0004](adr/0004-dashboards-con-marcadores-de-tabla.md).

### Guardas automaticas

`tests/test_catalog.py` verifica:

- Que no quede ningun marcador `{{...}}` sin sustituir.
- Que toda tabla referenciada exista en el registro de `finops.catalog`.
- Que ningun catalogo este escrito a mano en el JSON.
- Que los tres entornos sigan resolviendo a las mismas tablas (la condicion que
  permite un solo juego de archivos).
- Que los archivos versionados coincidan **byte a byte** con lo que produce el
  generador (falla si alguien edito un JSON a mano).

CI corre lo mismo via `python scripts/dashboards.py check`.

---

## Modificar un dashboard

1. Editar la funcion correspondiente en `scripts/dashboards.py`
   (`dashboard_ejecutivo`, `dashboard_costos`, `dashboard_optimizacion`).
2. Regenerar y verificar:

```bash
python scripts/dashboards.py generate
pytest tests/test_catalog.py -q
```

3. Commitear los tres archivos junto con el cambio del generador.
4. Desplegar a `dev` y revisar visualmente antes de promocionar.

### Agregar un widget

```python
# Un dataset nuevo
dataset(
    "mi_consulta", "Descripcion legible",
    """
SELECT dimension, SUM(total_cost_usd) AS costo_usd
FROM {{fct_cost_daily}}
WHERE usage_date >= CURRENT_DATE() - INTERVAL 30 DAYS
GROUP BY dimension
    """,
),

# Y su widget en el layout
chart("mi_grafico", "mi_consulta", "bar", "Titulo visible",
      x_campo="dimension", x_escala="categorical", x_titulo="Dimension",
      y_campo="costo_usd", y_titulo="USD",
      x=0, y=30, w=3, h=7),
```

La grilla real de Lakeview tiene **12 columnas**, pero el layout de este archivo
se escribe sobre una grilla logica de **6** y `_pos` la escala. `x` e `y` son
coordenadas, `w` y `h` el tamano. Los constructores disponibles son `markdown`, `counter`, `chart`
(tipos `line`, `bar`, `area`, `pie`, `scatter`) y `table`.

Si referencias una tabla nueva, primero declarala en `src/finops/catalog.py`: el
generador falla si un marcador no corresponde a una tabla del registro.

---

## La forma de un widget

El esquema de Lakeview no esta documentado al detalle, y el generador se
construyo primero contra la documentacion publica: varias suposiciones
resultaron equivocadas y produjeron widgets vacios. La forma que aparece abajo
esta **verificada contra un widget construido en la UI del workspace**.

```jsonc
{
  "widget": {
    "name": "ahorro_total",
    "queries": [
      {
        "name": "main_query",                    // referenciado por spec.data
        "query": {
          "datasetName": "ahorro_por_regla",
          "fields": [
            {"name": "sum(ahorro_mensual_usd)",  // el alias, no la columna
             "expression": "SUM(`ahorro_mensual_usd`)"}
          ],
          "disaggregated": false                 // false => hay agregacion
        }
      }
    ],
    "spec": {
      "version": 2,                              // 2 counter y tabla, 3 graficos
      "widgetType": "counter",
      "encodings": {"value": {"fieldName": "sum(ahorro_mensual_usd)"}},
      "frame": {"title": "Ahorro mensual estimado", "showTitle": true},
      "data": {"queryName": "main_query"}        // SIN esto el widget sale vacio
    }
  },
  "position": {"x": 0, "y": 2, "width": 4, "height": 3}
}
```

Los errores que costaron varias iteraciones, por si reaparecen:

| Sintoma | Causa |
|---|---|
| "Select fields to visualize" | Falta `spec.data.queryName`, o falta `pageType`/`layoutVersion` en la pagina |
| "Visualization has no fields selected" (tabla) | `version: 1`, o columnas con metadatos del formato viejo |
| Widget de texto en blanco | Se uso `textbox_spec`; el esquema espera `multilineTextboxSpec.lines` |
| Contador vacio | El campo no estaba agregado teniendo `disaggregated: false` |
| Torta que no renderiza | Se declararon ejes `x`/`y`; un pie usa `angle` y `color` |
| Tablero confinado a la izquierda | La grilla es de 12 columnas, no de 6 |

### La forma de una tabla

Una tabla **no** sigue el mismo patron que los graficos. Verificado contra un
widget reparado en la UI:

```jsonc
"spec": {
  "version": 2,                       // con 1 el widget sale vacio
  "widgetType": "table",
  "encodings": {
    "columns": [
      {"fieldName": "budget_name"},   // SOLO fieldName
      {"fieldName": "status"}
    ]
  },
  "data": {"queryName": "main_query"}
}
```

Los metadatos por columna (`displayName`, `type`, `displayAs`, `booleanValues`,
`alignContent`, `order`) pertenecen al formato **version 1**. Incluirlos en la
version 2 invalida la lista completa de columnas y produce
"Visualization has no fields selected".

La consecuencia practica: **los encabezados muestran el nombre crudo de la
columna**, no una etiqueta legible. El constructor `table` conserva la etiqueta
en su firma para documentar la intencion, pero hoy no se emite. Si hace falta un
encabezado en espanol, la via es aliasear la columna en el SQL del dataset.

Todos estan cubiertos por pruebas en `tests/test_catalog.py`, asi que no pueden
volver a colarse en un despliegue.

### Como obtener una referencia nueva

Si hace falta un tipo de widget que el generador no cubre, la via fiable es
construirlo en la UI, exportarlo y copiar su forma:

```bash
databricks bundle generate dashboard --existing-path "/Workspace/Users/<usuario>/<dashboard>" -t dev
```

Nunca al reves: editar el JSON a mano se pierde en el siguiente `generate`.

---

## Publicacion y refresco

Dos cosas que sorprenden la primera vez, y que no dependen del codigo:

**1. Lo que despliega el bundle es el borrador.** Un dashboard Lakeview tiene una
version *borrador* y una *publicada*. `databricks bundle deploy` actualiza el
borrador; quien abre el enlace publicado sigue viendo la version anterior hasta
que alguien pulsa **Publish** en la UI.

Si acabas de redesplegar y no ves los cambios, abre el dashboard y publicalo.

**2. Lakeview no refresca solo.** Ejecuta sus consultas al abrirlo, y ademas
sirve resultados en cache. Para que se actualice sin intervencion hay que
definirle un **Schedule** en la propia UI del dashboard (boton *Schedule*),
tipicamente poco despues de la hora del pipeline diario (07:00 America/Bogota),
por ejemplo a las 08:00.

El recurso `dashboards` del bundle no expone el horario de refresco, asi que ese
paso es manual y hay que repetirlo si el dashboard se recrea desde cero.

> **No edites el dashboard en la UI para cambiar su contenido.** El siguiente
> `deploy` sobrescribe el borrador y pierdes el cambio. Publicar y programar si
> son acciones de UI; el contenido se cambia en `scripts/dashboards.py`.

### Comprobar que se desplegaron

```bash
databricks bundle summary -t dev
```

Deben aparecer los tres dashboards. En el workspace quedan bajo
`${workspace.root_path}/dashboards`.

Si el deploy no los creo, la causa casi siempre es `warehouse_id` vacio en el
target correspondiente de `databricks.yml`.

---

## Consumidores

Para que un area pueda ver los dashboards:

```sql
GRANT USE CATALOG ON CATALOG finops TO `analistas-finops`;
GRANT USE SCHEMA, SELECT ON SCHEMA finops.gold TO `analistas-finops`;
```

Y compartir el dashboard desde la UI de Databricks con permiso `CAN_VIEW`. El
SQL warehouse (`var.warehouse_id`) tambien debe ser accesible para ellos, o
publicar el dashboard con credenciales embebidas.
