# 04 — Dashboards

Tres dashboards Lakeview, versionados en `dashboards/*.lvdash.json` y desplegados
por el bundle.

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

## Como se construyen y despliegan

Los dashboards **no se editan a mano**. La unica fuente de verdad es
`scripts/dashboards.py`, donde el SQL y el layout viven como codigo Python
legible y revisable en un diff.

```bash
python scripts/dashboards.py generate          # los tres entornos
python scripts/dashboards.py generate --env dev
python scripts/dashboards.py check             # verifica que esten al dia
```

`generate` escribe **nueve archivos versionados**:

```
dashboards/
├── dev/   finops_ejecutivo.lvdash.json  finops_costos...  finops_optimizacion...
├── qa/    (los mismos, con el catalogo finops_qa)
└── prd/   (los mismos, con el catalogo finops)
```

`resources/dashboards.yml` apunta a `dashboards/${bundle.target}/`, asi que
**`databricks bundle deploy` funciona directamente sobre el repositorio**: no hay
paso de build previo.

### Por que hay un archivo por entorno

Un dashboard Lakeview lleva el SQL embebido con nombres de tabla **literales**.
No existe un JSON unico valido para `finops_dev`, `finops_qa` y `finops`.

En `scripts/dashboards.py` las tablas se escriben como marcadores de la clave
logica del registro:

```sql
FROM {{fct_cost_daily}}
```

y `generate` los sustituye por el nombre completamente calificado de cada
entorno. El repositorio sigue siendo promocionable —el codigo fuente es
env-agnostico— y ademas el artefacto desplegable esta listo.

> **Por que se versionan archivos generados.** El CLI de Databricks excluye del
> arbol del bundle todo lo que git ignora. Un dashboard generado en una ruta
> ignorada (`.build/`, `dist/`, …) produce al desplegar:
> `failed to read serialized dashboard from file_path ...: no such file or directory`.
> Ver [ADR 0004](adr/0004-dashboards-con-marcadores-de-tabla.md).

### Guardas automaticas

`tests/test_catalog.py` verifica, para cada entorno:

- Que no quede ningun marcador `{{...}}` sin sustituir.
- Que toda tabla referenciada exista en el registro de `finops.catalog`.
- Que un dashboard de un entorno no consulte el catalogo de otro.
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

3. Commitear los nueve archivos junto con el cambio del generador.
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

La grilla de Lakeview tiene **6 columnas**. `x` e `y` son coordenadas, `w` y `h`
el tamano. Los constructores disponibles son `markdown`, `counter`, `chart`
(tipos `line`, `bar`, `area`, `pie`, `scatter`) y `table`.

Si referencias una tabla nueva, primero declarala en `src/finops/catalog.py`: el
generador falla si un marcador no corresponde a una tabla del registro.

---

## Nota sobre la primera revision

Los JSON fueron construidos siguiendo el esquema documentado de Lakeview, pero
sin un workspace disponible para validarlos visualmente. Tras el primer deploy
conviene abrirlos y revisar el detalle de cada widget; si algun encoding necesita
ajuste, la forma correcta de arreglarlo es **editar `scripts/dashboards.py` y
regenerar**, no editar el JSON, para que el cambio quede versionado y CI no lo
revierta.

Alternativamente, se puede ajustar en la UI, exportar el dashboard y trasladar
las diferencias al generador.

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
