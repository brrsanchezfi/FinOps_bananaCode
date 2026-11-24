# FinOps bananaCode

Plataforma de analitica **FinOps para Databricks**: ingiere las *system tables* de
Unity Catalog, valoriza el consumo, lo atribuye a equipos y centros de costo, y
entrega dashboards, presupuestos, deteccion de anomalias, pronostico,
recomendaciones de ahorro y alertamiento — todo empaquetado como un
**Databricks Asset Bundle** desplegable en `dev`, `qa` y `prd`.

```
system tables  ->  bronze  ->  silver  ->  gold  ->  dashboards + alertas
(billing, compute,   copia    valorizado   modelo     Lakeview, Teams/Slack
 lakeflow, query)  incremental y atribuido dimensional
```

---

## Que resuelve

| Pregunta | Donde se responde |
|---|---|
| Cuanto gastamos y como evoluciona | `fct_kpi_daily`, dashboard **Vista ejecutiva** |
| Quien gasta (equipo, centro de costo, proyecto) | `fct_cost_daily`, `fct_chargeback_monthly` |
| Vamos a exceder el presupuesto | `fct_budget_status` + alerta `FORECAST_OVERRUN` |
| Por que subio el costo ayer | `fct_cost_anomaly` + notebook `90_exploracion` |
| Donde podemos ahorrar | `fct_recommendation`, dashboard **Optimizacion** |
| Cuanto gasto no tiene responsable | `fct_tag_coverage_daily`, dashboard **Gobierno de etiquetado** |
| Que etiquetas se usan de verdad, ahora mismo | `vw_tag_inventory_live` (en vivo, sin pipeline) |
| Esta sano el pipeline | `ops_run_log`, `ops_data_quality`, `ops_alert_log` |

## Principio de diseno

**La logica de negocio vive en modulos de Python, no en los notebooks.** Los
notebooks son orquestadores delgados que leen parametros y llaman al paquete
`finops`. Todo lo que decide algo — clasificar un SKU, resolver una etiqueta,
detectar una anomalia, evaluar un presupuesto, estimar un ahorro — es una funcion
pura sobre tipos nativos de Python, cubierta por pruebas que **corren sin cluster**.

Los adaptadores de Spark solo leen, escriben y mapean.

```bash
python -m pytest -q   # 440+ pruebas, sin Databricks, en segundos
```

---

## Inicio rapido

### 1. Entorno local

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
pytest -q
python -m finops.cli validate
python -m finops.cli plan
```

En Windows PowerShell el activador es `.venv\Scripts\Activate.ps1`.

### 2. Configuracion de la instalacion

El repositorio viaja **neutro**: no sabe cuales son tus workspaces, tus unidades
de negocio ni tus presupuestos. Eso se declara una vez, en archivos que git
ignora:

```bash
cp conf/local.example.yml         conf/local.yml
cp conf/budgets.local.example.yml conf/budgets.local.yml
```

`conf/local.yml` se fusiona **encima** de `base.yml` y `<env>.yml`, asi que solo
hay que escribir lo que difiera. `conf/budgets.local.yml`, si existe, reemplaza
por completo a `conf/budgets.yml`. Lo minimo que hay que ajustar es el mapa
`tagging.workspace_defaults`, que asigna el ambiente por workspace y rescata
todo el consumo que ninguna policy etiqueta.

Ambos archivos se suben al workspace via `sync.include` en `databricks.yml`.

### 3. Autenticacion con Databricks

```bash
databricks auth login --host https://adb-XXXXXXXXXXXX.N.azuredatabricks.net -p finops
```

### 4. Despliegue

```bash
export BUNDLE_VAR_warehouse_id=$(databricks warehouses list -p finops -o json | head -1)
bash scripts/deploy.sh --profile finops
```

En Windows:

```powershell
pwsh scripts/deploy.ps1 -DatabricksProfile finops
```

El script encadena: validar configuracion → verificar dashboards → validar
bundle → desplegar. Ver [docs/03-despliegue.md](docs/03-despliegue.md) para los
prerrequisitos (permisos sobre `system.*`, `warehouse_id`, secretos de webhook).

### 5. Primera carga

```bash
databricks bundle run finops_backfill -t finops -p finops
```

Despues el pipeline diario queda programado, y puede lanzarse manualmente:

```bash
databricks bundle run finops_pipeline_diario -t finops
```

---

## Estructura del repositorio

```
├── databricks.yml              Bundle: un target (finops), variables, artefactos
├── resources/
│   ├── jobs.yml                Pipeline diario, alertamiento, backfill
│   └── dashboards.yml          Los cuatro dashboards Lakeview
├── conf/
│   ├── base.yml                Configuracion comun (fuentes, umbrales, reglas)
│   ├── budgets.yml             Presupuestos de EJEMPLO y reglas de chargeback
│   ├── local.example.yml       Plantilla del overlay de la instalacion
│   ├── budgets.local.example.yml   Plantilla de los presupuestos reales
│   └── local.yml, budgets.local.yml   Tu instalacion (ignorados por git)
├── src/finops/                 TODA la logica de negocio
│   ├── config.py               Carga, fusion y validacion de configuracion
│   ├── catalog.py              Registro central de tablas del modelo
│   ├── views.py                Vistas en vivo de gobierno de etiquetado
│   ├── governance.py           Puente con DKOps: sesion, logging y escritura
│   ├── contracts/tables/       Contrato DKOps de cada tabla (bronze, silver, gold)
│   ├── spark_utils.py          Lectura tolerante, bootstrap y utilidades Spark
│   ├── pipeline.py             Orquestacion por etapas
│   ├── notebook.py             Puente notebooks <-> paquete
│   ├── cli.py                  CLI: validate / plan / run
│   ├── ingestion/              system tables -> bronze
│   ├── transform/              pricing, tags, silver, gold
│   ├── analytics/              anomalias, pronostico, presupuestos,
│   │                           optimizacion, chargeback
│   ├── quality/                chequeos de calidad
│   └── alerting/               reglas, formateo, deduplicacion, despacho
├── notebooks/
│   ├── 00_orquestador.py       Pipeline completo (orquestador general)
│   ├── 10_etapa.py             Ejecutor de etapas (tarea de los jobs)
│   └── 90_exploracion.py       Consultas ad-hoc
├── dashboards/*.lvdash.json    Dashboards generados y versionados
├── scripts/
│   ├── dashboards.py           generador de los dashboards
│   └── deploy.sh / deploy.ps1  Despliegue de extremo a extremo
├── tests/                      Suite completa sin dependencia de Spark
└── docs/                       Documentacion (indice abajo)
```

---

## Documentacion

| Documento | Contenido |
|---|---|
| [01 — Arquitectura](docs/01-arquitectura.md) | Capas, flujo de datos, decisiones de diseno |
| [02 — Modelo de datos](docs/02-modelo-datos.md) | Cada tabla, su grano y sus columnas |
| [03 — Despliegue](docs/03-despliegue.md) | Prerrequisitos, permisos, bundle, CI/CD |
| [04 — Dashboards](docs/04-dashboards.md) | Los cuatro dashboards y como extenderlos |
| [05 — Alertas](docs/05-alertas.md) | Reglas, severidades, canales, deduplicacion |
| [06 — Modelo de costos](docs/06-modelo-costos.md) | Como se calcula el costo y que **no** incluye |
| [07 — Runbook](docs/07-runbook.md) | Operacion diaria, diagnostico, incidentes |
| [08 — Desarrollo](docs/08-desarrollo.md) | Convenciones, pruebas, como agregar una regla |
| [ADR](docs/adr/) | Registro de decisiones de arquitectura |

---

## Advertencias importantes

1. **El costo es el de Databricks (DBU), no la factura completa del cloud.**
   `system.billing.usage` no incluye el costo de las maquinas virtuales de Azure.
   El estimador de infraestructura (`pricing.infra_estimate`) esta **desactivado
   por defecto** y es una aproximacion. Ver [docs/06-modelo-costos.md](docs/06-modelo-costos.md).

2. **Los ahorros de las recomendaciones son estimaciones.** Cada recomendacion
   declara su `estimation_method` y su nivel de `confidence`. No comprometer
   cifras de ahorro sin validar el caso concreto.

3. **Los descuentos negociados deben configurarse.** Por defecto el modelo usa
   precio de lista (`discount_pct: 0.0` en `conf/local.yml`).

4. **Los dashboards se generan, no se editan a mano.** Viven como codigo en
   `scripts/dashboards.py` y se versionan ya resueltos en `dashboards/`. Tras
   cambiarlos: `python scripts/dashboards.py generate` y commitear.
   `databricks bundle deploy` no requiere ningun paso previo.

   Unica excepcion hoy: `finops_ejecutivo`, construido en la UI y declarado en
   `MANTENIDOS_A_MANO`. Su fuente de verdad es el JSON versionado; `generate` y
   `check` lo dejan en paz hasta que ese trabajo se backportee al constructor.

5. **La configuracion del cliente nunca se commitea.** Workspaces, unidades de
   negocio, montos y responsables van en `conf/local.yml` y
   `conf/budgets.local.yml`. `tests/test_neutralidad.py` falla si algo de eso
   aparece en un archivo versionado.

---

## Una instalacion, todos los ambientes

Se despliega **una sola instalacion** (target `finops`). El modelo lee
`system.billing.usage`, que cubre todos los workspaces de la cuenta, asi que esa
instalacion ya muestra el consumo de dev, qa y prd del cliente. Desplegar una
por ambiente produciria las mismas cifras varias veces
(ver [ADR 0006](docs/adr/0006-una-sola-instalacion.md)).

Ojo con la palabra "ambiente", que nombra dos cosas distintas:

- **El ambiente del tablero** es donde corre *nuestro* pipeline. Hay uno.
- **El ambiente de un recurso del cliente** (DEV / QA / PRD) es un dato que el
  tablero *mide*: la dimension `environment`, que sale de las etiquetas y de
  `tagging.workspace_defaults` en `conf/local.yml`.

La configuracion del producto vive en `conf/base.yml`. Lo propio de cada
instalacion -- incluido un perfil mas laxo mientras se implanta, con calidad no
bloqueante o logs en DEBUG -- va en `conf/local.yml`.

**El workspace no esta en el repositorio.** Sale del perfil del CLI
(`databricks auth login -p <perfil>`) o de `DATABRICKS_HOST`, porque esto se
despliega sobre la cuenta de cada cliente. Lo mismo el `warehouse_id` de los
dashboards, que se pasa con `--var` o `BUNDLE_VAR_warehouse_id`.

---

## Licencia

Propietario — DATAKNOW S.A.S.
