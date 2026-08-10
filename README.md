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
| Cuanto gasto no tiene responsable | `fct_tag_coverage_daily` |
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
python -m finops.cli validate --env dev
python -m finops.cli plan --env dev
```

En Windows PowerShell el activador es `.venv\Scripts\Activate.ps1`.

### 2. Autenticacion con Databricks

```bash
databricks auth login --profile dev
```

### 3. Despliegue

```bash
bash scripts/deploy.sh dev
```

En Windows:

```powershell
pwsh scripts/deploy.ps1 -Env dev
```

El script encadena: validar configuracion → verificar dashboards → validar
bundle → desplegar. Ver [docs/03-despliegue.md](docs/03-despliegue.md) para los
prerrequisitos (permisos sobre `system.*`, `warehouse_id`, secretos de webhook).

### 4. Primera carga

```bash
databricks bundle run finops_backfill -t dev
```

Despues el pipeline diario queda programado, y puede lanzarse manualmente:

```bash
databricks bundle run finops_pipeline_diario -t dev
```

---

## Estructura del repositorio

```
├── databricks.yml              Bundle: targets dev/qa/prd, variables, artefactos
├── resources/
│   ├── jobs.yml                Pipeline diario, alertamiento, backfill
│   └── dashboards.yml          Los tres dashboards Lakeview
├── conf/
│   ├── base.yml                Configuracion comun (fuentes, umbrales, reglas)
│   ├── dev.yml / qa.yml / prd.yml   Overlays por entorno
│   └── budgets.yml             Presupuestos y reglas de chargeback
├── src/finops/                 TODA la logica de negocio
│   ├── config.py               Carga, fusion y validacion de configuracion
│   ├── catalog.py              Registro central de tablas del modelo
│   ├── spark_utils.py          Unico punto de contacto con Spark/Delta
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
│   ├── 10_etapa.py             Ejecutor de una etapa (tareas del job)
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
| [04 — Dashboards](docs/04-dashboards.md) | Los tres dashboards y como extenderlos |
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
   precio de lista (`discount_pct: 0.0` en `conf/prd.yml`).

4. **Los dashboards se generan, no se editan a mano.** Viven como codigo en
   `scripts/dashboards.py` y se versionan ya resueltos en `dashboards/`. Tras
   cambiarlos: `python scripts/dashboards.py generate` y commitear.
   `databricks bundle deploy` no requiere ningun paso previo.

---

## Entornos

Los tres comparten el catalogo **`finops`**: el modelo describe el consumo de la
cuenta, no de un ambiente, asi que los tres producen las mismas cifras. Lo que
los separa es donde corre el codigo y con que umbrales
(ver [ADR 0005](docs/adr/0005-un-solo-catalogo.md)).

| Entorno | Workspace | Schedule | Alertas |
|---|---|---|---|
| `dev` | `adb-4198581253243445.5` | pausado | solo tabla |
| `qa`  | `adb-2370424844216896.16` | activo | tabla |
| `prd` | `adb-7042033821150253.13` | activo | tabla + Teams |

---

## Licencia

Propietario — DATAKNOW S.A.S.
