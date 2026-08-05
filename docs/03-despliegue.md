# 03 — Despliegue

## Prerrequisitos

### Herramientas

| Herramienta | Version | Verificacion |
|---|---|---|
| Databricks CLI | 0.230+ | `databricks --version` |
| Python | 3.10+ | `python --version` |
| Git | cualquiera | `git --version` |

```bash
pip install -e ".[dev]" build
```

`build` es necesario para que el bundle construya el wheel (`artifacts` en
`databricks.yml`).

### Permisos en Databricks

El principal que ejecuta el pipeline necesita:

**1. Lectura de las system tables.** En Unity Catalog las system tables se
habilitan por schema a nivel de metastore. Con un admin de cuenta:

```sql
-- Verificar que los schemas de sistema esten habilitados
SELECT * FROM system.information_schema.schemata WHERE catalog_name = 'system';

-- Conceder lectura al principal del pipeline
GRANT USE CATALOG ON CATALOG system TO `sp-finops`;
GRANT USE SCHEMA, SELECT ON SCHEMA system.billing  TO `sp-finops`;
GRANT USE SCHEMA, SELECT ON SCHEMA system.compute  TO `sp-finops`;
GRANT USE SCHEMA, SELECT ON SCHEMA system.lakeflow TO `sp-finops`;
GRANT USE SCHEMA, SELECT ON SCHEMA system.query    TO `sp-finops`;
GRANT USE SCHEMA, SELECT ON SCHEMA system.access   TO `sp-finops`;
```

`system.billing` y `system.compute` son obligatorios. Los demas son opcionales:
sin ellos el pipeline corre igual, con menos enriquecimiento (ver
`sources.*.optional` en `conf/base.yml`).

Si un schema de sistema no esta habilitado, se activa con la API de metastore:

```bash
databricks api patch /api/2.0/unity-catalog/metastores/<metastore-id>/systemschemas/lakeflow \
  --json '{"enable": true}'
```

**2. Creacion del catalogo destino.**

```sql
CREATE CATALOG IF NOT EXISTS finops;
GRANT CREATE SCHEMA, USE CATALOG ON CATALOG finops TO `sp-finops`;
```

O bien conceder `CREATE CATALOG` al principal y dejar que la etapa `setup` lo cree.

**3. Lectura para los consumidores de los dashboards.**

```sql
GRANT USE CATALOG ON CATALOG finops TO `analistas-finops`;
GRANT USE SCHEMA, SELECT ON SCHEMA finops.gold TO `analistas-finops`;
```

### SQL warehouse para los dashboards

```bash
databricks warehouses list -p prd --output json
```

Copiar el `id` al bloque `variables.warehouse_id` del target correspondiente en
`databricks.yml`. Sin el, el deploy de los dashboards falla.

### Secretos para el alertamiento (opcional)

```bash
databricks secrets create-scope finops -p prd
databricks secrets put-secret finops teams_webhook_url -p prd
```

Luego poner `enabled: true` en el canal correspondiente de `conf/prd.yml`. Si el
secreto no existe, el canal se omite con advertencia y las alertas igual quedan
registradas en `ops_alert_log`.

---

## Configuracion previa al primer despliegue

Editar antes de desplegar a `qa` o `prd`:

| Archivo | Que ajustar |
|---|---|
| `databricks.yml` | `warehouse_id` de cada target; `run_as.user_name` → service principal; `notification_email` |
| `conf/prd.yml` | `pricing.discounts` con el descuento negociado real |
| `conf/budgets.yml` | Presupuestos reales por equipo / centro de costo |
| `conf/base.yml` | `tagging.aliases` con las convenciones de etiquetado de la organizacion |

Validar sin desplegar nada:

```bash
python -m finops.cli validate --env prd --show
python -m finops.cli plan --env prd
```

---

## Despliegue

### Camino recomendado

```bash
bash scripts/deploy.sh prd
```

```powershell
pwsh scripts/deploy.ps1 -Env prd
```

Solo validar, sin desplegar:

```bash
bash scripts/deploy.sh prd --no-deploy
```

### Camino manual

```bash
python -m finops.cli validate --env prd
python scripts/dashboards.py render --env prd     # OBLIGATORIO
databricks bundle validate -t prd
databricks bundle deploy   -t prd
```

> **Por que el render es obligatorio.** Un dashboard Lakeview lleva el SQL con
> nombres de tabla literales. Los JSON versionados usan marcadores
> `{{fct_cost_daily}}` para poder promocionarse entre entornos; el render los
> sustituye por el FQN del catalogo destino y escribe en
> `.build/dashboards/<env>/`, que es a donde apunta `resources/dashboards.yml`.
> Sin ese paso el deploy falla porque los archivos no existen.

### Verificacion

```bash
databricks bundle summary -t prd
databricks bundle run finops_pipeline_diario -t prd
```

---

## Primera carga (backfill)

El pipeline diario solo procesa `ingestion.lookback_days` hacia atras. Para traer
la historia completa disponible en las system tables:

```bash
databricks bundle run finops_backfill -t prd
```

Usa `ingestion.initial_load_days` (400 dias en `prd`) y un cluster con
autoescalado y nodos spot. Puede tardar entre 20 minutos y varias horas segun el
volumen de la cuenta.

Alternativa con ventana acotada, util para probar antes de comprometer el
backfill completo:

```bash
databricks bundle run finops_pipeline_diario -t prd \
  --params full_refresh=true,overrides="ingestion.initial_load_days=60"
```

---

## Recursos desplegados

### Jobs

| Job | Schedule | Que hace |
|---|---|---|
| `finops_pipeline_diario` | 07:00 America/Bogota | Pipeline completo, 7 tareas encadenadas |
| `finops_alertas` | 07:00, 13:00, 19:00 | Solo reevalua alertas sobre gold |
| `finops_backfill` | manual | Recarga historica |

El pipeline diario corre a las 07:00 para dar margen sobre la latencia de
publicacion de `system.billing.usage` (tipicamente pocas horas, con reproceso de
7 dias hacia atras que recupera cualquier llegada tardia).

Las tareas comparten un unico `job_cluster`, asi que el cluster se levanta una
sola vez para todo el pipeline.

**La tarea `alertas` usa `run_if: AT_LEAST_ONE_SUCCESS`**: se ejecuta aunque
`analitica` o `calidad` fallen, porque una falla del pipeline es exactamente algo
que hay que notificar.

### Dashboards

Se publican en `${workspace.root_path}/dashboards`. En `dev` el `root_path` es la
carpeta personal del usuario (por `mode: development`); en `qa` y `prd` es
`/Workspace/Shared/.bundle/...`.

---

## CI/CD

### `.github/workflows/ci.yml` — en cada PR

1. Lint con `ruff` y suite completa de `pytest` (Python 3.10 y 3.12).
2. Validacion de la configuracion de los tres entornos.
3. Verificacion de que `dashboards/*.lvdash.json` este sincronizado con
   `scripts/dashboards.py` (falla si alguien edito el JSON a mano sin regenerar).
4. `databricks bundle validate` si hay secretos configurados.

### `.github/workflows/deploy.yml` — manual o por tag

Ejecuta pruebas, valida, renderiza y despliega. Usa GitHub Environments, lo que
permite exigir aprobacion manual antes de tocar `prd`.

Secretos requeridos en el repositorio:

| Secreto | Uso |
|---|---|
| `DATABRICKS_HOST` | URL del workspace destino |
| `DATABRICKS_CLIENT_ID` | Service principal (OAuth M2M) |
| `DATABRICKS_CLIENT_SECRET` | Secreto del service principal |

Para el job de validacion en PR se usan las variantes `*_DEV`.

---

## Promocion entre entornos

El mismo commit se despliega a los tres entornos. Lo unico que cambia es el
target del bundle y el overlay de configuracion:

```bash
bash scripts/deploy.sh dev    # catalogo finops_dev,  schedules pausados
bash scripts/deploy.sh qa     # catalogo finops_qa,   alertas solo a tabla
bash scripts/deploy.sh prd    # catalogo finops,      alertas a canales
```

No hay que editar SQL, ni nombres de tabla, ni notebooks para promocionar.

---

## Desmontaje

```bash
databricks bundle destroy -t dev
```

Elimina jobs y dashboards. **No borra los datos**: los schemas y tablas del
catalogo persisten. Para eliminarlos:

```sql
DROP SCHEMA IF EXISTS finops_dev.gold   CASCADE;
DROP SCHEMA IF EXISTS finops_dev.silver CASCADE;
DROP SCHEMA IF EXISTS finops_dev.bronze CASCADE;
```
