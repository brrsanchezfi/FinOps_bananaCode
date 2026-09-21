# 03 — Despliegue

## Prerrequisitos

### Herramientas

| Herramienta | Version | Verificacion |
|---|---|---|
| Databricks CLI | 0.230+ | `databricks --version` |
| Python | 3.10+ | `python --version` |
| Git | cualquiera | `git --version` |

```bash
pip install -e ".[dev]"
```

El bundle construye el wheel con `pip wheel` (ver `artifacts` en
`databricks.yml`), asi que no hace falta instalar el paquete `build`.

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

**2. Catalogo destino.**

Crealo una vez con un administrador de Unity Catalog:

```sql
CREATE CATALOG IF NOT EXISTS finops;
GRANT CREATE SCHEMA, USE CATALOG ON CATALOG finops TO `sp-finops`;
```

En `dev` la etapa `setup` lo crea sola si no existe (`catalog.create_if_missing:
true`). En `qa` y `prd` esta deshabilitado a proposito: crear catalogos es una
operacion de administracion, no de un pipeline de datos.

> **Si `CREATE CATALOG` falla con "Metastore storage root URL does not exist"**
>
> Ocurre cuando la cuenta tiene **Default Storage** habilitado y el metastore no
> tiene storage root definido: entonces `CREATE CATALOG` exige una ubicacion
> explicita. Tres salidas, en orden de preferencia:
>
> 1. **Crearlo desde Catalog Explorer** (*Create catalog* → tipo *Standard*),
>    que usa Default Storage sin pedir ubicacion. Es lo mas simple.
> 2. **Crearlo por SQL con ubicacion**, si tienes una external location
>    configurada:
>    ```sql
>    CREATE CATALOG finops
>      MANAGED LOCATION 'abfss://<contenedor>@<cuenta>.dfs.core.windows.net/<ruta>';
>    ```
> 3. **Dejar que el pipeline lo cree**, indicandole la ubicacion en
>    `conf/<env>.yml`:
>    ```yaml
>    catalog:
>      managed_location: "abfss://<contenedor>@<cuenta>.dfs.core.windows.net/<ruta>"
>    ```
>
> El pipeline no intenta crear el catalogo si ya existe, asi que una vez creado
> el problema no reaparece.

> **Si el catalogo se crea bien pero falla al escribir la primera TABLA**
>
> Dos variantes distintas, las dos con el mismo sintoma aparente ("el catalogo
> existe, los schemas existen, y aun asi no funciona"):
>
> 1. **`DAC_DOES_NOT_EXIST: Root storage credential for metastore ... does not
>    exist`.** El metastore declara un `storage_root` pero no tiene credencial
>    raiz. `CREATE SCHEMA` no falla -- los schemas quedan creados -- y el error
>    aparece recien al crear la primera tabla gestionada, asi que el catalogo se
>    ve perfectamente montado y no sirve. Solucion: dar al catalogo su propia
>    ubicacion, apuntando a una external location que si tenga credencial:
>    ```bash
>    databricks external-locations list -p <perfil>
>    databricks catalogs create <catalogo> >      --storage-root "abfss://<contenedor>@<cuenta>.dfs.core.windows.net/<ruta>" -p <perfil>
>    ```
>    y dejarlo anotado en `conf/local.yml` bajo `catalog.managed_location`, para
>    que una recreacion del catalogo no repita el diagnostico.
>
> 2. **`NATIVE_IO_ERROR ... DEADLINE_EXCEEDED: acquiring connection`.** La
>    credencial y la ruta estan bien (se puede comprobar escribiendo una tabla
>    desde un SQL warehouse serverless); lo que no se establece es la conexion
>    del compute al storage. Suele ser una regla de red en la cuenta de
>    almacenamiento que no contempla la subred de los clusters clasicos de ese
>    workspace.

**3. Lectura para los consumidores de los dashboards.**

```sql
GRANT USE CATALOG ON CATALOG finops TO `analistas-finops`;
GRANT USE SCHEMA, SELECT ON SCHEMA finops.gold TO `analistas-finops`;
```

### SQL warehouse para los dashboards

```bash
databricks warehouses list -p prd --output json
```

Pasarlo como variable del bundle. **No** se escribe en `databricks.yml`: el
warehouse pertenece al workspace del cliente y un valor quemado se heredaria en
otra instalacion.

```bash
export BUNDLE_VAR_warehouse_id=<id>          # bash
$env:BUNDLE_VAR_warehouse_id = "<id>"        # PowerShell
databricks bundle deploy -t dev -p finops --var="warehouse_id=<id>"   # o por invocacion
```

Sin el, el deploy de los dashboards falla con "variable warehouse_id has no
value".

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

El repositorio es **producto**: viaja sin saber cual es tu cuenta. Todo lo que
identifica a una instalacion vive en dos archivos que git ignora y que se crean
copiando su plantilla:

```bash
cp conf/local.example.yml         conf/local.yml
cp conf/budgets.local.example.yml conf/budgets.local.yml
```

| Archivo | Que ajustar | Versionado |
|---|---|---|
| `conf/local.yml` | `tagging.workspace_defaults` (ambiente por workspace), `tagging.value_map.cost_center` (unidades de negocio), `project.owner_email`, `pricing.discounts` | no |
| `conf/budgets.local.yml` | Presupuestos reales y sus responsables | no |
| perfil del CLI | Host del workspace (`databricks auth login -p <perfil>`) | no |
| `BUNDLE_VAR_warehouse_id` | SQL warehouse de los dashboards | no |
| `conf/base.yml` | `tagging.aliases`, solo si la organizacion usa claves de etiqueta que no estan ya contempladas | si |
| `databricks.yml` | `run_as.user_name` → service principal; `notification_email` via `--var` | si |

`conf/local.yml` se fusiona **encima** de `base.yml` y `<env>.yml`, asi que solo
se escribe lo que difiere. `conf/budgets.local.yml`, si existe, **reemplaza**
completo a `conf/budgets.yml` (no se fusionan: `budgets` es una lista y mezclar
dos produciria ids duplicados).

> Ambos archivos estan en `sync.include` de `databricks.yml`. Es lo que los sube
> al workspace: el CLI excluye del bundle lo que git ignora, asi que sin esa
> entrada el pipeline correria alla con la configuracion neutra del repositorio
> — sin presupuestos y con todo el costo en `SIN_ASIGNAR`, sin ningun error
> visible.

`tests/test_neutralidad.py` falla si configuracion de una instalacion se cuela
en un archivo versionado.

Validar sin desplegar nada:

```bash
python -m finops.cli validate --env prd --show
python -m finops.cli plan --env prd
```

---

## Despliegue

### Camino recomendado

```bash
bash scripts/deploy.sh prd --profile finops
```

```powershell
pwsh scripts/deploy.ps1 -Env prd -DatabricksProfile finops
```

Solo validar, sin desplegar:

```bash
bash scripts/deploy.sh prd --profile finops --no-deploy
```

### Camino manual

```bash
python -m finops.cli validate --env prd
databricks bundle validate -t prd
databricks bundle deploy   -t prd
```

No hay paso de build previo: los dashboards estan versionados ya resueltos en
`dashboards/`. Si cambiaste `scripts/dashboards.py`, regenera y commitea antes
de desplegar:

```bash
python scripts/dashboards.py generate
```

> **No pongas nunca un recurso del bundle en una ruta ignorada por git.** El CLI
> de Databricks construye el arbol de archivos del bundle respetando
> `.gitignore`, y un `file_path` hacia una ruta ignorada falla con
> `no such file or directory` aunque el archivo exista en disco. Es la razon por
> la que los dashboards generados se versionan; ver
> [ADR 0004](adr/0004-dashboards-con-marcadores-de-tabla.md).

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
3. `python scripts/dashboards.py check`: verifica que `dashboards/*.lvdash.json`
   este sincronizado con el generador (falla si alguien edito un JSON a mano).
4. `databricks bundle validate` si hay secretos configurados.

### `.github/workflows/deploy.yml` — manual o por tag

Ejecuta pruebas, valida y despliega. Usa GitHub Environments, lo que
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
bash scripts/deploy.sh dev    # schedules pausados, alertas solo a tabla
bash scripts/deploy.sh qa     # schedules activos
bash scripts/deploy.sh prd    # schedules activos, alertas a canales
```

No hay que editar SQL, ni nombres de tabla, ni notebooks para promocionar: los
tres comparten el catalogo `finops` (ver [ADR 0005](adr/0005-un-solo-catalogo.md)).

---

## Desmontaje

```bash
databricks bundle destroy -t dev
```

Elimina jobs y dashboards. **No borra los datos**: los schemas y tablas del
catalogo persisten. Para eliminarlos:

```sql
DROP SCHEMA IF EXISTS finops.gold   CASCADE;
DROP SCHEMA IF EXISTS finops.silver CASCADE;
DROP SCHEMA IF EXISTS finops.bronze CASCADE;
```
