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
databricks bundle deploy -t finops -p finops --var="warehouse_id=<id>"   # o por invocacion
```

Sin el, el deploy de los dashboards falla con "variable warehouse_id has no
value".

### Secretos para el alertamiento (opcional)

```bash
databricks secrets create-scope finops -p prd
databricks secrets put-secret finops teams_webhook_url -p prd
```

Luego poner `enabled: true` en el canal correspondiente de `conf/local.yml`. Si el
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
python -m finops.cli validate --show
python -m finops.cli plan
```

---

## Despliegue

### Camino recomendado

```bash
bash scripts/deploy.sh --profile finops
```

```powershell
pwsh scripts/deploy.ps1 -DatabricksProfile finops
```

Solo validar, sin desplegar:

```bash
bash scripts/deploy.sh --profile finops --no-deploy
```

### Camino manual

```bash
python -m finops.cli validate
databricks bundle validate -t finops
databricks bundle deploy   -t finops
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
databricks bundle summary -t finops
databricks bundle run finops_pipeline_diario -t finops
```

---

## Primera carga (backfill)

El pipeline diario solo procesa `ingestion.lookback_days` hacia atras. Para traer
la historia completa disponible en las system tables:

```bash
databricks bundle run finops_backfill -t finops
```

Usa `ingestion.initial_load_days` (400 dias en `prd`) y un cluster con
autoescalado y nodos spot. Puede tardar entre 20 minutos y varias horas segun el
volumen de la cuenta.

Alternativa con ventana acotada, util para probar antes de comprometer el
backfill completo:

```bash
databricks bundle run finops_pipeline_diario -t finops \
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

Se publican en `${workspace.root_path}/dashboards`.

### Modo del target: `production`

El target `finops` despliega en `mode: production`:

- Los recursos llevan su nombre limpio (`[FinOps] Pipeline diario`). En
  `development` el bundle les anteponia `[dev <usuario>]`.
- Los schedules **respetan** `pause_status`, en vez de quedar pausados a la
  fuerza. Siguen apagados porque el target fija `pipeline_paused: PAUSED`:
  encenderlos es una decision aparte.

  ```bash
  BUNDLE_VAR_pipeline_paused=UNPAUSED bash scripts/deploy.sh --profile <perfil>
  ```

**PENDIENTE — toda instalacion debe desplegarse con un service principal.**
Decidido el 2026-09-28; por ahora se sigue desplegando con el usuario mientras
se prepara. Hoy el `root_path` es `/Workspace/Users/<quien despliega>/.bundle/...`
y los jobs corren con la identidad de esa persona: si pierde el acceso, la
instalacion queda huerfana. Lo que falta:

1. un **service principal** como identidad de despliegue y de ejecucion
   (`run_as` en el target), y
2. un `root_path` compartido, por ejemplo `/Workspace/Shared/.bundle/finops`.

Ojo: cambiar el `root_path` de una instalacion ya desplegada NO la mueve. El
bundle arranca un estado nuevo en la ruta nueva, crea otro juego de jobs y
tableros, y deja los anteriores sin administrar. Hay que retirarlos a mano, o
decidir la ruta antes del primer despliegue.

---

## CI/CD

### `.github/workflows/ci.yml` — en cada PR

1. Lint con `ruff` y suite completa de `pytest` (Python 3.10 y 3.12).
2. Validacion de la configuracion del producto.
3. `python scripts/dashboards.py check`: verifica que `dashboards/*.lvdash.json`
   este sincronizado con el generador (falla si alguien edito un JSON a mano).
4. `databricks bundle validate` si hay secretos configurados.

### `.github/workflows/deploy.yml` — manual o por tag

Ejecuta pruebas, valida y despliega la instalacion `finops`. Usa el GitHub
Environment `finops` como compuerta de aprobacion manual antes de tocar el
workspace (no tiene relacion con los ambientes DEV/QA/PRD que el tablero mide).

Secretos requeridos en el repositorio, compartidos por los dos workflows:

| Secreto | Uso |
|---|---|
| `DATABRICKS_HOST` | URL del workspace destino |
| `DATABRICKS_CLIENT_ID` | Service principal (OAuth M2M) |
| `DATABRICKS_CLIENT_SECRET` | Secreto del service principal |
| `DATABRICKS_WAREHOUSE_ID` | SQL warehouse de los dashboards. Obligatorio: `databricks.yml` no trae valor por defecto |

Antes habia variantes `*_DEV` para el job de validacion en PR; con una sola
instalacion ya no hacen falta.

---

## Una sola instalacion

No hay promocion entre entornos porque no hay entornos de despliegue: se
despliega **una** instalacion (target `finops`), que ya cubre el consumo de dev,
qa y prd del cliente (ver [ADR 0006](adr/0006-una-sola-instalacion.md)).

```bash
bash scripts/deploy.sh --profile <perfil>
```

Si hace falta probar un cambio sin tocar la instalacion productiva, se monta una
SEGUNDA instalacion -- un banco de pruebas -- con su propio catalogo:

- **En otro workspace** (el caso normal): el mismo target `finops`, con el
  perfil de ese workspace y su propio `conf/local.yml`. Es como esta montado el
  banco de pruebas de este repositorio.

  ```bash
  bash scripts/deploy.sh --profile <perfil-del-banco-de-pruebas>
  ```

- **En el mismo workspace**: hace falta un segundo bloque en `targets` de
  `databricks.yml`, porque el nombre del target forma el `root_path` y el nombre
  de los jobs, y dos instalaciones con el mismo target se pisarian.

**No confundir** el ambiente del tablero (donde corre FinOps) con el ambiente de
un recurso del cliente (DEV / QA / PRD), que es un dato que el tablero mide y se
configura en `tagging.workspace_defaults`.

---

## Desmontaje

```bash
databricks bundle destroy -t finops
```

Elimina jobs y dashboards. **No borra los datos**: los schemas y tablas del
catalogo persisten. Para eliminarlos:

```sql
DROP SCHEMA IF EXISTS finops.gold   CASCADE;
DROP SCHEMA IF EXISTS finops.silver CASCADE;
DROP SCHEMA IF EXISTS finops.bronze CASCADE;
```
