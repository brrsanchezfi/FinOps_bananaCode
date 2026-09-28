# ADR 0006 — Una sola instalacion, que cubre todos los ambientes

- **Estado:** aceptada
- **Fecha:** 2026-09-27
- **Reemplaza parcialmente:** [ADR 0005](0005-un-solo-catalogo.md)

## Contexto

El ADR 0005 unifico el *catalogo* pero conservo **tres entornos de despliegue**:
dev, qa y prd, cada uno con su overlay (`conf/dev.yml`, `qa.yml`, `prd.yml`) y
sus umbrales. En la practica ya habia quedado un solo target de bundle (`dev`);
los otros dos se habian retirado porque desplegaban sobre workspaces de la misma
cuenta y solo duplicaban jobs y tableros.

El tablero se vende ahora como **producto terminado**. Y el argumento del ADR
0005 llega hasta el final: el modelo describe el consumo de la CUENTA, porque lee
`system.billing.usage`, que cubre todos los workspaces y todos los ambientes del
cliente. Una sola instalacion ya muestra dev, qa y prd. Tres instalaciones
mostrarian las mismas cifras tres veces.

### Dos cosas distintas que comparten la palabra "ambiente"

Es el punto que mas facilmente se confunde, y confundirlo rompe el modelo:

| | Que es | Que pasa con esta decision |
|---|---|---|
| **Ambiente del tablero** (`env`, el target del bundle) | Donde se despliega *nuestro* pipeline | Se colapsa a **una** instalacion |
| **Ambiente de un recurso del cliente** (la dimension `environment`) | Como se clasifica en DEV / QA / PRD el consumo que el tablero MIDE | **No se toca**: es el corazon del modelo |

La dimension `environment` sale de las etiquetas de los recursos, se homologa con
`tagging.value_map` y se rellena por workspace con `tagging.workspace_defaults`.
Nada de eso depende de cuantas instalaciones de FinOps haya.

## Decision

**Se despliega UNA instalacion, con target `finops`, y la configuracion del
producto vive en `conf/base.yml`.**

- `conf/dev.yml`, `qa.yml` y `prd.yml` desaparecen. `conf/base.yml` absorbe el
  perfil de `prd`, que era el del producto: ventana de 7 dias, calidad
  bloqueante, no crear catalogos, retencion de 1100 dias.
- Lo propio de cada instalacion -- incluido un perfil mas laxo durante una
  implantacion -- va en `conf/local.yml`, la capa que ya existia para eso.
- `env` deja de elegir un perfil de configuracion y pasa a ser una **etiqueta**
  que nombra la instalacion (`pipeline_environment` en `ops_run_log`). Se valida
  su forma, no una lista cerrada, y `conf/<env>.yml` pasa a ser opcional.
- El propio computo de FinOps se etiqueta con `resource_environment` (PRD por
  defecto), **no** con el nombre del target.

## Consecuencias

**A favor**

- El despliegue refleja lo que el producto es: una cosa, no tres.
- Desaparece una fuente de configuracion y con ella una clase de error ("este
  umbral esta distinto en qa que en prd y nadie sabe por que").
- La instalacion del cliente es mas simple: `conf/local.yml` y un target.

**En contra**

- **Ya no hay un lugar aislado para probar un cambio antes de que llegue a las
  tablas de la instalacion.** Un defecto que se despliega le pega directamente.
  La salida, si hace falta, es una SEGUNDA instalacion con su propio catalogo
  (el banco de pruebas `finops_lab` es exactamente eso): `--env`, el target y
  `conf/<env>.yml` siguen siendo sobrescribibles para ese caso.
- **El perfil laxo de `dev` ya no viaja en el repositorio.** Una implantacion
  que necesite calidad no bloqueante o logs en DEBUG tiene que declararlo en su
  `conf/local.yml`.
- **Renombrar el target recrea los jobs y los tableros**, porque su nombre y su
  `root_path` dependen de el. Es un costo de una sola vez.
- **Error latente que esta decision destapo.** Los tags de ambiente del propio
  computo de FinOps (`entorno` en el job, `environment` en el cluster) salian de
  `${bundle.target}`. Con el target `dev` funcionaban por casualidad, porque el
  `value_map` lleva `dev` a DEV. Al renombrarlo a `finops`, el costo del pipeline
  habria aparecido en un ambiente inexistente llamado `finops`. Se separo en
  `resource_environment`, y `tests/test_neutralidad.py` falla si un tag que es
  alias de la dimension vuelve a salir del target.

## Alternativa considerada

**Conservar el target `dev` y solo colapsar la configuracion.** No recrea
recursos, pero el nombre sigue sugiriendo que existen otros ambientes de
despliegue, que es justo la idea que esta decision retira. Se descarto por eso.
