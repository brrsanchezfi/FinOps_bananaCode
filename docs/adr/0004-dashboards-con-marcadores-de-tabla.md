# ADR 0004 — Dashboards generados por entorno y versionados

- **Estado:** aceptada
- **Fecha:** 2026-08-05
- **Revisada:** 2026-08-06 (ver "Correccion" al final)

## Contexto

Un dashboard Lakeview (`.lvdash.json`) lleva el SQL de sus datasets embebido, con
nombres de tabla **literales**. La plataforma se despliega en tres entornos con
tres catalogos distintos (`finops_dev`, `finops_qa`, `finops`).

Versionar el JSON tal cual significaria que el dashboard de `dev` no sirve en
`prd`, o mantener tres copias divergentes del mismo SQL.

Opciones evaluadas:

1. **Tres copias del JSON, una por entorno.** Divergen al primer cambio.
2. **Crear vistas con nombre fijo** en un catalogo comun que apunten al catalogo
   del entorno. Agrega una capa de indireccion que hay que mantener y desplegar,
   y confunde a quien consulta.
3. **Sustitucion de variables del bundle dentro del JSON.** DABs no garantiza la
   interpolacion dentro de archivos de dashboard; depender de eso es fragil.
4. **Marcadores propios + paso de render antes del deploy.**

## Decision

Se adopta la opcion 4, en la forma corregida descrita abajo.

`scripts/dashboards.py` es la unica fuente de verdad: ahi viven el SQL y el
layout como codigo Python. En ese SQL las tablas se escriben como **clave logica
del registro** (`src/finops/catalog.py`):

```sql
FROM {{fct_cost_daily}}
```

`python scripts/dashboards.py generate` sustituye los marcadores por el nombre
completamente calificado de cada entorno y escribe **nueve archivos versionados**:

```
dashboards/dev/*.lvdash.json     (catalogo finops_dev)
dashboards/qa/*.lvdash.json      (catalogo finops_qa)
dashboards/prd/*.lvdash.json     (catalogo finops)
```

`resources/dashboards.yml` apunta a `dashboards/${bundle.target}/`, asi que
`databricks bundle deploy` funciona sin ningun paso de build previo.

Los JSON **no se editan a mano**: `scripts/dashboards.py check` (y una prueba en
`tests/test_catalog.py`) verifica que coincidan byte a byte con el generador.

## Consecuencias

**A favor**

- Un solo origen de verdad para los tres entornos. Promocionar de dev a prd no
  requiere editar nada.
- El registro de tablas se convierte en el contrato: si alguien renombra una
  tabla en `catalog.py`, el render falla de inmediato con el marcador huerfano,
  en vez de producir un dashboard roto en silencio.
- El diff de un cambio de dashboard es legible (Python), no un JSON de miles de
  lineas reordenado.
- Dos pruebas automatizadas protegen el mecanismo: ningun dashboard puede
  referenciar una tabla inexistente, ninguno puede tener un catalogo incrustado.
  Y CI verifica que el JSON versionado este sincronizado con el generador.

**En contra**

- **Se versionan artefactos generados** (9 archivos JSON grandes). Pueden derivar
  del generador si alguien los edita a mano. Se mitiga con la verificacion byte a
  byte en `check`, en las pruebas y en CI.
- Cambiar el SQL de un dashboard produce un diff de 9 archivos ademas del cambio
  real en `scripts/dashboards.py`. Ruido en la revision, a cambio de que el
  deploy no tenga pasos ocultos.
- Editar un dashboard en la UI y guardar los cambios ahi los pierde en el
  siguiente deploy. El flujo correcto es exportar y trasladar las diferencias al
  generador.
- Agregar un cuarto entorno significa regenerar y versionar 3 archivos mas.

## Alternativa descartada

Se considero usar el hook experimental `experimental.scripts.postinit` de DABs
para generar los dashboards dentro de `bundle deploy`. Se descarto porque depende
de una funcionalidad experimental y del entorno Python local de quien despliega:
un fallo ahi rompe el deploy de forma poco diagnosticable.

---

## Correccion (2026-08-06)

**La primera version de esta decision estaba mal.** Establecia que `generate`
produjera un JSON unico con marcadores sin resolver, y que un paso aparte
(`render --env <env>`) escribiera el resultado en `.build/dashboards/<env>/`,
directorio que estaba en `.gitignore` por ser un artefacto de build.

Al desplegar fallaba con:

```
Error: failed to read serialized dashboard from file_path
.build/dashboards/dev/finops_optimizacion.lvdash.json: no such file or directory
```

aun con el archivo presente en disco.

**Causa:** el CLI de Databricks construye el arbol de archivos del bundle
respetando `.gitignore`. Un recurso cuyo `file_path` apunta a una ruta ignorada
por git es invisible para el CLI, sin importar que exista en el sistema de
archivos. El mensaje de error no lo insinua, lo que lo hace especialmente
costoso de diagnosticar.

**Ademas**, el diseno original tenia un segundo defecto ya anticipado en la
seccion "en contra": `databricks bundle deploy` no funcionaba por si solo. Un
paso obligatorio y facil de olvidar antes de cada deploy es una fuente
permanente de fallos.

**Correccion aplicada:** se elimino el paso de render. `generate` produce
directamente el artefacto final por entorno en `dashboards/<env>/`, ruta
versionada. Esto resuelve los dos problemas a la vez: el CLI ve los archivos, y
el deploy no tiene prerrequisitos.

**Leccion generalizable:** en un Databricks Asset Bundle, **ninguna ruta
referenciada por un recurso puede estar en `.gitignore`**. Si un artefacto debe
desplegarse, debe versionarse.

