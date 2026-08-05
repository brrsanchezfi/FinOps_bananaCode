# ADR 0004 — Dashboards versionados con marcadores de tabla y render por entorno

- **Estado:** aceptada
- **Fecha:** 2026-08-05

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

Se adopta la opcion 4.

Los JSON versionados en `dashboards/` referencian las tablas por su **clave
logica del registro** (`src/finops/catalog.py`):

```sql
FROM {{fct_cost_daily}}
```

`python scripts/dashboards.py render --env prd` sustituye los marcadores por el
nombre completamente calificado y escribe en `.build/dashboards/prd/`, que es a
donde apunta `resources/dashboards.yml`.

Ademas, los JSON **no se editan a mano**: se generan desde
`scripts/dashboards.py`, donde el SQL y el layout viven como codigo Python
legible y revisable en un diff.

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

- **`databricks bundle deploy` por si solo no basta**: hay que renderizar antes.
  Es el principal riesgo operativo de esta decision. Se mitiga con
  `scripts/deploy.sh` / `scripts/deploy.ps1`, que encadenan los pasos, y con
  documentacion explicita en el README, en `03-despliegue.md` y en un comentario
  al inicio de `resources/dashboards.yml`.
- Editar un dashboard en la UI y guardar los cambios ahi los pierde en el
  siguiente deploy. El flujo correcto es exportar y trasladar las diferencias al
  generador.
- Un paso mas en CI/CD.

## Alternativa descartada tardiamente

Se considero usar el hook experimental `experimental.scripts.postinit` de DABs
para automatizar el render dentro de `bundle deploy`. Se descarto porque depende
de una funcionalidad experimental y del entorno Python local de quien despliega:
un fallo ahi rompe el deploy de forma poco diagnosticable. Un script de
despliegue explicito es mas transparente.
