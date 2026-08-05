# Registro de decisiones de arquitectura (ADR)

Cada ADR documenta una decision con consecuencias duraderas: el contexto, las
alternativas evaluadas, lo que se decidio y **que se pierde** con esa eleccion.

| # | Decision | Estado |
|---|---|---|
| [0001](0001-logica-en-modulos-no-en-notebooks.md) | La logica de negocio vive en modulos de Python, no en notebooks | aceptada |
| [0002](0002-escritura-idempotente-por-rango-de-fechas.md) | Escritura idempotente por reemplazo de rango de fechas | aceptada |
| [0003](0003-deteccion-de-anomalias-con-mad.md) | Deteccion de anomalias con z-score modificado (mediana + MAD) | aceptada |
| [0004](0004-dashboards-con-marcadores-de-tabla.md) | Dashboards versionados con marcadores de tabla y render por entorno | aceptada |

## Cuando escribir uno

Cuando la decision sea costosa de revertir, cuando alguien vaya a preguntar
"¿por que esta hecho asi?" en seis meses, o cuando se descarten alternativas
razonables por motivos que no son obvios en el codigo.

No hace falta ADR para elecciones locales de implementacion.

## Formato

```markdown
# ADR NNNN — Titulo en una linea

- **Estado:** propuesta | aceptada | reemplazada por ADR-XXXX
- **Fecha:** AAAA-MM-DD

## Contexto
El problema y las restricciones. Que alternativas habia.

## Decision
Que se decidio, en presente.

## Consecuencias
**A favor** / **En contra**. La seccion "en contra" es obligatoria: una decision
sin costo no era una decision.
```
