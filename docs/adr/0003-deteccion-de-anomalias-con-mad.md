# ADR 0003 — Deteccion de anomalias con z-score modificado (mediana + MAD)

- **Estado:** aceptada
- **Fecha:** 2026-08-05

## Contexto

Hay que detectar dias en que el costo de una serie (equipo, centro de costo,
workspace, entidad) se desvia de forma significativa. Los candidatos habituales:

1. **Umbral fijo** ("alertar si supera X USD"). Simple, pero no se adapta: hay
   que mantener un umbral por serie y se rompe cuando la serie crece.
2. **Z-score clasico** (media y desviacion estandar). El problema es que ambas
   estadisticas son sensibles a los propios valores atipicos que se quieren
   detectar: un pico de un dia infla la desviacion y enmascara los siguientes
   ("efecto de enmascaramiento").
3. **Descomposicion estacional tipo STL o Prophet.** Mas potente, pero introduce
   dependencias pesadas, requiere ajuste por serie y su resultado es dificil de
   explicar a quien recibe la alerta.
4. **Z-score modificado** (mediana y desviacion absoluta mediana).

## Decision

Se adopta la opcion 4 como metodo por defecto:

```
score = 0.6745 × (x − mediana(base)) / MAD(base)
```

La mediana y la MAD tienen un punto de ruptura del 50%: la mitad de los datos
tendrian que ser atipicos para distorsionar la referencia. Un pico aislado no
contamina la base.

Se acompana de tres decisiones adicionales:

**Conciencia de dia de la semana.** Con `day_of_week_aware`, la base se construye
solo con los mismos dias de la semana. Sin esto, en una carga empresarial tipica
todos los sabados y domingos serian anomalias. Se exige un minimo de 3
observaciones del mismo dia; si no las hay, **el punto no se evalua** en lugar de
caer a la ventana completa, porque esa caida reintroduciria exactamente el falso
positivo que el modo evita.

**Doble filtro.** Un punto se reporta solo si supera el umbral de score **y** el
cambio relativo minimo (`min_pct_change`, 30% por defecto). Una serie muy estable
puede producir un score alto con una variacion economicamente irrelevante.

**Piso de materialidad.** Las series cuyo costo promedio esta bajo
`min_avg_cost_usd` no se evaluan. Duplicar un gasto de 3 USD no merece una alerta.

El z-score clasico queda disponible via `anomaly.method: zscore`.

## Degradacion cuando MAD = 0

Una serie casi constante tiene MAD 0, lo que dividiria por cero. La cadena de
respaldo es: MAD → desviacion absoluta media → desviacion estandar poblacional →
si la serie es perfectamente constante, score 0 cuando el valor coincide y un
score acotado (±99) cuando difiere.

El acote evita infinitos y mantiene la tabla de anomalias serializable, sin
perder la senal de que "algo cambio en una serie que nunca cambiaba".

## Consecuencias

**A favor**

- Robusto frente a los picos que debe detectar.
- Sin dependencias externas: `statistics` de la libreria estandar.
- Explicable: la alerta dice "el costo fue X, lo esperado era Y (mediana de los
  ultimos N mismos dias de la semana), desviacion Z".
- Se prueba sin Spark, con series deterministicas.

**En contra**

- Necesita historia: con `day_of_week_aware` se requieren ~3 semanas antes de
  producir detecciones. En `dev` los umbrales estan relajados para poder probar
  con poca historia.
- No modela estacionalidad mensual ni tendencia. Un crecimiento sostenido no se
  detecta como anomalia — para eso estan el pronostico y las alertas de
  presupuesto, que cubren ese caso desde otro angulo.
- La deteccion es por serie independiente: si un cambio afecta a muchos equipos a
  la vez, se generan muchas alertas. Lo acota el tope `max_alerts_per_run`.

## Parametros y su efecto

| Parametro | Efecto de subirlo |
|---|---|
| `score_threshold` (3.5) | Menos alertas, solo desviaciones mas extremas |
| `min_pct_change` (0.30) | Filtra cambios porcentualmente pequenos |
| `min_avg_cost_usd` (20) | Ignora series economicamente irrelevantes |
| `window_days` (28) | Base mas larga, mas estable, menos reactiva |
| `day_of_week_aware` | Elimina falsos positivos de fin de semana; exige mas historia |
