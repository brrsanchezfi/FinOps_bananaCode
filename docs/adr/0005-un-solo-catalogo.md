# ADR 0005 — Un solo catalogo para los tres entornos

- **Estado:** aceptada
- **Fecha:** 2026-08-10
- **Reemplaza parcialmente:** [ADR 0004](0004-dashboards-con-marcadores-de-tabla.md)

## Contexto

El diseno original daba un catalogo por entorno: `finops_dev`, `finops_qa`,
`finops`. Es el reflejo automatico de como se separan entornos en una
plataforma de datos de aplicacion, donde cada uno tiene *sus* datos.

Pero el modelo FinOps no encaja en ese molde. Sus fuentes son las system tables
de Unity Catalog, que son **de la cuenta**: `system.billing.usage` no tiene una
version de dev y otra de produccion. Los tres entornos leen exactamente lo mismo
y producen exactamente las mismas cifras.

Las consecuencias de separarlos eran todas costo y ninguna ventaja:

- Tres copias del mismo dato, con su almacenamiento y su computo de tres
  pipelines calculando lo mismo.
- Un juego de dashboards por entorno (nueve archivos), porque el SQL de Lakeview
  lleva los nombres de tabla literales.
- Comparar entornos exigia consultas entre catalogos.
- Tres catalogos que crear y gobernar.

## Decision

**Un solo catalogo `finops` con los schemas `bronze`, `silver` y `gold`,
compartido por los tres entornos.**

Lo que separa a los entornos pasa a ser lo que de verdad los distingue:

| | dev | qa | prd |
|---|---|---|---|
| Donde corre | workspace de dev | workspace de qa | workspace de prd |
| Schedules | pausados | activos | activos |
| Ventana de ingesta | 3 dias | 5 dias | 7 dias |
| Umbrales de alerta | laxos | estrictos | estrictos |
| Canales de alerta | solo tabla | tabla | tabla + Teams |
| Crea el catalogo | si | no | no |

Como consecuencia directa, los dashboards vuelven a ser **un solo juego** en
`dashboards/*.lvdash.json`: los nombres resueltos son identicos para los tres
entornos.

## Consecuencias

**A favor**

- Un solo lugar donde viven los datos, se gobiernan permisos y se buscan tablas.
- Tres archivos de dashboard en vez de nueve, y ningun catalogo escrito a mano:
  los nombres siguen saliendo de `conf/` via los marcadores `{{clave}}`.
- Desaparece la clase de bug "el dashboard de un entorno consulta el catalogo de
  otro".
- El pipeline es idempotente por rango de fechas, asi que dos entornos
  ejecutandose sobre el mismo destino convergen al mismo resultado en vez de
  corromperlo.

**En contra**

- **El aislamiento pasa de catalogo a schema.** Es una frontera mas fina: quien
  tenga `USE CATALOG finops` vera que existen los tres. Unity Catalog permite
  `GRANT` por schema, asi que el control sigue siendo efectivo, pero hay que
  ejercerlo a ese nivel.
- **Una corrida de dev escribe sobre las mismas tablas que produccion.** El
  riesgo real esta acotado porque las escrituras son idempotentes y el dato de
  origen es el mismo, pero un `full_refresh` o un cambio de esquema en dev
  afectan a lo que ve produccion.
- Si en el futuro hiciera falta experimentar sin tocar lo compartido, la salida
  es apuntar dev a schemas propios (`dev_bronze`, `dev_silver`, `dev_gold`) desde
  `conf/dev.yml`. En ese momento los dashboards vuelven a necesitar una version
  por entorno; una prueba
  (`test_los_tres_entornos_resuelven_a_las_mismas_tablas`) y el propio generador
  lo detectan y avisan.

## Alternativa considerada

**Catalogo unico con schemas prefijados por entorno** (`dev_gold`, `qa_gold`,
`prd_gold`). Conserva el aislamiento y unifica el gobierno, pero mantiene la
duplicacion de datos y de dashboards, que era el costo principal. Se descarto
por eso; queda documentada arriba como la salida si el aislamiento llega a pesar
mas que la duplicacion.
