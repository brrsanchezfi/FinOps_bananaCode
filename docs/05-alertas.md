# 05 — Alertas

## Como funciona

```
hechos de gold ──► reglas ──► filtro de severidad ──► deduplicacion ──► tope
                                                                          │
                              ops_alert_log  ◄────  canales  ◄────────────┘
                                                (Teams / Slack / tabla)
```

Toda alerta, se despache o no, queda registrada en `ops_alert_log`. Las
suprimidas se marcan con `dispatch_status = 'suppressed'`, asi que la tabla es la
fuente completa y los canales son solo la notificacion.

---

## Reglas

| `rule_id` | Cuando dispara | Severidad |
|---|---|---|
| `BUDGET_THRESHOLD` | Un presupuesto cruza uno de sus umbrales (50/80/90/100%) | medium → critical segun el % |
| `FORECAST_OVERRUN` | La proyeccion de cierre supera el presupuesto, aunque aun no se haya excedido | medium / high |
| `COST_ANOMALY` | Anomalia de costo tipo SPIKE con severidad >= configurada | high / critical |
| `DAILY_SPEND_SPIKE` | El gasto total del dia supera en +35% el promedio de los 7 dias previos | high / critical |
| `NEW_EXPENSIVE_ENTITY` | Un recurso nuevo (≤7 dias) ya acumula un costo relevante | medium / high |
| `TAG_COVERAGE_DROP` | La cobertura de la dimension principal cae bajo el minimo | medium / high |
| `DATA_QUALITY` | Chequeos de calidad bloqueantes fallidos | high |
| `PIPELINE_HEALTH` | Una etapa del pipeline FinOps fallo | critical |

Las **caidas** de costo (`direction = 'DROP'`) se registran en
`fct_cost_anomaly` pero **no se notifican**: una baja de gasto casi nunca es una
urgencia, y notificarla erosiona la atencion a las alertas que si lo son.

`FORECAST_OVERRUN` no dispara cuando el presupuesto ya esta excedido, para no
duplicar el mensaje de `BUDGET_THRESHOLD`.

---

## Deduplicacion

Cada alerta lleva una **huella** (`fingerprint`): un hash de las partes que
identifican *la misma condicion*.

| Regla | Componentes de la huella |
|---|---|
| `BUDGET_THRESHOLD` | id del presupuesto + periodo + umbral alcanzado |
| `COST_ANOMALY` | dimension + valor + fecha |
| `DAILY_SPEND_SPIKE` | ambito + fecha |
| `NEW_EXPENSIVE_ENTITY` | entity_key + fecha de primera aparicion |
| `TAG_COVERAGE_DROP` | dimension + mes |

Mientras la condicion persista, no se vuelve a notificar dentro del periodo de
enfriamiento (`alerting.cooldown_hours`, 24 h por defecto). Que el umbral forme
parte de la huella es deliberado: cruzar el 80% notifica una vez, y cruzar
despues el 100% vuelve a notificar, porque es una condicion distinta.

El despachador tambien deduplica dentro del mismo lote: si dos reglas producen la
misma huella en una corrida, solo se envia una.

---

## Severidades y enrutamiento

```
low  <  medium  <  high  <  critical
```

Se aplican dos filtros en cadena:

1. **`alerting.min_severity`** — global. Lo que no lo alcanza no se despacha
   (pero sigue en la tabla).
2. **`min_severity` de cada canal** — permite que Teams reciba solo `high`+
   mientras la tabla registra todo.

```yaml
alerting:
  min_severity: medium
  channels:
    - name: tabla
      type: table
      enabled: true
      min_severity: low          # registra todo
    - name: teams
      type: webhook
      enabled: true
      secret_scope: finops
      secret_key: teams_webhook_url
      format: teams
      min_severity: high         # solo lo importante llega al canal
```

**Tope por corrida** (`max_alerts_per_run`, 50 por defecto): si se supera, se
conservan las mas severas y el resto se registra como suprimido, con un resumen
agrupado en el log. Es una proteccion contra tormentas de alertas cuando algo se
rompe de forma masiva.

---

## Canales

| Tipo | Descripcion |
|---|---|
| `table` | Solo persiste en `ops_alert_log`. Siempre habilitado |
| `webhook` | Teams (MessageCard) o Slack (Block Kit), segun `format` |
| `noop` | Registra en log sin enviar. Se usa automaticamente en `dry_run` |

### Configurar Teams

1. En el canal de Teams: **Conectores → Incoming Webhook** → copiar la URL.
2. Guardar la URL como secreto:

```bash
databricks secrets create-scope finops -p prd
databricks secrets put-secret finops teams_webhook_url -p prd
```

3. Poner `enabled: true` en `conf/prd.yml`.

### Configurar Slack

Igual, con una **Incoming Webhook** de Slack y `format: slack`.

### Seguridad

La URL del webhook **nunca** se escribe en logs ni en `ops_alert_log`. Se resuelve
desde el scope de secretos en el momento del envio y se guarda en un atributo
privado del canal. `cfg.redacted()` la elimina antes de imprimir la
configuracion.

Si el secreto no existe o el principal no tiene permiso, el canal se omite con
una advertencia: la corrida no falla y las alertas siguen registrandose.

### Reintentos

Los webhooks reintentan hasta 3 veces con espera exponencial (2s, 4s). Un error
4xx que no sea 429 no se reintenta: reintentar un payload mal formado no ayuda.
El resultado de cada entrega queda en `ops_alert_log.delivery_detail`.

---

## Frecuencia

Dos jobs con proposito distinto:

| Job | Frecuencia | Que hace |
|---|---|---|
| `finops_pipeline_diario` | 1×/dia | Construye el modelo y despacha alertas al final |
| `finops_alertas` | 3×/dia | Reevalua las reglas sobre el gold ya construido |

El segundo existe para acortar el tiempo de deteccion de un desbordamiento de
presupuesto sin repetir la ingesta. Es posible porque la etapa `alerts` lee sus
insumos de las tablas gold cuando `analytics` no corrio en el mismo proceso, y
porque la deduplicacion evita que correr tres veces al dia triplique las
notificaciones.

---

## Ajustar el ruido

Si llegan demasiadas alertas:

| Sintoma | Ajuste |
|---|---|
| Muchas anomalias irrelevantes | Subir `anomaly.score_threshold` (3.5 → 4.5) o `anomaly.min_avg_cost_usd` |
| Anomalias los fines de semana | Verificar `anomaly.day_of_week_aware: true` |
| Anomalias en series pequenas | Subir `anomaly.min_pct_change` |
| Alertas de presupuesto muy frecuentes | Reducir `thresholds_pct` a `[80, 100]` |
| Repeticion de la misma alerta | Subir `alerting.cooldown_hours` |
| Ruido general en Teams | Subir el `min_severity` del canal a `critical` |

Si **no** llegan alertas que deberian llegar:

```sql
-- Que se genero y por que no se despacho
SELECT created_at, rule_id, severity, title, dispatch_status, delivery_detail
FROM finops.gold.ops_alert_log
WHERE created_at >= CURRENT_TIMESTAMP() - INTERVAL 3 DAYS
ORDER BY created_at DESC
```

- `dispatch_status = 'suppressed'` → enfriamiento o tope de corrida
- `dispatch_status = 'dispatched'` y `delivered = false` → problema del canal, ver
  `delivery_detail`
- La alerta no aparece → la regla no disparo; revisar sus umbrales, o
  `alerting.min_severity`

---

## Agregar una regla

1. Escribir la funcion en `src/finops/alerting/rules.py`. Debe ser pura, recibir
   datos ya materializados y devolver `list[Alert]`:

```python
def mi_regla(datos: list[dict], cfg: dict | None = None) -> list[Alert]:
    regla = (cfg or {}).get("mi_regla", {}) or {}
    if not regla.get("enabled", True):
        return []
    umbral = float(regla.get("umbral", 100.0))
    ...
    return [Alert(
        rule_id="MI_REGLA",
        severity="high",
        title="...",
        message="...",                       # con la cifra y la accion sugerida
        fingerprint=fingerprint("MI_REGLA", clave, periodo),
        scope="...",
    )]
```

2. Registrarla en `build_all()`.
3. Agregar su bloque a `alerting.rules` en `conf/base.yml`.
4. Escribir pruebas en `tests/test_alerting.py`: caso que dispara, caso que no,
   y regla deshabilitada.

**La huella debe identificar la condicion, no el momento.** Incluir un timestamp
la haria unica en cada corrida y anularia la deduplicacion.
