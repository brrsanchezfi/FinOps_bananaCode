# 08 — Desarrollo

## Entorno

```bash
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\Activate.ps1
pip install -e ".[dev]"

pytest -q
ruff check src tests scripts
python -m finops.cli validate --env dev
```

La suite corre **sin Spark ni Databricks**. Si quieres ejecutar tambien las
pruebas marcadas `@pytest.mark.spark`:

```bash
pip install -e ".[dev,spark]"
```

Sin `pyspark` instalado esas pruebas se omiten automaticamente (ver
`tests/conftest.py`).

---

## La regla principal

> **Lo que decide algo es una funcion pura. Spark solo lee, escribe y mapea.**

| Capa | Modulos | Depende de Spark |
|---|---|---|
| Logica de negocio | `analytics/*`, `transform/pricing.py`, `transform/tags.py`, `alerting/rules.py`, `alerting/notifier.py` (formateo), `quality/checks.py` (evaluadores), `config.py` | **No** |
| Adaptadores | `spark_utils.py`, `ingestion/*`, `transform/silver.py`, `transform/gold.py` | Si |
| Orquestacion | `pipeline.py`, `notebook.py`, `cli.py` | Si (import perezoso) |

Las importaciones de `pyspark` son **perezosas**: van dentro de la funcion o bajo
`if TYPE_CHECKING`. Por eso `import finops.pipeline` funciona en una maquina sin
pyspark, y la CLI puede validar configuracion en CI.

### Cuando una regla debe existir en Python y en Spark

Ocurre con la clasificacion de SKU y con el calculo de costo: se necesitan fila a
fila sobre millones de registros (Spark) y tambien en pruebas (Python).

La solucion es declarar la regla **una sola vez** como dato y que ambos caminos
la consuman:

```python
# pricing.py — declaracion unica
SKU_PATTERN_SPECS = (
    (r"SERVERLESS.*(SQL|DBSQL)", "SERVERLESS_SQL"),
    ...
)

# camino Python: re.compile sobre los mismos patrones
# camino Spark:  Column.rlike sobre los mismos patrones (silver.sku_group_expr)
```

No duplicar la logica en dos lugares: divergen en el primer cambio.

---

## Convenciones

- **Idioma:** comentarios, docstrings, mensajes de log y nombres de variables
  locales en espanol (sin tildes, para evitar problemas de codificacion en logs
  de cluster). Los nombres de API publica (funciones, clases, columnas del
  modelo) en ingles, porque son el contrato con SQL y con los dashboards.
- **Tipado:** anotaciones en toda funcion publica, con
  `from __future__ import annotations` al inicio del modulo.
- **Docstrings:** explican el **por que**, no el que. Si una decision no es obvia
  —por que MAD y no z-score, por que la entidad se resuelve por prioridad— eso va
  en el docstring.
- **Errores:** usar la jerarquia de `errors.py`. No lanzar `Exception` a secas.
- **Logs:** `get_logger(<subsistema>)`, nunca `print` en el paquete.
- **Longitud de linea:** 110 (`ruff`).

---

## Pruebas

Estructura por modulo: `tests/test_<modulo>.py`, con clases que agrupan por
comportamiento.

Toda funcionalidad nueva necesita, como minimo:

1. El caso feliz.
2. Los casos limite (entrada vacia, `None`, division por cero, serie constante).
3. El caso que **no** debe disparar.
4. La configuracion deshabilitada, si aplica.

```python
class TestMiRegla:
    def test_dispara_cuando_corresponde(self): ...
    def test_no_dispara_bajo_el_umbral(self): ...
    def test_entrada_vacia(self): ...
    def test_regla_deshabilitada(self): ...
```

Las pruebas usan datos deterministicos —nunca `random`— para que un fallo sea
siempre reproducible. Ver los helpers `serie_estable` y `serie_con_ruido` en
`tests/conftest.py`.

### Pruebas que protegen invariantes

Algunas pruebas no verifican una funcion sino una propiedad del sistema, y son
las mas valiosas:

- `test_catalog.py` — ningun dashboard referencia una tabla inexistente ni tiene
  un catalogo incrustado.
- `test_config.py` — los tres entornos versionados son validos.
- `test_chargeback.py::TestConciliacion` — el chargeback siempre suma el total.
- `test_pricing.py::test_identidad_de_componentes` — los componentes de costo
  siempre cuadran entre si.

---

## Como agregar…

### …una regla de optimizacion

1. Funcion pura en `analytics/optimization.py`:

```python
def rule_mi_hallazgo(profile: dict[str, Any], cfg: dict[str, Any]) -> Recommendation | None:
    """Descripcion de que detecta y por que importa."""
    if not aplica(profile):
        return None
    dias = int(profile.get("analysis_days") or 30)
    costo_mensual = _monthly(float(profile.get("cost_usd") or 0.0), dias)
    return _make(
        profile,
        rule_id="MI_HALLAZGO",
        title="...",
        savings=costo_mensual * float(cfg.get("assumed_savings_ratio", 0.10)),
        confidence=CONFIDENCE_MEDIUM,
        method="costo_mensual x ratio (explicar de donde sale el ratio)",
        recommendation="Accion concreta que debe tomar quien lea esto.",
        evidence={"dato_clave": ...},
    )
```

2. Registrarla en `RULES`.
3. Agregar su bloque a `optimization.rules` en `conf/base.yml`.
4. Pruebas en `tests/test_optimization.py`. Hay una prueba
   (`test_todas_las_reglas_tienen_configuracion_de_ejemplo`) que falla si olvidas
   la configuracion de prueba.

**`estimation_method` es obligatorio y debe ser honesto.** Si el ahorro es una
suposicion, decirlo en el metodo y bajar la confianza. Una recomendacion sin
metodo explicado no es accionable, es ruido.

Si el perfil de entidad necesita un campo nuevo, agregarlo en
`transform/gold.py:build_entity_profiles`.

### …una regla de alerta

Ver [05 — Alertas](05-alertas.md#agregar-una-regla).

### …una tabla al modelo

1. Declararla en `catalog.py` con su `TableDef` (clave = nombre fisico, y una
   `description` de al menos 20 caracteres: hay una prueba que lo verifica).
2. Agregarla a `ALL_TABLES`.
3. Escribir su constructor en `transform/gold.py` y registrarlo en `run_gold`.
4. Si va a un dashboard, referenciarla con `{{clave}}` en `scripts/dashboards.py`.

### …una fuente de datos

1. Declararla en `sources` de `conf/base.yml`, con `optional: true` si el
   pipeline debe sobrevivir sin ella.
2. Escribir la funcion de ingesta en `ingestion/system_tables.py` y registrarla en
   `INGESTORS`.
3. Declarar su tabla bronze en `catalog.py`.

Usar siempre `read_source()` (respeta `optional` y `fallback_tables`) y
`select_existing()` (tolera columnas ausentes).

---

## Flujo de trabajo

```bash
git checkout -b feat/mi-cambio

# ... cambios ...

pytest -q
ruff check --fix src tests scripts
python -m finops.cli validate --env prd

# Si tocaste dashboards
python scripts/dashboards.py generate

git add -A && git commit -m "feat: descripcion"
git push -u origin feat/mi-cambio
```

CI valida lint, pruebas en Python 3.10 y 3.12, la configuracion de los tres
entornos, y que los dashboards versionados esten sincronizados con el generador.

### Probar contra un workspace real

```bash
databricks auth login --profile dev
bash scripts/deploy.sh dev
databricks bundle run finops_pipeline_diario -t dev --params dry_run=true
```

`dry_run=true` recorre todo el pipeline y reporta cuantas filas escribiria, sin
tocar ninguna tabla. Es la forma mas rapida de validar permisos y esquemas.

---

## Depuracion

```bash
# Configuracion efectiva de un entorno (con secretos redactados)
python -m finops.cli validate --env prd --show

# Ventana, tablas destino y fuentes configuradas
python -m finops.cli plan --env prd

# Una etapa aislada, con log detallado
python -m finops.cli run --env dev --stages silver --log-level DEBUG
```

Dentro de un notebook, `ctx.cfg.redacted()` imprime la configuracion sin exponer
secretos, y `resultado.recorder.summary()` da el detalle de cada etapa con su
duracion y filas escritas.

---

## Deuda tecnica conocida

Documentada honestamente para que quien continue sepa donde esta parado:

1. **Los dashboards Lakeview no fueron validados visualmente** contra un
   workspace. El esquema JSON es correcto, pero algun encoding de widget puede
   necesitar ajuste tras el primer deploy. Ver
   [04 — Dashboards](04-dashboards.md#nota-sobre-la-primera-revision).

2. **No hay pruebas de integracion con Spark.** La logica esta cubierta al 100%
   en su version pura, pero las transformaciones de `silver.py` y `gold.py` solo
   se validan al ejecutarse contra un workspace. Anadir pruebas con una
   `SparkSession` local (marcadas `@pytest.mark.spark`) es la mejora de mayor
   valor pendiente.

3. **El costo de infraestructura de Azure no se integra.** El estimador por
   factor esta desactivado por defecto y es burdo. La solucion real es integrar
   una exportacion de Azure Cost Management como fuente adicional.

4. **`fct_job_run_cost` no atribuye el costo de ejecuciones sobre compute
   compartido.** Quedan con `has_cost = false` en lugar de repartir con un
   supuesto. Es una decision consciente, pero significa que el costo por
   ejecucion esta incompleto para esos jobs.
