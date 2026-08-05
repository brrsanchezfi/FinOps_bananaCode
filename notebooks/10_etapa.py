# Databricks notebook source
# MAGIC %md
# MAGIC # FinOps — Ejecutor de una etapa
# MAGIC
# MAGIC Notebook usado por las tareas del job multi-etapa. Cada tarea invoca este
# MAGIC mismo notebook con un valor distinto del parametro `stages`, lo que da
# MAGIC observabilidad por etapa en la UI de Workflows (duracion, reintentos y
# MAGIC fallos por separado) sin duplicar codigo.
# MAGIC
# MAGIC Para ejecutar el pipeline completo de una sola vez usar `00_orquestador`.

# COMMAND ----------

import sys
from pathlib import Path

_raiz = Path.cwd()
for _candidato in [_raiz, *_raiz.parents[:4]]:
    _src = _candidato / "src"
    if (_src / "finops" / "__init__.py").exists() and str(_src) not in sys.path:
        sys.path.insert(0, str(_src))
        break

from finops.notebook import bootstrap, resumen  # noqa: E402

ctx = bootstrap()
etapas = ctx.stages()
print(f"Etapas solicitadas: {etapas}")
print(ctx.cfg.describe())

# COMMAND ----------

resultado = ctx.run(etapas)

# COMMAND ----------

resumen(resultado)

# COMMAND ----------

import json  # noqa: E402

salida = {
    "run_id": resultado.run_id,
    "stages": {m.stage: m.status for m in resultado.recorder.metrics},
    "rows": {m.stage: m.rows for m in resultado.recorder.metrics},
    "ok": resultado.ok,
}
if ctx.dbutils is not None:
    ctx.dbutils.notebook.exit(json.dumps(salida))
else:
    print(json.dumps(salida, indent=2))
