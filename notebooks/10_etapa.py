# Databricks notebook source
# MAGIC %md
# MAGIC # FinOps — Ejecutor de etapas
# MAGIC
# MAGIC Notebook de la tarea de los jobs. Corre las etapas del parametro `stages`
# MAGIC (por defecto todas) en orden, en el mismo proceso; cada etapa abre con un
# MAGIC separador en el log.

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