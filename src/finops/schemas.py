"""Esquemas explicitos de las tablas que se escriben desde Python.

No se deja inferir el esquema a Spark. La inferencia falla de tres formas
distintas con datos reales:

  * `CANNOT_MERGE_TYPE` cuando una columna es entera en unas filas y decimal en
    otras (por ejemplo un presupuesto sin consumo frente a otro con consumo).
  * `NullType`, que Delta rechaza, cuando una columna opcional resulta nula en
    todas las filas de la primera corrida.
  * `struct` en vez de `map` para los diccionarios anidados: el runtime de
    Databricks activa `inferNestedDictAsStruct`, y entonces cada clave nueva
    cambia el esquema de la tabla.

Este modulo vive aparte de `pipeline` para que `ingestion` y `quality` puedan
usarlo sin depender de la capa de orquestacion.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

#: Columnas de auditoria comunes a las tablas derivadas.
#:
#: `pipeline_environment` es el ambiente que PRODUJO la fila (dev/qa/prd). No
#: confundir con `environment`, que en fct_recommendation y fct_cost_daily es el
#: ambiente DEL RECURSO, resuelto desde sus etiquetas.
AUDITORIA: dict[str, Any] = {"run_id": str, "pipeline_environment": str, "generated_at": datetime}

#: El pronostico aplana ForecastResult + ForecastPoint, asi que no proviene de
#: una sola dataclass y se declara explicitamente. Igual las tablas operativas,
#: cuyas filas son dicts construidos a mano.
FORECAST_SPEC: dict[str, Any] = {
    "series_key": str, "dimension": str, "dimension_value": str, "method": str,
    "generated_for_date": date, "history_days": int, "mape": float,
    "forecast_date": date, "predicted_cost_usd": float,
    "lower_bound_usd": float, "upper_bound_usd": float, "horizon_day": int,
    **AUDITORIA,
}

RUN_LOG_SPEC: dict[str, Any] = {
    "run_id": str, "pipeline_environment": str, "stage": str, "status": str,
    "duration_seconds": float, "rows_written": int, "details": dict,
    "error_message": str, "run_started_at": datetime, "run_date": date,
}

WATERMARK_SPEC: dict[str, Any] = {
    "source_key": str, "watermark_date": date, "rows_ingested": int,
    "run_id": str, "pipeline_environment": str, "updated_at": datetime, "details": dict,
}


def esquemas():
    """Se construyen de forma perezosa: requieren pyspark."""
    from .alerting.rules import Alert
    from .analytics.anomaly import AnomalyResult
    from .analytics.budgets import BudgetStatus
    from .analytics.chargeback import ChargebackLine
    from .analytics.optimization import Recommendation
    from .quality.checks import CheckResult
    from .spark_utils import schema_from_dataclass

    return {
        "anomaly": schema_from_dataclass(AnomalyResult, extra=AUDITORIA),
        "budget": schema_from_dataclass(BudgetStatus, extra=AUDITORIA),
        "recommendation": schema_from_dataclass(
            Recommendation, extra={**AUDITORIA, "analysis_date": date}
        ),
        "chargeback": schema_from_dataclass(ChargebackLine, extra=AUDITORIA),
        "quality": schema_from_dataclass(CheckResult, extra={"run_id": str, "pipeline_environment": str}),
        "alert": schema_from_dataclass(
            Alert,
            extra={
                "run_id": str, "pipeline_environment": str, "dispatch_status": str,
                "channels": str, "delivered": bool, "delivery_detail": str,
            },
        ),
        "forecast": _struct(FORECAST_SPEC),
        "run_log": _struct(RUN_LOG_SPEC),
        "watermark": _struct(WATERMARK_SPEC),
    }


def _struct(campos: dict[str, Any]):
    """StructType desde {nombre: tipo_python}, para filas que no son dataclass."""
    from pyspark.sql import types as T

    from .spark_utils import spark_type_for

    return T.StructType([T.StructField(n, spark_type_for(tp), True) for n, tp in campos.items()])




#: Tabla del modelo a la que corresponde cada esquema.
#:
#: Se usa para crearlas vacias en el arranque: son tablas que solo se escriben
#: cuando hay resultados, y varias tardan semanas en tenerlos (las anomalias y
#: el pronostico necesitan historia). Sin la tabla, los dashboards fallan con
#: TABLE_OR_VIEW_NOT_FOUND en vez de mostrarse vacios.
def tablas_con_esquema():
    """Devuelve [(TableDef, StructType)] de todas las tablas escritas desde Python."""
    from .catalog import (
        GOLD_ANOMALY,
        GOLD_BUDGET_STATUS,
        GOLD_CHARGEBACK,
        GOLD_FORECAST,
        GOLD_RECOMMENDATION,
        OPS_ALERTS,
        OPS_QUALITY,
        OPS_RUN_LOG,
        OPS_WATERMARK,
    )

    destino = {
        "anomaly": GOLD_ANOMALY,
        "forecast": GOLD_FORECAST,
        "budget": GOLD_BUDGET_STATUS,
        "recommendation": GOLD_RECOMMENDATION,
        "chargeback": GOLD_CHARGEBACK,
        "quality": OPS_QUALITY,
        "alert": OPS_ALERTS,
        "run_log": OPS_RUN_LOG,
        "watermark": OPS_WATERMARK,
    }
    construidos = esquemas()
    return [(destino[clave], construidos[clave]) for clave in destino]
