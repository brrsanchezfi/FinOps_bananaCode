"""Coherencia entre las filas que produce el codigo y los esquemas declarados.

Los esquemas de las tablas derivadas se construyen desde las anotaciones de las
dataclasses (`schema_from_dataclass`). Si un `to_row()` deja de coincidir con los
campos de su dataclass, la escritura fallaria en produccion con columnas nulas o
ausentes. Esta prueba lo detecta sin necesidad de Spark.
"""

from __future__ import annotations

import dataclasses
from datetime import date, datetime, timezone

from finops.alerting.rules import Alert
from finops.analytics.anomaly import AnomalyResult
from finops.analytics.budgets import evaluate_budget
from finops.analytics.chargeback import allocate
from finops.analytics.forecast import forecast_series
from finops.analytics.optimization import evaluate_all as evaluar_recomendaciones
from finops.quality.checks import evaluate_row_count
from finops.schemas import AUDITORIA, FORECAST_SPEC, RUN_LOG_SPEC, WATERMARK_SPEC


def campos(cls) -> set[str]:
    return {f.name for f in dataclasses.fields(cls)}


class TestFilasVsDataclass:
    """Cada to_row() debe producir exactamente los campos de su dataclass."""

    def test_budget_status(self):
        estado = evaluate_budget(
            {"id": "p", "name": "P", "scope": {}, "period": "monthly", "amount_usd": 100.0},
            [{"usage_date": date(2026, 3, 1), "total_cost_usd": 10.0}],
            as_of=date(2026, 3, 15),
        )
        assert set(estado.to_row()) == campos(type(estado))

    def test_anomaly_result(self):
        anomalia = AnomalyResult(
            "s", "team", "A", date(2026, 3, 1), 1.0, 2.0, -1.0, -0.5, -4.0,
            "DROP", "medium", "mad", 10, 28,
        )
        assert set(anomalia.to_row()) == campos(AnomalyResult)

    def test_recommendation(self):
        perfil = {
            "entity_key": "CLUSTER:c1", "entity_type": "CLUSTER", "cost_usd": 5000.0,
            "analysis_days": 30, "days_since_activity": 40,
        }
        recomendaciones = evaluar_recomendaciones([perfil], {"enabled": True, "rules": {}})
        assert recomendaciones, "el perfil deberia generar al menos una recomendacion"
        assert set(recomendaciones[0].to_row()) == campos(type(recomendaciones[0]))

    def test_chargeback_line(self):
        lineas = allocate(
            [{"cost_center": "CC", "total_cost_usd": 10.0, "entity_key": "JOB:1"}],
            period="2026-03",
            config={"allocation_dimension": "cost_center"},
        )
        assert set(lineas[0].to_row()) == campos(type(lineas[0]))

    def test_check_result(self):
        resultado = evaluate_row_count(10, 1, "t")
        assert set(resultado.to_row()) == campos(type(resultado))

    def test_alert(self):
        alerta = Alert("R", "high", "t", "m", "abc", "ORG")
        assert set(alerta.to_row()) == campos(Alert)


class TestEspecificacionesPlanas:
    """Los esquemas que no vienen de una dataclass deben cubrir sus filas."""

    def test_forecast(self):
        resultado = forecast_series(
            [(date(2026, 1, 1) + __import__("datetime").timedelta(days=i), 100.0) for i in range(40)],
            horizon_days=3,
            dimension="team",
            dimension_value="A",
        )
        claves_fila = set(resultado.to_rows()[0])
        declaradas = set(FORECAST_SPEC) - set(AUDITORIA)
        assert claves_fila == declaradas, (
            f"faltan en el esquema: {claves_fila - declaradas}; "
            f"sobran: {declaradas - claves_fila}"
        )

    def test_run_log(self):
        from finops.logging_utils import RunRecorder, StageMetric

        grabador = RunRecorder()
        grabador.add(StageMetric(stage="x", status="ok", duration_seconds=1.0, rows=5))
        fila = {
            **grabador.as_rows("r1", "dev")[0],
            "run_started_at": datetime.now(timezone.utc),
            "run_date": date(2026, 3, 1),
        }
        assert set(fila) == set(RUN_LOG_SPEC), (
            f"faltan: {set(fila) - set(RUN_LOG_SPEC)}; sobran: {set(RUN_LOG_SPEC) - set(fila)}"
        )

    def test_watermark(self):
        fila = {
            "source_key": "billing_usage",
            "watermark_date": date(2026, 3, 1),
            "rows_ingested": 10,
            "run_id": "r1",
            "pipeline_environment": "dev",
            "updated_at": datetime.now(timezone.utc),
            "details": {"k": "v"},
        }
        assert set(fila) == set(WATERMARK_SPEC)


class TestAuditoria:
    def test_columnas_de_auditoria_declaradas(self):
        assert set(AUDITORIA) == {"run_id", "pipeline_environment", "generated_at"}


class TestColisionDeNombres:
    """La columna de auditoria no puede chocar con un campo de la dataclass.

    `Recommendation` tiene un campo `environment` (el ambiente del recurso, que
    viene de sus etiquetas). Llamar igual a la columna de auditoria (el ambiente
    del pipeline) hacia que Delta rechazara la tabla con COLUMN_ALREADY_EXISTS.
    """

    def test_ninguna_dataclass_choca_con_la_auditoria(self):
        from finops.analytics.anomaly import AnomalyResult
        from finops.analytics.budgets import BudgetStatus
        from finops.analytics.chargeback import ChargebackLine
        from finops.analytics.optimization import Recommendation
        from finops.quality.checks import CheckResult

        for cls in (AnomalyResult, BudgetStatus, ChargebackLine, Recommendation, CheckResult, Alert):
            choques = campos(cls) & set(AUDITORIA)
            assert choques == set(), f"{cls.__name__} choca con la auditoria en {choques}"

    def test_recommendation_conserva_su_environment_de_etiqueta(self):
        """El ambiente del recurso sigue disponible: lo usa el dashboard."""
        assert "environment" in campos(Recommendation)
        assert "environment" not in AUDITORIA


def _importar_recommendation():
    from finops.analytics.optimization import Recommendation

    return Recommendation


Recommendation = _importar_recommendation()


class TestTiposDeSpark:
    """`spark_type_for` debe cubrir tambien los tipos pelados de las specs planas."""

    def _simple(self, anotacion) -> str:
        from finops.spark_utils import spark_type_for

        return spark_type_for(anotacion).simpleString()

    def test_dict_pelado_es_map(self):
        """Regresion: `dict` sin parametrizar caia al respaldo StringType y la
        columna `details` se escribia como texto en vez de mapa."""
        assert self._simple(dict) == "map<string,string>"

    def test_dict_parametrizado_es_map(self):
        assert self._simple(dict[str, str]) == "map<string,string>"

    def test_list_pelada_es_array(self):
        assert self._simple(list) == "array<string>"

    def test_opcionales(self):
        assert self._simple(float | None) == "double"
        assert self._simple(int | None) == "bigint"
        assert self._simple(str | None) == "string"

    def test_bool_antes_que_int(self):
        """bool es subclase de int en Python; el orden importa."""
        assert self._simple(bool) == "boolean"
        assert self._simple(int) == "bigint"

    def test_fechas(self):
        assert self._simple(date) == "date"
        assert self._simple(datetime) == "timestamp"


class TestTiposDeLasTablasOperativas:
    """Las columnas de metadatos deben ser MAP, no STRUCT ni STRING.

    Con STRUCT, cada clave nueva en `details` cambiaria el esquema de la tabla.
    """

    def test_columnas_de_metadatos_son_map(self):
        from finops.schemas import esquemas

        esperado = {
            "watermark": "details",
            "run_log": "details",
            "budget": "details",
            "alert": "context",
            "recommendation": "evidence",
            "chargeback": "details",
        }
        esq = esquemas()
        for tabla, columna in esperado.items():
            campo = next(f for f in esq[tabla].fields if f.name == columna)
            assert campo.dataType.simpleString() == "map<string,string>", (
                f"{tabla}.{columna} es {campo.dataType.simpleString()}"
            )
