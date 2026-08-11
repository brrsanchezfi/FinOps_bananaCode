"""Orquestacion de extremo a extremo del pipeline FinOps.

Etapas:

    setup      -> crea catalogo/schemas
    bronze     -> system tables a bronze (incremental por rango de fechas)
    silver     -> valorizacion, entidades y etiquetas
    gold       -> modelo dimensional
    analytics  -> anomalias, pronostico, presupuestos, recomendaciones, chargeback
    quality    -> chequeos de calidad
    alerts     -> reglas de alerta, deduplicacion y despacho
    maintenance-> OPTIMIZE/ZORDER y comentarios de catalogo

Cada etapa se puede ejecutar por separado (`stages=[...]`), lo que permite
reprocesar solo la analitica sin volver a ingerir, o correr las alertas con mayor
frecuencia que el pipeline completo.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable
from datetime import date, datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

from .alerting import rules as R
from .alerting.dispatcher import alerts_to_rows, dispatch
from .alerting.notifier import build_channels
from .analytics import budgets as B
from .analytics import chargeback as C
from .analytics import forecast as FC
from .analytics.anomaly import detect_many, group_points
from .analytics.optimization import evaluate_all as evaluate_recommendations
from .analytics.optimization import savings_summary
from .catalog import (
    GOLD_ANOMALY,
    GOLD_BUDGET_STATUS,
    GOLD_CHARGEBACK,
    GOLD_COST_DAILY,
    GOLD_FORECAST,
    GOLD_RECOMMENDATION,
    OPS_ALERTS,
    OPS_QUALITY,
    OPS_RUN_LOG,
    bootstrap,
)
from .catalog import (
    maintenance as run_maintenance,
)
from .config import FinOpsConfig
from .errors import PipelineError
from .ingestion.system_tables import run_bronze
from .ingestion.watermark import compute_window, read_watermarks, write_watermark
from .logging_utils import RunRecorder, configure_logging, get_logger, stage
from .quality.checks import enforce, run_checks
from .schemas import esquemas as _esquemas
from .spark_utils import (
    append_rows,
    configure_session,
    delete_date_range,
    merge_table,
    overwrite_table,
    replace_date_range,
    rows_to_dataframe,
    rows_to_dicts,
    table_exists,
)
from .transform.gold import build_entity_profiles, run_gold
from .transform.silver import run_silver

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import SparkSession

log = get_logger("pipeline")

ALL_STAGES = ("setup", "bronze", "silver", "gold", "analytics", "quality", "alerts", "maintenance")


class PipelineResult:
    """Resultado consolidado de una corrida."""

    def __init__(self, run_id: str, cfg: FinOpsConfig) -> None:
        self.run_id = run_id
        self.cfg = cfg
        self.recorder = RunRecorder()
        self.outputs: dict[str, Any] = {}
        self.started_at = datetime.now(timezone.utc)

    @property
    def ok(self) -> bool:
        return not self.recorder.failed

    def summary(self) -> str:
        duracion = (datetime.now(timezone.utc) - self.started_at).total_seconds()
        return (
            f"Corrida {self.run_id} | {self.cfg.describe()} | "
            f"{'OK' if self.ok else 'CON FALLAS'} en {duracion:.1f}s\n{self.recorder.summary()}"
        )


def new_run_id() -> str:
    return f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}-{uuid.uuid4().hex[:8]}"


# ---------------------------------------------------------------------------
# Etapas
# ---------------------------------------------------------------------------
def stage_setup(spark: SparkSession, cfg: FinOpsConfig, result: PipelineResult) -> None:
    with stage("setup.bootstrap", result.recorder) as metrica:
        creados = bootstrap(spark, cfg)
        metrica.details["schemas"] = ", ".join(creados)

    with stage("setup.views", result.recorder) as metrica:
        from .views import ensure_views

        vistas = ensure_views(spark, cfg)
        metrica.rows = len(vistas)
        metrica.details["vistas"] = ", ".join(vistas) or "ninguna"


def stage_bronze(spark: SparkSession, cfg: FinOpsConfig, result: PipelineResult) -> None:
    marcas = read_watermarks(spark, cfg)
    desde, hasta = compute_window(cfg, marcas.get("billing_usage"))
    log.info("Ventana efectiva de ingesta: [%s .. %s]", desde, hasta)

    filas = run_bronze(spark, cfg, result.run_id, recorder=result.recorder, min_date=desde, max_date=hasta)
    result.outputs["bronze"] = filas
    if filas.get("bronze.usage", 0) > 0:
        write_watermark(
            spark, cfg, "billing_usage", hasta,
            run_id=result.run_id, rows=filas["bronze.usage"], details={"min_date": desde},
        )


def stage_silver(spark: SparkSession, cfg: FinOpsConfig, result: PipelineResult) -> None:
    result.outputs["silver"] = run_silver(spark, cfg, result.run_id, recorder=result.recorder)


def stage_gold(spark: SparkSession, cfg: FinOpsConfig, result: PipelineResult) -> None:
    result.outputs["gold"] = run_gold(spark, cfg, result.run_id, recorder=result.recorder)


def stage_quality(spark: SparkSession, cfg: FinOpsConfig, result: PipelineResult) -> None:
    with stage("quality.checks", result.recorder) as metrica:
        resultados = run_checks(spark, cfg)
        filas = [
            {**r.to_row(), "run_id": result.run_id, "pipeline_environment": cfg.env} for r in resultados
        ]
        metrica.rows = append_rows(
            spark, filas, OPS_QUALITY.fqn(cfg), _esquemas()["quality"], dry_run=cfg.dry_run
        )
        fallidos = [r for r in resultados if not r.passed]
        metrica.details["fallidos"] = len(fallidos)
        result.outputs["quality"] = [
            {"check": r.check, "severity": r.severity, "message": r.message, "passed": r.passed}
            for r in resultados
        ]
        result.outputs["quality_failures"] = [
            {"check": r.check, "severity": r.severity, "message": r.message} for r in fallidos
        ]
        enforce(resultados, bool(cfg.get("quality.fail_pipeline_on_error", True)))


# ---------------------------------------------------------------------------
# Analitica
# ---------------------------------------------------------------------------
def _daily_series_by(spark: SparkSession, cfg: FinOpsConfig, dimension: str, since: date) -> list[dict[str, Any]]:
    """Serie diaria de costo agregada por una dimension, como lista de dicts."""
    from pyspark.sql import functions as F

    tabla = spark.table(GOLD_COST_DAILY.fqn(cfg))
    if dimension not in tabla.columns:
        log.warning("Dimension '%s' no existe en fct_cost_daily; se omite", dimension)
        return []
    agregado = (
        tabla.filter(F.col("usage_date") >= F.lit(since))
        .groupBy("usage_date", F.col(dimension).cast("string").alias("dim_value"))
        .agg(F.round(F.sum("total_cost_usd"), 6).alias("total_cost_usd"))
    )
    return rows_to_dicts(agregado)


def stage_analytics(spark: SparkSession, cfg: FinOpsConfig, result: PipelineResult) -> None:
    """Ejecuta anomalias, pronostico, presupuestos, recomendaciones y chargeback."""
    from pyspark.sql import functions as F

    propiedades = cfg.get("catalog.table_properties", {}) or {}
    hoy = cfg.max_date
    esquemas = _esquemas()

    # --- Anomalias ---
    anomalias_cfg = cfg.get("anomaly", {}) or {}
    todas_anomalias: list[Any] = []
    result.outputs["anomalies"] = todas_anomalias
    if anomalias_cfg.get("enabled", True):
        # Ventana de ENTRENAMIENTO: historia necesaria para construir la base.
        ventana_train = (
            int(anomalias_cfg.get("window_days", 28)) + int(anomalias_cfg.get("min_history_days", 14)) + 7
        )
        desde_train = hoy - timedelta(days=ventana_train)
        # Ventana de EVALUACION: los dias que efectivamente se re-analizan en
        # esta corrida. El reemplazo en la tabla debe cubrir SOLO este rango; si
        # cubriera el de entrenamiento, cada corrida borraria la historia de
        # anomalias que no vuelve a escribir.
        dias_evaluados = int(cfg.get("ingestion.lookback_days", 7)) + 1
        desde_eval = hoy - timedelta(days=dias_evaluados - 1)

        with stage("analytics.anomaly", result.recorder) as metrica:
            filas: list[dict[str, Any]] = []
            for dimension in anomalias_cfg.get("group_by_dimensions", []) or []:
                registros = _daily_series_by(spark, cfg, dimension, desde_train)
                if not registros:
                    continue
                series = group_points(registros, key_field="dim_value")
                detectadas = detect_many(
                    series, dimension=dimension, config=anomalias_cfg,
                    evaluate_last_n_days=dias_evaluados,
                )
                todas_anomalias.extend(detectadas)
                filas.extend(
                    {
                        **a.to_row(),
                        "run_id": result.run_id,
                        "pipeline_environment": cfg.env,
                        "generated_at": datetime.now(timezone.utc),
                    }
                    for a in detectadas
                )
            metrica.details["ventana_evaluada"] = f"{desde_eval} .. {hoy}"
            if filas:
                metrica.rows = replace_date_range(
                    spark, rows_to_dataframe(spark, filas, esquemas["anomaly"]), GOLD_ANOMALY.fqn(cfg),
                    date_column="usage_date", min_date=desde_eval, max_date=hoy,
                    properties=propiedades, dry_run=cfg.dry_run,
                )
            else:
                # Sin hallazgos igual hay que limpiar el rango: una anomalia
                # detectada ayer que hoy ya no lo es debe desaparecer.
                delete_date_range(
                    spark, GOLD_ANOMALY.fqn(cfg),
                    date_column="usage_date", min_date=desde_eval, max_date=hoy,
                    dry_run=cfg.dry_run,
                )
                metrica.rows = 0
                metrica.status = "skipped"

    # --- Pronostico ---
    pronosticos: list[FC.ForecastResult] = []
    forecast_cfg = cfg.get("forecast", {}) or {}
    if forecast_cfg.get("enabled", True):
        desde = hoy - timedelta(days=int(forecast_cfg.get("train_window_days", 120)))
        with stage("analytics.forecast", result.recorder) as metrica:
            filas = []
            # Serie total de la organizacion (usada por presupuestos globales).
            totales = _daily_series_by(spark, cfg, "workspace_id", desde)
            serie_org = group_points(
                [{**r, "dim_value": "ORGANIZACION"} for r in totales], key_field="dim_value"
            )
            resultado_org = FC.forecast_many(serie_org, dimension="organizacion", config=forecast_cfg, as_of=hoy)
            pronosticos.extend(resultado_org)

            for dimension in forecast_cfg.get("group_by_dimensions", []) or []:
                registros = _daily_series_by(spark, cfg, dimension, desde)
                if not registros:
                    continue
                series = group_points(registros, key_field="dim_value")
                pronosticos.extend(FC.forecast_many(series, dimension=dimension, config=forecast_cfg, as_of=hoy))

            for pronostico in pronosticos:
                filas.extend(
                    {**fila, "run_id": result.run_id, "pipeline_environment": cfg.env,
                     "generated_at": datetime.now(timezone.utc)}
                    for fila in pronostico.to_rows()
                )
            if filas:
                metrica.rows = overwrite_table(
                    spark, rows_to_dataframe(spark, filas, esquemas["forecast"]), GOLD_FORECAST.fqn(cfg),
                    properties=propiedades, dry_run=cfg.dry_run,
                )
            else:
                metrica.rows = 0
                metrica.status = "skipped"
            result.outputs["forecasts"] = pronosticos

    # --- Presupuestos ---
    estados: list[B.BudgetStatus] = []
    with stage("analytics.budgets", result.recorder) as metrica:
        # Se lee el ano corrido para poder evaluar presupuestos mensuales,
        # trimestrales y anuales con una sola lectura.
        inicio_periodo, _ = B.period_bounds("yearly", hoy)
        costo_diario = spark.table(GOLD_COST_DAILY.fqn(cfg))
        dimensiones_disponibles = [
            d for d in (cfg.get("tagging.dimensions") or []) if d in costo_diario.columns
        ]
        registros = rows_to_dicts(
            costo_diario.filter(F.col("usage_date") >= F.lit(inicio_periodo))
            .groupBy(
                "usage_date", "workspace_id", "sku_group", "entity_type", *dimensiones_disponibles
            )
            .agg(F.round(F.sum("total_cost_usd"), 6).alias("total_cost_usd"))
        )
        pronostico_org = next((p for p in pronosticos if p.dimension == "organizacion"), None)
        por_ambito = {}
        for presupuesto in (cfg.budgets.get("budgets") or []):
            ambito = presupuesto.get("scope") or {}
            if not ambito and pronostico_org is not None:
                por_ambito[str(presupuesto.get("id"))] = pronostico_org.points

        estados = B.evaluate_all(cfg.budgets, registros, as_of=hoy, forecasts_by_scope=por_ambito)
        filas = [
            {**e.to_row(), "run_id": result.run_id, "pipeline_environment": cfg.env,
             "generated_at": datetime.now(timezone.utc)}
            for e in estados
        ]
        if filas:
            metrica.rows = merge_table(
                spark, rows_to_dataframe(spark, filas, esquemas["budget"]), GOLD_BUDGET_STATUS.fqn(cfg),
                keys=["budget_id", "period_start", "as_of_date"],
                properties=propiedades, dry_run=cfg.dry_run,
            )
        else:
            metrica.rows = 0
            metrica.status = "skipped"
        result.outputs["budget_statuses"] = estados
        result.outputs["cost_records"] = registros

    # --- Recomendaciones ---
    optimizacion_cfg = cfg.get("optimization", {}) or {}
    if optimizacion_cfg.get("enabled", True):
        with stage("analytics.recommendations", result.recorder) as metrica:
            ventana = int(optimizacion_cfg.get("lookback_days", 30))
            perfiles_df = build_entity_profiles(spark, cfg, lookback_days=ventana)
            perfiles = rows_to_dicts(perfiles_df)
            recomendaciones = evaluate_recommendations(perfiles, optimizacion_cfg)
            filas = [
                {**r.to_row(), "run_id": result.run_id, "pipeline_environment": cfg.env,
                 "analysis_date": hoy, "generated_at": datetime.now(timezone.utc)}
                for r in recomendaciones
            ]
            if filas:
                metrica.rows = overwrite_table(
                    spark, rows_to_dataframe(spark, filas, esquemas["recommendation"]), GOLD_RECOMMENDATION.fqn(cfg),
                    properties=propiedades, dry_run=cfg.dry_run,
                )
            else:
                metrica.rows = 0
                metrica.status = "skipped"
            resumen = savings_summary(recomendaciones)
            metrica.details.update(
                {"ahorro_mensual_usd": resumen["total_monthly_savings_usd"], "recomendaciones": resumen["total_recommendations"]}
            )
            result.outputs["recommendations"] = recomendaciones
            result.outputs["savings_summary"] = resumen

    # --- Chargeback ---
    with stage("analytics.chargeback", result.recorder) as metrica:
        cfg_chargeback = cfg.budgets.get("chargeback", {}) or {}
        dimension = str(cfg_chargeback.get("allocation_dimension", "cost_center"))
        tabla = spark.table(GOLD_COST_DAILY.fqn(cfg))
        if dimension not in tabla.columns:
            metrica.status = "skipped"
            metrica.details["motivo"] = f"dimension '{dimension}' ausente"
            metrica.rows = 0
        else:
            meses = rows_to_dicts(
                tabla.filter(F.col("usage_date") >= F.lit(hoy - timedelta(days=400)))
                .groupBy("year_month", dimension, "entity_key", "entity_name")
                .agg(F.round(F.sum("total_cost_usd"), 6).alias("total_cost_usd"))
            )
            por_mes: dict[str, list[dict[str, Any]]] = {}
            for registro in meses:
                por_mes.setdefault(str(registro["year_month"]), []).append(registro)

            filas = []
            for periodo, registros_mes in sorted(por_mes.items()):
                lineas = C.allocate(registros_mes, period=periodo, config=cfg_chargeback,
                                    unallocated_value=str(cfg.get("tagging.unallocated_value", "SIN_ASIGNAR")))
                conciliacion = C.reconcile(
                    registros_mes, lineas, overhead_pct=float(cfg_chargeback.get("overhead_pct", 0.0) or 0.0)
                )
                if not conciliacion["reconciled"]:
                    log.warning("Chargeback %s no concilia: %s", periodo, conciliacion)
                filas.extend(
                    {**line.to_row(), "run_id": result.run_id, "pipeline_environment": cfg.env,
                     "generated_at": datetime.now(timezone.utc)}
                    for line in lineas
                )
            if filas:
                metrica.rows = merge_table(
                    spark, rows_to_dataframe(spark, filas, esquemas["chargeback"]), GOLD_CHARGEBACK.fqn(cfg),
                    keys=["period", "allocation_dimension", "unit"],
                    properties=propiedades, dry_run=cfg.dry_run,
                )
            else:
                metrica.rows = 0
                metrica.status = "skipped"


# ---------------------------------------------------------------------------
# Alertas
# ---------------------------------------------------------------------------
def _from_rows(cls: Any, rows: list[dict[str, Any]]) -> list[Any]:
    """Reconstruye dataclasses a partir de filas de gold.

    Se ignoran las columnas de auditoria (run_id, environment, generated_at) que
    no forman parte de la dataclass.
    """
    import dataclasses

    campos = {f.name for f in dataclasses.fields(cls)}
    salida = []
    for fila in rows:
        try:
            salida.append(cls(**{k: v for k, v in fila.items() if k in campos}))
        except TypeError as exc:  # esquema evolucionado: se omite la fila
            log.warning("No se pudo reconstruir %s desde gold: %s", cls.__name__, exc)
    return salida


def _load_budget_statuses(spark: SparkSession, cfg: FinOpsConfig) -> list[B.BudgetStatus]:
    """Lee el estado mas reciente de cada presupuesto desde gold."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    fqn = GOLD_BUDGET_STATUS.fqn(cfg)
    if not table_exists(spark, fqn):
        return []
    ventana = Window.partitionBy("budget_id").orderBy(F.col("as_of_date").desc())
    filas = rows_to_dicts(
        spark.table(fqn).withColumn("_rn", F.row_number().over(ventana)).filter(F.col("_rn") == 1).drop("_rn")
    )
    return _from_rows(B.BudgetStatus, filas)


def _load_anomalies(spark: SparkSession, cfg: FinOpsConfig) -> list[Any]:
    """Lee las anomalias detectadas en la ventana reciente desde gold."""
    from pyspark.sql import functions as F

    from .analytics.anomaly import AnomalyResult

    fqn = GOLD_ANOMALY.fqn(cfg)
    if not table_exists(spark, fqn):
        return []
    ventana = int(cfg.get("ingestion.lookback_days", 7)) + 1
    filas = rows_to_dicts(
        spark.table(fqn).filter(F.col("usage_date") >= F.lit(cfg.max_date - timedelta(days=ventana)))
    )
    return _from_rows(AnomalyResult, filas)


def _load_recent_failures(spark: SparkSession, cfg: FinOpsConfig, hours: int = 24) -> list[dict[str, Any]]:
    """Etapas fallidas del pipeline en las ultimas horas, segun ops_run_log."""
    from pyspark.sql import functions as F

    fqn = OPS_RUN_LOG.fqn(cfg)
    if not table_exists(spark, fqn):
        return []
    corte = datetime.now(timezone.utc) - timedelta(hours=hours)
    return rows_to_dicts(
        spark.table(fqn)
        .filter((F.col("status") == "error") & (F.col("run_started_at") >= F.lit(corte)))
        .select("stage", "status", "error_message", "run_id")
    )


def _alert_history(spark: SparkSession, cfg: FinOpsConfig) -> dict[str, datetime]:
    """Ultimo despacho por huella, dentro del periodo de enfriamiento."""
    from pyspark.sql import functions as F

    fqn = OPS_ALERTS.fqn(cfg)
    if not table_exists(spark, fqn):
        return {}
    horas = int(cfg.get("alerting.cooldown_hours", 24))
    corte = datetime.now(timezone.utc) - timedelta(hours=horas * 2)
    filas = (
        spark.table(fqn)
        .filter((F.col("dispatch_status") == "dispatched") & (F.col("created_at") >= F.lit(corte)))
        .groupBy("fingerprint")
        .agg(F.max("created_at").alias("last_seen"))
        .collect()
    )
    return {r["fingerprint"]: r["last_seen"] for r in filas}


def stage_alerts(
    spark: SparkSession,
    cfg: FinOpsConfig,
    result: PipelineResult,
    *,
    secret_resolver: Callable[[str, str], str] | None = None,
    dashboard_url: str = "",
) -> None:
    from pyspark.sql import functions as F

    with stage("alerts.build_and_dispatch", result.recorder) as metrica:
        alerting_cfg = cfg.get("alerting", {}) or {}

        # La etapa de alertas debe poder correr sola (job de alertamiento con
        # mayor frecuencia que el pipeline, o reintento aislado). Si la etapa de
        # analitica no corrio en este mismo proceso, los insumos se leen de gold.
        estados = result.outputs.get("budget_statuses")
        if estados is None:
            estados = _load_budget_statuses(spark, cfg)
            metrica.details["budget_source"] = "gold"
        anomalias = result.outputs.get("anomalies")
        if anomalias is None:
            anomalias = _load_anomalies(spark, cfg)
            metrica.details["anomaly_source"] = "gold"
        metricas_corrida = result.outputs.get("quality_failures") is not None
        fallas_pipeline = (
            result.recorder.as_rows(result.run_id, cfg.env)
            if metricas_corrida
            else _load_recent_failures(spark, cfg)
        )

        totales_diarios: list[tuple[date, float]] = []
        if table_exists(spark, GOLD_COST_DAILY.fqn(cfg)):
            filas = (
                spark.table(GOLD_COST_DAILY.fqn(cfg))
                .filter(F.col("usage_date") >= F.lit(cfg.max_date - timedelta(days=30)))
                .groupBy("usage_date")
                .agg(F.round(F.sum("total_cost_usd"), 4).alias("c"))
                .collect()
            )
            totales_diarios = [(r["usage_date"], float(r["c"] or 0.0)) for r in filas]

        nuevas_entidades: list[dict[str, Any]] = []
        from .catalog import GOLD_DIM_ENTITY

        if table_exists(spark, GOLD_DIM_ENTITY.fqn(cfg)):
            ventana = int((alerting_cfg.get("rules", {}).get("new_expensive_entity", {}) or {}).get("lookback_days", 7))
            nuevas_entidades = rows_to_dicts(
                spark.table(GOLD_DIM_ENTITY.fqn(cfg))
                .filter(F.col("first_seen_date") >= F.lit(cfg.max_date - timedelta(days=ventana)))
                .select(
                    "entity_key", "entity_name", "entity_type", "first_seen_date",
                    F.col("lifetime_cost_usd").alias("cost_usd"),
                    *[c for c in ("team", "owner_resolved") if c in spark.table(GOLD_DIM_ENTITY.fqn(cfg)).columns],
                )
            )
            for entidad in nuevas_entidades:
                entidad["owner"] = entidad.pop("owner_resolved", None)

        cobertura = {}
        from .catalog import GOLD_TAG_COVERAGE

        if table_exists(spark, GOLD_TAG_COVERAGE.fqn(cfg)):
            dimension_principal = str(
                (cfg.budgets.get("chargeback", {}) or {}).get("allocation_dimension", "cost_center")
            )
            fila = (
                spark.table(GOLD_TAG_COVERAGE.fqn(cfg))
                .filter((F.col("dimension") == dimension_principal) & (F.col("usage_date") == F.lit(cfg.max_date)))
                .agg(
                    F.sum("total_cost_usd").alias("total"),
                    F.sum("attributed_cost_usd").alias("attr"),
                )
                .collect()
            )
            if fila and fila[0]["total"]:
                total = float(fila[0]["total"])
                atribuido = float(fila[0]["attr"] or 0.0)
                cobertura = {
                    "dimension": dimension_principal,
                    "total_cost_usd": total,
                    "attributed_cost_usd": atribuido,
                    "unattributed_cost_usd": total - atribuido,
                    "coverage_ratio": atribuido / total if total > 0 else 1.0,
                }

        alertas = R.build_all(
            budget_statuses=estados,
            anomalies=anomalias,
            daily_totals=totales_diarios,
            new_entities=nuevas_entidades,
            coverage=cobertura or None,
            run_metrics=fallas_pipeline,
            quality_failures=result.outputs.get("quality_failures"),
            alerting_cfg=alerting_cfg,
            as_of=cfg.max_date,
            run_id=result.run_id,
        )

        canales = build_channels(
            alerting_cfg.get("channels"),
            secret_resolver=secret_resolver,
            env=cfg.env,
            dashboard_url=dashboard_url,
            dry_run=cfg.dry_run,
        )
        historial = _alert_history(spark, cfg)
        reporte = dispatch(alertas, canales, history=historial, config=alerting_cfg)

        suprimidas = [a for a in alertas if a not in reporte.alerts]
        filas_alerta = alerts_to_rows(
            reporte.alerts, reporte.deliveries, run_id=result.run_id, env=cfg.env, suppressed=suprimidas
        )
        if filas_alerta:
            append_rows(
                spark, filas_alerta, OPS_ALERTS.fqn(cfg), _esquemas()["alert"], dry_run=cfg.dry_run
            )

        metrica.rows = len(filas_alerta)
        metrica.details.update(
            {"generadas": reporte.generated, "despachadas": reporte.dispatched,
             "canales": ", ".join(c.name for c in canales) or "ninguno"}
        )
        result.outputs["alerts"] = reporte


def stage_maintenance(spark: SparkSession, cfg: FinOpsConfig, result: PipelineResult) -> None:
    from .catalog import apply_comments

    with stage("maintenance.optimize", result.recorder) as metrica:
        metrica.details["tablas_optimizadas"] = run_maintenance(spark, cfg)
        metrica.details["comentarios"] = apply_comments(spark, cfg)


# ---------------------------------------------------------------------------
# Punto de entrada
# ---------------------------------------------------------------------------
def run(
    cfg: FinOpsConfig,
    *,
    spark: SparkSession | None = None,
    stages: list[str] | tuple[str, ...] = ALL_STAGES,
    run_id: str | None = None,
    secret_resolver: Callable[[str, str], str] | None = None,
    dashboard_url: str = "",
    raise_on_error: bool = True,
) -> PipelineResult:
    """Ejecuta el pipeline FinOps.

    Args:
        cfg: configuracion efectiva (ver `finops.config.load_config`).
        spark: sesion existente; se crea una si es None.
        stages: subconjunto de `ALL_STAGES` a ejecutar, en ese orden.
        secret_resolver: callable(scope, key) -> str, tipicamente `dbutils.secrets.get`.
        dashboard_url: enlace que se incluye en las notificaciones.
        raise_on_error: si False, registra el error y continua con las etapas
            restantes (util para no perder el alertamiento si falla la ingesta).
    """
    from .spark_utils import get_spark

    configure_logging(str(cfg.get("runtime.log_level", "INFO")))
    sesion = spark or get_spark(f"finops-{cfg.env}")
    configure_session(sesion, cfg.get("runtime.shuffle_partitions", "auto"))

    resultado = PipelineResult(run_id or new_run_id(), cfg)
    log.info("=== FinOps %s === %s", resultado.run_id, cfg.describe())

    ejecutores: dict[str, Any] = {
        "setup": stage_setup,
        "bronze": stage_bronze,
        "silver": stage_silver,
        "gold": stage_gold,
        "analytics": stage_analytics,
        "quality": stage_quality,
        "alerts": lambda s, c, r: stage_alerts(
            s, c, r, secret_resolver=secret_resolver, dashboard_url=dashboard_url
        ),
        "maintenance": stage_maintenance,
    }

    seleccionadas = [s for s in ALL_STAGES if s in stages]
    if not seleccionadas:
        raise PipelineError("seleccion", f"Ninguna etapa valida en {stages}. Validas: {ALL_STAGES}")

    for nombre in seleccionadas:
        try:
            ejecutores[nombre](sesion, cfg, resultado)
        except Exception as exc:  # noqa: BLE001
            log.exception("Etapa '%s' fallo", nombre)
            if raise_on_error:
                _persist_run_log(sesion, cfg, resultado)
                raise PipelineError(nombre, exc) from exc

    _persist_run_log(sesion, cfg, resultado)
    log.info("%s", resultado.summary())
    return resultado


def _persist_run_log(spark: SparkSession, cfg: FinOpsConfig, result: PipelineResult) -> None:
    """Guarda la bitacora de la corrida (best-effort: nunca tumba el pipeline)."""
    try:
        filas = [
            {**fila, "run_started_at": result.started_at, "run_date": cfg.max_date}
            for fila in result.recorder.as_rows(result.run_id, cfg.env)
        ]
        append_rows(spark, filas, OPS_RUN_LOG.fqn(cfg), _esquemas()["run_log"], dry_run=cfg.dry_run)
    except Exception as exc:  # noqa: BLE001
        log.warning("No se pudo persistir la bitacora de la corrida: %s", exc)
