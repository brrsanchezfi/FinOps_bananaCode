"""Motor de recomendaciones de optimizacion de costo.

Cada regla es una funcion pura que recibe el *perfil* de una entidad de consumo
(un dict con metricas agregadas de la ventana de analisis) y la configuracion de
la regla, y devuelve una `Recommendation` o None.

El ahorro estimado es siempre una **estimacion acotada**: se documenta el metodo
en `estimation_method` y se acompana de un nivel de confianza, para que quien
lea el dashboard sepa cuanto pesar la cifra. Ninguna regla promete ahorro sin
explicar de donde sale.

Perfil de entidad esperado (todas las claves son opcionales; una regla que no
encuentra sus insumos simplemente no aplica):

    entity_key, entity_type, entity_id, entity_name, workspace_id,
    team, cost_center, project, environment, owner,
    cost_usd, cost_usd_prev, sku_group, is_serverless, is_photon,
    autotermination_minutes, autoscale_enabled, min_workers, max_workers,
    num_workers, spark_version, node_type_id, is_single_node,
    run_count, failed_run_count, avg_duration_minutes, last_activity_date,
    query_count, active_hours, warehouse_size, warehouse_type, enable_photon,
    untagged_cost_usd, analysis_days
"""

from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from datetime import date
from typing import Any

CONFIDENCE_HIGH = "alta"
CONFIDENCE_MEDIUM = "media"
CONFIDENCE_LOW = "baja"

_DBR_VERSION = re.compile(r"^(\d+)\.(\d+)")


@dataclass
class Recommendation:
    """Recomendacion accionable de optimizacion."""

    rule_id: str
    title: str
    entity_key: str
    entity_type: str
    entity_id: str | None
    entity_name: str | None
    workspace_id: str | None
    team: str | None
    cost_center: str | None
    environment: str | None
    owner: str | None
    observed_cost_usd: float
    estimated_monthly_savings_usd: float
    savings_pct: float
    confidence: str
    severity: str
    estimation_method: str
    recommendation: str
    evidence: dict[str, Any] = field(default_factory=dict)

    def to_row(self) -> dict[str, Any]:
        fila = asdict(self)
        fila["evidence"] = {k: str(v) for k, v in self.evidence.items()}
        return fila


def _monthly(cost: float, analysis_days: int) -> float:
    """Normaliza un costo observado en N dias a base mensual (30 dias)."""
    if analysis_days <= 0:
        return 0.0
    return cost / analysis_days * 30.0


def _severity_from_savings(savings: float) -> str:
    if savings >= 1000:
        return "critical"
    if savings >= 300:
        return "high"
    if savings >= 100:
        return "medium"
    return "low"


def _base_fields(profile: dict[str, Any]) -> dict[str, Any]:
    return {
        "entity_key": str(profile.get("entity_key") or "SIN_ENTIDAD"),
        "entity_type": str(profile.get("entity_type") or "UNKNOWN"),
        "entity_id": profile.get("entity_id"),
        "entity_name": profile.get("entity_name"),
        "workspace_id": profile.get("workspace_id"),
        "team": profile.get("team"),
        "cost_center": profile.get("cost_center"),
        "environment": profile.get("environment"),
        "owner": profile.get("owner"),
    }


def _make(
    profile: dict[str, Any],
    *,
    rule_id: str,
    title: str,
    savings: float,
    confidence: str,
    method: str,
    recommendation: str,
    evidence: dict[str, Any],
) -> Recommendation:
    costo = float(profile.get("cost_usd") or 0.0)
    dias = int(profile.get("analysis_days") or 30)
    costo_mensual = _monthly(costo, dias)
    ahorro = round(max(0.0, savings), 2)
    return Recommendation(
        rule_id=rule_id,
        title=title,
        observed_cost_usd=round(costo, 2),
        estimated_monthly_savings_usd=ahorro,
        savings_pct=round(ahorro / costo_mensual, 4) if costo_mensual > 0 else 0.0,
        confidence=confidence,
        severity=_severity_from_savings(ahorro),
        estimation_method=method,
        recommendation=recommendation,
        evidence=evidence,
        **_base_fields(profile),
    )


# ---------------------------------------------------------------------------
# Reglas
# ---------------------------------------------------------------------------
def rule_no_autotermination(profile: dict[str, Any], cfg: dict[str, Any]) -> Recommendation | None:
    """Cluster interactivo sin auto-terminacion o con umbral demasiado alto."""
    if profile.get("entity_type") != "CLUSTER" or profile.get("is_serverless"):
        return None
    minutos = profile.get("autotermination_minutes")
    if minutos is None:
        return None
    maximo = int(cfg.get("max_autotermination_minutes", 60))
    minutos = int(minutos)
    if 0 < minutos <= maximo:
        return None

    dias = int(profile.get("analysis_days") or 30)
    costo_mensual = _monthly(float(profile.get("cost_usd") or 0.0), dias)
    ratio = float(cfg.get("assumed_waste_ratio", 0.15))
    # Sin auto-terminacion el desperdicio esperado es mayor que con un umbral alto.
    factor = 2.0 if minutos == 0 else 1.0
    ahorro = costo_mensual * ratio * factor

    detalle = "sin auto-terminacion" if minutos == 0 else f"auto-terminacion en {minutos} min"
    return _make(
        profile,
        rule_id="NO_AUTOTERMINATION",
        title=f"Cluster {detalle}",
        savings=ahorro,
        confidence=CONFIDENCE_MEDIUM if minutos == 0 else CONFIDENCE_LOW,
        method=f"costo_mensual x {ratio} x {factor} (desperdicio asumido por tiempo ocioso)",
        recommendation=(
            f"Configurar `autotermination_minutes` en {maximo} minutos o menos para el cluster "
            f"{profile.get('entity_name') or profile.get('entity_id')}. "
            "Un cluster interactivo encendido fuera de uso factura DBU e infraestructura."
        ),
        evidence={"autotermination_minutes": minutos, "max_recomendado": maximo, "analysis_days": dias},
    )


def rule_all_purpose_for_jobs(profile: dict[str, Any], cfg: dict[str, Any]) -> Recommendation | None:
    """Jobs ejecutandose sobre compute all-purpose (tarifa DBU mas cara)."""
    if str(profile.get("sku_group")) != "ALL_PURPOSE":
        return None
    corridas = int(profile.get("run_count") or 0)
    if corridas < int(cfg.get("min_runs", 5)):
        return None

    dias = int(profile.get("analysis_days") or 30)
    costo_mensual = _monthly(float(profile.get("cost_usd") or 0.0), dias)
    ratio = float(cfg.get("assumed_savings_ratio", 0.45))
    return _make(
        profile,
        rule_id="ALL_PURPOSE_FOR_JOBS",
        title="Carga automatizada sobre compute all-purpose",
        savings=costo_mensual * ratio,
        confidence=CONFIDENCE_HIGH,
        method=f"costo_mensual x {ratio} (diferencial tarifario all-purpose vs jobs compute)",
        recommendation=(
            f"Migrar las {corridas} ejecuciones a un job cluster dedicado (o a jobs serverless). "
            "El SKU de jobs compute cuesta significativamente menos por DBU que all-purpose."
        ),
        evidence={"run_count": corridas, "sku_group": profile.get("sku_group"), "analysis_days": dias},
    )


def rule_legacy_runtime(profile: dict[str, Any], cfg: dict[str, Any]) -> Recommendation | None:
    """Runtime de Databricks antiguo: peor rendimiento por DBU y sin soporte."""
    version = profile.get("spark_version")
    if not version:
        return None
    coincidencia = _DBR_VERSION.match(str(version))
    if not coincidencia:
        return None
    mayor = int(coincidencia.group(1))
    minimo = int(cfg.get("min_dbr_major", 14))
    if mayor >= minimo:
        return None

    dias = int(profile.get("analysis_days") or 30)
    costo_mensual = _monthly(float(profile.get("cost_usd") or 0.0), dias)
    ratio = float(cfg.get("assumed_savings_ratio", 0.10))
    return _make(
        profile,
        rule_id="LEGACY_RUNTIME",
        title=f"Runtime obsoleto (DBR {mayor}.x)",
        savings=costo_mensual * ratio,
        confidence=CONFIDENCE_LOW,
        method=f"costo_mensual x {ratio} (mejora tipica de rendimiento por DBU al actualizar DBR)",
        recommendation=(
            f"Actualizar a DBR {minimo}.x LTS o superior. Ademas del ahorro por eficiencia, "
            "las versiones fuera de soporte no reciben parches de seguridad."
        ),
        evidence={"spark_version": version, "dbr_major": mayor, "min_requerido": minimo},
    )


def rule_failed_run_waste(profile: dict[str, Any], cfg: dict[str, Any]) -> Recommendation | None:
    """Costo consumido por ejecuciones que terminan en error."""
    corridas = int(profile.get("run_count") or 0)
    fallidas = int(profile.get("failed_run_count") or 0)
    if corridas <= 0 or fallidas < int(cfg.get("min_failed_runs", 3)):
        return None
    tasa = fallidas / corridas
    if tasa < float(cfg.get("min_failure_ratio", 0.15)):
        return None

    dias = int(profile.get("analysis_days") or 30)
    costo_mensual = _monthly(float(profile.get("cost_usd") or 0.0), dias)
    # El desperdicio se estima proporcional a la tasa de fallo.
    return _make(
        profile,
        rule_id="FAILED_RUN_WASTE",
        title=f"{fallidas} de {corridas} ejecuciones fallidas",
        savings=costo_mensual * tasa,
        confidence=CONFIDENCE_MEDIUM,
        method="costo_mensual x tasa_de_fallo (el compute de una corrida fallida es costo sin valor)",
        recommendation=(
            f"Estabilizar el job: {tasa:.0%} de las ejecuciones fallan. Revisar reintentos, "
            "dependencias de datos y timeouts. Considerar `max_retries` acotado y alertas de fallo."
        ),
        evidence={"run_count": corridas, "failed_run_count": fallidas, "failure_ratio": round(tasa, 4)},
    )


def rule_zombie_entity(profile: dict[str, Any], cfg: dict[str, Any]) -> Recommendation | None:
    """Entidad que sigue generando costo sin actividad reciente."""
    dias_inactiva = profile.get("days_since_activity")
    if dias_inactiva is None:
        return None
    umbral = int(cfg.get("idle_days", 14))
    if int(dias_inactiva) < umbral:
        return None
    costo = float(profile.get("cost_usd") or 0.0)
    if costo <= 0:
        return None

    dias = int(profile.get("analysis_days") or 30)
    costo_mensual = _monthly(costo, dias)
    return _make(
        profile,
        rule_id="ZOMBIE_ENTITY",
        title=f"Recurso sin actividad hace {int(dias_inactiva)} dias",
        savings=costo_mensual,
        confidence=CONFIDENCE_HIGH,
        method="costo_mensual completo (el recurso no registra actividad; el ahorro es su eliminacion)",
        recommendation=(
            f"Eliminar o suspender {profile.get('entity_name') or profile.get('entity_key')}: "
            f"genera costo pero no registra ejecuciones ni consultas hace {int(dias_inactiva)} dias."
        ),
        evidence={"days_since_activity": int(dias_inactiva), "last_activity_date": profile.get("last_activity_date")},
    )


def rule_no_autoscale(profile: dict[str, Any], cfg: dict[str, Any]) -> Recommendation | None:
    """Cluster con numero fijo de workers y tamano relevante."""
    if profile.get("entity_type") != "CLUSTER":
        return None
    if profile.get("autoscale_enabled"):
        return None
    workers = profile.get("num_workers")
    if workers is None:
        return None
    minimo = int(cfg.get("min_workers_fixed", 4))
    if int(workers) < minimo:
        return None

    dias = int(profile.get("analysis_days") or 30)
    costo_mensual = _monthly(float(profile.get("cost_usd") or 0.0), dias)
    ratio = float(cfg.get("assumed_savings_ratio", 0.15))
    return _make(
        profile,
        rule_id="NO_AUTOSCALE",
        title=f"Cluster fijo de {int(workers)} workers sin autoescalado",
        savings=costo_mensual * ratio,
        confidence=CONFIDENCE_LOW,
        method=f"costo_mensual x {ratio} (capacidad ociosa tipica en clusters de tamano fijo)",
        recommendation=(
            f"Habilitar autoescalado con min_workers menor a {int(workers)}. "
            "Un cluster fijo paga el pico durante toda la ventana de ejecucion."
        ),
        evidence={"num_workers": int(workers), "autoscale_enabled": False},
    )


def rule_oversized_warehouse(profile: dict[str, Any], cfg: dict[str, Any]) -> Recommendation | None:
    """SQL warehouse con poco trafico de consultas para su costo."""
    if profile.get("entity_type") != "WAREHOUSE":
        return None
    consultas = profile.get("query_count")
    horas = float(profile.get("active_hours") or 0.0)
    if consultas is None or horas <= 0:
        return None
    consultas_por_hora = float(consultas) / horas
    if consultas_por_hora > float(cfg.get("max_queries_per_hour", 5)):
        return None

    dias = int(profile.get("analysis_days") or 30)
    costo_mensual = _monthly(float(profile.get("cost_usd") or 0.0), dias)
    ratio = float(cfg.get("assumed_savings_ratio", 0.40))
    return _make(
        profile,
        rule_id="OVERSIZED_WAREHOUSE",
        title=f"Warehouse con {consultas_por_hora:.1f} consultas/hora activa",
        savings=costo_mensual * ratio,
        confidence=CONFIDENCE_MEDIUM,
        method=f"costo_mensual x {ratio} (reduccion de un escalon de tamano)",
        recommendation=(
            f"Reducir el tamano del warehouse {profile.get('entity_name') or profile.get('entity_id')} "
            "un escalon y/o bajar `auto_stop_mins`. El trafico observado no justifica la capacidad actual."
        ),
        evidence={
            "query_count": int(consultas),
            "active_hours": round(horas, 2),
            "queries_per_hour": round(consultas_por_hora, 3),
            "warehouse_size": profile.get("warehouse_size"),
        },
    )


def rule_serverless_candidate(profile: dict[str, Any], cfg: dict[str, Any]) -> Recommendation | None:
    """Jobs cortos y frecuentes: candidatos a serverless (sin costo de arranque)."""
    if profile.get("is_serverless"):
        return None
    corridas = int(profile.get("run_count") or 0)
    duracion = profile.get("avg_duration_minutes")
    if duracion is None or corridas < int(cfg.get("min_runs", 20)):
        return None
    if float(duracion) > float(cfg.get("max_avg_runtime_minutes", 12)):
        return None

    dias = int(profile.get("analysis_days") or 30)
    costo_mensual = _monthly(float(profile.get("cost_usd") or 0.0), dias)
    ratio = float(cfg.get("assumed_savings_ratio", 0.20))
    return _make(
        profile,
        rule_id="SERVERLESS_CANDIDATE",
        title=f"Job corto y frecuente ({corridas} corridas, {float(duracion):.1f} min promedio)",
        savings=costo_mensual * ratio,
        confidence=CONFIDENCE_LOW,
        method=f"costo_mensual x {ratio} (eliminacion del tiempo de arranque de cluster clasico)",
        recommendation=(
            "Evaluar jobs serverless: en cargas cortas y frecuentes el arranque del cluster clasico "
            "representa una fraccion significativa del tiempo facturado. Validar con una prueba A/B."
        ),
        evidence={"run_count": corridas, "avg_duration_minutes": round(float(duracion), 2)},
    )


def rule_untagged_spend(profile: dict[str, Any], cfg: dict[str, Any]) -> Recommendation | None:
    """Gasto que no puede imputarse a ningun responsable."""
    sin_etiqueta = float(profile.get("untagged_cost_usd") or 0.0)
    if sin_etiqueta < float(cfg.get("min_untagged_cost_usd", 100.0)):
        return None
    return _make(
        profile,
        rule_id="UNTAGGED_SPEND",
        title=f"USD {sin_etiqueta:,.0f} de gasto sin atribucion",
        savings=0.0,  # gobierno, no ahorro directo
        confidence=CONFIDENCE_HIGH,
        method="no aplica: hallazgo de gobierno, no de ahorro directo",
        recommendation=(
            "Aplicar etiquetas obligatorias (cost_center, team, environment) mediante politicas de "
            "cluster y plantillas de job. Sin atribucion no hay responsable del gasto."
        ),
        evidence={"untagged_cost_usd": round(sin_etiqueta, 2)},
    )


def rule_photon_opportunity(profile: dict[str, Any], cfg: dict[str, Any]) -> Recommendation | None:
    """Cargas SQL/ETL grandes sin Photon habilitado."""
    if profile.get("is_photon"):
        return None
    if str(profile.get("sku_group")) not in {"JOBS", "SQL", "DLT"}:
        return None
    dias = int(profile.get("analysis_days") or 30)
    costo_mensual = _monthly(float(profile.get("cost_usd") or 0.0), dias)
    if costo_mensual < float(cfg.get("min_monthly_cost_usd", 500.0)):
        return None

    ratio = float(cfg.get("assumed_savings_ratio", 0.12))
    return _make(
        profile,
        rule_id="PHOTON_OPPORTUNITY",
        title="Carga de alto costo sin Photon",
        savings=costo_mensual * ratio,
        confidence=CONFIDENCE_LOW,
        method=(
            f"costo_mensual x {ratio} (ganancia neta tipica: Photon acelera la ejecucion mas de lo que "
            "encarece la tarifa DBU, solo en cargas vectorizables)"
        ),
        recommendation=(
            "Probar Photon en esta carga y comparar costo total por ejecucion. Photon sube la tarifa "
            "por DBU, asi que solo conviene si la reduccion de tiempo la compensa: medir antes de adoptar."
        ),
        evidence={"sku_group": profile.get("sku_group"), "monthly_cost_usd": round(costo_mensual, 2)},
    )


#: Registro rule_id -> (clave_en_config, funcion)
RULES: dict[str, tuple[str, Callable[[dict[str, Any], dict[str, Any]], Recommendation | None]]] = {
    "NO_AUTOTERMINATION": ("no_autotermination", rule_no_autotermination),
    "ALL_PURPOSE_FOR_JOBS": ("all_purpose_for_jobs", rule_all_purpose_for_jobs),
    "LEGACY_RUNTIME": ("legacy_runtime", rule_legacy_runtime),
    "FAILED_RUN_WASTE": ("failed_run_waste", rule_failed_run_waste),
    "ZOMBIE_ENTITY": ("zombie_entity", rule_zombie_entity),
    "NO_AUTOSCALE": ("no_autoscale", rule_no_autoscale),
    "OVERSIZED_WAREHOUSE": ("oversized_warehouse", rule_oversized_warehouse),
    "SERVERLESS_CANDIDATE": ("serverless_candidate", rule_serverless_candidate),
    "UNTAGGED_SPEND": ("untagged_spend", rule_untagged_spend),
    "PHOTON_OPPORTUNITY": ("photon_opportunity", rule_photon_opportunity),
}

#: Reglas de gobierno que se reportan aunque su ahorro estimado sea 0.
GOVERNANCE_RULES = frozenset({"UNTAGGED_SPEND"})


def evaluate_profile(profile: dict[str, Any], optimization_cfg: dict[str, Any]) -> list[Recommendation]:
    """Aplica todas las reglas habilitadas a un perfil de entidad."""
    reglas_cfg = (optimization_cfg or {}).get("rules", {}) or {}
    minimo = float((optimization_cfg or {}).get("min_monthly_savings_usd", 0.0))

    salida: list[Recommendation] = []
    for rule_id, (clave, funcion) in RULES.items():
        cfg_regla = reglas_cfg.get(clave) or {}
        if not cfg_regla.get("enabled", True):
            continue
        try:
            resultado = funcion(profile, cfg_regla)
        except (TypeError, ValueError):
            # Un perfil incompleto no debe tumbar el motor completo.
            continue
        if resultado is None:
            continue
        if rule_id not in GOVERNANCE_RULES and resultado.estimated_monthly_savings_usd < minimo:
            continue
        salida.append(resultado)
    return salida


def evaluate_all(
    profiles: list[dict[str, Any]], optimization_cfg: dict[str, Any]
) -> list[Recommendation]:
    """Evalua todos los perfiles y ordena por ahorro estimado descendente."""
    if not (optimization_cfg or {}).get("enabled", True):
        return []
    salida: list[Recommendation] = []
    for perfil in profiles:
        salida.extend(evaluate_profile(perfil, optimization_cfg))
    salida.sort(key=lambda r: -r.estimated_monthly_savings_usd)
    return salida


def savings_summary(recommendations: list[Recommendation]) -> dict[str, Any]:
    """Resumen agregado del potencial de ahorro identificado."""
    por_regla: dict[str, dict[str, Any]] = {}
    for rec in recommendations:
        acumulado = por_regla.setdefault(rec.rule_id, {"count": 0, "savings_usd": 0.0})
        acumulado["count"] += 1
        acumulado["savings_usd"] = round(acumulado["savings_usd"] + rec.estimated_monthly_savings_usd, 2)
    return {
        "total_recommendations": len(recommendations),
        "total_monthly_savings_usd": round(sum(r.estimated_monthly_savings_usd for r in recommendations), 2),
        "by_rule": por_regla,
        "high_confidence_savings_usd": round(
            sum(r.estimated_monthly_savings_usd for r in recommendations if r.confidence == CONFIDENCE_HIGH), 2
        ),
    }


def build_profile_from_rows(
    entity_row: dict[str, Any], *, analysis_days: int, as_of: date | None = None
) -> dict[str, Any]:
    """Normaliza una fila de gold en el perfil que consumen las reglas."""
    perfil = dict(entity_row)
    perfil["analysis_days"] = analysis_days
    ultima = perfil.get("last_activity_date")
    if isinstance(ultima, date) and as_of:
        perfil["days_since_activity"] = (as_of - ultima).days
    return perfil
