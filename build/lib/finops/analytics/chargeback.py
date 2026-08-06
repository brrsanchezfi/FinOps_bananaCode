"""Chargeback / showback: imputacion de costo a unidades de negocio.

El costo total del periodo se descompone en tres bloques:

  * **directo**       imputable a una unidad por sus etiquetas
  * **compartido**    plataforma / gobierno / monitoreo (entidades que hacen match
                      con `shared_entity_patterns`), util para todos
  * **no atribuido**  sin etiqueta resoluble

Los bloques compartido y no atribuido se reparten entre las unidades segun la
estrategia configurada (`proportional` al costo directo, `even` en partes
iguales, o `none` = se deja como una unidad propia). Finalmente se aplica un
recargo administrativo opcional.

Invariante que se verifica en pruebas: la suma del chargeback de todas las
unidades es igual al costo total del periodo x (1 + overhead), salvo redondeo.
"""

from __future__ import annotations

import fnmatch
from dataclasses import asdict, dataclass, field
from typing import Any

STRATEGY_PROPORTIONAL = "proportional"
STRATEGY_EVEN = "even"
STRATEGY_NONE = "none"

UNALLOCATED_UNIT = "SIN_ASIGNAR"
SHARED_UNIT = "COSTO_COMPARTIDO"


@dataclass
class ChargebackLine:
    """Linea de imputacion para una unidad de negocio en un periodo."""

    period: str
    allocation_dimension: str
    unit: str
    direct_cost_usd: float
    allocated_shared_usd: float
    allocated_unallocated_usd: float
    overhead_usd: float
    total_chargeback_usd: float
    pct_of_total: float
    direct_pct_of_unit: float
    entity_count: int
    details: dict[str, Any] = field(default_factory=dict)

    def to_row(self) -> dict[str, Any]:
        fila = asdict(self)
        fila["details"] = {k: str(v) for k, v in self.details.items()}
        return fila


def is_shared_entity(record: dict[str, Any], patterns: list[str] | None) -> bool:
    """True si la entidad del registro corresponde a costo compartido."""
    if not patterns:
        return False
    candidatos = [
        str(record.get("entity_name") or ""),
        str(record.get("entity_key") or ""),
        str(record.get("project") or ""),
        str(record.get("application") or ""),
    ]
    for patron in patterns:
        patron_lower = str(patron).lower()
        for texto in candidatos:
            if texto and fnmatch.fnmatch(texto.lower(), patron_lower):
                return True
    return False


def _distribute(
    monto: float, unidades: list[str], pesos: dict[str, float], strategy: str
) -> dict[str, float]:
    """Reparte `monto` entre `unidades` segun la estrategia."""
    if monto <= 0 or not unidades:
        return dict.fromkeys(unidades, 0.0)

    if strategy == STRATEGY_EVEN:
        parte = monto / len(unidades)
        return dict.fromkeys(unidades, parte)

    total_pesos = sum(max(0.0, pesos.get(u, 0.0)) for u in unidades)
    if total_pesos <= 0:
        # Sin base proporcional, se degrada a reparto equitativo.
        parte = monto / len(unidades)
        return dict.fromkeys(unidades, parte)
    return {u: monto * max(0.0, pesos.get(u, 0.0)) / total_pesos for u in unidades}


def allocate(
    records: list[dict[str, Any]],
    *,
    period: str,
    config: dict[str, Any] | None = None,
    unallocated_value: str = "SIN_ASIGNAR",
    value_field: str = "total_cost_usd",
) -> list[ChargebackLine]:
    """Calcula el chargeback de un periodo.

    Args:
        records: filas agregadas del periodo con la dimension de imputacion,
            `entity_name`/`entity_key` y el costo.
        period: etiqueta del periodo (ej '2026-07').
        config: bloque `chargeback` de conf/budgets.yml.

    Returns:
        Una linea por unidad de negocio, ordenada por costo total descendente.
    """
    cfg = config or {}
    dimension = str(cfg.get("allocation_dimension", "cost_center"))
    estrategia_no_atribuido = str(cfg.get("unallocated_strategy", STRATEGY_PROPORTIONAL))
    estrategia_compartido = str(cfg.get("shared_cost_strategy", STRATEGY_PROPORTIONAL))
    patrones = cfg.get("shared_entity_patterns") or []
    overhead = float(cfg.get("overhead_pct", 0.0) or 0.0)

    directo: dict[str, float] = {}
    entidades: dict[str, set[str]] = {}
    compartido_total = 0.0
    no_atribuido_total = 0.0

    for registro in records:
        costo = float(registro.get(value_field) or 0.0)
        if costo == 0.0:
            continue
        if is_shared_entity(registro, patrones):
            compartido_total += costo
            continue
        unidad = registro.get(dimension)
        if unidad in (None, "", unallocated_value):
            no_atribuido_total += costo
            continue
        clave = str(unidad)
        directo[clave] = directo.get(clave, 0.0) + costo
        entidades.setdefault(clave, set()).add(str(registro.get("entity_key") or ""))

    unidades = sorted(directo)

    reparto_compartido: dict[str, float] = dict.fromkeys(unidades, 0.0)
    reparto_no_atribuido: dict[str, float] = dict.fromkeys(unidades, 0.0)
    residual_compartido = 0.0
    residual_no_atribuido = 0.0

    if unidades and estrategia_compartido != STRATEGY_NONE:
        reparto_compartido = _distribute(compartido_total, unidades, directo, estrategia_compartido)
    else:
        residual_compartido = compartido_total

    if unidades and estrategia_no_atribuido != STRATEGY_NONE:
        reparto_no_atribuido = _distribute(no_atribuido_total, unidades, directo, estrategia_no_atribuido)
    else:
        residual_no_atribuido = no_atribuido_total

    lineas: list[ChargebackLine] = []
    for unidad in unidades:
        base = directo[unidad] + reparto_compartido.get(unidad, 0.0) + reparto_no_atribuido.get(unidad, 0.0)
        recargo = base * overhead
        lineas.append(
            ChargebackLine(
                period=period,
                allocation_dimension=dimension,
                unit=unidad,
                direct_cost_usd=round(directo[unidad], 4),
                allocated_shared_usd=round(reparto_compartido.get(unidad, 0.0), 4),
                allocated_unallocated_usd=round(reparto_no_atribuido.get(unidad, 0.0), 4),
                overhead_usd=round(recargo, 4),
                total_chargeback_usd=round(base + recargo, 4),
                pct_of_total=0.0,  # se completa abajo
                direct_pct_of_unit=round(directo[unidad] / base, 6) if base > 0 else 0.0,
                entity_count=len(entidades.get(unidad, set())),
                details={
                    "shared_strategy": estrategia_compartido,
                    "unallocated_strategy": estrategia_no_atribuido,
                    "overhead_pct": overhead,
                },
            )
        )

    # Residuales cuando la estrategia es 'none' (o no hay unidades con costo directo).
    for etiqueta, monto in ((SHARED_UNIT, residual_compartido), (UNALLOCATED_UNIT, residual_no_atribuido)):
        if monto <= 0:
            continue
        recargo = monto * overhead
        lineas.append(
            ChargebackLine(
                period=period,
                allocation_dimension=dimension,
                unit=etiqueta,
                direct_cost_usd=round(monto, 4),
                allocated_shared_usd=0.0,
                allocated_unallocated_usd=0.0,
                overhead_usd=round(recargo, 4),
                total_chargeback_usd=round(monto + recargo, 4),
                pct_of_total=0.0,
                direct_pct_of_unit=1.0,
                entity_count=0,
                details={"residual": True},
            )
        )

    total = sum(line.total_chargeback_usd for line in lineas)
    for line in lineas:
        line.pct_of_total = round(line.total_chargeback_usd / total, 6) if total > 0 else 0.0

    lineas.sort(key=lambda line: -line.total_chargeback_usd)
    return lineas


def reconcile(records: list[dict[str, Any]], lines: list[ChargebackLine], *, overhead_pct: float = 0.0,
              value_field: str = "total_cost_usd", tolerance: float = 0.01) -> dict[str, Any]:
    """Verifica que el chargeback cuadre con el costo total del periodo."""
    total_origen = sum(float(r.get(value_field) or 0.0) for r in records)
    esperado = total_origen * (1.0 + overhead_pct)
    total_imputado = sum(line.total_chargeback_usd for line in lines)
    diferencia = total_imputado - esperado
    return {
        "source_total_usd": round(total_origen, 4),
        "expected_total_usd": round(esperado, 4),
        "allocated_total_usd": round(total_imputado, 4),
        "difference_usd": round(diferencia, 6),
        "reconciled": abs(diferencia) <= max(tolerance, abs(esperado) * 1e-6),
    }


def showback_report(lines: list[ChargebackLine], top_n: int = 10) -> dict[str, Any]:
    """Resumen ejecutivo del chargeback del periodo."""
    total = sum(line.total_chargeback_usd for line in lines)
    return {
        "period": lines[0].period if lines else None,
        "dimension": lines[0].allocation_dimension if lines else None,
        "total_usd": round(total, 2),
        "unit_count": len(lines),
        "top_units": [
            {"unit": line.unit, "total_usd": line.total_chargeback_usd, "pct": line.pct_of_total}
            for line in lines[:top_n]
        ],
        "shared_allocated_usd": round(sum(line.allocated_shared_usd for line in lines), 2),
        "unallocated_allocated_usd": round(sum(line.allocated_unallocated_usd for line in lines), 2),
    }
