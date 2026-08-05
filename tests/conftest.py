"""Fixtures compartidas de la suite de pruebas.

La logica de negocio de la plataforma es pura, por lo que la suite corre sin
Spark ni conexion a Databricks. Las pruebas que necesitan una SparkSession se
marcan con `@pytest.mark.spark` y se omiten si pyspark no esta instalado.
"""

from __future__ import annotations

import importlib.util
from datetime import date, timedelta
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
CONF_DIR = REPO_ROOT / "conf"

HAS_PYSPARK = importlib.util.find_spec("pyspark") is not None


def pytest_collection_modifyitems(config, items):  # noqa: ARG001
    if HAS_PYSPARK:
        return
    omitir = pytest.mark.skip(reason="pyspark no esta instalado")
    for item in items:
        if "spark" in item.keywords:
            item.add_marker(omitir)


@pytest.fixture
def conf_dir() -> Path:
    return CONF_DIR


@pytest.fixture
def cfg_dev():
    from finops.config import load_config

    return load_config("dev", conf_dir=CONF_DIR, use_env_vars=False, run_date="2026-07-15")


@pytest.fixture
def pricing_cfg() -> dict:
    return {
        "discounts": [
            {"name": "acuerdo_sql", "match": {"sku_group": "SQL"}, "discount_pct": 0.25},
            {"name": "acuerdo_general", "match": {}, "discount_pct": 0.10},
        ],
        "infra_estimate": {
            "enabled": True,
            "factor_by_compute": {"ALL_PURPOSE": 0.8, "JOBS": 0.5, "SQL": 0.6, "SERVERLESS": 0.0, "OTHER": 0.0},
        },
    }


@pytest.fixture
def tagging_cfg() -> dict:
    return {
        "dimensions": ["cost_center", "team", "environment"],
        "aliases": {
            "cost_center": ["cost_center", "costcenter", "cc", "centro_costo"],
            "team": ["team", "equipo", "squad"],
            "environment": ["environment", "env", "ambiente"],
        },
        "value_map": {"environment": {"prod": "PRD", "produccion": "PRD", "dev": "DEV"}},
        "unallocated_value": "SIN_ASIGNAR",
    }


def serie_estable(dias: int, valor: float, inicio: date | None = None) -> list[tuple[date, float]]:
    """Serie diaria plana, base para pruebas de anomalias y pronostico."""
    arranque = inicio or date(2026, 1, 1)
    return [(arranque + timedelta(days=i), valor) for i in range(dias)]


def serie_con_ruido(dias: int, base: float, amplitud: float, inicio: date | None = None) -> list[tuple[date, float]]:
    """Serie con variacion deterministica (sin aleatoriedad, para reproducibilidad)."""
    arranque = inicio or date(2026, 1, 1)
    return [(arranque + timedelta(days=i), base + amplitud * ((i % 5) - 2)) for i in range(dias)]
