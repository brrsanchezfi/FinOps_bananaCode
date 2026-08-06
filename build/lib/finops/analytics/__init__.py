"""Analitica FinOps: anomalias, pronostico, presupuestos, optimizacion, chargeback.

Todos los algoritmos son funciones puras sobre listas de dicts o de tuplas
(fecha, valor). No dependen de Spark ni de pandas, lo que permite ejecutarlos en
pruebas unitarias y tambien dentro de un `mapInPandas` si el volumen lo exige.
"""

from __future__ import annotations

__all__ = ["anomaly", "forecast", "budgets", "optimization", "chargeback"]
