"""Plataforma de analitica FinOps para Databricks.

El paquete se organiza en capas:

    finops.config        carga y validacion de configuracion
    finops.ingestion     lectura incremental de system tables -> bronze
    finops.transform     bronze -> silver -> gold (costos, etiquetas, entidades)
    finops.analytics     anomalias, pronostico, presupuestos, optimizacion, chargeback
    finops.quality       chequeos de calidad de datos
    finops.alerting      reglas de alerta y despacho a canales
    finops.pipeline      orquestacion de extremo a extremo

Regla de diseno: toda la logica de negocio se implementa como funciones puras
sobre estructuras de Python (dict / list / dataclass) en modulos `*_core` o en
funciones marcadas como puras, y los adaptadores de Spark solo se encargan de
leer, escribir y mapear. Esto permite probar la logica sin un cluster.
"""

__version__ = "0.1.0"

__all__ = ["__version__"]
