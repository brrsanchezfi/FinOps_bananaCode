"""Jerarquia de excepciones de la plataforma FinOps."""

from __future__ import annotations


class FinOpsError(Exception):
    """Error base. Todo error propio hereda de aqui."""


class ConfigError(FinOpsError):
    """Configuracion ausente, mal formada o inconsistente."""


class SourceUnavailableError(FinOpsError):
    """Una tabla de sistema requerida no existe o no es legible."""

    def __init__(self, table: str, reason: str = "") -> None:
        self.table = table
        self.reason = reason
        detalle = f": {reason}" if reason else ""
        super().__init__(f"Fuente no disponible '{table}'{detalle}")


class DataQualityError(FinOpsError):
    """Uno o mas chequeos de calidad fallaron con severidad bloqueante."""

    def __init__(self, failures: list[str]) -> None:
        self.failures = failures
        super().__init__("Fallas de calidad de datos:\n  - " + "\n  - ".join(failures))


class AlertDeliveryError(FinOpsError):
    """No se pudo entregar una alerta a un canal externo."""


class PipelineError(FinOpsError):
    """Fallo en la ejecucion de una etapa del pipeline."""

    def __init__(self, stage: str, cause: BaseException | str) -> None:
        self.stage = stage
        self.cause = cause
        super().__init__(f"Fallo la etapa '{stage}': {cause}")


class SchemaMismatchError(FinOpsError):
    """El esquema de una tabla existente es incompatible con el que se escribe.

    Ocurre con tablas creadas por una version anterior que dejaba inferir el
    esquema a Spark: los diccionarios de metadatos quedaron como `struct` y hoy
    se escriben como `map`, que es lo correcto (con `struct`, cada clave nueva
    cambiaria el esquema de la tabla).

    Las tablas afectadas son bitacoras operativas, asi que recrearlas no pierde
    informacion de negocio.
    """

    def __init__(self, table: str, reason: str = "") -> None:
        self.table = table
        self.reason = reason
        super().__init__(
            "\n".join(
                [
                    f"El esquema de '{table}' es incompatible con el que produce esta version.",
                    f"Detalle: {reason}",
                    "",
                    "Causa habitual: la tabla se creo con una version que dejaba inferir el",
                    "esquema, y las columnas de metadatos quedaron como STRUCT en vez de MAP.",
                    "",
                    "Solucion (es una bitacora operativa, no se pierde informacion de negocio):",
                    f"    DROP TABLE IF EXISTS {table};",
                    "y volver a ejecutar el pipeline.",
                ]
            )
        )
