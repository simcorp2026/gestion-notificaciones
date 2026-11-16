"""Clasificación de errores de entrega.

La política de reintento depende exclusivamente de la clase de la excepción:
- ``PermanentError``: va directo a la DLQ, no se reintenta.
- ``TransientError`` o cualquier excepción no clasificada: se reintenta con backoff.
"""

from __future__ import annotations


class NotificationError(Exception):
    """Base de los errores de dominio del sistema."""


class TransientError(NotificationError):
    """Fallo recuperable (timeout, proveedor no disponible, rate limit)."""


class PermanentError(NotificationError):
    """Fallo no recuperable (payload inválido, destinatario inválido)."""


def is_permanent(exc: BaseException) -> bool:
    """Toda excepción no clasificada explícitamente como permanente se trata como transitoria."""
    return isinstance(exc, PermanentError)
