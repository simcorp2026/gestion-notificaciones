"""Abstracción de mensajería. La lógica de negocio sólo depende de estos tipos, nunca del broker.

Los protocolos están divididos por rol para que cada componente dependa sólo de lo que usa:
el relay de ``EventPublisher``, la API admin de ``Replayer``, y los procesos completos de
``MessageBus``.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from typing import Any, Protocol

from notifications.contracts import Channel, Envelope


@dataclass(frozen=True, slots=True)
class DeliveryContext:
    channel: Channel
    attempt: int  # reintentos previos (0 = primera entrega)
    redelivered: bool
    headers: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class RawMessage:
    """Mensaje tal cual viaja por el broker (cuerpo opaco). Usado en DLQ y replay."""

    channel: Channel
    body: bytes
    headers: Mapping[str, Any] = field(default_factory=dict)
    message_id: str | None = None
    correlation_id: str | None = None
    content_type: str | None = None
    type: str | None = None


Handler = Callable[[Envelope, DeliveryContext], Awaitable[None]]
"""Procesa un mensaje. Sin excepción → ack. ``PermanentError`` → DLQ. Otra excepción → retry."""

DeadLetterHandler = Callable[[RawMessage], Awaitable[None]]
"""Archiva un mensaje muerto. Sin excepción → ack."""


class ConsumerHandle(Protocol):
    async def stop(self, drain_timeout: float = 30.0) -> None:
        """Deja de recibir mensajes y espera a los que están en vuelo (hasta ``drain_timeout``)."""
        ...


class EventPublisher(Protocol):
    async def publish(self, envelope: Envelope, channel: Channel) -> None:
        """Publica de forma durable; retorna sólo cuando el broker confirmó la recepción."""
        ...


class Replayer(Protocol):
    async def replay(self, message: RawMessage) -> None:
        """Devuelve un mensaje muerto a la cola principal de su canal (con confirm)."""
        ...


class MessageBus(EventPublisher, Replayer, Protocol):
    async def connect(self) -> None: ...

    async def close(self) -> None: ...

    async def setup_topology(self) -> None:
        """Declara exchanges/colas/topics. Idempotente."""
        ...

    async def consume(self, channel: Channel, handler: Handler) -> ConsumerHandle: ...

    async def consume_dead_letters(
        self, channel: Channel, handler: DeadLetterHandler
    ) -> ConsumerHandle: ...


class PublishError(Exception):
    """El broker no confirmó la publicación (nack, unroutable o timeout)."""
