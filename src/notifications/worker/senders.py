"""Proveedores de entrega por canal (simulados).

Un ``Sender`` real (SMTP, Twilio, FCM) implementaría el mismo ``Protocol`` y mapearía sus errores
a ``TransientError`` (timeouts, 5xx, rate limit) o ``PermanentError`` (4xx de validación).
"""

from __future__ import annotations

import asyncio
import random
import re
from typing import Protocol

from notifications.config import WorkerSettings
from notifications.contracts import Channel, NotificationRequestedV1
from notifications.errors import PermanentError, TransientError

_RECIPIENT_RULES: dict[Channel, tuple[re.Pattern[str], str]] = {
    Channel.EMAIL: (re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$"), "email inválido"),
    Channel.SMS: (re.compile(r"^\+[1-9]\d{6,14}$"), "teléfono no E.164"),
    Channel.PUSH: (re.compile(r"^[A-Za-z0-9:_\-]{16,4096}$"), "token push inválido"),
}


def validate_recipient(channel: Channel, recipient: str) -> None:
    pattern, reason = _RECIPIENT_RULES[channel]
    if not pattern.match(recipient):
        raise PermanentError(f"{reason}: {recipient!r}")


class Sender(Protocol):
    async def send(self, notification: NotificationRequestedV1) -> None:
        """Entrega la notificación o lanza ``TransientError`` / ``PermanentError``."""
        ...


class SimulatedSender:
    """Latencia aleatoria y tasas de fallo configurables por entorno (``WORKER_SIM_*``)."""

    def __init__(
        self,
        channel: Channel,
        *,
        latency_ms: tuple[int, int] = (50, 300),
        transient_failure_rate: float = 0.0,
        permanent_failure_rate: float = 0.0,
        rng: random.Random | None = None,
    ) -> None:
        self.channel = channel
        self._latency_ms = latency_ms
        self._transient = transient_failure_rate
        self._permanent = permanent_failure_rate
        self._rng = rng or random.Random()  # noqa: S311 - simulación, no criptografía

    @classmethod
    def from_settings(cls, channel: Channel, settings: WorkerSettings) -> SimulatedSender:
        return cls(
            channel,
            latency_ms=(settings.sim_latency_min_ms, settings.sim_latency_max_ms),
            transient_failure_rate=settings.sim_transient_failure_rate,
            permanent_failure_rate=settings.sim_permanent_failure_rate,
        )

    async def send(self, notification: NotificationRequestedV1) -> None:
        if notification.channel is not self.channel:
            raise PermanentError(f"canal {notification.channel} recibido por worker {self.channel}")
        validate_recipient(self.channel, notification.recipient)

        low, high = self._latency_ms
        await asyncio.sleep(self._rng.uniform(low, high) / 1000)

        roll = self._rng.random()
        if roll < self._permanent:
            raise PermanentError("proveedor rechazó el destinatario (simulado)")
        if roll < self._permanent + self._transient:
            raise TransientError("proveedor no disponible (simulado)")
