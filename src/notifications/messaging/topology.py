"""Nombres y argumentos de la topología (puro, sin I/O).

Por canal ``c``::

    exchange `notifications` (topic) ──notification.c──> c.main (quorum)
    c.main      ── reject / delivery-limit ──> `notifications.dlx` ──c.dlq──> c.dlq
    c.retry.<d> ── TTL expira ──> `notifications.dlx` ──c.main──> c.main
    worker ── publish directo (default exchange) ──> c.retry.<d> | c.dlq
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from notifications.config import RabbitMQSettings, RetrySettings
from notifications.contracts import Channel
from notifications.messaging.policy import format_delay


@dataclass(frozen=True, slots=True)
class Topology:
    retry_delays: tuple[float, ...] = (5, 30, 300)
    channels: tuple[Channel, ...] = tuple(Channel)
    exchange_base: str = "notifications"
    delivery_limit: int = 10
    # Prefijo para aislar topologías (tests, demo) en el mismo vhost.
    namespace: str = ""
    _ns: str = field(init=False, repr=False)

    def __post_init__(self) -> None:
        if not self.retry_delays:
            msg = "retry_delays no puede estar vacío"
            raise ValueError(msg)
        object.__setattr__(self, "_ns", f"{self.namespace}." if self.namespace else "")

    @classmethod
    def from_settings(
        cls, rabbit: RabbitMQSettings, retry: RetrySettings, *, namespace: str = ""
    ) -> Topology:
        return cls(
            retry_delays=tuple(retry.delays_seconds),
            exchange_base=rabbit.exchange,
            delivery_limit=rabbit.delivery_limit,
            namespace=namespace,
        )

    # ------------------------------------------------------------------ nombres
    @property
    def exchange(self) -> str:
        return f"{self._ns}{self.exchange_base}"

    @property
    def dlx(self) -> str:
        return f"{self.exchange}.dlx"

    @staticmethod
    def routing_key(channel: Channel) -> str:
        return f"notification.{channel}"

    def main_queue(self, channel: Channel) -> str:
        return f"{self._ns}{channel}.main"

    def retry_queue(self, channel: Channel, level: int) -> str:
        return f"{self._ns}{channel}.retry.{format_delay(self.retry_delays[level])}"

    def retry_queues(self, channel: Channel) -> list[str]:
        return [self.retry_queue(channel, i) for i in range(len(self.retry_delays))]

    def dlq(self, channel: Channel) -> str:
        return f"{self._ns}{channel}.dlq"

    # ---------------------------------------------------------------- argumentos
    def main_queue_arguments(self, channel: Channel) -> dict[str, Any]:
        return {
            "x-queue-type": "quorum",
            "x-dead-letter-exchange": self.dlx,
            "x-dead-letter-routing-key": self.dlq(channel),
            # Sin estas dos, el dead-lettering de quorum es at-most-once (ADR 005).
            "x-dead-letter-strategy": "at-least-once",
            "x-overflow": "reject-publish",
            "x-delivery-limit": self.delivery_limit,
        }

    def retry_queue_arguments(self, channel: Channel, level: int) -> dict[str, Any]:
        return {
            "x-queue-type": "classic",
            "x-message-ttl": round(self.retry_delays[level] * 1000),
            "x-dead-letter-exchange": self.dlx,
            "x-dead-letter-routing-key": self.main_queue(channel),
        }

    @staticmethod
    def dlq_arguments() -> dict[str, Any]:
        return {"x-queue-type": "quorum"}
