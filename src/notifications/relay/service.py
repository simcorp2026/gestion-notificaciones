"""Outbox relay: publica eventos pendientes al broker con confirms y los marca publicados.

Garantías:
- Una fila sólo se marca publicada después del confirm del broker y en la misma transacción
  que la bloqueó → si el relay o el broker caen, la fila sigue pendiente y se reintenta.
- Publicar y caer antes del commit produce un duplicado (at-least-once); el ``envelope.id`` es
  estable y el consumidor lo deduplica.
- ``FOR UPDATE SKIP LOCKED`` permite N réplicas sin publicar dos veces la misma fila a la vez.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from notifications.config import RelaySettings
from notifications.contracts import Envelope
from notifications.db import repositories as repo
from notifications.db.models import OutboxEvent
from notifications.messaging.bus import EventPublisher
from notifications.observability import NOTIFICATIONS_PUBLISHED, correlation_context, get_logger
from notifications.relay.listener import OutboxListener
from notifications.runtime import wait_or_stop

log = get_logger(__name__)


def compute_backoff(attempts: int, base: float, maximum: float) -> timedelta:
    """``base * 2^(attempts-1)`` con tope. ``attempts`` = fallos acumulados (>= 1)."""
    exponent = max(attempts - 1, 0)
    seconds = maximum if exponent >= 32 else min(base * (2**exponent), maximum)  # noqa: PLR2004
    return timedelta(seconds=seconds)


class OutboxRelay:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        bus: EventPublisher,
        settings: RelaySettings,
        *,
        listener: OutboxListener | None = None,
    ) -> None:
        self._sf = session_factory
        self._bus = bus
        self._settings = settings
        self._listener = listener

    async def run_once(self) -> int:
        """Procesa un lote. Devuelve cuántos eventos se publicaron."""
        async with self._sf() as session, session.begin():
            events = await repo.claim_outbox_batch(session, self._settings.batch_size)
            if not events:
                return 0
            results = await asyncio.gather(
                *(self._publish(e) for e in events), return_exceptions=True
            )

            published: list[OutboxEvent] = []
            for event, result in zip(events, results, strict=True):
                if isinstance(result, asyncio.CancelledError):
                    raise result
                if isinstance(result, BaseException):
                    backoff = compute_backoff(
                        event.publish_attempts + 1,
                        self._settings.backoff_base_seconds,
                        self._settings.backoff_max_seconds,
                    )
                    await repo.mark_publish_failed(session, event, repr(result), backoff)
                    log.warning(
                        "relay.publish_failed",
                        event_id=str(event.event_id),
                        attempts=event.publish_attempts + 1,
                        retry_in_s=backoff.total_seconds(),
                        error=repr(result),
                    )
                else:
                    published.append(event)

            await repo.mark_published(session, published)

        for event in published:
            NOTIFICATIONS_PUBLISHED.labels(channel=str(event.channel)).inc()
        if published:
            log.info(
                "relay.batch_published", count=len(published), failed=len(events) - len(published)
            )
        return len(published)

    async def _publish(self, event: OutboxEvent) -> None:
        envelope = Envelope.model_validate(event.envelope)
        with correlation_context(envelope.correlation_id):
            await self._bus.publish(envelope, event.channel)

    async def run(self, stop: asyncio.Event) -> None:
        log.info("relay.started", batch_size=self._settings.batch_size)
        wake = self._listener.event if self._listener else asyncio.Event()
        while not stop.is_set():
            if self._listener is not None:
                await self._listener.ensure_connected()
            wake.clear()
            try:
                published = await self.run_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("relay.iteration_failed")
                published = 0
            if published >= self._settings.batch_size:
                continue  # probablemente hay más trabajo: sin espera
            await wait_or_stop(stop, wake, within=self._settings.poll_interval_seconds)
        log.info("relay.stopped")
