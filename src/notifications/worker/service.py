"""Lógica del worker: idempotencia, historial de intentos, estados y clasificación de errores.

Flujo por mensaje:

1. Parseo del evento → ``PermanentError`` si el tipo/versión/payload no es válido.
2. Chequeo rápido de duplicado (``processed_messages``) → ack sin reprocesar.
3. Tx corta: intento ``in_progress`` + notificación ``processing`` (visible para la API).
4. Tx de efecto: ``claim`` en ``processed_messages`` → envío → ``sent`` → commit.
   Si el envío falla, rollback: la marca de procesado desaparece y el reintento puede repetirlo.
5. En fallo: se decide retry/DLQ con la misma política que el bus, se registra y se relanza
   para que el bus haga el retry o el dead-letter.
"""

from __future__ import annotations

import time

from pydantic import ValidationError
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from notifications.contracts import (
    NOTIFICATION_REQUESTED,
    Channel,
    Envelope,
    NotificationRequestedV1,
    NotificationStatus,
)
from notifications.db import delivery as repo
from notifications.db.models import AttemptOutcome
from notifications.errors import PermanentError
from notifications.messaging.bus import DeliveryContext
from notifications.messaging.policy import Outcome, decide
from notifications.observability import (
    NOTIFICATION_PROCESSING_SECONDS,
    NOTIFICATIONS_PROCESSED,
    get_logger,
)
from notifications.worker.senders import Sender

log = get_logger(__name__)

SUPPORTED_VERSIONS = frozenset({1})


def parse_event(envelope: Envelope) -> NotificationRequestedV1:
    if envelope.type != NOTIFICATION_REQUESTED:
        raise PermanentError(f"tipo de evento no soportado: {envelope.type!r}")
    if envelope.version not in SUPPORTED_VERSIONS:
        raise PermanentError(f"versión no soportada: {envelope.version}")
    try:
        return NotificationRequestedV1.model_validate(envelope.payload)
    except ValidationError as exc:
        raise PermanentError(f"payload inválido: {exc.error_count()} errores") from exc


class NotificationWorker:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        sender: Sender,
        channel: Channel,
        retry_delays: tuple[float, ...],
    ) -> None:
        self._sf = session_factory
        self._sender = sender
        self._channel = channel
        self._delays = retry_delays
        self.consumer = f"{channel}-worker"

    async def handle(self, envelope: Envelope, ctx: DeliveryContext) -> None:
        """``Handler`` del bus. Retorna → ack; lanza → retry/DLQ según la política."""
        started = time.perf_counter()
        result = "ok"
        try:
            result = await self._handle(envelope, ctx)
        except PermanentError:
            result = "dead"
            raise
        except Exception as exc:
            result = (
                "retry"
                if decide(exc, ctx.attempt, self._delays).outcome is Outcome.RETRY
                else "dead"
            )
            raise
        finally:
            channel = str(self._channel)
            NOTIFICATIONS_PROCESSED.labels(channel=channel, result=result).inc()
            NOTIFICATION_PROCESSING_SECONDS.labels(channel=channel).observe(
                time.perf_counter() - started
            )

    async def _handle(self, envelope: Envelope, ctx: DeliveryContext) -> str:
        event = parse_event(envelope)

        async with self._sf() as session:
            if await repo.is_processed(session, envelope.id, self.consumer):
                log.info("message.duplicate", stage="precheck")
                return "duplicate"

        try:
            async with self._sf() as session, session.begin():
                attempt_id = await repo.start_attempt(
                    session,
                    notification_id=event.notification_id,
                    message_id=envelope.id,
                    consumer=self.consumer,
                    attempt=ctx.attempt,
                )
        except IntegrityError as exc:  # FK: la notificación no existe → reintentar no ayuda
            raise PermanentError(f"notificación {event.notification_id} inexistente") from exc

        try:
            async with self._sf() as session, session.begin():
                if not await repo.claim_message(session, envelope.id, self.consumer):
                    # Otra entrega concurrente del mismo mensaje ya lo procesó.
                    await repo.finish_attempt(session, attempt_id, AttemptOutcome.DUPLICATE)
                    log.info("message.duplicate", stage="claim")
                    return "duplicate"
                await self._sender.send(event)
                await repo.set_status(session, event.notification_id, NotificationStatus.SENT)
                await repo.finish_attempt(session, attempt_id, AttemptOutcome.OK)
        except Exception as exc:
            await self._record_failure(event, attempt_id, exc, ctx.attempt)
            raise

        log.info("notification.sent", notification_id=str(event.notification_id))
        return "ok"

    async def _record_failure(
        self, event: NotificationRequestedV1, attempt_id: int, exc: Exception, attempt: int
    ) -> None:
        decision = decide(exc, attempt, self._delays)
        retrying = decision.outcome is Outcome.RETRY
        outcome = AttemptOutcome.RETRY if retrying else AttemptOutcome.DEAD
        status = NotificationStatus.RETRYING if retrying else NotificationStatus.DEAD
        try:
            async with self._sf() as session, session.begin():
                await repo.finish_attempt(session, attempt_id, outcome, error=exc)
                await repo.set_status(session, event.notification_id, status)
        except Exception:
            # Best-effort: si la DB está caída, el mensaje igualmente sigue su camino en el broker.
            log.exception("attempt.record_failed", attempt_id=attempt_id)
