"""Acceso a datos del consumidor: idempotencia, historial de intentos y estados."""

from __future__ import annotations

import uuid
from collections.abc import Sequence

from sqlalchemy import exists, func, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from notifications.contracts import NotificationStatus
from notifications.db.models import AttemptOutcome, DeliveryAttempt, Notification, ProcessedMessage

_ERROR_MESSAGE_MAX = 1000

# Una notificación ya enviada no retrocede de estado aunque llegue un duplicado tardío.
_FINAL_STATES = (NotificationStatus.SENT,)


async def is_processed(session: AsyncSession, message_id: uuid.UUID, consumer: str) -> bool:
    stmt = select(
        exists().where(
            ProcessedMessage.message_id == message_id, ProcessedMessage.consumer == consumer
        )
    )
    return bool(await session.scalar(stmt))


async def claim_message(session: AsyncSession, message_id: uuid.UUID, consumer: str) -> bool:
    """Inserta la marca de procesado en la transacción del llamador.

    ``ON CONFLICT DO NOTHING`` bloquea si otra transacción concurrente insertó la misma clave y
    aún no terminó: si ésta hace commit → ``False`` (duplicado); si hace rollback → ``True``.
    """
    stmt = (
        insert(ProcessedMessage)
        .values(message_id=message_id, consumer=consumer)
        .on_conflict_do_nothing(
            index_elements=[ProcessedMessage.message_id, ProcessedMessage.consumer]
        )
        .returning(ProcessedMessage.message_id)
    )
    return (await session.scalar(stmt)) is not None


async def start_attempt(
    session: AsyncSession,
    *,
    notification_id: uuid.UUID,
    message_id: uuid.UUID,
    consumer: str,
    attempt: int,
) -> int:
    """Registra el inicio de un intento y pasa la notificación a ``processing``."""
    attempt_id = await session.scalar(
        insert(DeliveryAttempt)
        .values(
            notification_id=notification_id,
            message_id=message_id,
            consumer=consumer,
            attempt=attempt,
            outcome=AttemptOutcome.IN_PROGRESS,
        )
        .returning(DeliveryAttempt.id)
    )
    await set_status(session, notification_id, NotificationStatus.PROCESSING)
    assert attempt_id is not None  # noqa: S101 - RETURNING siempre devuelve fila
    return int(attempt_id)


async def finish_attempt(
    session: AsyncSession,
    attempt_id: int,
    outcome: AttemptOutcome,
    *,
    error: BaseException | None = None,
) -> None:
    await session.execute(
        update(DeliveryAttempt)
        .where(DeliveryAttempt.id == attempt_id)
        .values(
            outcome=outcome,
            finished_at=func.now(),
            error_class=type(error).__name__ if error else None,
            error_message=str(error)[:_ERROR_MESSAGE_MAX] if error else None,
        )
    )


async def set_status(
    session: AsyncSession, notification_id: uuid.UUID, status: NotificationStatus
) -> None:
    await session.execute(
        update(Notification)
        .where(Notification.id == notification_id, Notification.status.not_in(_FINAL_STATES))
        .values(status=status)
    )


async def list_attempts(
    session: AsyncSession, notification_id: uuid.UUID
) -> Sequence[DeliveryAttempt]:
    stmt = (
        select(DeliveryAttempt)
        .where(DeliveryAttempt.notification_id == notification_id)
        .order_by(DeliveryAttempt.started_at, DeliveryAttempt.id)
    )
    return (await session.scalars(stmt)).all()
