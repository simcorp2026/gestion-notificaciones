"""Acceso a datos. Las funciones reciben una sesión: la transacción la controla el llamador."""

from __future__ import annotations

import hashlib
import json
import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Final

from sqlalchemy import func, select, text, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from notifications.contracts import (
    NOTIFICATION_REQUESTED,
    Channel,
    Envelope,
    NotificationRequestedV1,
    NotificationStatus,
)
from notifications.db.models import Notification, OutboxEvent

OUTBOX_NOTIFY_CHANNEL: Final = "outbox_new"
_LAST_ERROR_MAX = 1000


class IdempotencyConflictError(Exception):
    """Misma ``idempotency_key`` con un cuerpo distinto al original."""


@dataclass(frozen=True, slots=True)
class NewNotification:
    channel: Channel
    recipient: str
    template: str
    data: dict[str, Any]
    idempotency_key: str | None = None

    def request_hash(self) -> str:
        canonical = json.dumps(
            {
                "channel": str(self.channel),
                "recipient": self.recipient,
                "template": self.template,
                "data": self.data,
            },
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            default=str,
        )
        return hashlib.sha256(canonical.encode()).hexdigest()


@dataclass(frozen=True, slots=True)
class CreateResult:
    notification: Notification
    created: bool  # False → réplica idempotente de una petición previa


# ------------------------------------------------------------------------- notificaciones


async def create_notification(
    session: AsyncSession, req: NewNotification, *, correlation_id: str
) -> CreateResult:
    """Inserta notificación + evento de outbox en la transacción del llamador.

    Con ``idempotency_key`` usa ``ON CONFLICT DO NOTHING``: ante dos peticiones concurrentes,
    la segunda espera al commit de la primera y devuelve la fila existente.
    """
    request_hash = req.request_hash()
    notification_id = uuid.uuid4()
    values = {
        "id": notification_id,
        "channel": req.channel,
        "recipient": req.recipient,
        "template": req.template,
        "data": req.data,
        "idempotency_key": req.idempotency_key,
        "request_hash": request_hash,
        "status": NotificationStatus.PENDING,
        "correlation_id": correlation_id,
    }
    stmt = insert(Notification).values(**values)
    if req.idempotency_key is not None:
        stmt = stmt.on_conflict_do_nothing(index_elements=[Notification.idempotency_key])
    inserted = await session.scalar(stmt.returning(Notification.id))

    if inserted is None:
        existing = await session.scalar(
            select(Notification).where(Notification.idempotency_key == req.idempotency_key)
        )
        if existing is None:  # pragma: no cover - imposible salvo borrado concurrente
            msg = "conflicto de idempotencia sin fila existente"
            raise RuntimeError(msg)
        if existing.request_hash != request_hash:
            raise IdempotencyConflictError(req.idempotency_key)
        return CreateResult(existing, created=False)

    envelope = Envelope(
        type=NOTIFICATION_REQUESTED,
        version=1,
        correlation_id=correlation_id,
        payload=NotificationRequestedV1(
            notification_id=notification_id,
            channel=req.channel,
            recipient=req.recipient,
            template=req.template,
            data=req.data,
        ).model_dump(mode="json"),
    )
    session.add(
        OutboxEvent(
            event_id=envelope.id,
            notification_id=notification_id,
            channel=req.channel,
            event_type=envelope.type,
            envelope=envelope.model_dump(mode="json"),
        )
    )
    # pg_notify es transaccional: el relay sólo se despierta si el commit tiene éxito.
    await session.execute(text("SELECT pg_notify(:ch, '')"), {"ch": OUTBOX_NOTIFY_CHANNEL})
    await session.flush()

    notification = await session.get_one(Notification, notification_id)
    return CreateResult(notification, created=True)


async def get_notification(
    session: AsyncSession, notification_id: uuid.UUID
) -> Notification | None:
    return await session.get(Notification, notification_id)


# ------------------------------------------------------------------------------- outbox


async def claim_outbox_batch(session: AsyncSession, limit: int) -> Sequence[OutboxEvent]:
    """Bloquea hasta ``limit`` eventos pendientes; ``SKIP LOCKED`` reparte filas entre réplicas."""
    stmt = (
        select(OutboxEvent)
        .where(OutboxEvent.published_at.is_(None), OutboxEvent.available_at <= func.now())
        .order_by(OutboxEvent.available_at, OutboxEvent.id)
        .limit(limit)
        .with_for_update(skip_locked=True)
    )
    return (await session.scalars(stmt)).all()


async def mark_published(session: AsyncSession, events: Sequence[OutboxEvent]) -> None:
    if not events:
        return
    ids = [e.id for e in events]
    await session.execute(
        update(OutboxEvent)
        .where(OutboxEvent.id.in_(ids))
        .values(published_at=func.now(), last_error=None)
    )
    # Sólo avanza desde PENDING: si un worker ya la tomó (carrera), no se retrocede el estado.
    await session.execute(
        update(Notification)
        .where(
            Notification.id.in_([e.notification_id for e in events]),
            Notification.status == NotificationStatus.PENDING,
        )
        .values(status=NotificationStatus.PUBLISHED)
    )


async def mark_publish_failed(
    session: AsyncSession, event: OutboxEvent, error: str, backoff: timedelta
) -> None:
    await session.execute(
        update(OutboxEvent)
        .where(OutboxEvent.id == event.id)
        .values(
            publish_attempts=OutboxEvent.publish_attempts + 1,
            last_error=error[:_LAST_ERROR_MAX],
            available_at=func.now() + backoff,
        )
    )


async def count_pending_outbox(session: AsyncSession) -> int:
    stmt = select(func.count()).select_from(OutboxEvent).where(OutboxEvent.published_at.is_(None))
    return int(await session.scalar(stmt) or 0)
