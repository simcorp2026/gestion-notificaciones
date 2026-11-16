"""Acceso a datos de ``dead_letters``."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from notifications.contracts import Channel
from notifications.db.models import DeadLetter, DeadLetterStatus


async def insert_dead_letter(session: AsyncSession, values: dict[str, Any]) -> int | None:
    """Inserta; devuelve el id o ``None`` si ya estaba archivado (misma muerte re-entregada)."""
    stmt = (
        insert(DeadLetter)
        .values(**values)
        .on_conflict_do_nothing(
            index_elements=[DeadLetter.channel, DeadLetter.message_id, DeadLetter.dead_at]
        )
        .returning(DeadLetter.id)
    )
    return await session.scalar(stmt)


async def list_dead_letters(
    session: AsyncSession,
    channel: Channel,
    *,
    status: DeadLetterStatus | None,
    limit: int,
    cursor: int | None,
) -> Sequence[DeadLetter]:
    """Paginación por keyset (``id`` descendente): estable aunque entren filas nuevas."""
    stmt = select(DeadLetter).where(DeadLetter.channel == channel)
    if status is not None:
        stmt = stmt.where(DeadLetter.status == status)
    if cursor is not None:
        stmt = stmt.where(DeadLetter.id < cursor)
    stmt = stmt.order_by(DeadLetter.id.desc()).limit(limit)
    return (await session.scalars(stmt)).all()


async def get_dead_letter(
    session: AsyncSession, channel: Channel, dead_letter_id: int, *, for_update: bool = False
) -> DeadLetter | None:
    stmt = select(DeadLetter).where(DeadLetter.id == dead_letter_id, DeadLetter.channel == channel)
    if for_update:
        stmt = stmt.with_for_update()
    return await session.scalar(stmt)


async def claim_pending(
    session: AsyncSession, channel: Channel, *, limit: int, reason: str | None = None
) -> Sequence[DeadLetter]:
    stmt = select(DeadLetter).where(
        DeadLetter.channel == channel, DeadLetter.status == DeadLetterStatus.PENDING
    )
    if reason is not None:
        stmt = stmt.where(DeadLetter.reason == reason)
    stmt = stmt.order_by(DeadLetter.id).limit(limit).with_for_update(skip_locked=True)
    return (await session.scalars(stmt)).all()


async def mark_replayed(session: AsyncSession, dead_letter_id: int) -> None:
    await session.execute(
        update(DeadLetter)
        .where(DeadLetter.id == dead_letter_id)
        .values(
            status=DeadLetterStatus.REPLAYED,
            replayed_at=func.now(),
            replay_count=DeadLetter.replay_count + 1,
        )
    )


async def mark_discarded(session: AsyncSession, dead_letter_id: int) -> None:
    await session.execute(
        update(DeadLetter)
        .where(DeadLetter.id == dead_letter_id)
        .values(status=DeadLetterStatus.DISCARDED, discarded_at=func.now())
    )


async def count_pending_by_channel(session: AsyncSession) -> dict[Channel, int]:
    stmt = (
        select(DeadLetter.channel, func.count())
        .where(DeadLetter.status == DeadLetterStatus.PENDING)
        .group_by(DeadLetter.channel)
    )
    counts = dict.fromkeys(Channel, 0)
    for channel, n in (await session.execute(stmt)).all():
        counts[channel] = int(n)
    return counts
