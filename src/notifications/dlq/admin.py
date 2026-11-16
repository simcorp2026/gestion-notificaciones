"""Operaciones admin sobre dead letters: listar, replay (uno o todos) y descartar."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from notifications.contracts import Channel, NotificationStatus
from notifications.db import dead_letters as repo
from notifications.db import delivery
from notifications.db.models import DeadLetter, DeadLetterStatus
from notifications.dlq.parsing import replay_headers
from notifications.messaging.bus import PublishError, RawMessage, Replayer
from notifications.observability import DEAD_LETTERS_REPLAYED, get_logger

log = get_logger(__name__)


class DeadLetterNotFoundError(Exception):
    pass


class DeadLetterStateError(Exception):
    """La operación no aplica al estado actual (p. ej. replay de una fila ya reprocesada)."""


class BrokerUnavailableError(Exception):
    """No se pudo publicar el replay; la fila queda ``pending``."""


@dataclass(frozen=True, slots=True)
class ReplayAllResult:
    replayed: int
    failed: int


class DeadLetterAdmin:
    def __init__(
        self, session_factory: async_sessionmaker[AsyncSession], replayer: Replayer
    ) -> None:
        self._sf = session_factory
        self._replayer = replayer

    async def list(
        self,
        channel: Channel,
        *,
        status: DeadLetterStatus | None,
        limit: int,
        cursor: int | None,
    ) -> Sequence[DeadLetter]:
        async with self._sf() as session:
            return await repo.list_dead_letters(
                session, channel, status=status, limit=limit, cursor=cursor
            )

    async def get(self, channel: Channel, dead_letter_id: int) -> DeadLetter:
        async with self._sf() as session:
            row = await repo.get_dead_letter(session, channel, dead_letter_id)
        if row is None:
            raise DeadLetterNotFoundError(dead_letter_id)
        return row

    async def replay(self, channel: Channel, dead_letter_id: int) -> DeadLetter:
        async with self._sf() as session, session.begin():
            row = await repo.get_dead_letter(session, channel, dead_letter_id, for_update=True)
            if row is None:
                raise DeadLetterNotFoundError(dead_letter_id)
            if row.status is not DeadLetterStatus.PENDING:
                raise DeadLetterStateError(f"dead letter {dead_letter_id} está {row.status}")
            await self._replay_row(session, row)
        async with self._sf() as session:
            refreshed = await repo.get_dead_letter(session, channel, dead_letter_id)
        assert refreshed is not None  # noqa: S101
        return refreshed

    async def replay_all(
        self, channel: Channel, *, reason: str | None = None, batch_size: int = 100
    ) -> ReplayAllResult:
        """Reprocesa las filas pendientes que existían al empezar (las que vuelvan a morir
        durante la operación no entran: evita bucles con mensajes que siempre fallan)."""
        async with self._sf() as session:
            max_id = await session.scalar(
                select(func.max(DeadLetter.id)).where(DeadLetter.channel == channel)
            )
        if max_id is None:
            return ReplayAllResult(0, 0)

        replayed = failed = 0
        while True:
            async with self._sf() as session, session.begin():
                rows = [
                    r
                    for r in await repo.claim_pending(
                        session, channel, limit=batch_size, reason=reason
                    )
                    if r.id <= max_id
                ]
                if not rows:
                    break
                for row in rows:
                    try:
                        await self._replay_row(session, row)
                    except BrokerUnavailableError:
                        failed = await self._count_pending(session, channel, reason, max_id)
                        log.warning("dead_letter.replay_all_aborted", replayed=replayed)
                        return ReplayAllResult(replayed, failed)
                    replayed += 1
        log.info("dead_letter.replay_all", channel=str(channel), replayed=replayed)
        return ReplayAllResult(replayed, failed)

    async def discard(self, channel: Channel, dead_letter_id: int) -> None:
        async with self._sf() as session, session.begin():
            row = await repo.get_dead_letter(session, channel, dead_letter_id, for_update=True)
            if row is None:
                raise DeadLetterNotFoundError(dead_letter_id)
            if row.status is not DeadLetterStatus.PENDING:
                raise DeadLetterStateError(f"dead letter {dead_letter_id} está {row.status}")
            await repo.mark_discarded(session, dead_letter_id)
        log.info("dead_letter.discarded", dead_letter_id=dead_letter_id)

    # ------------------------------------------------------------------------ internos
    async def _replay_row(self, session: AsyncSession, row: DeadLetter) -> None:
        message = RawMessage(
            channel=row.channel,
            body=row.body,
            headers=replay_headers(
                row.headers, dead_letter_id=row.id, replay_count=row.replay_count + 1
            ),
            message_id=row.message_id,
            correlation_id=row.correlation_id,
            content_type=row.content_type,
            type=row.message_type,
        )
        try:
            await self._replayer.replay(message)
        except PublishError as exc:
            raise BrokerUnavailableError(str(exc)) from exc
        # Ya confirmado: si el commit fallara, el replay se repetiría (consumidor idempotente).
        await repo.mark_replayed(session, row.id)
        DEAD_LETTERS_REPLAYED.labels(channel=str(row.channel)).inc()
        if row.notification_id is not None:
            await delivery.set_status(session, row.notification_id, NotificationStatus.PUBLISHED)
        log.info(
            "dead_letter.replayed",
            dead_letter_id=row.id,
            message_id=row.message_id,
            replay_count=row.replay_count + 1,
        )

    @staticmethod
    async def _count_pending(
        session: AsyncSession, channel: Channel, reason: str | None, max_id: int
    ) -> int:
        stmt = (
            select(func.count())
            .select_from(DeadLetter)
            .where(
                DeadLetter.channel == channel,
                DeadLetter.status == DeadLetterStatus.PENDING,
                DeadLetter.id <= max_id,
            )
        )
        if reason is not None:
            stmt = stmt.where(DeadLetter.reason == reason)
        return int(await session.scalar(stmt) or 0)
