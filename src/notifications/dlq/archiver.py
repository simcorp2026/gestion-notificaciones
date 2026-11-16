"""Consumidor de ``<canal>.dlq``: persiste cada mensaje muerto en ``dead_letters`` y hace ack.

Es el punto único donde una notificación pasa a ``dead`` aunque el mensaje no haya llegado a
procesarlo un worker (envelope ilegible, ``delivery-limit`` del broker).
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import UTC, datetime
from typing import Any

from pydantic import ValidationError
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from notifications.contracts import (
    NOTIFICATION_REQUESTED,
    Envelope,
    NotificationRequestedV1,
    NotificationStatus,
)
from notifications.db import dead_letters as repo
from notifications.db import delivery
from notifications.dlq.parsing import extract_attempts, extract_dead_at, extract_reason, json_safe
from notifications.messaging.bus import RawMessage
from notifications.messaging.headers import H_ERROR_CLASS, H_ERROR_MESSAGE, H_TRACEBACK
from notifications.observability import DEAD_LETTERS_ARCHIVED, correlation_context, get_logger

log = get_logger(__name__)


def build_dead_letter(raw: RawMessage, *, now: datetime | None = None) -> dict[str, Any]:
    """Valores de la fila ``dead_letters`` a partir del mensaje crudo (puro, testeable)."""
    headers = raw.headers
    envelope: Envelope | None = None
    try:
        envelope = Envelope.from_bytes(raw.body)
    except ValidationError:
        envelope = None

    notification_id: uuid.UUID | None = None
    if envelope is not None and envelope.type == NOTIFICATION_REQUESTED:
        try:
            notification_id = NotificationRequestedV1.model_validate(
                envelope.payload
            ).notification_id
        except ValidationError:
            notification_id = None

    def _opt(key: str) -> str | None:
        value = headers.get(key)
        return str(value) if value is not None else None

    return {
        "channel": raw.channel,
        "message_id": raw.message_id,
        "notification_id": notification_id,
        "correlation_id": raw.correlation_id or (envelope.correlation_id if envelope else None),
        "reason": extract_reason(headers)[:32],
        "error_class": _opt(H_ERROR_CLASS),
        "error_message": _opt(H_ERROR_MESSAGE),
        "traceback": _opt(H_TRACEBACK),
        "attempts": extract_attempts(headers),
        "body": raw.body,
        "content_type": raw.content_type,
        "message_type": raw.type,
        "headers": json_safe(dict(headers)),
        "envelope": envelope.model_dump(mode="json") if envelope else None,
        "dead_at": extract_dead_at(headers) or now or datetime.now(UTC),
    }


class DeadLetterArchiver:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        retry_base_seconds: float = 0.5,
        retry_max_seconds: float = 30.0,
    ) -> None:
        self._sf = session_factory
        self._retry_base = retry_base_seconds
        self._retry_max = retry_max_seconds

    async def handle(self, raw: RawMessage) -> None:
        """``DeadLetterHandler`` del bus. Si la DB está caída, reintenta aquí sin devolver el
        mensaje a la cola (cada devolución cuenta para el ``delivery-limit`` de la quorum queue)."""
        values = build_dead_letter(raw)
        delay = self._retry_base
        with correlation_context(values["correlation_id"]):
            while True:
                try:
                    await self._archive(values)
                except (SQLAlchemyError, OSError) as exc:
                    log.warning("dead_letter.archive_retry", retry_in_s=delay, error=repr(exc))
                    await asyncio.sleep(delay)
                    delay = min(delay * 2, self._retry_max)
                else:
                    return

    async def _archive(self, values: dict[str, Any]) -> None:
        async with self._sf() as session, session.begin():
            inserted = await repo.insert_dead_letter(session, values)
            if inserted is not None and values["notification_id"] is not None:
                await delivery.set_status(
                    session, values["notification_id"], NotificationStatus.DEAD
                )
        if inserted is None:
            log.info("dead_letter.duplicate")
        else:
            DEAD_LETTERS_ARCHIVED.labels(
                channel=str(values["channel"]), reason=values["reason"]
            ).inc()
            log.info(
                "dead_letter.archived",
                dead_letter_id=inserted,
                reason=values["reason"],
                notification_id=str(values["notification_id"]),
            )
