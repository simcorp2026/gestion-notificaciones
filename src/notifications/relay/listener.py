"""LISTEN/NOTIFY sobre una conexión asyncpg dedicada: despierta al relay al insertar en outbox.

Es sólo una optimización de latencia. Si la conexión se pierde, el relay sigue funcionando por
polling y el listener se reconecta en la siguiente iteración.
"""

from __future__ import annotations

import asyncio
from typing import Any

import asyncpg

from notifications.db.repositories import OUTBOX_NOTIFY_CHANNEL
from notifications.observability import get_logger

log = get_logger(__name__)


def asyncpg_dsn(sqlalchemy_url: str) -> str:
    return sqlalchemy_url.replace("postgresql+asyncpg://", "postgresql://", 1)


class OutboxListener:
    def __init__(self, dsn: str, channel: str = OUTBOX_NOTIFY_CHANNEL) -> None:
        self._dsn = dsn
        self._channel = channel
        self._conn: Any = None  # asyncpg.Connection (sin stubs de tipos)
        self.event = asyncio.Event()

    @property
    def connected(self) -> bool:
        return self._conn is not None and not self._conn.is_closed()

    async def ensure_connected(self) -> None:
        if self.connected:
            return
        try:
            self._conn = await asyncpg.connect(self._dsn)
            await self._conn.add_listener(self._channel, self._on_notify)
            log.info("relay.listen_started", channel=self._channel)
            # Pudo haber inserts mientras no escuchábamos: forzamos una pasada.
            self.event.set()
        except (OSError, asyncpg.PostgresError) as exc:
            self._conn = None
            log.warning("relay.listen_unavailable", error=repr(exc))

    def _on_notify(self, *_: object) -> None:
        self.event.set()

    async def close(self) -> None:
        if self._conn is not None and not self._conn.is_closed():
            await self._conn.close()
        self._conn = None
