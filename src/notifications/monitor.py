"""Sondeo periódico de gauges que no pertenecen a ningún mensaje concreto.

- ``queue_depth{queue}``: management API de RabbitMQ (``/api/queues/<vhost>``).
- ``dlq_depth{channel}``: dead letters ``pending`` en Postgres (la cola ``<canal>.dlq`` se vacía
  en cuanto el archivador las persiste, así que la profundidad real vive en la tabla).
- ``outbox_pending``: eventos aún no publicados por el relay.

Corre en un único proceso (el de la DLQ) para no duplicar series si se escalan API o relay.
"""

from __future__ import annotations

import asyncio
from urllib.parse import quote, urlsplit

import httpx
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from notifications.db import dead_letters
from notifications.db import repositories as repo
from notifications.observability import DLQ_DEPTH, OUTBOX_PENDING, QUEUE_DEPTH, get_logger
from notifications.runtime import wait_or_stop

log = get_logger(__name__)


def credentials_from_amqp_url(amqp_url: str) -> tuple[str, str, str]:
    """``(usuario, contraseña, vhost)`` de la URL AMQP; la management API usa los mismos."""
    parts = urlsplit(amqp_url)
    vhost = parts.path[1:] if parts.path not in ("", "/") else "/"
    return parts.username or "guest", parts.password or "guest", vhost


class QueueMonitor:
    def __init__(
        self,
        management_url: str,
        amqp_url: str,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        interval: float = 5.0,
        http: httpx.AsyncClient | None = None,
    ) -> None:
        user, password, vhost = credentials_from_amqp_url(amqp_url)
        self._url = f"{management_url.rstrip('/')}/api/queues/{quote(vhost, safe='')}"
        self._sf = session_factory
        self._interval = interval
        self._http = http or httpx.AsyncClient(auth=(user, password), timeout=5.0)
        self._owns_http = http is None

    async def poll_once(self) -> None:
        await asyncio.gather(self._poll_broker(), self._poll_database())

    async def _poll_broker(self) -> None:
        try:
            resp = await self._http.get(self._url, params={"columns": "name,messages"})
            resp.raise_for_status()
            queues = resp.json()
        except (httpx.HTTPError, ValueError) as exc:
            log.warning("monitor.broker_poll_failed", error=repr(exc))
            return
        for q in queues:
            # ``messages`` puede faltar en colas recién creadas (stats aún no calculadas).
            QUEUE_DEPTH.labels(queue=q["name"]).set(q.get("messages") or 0)

    async def _poll_database(self) -> None:
        try:
            async with self._sf() as session:
                pending_dlq = await dead_letters.count_pending_by_channel(session)
                pending_outbox = await repo.count_pending_outbox(session)
        except Exception as exc:
            log.warning("monitor.db_poll_failed", error=repr(exc))
            return
        for channel, n in pending_dlq.items():
            DLQ_DEPTH.labels(channel=str(channel)).set(n)
        OUTBOX_PENDING.set(pending_outbox)

    async def run(self, stop: asyncio.Event) -> None:
        log.info("monitor.started", interval_s=self._interval)
        try:
            while not stop.is_set():
                await self.poll_once()
                await wait_or_stop(stop, within=self._interval)
        finally:
            if self._owns_http:
                await self._http.aclose()
