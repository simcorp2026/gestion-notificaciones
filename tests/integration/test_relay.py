"""Outbox relay: publicación, backoff, réplicas concurrentes, LISTEN/NOTIFY y caída del broker."""

from __future__ import annotations

import asyncio
import time
import uuid
from collections import Counter
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from notifications.config import RelaySettings
from notifications.contracts import Channel, Envelope, NotificationStatus
from notifications.db import repositories as repo
from notifications.db.models import Notification, OutboxEvent
from notifications.messaging.bus import ConsumerHandle, Handler, PublishError
from notifications.messaging.rabbitmq import RabbitMQBus
from notifications.messaging.topology import Topology
from notifications.relay.listener import OutboxListener, asyncpg_dsn
from notifications.relay.service import OutboxRelay

from .conftest import FAST_DELAYS, free_port, rabbit_container

SF = async_sessionmaker[AsyncSession]
FAST = RelaySettings(
    batch_size=50,
    poll_interval_seconds=0.1,
    backoff_base_seconds=0.2,
    backoff_max_seconds=1.0,
    listen_notify=False,
)


class FakeBus:
    """Registra publicaciones; falla para los event_id indicados (o para todos)."""

    def __init__(self, *, fail_all: bool = False, delay: float = 0.0) -> None:
        self.published: list[tuple[Envelope, Channel]] = []
        self.fail_ids: set[uuid.UUID] = set()
        self.fail_all = fail_all
        self.delay = delay

    async def connect(self) -> None: ...
    async def close(self) -> None: ...
    async def setup_topology(self) -> None: ...

    async def publish(self, envelope: Envelope, channel: Channel) -> None:
        if self.delay:
            await asyncio.sleep(self.delay)
        if self.fail_all or envelope.id in self.fail_ids:
            raise PublishError("broker caído (simulado)")
        self.published.append((envelope, channel))

    async def consume(self, channel: Channel, handler: Handler) -> ConsumerHandle:
        raise NotImplementedError


class SlowBus:
    """Envuelve un bus real añadiendo latencia, para poder matar el broker a mitad del lote."""

    def __init__(self, inner: RabbitMQBus, delay: float) -> None:
        self.inner = inner
        self.delay = delay

    async def connect(self) -> None: ...
    async def close(self) -> None: ...
    async def setup_topology(self) -> None: ...

    async def publish(self, envelope: Envelope, channel: Channel) -> None:
        await asyncio.sleep(self.delay)
        await self.inner.publish(envelope, channel)

    async def consume(self, channel: Channel, handler: Handler) -> ConsumerHandle:
        raise NotImplementedError


async def seed(sf: SF, n: int, channel: Channel = Channel.EMAIL) -> list[uuid.UUID]:
    """Crea ``n`` notificaciones por el camino real (repositorio); devuelve sus event_id."""
    async with sf() as s, s.begin():
        for i in range(n):
            await repo.create_notification(
                s,
                repo.NewNotification(channel, f"user{i}@example.com", "welcome", {"i": i}),
                correlation_id=f"seed-{i}",
            )
    async with sf() as s:
        return list((await s.scalars(select(OutboxEvent.event_id).order_by(OutboxEvent.id))).all())


async def pending(sf: SF) -> int:
    async with sf() as s:
        return await repo.count_pending_outbox(s)


async def eventually(
    check: Callable[[], Awaitable[bool]], within: float, what: str, step: float = 0.1
) -> None:
    deadline = time.monotonic() + within
    while time.monotonic() < deadline:
        if await check():
            return
        await asyncio.sleep(step)
    pytest.fail(f"timeout esperando: {what}")


# --------------------------------------------------------------------------------- tests


async def test_publishes_and_marks_published(session_factory: SF) -> None:
    event_ids = await seed(session_factory, 5)
    bus = FakeBus()

    published = await OutboxRelay(session_factory, bus, FAST).run_once()

    assert published == 5
    assert [e.id for e, _ in bus.published] == event_ids
    assert all(ch is Channel.EMAIL for _, ch in bus.published)
    assert await pending(session_factory) == 0
    async with session_factory() as s:
        statuses = set((await s.scalars(select(Notification.status))).all())
    assert statuses == {NotificationStatus.PUBLISHED}


async def test_failed_publish_stays_pending_with_backoff(session_factory: SF) -> None:
    await seed(session_factory, 3)
    bus = FakeBus(fail_all=True)
    relay = OutboxRelay(session_factory, bus, FAST)

    assert await relay.run_once() == 0

    async with session_factory() as s:
        rows = (await s.scalars(select(OutboxEvent))).all()
        statuses = set((await s.scalars(select(Notification.status))).all())
    assert all(r.published_at is None for r in rows)
    assert all(r.publish_attempts == 1 for r in rows)
    assert all("broker caído" in (r.last_error or "") for r in rows)
    assert all(r.available_at > datetime.now(UTC) for r in rows)
    assert statuses == {NotificationStatus.PENDING}

    # En backoff: una pasada inmediata no las toma.
    bus.fail_all = False
    assert await relay.run_once() == 0
    # Tras el backoff, se publican.
    await asyncio.sleep(FAST.backoff_base_seconds + 0.1)
    assert await relay.run_once() == 3


async def test_partial_failure_only_retries_failed_rows(session_factory: SF) -> None:
    event_ids = await seed(session_factory, 4)
    bus = FakeBus()
    bus.fail_ids = {event_ids[1]}

    assert await OutboxRelay(session_factory, bus, FAST).run_once() == 3

    async with session_factory() as s:
        still_pending = (
            await s.scalars(select(OutboxEvent.event_id).where(OutboxEvent.published_at.is_(None)))
        ).all()
    assert still_pending == [event_ids[1]]


async def test_concurrent_relays_never_publish_same_row_twice(session_factory: SF) -> None:
    event_ids = await seed(session_factory, 300)
    bus = FakeBus(delay=0.002)
    settings = RelaySettings(batch_size=25, listen_notify=False)
    relays = [OutboxRelay(session_factory, bus, settings) for _ in range(4)]

    async def drain(relay: OutboxRelay) -> None:
        while await relay.run_once() or await pending(session_factory):
            pass

    await asyncio.wait_for(asyncio.gather(*(drain(r) for r in relays)), timeout=60)

    counts = Counter(e.id for e, _ in bus.published)
    assert set(counts) == set(event_ids)
    assert max(counts.values()) == 1


async def test_listen_notify_wakes_relay_without_waiting_for_poll(
    session_factory: SF, migrated_db: str
) -> None:
    bus = FakeBus()
    listener = OutboxListener(asyncpg_dsn(migrated_db))
    slow_poll = RelaySettings(poll_interval_seconds=30, listen_notify=True)
    relay = OutboxRelay(session_factory, bus, slow_poll, listener=listener)
    stop = asyncio.Event()
    task = asyncio.create_task(relay.run(stop))
    try:
        await eventually(lambda: _async_true(listener.connected), 5, "listener conectado")
        await asyncio.sleep(0.2)  # el relay ya hizo su primera pasada y duerme 30 s

        await seed(session_factory, 1)

        await eventually(lambda: _async_true(len(bus.published) == 1), 3, "publicación por NOTIFY")
    finally:
        stop.set()
        await asyncio.wait_for(task, 5)
        await listener.close()


async def _async_true(value: bool) -> bool:
    return value


async def test_broker_crash_during_publish_loses_no_events(session_factory: SF) -> None:
    """Criterio de aceptación de la Fase 3: se mata (SIGKILL) RabbitMQ a mitad de publicación;
    al volver, todos los eventos del outbox están en la cola (duplicados sí, pérdidas no)."""
    port = free_port()
    container = rabbit_container(port)
    container.start()
    docker_container = container.get_wrapped_container()
    url = f"amqp://test:test@{container.get_container_host_ip()}:{port}/"
    topology = Topology(retry_delays=FAST_DELAYS, namespace="crash")
    bus = RabbitMQBus(url, topology, publish_timeout=2, reconnect_interval=0.5)
    try:
        await bus.connect()
        await bus.setup_topology()
        event_ids = await seed(session_factory, 400)

        settings = RelaySettings(
            batch_size=10,
            poll_interval_seconds=0.1,
            backoff_base_seconds=0.2,
            backoff_max_seconds=1.0,
            listen_notify=False,
        )
        relay = OutboxRelay(session_factory, SlowBus(bus, delay=0.05), settings)
        stop = asyncio.Event()
        task = asyncio.create_task(relay.run(stop))

        async def some_published() -> bool:
            return await pending(session_factory) <= 300

        await eventually(some_published, 30, "primeros 100 publicados")
        await asyncio.to_thread(docker_container.kill)  # SIGKILL, sin shutdown limpio
        await asyncio.sleep(3)
        pending_while_down = await pending(session_factory)
        assert pending_while_down > 0, "el broker cayó después de publicar todo: test no válido"

        async with session_factory() as s:
            failed = await s.scalar(
                select(func.count())
                .select_from(OutboxEvent)
                .where(OutboxEvent.publish_attempts > 0)
            )
        assert failed, "el relay debería haber registrado fallos mientras el broker estaba caído"

        await asyncio.to_thread(docker_container.start)

        async def all_published() -> bool:
            return await pending(session_factory) == 0

        await eventually(all_published, 90, "outbox vacío tras recuperar el broker", step=0.5)
        stop.set()
        await asyncio.wait_for(task, 10)

        received = await drain_queue(bus, topology.main_queue(Channel.EMAIL))
        missing = set(event_ids) - set(received)
        assert not missing, f"{len(missing)} eventos perdidos"
        # Duplicados posibles (at-least-once): publish confirmado + commit perdido.
        assert len(received) >= len(event_ids)
    finally:
        await bus.close()
        container.stop()


async def drain_queue(bus: RabbitMQBus, queue: str) -> list[uuid.UUID]:
    channel = await bus.connection.channel()
    ids: list[uuid.UUID] = []
    try:
        q = await channel.get_queue(queue)
        while (msg := await q.get(no_ack=True, fail=False)) is not None:
            ids.append(uuid.UUID(msg.message_id))
    finally:
        await channel.close()
    return ids
