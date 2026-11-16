"""Worker contra RabbitMQ y Postgres reales: idempotencia, clasificación de errores, historial."""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass, field

import httpx
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from notifications.config import RelaySettings
from notifications.contracts import Channel, Envelope, NotificationRequestedV1, NotificationStatus
from notifications.db import delivery
from notifications.db import repositories as repo
from notifications.db.models import AttemptOutcome, Notification, OutboxEvent, ProcessedMessage
from notifications.errors import TransientError
from notifications.messaging.rabbitmq import RabbitMQBus
from notifications.relay.service import OutboxRelay
from notifications.worker.senders import Sender, SimulatedSender
from notifications.worker.service import NotificationWorker

from .conftest import FAST_DELAYS
from .helpers import eventually, get_one, metric, queue_depth

SF = async_sessionmaker[AsyncSession]
CH = Channel.EMAIL


@dataclass
class ScriptedSender:
    """Falla según ``script`` (por llamada) o ``always``; registra cada envío intentado."""

    script: Sequence[BaseException | None] = ()
    always: BaseException | None = None
    delay: float = 0.0
    calls: list[uuid.UUID] = field(default_factory=list)

    async def send(self, notification: NotificationRequestedV1) -> None:
        call = len(self.calls)
        self.calls.append(notification.notification_id)
        if self.delay:
            await asyncio.sleep(self.delay)
        if self.always is not None:
            raise self.always
        if call < len(self.script) and (exc := self.script[call]) is not None:
            raise exc


async def create(sf: SF, recipient: str = "ada@example.com", channel: Channel = CH) -> Envelope:
    """Crea una notificación por el camino real y devuelve el envelope de su outbox."""
    async with sf() as s, s.begin():
        result = await repo.create_notification(
            s,
            repo.NewNotification(channel, recipient, "welcome", {"name": "Ada"}),
            correlation_id=f"test-{uuid.uuid4().hex[:6]}",
        )
        nid = result.notification.id
    async with sf() as s:
        row = await s.scalar(select(OutboxEvent).where(OutboxEvent.notification_id == nid))
        assert row is not None
        return Envelope.model_validate(row.envelope)


def notification_id(envelope: Envelope) -> uuid.UUID:
    return NotificationRequestedV1.model_validate(envelope.payload).notification_id


@asynccontextmanager
async def running(
    bus: RabbitMQBus, sf: SF, sender: Sender, channel: Channel = CH
) -> AsyncIterator[NotificationWorker]:
    worker = NotificationWorker(sf, sender, channel, FAST_DELAYS)
    consumer = await bus.consume(channel, worker.handle)
    try:
        yield worker
    finally:
        await consumer.stop(drain_timeout=5)


async def status_of(sf: SF, nid: uuid.UUID) -> NotificationStatus:
    async with sf() as s:
        return (await s.get_one(Notification, nid)).status


async def wait_status(
    sf: SF, nid: uuid.UUID, status: NotificationStatus, within: float = 10
) -> None:
    async def check() -> bool:
        return await status_of(sf, nid) is status

    await eventually(check, within, f"estado {status}")


async def outcomes(sf: SF, nid: uuid.UUID) -> list[tuple[int, AttemptOutcome]]:
    async with sf() as s:
        return [(a.attempt, a.outcome) for a in await delivery.list_attempts(s, nid)]


# --------------------------------------------------------------------------------- tests


async def test_success_marks_sent_and_records_attempt(
    bus: RabbitMQBus, session_factory: SF
) -> None:
    envelope = await create(session_factory)
    nid = notification_id(envelope)
    sender = ScriptedSender()

    async with running(bus, session_factory, sender):
        await bus.publish(envelope, CH)
        await wait_status(session_factory, nid, NotificationStatus.SENT)

    assert sender.calls == [nid]
    assert await outcomes(session_factory, nid) == [(0, AttemptOutcome.OK)]
    async with session_factory() as s:
        assert await delivery.is_processed(s, envelope.id, "email-worker")


async def test_duplicate_delivery_is_acked_without_reprocessing(
    bus: RabbitMQBus, session_factory: SF
) -> None:
    """Criterio de aceptación de la Fase 4 (duplicados)."""
    envelope = await create(session_factory)
    nid = notification_id(envelope)
    sender = ScriptedSender()
    dup_before = metric("notifications_processed_total", channel="email", result="duplicate")

    async with running(bus, session_factory, sender):
        await bus.publish(envelope, CH)
        await wait_status(session_factory, nid, NotificationStatus.SENT)
        await bus.publish(envelope, CH)  # p. ej. el relay republicó tras caer antes del commit

        async def duplicate_seen() -> bool:
            now = metric("notifications_processed_total", channel="email", result="duplicate")
            return now == dup_before + 1

        await eventually(duplicate_seen, 10, "duplicado procesado")

    assert sender.calls == [nid]
    assert await status_of(session_factory, nid) is NotificationStatus.SENT
    assert await queue_depth(bus, bus.topology.main_queue(CH)) == 0
    assert await queue_depth(bus, bus.topology.dlq(CH)) == 0


async def test_concurrent_duplicates_are_processed_once(
    bus: RabbitMQBus, session_factory: SF
) -> None:
    envelope = await create(session_factory)
    nid = notification_id(envelope)
    sender = ScriptedSender(delay=0.3)  # todas las copias en vuelo a la vez (prefetch=5)
    dup_before = metric("notifications_processed_total", channel="email", result="duplicate")

    async with running(bus, session_factory, sender):
        for _ in range(5):
            await bus.publish(envelope, CH)

        async def all_handled() -> bool:
            now = metric("notifications_processed_total", channel="email", result="duplicate")
            return now == dup_before + 4

        await eventually(all_handled, 10, "4 duplicados")

    assert sender.calls == [nid]
    async with session_factory() as s:
        processed = await s.scalar(
            select(func.count())
            .select_from(ProcessedMessage)
            .where(ProcessedMessage.message_id == envelope.id)
        )
    assert processed == 1


async def test_transient_failures_then_success_keeps_full_history(
    bus: RabbitMQBus, session_factory: SF
) -> None:
    envelope = await create(session_factory)
    nid = notification_id(envelope)
    sender = ScriptedSender(script=[TransientError("timeout"), RuntimeError("no clasificada")])

    async with running(bus, session_factory, sender):
        await bus.publish(envelope, CH)
        await wait_status(session_factory, nid, NotificationStatus.SENT)

    assert len(sender.calls) == 3
    assert await outcomes(session_factory, nid) == [
        (0, AttemptOutcome.RETRY),
        (1, AttemptOutcome.RETRY),
        (2, AttemptOutcome.OK),
    ]


async def test_exhausted_retries_end_dead_in_dlq(bus: RabbitMQBus, session_factory: SF) -> None:
    envelope = await create(session_factory)
    nid = notification_id(envelope)
    sender = ScriptedSender(always=TransientError("proveedor caído"))

    async with running(bus, session_factory, sender):
        await bus.publish(envelope, CH)
        await wait_status(session_factory, nid, NotificationStatus.DEAD)
        dead = await get_one(bus, bus.topology.dlq(CH))

    assert len(sender.calls) == 4
    assert [o for _, o in await outcomes(session_factory, nid)] == [
        AttemptOutcome.RETRY,
        AttemptOutcome.RETRY,
        AttemptOutcome.RETRY,
        AttemptOutcome.DEAD,
    ]
    assert dead.headers["x-dead-reason"] == "exhausted"
    assert dead.message_id == str(envelope.id)
    async with session_factory() as s:
        assert not await delivery.is_processed(s, envelope.id, "email-worker")


async def test_permanent_error_goes_straight_to_dlq(bus: RabbitMQBus, session_factory: SF) -> None:
    """Criterio de aceptación de la Fase 4 (errores permanentes): destinatario inválido."""
    envelope = await create(session_factory, recipient="esto-no-es-un-email")
    nid = notification_id(envelope)
    sender = SimulatedSender(CH, latency_ms=(0, 0))

    async with running(bus, session_factory, sender):
        await bus.publish(envelope, CH)
        await wait_status(session_factory, nid, NotificationStatus.DEAD)
        dead = await get_one(bus, bus.topology.dlq(CH))

    assert await outcomes(session_factory, nid) == [(0, AttemptOutcome.DEAD)]
    async with session_factory() as s:
        (attempt,) = await delivery.list_attempts(s, nid)
    assert attempt.error_class == "PermanentError"
    assert "email inválido" in (attempt.error_message or "")
    assert dead.headers["x-dead-reason"] == "permanent"
    assert dead.headers["x-attempt"] == 0
    for q in bus.topology.retry_queues(CH):
        assert await queue_depth(bus, q) == 0


async def test_unknown_event_type_is_permanent(bus: RabbitMQBus, session_factory: SF) -> None:
    sender = ScriptedSender()
    envelope = Envelope(type="notification.cancelled", payload={})

    async with running(bus, session_factory, sender):
        await bus.publish(envelope, CH)
        dead = await get_one(bus, bus.topology.dlq(CH))

    assert sender.calls == []
    assert dead.headers["x-dead-reason"] == "permanent"
    assert "no soportado" in str(dead.headers["x-error-message"])


async def test_missing_notification_is_permanent(bus: RabbitMQBus, session_factory: SF) -> None:
    sender = ScriptedSender()
    payload = NotificationRequestedV1(
        notification_id=uuid.uuid4(), channel=CH, recipient="a@b.co", template="t"
    )
    envelope = Envelope(type="notification.requested", payload=payload.model_dump(mode="json"))

    async with running(bus, session_factory, sender):
        await bus.publish(envelope, CH)
        dead = await get_one(bus, bus.topology.dlq(CH))

    assert sender.calls == []
    assert dead.headers["x-dead-reason"] == "permanent"
    assert "inexistente" in str(dead.headers["x-error-message"])


async def test_end_to_end_api_relay_worker(
    api: httpx.AsyncClient, bus: RabbitMQBus, session_factory: SF
) -> None:
    resp = await api.post(
        "/notifications",
        json={"channel": "sms", "recipient": "+34600111222", "template": "otp"},
        headers={"X-Correlation-ID": "e2e-1"},
    )
    nid = uuid.UUID(resp.json()["notification_id"])
    relay = OutboxRelay(session_factory, bus, RelaySettings(listen_notify=False))
    sender = SimulatedSender(Channel.SMS, latency_ms=(1, 5))

    async with running(bus, session_factory, sender, channel=Channel.SMS):
        assert await relay.run_once() == 1
        await wait_status(session_factory, nid, NotificationStatus.SENT)

    body = (await api.get(f"/notifications/{nid}")).json()
    assert body["status"] == "sent"
    assert body["correlation_id"] == "e2e-1"


async def test_permanent_error_from_simulated_provider_rate(
    bus: RabbitMQBus, session_factory: SF
) -> None:
    envelope = await create(session_factory)
    nid = notification_id(envelope)
    sender = SimulatedSender(CH, latency_ms=(0, 0), permanent_failure_rate=1.0)

    async with running(bus, session_factory, sender):
        await bus.publish(envelope, CH)
        await wait_status(session_factory, nid, NotificationStatus.DEAD)

    assert await outcomes(session_factory, nid) == [(0, AttemptOutcome.DEAD)]
