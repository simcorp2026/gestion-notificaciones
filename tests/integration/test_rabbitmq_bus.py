"""Flujo real contra RabbitMQ: éxito, retry escalonado, DLQ, mensajes inválidos, confirms."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Sequence
from dataclasses import dataclass, field

import pytest
from aio_pika import Message
from aio_pika.abc import AbstractIncomingMessage

from notifications.contracts import Channel, Envelope
from notifications.errors import PermanentError, TransientError
from notifications.messaging.bus import DeliveryContext, MessageBus, PublishError
from notifications.messaging.rabbitmq import RabbitMQBus

CH = Channel.EMAIL


@dataclass
class ScriptedHandler:
    """Lanza las excepciones de ``script`` en orden; después, éxito. Registra cada intento."""

    script: Sequence[BaseException | None] = ()
    always: BaseException | None = None
    delay: float = 0.0
    attempts: list[int] = field(default_factory=list)
    done: asyncio.Event = field(default_factory=asyncio.Event)
    expected_calls: int = 1

    async def __call__(self, envelope: Envelope, ctx: DeliveryContext) -> None:
        call = len(self.attempts)
        self.attempts.append(ctx.attempt)
        if len(self.attempts) >= self.expected_calls:
            self.done.set()
        if self.delay:
            await asyncio.sleep(self.delay)
        if self.always is not None:
            raise self.always
        if call < len(self.script) and (exc := self.script[call]) is not None:
            raise exc

    async def wait(self, within: float = 10) -> None:
        await asyncio.wait_for(self.done.wait(), within)


async def get_one(bus: RabbitMQBus, queue: str, within: float = 10) -> AbstractIncomingMessage:
    """Lee (y confirma) un mensaje de ``queue`` haciendo polling hasta ``within``."""
    channel = await bus.connection.channel()
    try:
        q = await channel.get_queue(queue, ensure=True)
        deadline = time.monotonic() + within
        while time.monotonic() < deadline:
            msg = await q.get(no_ack=True, fail=False)
            if msg is not None:
                return msg
            await asyncio.sleep(0.05)
        pytest.fail(f"sin mensajes en {queue} tras {within}s")
    finally:
        await channel.close()


async def depth(bus: RabbitMQBus, queue: str) -> int:
    channel = await bus.connection.channel()
    try:
        q = await channel.declare_queue(queue, passive=True)
        return q.declaration_result.message_count or 0
    finally:
        await channel.close()


async def run(bus: RabbitMQBus, handler: ScriptedHandler) -> Envelope:
    consumer = await bus.consume(CH, handler)
    envelope = Envelope(type="notification.requested", payload={"to": "user@example.com"})
    try:
        await bus.publish(envelope, CH)
        await handler.wait()
        await asyncio.sleep(0.3)  # deja terminar el ack/republish del último intento
    finally:
        await consumer.stop(drain_timeout=5)
    return envelope


# --------------------------------------------------------------------------- tests


async def test_rabbitmq_bus_satisfies_protocol(bus: RabbitMQBus) -> None:
    proto: MessageBus = bus  # verificación estática (mypy) + runtime trivial
    assert proto is bus


async def test_setup_topology_is_idempotent(bus: RabbitMQBus) -> None:
    await bus.setup_topology()


async def test_success_is_acked(bus: RabbitMQBus) -> None:
    handler = ScriptedHandler()

    await run(bus, handler)

    assert handler.attempts == [0]
    assert await depth(bus, bus.topology.main_queue(CH)) == 0
    assert await depth(bus, bus.topology.dlq(CH)) == 0


async def test_transient_exhausts_three_retries_then_dlq(bus: RabbitMQBus) -> None:
    """Criterio de aceptación de la Fase 2."""
    handler = ScriptedHandler(always=TransientError("smtp timeout"), expected_calls=4)

    envelope = await run(bus, handler)

    assert handler.attempts == [0, 1, 2, 3]
    dead = await get_one(bus, bus.topology.dlq(CH))
    headers = dead.headers
    assert headers["x-attempt"] == 3
    assert headers["x-dead-reason"] == "exhausted"
    assert headers["x-error-class"] == "TransientError"
    assert headers["x-error-message"] == "smtp timeout"
    assert "TransientError: smtp timeout" in str(headers["x-traceback"])
    assert headers["x-original-routing-key"] == bus.topology.main_queue(CH)
    assert "x-first-failed-at" in headers
    assert dead.message_id == str(envelope.id)
    assert dead.correlation_id == envelope.correlation_id
    assert Envelope.from_bytes(dead.body) == envelope
    for q in [bus.topology.main_queue(CH), *bus.topology.retry_queues(CH)]:
        assert await depth(bus, q) == 0, q


async def test_transient_then_success_does_not_reach_dlq(bus: RabbitMQBus) -> None:
    handler = ScriptedHandler(
        script=[TransientError("1"), RuntimeError("no clasificada")], expected_calls=3
    )

    await run(bus, handler)

    assert handler.attempts == [0, 1, 2]
    assert await depth(bus, bus.topology.dlq(CH)) == 0


async def test_permanent_goes_straight_to_dlq(bus: RabbitMQBus) -> None:
    handler = ScriptedHandler(always=PermanentError("destinatario inválido"))

    await run(bus, handler)

    assert handler.attempts == [0]
    dead = await get_one(bus, bus.topology.dlq(CH))
    assert dead.headers["x-dead-reason"] == "permanent"
    assert dead.headers["x-attempt"] == 0
    for q in bus.topology.retry_queues(CH):
        assert await depth(bus, q) == 0


async def test_invalid_envelope_goes_to_dlq(bus: RabbitMQBus) -> None:
    handler = ScriptedHandler()
    consumer = await bus.consume(CH, handler)
    try:
        await bus.publish_to_queue(
            Message(b"{not-json", message_id="bad-1"), bus.topology.main_queue(CH)
        )
        dead = await get_one(bus, bus.topology.dlq(CH))
    finally:
        await consumer.stop(drain_timeout=5)

    assert handler.attempts == []
    assert dead.headers["x-dead-reason"] == "invalid"
    assert dead.body == b"{not-json"


async def test_unroutable_publish_raises(bus: RabbitMQBus) -> None:
    with pytest.raises(PublishError):
        await bus.publish_to_queue(Message(b"x"), "cola-que-no-existe")


async def test_stop_waits_for_inflight_messages(bus: RabbitMQBus) -> None:
    handler = ScriptedHandler(delay=0.5)
    consumer = await bus.consume(CH, handler)
    await bus.publish(Envelope(type="t", payload={}), CH)
    await handler.wait()

    await consumer.stop(drain_timeout=5)  # el handler sigue durmiendo cuando se llama a stop

    assert await depth(bus, bus.topology.main_queue(CH)) == 0
