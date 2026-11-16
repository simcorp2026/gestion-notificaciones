"""Utilidades compartidas por los tests de integración."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable

import pytest
from aio_pika.abc import AbstractIncomingMessage
from prometheus_client import REGISTRY

from notifications.messaging.rabbitmq import RabbitMQBus


async def eventually(
    check: Callable[[], Awaitable[bool]], within: float, what: str, step: float = 0.05
) -> None:
    deadline = time.monotonic() + within
    while time.monotonic() < deadline:
        if await check():
            return
        await asyncio.sleep(step)
    pytest.fail(f"timeout ({within}s) esperando: {what}")


async def get_one(bus: RabbitMQBus, queue: str, within: float = 10) -> AbstractIncomingMessage:
    """Lee (y confirma) un mensaje de ``queue`` haciendo polling hasta ``within`` segundos."""
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


async def queue_depth(bus: RabbitMQBus, queue: str) -> int:
    channel = await bus.connection.channel()
    try:
        q = await channel.declare_queue(queue, passive=True)
        return q.declaration_result.message_count or 0
    finally:
        await channel.close()


def metric(name: str, **labels: str) -> float:
    return REGISTRY.get_sample_value(name, labels) or 0.0
