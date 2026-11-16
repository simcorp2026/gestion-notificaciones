"""Reinicio del broker con el worker vivo (criterio de aceptación de la Fase 6)."""

from __future__ import annotations

import asyncio

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from notifications.contracts import NotificationStatus
from notifications.db.models import AttemptOutcome
from notifications.messaging.bus import PublishError
from notifications.messaging.rabbitmq import RabbitMQBus
from notifications.messaging.topology import Topology
from notifications.worker.service import NotificationWorker

from .conftest import FAST_DELAYS, free_port, rabbit_container
from .helpers import eventually, metric
from .test_worker import CH, ScriptedSender, create, notification_id, outcomes, wait_status

SF = async_sessionmaker[AsyncSession]


async def publish_when_available(bus: RabbitMQBus, *args: object, within: float = 60) -> None:
    """Mientras la conexión robusta se recupera, el publish falla rápido: se reintenta."""
    deadline = asyncio.get_running_loop().time() + within
    while True:
        try:
            await bus.publish(*args)  # type: ignore[arg-type]
        except PublishError:
            if asyncio.get_running_loop().time() > deadline:
                raise
            await asyncio.sleep(0.5)
        else:
            return


async def test_worker_survives_broker_restart_without_reprocessing(session_factory: SF) -> None:
    port = free_port()
    container = rabbit_container(port)
    container.start()
    docker_container = container.get_wrapped_container()
    url = f"amqp://test:test@{container.get_container_host_ip()}:{port}/"
    bus = RabbitMQBus(
        url,
        Topology(retry_delays=FAST_DELAYS, namespace="restart"),
        publish_timeout=2,
        reconnect_interval=0.5,
    )
    sender = ScriptedSender(delay=1.5)  # deja un mensaje en vuelo durante el reinicio
    try:
        await bus.connect()
        await bus.setup_topology()
        worker = NotificationWorker(session_factory, sender, CH, FAST_DELAYS)
        consumer = await bus.consume(CH, worker.handle)

        # 1) Funciona antes del reinicio.
        before = await create(session_factory)
        await bus.publish(before, CH)
        await wait_status(session_factory, notification_id(before), NotificationStatus.SENT)

        # 2) Mensaje en vuelo (el sender tarda 1.5 s) y reinicio del broker a mitad.
        in_flight = await create(session_factory)
        await bus.publish(in_flight, CH)

        async def processing() -> bool:
            return len(sender.calls) == 2

        await eventually(processing, 10, "segundo mensaje en vuelo")
        await asyncio.to_thread(docker_container.stop, timeout=10)

        async def disconnected() -> bool:
            return metric("broker_connected") == 0

        await eventually(disconnected, 15, "broker_connected=0")
        await asyncio.to_thread(docker_container.start)

        async def reconnected() -> bool:
            return metric("broker_connected") == 1

        await eventually(reconnected, 60, "broker_connected=1", step=0.5)

        # 3) El mismo consumidor (sin reiniciar el worker) sigue procesando tras reconectar.
        after = await create(session_factory)
        await publish_when_available(bus, after, CH)
        await wait_status(session_factory, notification_id(after), NotificationStatus.SENT, 30)

        # El mensaje en vuelo se completó (commit antes del ack). Su ack se perdió con el canal,
        # así que el broker lo re-entregó: debe detectarse como duplicado, sin reenviarlo.
        await wait_status(session_factory, notification_id(in_flight), NotificationStatus.SENT, 30)
        await asyncio.sleep(1)
        assert sender.calls.count(notification_id(in_flight)) == 1
        assert await outcomes(session_factory, notification_id(in_flight)) == [
            (0, AttemptOutcome.OK)
        ]

        await consumer.stop(drain_timeout=5)
    finally:
        await bus.close()
        container.stop()
