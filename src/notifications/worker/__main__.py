"""Entrypoint: ``notifications-worker --channel email`` / ``python -m notifications.worker``."""

from __future__ import annotations

import argparse
import asyncio

from notifications.config import load_settings_or_exit
from notifications.contracts import Channel
from notifications.db.session import create_engine, create_session_factory
from notifications.messaging.rabbitmq import RabbitMQBus, RabbitMQConsumer
from notifications.observability import configure_logging, get_logger, start_metrics_server
from notifications.runtime import install_shutdown_handlers, retry_until_ready
from notifications.worker.senders import SimulatedSender
from notifications.worker.service import NotificationWorker

log = get_logger("notifications.worker")


async def _run(channel: Channel) -> None:
    settings = load_settings_or_exit()
    configure_logging(settings.log_level, json=settings.log_json)
    if settings.metrics.enabled:
        start_metrics_server(settings.metrics.port)

    stop = asyncio.Event()
    install_shutdown_handlers(stop)

    engine = create_engine(settings.db)
    bus = RabbitMQBus.from_settings(settings.rabbitmq, settings.retry)
    worker = NotificationWorker(
        create_session_factory(engine),
        SimulatedSender.from_settings(channel, settings.worker),
        channel,
        tuple(settings.retry.delays_seconds),
    )
    consumer: RabbitMQConsumer | None = None

    async def _connect() -> None:
        await bus.connect()
        await bus.setup_topology()

    try:
        if not await retry_until_ready("rabbitmq", _connect, stop):
            return
        consumer = await bus.consume(channel, worker.handle)
        log.info("worker.started", channel=str(channel), consumer=worker.consumer)
        await stop.wait()
    finally:
        if consumer is not None:
            await consumer.stop(drain_timeout=settings.worker.drain_timeout_seconds)
        await bus.close()
        await engine.dispose()
        log.info("worker.shutdown_complete", channel=str(channel))


def main() -> None:
    parser = argparse.ArgumentParser(description="Worker de notificaciones por canal")
    parser.add_argument("--channel", required=True, choices=[c.value for c in Channel])
    args = parser.parse_args()
    asyncio.run(_run(Channel(args.channel)))


if __name__ == "__main__":
    main()
