"""Entrypoint: ``notifications-dlq`` — archiva las DLQ en Postgres y sondea los gauges."""

from __future__ import annotations

import asyncio

from notifications.config import load_settings_or_exit
from notifications.contracts import Channel
from notifications.db.session import create_engine, create_session_factory
from notifications.dlq.archiver import DeadLetterArchiver
from notifications.messaging.rabbitmq import RabbitMQBus, RabbitMQDeadLetterConsumer
from notifications.monitor import QueueMonitor
from notifications.observability import configure_logging, get_logger, start_metrics_server
from notifications.runtime import install_shutdown_handlers, retry_until_ready

log = get_logger("notifications.dlq")


async def _run() -> None:
    settings = load_settings_or_exit()
    configure_logging(settings.log_level, json=settings.log_json)
    if settings.metrics.enabled:
        start_metrics_server(settings.metrics.port)

    stop = asyncio.Event()
    install_shutdown_handlers(stop)

    engine = create_engine(settings.db)
    bus = RabbitMQBus.from_settings(settings.rabbitmq, settings.retry)
    session_factory = create_session_factory(engine)
    archiver = DeadLetterArchiver(session_factory)
    consumers: list[RabbitMQDeadLetterConsumer] = []

    async def _connect() -> None:
        await bus.connect()
        await bus.setup_topology()

    try:
        if not await retry_until_ready("rabbitmq", _connect, stop):
            return
        for channel in Channel:
            consumers.append(await bus.consume_dead_letters(channel, archiver.handle))
        log.info("dlq_archiver.started", channels=[str(c) for c in Channel])
        # El monitor de gauges (colas, DLQ, outbox) vive aquí: proceso único, sin series duplicadas.
        monitor = QueueMonitor(
            str(settings.rabbitmq.management_url),
            str(settings.rabbitmq.url),
            session_factory,
            interval=settings.metrics.monitor_interval_seconds,
        )
        await monitor.run(stop)
    finally:
        for consumer in consumers:
            await consumer.stop(drain_timeout=settings.worker.drain_timeout_seconds)
        await bus.close()
        await engine.dispose()
        log.info("dlq_archiver.shutdown_complete")


def main() -> None:
    asyncio.run(_run())


if __name__ == "__main__":
    main()
