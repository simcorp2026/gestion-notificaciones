"""Entrypoint: ``notifications-relay`` / ``python -m notifications.relay``."""

from __future__ import annotations

import asyncio

from notifications.config import load_settings_or_exit
from notifications.db.session import create_engine, create_session_factory
from notifications.messaging.rabbitmq import RabbitMQBus
from notifications.observability import configure_logging, get_logger, start_metrics_server
from notifications.relay.listener import OutboxListener, asyncpg_dsn
from notifications.relay.service import OutboxRelay
from notifications.runtime import install_shutdown_handlers, retry_until_ready

log = get_logger("notifications.relay")


async def _run() -> None:
    settings = load_settings_or_exit()
    configure_logging(settings.log_level, json=settings.log_json)
    if settings.metrics.enabled:
        start_metrics_server(settings.metrics.port)

    stop = asyncio.Event()
    install_shutdown_handlers(stop)

    engine = create_engine(settings.db)
    bus = RabbitMQBus.from_settings(settings.rabbitmq, settings.retry)
    listener = (
        OutboxListener(asyncpg_dsn(str(settings.db.url))) if settings.relay.listen_notify else None
    )

    async def _connect_bus() -> None:
        await bus.connect()
        await bus.setup_topology()

    try:
        if not await retry_until_ready("rabbitmq", _connect_bus, stop):
            return
        relay = OutboxRelay(create_session_factory(engine), bus, settings.relay, listener=listener)
        await relay.run(stop)
    finally:
        if listener is not None:
            await listener.close()
        await bus.close()
        await engine.dispose()
        log.info("relay.shutdown_complete")


def main() -> None:
    asyncio.run(_run())


if __name__ == "__main__":
    main()
