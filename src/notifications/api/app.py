"""Factory de la aplicación FastAPI."""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from prometheus_client import make_asgi_app

from notifications import __version__
from notifications.api.middleware import CorrelationIdMiddleware
from notifications.api.routers import admin, health, notifications
from notifications.config import Settings, get_settings
from notifications.db.session import create_engine, create_session_factory
from notifications.dlq.admin import DeadLetterAdmin
from notifications.messaging.bus import Replayer
from notifications.messaging.rabbitmq import RabbitMQBus
from notifications.observability import get_logger
from notifications.runtime import retry_until_ready

log = get_logger(__name__)


def create_app(settings: Settings | None = None, *, replayer: Replayer | None = None) -> FastAPI:
    """``replayer`` inyectable (tests). Si no se pasa, se crea un ``RabbitMQBus`` que conecta en
    segundo plano: la API arranca y acepta POSTs aunque el broker esté caído; sólo el replay
    responde 503 hasta que conecte."""
    settings = settings or get_settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        engine = create_engine(settings.db)
        session_factory = create_session_factory(engine)
        app.state.engine = engine
        app.state.session_factory = session_factory

        owned_bus: RabbitMQBus | None = None
        connect_task: asyncio.Task[bool] | None = None
        stop = asyncio.Event()
        active_replayer: Replayer
        if replayer is None:
            owned_bus = RabbitMQBus.from_settings(settings.rabbitmq, settings.retry)
            connect_task = asyncio.create_task(
                retry_until_ready("rabbitmq", owned_bus.connect, stop)
            )
            active_replayer = owned_bus
        else:
            active_replayer = replayer
        app.state.dead_letter_admin = DeadLetterAdmin(session_factory, active_replayer)
        log.info("api.started")
        try:
            yield
        finally:
            stop.set()
            if connect_task is not None:
                connect_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await connect_task
            if owned_bus is not None:
                await owned_bus.close()
            await engine.dispose()
            log.info("api.stopped")

    app = FastAPI(title="Notifications", version=__version__, lifespan=lifespan)
    app.state.settings = settings
    app.add_middleware(CorrelationIdMiddleware)
    app.include_router(health.router)
    app.include_router(notifications.router)
    app.include_router(admin.router)
    # /metrics del propio proceso API (métricas de aceptación + runtime).
    app.mount("/metrics", make_asgi_app())
    return app
