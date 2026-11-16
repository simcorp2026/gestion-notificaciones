from __future__ import annotations

import asyncio
import socket
import uuid
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
import pytest
from alembic import command
from alembic.config import Config
from fastapi import FastAPI
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker
from testcontainers.community.postgres import PostgresContainer
from testcontainers.core.container import DockerContainer
from testcontainers.core.wait_strategies import LogMessageWaitStrategy

from notifications.api.app import create_app
from notifications.config import (
    ApiSettings,
    DatabaseSettings,
    RabbitMQSettings,
    Settings,
)
from notifications.db.session import create_engine, create_session_factory
from notifications.messaging.bus import PublishError, RawMessage
from notifications.messaging.rabbitmq import RabbitMQBus
from notifications.messaging.topology import Topology

ROOT = Path(__file__).resolve().parents[2]
RABBIT_IMAGE = "rabbitmq:4-management-alpine"
# Delays cortos: el recorrido completo de 3 retries tarda ~1.2 s.
FAST_DELAYS = (0.2, 0.4, 0.6)


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    for item in items:
        if Path(str(item.fspath)).is_relative_to(Path(__file__).parent):
            item.add_marker(pytest.mark.integration)


def make_alembic_config(url: str) -> Config:
    cfg = Config(str(ROOT / "alembic.ini"))
    cfg.set_main_option("script_location", str(ROOT / "migrations"))
    cfg.set_main_option("sqlalchemy.url", url)
    cfg.attributes["configure_logging"] = False
    return cfg


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def rabbit_container(host_port: int | None = None) -> DockerContainer:
    # Contenedor genérico: el módulo rabbitmq de testcontainers arrastra `pika` (cliente síncrono).
    c = (
        DockerContainer(RABBIT_IMAGE)
        .with_env("RABBITMQ_DEFAULT_USER", "test")
        .with_env("RABBITMQ_DEFAULT_PASS", "test")
        .waiting_for(LogMessageWaitStrategy("Server startup complete").with_startup_timeout(90))
    )
    return c.with_bind_ports(5672, host_port) if host_port else c.with_exposed_ports(5672)


# ----------------------------------------------------------------------------- postgres
@pytest.fixture(scope="session")
def postgres_url() -> Iterator[str]:
    with PostgresContainer("postgres:17-alpine", driver="asyncpg") as pg:
        yield pg.get_connection_url()


@pytest.fixture
def alembic_config(postgres_url: str) -> Config:
    return make_alembic_config(postgres_url)


@pytest.fixture(scope="session")
async def migrated_db(postgres_url: str) -> str:
    # env.py usa asyncio.run → se ejecuta en otro hilo para no chocar con el loop de pytest.
    await asyncio.to_thread(command.upgrade, make_alembic_config(postgres_url), "head")
    return postgres_url


@pytest.fixture(scope="session")
async def engine(migrated_db: str) -> AsyncIterator[AsyncEngine]:
    eng = create_engine(DatabaseSettings(url=migrated_db))  # type: ignore[arg-type]
    yield eng
    await eng.dispose()


@pytest.fixture
async def session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    """Base limpia por test."""
    async with engine.begin() as conn:
        await conn.execute(
            text(
                "TRUNCATE notifications, outbox, processed_messages, delivery_attempts,"
                " dead_letters RESTART IDENTITY CASCADE"
            )
        )
    return create_session_factory(engine)


# ----------------------------------------------------------------------------- rabbitmq
@pytest.fixture(scope="session")
def rabbitmq_url() -> Iterator[str]:
    container = rabbit_container()
    with container:
        host = container.get_container_host_ip()
        port = container.get_exposed_port(5672)
        yield f"amqp://test:test@{host}:{port}/"


@pytest.fixture
async def bus(rabbitmq_url: str) -> AsyncIterator[RabbitMQBus]:
    """Bus con topología aislada por test (namespace único) y delays cortos."""
    topology = Topology(retry_delays=FAST_DELAYS, namespace=f"t{uuid.uuid4().hex[:8]}")
    b = RabbitMQBus(rabbitmq_url, topology, prefetch=5, publish_timeout=5)
    await b.connect()
    await b.setup_topology()
    try:
        yield b
    finally:
        await b.close()


# ----------------------------------------------------------------------------- api
@pytest.fixture
def settings(migrated_db: str) -> Settings:
    return Settings(
        env="test",
        db=DatabaseSettings(url=migrated_db),  # type: ignore[arg-type]
        rabbitmq=RabbitMQSettings(
            url="amqp://unused:unused@localhost:1/",  # type: ignore[arg-type]
            management_url="http://localhost:1/",  # type: ignore[arg-type]
        ),
        api=ApiSettings(admin_key=ADMIN_KEY),  # type: ignore[arg-type]
    )


ADMIN_KEY = "test-admin-key-0123456789"


class UnavailableReplayer:
    """Replayer de un broker caído: toda publicación falla."""

    async def replay(self, message: RawMessage) -> None:
        raise PublishError("broker caído (test)")


@asynccontextmanager
async def _client(app: FastAPI) -> AsyncIterator[httpx.AsyncClient]:
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client,
    ):
        yield client


@pytest.fixture
async def api(
    settings: Settings, session_factory: async_sessionmaker[AsyncSession]
) -> AsyncIterator[httpx.AsyncClient]:
    """API sin broker (replay → 503)."""
    async with _client(create_app(settings, replayer=UnavailableReplayer())) as client:
        yield client


@pytest.fixture
async def admin_api(
    settings: Settings, session_factory: async_sessionmaker[AsyncSession], bus: RabbitMQBus
) -> AsyncIterator[httpx.AsyncClient]:
    """API con el bus real del test como replayer y la API key admin ya puesta."""
    async with _client(create_app(settings, replayer=bus)) as client:
        client.headers["X-API-Key"] = ADMIN_KEY
        yield client
