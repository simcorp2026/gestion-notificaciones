"""Entorno de Alembic async.

La URL se resuelve en este orden:
1. ``sqlalchemy.url`` en la config de Alembic (lo usan los tests con testcontainers).
2. ``DB_URL`` vía ``DatabaseSettings`` (falla rápido si no está definida).
"""

from __future__ import annotations

import asyncio

from alembic import context
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import create_async_engine

import notifications.db.models  # noqa: F401  - registra los modelos en Base.metadata
from notifications.config import DatabaseSettings
from notifications.db.base import Base
from notifications.observability import configure_logging

config = context.config
target_metadata = Base.metadata

if config.attributes.get("configure_logging", True):
    configure_logging("INFO", json=False)


def _database_url() -> str:
    url = config.get_main_option("sqlalchemy.url")
    if url:
        return url
    return str(DatabaseSettings().url)


def run_migrations_offline() -> None:
    context.configure(
        url=_database_url(),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
    )
    with context.begin_transaction():
        context.run_migrations()


def _do_run_migrations(connection: Connection) -> None:
    context.configure(connection=connection, target_metadata=target_metadata, compare_type=True)
    with context.begin_transaction():
        context.run_migrations()


async def run_migrations_online() -> None:
    engine = create_async_engine(_database_url())
    try:
        async with engine.connect() as connection:
            await connection.run_sync(_do_run_migrations)
    finally:
        await engine.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    asyncio.run(run_migrations_online())
