from __future__ import annotations

import asyncio

from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine


async def _current_revision(url: str) -> str | None:
    engine = create_async_engine(url)
    try:
        async with engine.connect() as conn:
            exists = await conn.scalar(text("SELECT to_regclass('alembic_version') IS NOT NULL"))
            if not exists:
                return None
            version: str | None = await conn.scalar(text("SELECT version_num FROM alembic_version"))
            return version
    finally:
        await engine.dispose()


async def test_upgrade_head_and_downgrade_base(alembic_config: Config, postgres_url: str) -> None:
    head = ScriptDirectory.from_config(alembic_config).get_current_head()

    # env.py usa asyncio.run → se ejecuta en otro hilo para no chocar con el loop del test.
    await asyncio.to_thread(command.upgrade, alembic_config, "head")
    assert await _current_revision(postgres_url) == head

    await asyncio.to_thread(command.downgrade, alembic_config, "base")
    assert await _current_revision(postgres_url) is None

    # Deja la base en head: el resto de tests de la sesión comparten este Postgres.
    await asyncio.to_thread(command.upgrade, alembic_config, "head")
    assert await _current_revision(postgres_url) == head
