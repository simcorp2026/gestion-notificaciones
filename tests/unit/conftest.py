from __future__ import annotations

import os
from pathlib import Path

import pytest

_PREFIXES = ("APP_", "DB_", "RABBITMQ_", "RETRY_", "API_")

VALID_ENV = {
    "DB_URL": "postgresql+asyncpg://u:p@localhost:5432/db",
    "RABBITMQ_URL": "amqp://u:p@localhost:5672/",
    "RABBITMQ_MANAGEMENT_URL": "http://localhost:15672/",
    "API_ADMIN_KEY": "x" * 16,
}


@pytest.fixture(autouse=True)
def isolated_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Aísla los tests del .env del repo y de variables del host."""
    monkeypatch.chdir(tmp_path)
    for key in list(os.environ):
        if key.startswith(_PREFIXES):
            monkeypatch.delenv(key)


@pytest.fixture
def valid_env(monkeypatch: pytest.MonkeyPatch) -> dict[str, str]:
    for k, v in VALID_ENV.items():
        monkeypatch.setenv(k, v)
    return dict(VALID_ENV)
