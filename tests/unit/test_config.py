from __future__ import annotations

import pytest
from pydantic import ValidationError

from notifications.config import RetrySettings, Settings, get_settings, load_settings_or_exit


def test_loads_valid_env(valid_env: dict[str, str]) -> None:
    settings = Settings()

    assert str(settings.db.url) == valid_env["DB_URL"]
    assert settings.rabbitmq.prefetch == 10
    assert settings.retry.delays_seconds == [5, 30, 300]
    assert settings.retry.max_attempts == 4
    assert settings.api.admin_key.get_secret_value() == valid_env["API_ADMIN_KEY"]


def test_missing_required_var_fails_fast(
    valid_env: dict[str, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("DB_URL")

    with pytest.raises(ValidationError, match="url"):
        Settings()


def test_sync_driver_rejected(valid_env: dict[str, str], monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DB_URL", "postgresql://u:p@localhost/db")

    with pytest.raises(ValidationError, match="asyncpg"):
        Settings()


def test_short_admin_key_rejected(
    valid_env: dict[str, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("API_ADMIN_KEY", "short")

    with pytest.raises(ValidationError):
        Settings()


@pytest.mark.parametrize("raw", ["[]", "[30,5]", "[5,5]", "[0,5]"])
def test_invalid_retry_delays(raw: str, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("RETRY_DELAYS_SECONDS", raw)

    with pytest.raises(ValidationError):
        RetrySettings()


def test_retry_delays_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("RETRY_DELAYS_SECONDS", "[1,2]")

    retry = RetrySettings()

    assert retry.delays_seconds == [1, 2]
    assert retry.max_attempts == 3


def test_entrypoint_helper_exits_with_code_2(capsys: pytest.CaptureFixture[str]) -> None:
    get_settings.cache_clear()
    with pytest.raises(SystemExit) as exc_info:
        load_settings_or_exit()

    assert exc_info.value.code == 2
    assert "Configuración inválida" in capsys.readouterr().err
