"""Entrypoint: ``notifications-api`` / ``python -m notifications.api``."""

from __future__ import annotations

import uvicorn

from notifications.api.app import create_app
from notifications.config import load_settings_or_exit
from notifications.observability import configure_logging


def main() -> None:
    settings = load_settings_or_exit()
    configure_logging(settings.log_level, json=settings.log_json)
    uvicorn.run(
        create_app(settings),
        host=settings.api.host,
        port=settings.api.port,
        log_config=None,  # uvicorn usa el logging ya configurado (structlog JSON)
        proxy_headers=True,
        timeout_graceful_shutdown=20,
    )


if __name__ == "__main__":
    main()
