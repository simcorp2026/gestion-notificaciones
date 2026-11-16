from __future__ import annotations

import json
from typing import Any

import httpx
import pytest
from prometheus_client import REGISTRY

from notifications.api.app import create_app
from notifications.config import Settings
from notifications.monitor import QueueMonitor, credentials_from_amqp_url


def _sample(name: str, **labels: str) -> float | None:
    return REGISTRY.get_sample_value(name, labels)


class _BrokenSessionFactory:
    """La DB no responde: el monitor debe seguir actualizando lo que sí puede."""

    def __call__(self) -> Any:
        raise OSError("db caída (test)")


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        ("amqp://u:p@rabbit:5672/", ("u", "p", "/")),
        ("amqp://u:p@rabbit:5672/prod", ("u", "p", "prod")),
        ("amqp://rabbit:5672", ("guest", "guest", "/")),
    ],
)
def test_credentials_from_amqp_url(url: str, expected: tuple[str, str, str]) -> None:
    assert credentials_from_amqp_url(url) == expected


async def test_monitor_sets_queue_depth_from_management_api() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        body = [
            {"name": "unit.email.main", "messages": 7},
            {"name": "unit.email.retry.5s", "messages": 2},
            {"name": "unit.sms.main"},  # cola recién creada: sin stats todavía
        ]
        return httpx.Response(200, content=json.dumps(body))

    http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    monitor = QueueMonitor(
        "http://rabbit:15672/",
        "amqp://u:p@rabbit:5672/",
        _BrokenSessionFactory(),  # type: ignore[arg-type]
        http=http,
    )

    await monitor.poll_once()

    assert str(seen[0].url).startswith("http://rabbit:15672/api/queues/%2F")
    assert _sample("queue_depth", queue="unit.email.main") == 7
    assert _sample("queue_depth", queue="unit.email.retry.5s") == 2
    assert _sample("queue_depth", queue="unit.sms.main") == 0


async def test_monitor_survives_management_api_errors() -> None:
    http = httpx.AsyncClient(transport=httpx.MockTransport(lambda _: httpx.Response(503)))
    monitor = QueueMonitor(
        "http://rabbit:15672",
        "amqp://u:p@rabbit/",
        _BrokenSessionFactory(),  # type: ignore[arg-type]
        http=http,
    )

    await monitor.poll_once()  # no lanza


async def test_api_exposes_metrics(valid_env: dict[str, str]) -> None:
    app = create_app(Settings())
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as client:
        resp = await client.get("/metrics/")

    assert resp.status_code == 200
    for name in (
        "notifications_accepted_total",
        "notifications_published_total",
        "notifications_processed_total",
        "notification_processing_seconds",
        "queue_depth",
        "dlq_depth",
    ):
        assert name in resp.text
