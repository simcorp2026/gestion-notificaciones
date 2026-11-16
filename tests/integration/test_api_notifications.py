from __future__ import annotations

import uuid
from typing import Any

import httpx
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from notifications.contracts import NOTIFICATION_REQUESTED, Envelope, NotificationRequestedV1
from notifications.db.models import Notification, OutboxEvent

BODY: dict[str, Any] = {
    "channel": "email",
    "recipient": "ada@example.com",
    "template": "welcome",
    "payload": {"name": "Ada"},
}


async def _count(sf: async_sessionmaker[AsyncSession], model: type[Any]) -> int:
    async with sf() as s:
        return int(await s.scalar(select(func.count()).select_from(model)) or 0)


async def test_create_persists_notification_and_outbox_atomically(
    api: httpx.AsyncClient, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    resp = await api.post("/notifications", json=BODY, headers={"X-Correlation-ID": "req-123"})

    assert resp.status_code == 202
    data = resp.json()
    nid = uuid.UUID(data["notification_id"])
    assert data["status"] == "pending"
    assert resp.headers["Location"] == f"/notifications/{nid}"
    assert resp.headers["X-Correlation-ID"] == "req-123"

    async with session_factory() as s:
        notification = await s.get_one(Notification, nid)
        event = (await s.scalars(select(OutboxEvent))).one()

    assert notification.correlation_id == "req-123"
    assert event.notification_id == nid
    assert event.published_at is None
    envelope = Envelope.model_validate(event.envelope)
    assert envelope.id == event.event_id
    assert envelope.type == NOTIFICATION_REQUESTED
    assert envelope.correlation_id == "req-123"
    payload = NotificationRequestedV1.model_validate(envelope.payload)
    assert payload.notification_id == nid
    assert payload.recipient == "ada@example.com"
    assert payload.data == {"name": "Ada"}


async def test_correlation_id_generated_when_missing(api: httpx.AsyncClient) -> None:
    resp = await api.post("/notifications", json=BODY)

    assert resp.status_code == 202
    assert resp.headers["X-Correlation-ID"]


async def test_idempotency_key_returns_same_notification(
    api: httpx.AsyncClient, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    body = {**BODY, "idempotency_key": "signup-42"}

    first = await api.post("/notifications", json=body)
    second = await api.post("/notifications", json=body)

    assert first.status_code == second.status_code == 202
    assert first.json()["notification_id"] == second.json()["notification_id"]
    assert "Idempotent-Replayed" not in first.headers
    assert second.headers["Idempotent-Replayed"] == "true"
    assert await _count(session_factory, Notification) == 1
    assert await _count(session_factory, OutboxEvent) == 1


async def test_idempotency_key_with_different_body_conflicts(
    api: httpx.AsyncClient, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    await api.post("/notifications", json={**BODY, "idempotency_key": "k1"})

    resp = await api.post(
        "/notifications", json={**BODY, "recipient": "otro@example.com", "idempotency_key": "k1"}
    )

    assert resp.status_code == 409
    assert await _count(session_factory, OutboxEvent) == 1


async def test_without_idempotency_key_each_request_is_new(
    api: httpx.AsyncClient, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    await api.post("/notifications", json=BODY)
    await api.post("/notifications", json=BODY)

    assert await _count(session_factory, Notification) == 2


async def test_validation_errors(api: httpx.AsyncClient) -> None:
    bad = [
        {**BODY, "channel": "fax"},
        {**BODY, "recipient": ""},
        {**BODY, "template": "Has Spaces"},
        {**BODY, "extra": 1},
        {k: v for k, v in BODY.items() if k != "channel"},
    ]
    for body in bad:
        resp = await api.post("/notifications", json=body)
        assert resp.status_code == 422, body


async def test_get_notification(api: httpx.AsyncClient) -> None:
    created = (await api.post("/notifications", json=BODY)).json()

    resp = await api.get(f"/notifications/{created['notification_id']}")

    assert resp.status_code == 200
    view = resp.json()
    assert view["status"] == "pending"
    assert view["channel"] == "email"
    assert view["recipient"] == "ada@example.com"


async def test_get_unknown_notification_is_404(api: httpx.AsyncClient) -> None:
    resp = await api.get(f"/notifications/{uuid.uuid4()}")

    assert resp.status_code == 404


async def test_health(api: httpx.AsyncClient) -> None:
    assert (await api.get("/health/live")).status_code == 200
    ready = await api.get("/health/ready")
    assert ready.status_code == 200
    assert ready.json()["database"] == "up"
