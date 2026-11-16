from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime

import httpx
import pytest
from fastapi import Depends, FastAPI
from pydantic import SecretStr

from notifications.api.security import API_KEY_HEADER, require_admin_key
from notifications.contracts import Channel, Envelope, NotificationRequestedV1
from notifications.dlq.archiver import build_dead_letter
from notifications.dlq.parsing import (
    extract_attempts,
    extract_dead_at,
    extract_reason,
    json_safe,
    replay_headers,
)
from notifications.messaging.bus import RawMessage

T0 = datetime(2026, 11, 16, 12, 0, tzinfo=UTC)

WORKER_HEADERS = {
    "x-attempt": 3,
    "x-dead-reason": "exhausted",
    "x-error-class": "TransientError",
    "x-error-message": "timeout",
    "x-traceback": "Traceback ...",
    "x-failed-at": T0.isoformat(),
    "x-first-failed-at": T0.isoformat(),
    "x-original-routing-key": "email.main",
    "custom": "se-conserva",
}
BROKER_HEADERS = {
    "x-attempt": 1,
    "x-death": [{"reason": "delivery_limit", "count": 1, "time": T0, "queue": "email.main"}],
    "x-first-death-reason": "delivery_limit",
}


def test_json_safe_converts_amqp_types() -> None:
    value = {"t": T0, "b": b"\xff\x00ok", "l": [{"n": 1}], "x": object()}

    safe = json_safe(value)

    json.dumps(safe)  # no lanza
    assert safe["t"] == T0.isoformat()
    assert safe["l"] == [{"n": 1}]


@pytest.mark.parametrize(
    ("headers", "expected"),
    [(WORKER_HEADERS, "exhausted"), (BROKER_HEADERS, "broker:delivery_limit"), ({}, "unknown")],
)
def test_extract_reason(headers: dict[str, object], expected: str) -> None:
    assert extract_reason(headers) == expected


def test_extract_attempts_and_dead_at() -> None:
    assert extract_attempts(WORKER_HEADERS) == 3
    assert extract_attempts({"x-death": [{"count": 2}]}) == 2
    assert extract_attempts({"x-attempt": "basura"}) == 0
    assert extract_dead_at(WORKER_HEADERS) == T0
    assert extract_dead_at(BROKER_HEADERS) == T0
    assert extract_dead_at({"x-failed-at": "no-es-fecha"}) is None


def test_replay_headers_reset_attempt_and_strip_failure_info() -> None:
    headers = replay_headers({**WORKER_HEADERS, **BROKER_HEADERS}, dead_letter_id=7, replay_count=2)

    assert headers == {
        "x-attempt": 0,
        "x-replay-count": 2,
        "x-replayed-from-dead-letter": 7,
        "custom": "se-conserva",
    }


def test_build_dead_letter_from_valid_envelope() -> None:
    nid = uuid.uuid4()
    payload = NotificationRequestedV1(
        notification_id=nid, channel=Channel.EMAIL, recipient="a@b.co", template="t"
    )
    envelope = Envelope(type="notification.requested", payload=payload.model_dump(mode="json"))
    raw = RawMessage(
        channel=Channel.EMAIL,
        body=envelope.to_bytes(),
        headers=WORKER_HEADERS,
        message_id=str(envelope.id),
    )

    row = build_dead_letter(raw)

    assert row["notification_id"] == nid
    assert row["correlation_id"] == envelope.correlation_id
    assert row["reason"] == "exhausted"
    assert row["attempts"] == 3
    assert row["dead_at"] == T0
    assert row["error_class"] == "TransientError"
    assert row["envelope"]["id"] == str(envelope.id)


def test_build_dead_letter_from_garbage() -> None:
    raw = RawMessage(channel=Channel.SMS, body=b"\x00not json", headers={})

    row = build_dead_letter(raw, now=T0)

    assert row["envelope"] is None
    assert row["notification_id"] is None
    assert row["reason"] == "unknown"
    assert row["dead_at"] == T0
    assert row["body"] == b"\x00not json"


# ------------------------------------------------------------------------------ API key


@pytest.fixture
def secured_app() -> FastAPI:
    app = FastAPI()

    class _Api:
        admin_key = SecretStr("k" * 20)

    class _Settings:
        api = _Api()

    app.state.settings = _Settings()

    @app.get("/secret", dependencies=[Depends(require_admin_key)])
    async def secret() -> dict[str, bool]:
        return {"ok": True}

    return app


@pytest.mark.parametrize(
    ("headers", "expected"),
    [({}, 401), ({API_KEY_HEADER: "mala"}, 401), ({API_KEY_HEADER: "k" * 20}, 200)],
)
async def test_admin_key(secured_app: FastAPI, headers: dict[str, str], expected: int) -> None:
    transport = httpx.ASGITransport(app=secured_app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as client:
        resp = await client.get("/secret", headers=headers)

    assert resp.status_code == expected
