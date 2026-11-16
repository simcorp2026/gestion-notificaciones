"""API sin base de datos: validación, hash de idempotencia y middleware de correlación."""

from __future__ import annotations

import httpx
import pytest
from fastapi import FastAPI
from pydantic import ValidationError

from notifications.api.middleware import CORRELATION_HEADER, CorrelationIdMiddleware
from notifications.api.schemas import CreateNotificationRequest
from notifications.contracts import Channel
from notifications.db.repositories import NewNotification
from notifications.observability import get_correlation_id


def _new(**overrides: object) -> NewNotification:
    base: dict[str, object] = {
        "channel": Channel.EMAIL,
        "recipient": "a@b.c",
        "template": "welcome",
        "data": {"x": 1, "y": [1, 2]},
    }
    return NewNotification(**{**base, **overrides})  # type: ignore[arg-type]


def test_request_hash_ignores_key_order_and_idempotency_key() -> None:
    a = _new(data={"x": 1, "y": [1, 2]}, idempotency_key="k1")
    b = _new(data={"y": [1, 2], "x": 1}, idempotency_key="k2")

    assert a.request_hash() == b.request_hash()


@pytest.mark.parametrize(
    "change",
    [{"recipient": "z@b.c"}, {"template": "other"}, {"data": {"x": 2}}, {"channel": Channel.SMS}],
)
def test_request_hash_changes_with_content(change: dict[str, object]) -> None:
    assert _new().request_hash() != _new(**change).request_hash()


def test_request_strips_whitespace_and_defaults_payload() -> None:
    req = CreateNotificationRequest.model_validate(
        {"channel": "sms", "recipient": "  +34600000000 ", "template": "otp"}
    )

    assert req.recipient == "+34600000000"
    assert req.payload == {}
    assert req.idempotency_key is None


@pytest.mark.parametrize(
    "body",
    [
        {"channel": "fax", "recipient": "x", "template": "t"},
        {"channel": "sms", "recipient": "", "template": "t"},
        {"channel": "sms", "recipient": "x", "template": "UPPER"},
        {"channel": "sms", "recipient": "x", "template": "t", "unknown": 1},
        {"channel": "sms", "recipient": "x", "template": "t", "idempotency_key": ""},
    ],
)
def test_request_validation(body: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        CreateNotificationRequest.model_validate(body)


@pytest.fixture
def echo_app() -> FastAPI:
    app = FastAPI()
    app.add_middleware(CorrelationIdMiddleware)

    @app.get("/echo")
    async def echo() -> dict[str, str | None]:
        return {"cid": get_correlation_id()}

    return app


async def _get(app: FastAPI, headers: dict[str, str] | None = None) -> httpx.Response:
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as client:
        return await client.get("/echo", headers=headers)


async def test_correlation_header_propagates_to_context(echo_app: FastAPI) -> None:
    resp = await _get(echo_app, {CORRELATION_HEADER: "abc-1"})

    assert resp.json() == {"cid": "abc-1"}
    assert resp.headers[CORRELATION_HEADER] == "abc-1"


@pytest.mark.parametrize("bad", ["", "con espacios", "x" * 65, "<script>"])
async def test_invalid_correlation_header_is_replaced(echo_app: FastAPI, bad: str) -> None:
    resp = await _get(echo_app, {CORRELATION_HEADER: bad})

    cid = resp.json()["cid"]
    assert cid
    assert cid != bad
    assert resp.headers[CORRELATION_HEADER] == cid


async def test_correlation_generated_when_absent(echo_app: FastAPI) -> None:
    resp = await _get(echo_app)

    assert resp.json()["cid"] == resp.headers[CORRELATION_HEADER]
