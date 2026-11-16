from __future__ import annotations

from datetime import datetime

import pytest
from pydantic import ValidationError

from notifications.contracts import Envelope


def test_roundtrip_bytes() -> None:
    env = Envelope(type="notification.requested", payload={"to": "a@b.c"})

    assert Envelope.from_bytes(env.to_bytes()) == env


def test_defaults_are_generated() -> None:
    env = Envelope(type="x", payload={})

    assert env.id
    assert env.correlation_id
    assert env.version == 1
    assert env.occurred_at.tzinfo is not None


def test_extra_fields_rejected() -> None:
    with pytest.raises(ValidationError):
        Envelope.from_bytes(b'{"type":"x","payload":{},"unexpected":1}')


def test_naive_datetime_rejected() -> None:
    with pytest.raises(ValidationError):
        Envelope(type="x", payload={}, occurred_at=datetime(2026, 1, 1))  # noqa: DTZ001


def test_garbage_rejected() -> None:
    with pytest.raises(ValidationError):
        Envelope.from_bytes(b"not json")
