from __future__ import annotations

import random
import time
import uuid

import pytest
from pydantic import ValidationError

from notifications.config import WorkerSettings
from notifications.contracts import Channel, Envelope, NotificationRequestedV1
from notifications.errors import PermanentError, TransientError
from notifications.worker.senders import SimulatedSender, validate_recipient
from notifications.worker.service import parse_event


def _event(channel: Channel = Channel.EMAIL, recipient: str = "a@b.co") -> NotificationRequestedV1:
    return NotificationRequestedV1(
        notification_id=uuid.uuid4(), channel=channel, recipient=recipient, template="t"
    )


# ------------------------------------------------------------------- validación destinatario


@pytest.mark.parametrize(
    ("channel", "recipient"),
    [
        (Channel.EMAIL, "ada@example.com"),
        (Channel.SMS, "+34600111222"),
        (Channel.PUSH, "fcm:abcdefghijklmnop_1234"),
    ],
)
def test_valid_recipients(channel: Channel, recipient: str) -> None:
    validate_recipient(channel, recipient)


@pytest.mark.parametrize(
    ("channel", "recipient"),
    [
        (Channel.EMAIL, "sin-arroba"),
        (Channel.EMAIL, "a@b"),
        (Channel.SMS, "600111222"),
        (Channel.SMS, "+0123456789"),
        (Channel.PUSH, "corto"),
    ],
)
def test_invalid_recipients_are_permanent(channel: Channel, recipient: str) -> None:
    with pytest.raises(PermanentError):
        validate_recipient(channel, recipient)


# ------------------------------------------------------------------------- SimulatedSender


async def test_simulated_sender_success_without_failures() -> None:
    await SimulatedSender(Channel.EMAIL, latency_ms=(0, 0)).send(_event())


async def test_simulated_sender_transient_rate() -> None:
    sender = SimulatedSender(Channel.EMAIL, latency_ms=(0, 0), transient_failure_rate=1.0)

    with pytest.raises(TransientError):
        await sender.send(_event())


async def test_simulated_sender_permanent_rate() -> None:
    sender = SimulatedSender(Channel.EMAIL, latency_ms=(0, 0), permanent_failure_rate=1.0)

    with pytest.raises(PermanentError):
        await sender.send(_event())


async def test_simulated_sender_rejects_other_channel() -> None:
    sender = SimulatedSender(Channel.SMS, latency_ms=(0, 0))

    with pytest.raises(PermanentError, match="canal"):
        await sender.send(_event(Channel.EMAIL))


async def test_simulated_sender_failure_rate_is_roughly_respected() -> None:
    sender = SimulatedSender(
        Channel.EMAIL, latency_ms=(0, 0), transient_failure_rate=0.3, rng=random.Random(42)
    )
    failures = 0
    for _ in range(1000):
        try:
            await sender.send(_event())
        except TransientError:
            failures += 1

    assert 250 < failures < 350


async def test_simulated_latency_is_applied() -> None:
    sender = SimulatedSender(Channel.EMAIL, latency_ms=(30, 30))

    started = time.perf_counter()
    await sender.send(_event())

    assert time.perf_counter() - started >= 0.025


# -------------------------------------------------------------------------- parse_event


def test_parse_valid_event() -> None:
    event = _event()
    envelope = Envelope(type="notification.requested", payload=event.model_dump(mode="json"))

    assert parse_event(envelope) == event


@pytest.mark.parametrize(
    "envelope",
    [
        Envelope(type="notification.cancelled", payload={}),
        Envelope(type="notification.requested", version=2, payload={}),
        Envelope(type="notification.requested", payload={"channel": "email"}),
    ],
)
def test_parse_invalid_event_is_permanent(envelope: Envelope) -> None:
    with pytest.raises(PermanentError):
        parse_event(envelope)


# ---------------------------------------------------------------------------- settings


def test_worker_settings_validation() -> None:
    with pytest.raises(ValidationError):
        WorkerSettings(sim_latency_min_ms=500, sim_latency_max_ms=100)
    with pytest.raises(ValidationError):
        WorkerSettings(sim_transient_failure_rate=0.7, sim_permanent_failure_rate=0.5)
    with pytest.raises(ValidationError):
        WorkerSettings(sim_transient_failure_rate=1.5)
