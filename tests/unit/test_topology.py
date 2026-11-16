from __future__ import annotations

import pytest

from notifications.contracts import Channel
from notifications.messaging.topology import Topology


def test_default_names_match_spec() -> None:
    t = Topology()

    assert t.exchange == "notifications"
    assert t.dlx == "notifications.dlx"
    assert t.routing_key(Channel.EMAIL) == "notification.email"
    assert t.main_queue(Channel.EMAIL) == "email.main"
    assert t.retry_queues(Channel.EMAIL) == [
        "email.retry.5s",
        "email.retry.30s",
        "email.retry.300s",
    ]
    assert t.dlq(Channel.SMS) == "sms.dlq"


def test_namespace_isolates_every_name() -> None:
    t = Topology(namespace="t1", retry_delays=(0.2,))

    names = [t.exchange, t.dlx, t.main_queue(Channel.PUSH), t.dlq(Channel.PUSH)]
    names += t.retry_queues(Channel.PUSH)

    assert all(n.startswith("t1.") for n in names)
    assert t.retry_queues(Channel.PUSH) == ["t1.push.retry.200ms"]
    # La routing key es un contrato del evento, no se prefija.
    assert t.routing_key(Channel.PUSH) == "notification.push"


def test_main_queue_is_quorum_with_safe_dead_lettering() -> None:
    t = Topology(delivery_limit=7)
    args = t.main_queue_arguments(Channel.EMAIL)

    assert args == {
        "x-queue-type": "quorum",
        "x-dead-letter-exchange": "notifications.dlx",
        "x-dead-letter-routing-key": "email.dlq",
        "x-dead-letter-strategy": "at-least-once",
        "x-overflow": "reject-publish",
        "x-delivery-limit": 7,
    }


def test_retry_queue_ttl_routes_back_to_main() -> None:
    t = Topology(retry_delays=(0.5, 30))

    assert t.retry_queue_arguments(Channel.SMS, 0) == {
        "x-queue-type": "classic",
        "x-message-ttl": 500,
        "x-dead-letter-exchange": "notifications.dlx",
        "x-dead-letter-routing-key": "sms.main",
    }
    assert t.retry_queue_arguments(Channel.SMS, 1)["x-message-ttl"] == 30_000


def test_empty_delays_rejected() -> None:
    with pytest.raises(ValueError, match="retry_delays"):
        Topology(retry_delays=())
