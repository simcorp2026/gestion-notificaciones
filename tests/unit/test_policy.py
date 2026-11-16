from __future__ import annotations

import pytest

from notifications.errors import PermanentError, TransientError
from notifications.messaging.policy import (
    DeadReason,
    Outcome,
    decide,
    format_delay,
    summarize_traceback,
)

DELAYS = (5.0, 30.0, 300.0)


@pytest.mark.parametrize(("attempt", "level"), [(0, 0), (1, 1), (2, 2)])
def test_transient_escalates_through_retry_levels(attempt: int, level: int) -> None:
    d = decide(TransientError("timeout"), attempt, DELAYS)

    assert d.outcome is Outcome.RETRY
    assert d.retry_level == level
    assert d.next_attempt == attempt + 1
    assert d.reason is None


def test_unclassified_exception_is_retried() -> None:
    assert decide(RuntimeError("boom"), 0, DELAYS).outcome is Outcome.RETRY


@pytest.mark.parametrize("attempt", [3, 4, 100])
def test_exhausted_goes_to_dlq(attempt: int) -> None:
    d = decide(TransientError("timeout"), attempt, DELAYS)

    assert d.outcome is Outcome.DEAD
    assert d.reason is DeadReason.EXHAUSTED
    assert d.next_attempt == attempt


@pytest.mark.parametrize("attempt", [0, 1, 3])
def test_permanent_goes_to_dlq_without_retry(attempt: int) -> None:
    d = decide(PermanentError("destinatario inválido"), attempt, DELAYS)

    assert d.outcome is Outcome.DEAD
    assert d.reason is DeadReason.PERMANENT


def test_total_handler_calls_equals_retries_plus_one() -> None:
    attempt, calls = 0, 0
    while True:
        calls += 1
        d = decide(TransientError(), attempt, DELAYS)
        if d.outcome is Outcome.DEAD:
            break
        attempt = d.next_attempt
    assert calls == len(DELAYS) + 1


def test_negative_attempt_rejected() -> None:
    with pytest.raises(ValueError, match="attempt"):
        decide(TransientError(), -1, DELAYS)


@pytest.mark.parametrize(
    ("seconds", "expected"),
    [(5, "5s"), (30.0, "30s"), (300, "300s"), (0.25, "250ms"), (1.5, "1500ms"), (0.001, "1ms")],
)
def test_format_delay(seconds: float, expected: str) -> None:
    assert format_delay(seconds) == expected


@pytest.mark.parametrize("seconds", [0, 0.0004, -1])
def test_format_delay_rejects_non_positive(seconds: float) -> None:
    with pytest.raises(ValueError, match="delay"):
        format_delay(seconds)


def test_summarize_traceback_keeps_tail() -> None:
    def deep(n: int) -> None:
        if n == 0:
            raise TransientError("fin-de-la-pila")
        deep(n - 1)

    try:
        deep(50)
    except TransientError as exc:
        summary = summarize_traceback(exc, limit=300)
    else:  # pragma: no cover
        pytest.fail("no lanzó")

    assert len(summary) == 300
    assert summary.startswith("…")
    assert summary.endswith("TransientError: fin-de-la-pila")
