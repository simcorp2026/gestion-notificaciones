from __future__ import annotations

from datetime import timedelta

import pytest

from notifications.relay.service import compute_backoff


@pytest.mark.parametrize(
    ("attempts", "expected"),
    [(0, 1), (1, 1), (2, 2), (3, 4), (6, 32), (7, 60), (100, 60), (10_000, 60)],
)
def test_exponential_with_cap(attempts: int, expected: float) -> None:
    assert compute_backoff(attempts, base=1, maximum=60) == timedelta(seconds=expected)


def test_monotonic_non_decreasing() -> None:
    values = [compute_backoff(a, base=0.5, maximum=30) for a in range(1, 50)]
    assert values == sorted(values)
