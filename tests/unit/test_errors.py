from __future__ import annotations

import pytest

from notifications.errors import PermanentError, TransientError, is_permanent


@pytest.mark.parametrize(
    ("exc", "expected"),
    [
        (PermanentError("destinatario inválido"), True),
        (TransientError("timeout"), False),
        (RuntimeError("no clasificada"), False),
        (TimeoutError(), False),
    ],
)
def test_classification(exc: Exception, expected: bool) -> None:
    assert is_permanent(exc) is expected
