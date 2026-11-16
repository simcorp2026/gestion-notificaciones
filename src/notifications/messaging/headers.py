"""Nombres de cabeceras de mensaje (contrato entre worker, DLQ y replay; neutral al broker)."""

from __future__ import annotations

from typing import Final

H_ATTEMPT: Final = "x-attempt"
H_ERROR_CLASS: Final = "x-error-class"
H_ERROR_MESSAGE: Final = "x-error-message"
H_TRACEBACK: Final = "x-traceback"
H_DEAD_REASON: Final = "x-dead-reason"
H_FAILED_AT: Final = "x-failed-at"
H_FIRST_FAILED_AT: Final = "x-first-failed-at"
H_ORIGINAL_ROUTING_KEY: Final = "x-original-routing-key"
