"""Política de reintentos, independiente del broker (la reutiliza cualquier adaptador)."""

from __future__ import annotations

import traceback
from dataclasses import dataclass
from enum import StrEnum

from notifications.errors import is_permanent

TRACEBACK_MAX_CHARS = 2000


class Outcome(StrEnum):
    RETRY = "retry"
    DEAD = "dead"


class DeadReason(StrEnum):
    PERMANENT = "permanent"  # PermanentError: no se reintenta
    EXHAUSTED = "exhausted"  # reintentos agotados
    INVALID = "invalid"  # el mensaje no es un Envelope válido


@dataclass(frozen=True, slots=True)
class FailureDecision:
    outcome: Outcome
    next_attempt: int
    retry_level: int | None = None  # índice en ``delays`` cuando outcome == RETRY
    reason: DeadReason | None = None  # cuando outcome == DEAD


def decide(exc: BaseException, attempt: int, delays: tuple[float, ...]) -> FailureDecision:
    """Decide qué hacer con un mensaje fallido.

    ``attempt`` = reintentos ya realizados (cabecera ``x-attempt``; 0 en la primera entrega).
    El reintento ``n`` (0-based) usa ``delays[n]``; con ``len(delays)`` reintentos agotados → DLQ.
    """
    if attempt < 0:
        msg = f"attempt debe ser >= 0, recibido {attempt}"
        raise ValueError(msg)
    if is_permanent(exc):
        return FailureDecision(Outcome.DEAD, next_attempt=attempt, reason=DeadReason.PERMANENT)
    if attempt >= len(delays):
        return FailureDecision(Outcome.DEAD, next_attempt=attempt, reason=DeadReason.EXHAUSTED)
    return FailureDecision(Outcome.RETRY, next_attempt=attempt + 1, retry_level=attempt)


def format_delay(seconds: float) -> str:
    """Sufijo legible y estable para nombres de cola: 5 → ``5s``, 0.25 → ``250ms``."""
    ms = round(seconds * 1000)
    if ms <= 0:
        msg = f"delay debe ser > 0, recibido {seconds}"
        raise ValueError(msg)
    return f"{ms // 1000}s" if ms % 1000 == 0 else f"{ms}ms"


def summarize_traceback(exc: BaseException, limit: int = TRACEBACK_MAX_CHARS) -> str:
    """Traceback recortado por el principio (lo relevante está al final)."""
    text = "".join(traceback.format_exception(exc)).strip()
    return text if len(text) <= limit else "…" + text[-(limit - 1) :]
