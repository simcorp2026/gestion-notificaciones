"""Funciones puras para interpretar mensajes muertos y preparar su replay."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any, Final

from notifications.messaging.headers import (
    H_ATTEMPT,
    H_DEAD_REASON,
    H_ERROR_CLASS,
    H_ERROR_MESSAGE,
    H_FAILED_AT,
    H_FIRST_FAILED_AT,
    H_ORIGINAL_ROUTING_KEY,
    H_TRACEBACK,
)

H_REPLAY_COUNT: Final = "x-replay-count"
H_REPLAYED_FROM: Final = "x-replayed-from-dead-letter"

# Cabeceras de fallo que no deben viajar con un replay (el mensaje vuelve "limpio").
_FAILURE_HEADERS: Final = frozenset(
    {
        H_DEAD_REASON,
        H_ERROR_CLASS,
        H_ERROR_MESSAGE,
        H_FAILED_AT,
        H_FIRST_FAILED_AT,
        H_ORIGINAL_ROUTING_KEY,
        H_TRACEBACK,
        "x-death",
        "x-first-death-exchange",
        "x-first-death-queue",
        "x-first-death-reason",
        "x-last-death-exchange",
        "x-last-death-queue",
        "x-last-death-reason",
        "x-delivery-count",
    }
)


def json_safe(value: Any) -> Any:
    """Convierte cabeceras AMQP (datetime, bytes, listas de tablas) a JSON."""
    if isinstance(value, Mapping):
        return {str(k): json_safe(v) for k, v in value.items()}
    if isinstance(value, list | tuple):
        return [json_safe(v) for v in value]
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, bytes | bytearray):
        return bytes(value).decode("utf-8", errors="replace")
    if value is None or isinstance(value, bool | int | float | str):
        return value
    return str(value)


def _first_death(headers: Mapping[str, Any]) -> Mapping[str, Any] | None:
    deaths = headers.get("x-death")
    if isinstance(deaths, list | tuple) and deaths and isinstance(deaths[0], Mapping):
        return deaths[0]
    return None


def extract_reason(headers: Mapping[str, Any]) -> str:
    """``x-dead-reason`` del worker; si no, el motivo del DLX del broker (``broker:<razón>``)."""
    reason = headers.get(H_DEAD_REASON)
    if reason:
        return str(reason)
    death = _first_death(headers)
    if death is not None and death.get("reason"):
        return f"broker:{death['reason']}"
    return "unknown"


def extract_attempts(headers: Mapping[str, Any]) -> int:
    for raw in (headers.get(H_ATTEMPT), (_first_death(headers) or {}).get("count")):
        try:
            if raw is not None:
                return max(int(raw), 0)
        except (TypeError, ValueError):
            continue
    return 0


def extract_dead_at(headers: Mapping[str, Any]) -> datetime | None:
    raw = headers.get(H_FAILED_AT)
    if isinstance(raw, str):
        try:
            parsed = datetime.fromisoformat(raw)
        except ValueError:
            parsed = None
        if parsed is not None:
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
    death = _first_death(headers)
    if death is not None and isinstance(death.get("time"), datetime):
        t: datetime = death["time"]
        return t if t.tzinfo else t.replace(tzinfo=UTC)
    return None


def replay_headers(
    headers: Mapping[str, Any], *, dead_letter_id: int, replay_count: int
) -> dict[str, Any]:
    """Cabeceras para devolver el mensaje a ``main``: sin rastro de fallos y ``x-attempt=0``."""
    clean = {k: v for k, v in headers.items() if k not in _FAILURE_HEADERS}
    clean[H_ATTEMPT] = 0
    clean[H_REPLAY_COUNT] = replay_count
    clean[H_REPLAYED_FROM] = dead_letter_id
    return clean
