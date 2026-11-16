"""Envelope estándar de eventos y canales de entrega."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, Self

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field


class Channel(StrEnum):
    EMAIL = "email"
    SMS = "sms"
    PUSH = "push"


class Envelope(BaseModel):
    """``{ id, type, version, occurred_at, correlation_id, payload }``.

    ``id`` es el identificador de idempotencia del mensaje: se mantiene en reintentos y replays.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    id: uuid.UUID = Field(default_factory=uuid.uuid4)
    type: str = Field(min_length=1)
    version: int = Field(default=1, ge=1)
    occurred_at: AwareDatetime = Field(default_factory=lambda: datetime.now(UTC))
    correlation_id: str = Field(default_factory=lambda: str(uuid.uuid4()), min_length=1)
    payload: dict[str, Any]

    def to_bytes(self) -> bytes:
        return self.model_dump_json().encode()

    @classmethod
    def from_bytes(cls, raw: bytes) -> Self:
        return cls.model_validate_json(raw)
