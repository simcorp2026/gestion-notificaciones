"""Modelos HTTP (separados de los contratos de eventos)."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from notifications.contracts import Channel, NotificationStatus
from notifications.db.models import AttemptOutcome, DeadLetterStatus

_MAX_PAYLOAD_KEYS = 100


class CreateNotificationRequest(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        str_strip_whitespace=True,
        json_schema_extra={
            "examples": [
                {
                    "channel": "email",
                    "recipient": "user@example.com",
                    "template": "welcome",
                    "payload": {"name": "Ada"},
                    "idempotency_key": "signup-42",
                }
            ]
        },
    )

    channel: Channel
    recipient: str = Field(min_length=1, max_length=320)
    template: str = Field(min_length=1, max_length=100, pattern=r"^[a-z0-9][a-z0-9_.-]*$")
    payload: dict[str, Any] = Field(default_factory=dict, max_length=_MAX_PAYLOAD_KEYS)
    idempotency_key: str | None = Field(default=None, min_length=1, max_length=200)


class CreateNotificationResponse(BaseModel):
    notification_id: uuid.UUID
    status: NotificationStatus


class AttemptView(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    attempt: int
    outcome: AttemptOutcome
    consumer: str
    error_class: str | None
    error_message: str | None
    started_at: datetime
    finished_at: datetime | None


class NotificationView(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    channel: Channel
    recipient: str
    template: str
    status: NotificationStatus
    correlation_id: str
    idempotency_key: str | None
    created_at: datetime
    updated_at: datetime
    attempts: list[AttemptView] = Field(default_factory=list)


# ------------------------------------------------------------------------------- admin


class DeadLetterSummary(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    channel: Channel
    message_id: str | None
    notification_id: uuid.UUID | None
    correlation_id: str | None
    reason: str
    error_class: str | None
    error_message: str | None
    attempts: int
    dead_at: datetime
    archived_at: datetime
    status: DeadLetterStatus
    replay_count: int
    replayed_at: datetime | None
    discarded_at: datetime | None


class DeadLetterDetail(DeadLetterSummary):
    traceback: str | None
    headers: dict[str, Any]
    envelope: dict[str, Any] | None
    body_preview: str


class DeadLetterPage(BaseModel):
    items: list[DeadLetterSummary]
    next_cursor: int | None = Field(
        description="Pasa este valor como `cursor` para la página siguiente"
    )


class ReplayAllResponse(BaseModel):
    replayed: int
    failed: int
