"""Eventos de notificación (versionados) y estados."""

from __future__ import annotations

import uuid
from enum import StrEnum
from typing import Any, Final

from pydantic import BaseModel, ConfigDict, Field

from notifications.contracts.envelope import Channel

NOTIFICATION_REQUESTED: Final = "notification.requested"


class NotificationStatus(StrEnum):
    PENDING = "pending"  # en outbox, aún no publicada
    PUBLISHED = "published"  # confirmada por el broker
    PROCESSING = "processing"  # un worker la está procesando
    SENT = "sent"  # entregada
    RETRYING = "retrying"  # fallo transitorio, esperando reintento
    DEAD = "dead"  # en DLQ


class NotificationRequestedV1(BaseModel):
    """``payload`` del envelope ``notification.requested`` v1."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    notification_id: uuid.UUID
    channel: Channel
    recipient: str = Field(min_length=1, max_length=320)
    template: str = Field(min_length=1, max_length=100)
    data: dict[str, Any] = Field(default_factory=dict)
