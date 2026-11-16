"""Modelos ORM. Todos se importan aquí para que Alembic los vea en ``Base.metadata``."""

from __future__ import annotations

import uuid
from datetime import datetime
from enum import StrEnum
from typing import Any

from sqlalchemy import (
    BigInteger,
    DateTime,
    Enum,
    ForeignKey,
    Identity,
    Index,
    Integer,
    LargeBinary,
    String,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from notifications.contracts import Channel, NotificationStatus
from notifications.db.base import Base


def _str_enum(enum_cls: type[StrEnum], name: str) -> Enum:
    """VARCHAR + CHECK (no enum nativo de Postgres: añadir valores no requiere ALTER TYPE)."""
    return Enum(
        enum_cls,
        name=name,
        native_enum=False,
        create_constraint=True,
        length=16,
        values_callable=lambda e: [m.value for m in e],
        validate_strings=True,
    )


def _now() -> Any:
    return func.now()


class Notification(Base):
    __tablename__ = "notifications"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True)
    channel: Mapped[Channel] = mapped_column(_str_enum(Channel, "channel"))
    recipient: Mapped[str] = mapped_column(String(320))
    template: Mapped[str] = mapped_column(String(100))
    data: Mapped[dict[str, Any]] = mapped_column(JSONB)
    # NULL permitido y no único entre NULLs: la clave es opcional.
    idempotency_key: Mapped[str | None] = mapped_column(String(200), unique=True)
    request_hash: Mapped[str] = mapped_column(String(64))
    status: Mapped[NotificationStatus] = mapped_column(
        _str_enum(NotificationStatus, "status"), default=NotificationStatus.PENDING
    )
    correlation_id: Mapped[str] = mapped_column(String(64))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=_now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=_now(), onupdate=_now()
    )


class OutboxEvent(Base):
    __tablename__ = "outbox"

    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)
    event_id: Mapped[uuid.UUID] = mapped_column(unique=True)
    notification_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("notifications.id", ondelete="CASCADE"), index=True
    )
    channel: Mapped[Channel] = mapped_column(_str_enum(Channel, "channel"))
    event_type: Mapped[str] = mapped_column(String(100))
    envelope: Mapped[dict[str, Any]] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=_now())
    # El relay sólo toma filas con available_at <= now(): implementa el backoff por fila.
    available_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=_now())
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    publish_attempts: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    last_error: Mapped[str | None] = mapped_column(Text)

    __table_args__ = (
        # Índice parcial: sólo filas pendientes → se mantiene pequeño aunque la tabla crezca.
        Index(
            "ix_outbox_pending",
            "available_at",
            "id",
            postgresql_where=text("published_at IS NULL"),
        ),
    )


class ProcessedMessage(Base):
    """Idempotencia del consumidor: una fila por (mensaje, consumidor) procesado con éxito."""

    __tablename__ = "processed_messages"

    message_id: Mapped[uuid.UUID] = mapped_column(primary_key=True)
    consumer: Mapped[str] = mapped_column(String(64), primary_key=True)
    processed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=_now())


class AttemptOutcome(StrEnum):
    IN_PROGRESS = "in_progress"
    OK = "ok"
    RETRY = "retry"
    DEAD = "dead"
    DUPLICATE = "duplicate"


class DeliveryAttempt(Base):
    """Historial: una fila por entrega de un mensaje a un worker."""

    __tablename__ = "delivery_attempts"

    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)
    notification_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("notifications.id", ondelete="CASCADE"), index=True
    )
    message_id: Mapped[uuid.UUID]
    consumer: Mapped[str] = mapped_column(String(64))
    attempt: Mapped[int] = mapped_column(Integer)
    outcome: Mapped[AttemptOutcome] = mapped_column(
        _str_enum(AttemptOutcome, "outcome"), default=AttemptOutcome.IN_PROGRESS
    )
    error_class: Mapped[str | None] = mapped_column(String(200))
    error_message: Mapped[str | None] = mapped_column(Text)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=_now())
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class DeadLetterStatus(StrEnum):
    PENDING = "pending"
    REPLAYED = "replayed"
    DISCARDED = "discarded"


class DeadLetter(Base):
    """Copia persistente de cada mensaje muerto: fuente de la API admin (ADR 004).

    Sin FK a ``notifications``: un mensaje ilegible o huérfano también debe archivarse.
    """

    __tablename__ = "dead_letters"

    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)
    channel: Mapped[Channel] = mapped_column(_str_enum(Channel, "channel"))
    message_id: Mapped[str | None] = mapped_column(String(128))
    notification_id: Mapped[uuid.UUID | None] = mapped_column(index=True)
    correlation_id: Mapped[str | None] = mapped_column(String(64))
    reason: Mapped[str] = mapped_column(String(32))
    error_class: Mapped[str | None] = mapped_column(String(200))
    error_message: Mapped[str | None] = mapped_column(Text)
    traceback: Mapped[str | None] = mapped_column(Text)
    attempts: Mapped[int] = mapped_column(Integer)
    body: Mapped[bytes] = mapped_column(LargeBinary)
    content_type: Mapped[str | None] = mapped_column(String(100))
    message_type: Mapped[str | None] = mapped_column(String(100))
    headers: Mapped[dict[str, Any]] = mapped_column(JSONB)
    envelope: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    dead_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    archived_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=_now())
    status: Mapped[DeadLetterStatus] = mapped_column(
        _str_enum(DeadLetterStatus, "dead_letter_status"),
        default=DeadLetterStatus.PENDING,
        server_default=DeadLetterStatus.PENDING.value,
    )
    replay_count: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    replayed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    discarded_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    __table_args__ = (
        # Dedupe del archivador: la misma muerte entregada dos veces (crash antes del ack).
        UniqueConstraint("channel", "message_id", "dead_at"),
        Index("ix_dead_letters_channel_status_id", "channel", "status", "id"),
    )


__all__ = [
    "AttemptOutcome",
    "Base",
    "DeadLetter",
    "DeadLetterStatus",
    "DeliveryAttempt",
    "Notification",
    "OutboxEvent",
    "ProcessedMessage",
]
