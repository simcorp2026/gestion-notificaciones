"""DLQ: archivado en Postgres, API admin (listar, replay, descartar) y replay end-to-end."""

from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta

import httpx
from aio_pika import Message
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from notifications.contracts import Channel, Envelope, NotificationStatus
from notifications.db import dead_letters as dl_repo
from notifications.db.models import AttemptOutcome, DeadLetter, DeadLetterStatus
from notifications.dlq.archiver import DeadLetterArchiver, build_dead_letter
from notifications.errors import PermanentError
from notifications.messaging.bus import RawMessage
from notifications.messaging.rabbitmq import RabbitMQBus

from .helpers import eventually, get_one, queue_depth
from .test_worker import (
    CH,
    ScriptedSender,
    create,
    notification_id,
    outcomes,
    running,
    status_of,
    wait_status,
)

SF = async_sessionmaker[AsyncSession]


@asynccontextmanager
async def archiving(bus: RabbitMQBus, sf: SF, channel: Channel = CH) -> AsyncIterator[None]:
    consumer = await bus.consume_dead_letters(channel, DeadLetterArchiver(sf).handle)
    try:
        yield
    finally:
        await consumer.stop(drain_timeout=5)


async def dead_rows(sf: SF) -> Sequence[DeadLetter]:
    async with sf() as s:
        return (await s.scalars(select(DeadLetter).order_by(DeadLetter.id))).all()


async def wait_rows(sf: SF, n: int) -> Sequence[DeadLetter]:
    async def check() -> bool:
        return len(await dead_rows(sf)) >= n

    await eventually(check, 10, f"{n} dead letters archivadas")
    return await dead_rows(sf)


async def seed_dead_letters(sf: SF, n: int) -> list[DeadLetter]:
    """Inserta ``n`` dead letters de notificaciones reales (sin pasar por el broker)."""
    rows = []
    for i in range(n):
        envelope = await create(sf)
        raw = RawMessage(
            channel=CH,
            body=envelope.to_bytes(),
            headers={
                "x-attempt": 3,
                "x-dead-reason": "exhausted" if i % 2 == 0 else "permanent",
                "x-error-class": "TransientError",
                "x-failed-at": (datetime.now(UTC) + timedelta(seconds=i)).isoformat(),
            },
            message_id=str(envelope.id),
            correlation_id=envelope.correlation_id,
            content_type="application/json",
            type=envelope.type,
        )
        async with sf() as s, s.begin():
            dl_id = await dl_repo.insert_dead_letter(s, build_dead_letter(raw))
        async with sf() as s:
            row = await s.get_one(DeadLetter, dl_id)
            rows.append(row)
    return rows


# ------------------------------------------------------------------------------ archivador


async def test_archiver_persists_dead_letter_and_marks_notification_dead(
    bus: RabbitMQBus, session_factory: SF
) -> None:
    envelope = await create(session_factory)
    nid = notification_id(envelope)

    async with (
        archiving(bus, session_factory),
        running(bus, session_factory, ScriptedSender(always=PermanentError("rechazado"))),
    ):
        await bus.publish(envelope, CH)
        (row,) = await wait_rows(session_factory, 1)

    assert row.message_id == str(envelope.id)
    assert row.notification_id == nid
    assert row.correlation_id == envelope.correlation_id
    assert row.reason == "permanent"
    assert row.attempts == 0
    assert row.error_class == "PermanentError"
    assert row.error_message == "rechazado"
    assert "PermanentError: rechazado" in (row.traceback or "")
    assert row.status is DeadLetterStatus.PENDING
    assert Envelope.model_validate(row.envelope) == envelope
    assert await status_of(session_factory, nid) is NotificationStatus.DEAD
    assert await queue_depth(bus, bus.topology.dlq(CH)) == 0  # archivado → ack


async def test_archiver_keeps_unparseable_messages(bus: RabbitMQBus, session_factory: SF) -> None:
    async with archiving(bus, session_factory), running(bus, session_factory, ScriptedSender()):
        await bus.publish_to_queue(
            Message(b"{roto", message_id="raw-1"), bus.topology.main_queue(CH)
        )
        (row,) = await wait_rows(session_factory, 1)

    assert row.reason == "invalid"
    assert row.envelope is None
    assert row.notification_id is None
    assert row.body == b"{roto"


async def test_archiver_dedupes_same_death_delivered_twice(session_factory: SF) -> None:
    envelope = await create(session_factory)
    raw = RawMessage(
        channel=CH,
        body=envelope.to_bytes(),
        headers={"x-dead-reason": "exhausted", "x-failed-at": datetime.now(UTC).isoformat()},
        message_id=str(envelope.id),
    )
    archiver = DeadLetterArchiver(session_factory)

    await archiver.handle(raw)
    await archiver.handle(raw)  # crash del archivador antes del ack → re-entrega

    assert len(await dead_rows(session_factory)) == 1


async def test_broker_dead_letter_without_worker_marks_notification_dead(
    session_factory: SF,
) -> None:
    """``delivery-limit`` superado: el broker lo manda a la DLQ sin pasar por el worker."""
    envelope = await create(session_factory)
    raw = RawMessage(
        channel=CH,
        body=envelope.to_bytes(),
        headers={"x-death": [{"reason": "delivery_limit", "count": 1, "time": datetime.now(UTC)}]},
        message_id=str(envelope.id),
    )

    await DeadLetterArchiver(session_factory).handle(raw)

    (row,) = await dead_rows(session_factory)
    assert row.reason == "broker:delivery_limit"
    assert await status_of(session_factory, notification_id(envelope)) is NotificationStatus.DEAD


# ------------------------------------------------------------------------------- API admin


async def test_admin_requires_api_key(admin_api: httpx.AsyncClient) -> None:
    for headers in ({"X-API-Key": ""}, {"X-API-Key": "incorrecta"}):
        resp = await admin_api.get("/admin/dlq/email", headers=headers)
        assert resp.status_code == 401


async def test_list_is_paginated_newest_first(
    admin_api: httpx.AsyncClient, session_factory: SF
) -> None:
    rows = await seed_dead_letters(session_factory, 5)
    expected = [r.id for r in reversed(rows)]

    seen: list[int] = []
    cursor: int | None = None
    pages = 0
    while True:
        params: dict[str, int] = {"limit": 2}
        if cursor is not None:
            params["cursor"] = cursor
        page = (await admin_api.get("/admin/dlq/email", params=params)).json()
        seen += [item["id"] for item in page["items"]]
        pages += 1
        cursor = page["next_cursor"]
        if cursor is None:
            break

    assert seen == expected
    assert pages == 3
    other = (await admin_api.get("/admin/dlq/sms")).json()
    assert other["items"] == []


async def test_detail_and_404(admin_api: httpx.AsyncClient, session_factory: SF) -> None:
    (row,) = await seed_dead_letters(session_factory, 1)

    detail = (await admin_api.get(f"/admin/dlq/email/{row.id}")).json()

    assert detail["reason"] == "exhausted"
    assert detail["envelope"]["id"] == row.message_id
    assert detail["headers"]["x-attempt"] == 3
    assert detail["body_preview"].startswith("{")
    assert (await admin_api.get(f"/admin/dlq/sms/{row.id}")).status_code == 404
    assert (await admin_api.get("/admin/dlq/email/999999")).status_code == 404


async def test_replay_returns_message_to_main_with_attempt_reset(
    admin_api: httpx.AsyncClient, bus: RabbitMQBus, session_factory: SF
) -> None:
    """Criterio de aceptación de la Fase 5 (a nivel de broker)."""
    (row,) = await seed_dead_letters(session_factory, 1)

    resp = await admin_api.post(f"/admin/dlq/email/{row.id}/replay")

    assert resp.status_code == 202
    assert resp.json()["status"] == "replayed"
    assert resp.json()["replay_count"] == 1
    msg = await get_one(bus, bus.topology.main_queue(CH))
    assert msg.message_id == row.message_id
    assert msg.correlation_id == row.correlation_id
    assert msg.headers["x-attempt"] == 0
    assert msg.headers["x-replay-count"] == 1
    assert msg.headers["x-replayed-from-dead-letter"] == row.id
    assert "x-dead-reason" not in msg.headers
    assert "x-traceback" not in msg.headers
    assert msg.body == row.body
    assert row.notification_id is not None
    assert await status_of(session_factory, row.notification_id) is NotificationStatus.PUBLISHED
    # Una dead letter ya reprocesada no se puede reprocesar ni descartar de nuevo.
    assert (await admin_api.post(f"/admin/dlq/email/{row.id}/replay")).status_code == 409
    assert (await admin_api.delete(f"/admin/dlq/email/{row.id}")).status_code == 409


async def test_discard(admin_api: httpx.AsyncClient, session_factory: SF) -> None:
    (row,) = await seed_dead_letters(session_factory, 1)

    assert (await admin_api.delete(f"/admin/dlq/email/{row.id}")).status_code == 204

    pending = (await admin_api.get("/admin/dlq/email")).json()["items"]
    discarded = (await admin_api.get("/admin/dlq/email", params={"status": "discarded"})).json()
    assert pending == []
    assert [i["id"] for i in discarded["items"]] == [row.id]
    assert (await admin_api.post(f"/admin/dlq/email/{row.id}/replay")).status_code == 409
    assert (await admin_api.delete("/admin/dlq/email/999999")).status_code == 404


async def test_replay_all_with_reason_filter(
    admin_api: httpx.AsyncClient, bus: RabbitMQBus, session_factory: SF
) -> None:
    await seed_dead_letters(session_factory, 5)  # 3 exhausted + 2 permanent

    only_exhausted = await admin_api.post(
        "/admin/dlq/email/replay-all", params={"reason": "exhausted"}
    )
    rest = await admin_api.post("/admin/dlq/email/replay-all")

    assert only_exhausted.json() == {"replayed": 3, "failed": 0}
    assert rest.json() == {"replayed": 2, "failed": 0}
    assert await queue_depth(bus, bus.topology.main_queue(CH)) == 5
    async with session_factory() as s:
        pending = await s.scalar(
            select(func.count())
            .select_from(DeadLetter)
            .where(DeadLetter.status == DeadLetterStatus.PENDING)
        )
    assert pending == 0


async def test_replay_with_broker_down_is_503_and_stays_pending(
    api: httpx.AsyncClient, session_factory: SF
) -> None:
    (row,) = await seed_dead_letters(session_factory, 1)
    headers = {"X-API-Key": "test-admin-key-0123456789"}

    single = await api.post(f"/admin/dlq/email/{row.id}/replay", headers=headers)
    bulk = await api.post("/admin/dlq/email/replay-all", headers=headers)

    assert single.status_code == 503
    assert bulk.status_code == 503
    async with session_factory() as s:
        assert (await s.get_one(DeadLetter, row.id)).status is DeadLetterStatus.PENDING


# ---------------------------------------------------------------------------- end to end


async def test_dead_notification_is_delivered_after_replay(
    admin_api: httpx.AsyncClient, bus: RabbitMQBus, session_factory: SF
) -> None:
    """Criterio de aceptación de la Fase 5: muere → se archiva → replay → el worker la
    procesa desde ``x-attempt=0`` y termina ``sent``; el historial conserva ambos intentos."""
    envelope = await create(session_factory)
    nid = notification_id(envelope)
    # Primer envío: rechazo permanente (p. ej. proveedor mal configurado). Tras corregirlo, OK.
    sender = ScriptedSender(script=[PermanentError("plantilla inexistente en proveedor")])

    async with archiving(bus, session_factory), running(bus, session_factory, sender):
        await bus.publish(envelope, CH)
        (row,) = await wait_rows(session_factory, 1)
        await wait_status(session_factory, nid, NotificationStatus.DEAD)

        resp = await admin_api.post(f"/admin/dlq/email/{row.id}/replay")
        assert resp.status_code == 202
        await wait_status(session_factory, nid, NotificationStatus.SENT)

    assert len(sender.calls) == 2
    assert await outcomes(session_factory, nid) == [
        (0, AttemptOutcome.DEAD),
        (0, AttemptOutcome.OK),  # x-attempt reiniciado por el replay
    ]
    view = (await admin_api.get(f"/notifications/{nid}")).json()
    assert view["status"] == "sent"
    assert [(a["attempt"], a["outcome"]) for a in view["attempts"]] == [(0, "dead"), (0, "ok")]
    assert view["attempts"][0]["error_class"] == "PermanentError"
