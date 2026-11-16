from __future__ import annotations

import uuid

from fastapi import APIRouter, HTTPException, Response, status

from notifications.api.deps import SessionDep
from notifications.api.schemas import (
    AttemptView,
    CreateNotificationRequest,
    CreateNotificationResponse,
    NotificationView,
)
from notifications.db import delivery
from notifications.db import repositories as repo
from notifications.observability import (
    NOTIFICATIONS_ACCEPTED,
    get_correlation_id,
    get_logger,
    new_correlation_id,
)

router = APIRouter(prefix="/notifications", tags=["notifications"])
log = get_logger(__name__)

IDEMPOTENT_REPLAY_HEADER = "Idempotent-Replayed"


@router.post(
    "",
    status_code=status.HTTP_202_ACCEPTED,
    responses={409: {"description": "idempotency_key reutilizada con otro cuerpo"}},
)
async def create_notification(
    body: CreateNotificationRequest, session: SessionDep, response: Response
) -> CreateNotificationResponse:
    req = repo.NewNotification(
        channel=body.channel,
        recipient=body.recipient,
        template=body.template,
        data=body.payload,
        idempotency_key=body.idempotency_key,
    )
    try:
        async with session.begin():
            result = await repo.create_notification(
                session, req, correlation_id=get_correlation_id() or new_correlation_id()
            )
    except repo.IdempotencyConflictError as exc:
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            detail="idempotency_key ya usada con un cuerpo distinto",
        ) from exc

    n = result.notification
    response.headers["Location"] = f"/notifications/{n.id}"
    if result.created:
        NOTIFICATIONS_ACCEPTED.labels(channel=str(n.channel)).inc()
    else:
        response.headers[IDEMPOTENT_REPLAY_HEADER] = "true"
    log.info(
        "notification.accepted",
        notification_id=str(n.id),
        channel=str(n.channel),
        replayed=not result.created,
    )
    return CreateNotificationResponse(notification_id=n.id, status=n.status)


@router.get("/{notification_id}", responses={404: {"description": "no existe"}})
async def get_notification(notification_id: uuid.UUID, session: SessionDep) -> NotificationView:
    n = await repo.get_notification(session, notification_id)
    if n is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail="notification not found")
    attempts = await delivery.list_attempts(session, notification_id)
    view = NotificationView.model_validate(n)
    view.attempts = [AttemptView.model_validate(a) for a in attempts]
    return view
