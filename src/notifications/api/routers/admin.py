from __future__ import annotations

from typing import Annotated, Any, Literal

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response, status

from notifications.api.schemas import (
    DeadLetterDetail,
    DeadLetterPage,
    DeadLetterSummary,
    ReplayAllResponse,
)
from notifications.api.security import require_admin_key
from notifications.contracts import Channel
from notifications.db.models import DeadLetter, DeadLetterStatus
from notifications.dlq.admin import (
    BrokerUnavailableError,
    DeadLetterAdmin,
    DeadLetterNotFoundError,
    DeadLetterStateError,
)

router = APIRouter(
    prefix="/admin/dlq",
    tags=["admin"],
    dependencies=[Depends(require_admin_key)],
    responses={401: {"description": "API key inválida o ausente"}},
)

_BODY_PREVIEW_MAX = 2000
_ERRORS: dict[int | str, dict[str, Any]] = {
    404: {"description": "No existe en ese canal"},
    409: {"description": "No está en estado pending"},
    503: {"description": "Broker no disponible; la dead letter sigue pending"},
}


def _admin(request: Request) -> DeadLetterAdmin:
    admin: DeadLetterAdmin = request.app.state.dead_letter_admin
    return admin


AdminDep = Annotated[DeadLetterAdmin, Depends(_admin)]


def _detail(row: DeadLetter) -> DeadLetterDetail:
    summary = DeadLetterSummary.model_validate(row)
    return DeadLetterDetail(
        **summary.model_dump(),
        traceback=row.traceback,
        headers=row.headers,
        envelope=row.envelope,
        body_preview=row.body[:_BODY_PREVIEW_MAX].decode("utf-8", errors="replace"),
    )


def _translate(exc: Exception) -> HTTPException:
    if isinstance(exc, DeadLetterNotFoundError):
        return HTTPException(status.HTTP_404_NOT_FOUND, detail="dead letter not found")
    if isinstance(exc, DeadLetterStateError):
        return HTTPException(status.HTTP_409_CONFLICT, detail=str(exc))
    if isinstance(exc, BrokerUnavailableError):
        return HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, detail="broker no disponible")
    raise exc


@router.get("/{channel}")
async def list_dead_letters(
    channel: Channel,
    admin: AdminDep,
    status_filter: Annotated[
        DeadLetterStatus | Literal["all"], Query(alias="status")
    ] = DeadLetterStatus.PENDING,
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
    cursor: Annotated[int | None, Query(ge=1)] = None,
) -> DeadLetterPage:
    rows = await admin.list(
        channel,
        status=None if status_filter == "all" else status_filter,
        limit=limit + 1,  # uno extra para saber si hay página siguiente
        cursor=cursor,
    )
    page = rows[:limit]
    return DeadLetterPage(
        items=[DeadLetterSummary.model_validate(r) for r in page],
        next_cursor=page[-1].id if len(rows) > limit else None,
    )


@router.get("/{channel}/{dead_letter_id}", responses={404: _ERRORS[404]})
async def get_dead_letter(
    channel: Channel, dead_letter_id: int, admin: AdminDep
) -> DeadLetterDetail:
    try:
        return _detail(await admin.get(channel, dead_letter_id))
    except DeadLetterNotFoundError as exc:
        raise _translate(exc) from exc


@router.post(
    "/{channel}/replay-all",
    responses={503: _ERRORS[503]},
)
async def replay_all(
    channel: Channel,
    admin: AdminDep,
    reason: Annotated[str | None, Query(max_length=32, description="Sólo este motivo")] = None,
) -> ReplayAllResponse:
    result = await admin.replay_all(channel, reason=reason)
    if result.failed and not result.replayed:
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, detail="broker no disponible")
    return ReplayAllResponse(replayed=result.replayed, failed=result.failed)


@router.post(
    "/{channel}/{dead_letter_id}/replay",
    status_code=status.HTTP_202_ACCEPTED,
    responses=_ERRORS,
)
async def replay_dead_letter(
    channel: Channel, dead_letter_id: int, admin: AdminDep
) -> DeadLetterSummary:
    try:
        row = await admin.replay(channel, dead_letter_id)
    except (DeadLetterNotFoundError, DeadLetterStateError, BrokerUnavailableError) as exc:
        raise _translate(exc) from exc
    return DeadLetterSummary.model_validate(row)


@router.delete(
    "/{channel}/{dead_letter_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    responses={404: _ERRORS[404], 409: _ERRORS[409]},
)
async def discard_dead_letter(channel: Channel, dead_letter_id: int, admin: AdminDep) -> Response:
    try:
        await admin.discard(channel, dead_letter_id)
    except (DeadLetterNotFoundError, DeadLetterStateError) as exc:
        raise _translate(exc) from exc
    return Response(status_code=status.HTTP_204_NO_CONTENT)
