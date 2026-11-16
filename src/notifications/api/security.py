"""Autenticación por API key para los endpoints admin."""

from __future__ import annotations

import secrets
from typing import Annotated

from fastapi import Depends, HTTPException, Request, status
from fastapi.security import APIKeyHeader

API_KEY_HEADER = "X-API-Key"

_scheme = APIKeyHeader(name=API_KEY_HEADER, auto_error=False, description="Clave de administración")


async def require_admin_key(
    request: Request, provided: Annotated[str | None, Depends(_scheme)]
) -> None:
    expected: str = request.app.state.settings.api.admin_key.get_secret_value()
    # compare_digest: tiempo constante, no filtra prefijos correctos por timing.
    if provided is None or not secrets.compare_digest(provided.encode(), expected.encode()):
        raise HTTPException(
            status.HTTP_401_UNAUTHORIZED,
            detail="API key inválida o ausente",
            headers={"WWW-Authenticate": API_KEY_HEADER},
        )
