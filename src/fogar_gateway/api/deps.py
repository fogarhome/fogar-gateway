"""
deps.py
~~~~~~~
FastAPI dependencies compartidas: quién llama (M20).

- La **máquina**: `X-Admin-Token` = `ADMIN_TOKEN`. Es la CLI y la recuperación en el host del
  gateway; puede todo.
- Un **usuario**: `Authorization: Bearer <token>` de un dispositivo personal (`/auth/login`), con
  su rol en el hogar.
"""

import asyncio
import secrets
from dataclasses import dataclass

from fastapi import Depends, Header, HTTPException, status
from sqlmodel import Session

from fogar_gateway.core.config import settings
from fogar_gateway.core.exceptions import ClientInactive, ClientNotFound
from fogar_gateway.db.database import get_engine
from fogar_gateway.db.models import MembershipRecord, UserRecord
from fogar_gateway.models.schemas import Client
from fogar_gateway.services.db_client import db_client

ADMIN_ROLES = frozenset({"owner", "admin"})

_UNAUTHENTICATED = HTTPException(
    status_code=status.HTTP_401_UNAUTHORIZED,
    detail="not_authenticated",
    headers={"WWW-Authenticate": "Bearer"},
)


def admin_token_matches(given: str) -> bool:
    """`given` es el ADMIN_TOKEN: la única comprobación, para `/admin` (X-Admin-Token) y para el
    `admin_token` del handshake WS (M19). Sin ADMIN_TOKEN configurado, nunca."""
    if not settings.ADMIN_TOKEN:
        return False
    # G-30 (#216): tiempo constante. Bytes porque compare_digest(str, str) lanza
    # TypeError con caracteres no ASCII (eso sería un 500 en vez de un 401).
    return secrets.compare_digest(given.encode("utf-8"), settings.ADMIN_TOKEN.encode("utf-8"))


@dataclass(frozen=True)
class Caller:
    """Quien llama con el token de un dispositivo personal."""

    device: Client
    user: UserRecord
    membership: MembershipRecord

    @property
    def is_admin(self) -> bool:
        return self.membership.role in ADMIN_ROLES


def _user_and_membership(user_id: str, home_id: str) -> tuple[UserRecord, MembershipRecord] | None:
    with Session(get_engine()) as session:
        user = session.get(UserRecord, user_id)
        membership = session.get(MembershipRecord, (user_id, home_id))
    if user is None or not user.is_active or membership is None:
        return None
    return user, membership


async def _caller_from_bearer(authorization: str | None) -> Caller:
    scheme, _, token = (authorization or "").partition(" ")
    if scheme.lower() != "bearer" or not token.strip():
        raise _UNAUTHENTICATED
    try:
        device, _ = await db_client.get_session(token.strip())
    except (ClientNotFound, ClientInactive):
        raise _UNAUTHENTICATED from None
    if device.user_id is None:  # un dispositivo del hogar no tiene cuenta
        raise _UNAUTHENTICATED
    found = await asyncio.to_thread(_user_and_membership, device.user_id, device.home_id)
    if found is None:
        raise _UNAUTHENTICATED
    return Caller(device, *found)


async def current_caller(authorization: str | None = Header(default=None)) -> Caller:
    """El dispositivo personal del token, con su usuario activo y su rol en el hogar."""
    return await _caller_from_bearer(authorization)


@dataclass(frozen=True)
class Machine:
    """La CLI o la recuperación con `ADMIN_TOKEN`, en la máquina del gateway."""


Principal = Machine | Caller


async def current_principal(
    x_admin_token: str | None = Header(default=None),
    authorization: str | None = Header(default=None),
) -> Principal:
    """La máquina (`X-Admin-Token`) o un usuario (`Bearer`). Un `X-Admin-Token` presente manda:
    si no vale, es 401 aunque también venga un Bearer."""
    if x_admin_token is not None:
        if not settings.ADMIN_TOKEN:
            # 503 si ADMIN_TOKEN no está configurado (evita exponerlo por accidente).
            raise HTTPException(status_code=503, detail="Admin API not configured")
        if not admin_token_matches(x_admin_token):
            raise HTTPException(status_code=401, detail="Invalid admin token")
        return Machine()
    return await _caller_from_bearer(authorization)


async def require_admin(principal: Principal = Depends(current_principal)) -> Principal:
    """`/admin`: la máquina, o un usuario dueño o administrador (M20). Un miembro, 403."""
    if isinstance(principal, Caller) and not principal.is_admin:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="forbidden")
    return principal
