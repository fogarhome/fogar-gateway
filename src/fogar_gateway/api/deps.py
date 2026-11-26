"""
deps.py
~~~~~~~
FastAPI dependencies compartidas: quién llama (M20) y con qué permisos (5.0, #410).

- La **máquina**: `X-Admin-Token` = `ADMIN_TOKEN`. Es la CLI y la recuperación en el host del
  gateway; puede todo.
- Un **aparato**: `Authorization: Bearer <token>`, el mismo token que manda en el handshake de
  `/ws/stream`. Personal (un inicio de sesión, con su persona y su rol) o del hogar (emparejado).
  Puede lo que dicen sus scopes efectivos (`Client.scopes`): `require_scope` es el único sitio que
  los comprueba.
"""

import asyncio
import logging
import secrets
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from fastapi import Depends, Header, HTTPException, status
from sqlmodel import Session

from fogar_gateway.core.config import settings
from fogar_gateway.core.exceptions import ClientInactive, ClientNotFound
from fogar_gateway.db.database import get_engine
from fogar_gateway.db.models import MembershipRecord, UserRecord
from fogar_gateway.models.schemas import Client
from fogar_gateway.services.db_client import db_client
from fogar_gateway.services.reconnection import describe_error
from fogar_gateway.services.scopes import HOME_ADMIN, Scope

logger = logging.getLogger(__name__)

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
    """Quien llama con el token de un aparato. Uno del hogar no tiene persona (`user` y
    `membership` None)."""

    device: Client
    user: UserRecord | None = None
    membership: MembershipRecord | None = None

    @property
    def is_admin(self) -> bool:
        return self.membership is not None and self.membership.role in ADMIN_ROLES


@dataclass(frozen=True)
class PersonalCaller(Caller):
    """Un aparato personal: siempre con su persona activa y su membresía (`/auth`)."""

    user: UserRecord
    membership: MembershipRecord


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
    except ValueError as e:
        # C-r7-2: fila con `allowed_agents` corrupto (editada a mano): como /v1, 500 y solo el
        # tipo en el log; la credencial no es el problema.
        logger.error(f"Bearer: configuración del cliente ilegible: {describe_error(e)}")
        raise HTTPException(status_code=500, detail="internal_error") from None
    if device.user_id is None:  # del hogar: sin persona
        return Caller(device)
    found = await asyncio.to_thread(_user_and_membership, device.user_id, device.home_id)
    if found is None:
        raise _UNAUTHENTICATED
    return PersonalCaller(device, *found)


async def current_caller(authorization: str | None = Header(default=None)) -> PersonalCaller:
    """El aparato personal del token, con su persona activa y su rol en el hogar (`/auth`). Uno
    del hogar no tiene cuenta: 401."""
    caller = await _caller_from_bearer(authorization)
    if not isinstance(caller, PersonalCaller):
        raise _UNAUTHENTICATED
    return caller


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


MISSING_SCOPE = "missing_scope"


def missing_scope(scope: str) -> HTTPException:
    """403 `missing_scope`, con el scope que falta en `WWW-Authenticate` (RFC 6750)."""
    return HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail=MISSING_SCOPE,
        headers={"WWW-Authenticate": f'Bearer error="insufficient_scope", scope="{scope}"'},
    )


def require_scope(scope: Scope) -> Callable[..., Awaitable[Principal]]:
    """La dependencia de una ruta que exige `scope` (#410): la máquina siempre; un aparato, si lo
    tiene entre sus efectivos (lo concedido ∩ el tope de hoy). La visibilidad de M20 se aplica
    además, en cada ruta."""

    async def dependency(principal: Principal = Depends(current_principal)) -> Principal:
        if isinstance(principal, Caller) and scope not in principal.device.scopes:
            raise missing_scope(scope)
        return principal

    return dependency


# `/admin`: la máquina, o un aparato con `home:admin` (solo una persona dueña o administradora
# puede tenerlo).
require_admin = require_scope(HOME_ADMIN)
