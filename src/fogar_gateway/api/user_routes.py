"""
user_routes.py
~~~~~~~~~~~~~~
Usuarios del hogar (M20), bajo `/admin` (`require_admin`: la máquina, un dueño o un
administrador).

  GET    /admin/users                  — lista, con el rol y el agente de cada uno
  POST   /admin/users                  — alta con contraseña
  PATCH  /admin/users/{id}             — nombre, rol, agente, activo
  PUT    /admin/users/{id}/password    — contraseña nueva (la recuperación de la CLI)
  DELETE /admin/users/{id}             — borra al usuario, sus dispositivos y sus conversaciones
  GET    /admin/home                   — el hogar: nombre, agente de lo común y dispositivo de avisos
  PATCH  /admin/home                   — cambia el nombre, el agente de lo común o el dispositivo de
                                         avisos (cualquier aparato activo del hogar; si no, 422
                                         `invalid_notify_device`)

Cada usuario sale con su `effective_agent` (el suyo, el de lo común, `DEFAULT_AGENT`, el de
OpenClaw o `main`) y `agent_shared`: lo atiende el mismo agente que a otra persona, así que su
memoria es común aunque las conversaciones sean privadas. La web lo avisa.

Reglas:
- **Siempre queda al menos un dueño activo:** no se borra, degrada ni desactiva al último (409
  `last_owner`).
- **Un administrador no toca a un dueño** ni da el rol de dueño (403 `owner_only`): solo un dueño o
  la máquina. Así un administrador no puede quedarse con el hogar.
- Desactivar o borrar a alguien cierra ya las sesiones vivas de sus dispositivos (4200).
- Borrar se lleva sus conversaciones privadas del gateway (en OpenClaw quedan hasta que se limpie
  su agente) y sus dispositivos personales.
"""

import asyncio
from datetime import datetime
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import BaseModel
from sqlalchemy.exc import IntegrityError
from sqlmodel import Session, col, select

from fogar_gateway.api.admin_routes import close_client_sessions
from fogar_gateway.api.deps import Caller, Principal, require_admin
from fogar_gateway.core.config import settings
from fogar_gateway.db.database import get_engine
from fogar_gateway.db.models import (
    DEFAULT_HOME_ID,
    ClientRecord,
    HomeRecord,
    MembershipRecord,
    UserRecord,
)
from fogar_gateway.models.admin_schemas import HomeUpdate, PasswordSet, UserCreate, UserUpdate
from fogar_gateway.services.auth import hash_password
from fogar_gateway.services.notify_device import (
    INVALID_NOTIFY_DEVICE,
    clear_notify_device,
    home_device_ok,
)

router = APIRouter(prefix="/admin/users", dependencies=[Depends(require_admin)])


class UserResponse(BaseModel):
    id: str
    username: str
    display_name: str
    role: str
    agent: str | None
    is_active: bool
    created_at: datetime
    effective_agent: str | None = None  # solo en la lista
    agent_shared: bool | None = None  # solo en la lista


def _view(user: UserRecord, m: MembershipRecord) -> UserResponse:
    return UserResponse(
        id=user.id, username=user.username, display_name=user.display_name, role=m.role,
        agent=m.agent, is_active=user.is_active, created_at=user.created_at,
    )


def _fallback_agent(home: HomeRecord | None, gateway_info: Any) -> str:
    """El agente de quien no tiene uno propio: el de lo común y, si no, la cascada de siempre."""
    default = gateway_info.default_agent_id if gateway_info is not None else None
    return (
        (home.agent if home is not None else None)
        or settings.DEFAULT_AGENT
        or default
        or "main"
    ).lower()


def _get(session: Session, user_id: str) -> tuple[UserRecord, MembershipRecord]:
    user = session.get(UserRecord, user_id)
    membership = session.get(MembershipRecord, (user_id, DEFAULT_HOME_ID))
    if user is None or membership is None:
        raise HTTPException(status_code=404, detail="User not found")
    return user, membership


def _is_owner_level(principal: Principal) -> bool:
    if not isinstance(principal, Caller):
        return True  # la máquina
    return principal.membership is not None and principal.membership.role == "owner"


def _require_owner_level(principal: Principal) -> None:
    if not _is_owner_level(principal):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="owner_only")


def _other_active_owners(session: Session, user_id: str) -> int:
    rows = session.exec(
        select(MembershipRecord, UserRecord)
        .join(UserRecord, col(UserRecord.id) == col(MembershipRecord.user_id))
        .where(
            MembershipRecord.home_id == DEFAULT_HOME_ID,
            MembershipRecord.role == "owner",
            col(UserRecord.is_active).is_(True),
            MembershipRecord.user_id != user_id,
        )
    ).all()
    return len(rows)


def _keep_an_owner(session: Session, user: UserRecord, m: MembershipRecord) -> None:
    """Para quitarle a `user` la condición de dueño activo: 409 si es el último."""
    if m.role == "owner" and user.is_active and _other_active_owners(session, user.id) == 0:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="last_owner")


def _device_ids(session: Session, user_id: str) -> list[str]:
    return list(session.exec(select(ClientRecord.id).where(ClientRecord.user_id == user_id)).all())


@router.get("", response_model=list[UserResponse])
def list_users(request: Request) -> list[UserResponse]:
    with Session(get_engine()) as session:
        rows = session.exec(
            select(UserRecord, MembershipRecord)
            .join(MembershipRecord, col(MembershipRecord.user_id) == col(UserRecord.id))
            .where(MembershipRecord.home_id == DEFAULT_HOME_ID)
            .order_by(col(UserRecord.created_at))
        ).all()
        home = session.get(HomeRecord, DEFAULT_HOME_ID)
    fallback = _fallback_agent(home, request.app.state.openclaw.gateway_info)
    views = [_view(u, m) for u, m in rows]
    effective = [(m.agent or fallback).lower() for _, m in rows]
    for view, agent in zip(views, effective, strict=True):
        view.effective_agent = agent
        view.agent_shared = effective.count(agent) > 1
    return views


@router.post("", response_model=UserResponse, status_code=status.HTTP_201_CREATED)
def create_user(body: UserCreate, principal: Principal = Depends(require_admin)) -> UserResponse:
    if body.role == "owner":
        _require_owner_level(principal)
    password_hash = hash_password(body.password)
    with Session(get_engine()) as session:
        user = UserRecord(
            username=body.username, display_name=body.display_name, password_hash=password_hash
        )
        session.add(user)
        try:
            session.flush()
        except IntegrityError:
            session.rollback()
            raise HTTPException(status_code=409, detail="username_taken") from None
        membership = MembershipRecord(
            user_id=user.id, home_id=DEFAULT_HOME_ID, role=body.role, agent=body.agent
        )
        session.add(membership)
        session.commit()
        session.refresh(user)
        session.refresh(membership)
        return _view(user, membership)


def _update(user_id: str, body: UserUpdate, principal: Principal) -> tuple[UserResponse, list[str]]:
    patch = body.model_dump(exclude_unset=True)
    if any(v is None for k, v in patch.items() if k != "agent"):
        raise HTTPException(status_code=422, detail="null_not_allowed")
    with Session(get_engine()) as session:
        user, m = _get(session, user_id)
        if m.role == "owner" or patch.get("role") == "owner":
            _require_owner_level(principal)
        loses_owner = (patch.get("role", "owner") != "owner") or patch.get("is_active") is False
        if loses_owner:
            _keep_an_owner(session, user, m)
        if "display_name" in patch:
            user.display_name = patch["display_name"]
        if "is_active" in patch:
            user.is_active = patch["is_active"]
        if "role" in patch:
            m.role = patch["role"]
        if "agent" in patch:
            m.agent = patch["agent"]
        session.add(user)
        session.add(m)
        session.commit()
        session.refresh(user)
        session.refresh(m)
        cut = _device_ids(session, user_id) if patch.get("is_active") is False else []
        return _view(user, m), cut


@router.patch("/{user_id}", response_model=UserResponse)
async def update_user(
    user_id: str, body: UserUpdate, request: Request, principal: Principal = Depends(require_admin)
) -> UserResponse:
    view, cut = await asyncio.to_thread(_update, user_id, body, principal)
    for device_id in cut:
        close_client_sessions(request.app, device_id)
    return view


@router.put("/{user_id}/password", status_code=status.HTTP_204_NO_CONTENT)
def set_password(
    user_id: str, body: PasswordSet, principal: Principal = Depends(require_admin)
) -> None:
    """Las sesiones abiertas siguen: un dispositivo se retira aparte (`/auth/devices`)."""
    password_hash = hash_password(body.password)
    with Session(get_engine()) as session:
        user, m = _get(session, user_id)
        if m.role == "owner":
            _require_owner_level(principal)
        user.password_hash = password_hash
        session.add(user)
        session.commit()


def _delete(user_id: str, principal: Principal) -> list[str]:
    with Session(get_engine()) as session:
        user, m = _get(session, user_id)
        if m.role == "owner":
            _require_owner_level(principal)
        _keep_an_owner(session, user, m)
        devices = _device_ids(session, user_id)
        for device_id in devices:  # la FK ya lo pondría a NULL; así no depende del PRAGMA
            clear_notify_device(session, device_id)
        # ON DELETE CASCADE: sus conversaciones privadas, su pertenencia y sus dispositivos.
        session.delete(user)
        session.commit()
        return devices


@router.delete("/{user_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_user(
    user_id: str, request: Request, principal: Principal = Depends(require_admin)
) -> None:
    devices = await asyncio.to_thread(_delete, user_id, principal)
    for device_id in devices:
        close_client_sessions(request.app, device_id)
    conversations = getattr(request.app.state, "conversations", None)
    if conversations is not None:
        await conversations.load()  # el índice de push ya no debe ver las que se fueron


# ── El hogar ──────────────────────────────────────────────────────────────────

home_router = APIRouter(prefix="/admin/home", dependencies=[Depends(require_admin)])


class HomeResponse(BaseModel):
    id: str
    name: str
    agent: str | None  # el de lo común; null = la cascada de siempre
    notify_client_id: str | None = None  # el dispositivo de avisos; null = ninguno (#407)


def _home_view(home: HomeRecord) -> HomeResponse:
    return HomeResponse(
        id=home.id, name=home.name, agent=home.agent, notify_client_id=home.notify_client_id
    )


@home_router.get("", response_model=HomeResponse)
def get_home() -> HomeResponse:
    with Session(get_engine()) as session:
        home = session.get(HomeRecord, DEFAULT_HOME_ID)
    if home is None:
        raise HTTPException(status_code=404, detail="Home not found")
    return _home_view(home)


@home_router.patch("", response_model=HomeResponse)
def update_home(body: HomeUpdate) -> HomeResponse:
    patch = body.model_dump(exclude_unset=True)
    if patch.get("name", "") is None:
        raise HTTPException(status_code=422, detail="null_not_allowed")
    with Session(get_engine()) as session:
        home = session.get(HomeRecord, DEFAULT_HOME_ID)
        if home is None:
            raise HTTPException(status_code=404, detail="Home not found")
        notify = patch.get("notify_client_id")
        if notify is not None and not home_device_ok(session, home.id, notify):
            raise HTTPException(status_code=422, detail=INVALID_NOTIFY_DEVICE)
        for field, value in patch.items():
            setattr(home, field, value)
        session.add(home)
        session.commit()
        session.refresh(home)
        return _home_view(home)
