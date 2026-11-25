"""
auth_routes.py
~~~~~~~~~~~~~~
Sesión de usuario (M20). Cada inicio de sesión es un **dispositivo personal**: una fila de
`clients` con `user_id`, cuya clave (el token del dispositivo, opaco, guardado con hash) se usa
como `Authorization: Bearer` aquí y como `client_key` en `/ws/stream`. Sin refresco ni acceso corto:
cada petición busca el token en la BD, así que retirar el dispositivo lo corta al momento.

  POST   /auth/login               — usuario + contraseña → dispositivo personal y su token (una vez)
  POST   /auth/logout              — retira el dispositivo de esta petición
  GET    /auth/me                  — quién soy: usuario, rol, hogar, este dispositivo y mi
                                     dispositivo de avisos
  PATCH  /auth/me                  — elige mi dispositivo de avisos: uno de mis aparatos
                                     personales activos (`null` lo quita)
  GET    /auth/devices             — mis dispositivos con sesión
  DELETE /auth/devices/{id}        — retira uno de los míos

Errores con código estable en `detail`: `insecure_transport` (403), `too_many_attempts` (429,
con `Retry-After`), `invalid_credentials` (401), `not_authenticated` (401),
`invalid_notify_device` (422).
"""

import asyncio
import logging
import math
from datetime import UTC, datetime
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request, status
from sqlmodel import Session, select

from fogar_gateway.api.admin_routes import (
    close_client_sessions,
    create_client_record,
    retire_client,
)
from fogar_gateway.api.deps import Caller, current_caller
from fogar_gateway.core.logging import fingerprint_key
from fogar_gateway.db.database import get_engine
from fogar_gateway.db.models import ClientRecord, HomeRecord, MembershipRecord, UserRecord
from fogar_gateway.models.admin_schemas import ClientCreate, LoginRequest, MeUpdate
from fogar_gateway.services.auth import (
    DUMMY_PASSWORD_HASH,
    LoginLimiter,
    login_transport_ok,
    new_key,
    verify_password,
)
from fogar_gateway.services.notify_device import INVALID_NOTIFY_DEVICE, personal_device_ok

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/auth")

def _user_view(user: UserRecord, membership: MembershipRecord) -> dict[str, Any]:
    return {
        "id": user.id,
        "username": user.username,
        "display_name": user.display_name,
        "role": membership.role,
    }


def _device_view(rec: ClientRecord, current_id: str | None = None) -> dict[str, Any]:
    return {
        "id": rec.id,
        "name": rec.name,
        "client_type": rec.client_type,
        "created_at": rec.created_at,
        "last_seen_at": rec.last_seen_at,
        "is_active": rec.is_active,  # un administrador puede desactivarlo sin retirarlo
        "current": rec.id == current_id,
    }


def _limiter(request: Request) -> LoginLimiter:
    limiter: LoginLimiter = request.app.state.login_limiter
    return limiter


@router.post("/login", status_code=status.HTTP_201_CREATED)
def login(body: LoginRequest, request: Request) -> dict[str, Any]:
    if not login_transport_ok(request):
        # Sin HTTPS, desde la LAN: la contraseña viajaría en claro.
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="insecure_transport")
    limiter = _limiter(request)
    peer = request.client.host if request.client else "?"
    username = body.username.lower()
    keys = (f"ip:{peer}", f"user:{username}")
    wait = limiter.retry_after(*keys)
    if wait > 0:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="too_many_attempts",
            headers={"Retry-After": str(math.ceil(wait))},
        )

    with Session(get_engine()) as session:
        user = session.exec(select(UserRecord).where(UserRecord.username == username)).first()
        # Siempre un scrypt, exista o no la cuenta: el tiempo no dice si existe.
        ok = verify_password(body.password, user.password_hash if user else DUMMY_PASSWORD_HASH)
        membership = session.exec(
            select(MembershipRecord).where(MembershipRecord.user_id == user.id)
        ).first() if user is not None else None
        if not ok or user is None or not user.is_active or membership is None:
            limiter.failed(*keys)
            # Sin el nombre: alguien puede teclear la contraseña en ese campo.
            logger.warning("Inicio de sesión fallido desde %s", peer)
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED, detail="invalid_credentials"
            )
        limiter.succeeded(*keys)

        key = new_key()
        device = create_client_record(
            session,
            ClientCreate(name=body.device_name, client_type=body.client_type, client_key=key),
            user_id=user.id,
            capabilities=body.capabilities,
        )
        device.last_seen_at = datetime.now(UTC)  # acaba de entrar
        session.add(device)
        session.commit()
        session.refresh(device)
        home = session.get(HomeRecord, device.home_id)
        logger.info(
            "Inicio de sesión: usuario %s, dispositivo %s (fp=%s)",
            user.id, device.id, fingerprint_key(key),
        )
        return {
            "client_id": device.id,
            "client_key": key,
            "user": _user_view(user, membership),
            "home": {"id": home.id, "name": home.name} if home else None,
        }


def _me(caller: Caller) -> dict[str, Any]:
    with Session(get_engine()) as session:
        home = session.get(HomeRecord, caller.device.home_id)
        device = session.get(ClientRecord, caller.device.id)
        # Leída aquí, no de `caller.membership`: un PATCH acaba de cambiarla.
        membership = session.get(MembershipRecord, (caller.user.id, caller.device.home_id))
    return {
        "user": _user_view(caller.user, caller.membership),
        "home": {"id": home.id, "name": home.name} if home else None,
        "device": _device_view(device, caller.device.id) if device else None,
        "notify_client_id": membership.notify_client_id if membership else None,
    }


@router.get("/me")
def me(caller: Caller = Depends(current_caller)) -> dict[str, Any]:
    return _me(caller)


@router.patch("/me")
def update_me(body: MeUpdate, caller: Caller = Depends(current_caller)) -> dict[str, Any]:
    """El dispositivo de avisos de la persona (#407): solo uno de sus aparatos personales activos;
    un aviso privado nunca suena en un aparato del hogar ni de otra persona."""
    patch = body.model_dump(exclude_unset=True)
    if "notify_client_id" in patch:
        notify = patch["notify_client_id"]
        home_id = caller.device.home_id
        with Session(get_engine()) as session:
            if notify is not None and not personal_device_ok(
                session, caller.user.id, home_id, notify
            ):
                raise HTTPException(status_code=422, detail=INVALID_NOTIFY_DEVICE)
            membership = session.get(MembershipRecord, (caller.user.id, home_id))
            if membership is None:  # dada de baja entre la auth y aquí
                raise HTTPException(status_code=401, detail="not_authenticated")
            membership.notify_client_id = notify
            session.add(membership)
            session.commit()
    return _me(caller)


@router.get("/devices")
def my_devices(caller: Caller = Depends(current_caller)) -> list[dict[str, Any]]:
    with Session(get_engine()) as session:
        rows = session.exec(
            select(ClientRecord).where(
                ClientRecord.user_id == caller.user.id,
                ClientRecord.retired_at.is_(None),  # type: ignore[union-attr]
            )
        ).all()
    return [_device_view(r, caller.device.id) for r in rows]


def _retire_own(user_id: str, device_id: str) -> bool:
    with Session(get_engine()) as session:
        rec = session.get(ClientRecord, device_id)
        if rec is None or rec.user_id != user_id or rec.retired_at is not None:
            return False
        retire_client(session, rec)
        return True


@router.delete("/devices/{device_id}", status_code=status.HTTP_204_NO_CONTENT)
async def retire_my_device(
    device_id: str, request: Request, caller: Caller = Depends(current_caller)
) -> None:
    if not await asyncio.to_thread(_retire_own, caller.user.id, device_id):
        raise HTTPException(status_code=404, detail="Device not found")  # ajeno o inexistente
    close_client_sessions(request.app, device_id)


@router.post("/logout", status_code=status.HTTP_204_NO_CONTENT)
async def logout(request: Request, caller: Caller = Depends(current_caller)) -> None:
    await asyncio.to_thread(_retire_own, caller.user.id, caller.device.id)
    close_client_sessions(request.app, caller.device.id)
