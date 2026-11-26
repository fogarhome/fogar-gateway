"""
home_routes.py
~~~~~~~~~~~~~~
El hogar (5.0, #411; antes `/home`, `/home/users` y `/home/devices`): su ficha, sus
personas (M20) y sus aparatos. Leer exige `home:read`; todo lo que escribe, `home:admin` (la
máquina o el aparato personal de un dueño o administrador que lo pidió).

  GET    /home                        — nombre, agente de lo común y dispositivo de avisos
  PATCH  /home                        — los cambia (aviso: cualquier aparato activo del hogar; si
                                        no, 422 `invalid_notify_device`)
  GET    /home/users                  — personas, con el rol y el agente de cada una
  POST   /home/users                  — alta con contraseña
  PATCH  /home/users/{id}             — nombre, rol, agente, activo
  PUT    /home/users/{id}/password    — contraseña nueva (la recuperación de la CLI)
  DELETE /home/users/{id}             — borra a la persona, sus aparatos y sus conversaciones
  GET    /home/devices[/{id}]         — aparatos (sin la clave: su huella `key_fp`), con sus scopes
  POST   /home/devices                — alta (la respuesta trae su `client_key`, la única vez)
  PATCH  /home/devices/{id}           — campos, scopes (uno del hogar hasta `home:read`; a uno
                                        personal solo se le quitan)
  DELETE /home/devices/{id}           — lo retira (no lo borra) y corta sus sesiones (4200)
  POST   /home/devices/{id}/rotate-key — clave nueva

Cada persona sale con su `effective_agent` (el suyo, el de lo común, `DEFAULT_AGENT`, el de
OpenClaw o `main`) y `agent_shared`: la atiende el mismo agente que a otra, así que su memoria es
común aunque las conversaciones sean privadas. La web lo avisa.

Reglas:
- **Siempre queda al menos un dueño activo:** no se borra, degrada ni desactiva al último (409
  `last_owner`).
- **Un administrador no toca a un dueño** ni da el rol de dueño (403 `owner_only`): solo un dueño o
  la máquina. Así un administrador no puede quedarse con el hogar.
- Desactivar o borrar a alguien cierra ya las sesiones vivas de sus aparatos (4200).
- Borrar se lleva sus conversaciones privadas del gateway (en OpenClaw quedan hasta que se limpie
  su agente) y sus aparatos personales.
"""

import asyncio
import json
from datetime import UTC, datetime
from typing import Any

from fastapi import APIRouter, Depends, FastAPI, HTTPException, Request, status
from pydantic import BaseModel
from sqlalchemy.exc import IntegrityError
from sqlmodel import Session, col, select

from fogar_gateway.api.deps import Caller, Principal, require_admin, require_scope
from fogar_gateway.core.config import settings
from fogar_gateway.db.database import get_db_session, get_engine
from fogar_gateway.db.models import (
    DEFAULT_HOME_ID,
    ClientRecord,
    HomeRecord,
    MembershipRecord,
    UserRecord,
)
from fogar_gateway.models.admin_schemas import (
    ClientCreate,
    ClientResponse,
    ClientUpdate,
    ClientWithKey,
    HomeUpdate,
    PasswordSet,
    UserCreate,
    UserUpdate,
)
from fogar_gateway.services.auth import hash_key, hash_password, new_key
from fogar_gateway.services.notify_device import (
    INVALID_NOTIFY_DEVICE,
    clear_notify_device,
    home_device_ok,
)
from fogar_gateway.services.scopes import (
    DEFAULT_SCOPES,
    HOME_READ,
    HOUSEHOLD_CAP,
    SCOPE_NOT_ALLOWED,
    normalize_scopes,
    to_list,
)

_ROTATE_KEY_ATTEMPTS = 3

# Una competencia, un router (5.0, #411): leer es `home:read`; lo que escribe exige además
# `home:admin` (`require_admin`, en cada ruta).
router = APIRouter(prefix="/home", dependencies=[Depends(require_scope(HOME_READ))])


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


@router.get("/users", response_model=list[UserResponse])
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


@router.post("/users", response_model=UserResponse, status_code=status.HTTP_201_CREATED)
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


@router.patch("/users/{user_id}", response_model=UserResponse)
async def update_user(
    user_id: str, body: UserUpdate, request: Request, principal: Principal = Depends(require_admin)
) -> UserResponse:
    view, cut = await asyncio.to_thread(_update, user_id, body, principal)
    for device_id in cut:
        close_client_sessions(request.app, device_id)
    return view


@router.put("/users/{user_id}/password", status_code=status.HTTP_204_NO_CONTENT)
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


@router.delete("/users/{user_id}", status_code=status.HTTP_204_NO_CONTENT)
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

class HomeResponse(BaseModel):
    id: str
    name: str
    agent: str | None  # el de lo común; null = la cascada de siempre
    notify_client_id: str | None = None  # el dispositivo de avisos; null = ninguno (#407)


def _home_view(home: HomeRecord) -> HomeResponse:
    return HomeResponse(
        id=home.id, name=home.name, agent=home.agent, notify_client_id=home.notify_client_id
    )


@router.get("", response_model=HomeResponse)
def get_home() -> HomeResponse:
    with Session(get_engine()) as session:
        home = session.get(HomeRecord, DEFAULT_HOME_ID)
    if home is None:
        raise HTTPException(status_code=404, detail="Home not found")
    return _home_view(home)


@router.patch("", response_model=HomeResponse, dependencies=[Depends(require_admin)])
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


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _get_or_404(session: Session, client_id: str) -> ClientRecord:
    rec = session.get(ClientRecord, client_id)
    if rec is None or rec.retired_at is not None:
        raise HTTPException(status_code=404, detail="Client not found")
    return rec


def _check_agent_consistency(default_agent: str | None, allowed_agents: list[str] | None) -> None:
    """422 si el estado resultante deja `default_agent` fuera de `allowed_agents`.

    `allowed_agents is None` = sin restricción; `[]` = todo denegado (#105), así que
    un `default_agent` con `[]` es inconsistente.
    """
    if default_agent is None or allowed_agents is None:
        return
    if default_agent.lower() not in [a.lower() for a in allowed_agents]:  # filas de la 2.x (C-1)
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="default_agent must be one of allowed_agents",
        )


def check_household_scopes(scopes: frozenset[str]) -> None:
    """Un aparato del hogar llega como mucho a `home:read` (#410): 422 si piden más."""
    if not scopes <= HOUSEHOLD_CAP:
        raise HTTPException(status_code=422, detail=SCOPE_NOT_ALLOWED)


def _lists_to_json(data: dict[str, Any]) -> dict[str, Any]:
    """Serializa a JSON las listas que la BD guarda como texto (si vienen y no son null)."""
    for field in ("allowed_agents", "output_mode"):
        if data.get(field) is not None:
            data[field] = json.dumps(data[field])
    return data


# ---------------------------------------------------------------------------
# Aparatos (antes /home/devices)
# ---------------------------------------------------------------------------


@router.get("/devices", response_model=list[ClientResponse])
def list_clients(session: Session = Depends(get_db_session)) -> list[ClientResponse]:
    records = session.exec(select(ClientRecord).where(ClientRecord.retired_at.is_(None))).all()  # type: ignore[union-attr]
    return [ClientResponse.from_record(r) for r in records]


def create_client_record(
    session: Session,
    body: ClientCreate,
    *,
    client_id: str | None = None,
    user_id: str | None = None,
    capabilities: list[str] | None = None,
) -> ClientRecord:
    """Crea el cliente: la única vía, la usan `POST /home/devices`, el emparejamiento (#317) y
    el inicio de sesión (M20, un dispositivo personal con `user_id`). `client_id` fija el id; si
    no, uno nuevo. Guarda solo el hash de `body.client_key` (M20): quien necesite enseñar la
    clave la pone en `body` antes."""
    _check_agent_consistency(body.default_agent, body.allowed_agents)
    scopes = normalize_scopes(body.scopes) if body.scopes is not None else DEFAULT_SCOPES
    if user_id is None:
        check_household_scopes(scopes)
    data = _lists_to_json(body.model_dump())
    data["scopes"] = json.dumps(to_list(scopes))
    data["key_hash"] = hash_key(data.pop("client_key") or new_key())
    data["user_id"] = user_id  # M20: con usuario, un dispositivo personal (un inicio de sesión)
    data["capabilities"] = json.dumps(capabilities) if capabilities is not None else None
    if client_id is not None:
        data["id"] = client_id
    record = ClientRecord(**data)
    session.add(record)
    try:
        session.commit()
    except IntegrityError:
        # client_key duplicada (única restricción alcanzable tras validar la entrada).
        # `from None` y sin loguear la excepción: su mensaje/parámetros contienen la
        # client_key completa (credencial bearer, G-27a).
        session.rollback()
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT, detail="client_key already exists"
        ) from None
    session.refresh(record)
    return record


@router.post("/devices", response_model=ClientWithKey, status_code=status.HTTP_201_CREATED, dependencies=[Depends(require_admin)])
def create_client(
    body: ClientCreate,
    session: Session = Depends(get_db_session),
) -> ClientWithKey:
    key = body.client_key or new_key()
    record = create_client_record(session, body.model_copy(update={"client_key": key}))
    return ClientWithKey.from_record(record, key)


@router.get("/devices/{client_id}", response_model=ClientResponse)
def get_client(
    client_id: str,
    session: Session = Depends(get_db_session),
) -> ClientResponse:
    return ClientResponse.from_record(_get_or_404(session, client_id))


@router.patch("/devices/{client_id}", response_model=ClientResponse, dependencies=[Depends(require_admin)])
def update_client(
    client_id: str,
    body: ClientUpdate,
    session: Session = Depends(get_db_session),
) -> ClientResponse:
    rec = _get_or_404(session, client_id)
    patch = body.model_dump(exclude_unset=True)
    # Solo si el PATCH toca alguno de los dos campos: una fila antigua inconsistente
    # que se edita por otros campos no debe fallar (solo se valida la entrada, #122).
    if "default_agent" in patch or "allowed_agents" in patch:
        _check_agent_consistency(
            patch.get("default_agent", rec.default_agent),
            patch["allowed_agents"]
            if "allowed_agents" in patch
            else (json.loads(rec.allowed_agents) if rec.allowed_agents else None),
        )
    if "scopes" in patch:
        scopes = normalize_scopes(patch["scopes"])
        # Uno del hogar, hasta su tope; uno personal solo puede perder lo que pidió (para pedir
        # más, su persona inicia sesión otra vez).
        limit = HOUSEHOLD_CAP if rec.user_id is None else normalize_scopes(
            ClientResponse.from_record(rec).scopes
        )
        if not scopes <= limit:
            raise HTTPException(status_code=422, detail=SCOPE_NOT_ALLOWED)
        patch["scopes"] = json.dumps(to_list(scopes))
    for field, value in _lists_to_json(patch).items():
        setattr(rec, field, value)
    session.add(rec)
    session.commit()
    session.refresh(rec)
    return ClientResponse.from_record(rec)


def retire_client(session: Session, rec: ClientRecord) -> None:
    """Retirar, no borrar (M20): sus conversaciones conservan hogar y dispositivo. La clave pasa
    a un valor que nadie conoce y deja de ser dispositivo de avisos donde estuviera elegido
    (#407). Sus sesiones vivas las corta `close_client_sessions`."""
    clear_notify_device(session, rec.id)
    rec.retired_at = datetime.now(UTC)
    rec.is_active = False
    rec.key_hash = hash_key(new_key())  # nadie conoce la clave: ya no vale
    session.add(rec)
    session.commit()


def close_client_sessions(app: FastAPI, client_id: str) -> None:
    """Cierra ya (4200) las sesiones vivas de un cliente, sin esperar a la revalidación."""
    for bridge in app.state.client_registry.for_client(client_id):
        bridge.revoke()


def _retire_client_record(client_id: str) -> None:
    with Session(get_engine()) as session:
        retire_client(session, _get_or_404(session, client_id))


@router.delete("/devices/{client_id}", status_code=status.HTTP_204_NO_CONTENT, dependencies=[Depends(require_admin)])
async def delete_client(client_id: str, request: Request) -> None:
    await asyncio.to_thread(_retire_client_record, client_id)
    close_client_sessions(request.app, client_id)


@router.post("/devices/{client_id}/rotate-key", response_model=ClientWithKey, dependencies=[Depends(require_admin)])
def rotate_client_key(
    client_id: str,
    session: Session = Depends(get_db_session),
) -> ClientWithKey:
    rec = _get_or_404(session, client_id)
    for _ in range(_ROTATE_KEY_ATTEMPTS):
        key = new_key()
        rec.key_hash = hash_key(key)
        session.add(rec)
        try:
            session.commit()
            break
        except IntegrityError:
            # Colisión de client_key: reintento con otra. Sin loguear ni encadenar la
            # excepción (su mensaje lleva la clave, G-27a).
            session.rollback()
    else:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT, detail="could not generate a unique client_key"
        ) from None
    session.refresh(rec)
    return ClientWithKey.from_record(rec, key)
