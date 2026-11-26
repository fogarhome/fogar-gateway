"""
db_client.py
~~~~~~~~~~~~
Cliente local SQLite: identidad y configuración de cada cliente, sin servicio externo.

Interfaz pública invariante:
  db_client.get_session(client_key) → (Client, ClientConfig)
"""

import asyncio
import json
import logging
from datetime import UTC, datetime

from sqlmodel import Session, col, select, update

from fogar_gateway.core.exceptions import ClientInactive, ClientNotFound
from fogar_gateway.db.database import get_engine
from fogar_gateway.db.models import ClientRecord, HomeRecord, MembershipRecord, UserRecord
from fogar_gateway.models.schemas import Client, ClientConfig
from fogar_gateway.services.auth import hash_key
from fogar_gateway.services.notify_device import notify_client_for
from fogar_gateway.services.scopes import normalize_scopes, scope_cap

logger = logging.getLogger(__name__)


def _parse_allowed_agents(raw: str | None) -> list[str] | None:
    """Parse the JSON-encoded `allowed_agents` field from ClientRecord.

    - None / "" / "null"  → None   (sin restricción)
    - "[]"                 → []     (deniega todo)
    - '["a","b"]'         → ["a", "b"]
    - malformed JSON       → raises ValueError (fail loud; never silently bypass)

    Returns None for the legacy "empty/null" inputs so that an admin who
    has not configured the field exercises the no-restriction path.
    """
    # The "null" string check below is load-bearing: json.loads("null")
    # returns Python None, which would slip past the isinstance check and
    # raise a confusing ValueError. Intercept it explicitly as "no
    # restriction" instead.
    if raw is None or raw == "" or raw == "null":
        return None
    parsed = json.loads(raw)
    if not isinstance(parsed, list):
        raise ValueError(f"allowed_agents must be a JSON list, got {type(parsed).__name__}")
    return [str(x) for x in parsed]


class DbClient:
    """
    Wrapper sobre la BD SQLite local. Sin caché (#265): cada llamada consulta la BD
    (un SELECT por índice), así que un cambio hecho desde la CLI u otro proceso se ve
    en la siguiente revalidación.

    `engine` opcional para facilitar tests en memoria;
    en producción usa el engine global de fogar_gateway.db.database.
    """

    def __init__(self, engine=None):
        self._engine = engine

    def _get_engine(self):
        return self._engine if self._engine is not None else get_engine()

    async def get_session(self, client_key: str) -> tuple[Client, ClientConfig]:
        """
        Resuelve client_key → (Client, ClientConfig). La E/S de SQLite corre en un
        hilo (#240): un `database is locked` no congela el event loop.

        Raises:
            ClientNotFound: la key no existe.
            ClientInactive: el cliente está desactivado.
        """
        return await asyncio.to_thread(self._lookup, hash_key(client_key))

    async def touch(self, client_id: str, capabilities: list[str] | None) -> None:
        """El dispositivo acaba de entrar (M20): `last_seen_at` y, si las manda, sus capacidades."""
        await asyncio.to_thread(self._touch, client_id, capabilities)

    def _touch(self, client_id: str, capabilities: list[str] | None) -> None:
        values: dict[str, object] = {"last_seen_at": datetime.now(UTC)}
        if capabilities is not None:
            values["capabilities"] = json.dumps(capabilities)
        with Session(self._get_engine()) as session:
            session.exec(  # type: ignore[call-overload]
                update(ClientRecord).where(col(ClientRecord.id) == client_id).values(**values)
            )
            session.commit()

    async def notify_device(self, home_id: str, owner_user_id: str | None) -> str | None:
        """El dispositivo de avisos de una conversación de `home_id` (privada si trae
        `owner_user_id`), si vale ahora (`notify_device.notify_client_for`; 5.0 PR 2c.3). En un
        hilo, como el resto: nunca para el event loop."""
        return await asyncio.to_thread(self._notify_device, home_id, owner_user_id)

    def _notify_device(self, home_id: str, owner_user_id: str | None) -> str | None:
        with Session(self._get_engine()) as session:
            return notify_client_for(session, home_id, owner_user_id)

    async def revalidate(self, key_hash: str) -> tuple[Client, ClientConfig]:
        """Como `get_session`, desde el hash que ya guarda la sesión (la clave no se guarda)."""
        return await asyncio.to_thread(self._lookup, key_hash)

    def _lookup(self, key_hash: str) -> tuple[Client, ClientConfig]:
        with Session(self._get_engine()) as session:
            record: ClientRecord | None = session.exec(
                select(ClientRecord).where(ClientRecord.key_hash == key_hash)
            ).first()
            # Un dispositivo personal vale lo que su usuario (M20): desactivarlo corta sus
            # sesiones en la siguiente revalidación, también por /v1 y el WebSocket.
            user = session.get(UserRecord, record.user_id) if record and record.user_id else None
            membership = (
                session.get(MembershipRecord, (record.user_id, record.home_id))
                if record and record.user_id else None
            )
            household_agent = _household_agent(session, record) if record is not None else None

        # Sin argumentos: el mensaje de la excepción no lleva ni el hash.
        if record is None:
            raise ClientNotFound()
        if not record.is_active or (record.user_id is not None and (user is None or not user.is_active)):
            raise ClientInactive()

        client = Client(
            id=record.id,
            key_hash=record.key_hash,
            user_id=record.user_id,
            home_id=record.home_id,
            is_active=record.is_active,
            name=record.name,
            scopes=_effective_scopes(record, membership),
        )
        # `include` explícito: ClientConfig es extra="allow" y no debe recibir id/client_key.
        config = ClientConfig(
            **record.model_dump(include=_CONFIG_COLUMNS),
            allowed_agents=_parse_allowed_agents(record.allowed_agents),
            household_agent=household_agent,
        )
        return client, config


_CONFIG_COLUMNS = set(ClientConfig.model_fields) - {"allowed_agents", "household_agent"}


def _effective_scopes(record: ClientRecord, membership: MembershipRecord | None) -> frozenset[str]:
    """Lo concedido ∩ el tope de hoy (#410): el rol se lee en cada consulta, así que bajárselo a
    una persona recorta sus aparatos en la siguiente petición o revalidación. Un JSON roto no
    concede nada (nunca de más)."""
    try:
        granted = json.loads(record.scopes)
    except ValueError:
        granted = None
    if not isinstance(granted, list):
        logger.error("Cliente %s: scopes ilegibles; sin permisos.", record.id)
        return frozenset()
    role = membership.role if membership is not None else None
    return normalize_scopes(str(g) for g in granted) & scope_cap(record.user_id, role)


def _household_agent(session: Session, record: ClientRecord) -> str | None:
    """El agente que el hogar da a este dispositivo (M20): el de su usuario o, si no tiene o es
    del hogar, el de lo común."""
    if record.user_id is not None:
        membership = session.get(MembershipRecord, (record.user_id, record.home_id))
        if membership is not None and membership.agent:
            return membership.agent
    home = session.get(HomeRecord, record.home_id)
    return home.agent if home is not None else None

# Singleton — importar este objeto directamente
db_client = DbClient()
