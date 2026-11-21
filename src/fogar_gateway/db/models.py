import uuid
from datetime import UTC, date, datetime

from sqlalchemy import DDL, ForeignKeyConstraint, Index, UniqueConstraint, event
from sqlmodel import Field, SQLModel


def _gen_uuid() -> str:
    return str(uuid.uuid4())


# shortcut: un solo hogar por gateway (M20); su id es fijo y es el valor por defecto de
# `home_id`. Cuando haya varios hogares (Cloud), cada alta tendrá que decir el suyo.
DEFAULT_HOME_ID = "home"
DEFAULT_HOME_NAME = "Mi hogar"


class HomeRecord(SQLModel, table=True):
    """El hogar (M20). `agent`: el que atiende lo común; NULL = la cascada de siempre."""

    __tablename__ = "homes"

    id: str = Field(default_factory=_gen_uuid, primary_key=True)
    name: str
    agent: str | None = Field(default=None)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class UserRecord(SQLModel, table=True):
    """Un usuario (M20). Global: no vive dentro del hogar, pertenece a hogares por
    `memberships`."""

    __tablename__ = "users"

    id: str = Field(default_factory=_gen_uuid, primary_key=True)
    username: str = Field(unique=True, index=True)
    display_name: str
    password_hash: str
    is_active: bool = Field(default=True)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class MembershipRecord(SQLModel, table=True):
    """Usuario ↔ hogar con su rol (`owner`, `admin`, `member`) y su agente (NULL = el del
    hogar)."""

    __tablename__ = "memberships"

    user_id: str = Field(foreign_key="users.id", primary_key=True, ondelete="CASCADE")
    home_id: str = Field(foreign_key="homes.id", primary_key=True, ondelete="CASCADE")
    role: str
    agent: str | None = Field(default=None)


class ClientRecord(SQLModel, table=True):
    """Un dispositivo. `user_id` NULL = del hogar (emparejado); con valor = personal (un inicio
    de sesión de ese usuario). Retirar no borra (`retired_at`): sus conversaciones conservan
    siempre hogar y dispositivo (M20)."""

    __tablename__ = "clients"
    # Destino de la clave foránea compuesta de `conversations`.
    __table_args__ = (UniqueConstraint("id", "home_id", name="uq_clients_id_home"),)

    id: str = Field(default_factory=_gen_uuid, primary_key=True)
    home_id: str = Field(default=DEFAULT_HOME_ID, foreign_key="homes.id", index=True)
    user_id: str | None = Field(
        default=None, foreign_key="users.id", index=True, ondelete="CASCADE"
    )
    name: str
    client_key: str = Field(unique=True, index=True)
    is_active: bool = Field(default=True)
    client_type: str | None = Field(default=None)
    default_agent: str | None = Field(default=None)
    allowed_agents: str | None = Field(default=None)  # JSON list serializado
    capabilities: str | None = Field(default=None)  # JSON list serializado
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    last_seen_at: datetime | None = Field(default=None)
    retired_at: datetime | None = Field(default=None)

    # Pipeline config
    stt_language: str = Field(default="es")
    stt_vad_thold: float = Field(default=0.0)
    tts_voice: str = Field(default="af_heart")
    tts_speed: float = Field(default=1.0)
    barge_in_enabled: bool = Field(default=True)
    barge_in_min_chars: int = Field(default=5)
    output_mode: str | None = Field(default=None)  # JSON list, e.g. '["audio","text"]'
    silence_timeout_s: float = Field(default=2.0)
    max_silence_turns: int = Field(default=3)
    push_enabled: bool = Field(default=True)
    tool_calls_enabled: bool = Field(default=False)


class ConversationRecord(SQLModel, table=True):
    """Una conversación (M19): el índice de una sesión de OpenClaw. Sin texto: lo dicho vive en
    OpenClaw (`chat.history`) y el título también se lee de allí.

    M20: siempre con hogar y dispositivo (el que la creó). `owner_user_id` NULL = del hogar.
    La clave compuesta impide que quede en otro hogar que su dispositivo; sin ON DELETE, un
    dispositivo con conversaciones no se borra (se retira). Borrar al usuario borra las suyas."""

    __tablename__ = "conversations"
    __table_args__ = (
        # Una conversación por defecto por dispositivo, agente y día. `day` es NULL en las demás,
        # y en SQLite los NULL no chocan entre sí en un índice único.
        Index("uq_conversations_default_day", "client_id", "agent", "day", unique=True),
        ForeignKeyConstraint(
            ["client_id", "home_id"],
            ["clients.id", "clients.home_id"],
            name="fk_conversations_client_home",
        ),
    )

    id: str = Field(default_factory=_gen_uuid, primary_key=True)
    home_id: str = Field(default=DEFAULT_HOME_ID, foreign_key="homes.id", index=True)
    client_id: str = Field(index=True)
    owner_user_id: str | None = Field(
        default=None, foreign_key="users.id", index=True, ondelete="CASCADE"
    )
    agent: str
    session_key: str = Field(unique=True, index=True)
    kind: str  # default | created | legacy
    day: date | None = Field(default=None)  # solo `default`
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    last_turn_at: datetime | None = Field(default=None)
    turn_count: int = Field(default=0)


# El hogar existe siempre: lo crea la migración inicial y, para `create_all` (tests), este DDL.
event.listen(
    HomeRecord.__table__,  # type: ignore[attr-defined]
    "after_create",
    DDL(f"INSERT INTO homes (id, name, created_at) VALUES "
        f"('{DEFAULT_HOME_ID}', '{DEFAULT_HOME_NAME}', CURRENT_TIMESTAMP)"),
)
