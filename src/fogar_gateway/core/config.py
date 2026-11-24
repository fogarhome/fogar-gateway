from typing import Annotated, Literal

from pydantic import Field, SecretStr, StringConstraints, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from fogar_gateway import paths
from fogar_gateway.access import parse_public_urls
from fogar_gateway.core.session_key import AGENT_ID_PATTERN


class Settings(BaseSettings):
    # Nunca el .env del directorio actual: <base>/config.env (o --home / FOGAR_GATEWAY_HOME).
    # Ningún plazo admite inf ni nan (D-r7-1): un HANDSHAKE_TIMEOUT_S=inf no caduca nunca.
    model_config = SettingsConfigDict(env_file=paths.config_file(), allow_inf_nan=False)

    # Base de datos interna. Una ruta SQLite relativa cuelga del directorio base, también el
    # default: el engine, Alembic, el candado y `fogar-gw doctor` ven la misma ruta absoluta.
    DATABASE_URL: str = Field(default="sqlite:///data/gateway.db", validate_default=True)

    @field_validator("DATABASE_URL")
    @classmethod
    def _absolute_database_url(cls, v: str) -> str:
        return paths.absolute_database_url(v)

    # Host de escucha de `fogar-gw serve` (puerto 8004). Detrás de un proxy en el mismo host,
    # 127.0.0.1. Vacío = 0.0.0.0, como el `${GATEWAY_HOST:-0.0.0.0}` del CMD anterior.
    GATEWAY_HOST: str = "0.0.0.0"

    # Nombre con el que se anuncia en /.well-known/fogar-gateway (#317). Vacío = nombre del equipo;
    # en Docker conviene fijarlo, porque ahí el nombre del equipo suele ser el id del contenedor.
    GATEWAY_NAME: str = ""

    # URLs https propias del usuario (CSV, sin ruta) por las que se llega al gateway: excepción
    # a «host:port sin protocolo». Solo se valida; se guarda el texto original.
    PUBLIC_URLS: str = ""

    @field_validator("PUBLIC_URLS")
    @classmethod
    def _valid_public_urls(cls, v: str) -> str:
        parse_public_urls(v)
        return v

    # Un solo proceso por base de datos (M6f, #218): flock sobre <dir de la BD>/gateway.lock.
    # Estado en memoria (sesiones, cachés, registro de bridges) no se comparte entre workers.
    SINGLE_PROCESS_LOCK: bool = True

    # Revocación de sesiones vivas (M6f, G-06): cada sesión se revalida contra la BD cada
    # SESSION_REVALIDATE_INTERVAL_S (db_client no cachea: #265).
    SESSION_REVALIDATE_INTERVAL_S: float = Field(default=10.0, gt=0)

    # Admin API
    ADMIN_TOKEN: str = ""

    # /v1/* trusted-origin auth (issue #52)
    TRUST_LOOPBACK: bool = True
    TRUSTED_NETWORKS: str = (
        ""  # CSV of CIDRs, e.g. "192.168.1.0/24". Empty = only loopback trusted.
    )
    TRUSTED_PROXIES: str = "127.0.0.1,::1"  # CSV of IPs/CIDRs allowed to set X-Real-IP

    # Servicios externos
    TRANSCRIBER_WS_URL: str = "localhost:9000"
    # Token de servicio para el transcriber (su AUTH_TOKEN). Nunca la client_key (#218).
    TRANSCRIBER_TOKEN: str = ""
    TTS_WS_URL: str = "localhost:8005"
    # Vacío = el servicio TTS no usa clave (como TRANSCRIBER_TOKEN).
    TTS_TOKEN: str = ""

    # Proveedores de voz (src/fogar_gateway/services/speech/). Se eligen al arrancar; cambiarlos = reiniciar.
    # "off" = como con el servicio caído: STT off, solo entra texto; TTS off, respuestas en texto.
    STT_PROVIDER: Literal["fogar", "elevenlabs", "off"] = "fogar"
    TTS_PROVIDER: Literal["fogar", "elevenlabs", "off"] = "fogar"
    ELEVENLABS_API_KEY: SecretStr | None = None
    ELEVENLABS_BASE_URL: str = "wss://api.elevenlabs.io"
    # Resto de ajustes de ElevenLabs: constantes en src/fogar_gateway/services/speech/elevenlabs/ (#264).
    ELEVENLABS_TTS_MODEL: str = "eleven_flash_v2_5"
    ELEVENLABS_TTS_DEFAULT_VOICE: str | None = "gc61tTk93h3LBXPxew2V"  # voz en español (no es secreto)
    # voz configurada en el cliente (p. ej. "af_heart") -> id de voz del proveedor (JSON)
    TTS_VOICE_MAP: dict[str, str] = {}
    # Puerta de energía hacia el STT de ElevenLabs (solo se envía audio mientras se habla).
    # fogar nunca la usa.
    STT_AUDIO_GATE: Literal["off", "energy"] = "energy"

    STT_GATE_RMS_DBFS: float = -45.0
    OPENCLAW_HOST: str = "127.0.0.1"
    OPENCLAW_PORT: int = 18789
    # wss:// en vez de ws:// (OpenClaw de otra máquina tras TLS, p. ej. su `tailscale serve`).
    OPENCLAW_TLS: bool = False
    OPENCLAW_TOKEN: str = ""
    # Agente por defecto de la instalación (#245): entra en la cascada de resolve_agent() entre
    # el `default_agent` del cliente y el agente por defecto de OpenClaw. Vacío = el de OpenClaw.
    # Misma regla que el `default_agent` de un cliente (admin_schemas.AgentName), la de un id de
    # agente de OpenClaw (AGENT_ID_PATTERN); vacío = sin agente de instalación.
    DEFAULT_AGENT: Annotated[
        str,
        StringConstraints(strip_whitespace=True, to_lower=True, pattern=f"^$|{AGENT_ID_PATTERN}"),
    ] = ""

    # Hora local del host (0-23) a la que empieza el día de las conversaciones por defecto (M19):
    # una por cliente, agente y día. Un turno que empieza tras el corte pasa a la del día nuevo.
    CONVERSATION_DAY_START_HOUR: int = Field(default=5, ge=0, le=23)
    ORCHESTRATOR_RECONNECT_INITIAL_BACKOFF: float = Field(default=1.0, gt=0)
    ORCHESTRATOR_RECONNECT_MAX_BACKOFF: float = Field(default=60.0, gt=0)
    TRANSCRIBER_RECONNECT_INITIAL_BACKOFF: float = Field(default=1.0, gt=0)
    TRANSCRIBER_RECONNECT_MAX_BACKOFF: float = Field(default=60.0, gt=0)
    TRANSCRIBER_RECONNECT_MAX_DURATION: float = Field(default=300.0, gt=0)
    TTS_RECONNECT_INITIAL_BACKOFF: float = Field(default=1.0, gt=0)
    TTS_RECONNECT_MAX_BACKOFF: float = Field(default=60.0, gt=0)
    TTS_AUTH_TIMEOUT_S: float = Field(default=10.0, gt=0)
    TRANSCRIBER_READY_TIMEOUT_S: float = Field(default=10.0, gt=0)

    # Deadlines acotados (issue #115)
    # 5 s (antes 10, #216): un handshake es un único mensaje pequeño; el margen largo solo
    # sirve a quien conecta y se queda callado, reteniendo la conexión sin autenticar.
    HANDSHAKE_TIMEOUT_S: float = Field(default=5.0, gt=0)
    # Tope (caracteres) del primer mensaje, comprobado antes de parsear/autenticar (#216).
    HANDSHAKE_MAX_BYTES: int = Field(default=4096, gt=0)
    TURN_TIMEOUT_S: float = Field(default=120.0, gt=0)
    IDLE_TIMEOUT_S: float = Field(default=300.0, gt=0)
    # Tope de cada paso del cierre de una sesión (cancelar el turno, el push, las tareas,
    # cerrar el transcriber y el tracker) y del drenado de su cola de salida (#221).
    SESSION_TEARDOWN_S: float = Field(default=5.0, gt=0)
    LOG_FORMAT: Literal["text", "json"] = "text"

    # Drenado del audio TTS de un push al cerrar su turno (issue #130 S3a): tope de tiempo
    # sin progreso del TTS (se reinicia con cada trozo), no duración total
    PUSH_TTS_DRAIN_TIMEOUT_S: float = Field(default=30.0, gt=0)

    # Pacing del audio TTS (7e): el gateway envía a ritmo de reproducción, como mucho
    # AUDIO_LEAD_S por delante
    AUDIO_PACING_ENABLED: bool = True
    AUDIO_LEAD_S: float = Field(default=2.0, gt=0)

    # Un envío al cliente bloqueado más de esto lo marca como lento y cierra la
    # conexión (issue #130 S2). Es por envío, no acumulado.
    CLIENT_SEND_TIMEOUT_S: float = Field(default=10.0, gt=0)

    # Eventos de OpenClaw que una conversación retiene mientras espera el ack de un
    # `send` o a que se cierre el turno abierto (issue #130 S3b, 5.0). Lo que pase del
    # tope se descarta (con un aviso en el log) y el turno sigue.
    TURN_QUEUE_MAX: int = Field(default=1000, gt=0)


settings = Settings()
