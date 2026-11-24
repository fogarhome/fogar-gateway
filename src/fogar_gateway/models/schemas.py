from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, StringConstraints

from fogar_gateway.core.session_key import AGENT_ID_PATTERN

# Modos de salida conocidos. Compartido por el handshake WS y la API admin (#122).
OutputMode = Literal["audio", "text", "status"]

# =====================================================================
# Identidad y configuración del cliente (BD local, src/fogar_gateway/db)
# =====================================================================


class Client(BaseModel):
    """Registro de cliente resuelto desde la BD local."""

    id: str
    key_hash: str  # nunca la clave: solo su hash (M20), con el que se revalida la sesión
    is_active: bool
    name: str = ""
    user_id: str | None = None  # M20: None = dispositivo del hogar; con valor, personal
    home_id: str = "home"

    model_config = ConfigDict(extra="allow")


class ClientConfig(BaseModel):
    """Configuración por cliente leída desde la BD local."""

    stt_language: str = "es"
    stt_vad_thold: float = 0.0
    tts_voice: str = "af_heart"
    tts_speed: float = 1.0
    silence_timeout_s: float = 2.0
    push_enabled: bool = True
    tool_calls_enabled: bool = False
    # En minúsculas, como OpenClaw: una fila anterior a la 3.0 puede guardar "Main" (C-1).
    default_agent: Annotated[str, StringConstraints(to_lower=True)] | None = None
    allowed_agents: list[Annotated[str, StringConstraints(to_lower=True)]] | None = None
    # M20: el agente que el hogar asigna a quien habla por este dispositivo: el de su usuario si
    # es personal, el de lo común si es del hogar (`memberships.agent` / `homes.agent`).
    household_agent: Annotated[str, StringConstraints(to_lower=True)] | None = None

    model_config = ConfigDict(extra="allow")


# =====================================================================
# Gateway (Client <-> Gateway)
# =====================================================================


CREDENTIAL_MAX_LEN = 1024


# Una capacidad del dispositivo (M20): minúsculas, cifras, `_` y `-`; hasta 32 por dispositivo.
Capability = Annotated[str, StringConstraints(pattern=r"^[a-z][a-z0-9_-]{0,31}$")]
Capabilities = Annotated[list[Capability], Field(max_length=32)]


# Versión mayor del protocolo WebSocket cliente-gateway (docs/client-protocol.md). Sube solo si se
# quita algo o cambia lo que hace; lo nuevo se añade sin subirla.
PROTOCOL_VERSION: int = 5


class Handshake(BaseModel):
    """
    Configuración inicial que el cliente envía al Gateway al conectar por WebSocket. Define
    qué patas internas del ecosistema se deben encender.

    Credencial: `client_key`, la clave de un dispositivo emparejado o el token de uno con sesión
    (`/auth/login`, M20). La web entra igual que cualquier otro dispositivo: ya no hay `admin_token`.
    """

    # Con tope (como `conversation`): un valor enorme o un surrogate suelto ("\ud800") falla la
    # validación (cierre 4000) en vez de reventar al codificarlo (auditoría 3.0, D-5).
    client_key: Annotated[str, StringConstraints(max_length=CREDENTIAL_MAX_LEN)]
    # La versión la comprueba routes.py sobre el JSON crudo (cierre 4003) antes de validar.
    protocol: int | None = None
    input_mode: Literal["audio", "text"]
    output_mode: list[OutputMode]
    # Id de agente de OpenClaw, en minúsculas; None, "" o solo espacios → la cascada de
    # resolve_agent. Un id que OpenClaw no admite (`:`, espacios…) es 4000 (auditoría 3.0, C-2).
    agent: (
        Annotated[
            str,
            StringConstraints(strip_whitespace=True, to_lower=True, pattern=f"^$|{AGENT_ID_PATTERN}"),
        ]
        | None
    ) = None
    # Id de una conversación del cliente o "new"; None → la conversación por defecto (M19).
    conversation: Annotated[str, StringConstraints(min_length=1, max_length=64)] | None = None
    # Lo que el aparato sabe hacer (pantalla, micrófono…; M20): se guarda y se enseña, aún no
    # decide nada. Sin el campo, se conservan las que ya tenía.
    capabilities: Capabilities | None = None

    model_config = ConfigDict(extra="allow")


# =====================================================================
# Transcriber (Gateway <-> FogarTranscriber)
# =====================================================================


class TranscriberConfig(BaseModel):
    """
    Mensaje de handshake inicial requerido por el servidor de transcripción (C++)
    antes de aceptar recibir ráfagas de audio PCM float32.
    """

    type: Literal["config"] = "config"
    language: str = "es"
    token: str = ""
    vad_thold: float = 0.0


class TranscriberMessage(BaseModel):
    """
    Formato de salida proveniente del Transcriptor C++.
    Tipos posibles: "transcription", "ready", "warning", "error", "status".
    """

    type: str
    text: str | None = None
    is_final: bool | None = None
    message: str | None = None  # descripción legible en warning/error
    code: str | None = None  # código de error/warning: AUTH_FAILED, buffer_full, etc.
    session_id: str | None = None  # presente en el mensaje "ready"
    state: str | None = None  # "busy"/"ok" en mensajes type="status" (capacidad, #143)
    reason: str | None = None  # motivo en status busy / final con complete=false
    complete: bool | None = None  # False en un final truncado por saturación de GPU
