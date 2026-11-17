# Un id de agente válido para OpenClaw (`VALID_ID_RE` de su `agent-id`, 2026.10.5): empieza por
# letra o cifra, luego letras, cifras, `_` o `-`, hasta 64; sin `:` ni espacios, que romperían
# la clave `agent:{agente}:…`. OpenClaw lo pasa a minúsculas, y la clave entera también
# (`normalizeSessionKeyPreservingOpaquePeerIds`, 2026.10.5): el gateway los usa ya en minúsculas,
# o los eventos de OpenClaw no casarían con la clave que espera (auditoría 3.0, C-1).
AGENT_ID_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$"


def make_session_key(agent: str, client_id: str) -> str:
    return f"agent:{agent}:{client_id}".lower()
