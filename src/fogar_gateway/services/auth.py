"""
auth.py
~~~~~~~
Credenciales de M20 (hogar con usuarios). Solo la biblioteca estándar.

- Claves de dispositivo: un token opaco por dispositivo (emparejado o con sesión). Se guarda solo
  su SHA-256 (`hash_key`): al ser 256 bits aleatorios basta un hash rápido, como las claves de
  API. Su huella de log (`fingerprint_key`) son los 8 primeros caracteres de ese mismo hash.
- Contraseñas: scrypt con sal propia (`hash_password` / `verify_password`).
- `LoginLimiter`: intentos fallidos por IP y por cuenta, con espera creciente.
- `login_transport_ok`: dónde se puede iniciar sesión sin HTTPS.
"""

import base64
import hashlib
import hmac
import ipaddress
import secrets
import time
from collections.abc import Callable

from starlette.requests import HTTPConnection

KEY_PREFIX = "fgr_"


def new_key() -> str:
    """Clave nueva de dispositivo: prefijo reconocible + 256 bits aleatorios."""
    return KEY_PREFIX + secrets.token_urlsafe(32)


def hash_key(key: str) -> str:
    """Lo único que se guarda de una clave de dispositivo."""
    return hashlib.sha256(key.encode("utf-8")).hexdigest()


# --- Contraseñas -------------------------------------------------------------------------

# n=2^14, r=8, p=1: el mínimo que recomienda OWASP para scrypt (~16 MiB, decenas de ms).
_SCRYPT_N, _SCRYPT_R, _SCRYPT_P = 2**14, 8, 1
_SCRYPT_MAXMEM = 64 * 1024 * 1024


def _b64(raw: bytes) -> str:
    return base64.b64encode(raw).decode("ascii")


def _scrypt(password: str, salt: bytes, n: int, r: int, p: int) -> bytes:
    return hashlib.scrypt(
        password.encode("utf-8"), salt=salt, n=n, r=r, p=p, maxmem=_SCRYPT_MAXMEM, dklen=32
    )


def hash_password(password: str) -> str:
    salt = secrets.token_bytes(16)
    digest = _scrypt(password, salt, _SCRYPT_N, _SCRYPT_R, _SCRYPT_P)
    return f"scrypt${_SCRYPT_N}${_SCRYPT_R}${_SCRYPT_P}${_b64(salt)}${_b64(digest)}"


def verify_password(password: str, stored: str) -> bool:
    """Tiempo constante en la comparación; un hash con otro formato nunca vale."""
    try:
        scheme, n, r, p, salt, digest = stored.split("$")
        if scheme != "scrypt":
            return False
        expected = base64.b64decode(digest)
        got = _scrypt(password, base64.b64decode(salt), int(n), int(r), int(p))
    except ValueError:
        return False
    return hmac.compare_digest(got, expected)


# Para que una cuenta que no existe tarde lo mismo que una contraseña mala.
DUMMY_PASSWORD_HASH = hash_password(secrets.token_urlsafe(16))


# --- Intentos de inicio de sesión ------------------------------------------------------------


class LoginLimiter:
    """Intentos fallidos por clave (`ip:<ip>` y `user:<nombre>`), en memoria como los códigos de
    emparejamiento. Tras `free` fallos, cada uno más dobla la espera (desde `base_s`, hasta
    `max_s`); un acierto borra la cuenta de esa clave. Se olvida al reiniciar."""

    def __init__(
        self,
        free: int = 5,
        base_s: float = 1.0,
        max_s: float = 300.0,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._free, self._base, self._max, self._clock = free, base_s, max_s, clock
        self._fails: dict[str, tuple[int, float]] = {}  # clave → (fallos, bloqueada hasta)

    def retry_after(self, *keys: str) -> float:
        """Segundos que faltan para poder intentarlo (0 = ya)."""
        now = self._clock()
        return max([0.0, *(self._fails.get(k, (0, 0.0))[1] - now for k in keys)])

    def failed(self, *keys: str) -> None:
        now = self._clock()
        for k in keys:
            count = self._fails.get(k, (0, 0.0))[0] + 1
            over = count - self._free
            wait = min(self._base * 2 ** (over - 1), self._max) if over > 0 else 0.0
            self._fails[k] = (count, now + wait)
        # shortcut: sin barrido periódico; una entrada por IP o nombre fallido. Basta para un
        # hogar; si /auth se expone a Internet sin proxy, barrer las viejas.

    def succeeded(self, *keys: str) -> None:
        for k in keys:
            self._fails.pop(k, None)


# --- Dónde se puede iniciar sesión sin HTTPS ---------------------------------------------------

# Cabeceras que solo pone un túnel con HTTPS delante (tailscale serve, cloudflared).
_HTTPS_TUNNEL_HEADERS = ("tailscale-user-login", "cf-connecting-ip")
# Las de cualquier proxy: con ellas, el par loopback no es necesariamente esta máquina.
_PROXY_HEADERS = ("x-forwarded-for", "x-real-ip", "forwarded")
# Tailscale: 100.64.0.0/10 y su ULA IPv6. El tráfico de la tailnet ya va cifrado (WireGuard).
_TAILNET = (ipaddress.ip_network("100.64.0.0/10"), ipaddress.ip_network("fd7a:115c:a1e0::/48"))


def login_transport_ok(connection: HTTPConnection) -> bool:
    """¿Puede viajar una contraseña por esta conexión? Por HTTPS siempre, también detrás de un
    túnel HTTPS en esta máquina. Por HTTP directo, solo desde esta máquina o la tailnet: en la LAN
    iría en claro.

    `serve` arranca uvicorn sin cabeceras de proxy, así que el esquema es el de la conexión al
    gateway y el par TCP es quien conecta de verdad, salvo un túnel en esta máquina (par
    loopback), que se reconoce por sus cabeceras de identidad."""
    if connection.url.scheme in ("https", "wss"):
        return True
    peer = connection.client.host if connection.client else ""
    try:
        ip = ipaddress.ip_address(peer)
    except ValueError:
        return False
    if ip.is_loopback:
        if any(h in connection.headers for h in _HTTPS_TUNNEL_HEADERS):
            return True  # túnel HTTPS en esta máquina
        if any(h in connection.headers for h in _PROXY_HEADERS):
            # Otro proxy en esta máquina: puede venir de la LAN por HTTP. Solo si dice HTTPS.
            return connection.headers.get("x-forwarded-proto", "").lower() == "https"
        return True  # alguien en esta misma máquina
    if any(h in connection.headers for h in _HTTPS_TUNNEL_HEADERS):
        return False  # esas cabeceras solo valen desde un túnel en esta máquina
    return any(ip in net for net in _TAILNET)
