"""services/auth.py (M20): claves con hash, contraseñas, intentos y dónde se puede iniciar sesión."""

import pytest
from starlette.requests import Request

from fogar_gateway.core.logging import fingerprint_key
from fogar_gateway.services.auth import (
    KEY_PREFIX,
    LoginLimiter,
    hash_key,
    hash_password,
    login_transport_ok,
    new_key,
    verify_password,
)


def test_keys_are_prefixed_random_and_stored_only_as_their_hash():
    a, b = new_key(), new_key()
    assert a != b and a.startswith(KEY_PREFIX) and len(a) > 40
    assert hash_key(a) != a and len(hash_key(a)) == 64
    # La huella de log es el principio del hash guardado: /admin la saca sin conocer la clave.
    assert hash_key(a)[:8] == fingerprint_key(a)


def test_password_round_trip_and_salt():
    stored = hash_password("una frase larga")
    assert stored.startswith("scrypt$") and "una frase larga" not in stored
    assert verify_password("una frase larga", stored)
    assert not verify_password("otra", stored)
    assert hash_password("una frase larga") != stored  # sal propia


@pytest.mark.parametrize("stored", ["", "texto", "bcrypt$1$2$3$4$5", "scrypt$x$8$1$AA==$AA=="])
def test_a_malformed_hash_never_matches(stored):
    assert not verify_password("loquesea", stored)


def test_limiter_waits_after_free_attempts_and_grows():
    now = [0.0]
    lim = LoginLimiter(free=2, base_s=1.0, max_s=4.0, clock=lambda: now[0])
    keys = ("ip:1.2.3.4", "user:sito")
    lim.failed(*keys)
    lim.failed(*keys)
    assert lim.retry_after(*keys) == 0.0  # los gratuitos
    lim.failed(*keys)
    assert lim.retry_after(*keys) == 1.0
    lim.failed(*keys)
    assert lim.retry_after(*keys) == 2.0
    for _ in range(5):
        lim.failed(*keys)
    assert lim.retry_after(*keys) == 4.0  # tope
    assert lim.retry_after("ip:otra") == 0.0  # cada clave por su lado
    now[0] = 10.0
    assert lim.retry_after(*keys) == 0.0
    lim.succeeded(*keys)
    lim.failed(*keys)
    assert lim.retry_after(*keys) == 0.0  # un acierto empieza de cero


def test_the_slowest_key_wins():
    lim = LoginLimiter(free=0, base_s=1.0, clock=lambda: 0.0)
    lim.failed("user:sito")
    assert lim.retry_after("ip:9.9.9.9", "user:sito") == 1.0


def _conn(peer: str, scheme: str = "http", headers: dict[str, str] | None = None) -> Request:
    return Request({
        "type": "http",
        "scheme": scheme,
        "method": "POST",
        "path": "/auth/login",
        "server": ("gateway", 8004),
        "client": (peer, 50000),
        "headers": [(k.lower().encode(), v.encode()) for k, v in (headers or {}).items()],
    })


@pytest.mark.parametrize(
    ("peer", "scheme", "headers", "ok"),
    [
        ("192.168.1.30", "https", {}, True),  # HTTPS directo, desde donde sea
        ("127.0.0.1", "http", {}, True),  # esta máquina
        ("::1", "http", {}, True),
        ("100.101.102.103", "http", {}, True),  # tailnet (WireGuard)
        ("fd7a:115c:a1e0::1", "http", {}, True),
        ("192.168.1.30", "http", {}, False),  # LAN por HTTP: en claro
        ("8.8.8.8", "http", {}, False),
        # tailscale serve / cloudflared en esta máquina: HTTPS hasta el túnel
        ("127.0.0.1", "http", {"Tailscale-User-Login": "sito@x"}, True),
        ("127.0.0.1", "http", {"Cf-Connecting-Ip": "8.8.8.8"}, True),
        # cabeceras de túnel desde fuera: nadie las pone de verdad, no valen
        ("192.168.1.30", "http", {"Tailscale-User-Login": "sito@x"}, False),
        # otro proxy en esta máquina: solo si dice HTTPS
        ("127.0.0.1", "http", {"X-Forwarded-For": "192.168.1.30"}, False),
        ("127.0.0.1", "http", {"X-Real-IP": "192.168.1.30"}, False),
        ("127.0.0.1", "http", {"X-Forwarded-For": "1.2.3.4", "X-Forwarded-Proto": "https"}, True),
        ("no-es-una-ip", "http", {}, False),
    ],
)
def test_login_transport(peer, scheme, headers, ok):
    assert login_transport_ok(_conn(peer, scheme, headers)) is ok
