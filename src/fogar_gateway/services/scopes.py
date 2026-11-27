"""
scopes.py
~~~~~~~~~
Permisos por aparato (5.0, #410). Solo stdlib: lo usan la BD, los schemas y las rutas.

Cada aparato guarda los scopes que se le concedieron (`clients.scopes`, expandidos: `write`
incluye `read` y `admin` incluye `read`). Lo que vale en cada petición es lo guardado ∩ el tope de
hoy (`scope_cap`): el rol de su persona o, en un aparato del hogar, como mucho `home:read`. Así,
bajar el rol a alguien recorta sus aparatos sin que inicien sesión otra vez.
"""

from collections.abc import Iterable
from typing import Final, Literal

Scope = Literal["conversations:read", "conversations:write", "home:read", "home:admin"]

CONVERSATIONS_READ: Final = "conversations:read"
CONVERSATIONS_WRITE: Final = "conversations:write"
HOME_READ: Final = "home:read"
HOME_ADMIN: Final = "home:admin"

ALL_SCOPES = frozenset({CONVERSATIONS_READ, CONVERSATIONS_WRITE, HOME_READ, HOME_ADMIN})
_IMPLIES = {CONVERSATIONS_WRITE: CONVERSATIONS_READ, HOME_ADMIN: HOME_READ}

# Lo que se concede si nadie pide nada (login sin `scopes`, plantilla o alta sin `scopes`).
DEFAULT_SCOPES = frozenset({CONVERSATIONS_READ, CONVERSATIONS_WRITE})
# `home:admin` exige una persona detrás: un aparato del hogar (y su `/pair` público) nunca lo tiene.
HOUSEHOLD_CAP = frozenset({CONVERSATIONS_READ, CONVERSATIONS_WRITE, HOME_READ})
_ROLE_CAP = {
    "owner": ALL_SCOPES,
    "admin": ALL_SCOPES,
    "member": DEFAULT_SCOPES,
}

SCOPE_NOT_ALLOWED = "scope_not_allowed"


def normalize_scopes(scopes: Iterable[str]) -> frozenset[str]:
    """Los scopes con lo que incluyen (`write` → `read`, `admin` → `read`); los desconocidos fuera."""
    out = {s for s in scopes if s in ALL_SCOPES}
    out |= {_IMPLIES[s] for s in out if s in _IMPLIES}
    return frozenset(out)


def scope_cap(user_id: str | None, role: str | None) -> frozenset[str]:
    """El tope de un aparato: del hogar (`user_id` None), `HOUSEHOLD_CAP`; personal, el de su rol
    (sin rol conocido, nada)."""
    if user_id is None:
        return HOUSEHOLD_CAP
    return _ROLE_CAP.get(role or "", frozenset())


def to_list(scopes: Iterable[str]) -> list[str]:
    """Orden estable para guardar y enseñar."""
    return sorted(scopes)
