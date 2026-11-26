"""Permisos por aparato (#410): lo que incluye cada scope y el tope de cada aparato."""

from fogar_gateway.services.scopes import (
    ALL_SCOPES,
    DEFAULT_SCOPES,
    HOUSEHOLD_CAP,
    normalize_scopes,
    scope_cap,
)


def test_write_and_admin_include_their_read():
    assert normalize_scopes(["conversations:write"]) == DEFAULT_SCOPES
    assert normalize_scopes(["home:admin"]) == {"home:admin", "home:read"}


def test_unknown_scopes_are_dropped():
    assert normalize_scopes(["home:root", "conversations:read"]) == {"conversations:read"}


def test_caps():
    assert scope_cap(None, None) == HOUSEHOLD_CAP  # del hogar: nunca home:admin
    assert "home:admin" not in HOUSEHOLD_CAP
    assert scope_cap("u", "owner") == scope_cap("u", "admin") == ALL_SCOPES
    assert scope_cap("u", "member") == DEFAULT_SCOPES
    assert scope_cap("u", None) == frozenset()  # sin membresía, nada
