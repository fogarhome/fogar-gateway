"""`fogar-gw users` (M20): las personas del hogar por /admin/users."""

import pytest
from click.testing import CliRunner

from fogar_gateway.cli import users
from fogar_gateway.cli.app import cli
from tests.fakes.cli_api import FakeApi

SITO = {"id": "u-sito", "username": "sito", "display_name": "Sito", "role": "owner",
        "is_active": True, "effective_agent": "main", "agent_shared": True}
ANA = {**SITO, "id": "u-ana", "username": "ana", "display_name": "Ana", "role": "member"}
LIST = {("GET", "/admin/users"): [SITO, ANA]}


def run(*args, input=None):
    return CliRunner().invoke(cli, list(args), input=input)


@pytest.fixture()
def no_tty(monkeypatch):
    monkeypatch.setattr(users, "_stdin_isatty", lambda: False)


def test_list_shows_roles_and_the_shared_agent_warning(use_api):
    use_api(FakeApi(LIST))
    r = run("users", "list")
    assert r.exit_code == 0, r.output
    assert "dueño" in r.output and "miembro" in r.output and "main *" in r.output
    assert "memoria es común" in r.output


def test_add_reads_the_password_from_stdin_never_from_an_argument(use_api, no_tty):
    fake = use_api(FakeApi({("POST", "/admin/users"): {**ANA, "role": "admin"}}))
    r = run("users", "add", "Ana", "--role", "admin", "--agent", "ana", input="contraseña larga\n")
    assert r.exit_code == 0, r.output
    assert fake.calls == [("POST", "/admin/users", {
        "username": "Ana", "display_name": "Ana", "role": "admin", "password": "contraseña larga",
        "agent": "ana"})]
    assert "contraseña larga" not in r.output


def test_add_without_a_password_is_an_error(use_api, no_tty):
    use_api(FakeApi({}))
    assert run("users", "add", "ana", input="").exit_code == 2


def test_reset_password_finds_by_username(use_api, no_tty):
    fake = use_api(FakeApi({**LIST, ("PUT", "/admin/users/u-sito/password"): None}))
    r = run("users", "reset-password", "SITO", input="nueva contraseña\n")
    assert r.exit_code == 0, r.output
    assert ("PUT", "/admin/users/u-sito/password", {"password": "nueva contraseña"}) in fake.calls


def test_edit_common_agent_sends_null(use_api):
    fake = use_api(FakeApi({**LIST, ("PATCH", "/admin/users/u-ana"): ANA}))
    r = run("users", "edit", "ana", "--common-agent")
    assert r.exit_code == 0, r.output
    assert ("PATCH", "/admin/users/u-ana", {"agent": None}) in fake.calls
    assert run("users", "edit", "ana").exit_code == 2  # nada que cambiar


def test_remove_asks_first_and_unknown_user_fails(use_api):
    fake = use_api(FakeApi({**LIST, ("DELETE", "/admin/users/u-ana"): None}))
    r = run("--yes", "users", "remove", "ana")
    assert r.exit_code == 0, r.output
    assert ("DELETE", "/admin/users/u-ana", None) in fake.calls
    assert run("users", "remove", "nadie").exit_code == 1


def test_stable_codes_are_explained(use_api):
    from fogar_gateway.cli import api

    assert "último dueño" in api.CODES["last_owner"]
