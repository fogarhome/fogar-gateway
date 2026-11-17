"""Servicio de usuario de systemd: unidad y elección del ejecutable (M14d, #296)."""

import json
import sys
from pathlib import Path

import pytest

from fogar_gateway.cli import api, service
from fogar_gateway.cli import serve as serve_mod

UNIT = """[Unit]
Description=Fogar gateway
StartLimitIntervalSec=60
StartLimitBurst=5

[Service]
Type=exec
ExecStart=/home/u/.local/bin/fogar-gw serve
Environment=FOGAR_GATEWAY_HOME=/home/u/.fogar-gateway
Restart=on-failure
RestartSec=5
RestartPreventExitStatus=3
TimeoutStopSec=45
UMask=0077

[Install]
WantedBy=default.target
"""


def test_unit_text_exact():
    text = service.render_unit(Path("/home/u/.local/bin/fogar-gw"), Path("/home/u/.fogar-gateway"))
    assert text == UNIT
    assert "--create-db" not in text and "LOG_FORMAT" not in text


def test_unit_quotes_and_escapes_odd_paths():
    # Espacio, especificador %, variable $, comilla y barra: la unidad no debe partir ni expandir.
    odd = 'a b%c$d"e\\f'
    text = service.render_unit(Path(f"/x/{odd}/fogar-gw"), Path(f"/h/{odd}"))
    # En ExecStart, systemd expande % y $ (se doblan); en Environment= solo % ($ es literal).
    assert 'ExecStart="/x/a b%%c$$d\\"e\\\\f/fogar-gw" serve\n' in text
    assert 'Environment="FOGAR_GATEWAY_HOME=/h/a b%%c$d\\"e\\\\f"\n' in text


def test_pick_exe_makes_relative_exe_absolute_without_resolving(monkeypatch, tmp_path):
    (tmp_path / "otro").mkdir()
    monkeypatch.chdir(tmp_path / "otro")
    # systemd rechaza una ruta con `..`: se normaliza sin resolver symlinks.
    exe = service.pick_exe(Path("../.venv/bin/fogar-gw"))
    assert exe == tmp_path / ".venv/bin/fogar-gw" and exe.is_absolute()


def test_pick_exe_bare_name_is_looked_up_on_path(monkeypatch, tmp_path):
    exe = tmp_path / "fogar-gw"
    exe.write_text("")
    monkeypatch.setattr(service.shutil, "which", lambda name: str(exe) if str(name) == "fogar-gw" else None)
    monkeypatch.chdir(tmp_path.parent)
    assert service.pick_exe(Path("fogar-gw")) == exe


def test_pick_exe_bare_name_not_on_path_fails(monkeypatch):
    monkeypatch.setattr(service.shutil, "which", lambda name: None)
    with pytest.raises(api.ApiError, match="No encuentro"):
        service.pick_exe(Path("fogar-gw"))


def test_stop_timeout_matches_compose_and_exceeds_graceful_shutdown():
    root = Path(__file__).resolve().parents[2]
    compose = (root / "docker-compose.yml").read_text()
    assert "stop_grace_period: 45s" in compose
    assert "TimeoutStopSec=45" in UNIT
    assert serve_mod.GRACEFUL_SHUTDOWN_S < 45


def test_unit_path_respects_xdg(monkeypatch, tmp_path):
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path))
    assert service.unit_path() == tmp_path / "systemd/user/fogar-gateway.service"
    monkeypatch.delenv("XDG_CONFIG_HOME")
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    assert service.unit_path() == tmp_path / ".config/systemd/user/fogar-gateway.service"


def _tool_env(base: Path, *, editable: bool | None = None) -> Path:
    """Crea `<base>/bin/fogar-gw` con su site-packages; devuelve el ejecutable."""
    exe = base / "bin" / "fogar-gw"
    exe.parent.mkdir(parents=True)
    exe.write_text("#!/bin/sh\n")
    if editable is not None:
        dist = base / "lib/python3.12/site-packages/fogar_gateway-0.1.dist-info"
        dist.mkdir(parents=True)
        (dist / "direct_url.json").write_text(
            json.dumps({"url": "file:///x", "dir_info": {"editable": editable}})
        )
    return exe


def test_pick_exe_accepts_uv_tool_and_keeps_symlink(monkeypatch, tmp_path):
    real = _tool_env(tmp_path / "tools/fogar-gateway", editable=False)
    link = tmp_path / "local/bin/fogar-gw"
    link.parent.mkdir(parents=True)
    link.symlink_to(real)
    monkeypatch.setattr(service.shutil, "which", lambda name: str(link))
    assert service.pick_exe(None) == link  # sin resolver: sobrevive a `uv tool upgrade`


@pytest.mark.parametrize("kind", ["pyproject", "editable"])
def test_exe_inside_checkout_is_rejected(monkeypatch, tmp_path, kind):
    if kind == "pyproject":
        (tmp_path / "clone").mkdir()
        (tmp_path / "clone/pyproject.toml").write_text("")
        exe = _tool_env(tmp_path / "clone/.venv")
    else:
        exe = _tool_env(tmp_path / "venv", editable=True)
    monkeypatch.setattr(service.shutil, "which", lambda name: str(exe))
    with pytest.raises(api.ApiError, match="--exe") as e:
        service.pick_exe(None)
    assert e.value.exit_code == 1
    assert service.pick_exe(exe) == exe  # explícito: se acepta


def test_pick_exe_without_fogar_gw_fails(monkeypatch):
    monkeypatch.setattr(service.shutil, "which", lambda name: None)
    with pytest.raises(api.ApiError, match="--exe") as e:
        service.pick_exe(None)
    assert e.value.exit_code == 1


def test_require_systemd_off_linux(monkeypatch):
    monkeypatch.setattr(sys, "platform", "darwin")
    with pytest.raises(api.ApiError, match="docker compose") as e:
        service.require_systemd()
    assert e.value.exit_code == 1


def test_require_systemd_without_systemctl(monkeypatch):
    monkeypatch.setattr(sys, "platform", "linux")
    monkeypatch.setattr(service.shutil, "which", lambda name: None)
    with pytest.raises(api.ApiError, match="docker compose"):
        service.require_systemd()
    monkeypatch.setattr(service.shutil, "which", lambda name: "/usr/bin/systemctl")
    service.require_systemd()


def test_systemctl_runs_user_instance(monkeypatch):
    import subprocess

    calls = []

    def fake_run(cmd, **kw):
        calls.append((cmd, kw))
        return subprocess.CompletedProcess(cmd, 0, "", "")

    monkeypatch.setattr(subprocess, "run", fake_run)
    service.systemctl("start", "fogar-gateway.service", check=False)
    cmd, kw = calls[0]
    assert cmd == ["systemctl", "--user", "start", "fogar-gateway.service"]
    assert kw["check"] is False and kw["text"] is True and kw["capture_output"] is True


def test_pick_exe_ignores_distant_pyproject(monkeypatch, tmp_path):
    (tmp_path / "pyproject.toml").write_text("")  # suelto en un ancestro lejano (~)
    exe = _tool_env(tmp_path / ".local/share/uv/tools/fogar-gateway", editable=False)
    monkeypatch.setattr(service.shutil, "which", lambda name: str(exe))
    assert service.pick_exe(None) == exe


def test_importing_service_does_not_load_subprocess():
    import subprocess

    code = "import sys; import fogar_gateway.cli.service; print('subprocess' in sys.modules)"
    r = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True)
    assert r.stdout.strip() == "False"


# --- install / uninstall / start / stop / restart (M14d, B2 y B3) ---

import shutil  # noqa: E402
import socket  # noqa: E402
import subprocess  # noqa: E402
from types import SimpleNamespace  # noqa: E402

from click.testing import CliRunner  # noqa: E402

from fogar_gateway.cli import config as cfg  # noqa: E402
from fogar_gateway.cli import output  # noqa: E402
from fogar_gateway.cli.app import cli  # noqa: E402

EXE = "/home/u/.local/bin/fogar-gw"
REAL_PORT_FREE = getattr(service, "port_free", None)  # antes de que la fixture lo mockee


@pytest.fixture()
def svc(monkeypatch, tmp_path):
    """Todo mockeado: sin systemd, sin red, sin loginctl. `calls` recoge los `systemctl`."""
    home = tmp_path / "home"
    (home / "data").mkdir(parents=True)
    (home / "data" / "gateway.db").write_text("db")
    (home / "config.env").write_text("ADMIN_TOKEN=x\n")
    monkeypatch.setenv("FOGAR_GATEWAY_HOME", str(home))
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "xdg"))
    monkeypatch.setattr(cfg, "load", lambda: (SimpleNamespace(
        DATABASE_URL=f"sqlite:///{home / 'data' / 'gateway.db'}"), [], {}))
    monkeypatch.setattr(service, "require_systemd", lambda: None)
    monkeypatch.setattr(service, "port_free", lambda: True)
    monkeypatch.setattr(service, "linger_enabled", lambda user: True)
    monkeypatch.setattr(service, "_probe", lambda url: True)
    monkeypatch.setattr(service, "DOCKERENV", tmp_path / "no-dockerenv")
    monkeypatch.setattr(output, "_stdin_isatty", lambda: False)
    calls: list[tuple[str, ...]] = []

    state = SimpleNamespace(home=home, calls=calls, unit=service.unit_path(), tmp=tmp_path,
                            active=False)

    def fake(*args, check=True):
        if args[0] == "is-active":  # no cuenta como acción: solo sondea
            return subprocess.CompletedProcess(args, 0 if state.active else 3, "", "")
        calls.append(args)
        return subprocess.CompletedProcess(args, 0, "", "")

    monkeypatch.setattr(service, "systemctl", fake)
    return state


def run(*args, **kw):
    return CliRunner().invoke(cli, list(args), **kw)


def _unit_text(svc):
    return service.render_unit(Path(EXE), svc.home)


def test_install_refuses_without_db(svc):
    (svc.home / "data" / "gateway.db").unlink()
    r = run("install", "--exe", EXE)
    assert r.exit_code == 1
    assert "No hay BD en" in r.output and "fogar-gw init" in r.output
    assert "migrate --from-docker" in r.output
    assert not svc.unit.exists() and svc.calls == []


def test_install_refuses_with_invalid_config(svc, monkeypatch):
    monkeypatch.setattr(cfg, "load", lambda: (None, ["PORT: int_parsing"], {}))
    r = run("install", "--exe", EXE)
    assert r.exit_code == 1
    assert "PORT: int_parsing" in r.output
    assert not svc.unit.exists() and svc.calls == []


def test_install_refuses_when_port_busy(monkeypatch):
    with socket.socket() as srv:
        srv.bind(("127.0.0.1", 0))
        srv.listen()
        monkeypatch.setattr(serve_mod, "PORT", srv.getsockname()[1])
        assert REAL_PORT_FREE() is False  # con SO_REUSEADDR sigue viendo un listener real
    assert REAL_PORT_FREE() is True  # libre al cerrar el que escucha


def test_install_stops_when_port_busy(svc, monkeypatch):
    monkeypatch.setattr(service, "port_free", lambda: False)
    r = run("install", "--exe", EXE)
    assert r.exit_code == 1
    assert "otro gateway" in r.output and "contenedor" in r.output
    assert not svc.unit.exists() and svc.calls == []


def test_install_active_service_same_unit_skips_port_check(svc, monkeypatch):
    svc.unit.parent.mkdir(parents=True)
    svc.unit.write_text(_unit_text(svc))
    svc.active = True
    monkeypatch.setattr(service, "port_free", lambda: False)  # el 8004 es nuestro
    before = svc.unit.stat().st_mtime_ns
    r = run("--json", "install", "--exe", EXE)
    assert r.exit_code == 0, r.output
    assert svc.unit.stat().st_mtime_ns == before
    assert svc.calls == [("enable", "--now", "fogar-gateway.service")]


def test_install_active_service_new_unit_restarts(svc, monkeypatch):
    svc.unit.parent.mkdir(parents=True)
    svc.unit.write_text("viejo")
    svc.active = True
    monkeypatch.setattr(service, "port_free", lambda: False)
    r = run("--yes", "install", "--exe", EXE)
    assert r.exit_code == 0, r.output
    assert svc.unit.read_text() == _unit_text(svc)
    assert svc.calls == [("daemon-reload",), ("enable", "--now", "fogar-gateway.service"),
                         ("restart", "fogar-gateway.service")]


def test_install_inactive_existing_unit_and_port_busy_refuses(svc, monkeypatch):
    svc.unit.parent.mkdir(parents=True)
    svc.unit.write_text(_unit_text(svc))
    before = svc.unit.read_bytes()
    svc.active = False
    monkeypatch.setattr(service, "port_free", lambda: False)
    r = run("--yes", "install", "--exe", EXE)
    assert r.exit_code == 1 and "ocupado" in r.output
    assert svc.unit.read_bytes() == before and svc.calls == []


def test_install_without_unit_and_port_busy_still_refuses(svc, monkeypatch):
    svc.active = True  # da igual: sin unidad no se sondea
    monkeypatch.setattr(service, "port_free", lambda: False)
    assert run("install", "--exe", EXE).exit_code == 1
    assert svc.calls == []


def test_install_writes_unit_and_enables(svc):
    r = run("--json", "install", "--exe", EXE)
    assert r.exit_code == 0, r.output
    assert svc.unit.read_text() == _unit_text(svc)
    assert svc.calls == [("daemon-reload",), ("enable", "--now", "fogar-gateway.service")]
    assert json.loads(r.stdout) == {"unit": str(svc.unit), "exe": EXE, "home": str(svc.home),
                                    "state": "instalado"}
    assert not list(svc.unit.parent.glob("*.tmp*"))  # escritura atómica sin restos


def test_install_same_unit_is_noop(svc):
    svc.unit.parent.mkdir(parents=True)
    svc.unit.write_text(_unit_text(svc))
    before = svc.unit.stat().st_mtime_ns
    r = run("--json", "install", "--exe", EXE)
    assert r.exit_code == 0, r.output
    assert svc.unit.stat().st_mtime_ns == before
    assert svc.calls == [("enable", "--now", "fogar-gateway.service")]
    assert json.loads(r.stdout)["state"] == "sin cambios"


def test_install_overwrite_needs_confirm(svc):
    svc.unit.parent.mkdir(parents=True)
    svc.unit.write_text("viejo")
    r = run("install", "--exe", EXE)
    assert r.exit_code == 2 and "--yes" in r.output
    assert svc.unit.read_text() == "viejo" and svc.calls == []
    r = run("--yes", "install", "--exe", EXE)
    assert r.exit_code == 0, r.output
    assert svc.unit.read_text() == _unit_text(svc)


def test_install_warns_without_linger(svc, monkeypatch):
    monkeypatch.setenv("USER", "sito")
    monkeypatch.setattr(service, "linger_enabled", lambda user: False)
    r = run("install", "--exe", EXE)
    assert r.exit_code == 0, r.output
    assert "loginctl enable-linger sito" in r.output


def test_install_fails_with_hint_when_not_healthy(svc, monkeypatch):
    monkeypatch.setattr(service, "_probe", lambda url: False)
    monkeypatch.setattr(service, "HEALTH_TIMEOUT_S", 0.05)
    monkeypatch.setattr(service.time, "sleep", lambda s: None)
    r = run("install", "--exe", EXE)
    assert r.exit_code == 1 and "fogar-gw logs -n 50" in r.output


def test_health_wait_outlasts_openclaw_startup_retry():
    # El lifespan espera el primer connect (que reintenta mientras OpenClaw arranca) antes de abrir el 8004.
    from fogar_gateway.services.openclaw.client import _STARTUP_RETRY_BUDGET_S

    assert service.HEALTH_TIMEOUT_S > _STARTUP_RETRY_BUDGET_S


def test_wait_healthy_retries_until_ok(svc, monkeypatch):
    answers = iter([False, False, True])
    monkeypatch.setattr(service, "_probe", lambda url: next(answers))
    monkeypatch.setattr(service.time, "sleep", lambda s: None)
    service.wait_healthy("http://127.0.0.1:8004")


def test_uninstall_keeps_home(svc):
    svc.unit.parent.mkdir(parents=True)
    svc.unit.write_text(_unit_text(svc))
    r = run("--yes", "--json", "uninstall")
    assert r.exit_code == 0, r.output
    assert not svc.unit.exists()
    assert svc.calls == [("disable", "--now", "fogar-gateway.service"), ("daemon-reload",)]
    assert (svc.home / "data" / "gateway.db").read_text() == "db"
    assert (svc.home / "config.env").read_text() == "ADMIN_TOKEN=x\n"
    assert json.loads(r.stdout) == {"unit": str(svc.unit), "exe": None, "home": str(svc.home),
                                    "state": "desinstalado"}
    r = run("--yes", "uninstall")  # sin unidad: 0 y lo dice
    assert r.exit_code == 0 and "no está instalado" in r.output


def test_uninstall_says_where_data_stays(svc):
    svc.unit.parent.mkdir(parents=True)
    svc.unit.write_text(_unit_text(svc))
    r = run("--yes", "uninstall")
    assert str(svc.home) in r.output


def test_uninstall_needs_confirm_without_tty(svc):
    svc.unit.parent.mkdir(parents=True)
    svc.unit.write_text(_unit_text(svc))
    r = run("uninstall")
    assert r.exit_code == 2 and "--yes" in r.output
    assert svc.unit.exists() and svc.calls == []


@pytest.fixture()
def installed(svc):
    svc.unit.parent.mkdir(parents=True)
    svc.unit.write_text(_unit_text(svc))
    return svc


def test_start_stop_restart_call_systemctl(installed, monkeypatch):
    waits = []
    monkeypatch.setattr(service, "_probe", lambda url: waits.append(url) or True)
    assert run("start").exit_code == 0
    assert run("stop").exit_code == 0
    assert run("--yes", "--json", "restart").exit_code == 0
    assert installed.calls == [("start", "fogar-gateway.service"), ("stop", "fogar-gateway.service"),
                               ("restart", "fogar-gateway.service")]
    assert waits == ["http://127.0.0.1:8004", "http://127.0.0.1:8004"]  # start y restart, no stop


def test_restart_needs_confirm(installed):
    r = run("restart")
    assert r.exit_code == 2 and "--yes" in r.output
    assert installed.calls == []


@pytest.mark.parametrize("verb", ["start", "stop", "restart"])
def test_without_unit_explains_docker(svc, verb):
    r = run("--yes", verb)
    assert r.exit_code == 1
    assert service.NO_SERVICE_MSG in r.output and "docker compose restart" in r.output
    assert svc.calls == []


def test_inside_container_explains_docker(installed, monkeypatch):
    dockerenv = installed.tmp / ".dockerenv"
    dockerenv.write_text("")
    monkeypatch.setattr(service, "DOCKERENV", dockerenv)
    r = run("--yes", "restart")
    assert r.exit_code == 1 and service.NO_SERVICE_MSG in r.output
    assert installed.calls == []


# --- uninstall --purge ---


@pytest.fixture()
def purge(svc, monkeypatch):
    svc.unit.parent.mkdir(parents=True)
    svc.unit.write_text(_unit_text(svc))
    monkeypatch.setattr(service, "_uninstall_program", lambda: "pendiente")
    return svc


def test_purge_removes_service_and_home(purge):
    r = run("--yes", "--json", "uninstall", "--purge")
    assert r.exit_code == 0, r.output
    assert not purge.unit.exists() and not purge.home.exists()
    assert purge.calls == [("disable", "--now", "fogar-gateway.service"), ("daemon-reload",)]
    assert json.loads(r.stdout) == {"unit": str(purge.unit), "exe": None,
                                    "home": str(purge.home), "state": "purgado",
                                    "program": "pendiente"}


def test_purge_final_message_needs_nothing_uv_just_uninstalled(purge, monkeypatch):
    # En un terminal la CLI pinta con rich, que se importa al usarlo: tras `uv tool uninstall`
    # ya no existe y el mensaje final no puede depender de él.
    monkeypatch.setattr(output, "_isatty", lambda: True)
    monkeypatch.setattr(service, "_uninstall_program", lambda: "desinstalado")
    for mod in ("rich", "rich.console", "rich.markup", "rich.table"):
        monkeypatch.setitem(sys.modules, mod, None)
    r = run("--yes", "uninstall", "--purge")
    assert r.exit_code == 0, r.output
    assert "Borrado:" in r.output and "Programa desinstalado" in r.output
    assert not purge.home.exists()


def test_purge_without_tty_needs_yes(purge):
    r = run("uninstall", "--purge")
    assert r.exit_code == 2 and "--yes" in r.output
    assert purge.home.exists() and purge.unit.exists() and purge.calls == []


def test_purge_asks_to_type_the_path(purge, monkeypatch):
    monkeypatch.setattr(output, "_stdin_isatty", lambda: True)
    r = run("uninstall", "--purge", input="otra\n")
    assert r.exit_code == 1 and "no se ha borrado nada" in r.output
    assert purge.home.exists() and purge.unit.exists()
    r = run("uninstall", "--purge", input=f"{purge.home}\n")
    assert r.exit_code == 0, r.output
    assert not purge.home.exists() and not purge.unit.exists()
    assert "uv tool uninstall fogar-gateway" in r.output


def test_purge_refuses_a_dir_that_is_not_an_install(purge):
    (purge.home / "config.env").unlink()
    shutil.rmtree(purge.home / "data")
    (purge.home / "mis-cosas.txt").write_text("no")
    r = run("--yes", "uninstall", "--purge")
    assert r.exit_code == 1 and "no parece una instalación" in r.output
    assert (purge.home / "mis-cosas.txt").exists() and purge.unit.exists()


def test_purge_refuses_while_a_gateway_holds_the_lock(purge):
    import fcntl

    lock = purge.home / "data" / "gateway.lock"
    with lock.open("a") as held:
        fcntl.flock(held.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        r = run("--yes", "uninstall", "--purge")
    assert r.exit_code == 1 and "gateway en marcha" in r.output
    assert (purge.home / "data" / "gateway.db").read_text() == "db"


def test_purge_refuses_a_db_outside_home(purge):
    elsewhere = purge.tmp / "fuera"
    elsewhere.mkdir()
    (elsewhere / "gw.db").write_text("db")
    (purge.home / "config.env").write_text(f"DATABASE_URL=sqlite:///{elsewhere / 'gw.db'}\n")
    r = run("--yes", "uninstall", "--purge")
    assert r.exit_code == 1 and "no está dentro" in r.output
    assert (elsewhere / "gw.db").exists() and purge.home.exists() and purge.unit.exists()


def test_purge_ignores_database_url_of_the_shell(purge, monkeypatch):
    other = purge.tmp / "otraapp"
    other.mkdir()
    (other / "app.db").write_text("ajena")
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{other / 'app.db'}")
    r = run("--yes", "uninstall", "--purge")
    assert r.exit_code == 0, r.output
    assert not purge.home.exists() and (other / "app.db").read_text() == "ajena"


def test_purge_resolves_dotdot_and_never_wipes_the_user_home(purge, monkeypatch):
    user_home = purge.tmp / "usuario"
    (user_home / "data").mkdir(parents=True)
    (user_home / "config.env").write_text("x=1\n")
    (user_home / "Documentos.txt").write_text("mío")
    monkeypatch.setattr(Path, "home", lambda: user_home)
    monkeypatch.setenv("FOGAR_GATEWAY_HOME", str(user_home / "x" / ".."))
    r = run("--yes", "uninstall", "--purge")
    assert r.exit_code == 1 and "no parece una instalación" in r.output
    assert (user_home / "Documentos.txt").exists()


def test_purge_does_not_follow_a_data_symlink(purge):
    real = purge.tmp / "otro-disco"
    shutil.move(purge.home / "data", real)
    (purge.home / "data").symlink_to(real)
    r = run("--yes", "uninstall", "--purge")
    assert r.exit_code == 1 and "enlace" in r.output
    assert (real / "gateway.db").read_text() == "db" and purge.unit.exists()


def test_purge_of_another_base_leaves_the_installed_service(purge, monkeypatch):
    other = purge.tmp / "prueba"
    other.mkdir()
    (other / "config.env").write_text("ADMIN_TOKEN=x\n")
    called = []
    monkeypatch.setattr(service, "_uninstall_program", lambda: called.append(1) or "desinstalado")
    monkeypatch.setenv("FOGAR_GATEWAY_HOME", str(other))
    r = run("--yes", "uninstall", "--purge")
    assert r.exit_code == 0, r.output
    assert not other.exists() and purge.home.exists()
    assert purge.unit.exists() and purge.calls == [] and called == []
    assert "otra base" in r.output


def test_purge_without_unit_keeps_the_program_unless_default_base(purge, monkeypatch):
    called = []
    monkeypatch.setattr(service, "_uninstall_program", lambda: called.append(1) or "desinstalado")
    purge.unit.unlink()
    r = run("--yes", "--json", "uninstall", "--purge")  # base de prueba, no ~/.fogar-gateway
    assert r.exit_code == 0, r.output
    assert not purge.home.exists() and called == []
    assert json.loads(r.stdout)["program"] == "pendiente"
    default = purge.tmp / "usuario" / ".fogar-gateway"
    (default / "data").mkdir(parents=True)
    (default / "data" / "gateway.db").write_text("db")
    monkeypatch.setattr(Path, "home", lambda: purge.tmp / "usuario")
    monkeypatch.setenv("FOGAR_GATEWAY_HOME", str(default))
    r = run("--yes", "uninstall", "--purge")
    assert r.exit_code == 0, r.output
    assert not default.exists() and called == [1]


def test_purge_needs_admin_token_or_db_as_install_mark(purge):
    (purge.home / "data" / "gateway.db").unlink()
    (purge.home / "config.env").write_text("OTRA_COSA=1\n")
    r = run("--yes", "uninstall", "--purge")
    assert r.exit_code == 1 and "no parece una instalación" in r.output
    assert purge.home.exists()


def test_purge_accepts_relative_database_url_under_a_symlinked_base(purge, monkeypatch):
    (purge.home / "config.env").write_text("ADMIN_TOKEN=x\nDATABASE_URL=sqlite:///data/gateway.db\n")
    link = purge.tmp / "enlace"
    link.symlink_to(purge.home)
    monkeypatch.setenv("FOGAR_GATEWAY_HOME", str(link))
    r = run("--yes", "uninstall", "--purge")
    assert r.exit_code == 0, r.output
    assert not purge.home.exists()
