"""
service.py
~~~~~~~~~~
Servicio de usuario de systemd de `fogar-gw`: la unidad, la elección del ejecutable y los
comandos `install`, `uninstall`, `start`, `stop` y `restart`. `subprocess` se importa dentro
de las funciones que lo usan (`--help` < 150 ms).
"""

import json
import os
import re
import shutil
import sys
import time
from pathlib import Path
from typing import TYPE_CHECKING

import click

from fogar_gateway import paths
from fogar_gateway.cli import api
from fogar_gateway.cli.api import ApiError
from fogar_gateway.cli.output import Out, confirm

if TYPE_CHECKING:
    from subprocess import CompletedProcess

UNIT_NAME = "fogar-gateway.service"
# Más que los 30 s de reintento ante startup-sidecars (+ el último intento): el lifespan los espera antes de abrir el 8004.
HEALTH_TIMEOUT_S = 45.0
DOCKERENV = Path("/.dockerenv")
NO_SERVICE_MSG = ("No hay servicio instalado. En Docker: `docker compose restart` / "
                  "`docker compose logs -f`. Para instalarlo: `fogar-gw install`.")

# TimeoutStopSec = stop_grace_period de docker-compose.yml; mayor que serve.GRACEFUL_SHUTDOWN_S.
# Sin --create-db ni LOG_FORMAT: manda la configuración. Exit 3 = fallo de arranque que
# reintentar no arregla (RestartPreventExitStatus).
_UNIT = """[Unit]
Description=Fogar gateway
StartLimitIntervalSec=60
StartLimitBurst=5

[Service]
Type=exec
ExecStart={exe} serve
Environment={env}
Restart=on-failure
RestartSec=5
RestartPreventExitStatus=3
TimeoutStopSec=45
UMask=0077

[Install]
WantedBy=default.target
"""


def unit_path() -> Path:
    config = os.environ.get("XDG_CONFIG_HOME") or Path.home() / ".config"
    return Path(config) / "systemd" / "user" / UNIT_NAME


# Lo que systemd lee igual sin comillas: así una unidad de una versión anterior sigue idéntica.
_BARE = re.compile(r"[A-Za-z0-9_./:@+,=-]+")


def _quote(value: str) -> str:
    # Comillas de systemd con escapes C; % es un especificador en ExecStart= y Environment=.
    if _BARE.fullmatch(value):
        return value
    escaped = value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")
    return '"' + escaped.replace("%", "%%") + '"'


def render_unit(exe: Path, home: Path) -> str:
    # $ solo se expande en ExecStart=; en Environment= es literal.
    return _UNIT.format(exe=_quote(str(exe)).replace("$", "$$"),
                        env=_quote(f"FOGAR_GATEWAY_HOME={home}"))


def _in_checkout(exe: Path) -> bool:
    real = exe.resolve()
    # Solo <env> y <clon> (<clon>/.venv/bin/fogar-gw): un pyproject.toml suelto en ~ o / no cuenta.
    if any((p / "pyproject.toml").is_file() for p in real.parents[1:3]):
        return True
    # shortcut: solo mira el site-packages junto al ejecutable (<env>/bin/fogar-gw), el
    # layout de uv tool y de venv; otro layout no se detecta, hay que pasar --exe a mano.
    site = "lib*/python*/site-packages/fogar[_-]gateway-*.dist-info/direct_url.json"
    return any(json.loads(f.read_text()).get("dir_info", {}).get("editable") is True
               for f in real.parent.parent.glob(site))


def pick_exe(explicit: Path | None) -> Path:
    """El `fogar-gw` de uv tool. Sin resolver el symlink: así sobrevive a `uv tool upgrade`."""
    if explicit is not None:
        # Un nombre suelto se busca en el PATH; una ruta se hace absoluta y normalizada (systemd
        # rechaza `..`) con abspath, que no resuelve el symlink.
        found = shutil.which(explicit) if os.sep not in str(explicit) else os.path.abspath(explicit)
        if found is None:
            raise ApiError(f"No encuentro `{explicit}` en el PATH; indica su ruta con --exe.", 1)
        return Path(found)
    found = shutil.which("fogar-gw")
    if found is None:
        raise ApiError("No encuentro `fogar-gw` en el PATH; instálalo con `uv tool install` "
                       "o indica la ruta con --exe.", 1)
    exe = Path(found)
    if _in_checkout(exe):
        raise ApiError(f"{exe} está dentro de un clon o es una instalación editable: la unidad "
                       "se rompería al mover o borrar el clon. Instala con `uv tool install` "
                       "o indica otro ejecutable con --exe.", 1)
    return exe


def systemctl(*args: str, check: bool = True) -> "CompletedProcess[str]":
    import subprocess

    return subprocess.run(["systemctl", "--user", *args], check=check, text=True,
                          capture_output=True)


def require_systemd() -> None:
    if sys.platform != "linux" or shutil.which("systemctl") is None:
        raise ApiError("Este comando necesita Linux con systemd; en Docker usa "
                       "`docker compose up -d`, `docker compose logs`, etc.", 1)


def systemctl_or_fail(*args: str) -> None:
    import subprocess

    try:
        systemctl(*args)
    except subprocess.CalledProcessError as e:
        detail = (e.stderr or "").strip()
        raise ApiError(f"`systemctl --user {' '.join(args)}` ha fallado"
                       f"{f': {detail}' if detail else ''}.", 1) from None


def is_active() -> bool:
    """El servicio de usuario está en marcha (`systemctl --user is-active`)."""
    return systemctl("is-active", "--quiet", UNIT_NAME, check=False).returncode == 0


def port_free() -> bool:
    """El puerto del gateway (127.0.0.1) no lo usa nadie: ni bind ni conexión."""
    import socket

    from fogar_gateway.cli import serve

    with socket.socket() as s:
        # Sin esto, conexiones en TIME_WAIT tras un `docker compose stop` darían un falso «ocupado».
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            s.bind(("127.0.0.1", serve.PORT))
        except OSError:
            return False
    with socket.socket() as s:
        s.settimeout(1)
        return s.connect_ex(("127.0.0.1", serve.PORT)) != 0


def linger_enabled(user: str) -> bool:
    import subprocess

    try:
        r = subprocess.run(["loginctl", "show-user", user, "-p", "Linger"], text=True,
                           capture_output=True, check=False)
    except OSError:
        return False
    return r.stdout.strip() == "Linger=yes"


def _probe(url: str) -> bool:
    try:
        api.Api(url, "", timeout=2).request("GET", "/healthz")
    except ApiError:
        return False
    return True


def wait_healthy(url: str) -> None:
    """Espera hasta HEALTH_TIMEOUT_S a que `/healthz` responda; la usan install, start y restart."""
    deadline = time.monotonic() + HEALTH_TIMEOUT_S
    while not _probe(url):
        if time.monotonic() >= deadline:
            raise ApiError(f"El servicio no responde en {HEALTH_TIMEOUT_S:.0f} s. "
                           "Mira los registros con `fogar-gw logs -n 50`.", 1)
        time.sleep(0.5)


def _out(ctx: click.Context) -> Out:
    return Out(ctx.find_root().obj["json"])


@click.command()
@click.option("--exe", type=click.Path(path_type=Path), help="Ejecutable `fogar-gw` de la unidad.")
@click.pass_context
def install(ctx: click.Context, exe: Path | None) -> None:
    """Instala y arranca el servicio."""
    from fogar_gateway.cli import config as cfg

    out = _out(ctx)
    require_systemd()
    own_unit_or_fail()
    settings, _ = cfg._load_or_fail(out)
    db = paths.sqlite_file(settings.DATABASE_URL)
    if db is not None and not db.exists():
        raise ApiError(f"No hay BD en {db}. Ejecuta `fogar-gw init` (instalación nueva) o "
                       "`fogar-gw migrate --from-docker <carpeta>`.", 1)
    unit = unit_path()
    # Si nuestro servicio ya corre, el 8004 es suyo: se puede reinstalar o actualizar la unidad.
    active = unit.exists() and is_active()
    if not active and not port_free():
        raise ApiError(f"El puerto {_port()} está ocupado: ¿hay otro gateway (el contenedor)? "
                       "Páralo antes de instalar el servicio.", 1)
    exe = pick_exe(exe)
    home = paths.home()
    text = render_unit(exe, home)
    if unit.exists() and unit.read_text() == text:
        state = "sin cambios"
    else:
        if unit.exists():
            confirm(ctx, f"{unit} ya existe y es distinta. ¿Sobrescribirla?")
        unit.parent.mkdir(parents=True, exist_ok=True)
        tmp = unit.with_name(f".{UNIT_NAME}.tmp")
        tmp.write_text(text)
        os.replace(tmp, unit)  # atómico: systemd nunca ve una unidad a medias
        systemctl_or_fail("daemon-reload")
        state = "instalado"
    systemctl_or_fail("enable", "--now", UNIT_NAME)
    if active and state == "instalado":
        systemctl_or_fail("restart", UNIT_NAME)  # si no, seguiría corriendo el ejecutable viejo
    user = os.environ.get("USER") or os.environ.get("LOGNAME") or ""
    if not linger_enabled(user):
        out.error(f"Aviso: sin linger el servicio se para al cerrar tu sesión. "
                  f"Ejecuta `loginctl enable-linger {user}`.")
    wait_healthy(ctx.find_root().obj["url"])
    if out.as_json:
        out.json({"unit": str(unit), "exe": str(exe), "home": str(home), "state": state})
        return
    out.pairs([("unidad", unit), ("ejecutable", exe), ("base", home), ("estado", state)])
    out.message("Servicio en marcha.")


def _port() -> int:
    from fogar_gateway.cli import serve

    return serve.PORT


@click.command()
@click.option("--purge", is_flag=True,
              help="Borra también todos los datos (el directorio base y la BD) y el programa.")
@click.pass_context
def uninstall(ctx: click.Context, purge: bool) -> None:
    """Quita el servicio; no toca los datos (con --purge, lo borra todo)."""
    out = _out(ctx)
    unit = unit_path()
    home = paths.home()
    if purge:
        _purge(ctx, out, unit, home)
        return
    if not unit.exists():
        if out.as_json:
            out.json({"unit": str(unit), "exe": None, "home": str(home), "state": "no instalado"})
        else:
            out.message(f"El servicio no está instalado ({unit}).")
        return
    require_systemd()
    confirm(ctx, f"¿Quitar el servicio {UNIT_NAME}?")
    _remove_unit(unit)
    if out.as_json:
        out.json({"unit": str(unit), "exe": None, "home": str(home), "state": "desinstalado"})
        return
    out.message(f"Servicio quitado. No se toca el directorio base ({home}): tus datos siguen ahí.")


def _remove_unit(unit: Path) -> None:
    systemctl_or_fail("disable", "--now", UNIT_NAME)
    unit.unlink()
    systemctl_or_fail("daemon-reload")


def _purge(ctx: click.Context, out: Out, unit: Path, home: Path) -> None:
    """Servicio, directorio base (BD incluida) y programa. Solo borra dentro de la base, resuelta
    (sin `..` ni enlaces): una BD fuera de ella se niega. Nunca toca uv, linger, Tailscale ni
    OpenClaw."""
    from fogar_gateway.cli import output

    if DOCKERENV.exists():
        raise ApiError("En Docker los datos viven en el volumen: `docker compose down -v`.", 1)
    home = home.resolve()
    user_home = Path.home().resolve()
    config = home / "config.env"
    # La BD que usa el servicio: la de config.env (systemd no hereda esta shell), nunca la del entorno.
    env = api.read_env_file(config) if config.is_file() else {}
    # Marca de instalación: un config.env con ADMIN_TOKEN (init lo escribe) o la BD; un
    # config.env cualquiera de otro proyecto no basta.
    if home == Path(home.anchor) or home == user_home or home in user_home.parents or not (
            "ADMIN_TOKEN" in env or (home / "data" / "gateway.db").is_file()):
        raise ApiError(f"{home} no parece una instalación de fogar-gateway (sin ADMIN_TOKEN en "
                       "config.env ni data/gateway.db): no se borra nada.", 1)
    url = env.get("DATABASE_URL")
    db = paths.sqlite_file(url) if url else None
    if url and (db is None or not db.resolve().is_relative_to(home)):
        raise ApiError(f"La BD ({db or url}) no está dentro de {home}: --purge solo borra dentro "
                       "de la base. Bórrala a mano o quita DATABASE_URL de config.env.", 1)
    lock = (db.resolve().parent if db else home / "data") / paths.LOCK_FILENAME
    if (home / "data").resolve() != home / "data":
        raise ApiError(f"{home}/data es un enlace a otro sitio: --purge no lo sigue. "
                       "Bórralo a mano.", 1)
    # Unidad y programa son de la instalación del servicio: solo se tocan si son de esta base
    # (sin unidad, solo la base por defecto, la de install.sh).
    ours = _unit_home(unit) == home if unit.exists() else (
        home == user_home / ".fogar-gateway")
    if not ctx.find_root().obj["yes"]:
        if not output._stdin_isatty():
            raise ApiError("Esto lo borra todo. Sin terminal para preguntar: repite con --yes.", 2)
        typed = click.prompt(
            f"Se borrarán {'el servicio, ' if ours and unit.exists() else ''}{home} (BD con "
            "clientes y claves, config.env) y el programa, sin vuelta atrás. Escribe la ruta "
            "para confirmar", default="", show_default=False, err=True)
        if typed.strip() not in (str(home), str(paths.home())):
            raise ApiError("La ruta no coincide: no se ha borrado nada.", 1)
    removed = unit.exists() and ours
    if removed:
        require_systemd()
        _remove_unit(unit)
    # Con el servicio parado, un gateway con el lock es un `serve` a mano: no se le borra la BD.
    if _lock_busy(lock):
        quitado = "El servicio ya está quitado; " if removed else ""
        raise ApiError(f"Hay un gateway en marcha ({lock}): páralo y repite. {quitado}Los "
                       "datos siguen.", 1)
    try:
        shutil.rmtree(home)
    except OSError as e:
        raise ApiError(f"No se ha podido borrar todo {home} ({type(e).__name__}): lo que queda "
                       "sigue ahí; revisa sus permisos y repite.", 1) from None
    program = _uninstall_program() if ours else "pendiente"
    if out.as_json:
        out.json({"unit": str(unit), "exe": None, "home": str(home), "state": "purgado",
                  "program": program})
        return
    # click.echo, no out.message: tras `uv tool uninstall` ya no está rich (se importa al usarlo).
    if not ours:
        click.echo("El servicio y el programa son de otra base: no se han tocado.")
    click.echo(f"Borrado: {home}{' y el servicio' if removed else ''}.")
    if program == "desinstalado":
        click.echo("Programa desinstalado (uv tool uninstall fogar-gateway).")
    elif ours:
        click.echo("Quita el programa con: uv tool uninstall fogar-gateway")


def _unit_home(unit: Path) -> Path | None:
    """El FOGAR_GATEWAY_HOME de la unidad instalada (resuelto), o None si no se entiende."""
    m = re.search(r'^Environment="?FOGAR_GATEWAY_HOME=([^"\n]*)"?$', unit.read_text(), re.M)
    return Path(m.group(1)).resolve() if m else None


def own_unit_or_fail(hint: str | None = None) -> None:
    """La unidad instalada tiene que ser de esta base: si no, instalarla, arrancarla, pararla o
    reiniciarla tocaría otro gateway (p. ej. producción, con `FOGAR_GATEWAY_HOME` de prueba). Sin
    la variable en la unidad, systemd usa la base por defecto."""
    unit = unit_path()
    if not unit.exists():
        return
    theirs = _unit_home(unit) or (Path.home() / ".fogar-gateway").resolve()
    if theirs != paths.home().resolve():
        hint = hint or f"Para manejarlo, indica su base: `fogar-gw --home {theirs} …`."
        raise ApiError(f"El servicio instalado ({unit}) es de otra base ({theirs}), no de "
                       f"{paths.home()}: no lo toco. {hint}", 1)


def _lock_busy(lock: Path) -> bool:
    import fcntl

    if not lock.exists():
        return False
    with lock.open("a") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return True
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
    return False


def _uninstall_program() -> str:
    """`uv tool uninstall` si así se instaló; si no, el usuario lo quita a mano."""
    import subprocess

    uv = shutil.which("uv")
    if uv is None:
        return "pendiente"
    listed = subprocess.run([uv, "tool", "list"], text=True, capture_output=True, check=False)
    if not re.search(r"^fogar-gateway\s", listed.stdout, re.M):
        return "pendiente"
    done = subprocess.run([uv, "tool", "uninstall", "fogar-gateway"], text=True,
                          capture_output=True, check=False)
    return "desinstalado" if done.returncode == 0 else "pendiente"


def require_unit() -> None:
    if DOCKERENV.exists() or not unit_path().exists():
        raise ApiError(NO_SERVICE_MSG, 1)
    require_systemd()


def _verb(ctx: click.Context, verb: str, state: str, wait: bool) -> None:
    require_unit()
    own_unit_or_fail()
    systemctl_or_fail(verb, UNIT_NAME)
    if wait:
        wait_healthy(ctx.find_root().obj["url"])
    out = _out(ctx)
    if out.as_json:
        out.json({"unit": str(unit_path()), "state": state})
    else:
        out.message(f"Servicio {state}.")


@click.command()
@click.pass_context
def start(ctx: click.Context) -> None:
    """Arranca el servicio."""
    _verb(ctx, "start", "arrancado", wait=True)


@click.command()
@click.pass_context
def stop(ctx: click.Context) -> None:
    """Para el servicio."""
    _verb(ctx, "stop", "parado", wait=False)


@click.command()
@click.pass_context
def restart(ctx: click.Context) -> None:
    """Reinicia el servicio."""
    require_unit()  # antes de preguntar: sin servicio no hay nada que reiniciar
    own_unit_or_fail()
    confirm(ctx, f"¿Reiniciar {UNIT_NAME}? Se cortarán las sesiones abiertas.")
    _verb(ctx, "restart", "reiniciado", wait=True)
