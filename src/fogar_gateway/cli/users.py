"""`fogar-gw users …`: las personas del hogar, por la API /admin/users (M20).

Con el `ADMIN_TOKEN` de la máquina: es también la recuperación (`reset-password`) cuando el
dueño pierde la contraseña. Las contraseñas se piden ocultas o se leen de stdin, nunca como
argumento (no acaban en el historial de la shell).
"""

import sys
from typing import Any

import click

from fogar_gateway.cli import api
from fogar_gateway.cli.output import MUTED, Out, confirm

ROLES = ("owner", "admin", "member")
ROLE_NAMES = {"owner": "dueño", "admin": "administrador", "member": "miembro"}

COLUMNS = [
    ("USUARIO", "username"),
    ("NOMBRE", "display_name"),
    ("ROL", "role_name"),
    ("AGENTE", "agent_text"),
    ("ACTIVO", "is_active"),
]

SHARED_NOTE = ("* comparte agente con otra persona: su memoria es común aunque las conversaciones "
               "sean privadas.")


def _out(ctx: click.Context) -> Out:
    return Out(ctx.find_root().obj["json"])


def _stdin_isatty() -> bool:
    return sys.stdin.isatty()


def _password() -> str:
    """Oculta y dos veces en una terminal; una línea de stdin si no la hay."""
    if _stdin_isatty():
        value = click.prompt("Contraseña (8 o más)", hide_input=True,
                             confirmation_prompt="Repítela", err=True)
    else:
        value = sys.stdin.readline().rstrip("\n")
    if not value:
        raise api.ApiError("No ha llegado ninguna contraseña.", 2)
    return str(value)


def _find(client: api.Api, ref: str) -> dict[str, Any]:
    """Por nombre de usuario (sin distinguir mayúsculas) o por id."""
    users = client.request("GET", "/admin/users")
    ref = ref.strip()
    for u in users:
        if u["id"] == ref or u["username"] == ref.lower():
            return dict(u)
    raise api.ApiError(f"No hay ningún usuario «{ref}». Mira `fogar-gw users list`.")


@click.group()
def users() -> None:
    """Gestiona las personas del hogar (por nombre de usuario o id)."""


@users.command("list")
@click.pass_context
def list_(ctx: click.Context) -> None:
    """Lista las personas, su rol y el agente que las atiende."""
    out = _out(ctx)
    rows = api.from_context(ctx).request("GET", "/admin/users")
    if out.as_json:
        out.json(rows)
        return
    if not rows:
        out.message("Sin usuarios. Crea el dueño con `fogar-gw users add NOMBRE --role owner`.")
        return
    out.table(COLUMNS, [
        {**r, "role_name": ROLE_NAMES.get(r["role"], r["role"]),
         "agent_text": f"{r.get('effective_agent') or '?'}{' *' if r.get('agent_shared') else ''}"}
        for r in rows
    ])
    if any(r.get("agent_shared") for r in rows):
        out.message(SHARED_NOTE, MUTED)


@users.command()
@click.argument("username")
@click.option("--name", "display_name", help="Nombre visible (por defecto, el de usuario).")
@click.option("--role", type=click.Choice(ROLES), default="member", show_default=True)
@click.option("--agent", help="Agente que la atiende (por defecto, el de lo común).")
@click.pass_context
def add(ctx: click.Context, username: str, display_name: str | None, role: str,
        agent: str | None) -> None:
    """Da de alta a una persona; la contraseña, oculta o por stdin."""
    body: dict[str, Any] = {"username": username, "display_name": display_name or username,
                            "role": role, "password": _password()}
    if agent:
        body["agent"] = agent
    data = api.from_context(ctx).request("POST", "/admin/users", body)
    out = _out(ctx)
    if out.as_json:
        out.json(data)
    else:
        out.message(f"«{data['username']}» dado de alta como {ROLE_NAMES[data['role']]}. "
                    "Ya puede entrar en el dashboard con su usuario y contraseña.")


def _patch(ctx: click.Context, ref: str, body: dict[str, Any], done: str) -> None:
    client = api.from_context(ctx)
    data = client.request("PATCH", f"/admin/users/{_find(client, ref)['id']}", body)
    out = _out(ctx)
    if out.as_json:
        out.json(data)
    else:
        out.message(done.format(**data))


@users.command()
@click.argument("ref")
@click.option("--name", "display_name", help="Nombre visible nuevo.")
@click.option("--role", type=click.Choice(ROLES))
@click.option("--agent", help="Agente que la atiende.")
@click.option("--common-agent", is_flag=True, help="Que la atienda el agente de lo común.")
@click.pass_context
def edit(ctx: click.Context, ref: str, display_name: str | None, role: str | None,
         agent: str | None, common_agent: bool) -> None:
    """Cambia el nombre, el rol o el agente de una persona."""
    if agent and common_agent:
        raise api.ApiError("--agent y --common-agent no van juntos.", 2)
    body: dict[str, Any] = {}
    if display_name:
        body["display_name"] = display_name
    if role:
        body["role"] = role
    if agent:
        body["agent"] = agent
    if common_agent:
        body["agent"] = None
    if not body:
        raise api.ApiError("Nada que cambiar: indica al menos una opción.", 2)
    _patch(ctx, ref, body, "«{username}» actualizado.")


@users.command()
@click.argument("ref")
@click.pass_context
def enable(ctx: click.Context, ref: str) -> None:
    """Reactiva a una persona."""
    _patch(ctx, ref, {"is_active": True}, "«{username}» activado.")


@users.command()
@click.argument("ref")
@click.pass_context
def disable(ctx: click.Context, ref: str) -> None:
    """Desactiva a una persona: no entra y sus dispositivos se cortan ya."""
    _patch(ctx, ref, {"is_active": False},
           "«{username}» desactivado; sus dispositivos se han desconectado.")


@users.command("reset-password")
@click.argument("ref")
@click.pass_context
def reset_password(ctx: click.Context, ref: str) -> None:
    """Pone una contraseña nueva (la recuperación, desde la máquina del gateway)."""
    client = api.from_context(ctx)
    target = _find(client, ref)
    client.request("PUT", f"/admin/users/{target['id']}/password", {"password": _password()})
    out = _out(ctx)
    if out.as_json:
        out.json({"id": target["id"], "password_set": True})
    else:
        out.message(f"Contraseña de «{target['username']}» cambiada. Sus dispositivos con sesión "
                    "siguen dentro; retíralos desde el dashboard si hace falta.")


@users.command()
@click.argument("ref")
@click.pass_context
def remove(ctx: click.Context, ref: str) -> None:
    """Borra a una persona, sus dispositivos y sus conversaciones privadas."""
    client = api.from_context(ctx)
    target = _find(client, ref)
    confirm(ctx, f"¿Borrar a «{target['username']}»? Se van sus dispositivos y sus conversaciones "
                 "privadas del gateway (en OpenClaw quedan hasta que limpies su agente).")
    client.request("DELETE", f"/admin/users/{target['id']}")
    out = _out(ctx)
    if out.as_json:
        out.json({"id": target["id"], "removed": True})
    else:
        out.message(f"«{target['username']}» borrado.")
