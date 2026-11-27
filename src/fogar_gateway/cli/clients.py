"""`fogar-gw clients …`: gestión de clientes por la API /home/devices."""

import json
import sys
from collections.abc import Iterable
from typing import Any

import click

from fogar_gateway.cli import api, refs
from fogar_gateway.cli.output import ACCENT, MUTED, Out, confirm

COLUMNS = [
    ("NOMBRE", "name"),
    ("ID", "short_id"),
    ("TIPO", "client_type"),
    ("ACTIVO", "is_active"),
    ("AGENTE", "default_agent"),
    ("FP", "key_fp"),
]


# Lo que un administrador puede conceder a un aparato del hogar (5.0, #410): nunca home:admin.
HOUSEHOLD_SCOPES = ("conversations:read", "conversations:write", "home:read")


def _out(ctx: click.Context) -> Out:
    return Out(ctx.find_root().obj["json"])


def _stdin_isatty() -> bool:
    return sys.stdin.isatty()


def _show_key(out: Out, data: dict[str, Any]) -> None:
    if out.as_json:
        out.json(data)
        return
    # La clave primero: el servidor ya la ha creado o rotado y no se puede volver a pedir.
    # En la 5.0 la credencial de un aparato se llama `token` (como en /pair); la API la sigue
    # llamando `client_key` en /home/devices.
    out.message(f"token: {data['client_key']}", ACCENT)
    out.pairs([("nombre", data["name"]), ("id", data["id"]), ("fp", data["key_fp"])])
    out.message("Guárdalo ahora: no se volverá a mostrar.", MUTED)


# Campos que devuelve el gateway pero no se editan.
READ_ONLY = {"id", "key_fp", "created_at", "client_key", "user_id", "capabilities", "last_seen_at"}


def set_values(pairs: tuple[str, ...], fields: Iterable[str]) -> dict[str, Any]:
    """`campo=texto` o `campo:=json`, como httpie. El nombre se comprueba contra `fields`: la API
    ignora los desconocidos a propósito (#100), así que un error de tecleo no cambiaría nada
    sin avisar (#306). También lo usa `fogar-gw pair` (#317)."""
    editable = sorted(fields)
    values: dict[str, Any] = {}
    for item in pairs:
        field, sep, raw = item.partition("=")
        as_json = field.endswith(":")
        field = field.removesuffix(":")
        if not sep or not field:
            raise api.ApiError(f"--set espera campo=valor o campo:=json, no «{item}».", 2)
        if field not in editable:
            raise api.ApiError(
                f"«{field}» no es un campo editable. Campos: {', '.join(editable)}.", 2
            )
        if not as_json:
            values[field] = raw  # el servidor convierte números y booleanos
            continue
        try:
            values[field] = json.loads(raw)
        except ValueError:
            raise api.ApiError(f"--set {field}:= espera JSON válido.", 2) from None
    return values


def _no_key(data: dict[str, Any]) -> dict[str, Any]:
    # Un gateway anterior a #293 aún manda client_key en lecturas: nunca se enseña.
    return {k: v for k, v in data.items() if k != "client_key"}


def _find(client: api.Api, ref: str) -> dict[str, Any]:
    return refs.resolve(client.request("GET", "/home/devices"), ref)


@click.group()
def clients() -> None:
    """Gestiona los clientes (por id, prefijo de id o nombre)."""


@clients.command("list")
@click.pass_context
def list_(ctx: click.Context) -> None:
    """Lista los clientes."""
    out = _out(ctx)
    rows = [_no_key(r) for r in api.from_context(ctx).request("GET", "/home/devices")]
    if out.as_json:
        out.json(rows)
    elif not rows:
        out.message("Sin clientes. Crea uno con `fogar-gw clients add --name …`.")
    else:
        out.table(COLUMNS, [{**r, "short_id": r["id"][:8]} for r in rows])


@clients.command()
@click.argument("ref")
@click.pass_context
def show(ctx: click.Context, ref: str) -> None:
    """Enseña todos los datos de un cliente, salvo su clave."""
    out = _out(ctx)
    client = api.from_context(ctx)
    data = _no_key(client.request("GET", f"/home/devices/{_find(client, ref)['id']}"))
    if out.as_json:
        out.json(data)
    else:
        out.pairs(list(data.items()))


@clients.command()
@click.option("--name", required=True, help="Nombre del cliente.")
@click.option("--type", "client_type", help="Tipo: ha, esp32, web, app o screen.")
@click.option("--agent", help="Agente por defecto.")
@click.option("--allowed-agent", "allowed", multiple=True, help="Agente permitido (repetible).")
@click.option("--scope", "scopes", multiple=True, type=click.Choice(HOUSEHOLD_SCOPES),
              help="Permiso que se le concede (repetible). Sin ninguno, conversations:write.")
@click.option("--import-key", is_flag=True, help="Importar una clave existente desde stdin.")
@click.pass_context
def add(
    ctx: click.Context,
    name: str,
    client_type: str | None,
    agent: str | None,
    allowed: tuple[str, ...],
    scopes: tuple[str, ...],
    import_key: bool,
) -> None:
    """Crea un cliente y enseña su clave una sola vez."""
    body: dict[str, Any] = {"name": name}
    if client_type:
        body["client_type"] = client_type
    if agent:
        body["default_agent"] = agent
    if allowed:
        body["allowed_agents"] = list(allowed)
    if scopes:
        body["scopes"] = list(scopes)
    if import_key:
        # Por stdin, nunca por argumento: así no acaba en el historial de la shell.
        key = (
            click.prompt("Clave a importar", hide_input=True, err=True)
            if _stdin_isatty()
            else sys.stdin.readline()
        ).strip()
        if not key:
            raise api.ApiError("--import-key no ha recibido ninguna clave por stdin.", 2)
        body["client_key"] = key
    _show_key(_out(ctx), api.from_context(ctx).request("POST", "/home/devices", body))


@clients.command()
@click.argument("ref")
@click.option("--name", help="Nombre nuevo.")
@click.option("--type", "client_type", help="Tipo: ha, esp32, web, app o screen.")
@click.option("--agent", help="Agente por defecto.")
@click.option("--allowed-agent", "allowed", multiple=True, help="Agente permitido (repetible).")
@click.option("--set", "sets", multiple=True, metavar="CAMPO=TEXTO|CAMPO:=JSON",
              help="Cualquier otro campo (voz, turno…). Con := el valor va como JSON (listas, null).")
@click.pass_context
def edit(
    ctx: click.Context,
    ref: str,
    name: str | None,
    client_type: str | None,
    agent: str | None,
    allowed: tuple[str, ...],
    sets: tuple[str, ...],
) -> None:
    """Cambia campos de un cliente."""
    body: dict[str, Any] = {}
    if name:
        body["name"] = name
    if client_type:
        body["client_type"] = client_type
    if agent:
        body["default_agent"] = agent
    if allowed:
        body["allowed_agents"] = list(allowed)
    client = api.from_context(ctx)
    target = _find(client, ref)
    body.update(set_values(sets, set(target) - READ_ONLY))
    if not body:
        raise api.ApiError("Nada que cambiar: indica al menos una opción.", 2)
    data = _no_key(client.request("PATCH", f"/home/devices/{target['id']}", body))
    out = _out(ctx)
    if out.as_json:
        out.json(data)
    else:
        out.pairs(list(data.items()))


def _set_active(ctx: click.Context, ref: str, active: bool) -> None:
    client = api.from_context(ctx)
    data = _no_key(
        client.request("PATCH", f"/home/devices/{_find(client, ref)['id']}", {"is_active": active})
    )
    out = _out(ctx)
    if out.as_json:
        out.json(data)
    elif active:
        out.message(f"Cliente «{data['name']}» activado.")
    else:
        out.message(f"Cliente «{data['name']}» desactivado; sus sesiones abiertas se cortan en ≤ 10 s.")


@clients.command()
@click.argument("ref")
@click.pass_context
def enable(ctx: click.Context, ref: str) -> None:
    """Activa un cliente."""
    _set_active(ctx, ref, True)


@clients.command()
@click.argument("ref")
@click.pass_context
def disable(ctx: click.Context, ref: str) -> None:
    """Desactiva un cliente y corta sus sesiones."""
    _set_active(ctx, ref, False)


@clients.command("rotate-key")
@click.argument("ref")
@click.pass_context
def rotate_key(ctx: click.Context, ref: str) -> None:
    """Genera una clave nueva; la anterior deja de valer."""
    client = api.from_context(ctx)
    target = _find(client, ref)
    confirm(ctx, f"¿Rotar la clave de «{target['name']}»? La actual dejará de valer.")
    _show_key(_out(ctx), client.request("POST", f"/home/devices/{target['id']}/rotate-key"))


@clients.command()
@click.argument("ref")
@click.pass_context
def remove(ctx: click.Context, ref: str) -> None:
    """Retira un cliente: su clave deja de valer y sus conversaciones se conservan."""
    client = api.from_context(ctx)
    target = _find(client, ref)
    confirm(ctx, f"¿Retirar el cliente «{target['name']}»? Su clave dejará de valer.")
    client.request("DELETE", f"/home/devices/{target['id']}")
    out = _out(ctx)
    if out.as_json:
        out.json({"id": target["id"], "removed": True})
    else:
        out.message(f"Cliente «{target['name']}» retirado.")
