"""
network.py
~~~~~~~~~~
La IP real de quien llama, para los logs (HTTP y WebSocket).

resolve_client_ip(conn)  -> str   — IP real, respetando X-Real-IP solo cuando el peer TCP es un
                                    proxy de TRUSTED_PROXIES.
client_ip_for_log(conn)  -> str   — la misma, con el peer si no se pudo fiar.

Desde la 5.0 (#413) ninguna IP exime de credencial: `/v1` exige siempre Bearer, así que ya no hay
origen de confianza ni `TRUSTED_NETWORKS`/`TRUST_LOOPBACK`.
"""

import functools
import ipaddress
import logging

from starlette.requests import HTTPConnection

# `settings` se importa dentro de cada función que lo usa: la CLI usa las funciones puras
# (validar entradas) aunque config.env no valide y `Settings()` falle al importarse (#359).


logger = logging.getLogger(__name__)

_IpNetwork = ipaddress.IPv4Network | ipaddress.IPv6Network

# Not an IP on purpose: it reads well in logs.
NO_REAL_IP = "proxy-without-real-ip"


def _entries(csv: str) -> list[str]:
    return [raw.strip() for raw in csv.split(",") if raw.strip()]


_IPV4_MAPPED = ipaddress.IPv6Network("::ffff:0:0/96")


def _too_wide(net: _IpNetwork) -> bool:
    """Wider than /8 (IPv4) or /16 (IPv6), or covering all IPv4-mapped addresses."""
    if isinstance(net, ipaddress.IPv4Network):
        return net.prefixlen < 8
    if net.supernet_of(_IPV4_MAPPED) or (net.subnet_of(_IPV4_MAPPED) and net.prefixlen < 104):
        return True
    return net.prefixlen < 16


def entry_problem(raw: str, *, strict: bool = False) -> str | None:
    """Why `raw` is not a usable IP/CIDR entry, or None.

    Runtime (strict=False) rejects only what can't be a network or is /0, so an upgrade never
    breaks an install. The CLI (strict=True) also rejects networks that are too wide; host bits
    (192.168.1.5/24) are reported apart by host_bits().
    """
    if "%" in raw:  # zone id: free text, never a network
        return "lleva identificador de zona (%)"
    try:
        net = ipaddress.ip_network(raw, strict=False)
    except ValueError:
        return "no es una IP ni un CIDR"
    if net.prefixlen == 0:
        return "/0 abarca todas las direcciones"
    if strict and _too_wide(net):
        return "red demasiado amplia (más que /8 en IPv4 o /16 en IPv6, o todo ::ffff:0:0/96)"
    return None


def host_bits(raw: str) -> bool:
    """A valid entry written with host bits set (192.168.1.5/24 = 192.168.1.0/24)."""
    if entry_problem(raw):
        return False
    try:
        ipaddress.ip_network(raw)
    except ValueError:
        return True
    return False


def invalid_entries(csv: str, *, strict: bool = False) -> list[str]:
    if not strict:
        return list(_parsed(csv)[1])
    return [raw for raw in _entries(csv) if entry_problem(raw, strict=True)]


def wide_entries(csv: str) -> list[str]:
    """Entries the gateway uses but that cover too much (warned at startup)."""
    return [raw for raw in _entries(csv) if not entry_problem(raw) and entry_problem(raw, strict=True)]


@functools.lru_cache(maxsize=16)
def _parsed(csv: str) -> tuple[tuple[_IpNetwork, ...], tuple[str, ...]]:
    """(usable networks, invalid entries) of a CSV, parsed once per distinct string."""
    nets: list[_IpNetwork] = []
    bad: list[str] = []
    for raw in _entries(csv):
        if entry_problem(raw):
            bad.append(raw)
        else:
            nets.append(ipaddress.ip_network(raw, strict=False))
    return tuple(nets), tuple(bad)


def _parse_networks(csv: str) -> tuple[_IpNetwork, ...]:
    # Invalid entries are skipped silently here; warn_if_open_proxy_config() reports them once.
    return _parsed(csv)[0]


def _ip_in_networks(ip_str: str, networks: tuple[_IpNetwork, ...]) -> bool:
    try:
        ip = ipaddress.ip_address(ip_str)
    except ValueError:
        return False
    return any(ip in net for net in networks)


def _is_loopback(ip_str: str) -> bool:
    try:
        return ipaddress.ip_address(ip_str).is_loopback
    except ValueError:
        return False


def is_trusted_proxy(ip_str: str) -> bool:
    """True if ip_str is allowed to set X-Real-IP (settings.TRUSTED_PROXIES)."""
    from fogar_gateway.core.config import settings

    return _ip_in_networks(ip_str, _parse_networks(settings.TRUSTED_PROXIES))


def _proxies_invalid() -> bool:
    from fogar_gateway.core.config import settings

    return bool(_parsed(settings.TRUSTED_PROXIES)[1])


def warn_if_open_proxy_config() -> None:
    """Startup WARNINGs about TRUSTED_PROXIES (F-r3-1): invalid entries (no peer is a proxy, the
    logs show the proxy's address) and entries that cover far more than a proxy."""
    from fogar_gateway.core.config import settings

    if bad := invalid_entries(settings.TRUSTED_PROXIES):
        logger.warning(
            f"TRUSTED_PROXIES has invalid entries {bad!r}: no peer is a trusted proxy until it "
            "is fixed (`fogar-gw config validate`)."
        )
    if wide := wide_entries(settings.TRUSTED_PROXIES):
        logger.warning(
            f"TRUSTED_PROXIES: {wide!r} cover far more than a proxy; "
            "check them with `fogar-gw config validate`."
        )


# Headers that only a tunnel or proxy sets (tailscale serve, cloudflared, RFC 7239 `Forwarded`):
# a request carrying them is never attributed to a single address, whatever it says
# (auditoría 3.0, D-1, D-r2-1).
_TUNNEL_HEADERS = ("tailscale-user-login", "tailscale-user-name", "cf-connecting-ip", "forwarded")


def resolve_client_ip(connection: HTTPConnection) -> str:
    """
    Returns the caller's real IP, for logs.

    If the immediate TCP peer is a trusted proxy (settings.TRUSTED_PROXIES),
    its X-Real-IP is the client address. A proxy peer WITHOUT a valid X-Real-IP
    gets a non-IP sentinel (G-31, #216) instead of the proxy's own address.
    Otherwise the peer connected directly — use its address as-is and ignore any
    X-Real-IP it sends.

    D-1: a proxy that passes the client's X-Real-IP through (tailscale serve,
    cloudflared) still sets X-Forwarded-For with the real address last, while
    nginx overwriting X-Real-IP with `proxy_add_x_forwarded_for` agrees with it.
    So no result is trusted next to a tunnel header (Tailscale-User-*,
    Cf-Connecting-Ip, Forwarded) or any X-Forwarded-For hop other than itself (a tunnel without identity headers,
    tailscale serve in front of nginx, a tunnel on another TRUSTED_NETWORKS host).
    X-Real-IP must arrive exactly once (D-r2-2).
    """
    peer = connection.client.host if connection.client else ""
    if _proxies_invalid():  # fail closed: we can't tell which peer is the proxy (F-r3-1)
        return NO_REAL_IP

    if is_trusted_proxy(peer):
        real_ips = connection.headers.getlist("x-real-ip")
        if len(real_ips) != 1:  # a proxy that appends instead of overwriting (D-r2-2)
            return NO_REAL_IP
        real_ip = real_ips[0].strip()
        if "%" in real_ip:  # ip_address acepta un scope id IPv6 con texto libre (D-r3-1)
            return NO_REAL_IP
        try:
            ipaddress.ip_address(real_ip)
        except ValueError:
            return NO_REAL_IP
        # Un loopback reenviado por un proxy de otra máquina no es este host (D-r4-2).
        if _is_loopback(real_ip) and not _is_loopback(peer):
            return NO_REAL_IP
        ip = real_ip
    else:
        if connection.headers.get("x-real-ip"):
            logger.warning(
                f"Ignoring X-Real-IP header from untrusted peer {peer!r} "
                "(possible spoofing attempt)"
            )
        ip = peer

    # Confianza solo si nadie más aparece en la cadena, sea cual sea la IP (D-r2-1): un túnel sin
    # cabeceras de identidad (nodo etiquetado, Funnel), un tailscale serve delante de nginx o un
    # túnel en otra máquina de TRUSTED_NETWORKS dejan su salto en XFF.
    hops = [h.strip() for h in ",".join(connection.headers.getlist("x-forwarded-for")).split(",")]
    foreign_hop = any(h and h != ip for h in hops)
    if foreign_hop or any(h in connection.headers for h in _TUNNEL_HEADERS):
        return NO_REAL_IP
    return ip


def client_ip_for_log(connection: HTTPConnection) -> str:
    """resolve_client_ip() for logs: a sentinel also names the TCP peer, so a header can't erase it."""
    ip = resolve_client_ip(connection)
    if ip == NO_REAL_IP:
        return f"{ip}(peer={connection.client.host if connection.client else ''})"
    return ip
