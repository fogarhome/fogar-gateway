import logging

import pytest
from starlette.datastructures import Headers
from starlette.websockets import WebSocket

from fogar_gateway.core.config import settings
from fogar_gateway.core.network import (
    NO_REAL_IP,
    client_ip_for_log,
    is_trusted_origin,
    is_trusted_proxy,
    resolve_client_ip,
    warn_if_open_proxy_config,
)


class _FakeClient:
    def __init__(self, host):
        self.host = host


class _FakeRequest:
    def __init__(self, host, headers=None):
        self.client = _FakeClient(host)
        self.headers = Headers(headers or {})


@pytest.fixture(autouse=True)
def _reset_network_settings(monkeypatch):
    monkeypatch.setattr(settings, "TRUST_LOOPBACK", True)
    monkeypatch.setattr(settings, "TRUSTED_NETWORKS", "")
    monkeypatch.setattr(settings, "TRUSTED_PROXIES", "127.0.0.1,::1")


def test_loopback_trusted_by_default():
    assert is_trusted_origin("127.0.0.1") is True
    assert is_trusted_origin("::1") is True


def test_loopback_untrusted_when_disabled(monkeypatch):
    monkeypatch.setattr(settings, "TRUST_LOOPBACK", False)
    assert is_trusted_origin("127.0.0.1") is False


def test_ip_in_trusted_networks_is_trusted(monkeypatch):
    monkeypatch.setattr(settings, "TRUSTED_NETWORKS", "192.168.1.0/24")
    assert is_trusted_origin("192.168.1.42") is True


def test_ip_outside_trusted_networks_is_untrusted(monkeypatch):
    monkeypatch.setattr(settings, "TRUSTED_NETWORKS", "192.168.1.0/24")
    assert is_trusted_origin("203.0.113.5") is False


def test_malformed_ip_is_untrusted():
    assert is_trusted_origin("testclient") is False


def test_is_trusted_proxy_matches_default_loopback():
    assert is_trusted_proxy("127.0.0.1") is True
    assert is_trusted_proxy("203.0.113.5") is False


def test_resolve_client_ip_honors_x_real_ip_from_trusted_proxy():
    request = _FakeRequest("127.0.0.1", headers={"x-real-ip": "192.168.1.50"})
    assert resolve_client_ip(request) == "192.168.1.50"


def test_resolve_client_ip_ignores_x_real_ip_from_untrusted_peer():
    request = _FakeRequest("203.0.113.5", headers={"x-real-ip": "127.0.0.1"})
    assert resolve_client_ip(request) == "203.0.113.5"


def test_resolve_client_ip_trusted_proxy_without_header_fails_closed():
    # G-31 (#216): a same-host proxy that sets no X-Real-IP must not look like loopback.
    request = _FakeRequest("127.0.0.1")
    assert is_trusted_origin(resolve_client_ip(request)) is False


def test_resolve_client_ip_trusted_proxy_with_invalid_header_fails_closed():
    for bad in ("", "  ", "garbage", "1.2.3.4, 5.6.7.8"):
        request = _FakeRequest("127.0.0.1", headers={"x-real-ip": bad})
        assert is_trusted_origin(resolve_client_ip(request)) is False


def test_nginx_overwriting_x_real_ip_with_matching_xff_is_honored():
    # nginx: X-Real-IP $remote_addr + proxy_add_x_forwarded_for (real IP last).
    request = _FakeRequest("127.0.0.1", headers={"x-real-ip": "192.168.1.50", "x-forwarded-for": "192.168.1.50"})
    assert resolve_client_ip(request) == "192.168.1.50"
    # D-r2-1: an earlier hop (sent by the client or by a tunnel in front) is no longer accepted.
    request = _FakeRequest(
        "127.0.0.1", headers={"x-real-ip": "192.168.1.50", "x-forwarded-for": "10.9.9.9, 192.168.1.50"}
    )
    assert resolve_client_ip(request) == NO_REAL_IP


@pytest.mark.parametrize("tunnel", [{"cf-connecting-ip": "203.0.113.9"}, {"tailscale-user-login": "a@b"}])
def test_tunnel_on_trusted_network_host_is_not_trusted(monkeypatch, tunnel):
    # D-r2-1: cloudflared / tailscale serve on another LAN machine inside TRUSTED_NETWORKS.
    monkeypatch.setattr(settings, "TRUSTED_NETWORKS", "192.168.1.0/24")
    direct = _FakeRequest("192.168.1.5", headers={**tunnel, "x-forwarded-for": "203.0.113.9"})
    assert not is_trusted_origin(resolve_client_ip(direct))
    # Same with a LAN proxy in TRUSTED_PROXIES vouching for itself.
    monkeypatch.setattr(settings, "TRUSTED_PROXIES", "192.168.1.10")
    proxied = _FakeRequest(
        "192.168.1.10",
        headers={"x-real-ip": "192.168.1.10", "x-forwarded-for": "203.0.113.9, 192.168.1.10", **tunnel},
    )
    assert not is_trusted_origin(resolve_client_ip(proxied))


@pytest.mark.parametrize(
    "headers",
    [
        {"cf-connecting-ip": "203.0.113.9"},
        {"tailscale-user-login": "a@b", "x-forwarded-for": "192.168.1.5"},
        {"forwarded": "for=203.0.113.9"},  # RFC 7239
    ],
)
def test_tunnel_or_forwarded_header_alone_on_trusted_network_is_not_trusted(monkeypatch, headers):
    # Sin XFF (o con XFF igual al peer): la cabecera de túnel o Forwarded basta para negar.
    monkeypatch.setattr(settings, "TRUSTED_NETWORKS", "192.168.1.0/24")
    assert not is_trusted_origin(resolve_client_ip(_FakeRequest("192.168.1.5", headers=headers)))


def test_client_ip_for_log_keeps_the_peer_on_sentinel():
    # Una cabecera no borra del log la IP del atacante.
    request = _FakeRequest("203.0.113.5", headers={"cf-connecting-ip": "1.2.3.4"})
    assert client_ip_for_log(request) == f"{NO_REAL_IP}(peer=203.0.113.5)"
    assert client_ip_for_log(_FakeRequest("203.0.113.5")) == "203.0.113.5"


def test_direct_trusted_network_peer_without_proxy_headers_is_trusted(monkeypatch):
    # Home Assistant straight to the gateway: no XFF, no tunnel headers.
    monkeypatch.setattr(settings, "TRUSTED_NETWORKS", "192.168.1.0/24")
    assert is_trusted_origin(resolve_client_ip(_FakeRequest("192.168.1.5")))


@pytest.mark.parametrize("values", [["127.0.0.1", "203.0.113.5"], ["203.0.113.5", "127.0.0.1"]])
def test_repeated_x_real_ip_fails_closed(values):
    # D-r2-2: a proxy that appends X-Real-IP instead of overwriting it.
    request = _FakeRequest("127.0.0.1")
    request.headers = Headers(raw=[(b"x-real-ip", v.encode()) for v in values])
    assert not is_trusted_origin(resolve_client_ip(request))


def test_proxy_passing_client_x_real_ip_through_fails_closed():
    # D-1: tailscale serve / cloudflared keep the client's X-Real-IP but put the real
    # address last in X-Forwarded-For; a disagreement is never trusted.
    request = _FakeRequest(
        "127.0.0.1", headers={"x-real-ip": "127.0.0.1", "x-forwarded-for": "100.64.0.7"}
    )
    assert is_trusted_origin(resolve_client_ip(request)) is False


def test_nginx_with_client_injected_xff_fails_closed():
    # nginx that only overwrites X-Real-IP forwards the client's XFF untouched.
    request = _FakeRequest(
        "127.0.0.1", headers={"x-real-ip": "203.0.113.5", "x-forwarded-for": "127.0.0.1"}
    )
    assert is_trusted_origin(resolve_client_ip(request)) is False


@pytest.mark.parametrize("header", ["tailscale-user-login", "tailscale-user-name", "cf-connecting-ip"])
def test_tunnel_headers_never_earn_loopback_exemption(monkeypatch, header):
    vouched = _FakeRequest("127.0.0.1", headers={"x-real-ip": "127.0.0.1", header: "x"})
    assert is_trusted_origin(resolve_client_ip(vouched)) is False
    # Also without a proxy list (`TRUSTED_PROXIES=` with `fogar-gw access`).
    monkeypatch.setattr(settings, "TRUSTED_PROXIES", "")
    direct = _FakeRequest("127.0.0.1", headers={header: "x"})
    assert is_trusted_origin(resolve_client_ip(direct)) is False


def test_resolve_client_ip_direct_peer_unchanged():
    request = _FakeRequest("192.168.1.9")
    assert resolve_client_ip(request) == "192.168.1.9"


def test_resolve_client_ip_accepts_websocket_connection():
    async def dummy_receive():
        return {"type": "websocket.connect"}

    async def dummy_send(data):
        pass

    websocket = WebSocket(
        {
            "type": "websocket",
            "asgi": {"version": "3.0", "spec_version": "2.3"},
            "scheme": "ws",
            "path": "/ws/stream",
            "raw_path": b"/ws/stream",
            "query_string": b"",
            "headers": [(b"x-real-ip", b"192.168.1.50")],
            "client": ("127.0.0.1", 50000),
            "server": ("testserver", 80),
            "subprotocols": [],
            "state": {},
        },
        receive=dummy_receive,
        send=dummy_send,
    )

    assert resolve_client_ip(websocket) == "192.168.1.50"


def test_tunnel_without_identity_headers_and_no_proxies_is_not_loopback(monkeypatch):
    # D-1 (revisión): tailscale serve desde un nodo etiquetado o Funnel no manda Tailscale-User-*;
    # con TRUSTED_PROXIES vacío el peer 127.0.0.1 no basta si XFF trae un salto de fuera.
    monkeypatch.setattr(settings, "TRUSTED_PROXIES", "")
    ip = resolve_client_ip(_FakeRequest("127.0.0.1", headers={"x-forwarded-for": "100.101.102.103"}))
    assert not is_trusted_origin(ip)


def test_tunnel_in_front_of_nginx_is_not_loopback():
    # D-1 (revisión): tailscale serve -> nginx -> gateway: nginx pone X-Real-IP 127.0.0.1 y
    # deja el salto del túnel en XFF.
    request = _FakeRequest(
        "127.0.0.1", headers={"x-real-ip": "127.0.0.1", "x-forwarded-for": "100.101.102.103, 127.0.0.1"}
    )
    assert not is_trusted_origin(resolve_client_ip(request))


def test_loopback_client_behind_same_host_nginx_still_trusted():
    # Home Assistant en el mismo equipo detrás del nginx documentado: toda la cadena es loopback.
    request = _FakeRequest("127.0.0.1", headers={"x-real-ip": "127.0.0.1", "x-forwarded-for": "127.0.0.1"})
    assert is_trusted_origin(resolve_client_ip(request))


def test_x_real_ip_with_ipv6_scope_id_fails_closed(caplog):
    # D-r3-1: ip_address acepta "::1%<texto libre>"; ese texto acabaría en los logs como loopback.
    request = _FakeRequest("127.0.0.1", headers={"x-real-ip": "::1%x request_id=FORGED"})
    assert resolve_client_ip(request) == NO_REAL_IP
    assert "FORGED" not in client_ip_for_log(request)
    assert "FORGED" not in caplog.text



@pytest.mark.parametrize("bad", ["127.0.0.1;::1", "localhost", "192.168.1.0/33", "::1%eth0", "0.0.0.0/0"])
def test_invalid_trusted_proxies_fail_closed_for_everyone(monkeypatch, caplog, bad):
    # F-r3-1: antes la entrada se ignoraba (lista vacía) y un proxy del mismo host que solo pone
    # X-Real-IP abría /v1. Ahora nadie es proxy ni origen exento, y el aviso sale al arrancar.
    monkeypatch.setattr(settings, "TRUSTED_PROXIES", f"127.0.0.1,{bad}")
    monkeypatch.setattr(settings, "TRUSTED_NETWORKS", "192.168.1.0/24")
    with caplog.at_level(logging.WARNING):
        for request in (
            _FakeRequest("127.0.0.1", headers={"x-real-ip": "192.168.1.9"}),
            _FakeRequest("127.0.0.1"),
            _FakeRequest("192.168.1.5"),
        ):
            assert resolve_client_ip(request) == NO_REAL_IP
        assert not caplog.records  # nada por petición
        warn_if_open_proxy_config()
    assert len(caplog.records) == 1 and "TRUSTED_PROXIES" in caplog.text and bad in caplog.text


@pytest.mark.parametrize("bad", ["nope", "0.0.0.0/0", "::/0"])
def test_invalid_or_catch_all_trusted_networks_are_ignored(monkeypatch, caplog, bad):
    monkeypatch.setattr(settings, "TRUSTED_NETWORKS", f"{bad},192.168.1.0/24")
    assert is_trusted_origin("192.168.1.5")
    assert not is_trusted_origin("203.0.113.9") and not is_trusted_origin("2001:db8::1")
    with caplog.at_level(logging.WARNING):
        warn_if_open_proxy_config()
    assert "TRUSTED_NETWORKS" in caplog.text and bad in caplog.text


def test_valid_lists_with_spaces_ipv6_and_host_bits_still_work(monkeypatch):
    monkeypatch.setattr(settings, "TRUSTED_PROXIES", " 127.0.0.1 , ::1 ,, fd00::/8")
    monkeypatch.setattr(settings, "TRUSTED_NETWORKS", "10.0.0.0/8, 192.168.1.5/24")
    assert resolve_client_ip(_FakeRequest("::1", headers={"x-real-ip": "192.168.1.7"})) == "192.168.1.7"
    assert is_trusted_origin("192.168.1.7") and is_trusted_origin("10.1.2.3")
