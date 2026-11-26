"""`/events` por aparato (5.0, #412): un adaptador por canal visible, ids que siempre suben,
reanudar o `resync`, y la entrega de avisos de la 2c intacta. Canales reales en un hub (como en
`test_channel_notify`), sin HTTP: la ruta se prueba en `tests/integration/test_events_route.py`.
"""

import json

import pytest

from fogar_gateway.models.schemas import Client, ClientConfig
from fogar_gateway.services import events as ev
from fogar_gateway.services.channel import ChannelHub
from tests.unit import test_channel_notify as notify
from tests.unit.test_channel_notify import HOME, OTHER, PRIVATE, assert_pairs, push, settle, texts

hub = notify.hub  # las fixtures de test_channel_notify (canales reales en un hub)
session = notify.session


def _client(client_id="tablet", user_id=None) -> Client:
    return Client(id=client_id, key_hash="k", is_active=True, user_id=user_id)


def _events(device: ev.DeviceEvents) -> list[tuple[int, dict]]:
    out = []
    for _, line in device._buffer:
        head, data = line.split("\n")[:2]
        out.append((int(head.removeprefix("id: ")), json.loads(data.removeprefix("data: "))))
    return out


def _kinds(device) -> list[tuple]:
    return [
        (e["type"], e.get("conversation"), e.get("text") or e.get("origin") or e.get("reason"))
        for _, e in _events(device)
    ]


@pytest.fixture
def events(hub: ChannelHub):
    return ev.EventHub(hub)


async def test_a_push_reaches_the_device_with_ids_that_always_grow(hub, events):
    device = events.open(_client(), ClientConfig())
    await push(hub, HOME, "hola", "!")
    assert _kinds(device) == [
        ("turn_start", "conv-home", "push"),
        ("token", "conv-home", "hola"),
        ("token", "conv-home", "!"),
        ("turn_end", "conv-home", "done"),
    ]
    ids = [i for i, _ in _events(device)]
    assert ids == sorted(ids) and len(set(ids)) == len(ids)
    start, end = _events(device)[0][1], _events(device)[-1][1]
    assert start["turn_id"] == end["turn_id"] and start["by"] is None


async def test_one_stream_sees_every_visible_conversation_and_new_channels(hub, events):
    device = events.open(_client(), ClientConfig())
    await push(hub, HOME, "uno")  # el canal nace después de abrir /events
    await push(hub, OTHER, "dos")
    assert [t for _, c, t in _kinds(device) if c] == ["push", "uno", "done", "push", "dos", "done"]
    assert {c for _, c, _ in _kinds(device)} == {"conv-home", "conv-other"}


async def test_private_push_never_reaches_a_home_device_nor_another_person(hub, events):
    tablet = events.open(_client("tablet"), ClientConfig())
    luis = events.open(_client("luis-phone", user_id="luis"), ClientConfig())
    ana = events.open(_client("ana-phone", user_id="ana"), ClientConfig())
    await push(hub, PRIVATE, "secreto")
    assert _kinds(tablet) == [] and _kinds(luis) == []
    assert ("token", "conv-private", "secreto") in _kinds(ana)


async def test_joining_mid_turn_gets_its_start_and_the_text_so_far(hub, events):
    channel = await push(hub, HOME, "a medias", end=False)
    device = events.open(_client(), ClientConfig())
    assert _kinds(device) == [("turn_start", "conv-home", "push"), ("token", "conv-home", "a medias")]
    channel.enqueue_push("turn_end", HOME.session_key)
    await channel.push_idle()
    assert _kinds(device)[-1] == ("turn_end", "conv-home", "done")


async def test_push_disabled_or_agent_not_allowed_gets_nothing(hub, events):
    off = events.open(_client("off"), ClientConfig(push_enabled=False))
    other_agent = events.open(_client("x"), ClientConfig(allowed_agents=["otro"]))
    await push(hub, HOME, "aviso")
    assert _kinds(off) == [] and _kinds(other_agent) == []


async def test_resume_replays_what_was_missed_and_a_gap_is_a_resync(hub, events, monkeypatch):
    monkeypatch.setattr(ev, "BUFFER_MAX", 4)
    device = events.open(_client(), ClientConfig())
    await push(hub, HOME, "uno")  # 3 eventos
    first = _events(device)[0][0]
    assert len(device.replay(first)) == 2
    assert device.replay(ev.current_id()) == []
    assert device.replay(ev.current_id() + 10) is None  # de otro proceso
    await push(hub, HOME, "dos")  # el búfer (4) ya no tiene el primero
    assert device.replay(first - 1) is None
    assert device.replay(_events(device)[0][0]) is not None


async def test_a_text_only_notify_device_still_gets_its_push_with_a_web_open(session, hub, events):
    # 2c sin cambios: un /events adjunto (una web abierta) no cuenta como «alguien lo ve».
    reader, reader_ws = session("reader", OTHER, output_mode=("text",))
    hub.notify[HOME.id] = "reader"
    web = events.open(_client("web", user_id="sito"), ClientConfig())
    await push(hub, HOME, "recordatorio")
    await settle(reader)
    assert texts(reader_ws) == ["recordatorio"]
    assert_pairs(reader_ws)
    assert ("token", "conv-home", "recordatorio") in _kinds(web)


async def test_closing_lets_go_of_the_channels(hub, events):
    device = events.open(_client(), ClientConfig())
    await push(hub, HOME, "uno")
    channel = hub.get(HOME.session_key)
    assert channel is not None and channel.subscribers()
    listener = device.listen()
    events.close_device("tablet")
    assert hub.get(HOME.session_key) is None  # sin nadie: el canal se suelta
    assert listener.queue.get_nowait() is None  # el stream termina
    assert events.device("tablet") is None


async def test_a_slow_reader_is_cut_and_resumes_from_the_buffer(hub, events, monkeypatch):
    monkeypatch.setattr(ev, "QUEUE_MAX", 2)
    device = events.open(_client(), ClientConfig())
    listener = device.listen()
    await push(hub, HOME, "uno", "dos", "tres")
    assert listener.overflowed
    items = []
    while not listener.queue.empty():
        items.append(listener.queue.get_nowait())
    assert items[-1] is None  # el centinela: el stream termina
    assert len(_events(device)) == 5  # el búfer lo tiene todo
