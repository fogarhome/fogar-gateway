"""M20: cada conversación tiene hogar, dispositivo y (opcional) dueño; quién la ve y a quién
llega su push."""

from types import SimpleNamespace

import pytest
from sqlmodel import Session, SQLModel, create_engine

from fogar_gateway.db.models import ClientRecord, MembershipRecord, UserRecord
from fogar_gateway.services.auth import hash_key
from fogar_gateway.services.conversations import (
    Conversation,
    ConversationNotFound,
    ConversationService,
    session_key_for,
)
from fogar_gateway.services.openclaw.registry import ClientRegistry

HOME = "home"


@pytest.fixture
def service(tmp_path):
    engine = create_engine(f"sqlite:///{tmp_path / 'gateway.db'}")
    SQLModel.metadata.create_all(engine)
    with Session(engine) as s:
        for uid in ("sito", "ana"):
            s.add(UserRecord(id=uid, username=uid, display_name=uid, password_hash="x"))
        s.commit()
        for uid in ("sito", "ana"):
            s.add(MembershipRecord(user_id=uid, home_id=HOME, role="member"))
        # La tele y el altavoz, del hogar; el Mac y el iPad de Sito y el Mac de Ana, personales.
        for cid, user in (("tv", None), ("altavoz", None), ("mac", "sito"), ("ipad", "sito"),
                          ("ana-mac", "ana")):
            s.add(ClientRecord(id=cid, name=cid, key_hash=hash_key(cid), user_id=user))
        s.commit()
    return ConversationService(engine)


async def test_the_owner_and_home_come_from_the_device(service):
    tv = await service.default_for("tv", "main")
    mac = await service.create("mac", "main")
    assert (tv.home_id, tv.owner_user_id) == (HOME, None)  # del hogar
    assert (mac.home_id, mac.owner_user_id) == (HOME, "sito")  # privada de Sito


@pytest.mark.parametrize(
    ("viewer", "sees_home", "sees_sitos"),
    [
        ((HOME, None), True, False),  # un dispositivo del hogar: solo lo común
        ((HOME, "sito"), True, True),  # cualquier dispositivo de Sito: lo común y lo suyo
        ((HOME, "ana"), True, False),  # Ana no ve lo de Sito
        (("otro-hogar", "sito"), False, False),
    ],
)
async def test_who_can_open_what(service, viewer, sees_home, sees_sitos):
    home = await service.default_for("tv", "main")
    sitos = await service.create("mac", "main")
    for conv, expected in ((home, sees_home), (sitos, sees_sitos)):
        if expected:
            assert (await service.get_visible(conv.id, *viewer)).id == conv.id
        else:
            with pytest.raises(ConversationNotFound):
                await service.get_visible(conv.id, *viewer)


async def test_the_list_is_filtered_by_who_looks(service):
    home = await service.create("altavoz", "main")
    sitos = await service.create("ipad", "main")
    anas = await service.create("ana-mac", "main")
    ids = lambda rows: {r.record.id for r in rows}  # noqa: E731
    assert ids(await service.list_rows()) == {home.id, sitos.id, anas.id}  # la máquina
    assert ids(await service.list_rows(visible_to=(HOME, "ana"))) == {home.id, anas.id}
    assert ids(await service.list_rows(visible_to=(HOME, None))) == {home.id}


def _bridge(user_id):
    return SimpleNamespace(client=SimpleNamespace(user_id=user_id))


def _conv(cid, client, owner=None):
    return Conversation(cid, client, "main", session_key_for("main", cid), "created",
                        owner_user_id=owner)


def test_a_private_push_never_reaches_a_home_device_or_someone_else():
    reg = ClientRegistry()
    tv, mac = _bridge(None), _bridge("sito")
    reg.register("tv", tv, _conv("hogar", "tv"))
    reg.register("mac", mac, _conv("priv", "mac", "sito"))
    assert reg.route(session_key_for("main", "priv")) is mac
    assert reg.route(session_key_for("main", "hogar")) is tv
    # Aunque algo saliera mal y la tele tuviera abierta una privada: no le llega.
    reg.register("tv", tv, _conv("priv2", "tv", "sito"))
    assert reg.route(session_key_for("main", "priv2")) is None
    reg.register("ana", _bridge("ana"), _conv("priv3", "ana", "sito"))
    assert reg.route(session_key_for("main", "priv3")) is None
