"""ChatTextTracker / missing_tail (#241, #243): every tracker case lives here.

The stream (`stream_response`) and push (`deliver_push`) wiring keep two tests each, in
tests/integration/test_openclaw_real_ws.py and tests/unit/test_bridge_push.py.
"""

import logging

import pytest

from fogar_gateway.services.openclaw.chat_text import ChatTextTracker, missing_tail


def _m(text):
    return {"role": "assistant", "content": [{"type": "text", "text": text}]}


def _delta(text, message=None, replace=False):
    p = {"state": "delta", "deltaText": text}
    if message is not None:
        p["message"] = _m(message)
    if replace:
        p["replace"] = True
    return p


def _final(text):
    return {"state": "final", "message": _m(text)}


# ── missing_tail ──────────────────────────────────────────────────────────────


def test_missing_tail_policy():
    assert missing_tail("1, 2, 3,", "1, 2, 3, 4, 5") == " 4, 5"
    assert missing_tail("", "todo") == "todo"
    assert missing_tail("igual", "igual") == ""
    assert missing_tail(" Hola", "Hola mundo") == " mundo"
    assert missing_tail("abc", "xyz") == "xyz"
    assert missing_tail("algo", "") == ""


def test_real_divergence_still_emits_full_with_warning(caplog):
    caplog.set_level(logging.DEBUG)
    assert missing_tail("abc", "xyz") == "xyz"
    assert "diverges" in caplog.text


def test_whitespace_only_emitted_does_not_warn(caplog):
    caplog.set_level(logging.DEBUG)
    assert missing_tail("  ", "Hola") == "Hola"
    assert "diverges" not in caplog.text
    t = ChatTextTracker()
    t.feed({"state": "delta", "deltaText": "\n"})
    assert t.feed(_final("Hola")) == "Hola"
    assert "diverges" not in caplog.text


# ── #241: full text only in `final` ───────────────────────────────────────────


def test_final_without_any_delta_emits_full_text():
    assert ChatTextTracker().feed(_final("Hola completo")) == "Hola completo"


def test_final_with_trimmed_leading_space_emits_only_suffix():
    t = ChatTextTracker()
    assert t.feed({"state": "delta", "deltaText": " Hola"}) == " Hola"
    assert t.feed(_final("Hola mundo")) == " mundo"


def test_completely_different_final_is_emitted_in_full():
    t = ChatTextTracker()
    t.feed({"state": "delta", "deltaText": "abc"})
    assert t.feed(_final("xyz")) == "xyz"


@pytest.mark.parametrize(
    "final",
    [
        {"state": "final"},
        {"state": "final", "message": {"role": "assistant", "content": [{"type": "image"}]}},
    ],
    ids=["no_message", "no_text_parts"],
)
def test_final_without_usable_message_emits_nothing(final):
    t = ChatTextTracker()
    assert t.feed({"state": "delta", "deltaText": "Hola"}) == "Hola"
    assert t.feed(final) == ""


def test_string_content_is_accepted():
    final = {"state": "final", "message": {"role": "assistant", "content": "texto plano"}}
    assert ChatTextTracker().feed(final) == "texto plano"


def test_diverge_warns_without_content(caplog):
    caplog.set_level(logging.DEBUG)
    t = ChatTextTracker()
    t.feed({"state": "delta", "deltaText": "PRIVADO uno"})
    assert t.feed(_final("PRIVADO dos")) == "PRIVADO dos"
    assert "diverges" in caplog.text
    assert "PRIVADO" not in caplog.text


# ── #243: delta / replace / snapshot ──────────────────────────────────────────


def test_captured_sequence():
    t = ChatTextTracker()
    out = [
        t.feed(_delta("1, 2, 3,", "1, 2, 3,")),
        t.feed(_delta("", "", replace=True)),
        t.feed(_final("1, 2, 3, 4, 5")),
    ]
    assert out == ["1, 2, 3,", "", " 4, 5"]


def test_no_duplicate_after_empty_replace_delta_without_message_and_full_final():
    t = ChatTextTracker()
    out = [
        t.feed(_delta("1, 2, 3,", "1, 2, 3,")),
        t.feed(_delta("", "", replace=True)),
        t.feed(_delta(" 4")),
        t.feed(_final("1, 2, 3, 4, 5")),
    ]
    assert "".join(out) == "1, 2, 3, 4, 5"
    assert out == ["1, 2, 3,", "", " 4", ", 5"]


def test_replace_nonempty_does_not_emit_whole_text_as_addition(caplog):
    caplog.set_level(logging.DEBUG)
    t = ChatTextTracker()
    assert t.feed(_delta("Hola mun", "Hola mun")) == "Hola mun"
    assert t.feed(_delta("Hola, mundo", "Hola, mundo", replace=True)) == "Hola, mundo"
    assert "diverges" in caplog.text
    assert "Hola" not in caplog.text


def test_replace_extending_emitted_emits_only_suffix():
    t = ChatTextTracker()
    t.feed(_delta("Hola", "Hola"))
    assert t.feed(_delta("Hola mundo", "Hola mundo", replace=True)) == " mundo"


def test_first_frame_snapshot_includes_delta_once():
    t = ChatTextTracker()
    assert t.feed(_delta("Hola", "Hola")) == "Hola"
    assert t.feed(_delta(" mundo")) == " mundo"
    assert t.feed(_final("Hola mundo")) == ""


def test_join_mid_run_emits_whole_snapshot():
    assert ChatTextTracker().feed(_delta("mundo", "Hola mundo")) == "Hola mundo"


def test_later_delta_without_message_adds_only_addition():
    t = ChatTextTracker()
    t.feed(_delta("Hola", "Hola"))
    assert t.feed(_delta(" a")) == " a"
    assert t.feed(_delta(" todos")) == " todos"
    assert t.feed(_final("Hola a todos")) == ""


def test_snapshot_only_frame_applies_missing():
    t = ChatTextTracker()
    t.feed(_delta("Hola", "Hola"))
    assert t.feed({"state": "delta", "message": _m("Hola mundo")}) == " mundo"


def test_final_correction_diverges(caplog):
    caplog.set_level(logging.DEBUG)
    t = ChatTextTracker()
    t.feed(_delta("SECRETO uno", "SECRETO uno"))
    assert t.feed(_final("SECRETO dos")) == "SECRETO dos"
    assert "diverges" in caplog.text
    assert "SECRETO" not in caplog.text


def test_replace_empty_delta_does_not_change_emitted():
    t = ChatTextTracker()
    assert t.feed({"state": "delta", "deltaText": "1, 2, 3,"}) == "1, 2, 3,"
    assert t.feed({"state": "delta", "deltaText": "", "replace": True}) == ""
    assert t.feed(_final("1, 2, 3, 4, 5")) == " 4, 5"


def test_reset_between_responses():
    t = ChatTextTracker()
    t.feed(_delta("uno", "uno"))
    t.feed(_final("uno"))
    assert t.feed(_delta("dos", "dos")) == "dos"


def test_final_resets_for_next_response_without_snapshot():
    t = ChatTextTracker()
    assert t.feed(_final("completo")) == "completo"
    assert t.feed({"state": "delta", "deltaText": "otra"}) == "otra"
    assert t.feed(_final("otra")) == ""


def test_all_out_resets_with_tracker():
    t = ChatTextTracker()
    t.feed(_delta("uno", "uno"))
    t.reset()
    assert t.feed(_final("dos")) == "dos"


def test_aborted_and_error_do_not_emit_text_tail():
    for state in ("aborted", "error"):
        t = ChatTextTracker()
        assert t.feed({"state": "delta", "deltaText": "Hola"}) == "Hola"
        assert t.feed({"state": state, "message": _m("Hola mundo")}) == ""


# ── #243: retraction to a strict prefix of what was emitted ──────────────────


def test_final_retracted_to_prefix_emits_nothing(caplog):
    caplog.set_level(logging.DEBUG)
    t = ChatTextTracker()
    assert t.feed({"state": "delta", "deltaText": "SECRETO, mundo. Adios"}) == "SECRETO, mundo. Adios"
    assert t.feed(_final("SECRETO, mundo.")) == ""
    assert "retracted" in caplog.text
    assert "diverges" not in caplog.text
    assert "SECRETO" not in caplog.text
    assert not [r for r in caplog.records if r.levelno >= logging.WARNING]


def test_replace_retracted_to_prefix_emits_nothing(caplog):
    caplog.set_level(logging.DEBUG)
    t = ChatTextTracker()
    t.feed(_delta("SECRETO, mundo. Adios", "SECRETO, mundo. Adios"))
    assert t.feed(_delta("SECRETO, mundo.", "SECRETO, mundo.", replace=True)) == ""
    assert "diverges" not in caplog.text
    assert "SECRETO" not in caplog.text


def test_growth_after_retraction_emits_only_new_tail():
    t = ChatTextTracker()
    t.feed({"state": "delta", "deltaText": "Hola mundo"})
    assert t.feed({"state": "delta", "deltaText": "Hola", "replace": True}) == ""
    assert t.feed({"state": "delta", "deltaText": " mundo!"}) == "!"


def test_emoji_split_between_two_snapshots_is_not_emitted_twice():
    """G-r3-1 (revisión): «Hola \\ud83d» y luego «Hola 😀 amigo». Sin el surrogate suelto (ni
    sustituto) el primero sigue siendo prefijo del segundo: nada se repite."""
    import json

    from fogar_gateway.services.openclaw.client import _loads_frame

    t = ChatTextTracker()
    first = _loads_frame(json.dumps({"payload": _delta("", message="Hola \ud83d")}))
    second = _loads_frame(json.dumps({"payload": _delta("", message="Hola \U0001f600 amigo")}))
    out = t.feed(first["payload"]) + t.feed(second["payload"])
    assert out == "Hola \U0001f600 amigo"


def test_emoji_split_between_two_delta_texts_is_joined():
    """B-r4-1: la mitad alta espera a su pareja; nada se reemite al llegar el final."""
    t = ChatTextTracker()
    out = [
        t.feed(_delta("Hola ", message="Hola ")),
        t.feed({"state": "delta", "deltaText": "amigo \ud83d"}),
        t.feed({"state": "delta", "deltaText": "\ude00 qué tal"}),
        t.feed({"state": "final", "message": {"content": "Hola amigo \U0001f600 qué tal"}}),
    ]
    assert out == ["Hola ", "amigo ", "\U0001f600 qué tal", ""]
    assert "".join(out).encode()  # UTF-8 válido


def test_loads_frame_keeps_chat_text_halves_but_cleans_the_rest():
    import json

    from fogar_gateway.services.openclaw.client import _loads_frame

    chat = {"sessionKey": "k", "state": "d\ud800", "deltaText": "a\ud83d", "message": {"content": "b\ude00"}}
    frame = _loads_frame(json.dumps({"type": "event", "event": "chat", "payload": chat}))
    assert frame["payload"] == {"sessionKey": "k", "state": "d", "deltaText": "a\ud83d", "message": {"content": "b\ude00"}}
    other = _loads_frame(json.dumps({"type": "res", "payload": {"title": "t\ud800"}}))
    assert other["payload"]["title"] == "t"


@pytest.mark.parametrize("key", ["sessionKey", "runId"])
@pytest.mark.parametrize("event", ["chat", "agent"])
def test_loads_frame_drops_a_frame_whose_routing_key_would_change(key, event):
    """B-r5-1: "k" + surrogate suelto, limpio, sería "k", la clave de otra sesión: la trama se descarta
    (ValueError, que _listen registra solo por tipo) en vez de enrutarse."""
    import json

    from fogar_gateway.services.openclaw.client import _loads_frame

    payload = {"sessionKey": "k", "runId": "r", key: "k\ud800", "deltaText": "hola"}
    with pytest.raises(ValueError):
        _loads_frame(json.dumps({"type": "event", "event": event, "payload": payload}))


def test_loads_frame_binary_chat_keeps_text_halves():
    """B-r5-3: trama binaria (utf-16 con surrogates crudos): deltaText conserva la mitad para
    ChatTextTracker, el resto se limpia y una sessionKey con surrogate descarta la trama."""
    import json

    from fogar_gateway.services.openclaw.client import _loads_frame

    def raw(payload):
        frame = {"type": "event", "event": "chat", "payload": payload}
        return json.dumps(frame, ensure_ascii=False).encode("utf-16", "surrogatepass")

    frame = _loads_frame(raw({"sessionKey": "k", "state": "d\ud800", "deltaText": "a\ud83d"}))
    assert frame["payload"] == {"sessionKey": "k", "state": "d", "deltaText": "a\ud83d"}
    with pytest.raises(ValueError):
        _loads_frame(raw({"sessionKey": "k\ud800", "deltaText": "a"}))


def test_loads_frame_bytes_with_raw_surrogate_are_valid_utf8():
    """C-r4-1: bytes UTF-8 de un surrogate crudo (sin el escape \\ud) también se sanean, sin
    carácter de sustitución."""
    from fogar_gateway.services.openclaw.client import _loads_frame

    frame = _loads_frame(b'{"type": "res", "payload": {"title": "t\xed\xa0\x80"}}')
    assert frame["payload"]["title"] == "t"  # sin U+FFFD ni surrogate


def test_text_without_surrogates_is_not_reencoded(monkeypatch):
    """Revisión r4: sin surrogates no se re-codifica `current` en cada delta (O(n²))."""
    from fogar_gateway.services.openclaw import chat_text

    calls = []
    monkeypatch.setattr(chat_text, "_paired", lambda s: calls.append(s) or s)
    t = ChatTextTracker()
    for word in ("Hola ", "amigo ", "qué tal"):
        t.feed({"state": "delta", "deltaText": word})
    assert calls == []
    t.feed({"state": "delta", "deltaText": " \ud83d"})
    t.feed({"state": "delta", "deltaText": "\ude00"})
    assert len(calls) == 2
    t.feed({"state": "final", "message": {"content": "otro"}})
    assert len(calls) == 2  # un snapshot nuevo sin surrogates
