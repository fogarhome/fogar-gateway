import logging

import pytest
from pydantic import SecretStr, ValidationError

from fogar_gateway.core.config import Settings
from fogar_gateway.core.startup_checks import SpeechConfigError, check_speech_settings
from fogar_gateway.services.speech import voices
from fogar_gateway.services.speech.voices import resolve_voice


@pytest.fixture(autouse=True)
def _reset_warned(monkeypatch):
    monkeypatch.delenv("ELEVENLABS_API_KEY", raising=False)
    voices._warned.clear()
    yield
    voices._warned.clear()


def _resolve(configured, **kw):
    return resolve_voice(
        configured,
        voice_map=kw.get("voice_map", {"af_heart": "EL_ID"}),
        accepts_native=kw.get("accepts_native", lambda v: v.startswith("native_")),
        default=kw.get("default", "DEFAULT"),
    )


def test_resolve_voice_map_then_native_then_default():
    assert _resolve("af_heart") == "EL_ID"
    assert _resolve("native_x") == "native_x"
    assert _resolve("other") == "DEFAULT"
    assert _resolve(None) == "DEFAULT"


def test_resolve_voice_warns_once_per_value(caplog):
    with caplog.at_level(logging.WARNING, logger=voices.logger.name):
        _resolve("other")
        _resolve("other")
        _resolve("another")
    assert len([r for r in caplog.records if r.levelno == logging.WARNING]) == 2


def _s(**kw):
    return Settings(_env_file=None, **kw)


def test_startup_fails_without_key_when_elevenlabs():
    s = _s(ELEVENLABS_TTS_DEFAULT_VOICE="v1")
    with pytest.raises(SpeechConfigError) as e:
        check_speech_settings(s, stt_provider="elevenlabs", tts_provider="fogar")
    assert "ELEVENLABS_API_KEY" in str(e.value)
    s2 = _s(ELEVENLABS_API_KEY="   ", ELEVENLABS_TTS_DEFAULT_VOICE="v1")
    with pytest.raises(SpeechConfigError):
        check_speech_settings(s2, stt_provider="fogar", tts_provider="elevenlabs")
    secret = "sk-very-secret-value"
    ok = _s(ELEVENLABS_API_KEY=secret, ELEVENLABS_TTS_DEFAULT_VOICE="v1")
    check_speech_settings(ok, stt_provider="elevenlabs", tts_provider="elevenlabs")
    assert isinstance(ok.ELEVENLABS_API_KEY, SecretStr)
    check_speech_settings(_s(), stt_provider="fogar", tts_provider="fogar")


def test_tts_elevenlabs_requires_default_voice():
    assert _s().ELEVENLABS_TTS_DEFAULT_VOICE == "gc61tTk93h3LBXPxew2V"
    check_speech_settings(_s(ELEVENLABS_API_KEY="k"), stt_provider="fogar", tts_provider="elevenlabs")
    s = _s(ELEVENLABS_API_KEY="k", ELEVENLABS_TTS_DEFAULT_VOICE="  ")
    with pytest.raises(SpeechConfigError) as e:
        check_speech_settings(s, stt_provider="fogar", tts_provider="elevenlabs")
    assert "ELEVENLABS_TTS_DEFAULT_VOICE" in str(e.value)


@pytest.mark.parametrize("model", ["eleven_v3", "eleven_v4_beta"])
def test_rejects_v3_model_for_ws(model):
    s = _s(ELEVENLABS_API_KEY="k", ELEVENLABS_TTS_DEFAULT_VOICE="v", ELEVENLABS_TTS_MODEL=model)
    with pytest.raises(SpeechConfigError) as e:
        check_speech_settings(s, stt_provider="fogar", tts_provider="elevenlabs")
    assert "ELEVENLABS_TTS_MODEL" in str(e.value) and model not in str(e.value)


def test_stt_audio_gate_auto_from_4x_is_energy_with_one_warning(monkeypatch, caplog):
    """#422 quitó «auto» (era «energy»): un config.env de la 4.x arranca igual, con un aviso."""
    from fogar_gateway.core import config as config_module

    monkeypatch.setattr(config_module._logger, "auto_gate_warned", False, raising=False)
    monkeypatch.setenv("STT_AUDIO_GATE", "auto")
    caplog.set_level(logging.WARNING)
    assert Settings().STT_AUDIO_GATE == "energy"
    assert Settings().STT_AUDIO_GATE == "energy"
    warnings = [r.getMessage() for r in caplog.records if "STT_AUDIO_GATE" in r.getMessage()]
    assert warnings == [
        "Config: STT_AUDIO_GATE=auto ya no existe desde la 5.0; uso energy "
        "(cámbialo con `fogar-gw config set STT_AUDIO_GATE energy`)."
    ]


def test_stt_audio_gate_other_values_are_still_rejected(monkeypatch):
    monkeypatch.setenv("STT_AUDIO_GATE", "loud")
    with pytest.raises(ValidationError, match="STT_AUDIO_GATE"):
        Settings()
