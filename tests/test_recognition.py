import asyncio
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from astrbot_plugin_chat_corpus.recognition import provider_for, recognize, usable_text


@pytest.mark.parametrize(
    "result", [None, {}, {"transcript": " "}, {"transcript": "[听不清]"}]
)
def test_empty_voice_cannot_release_original(result):
    assert usable_text("wav", result) is None


def test_image_model_receives_pixels_and_keeps_ocr_separate(tmp_path, monkeypatch):
    monkeypatch.setenv("ASTRBOT_ROOT", str(tmp_path))
    pytest.importorskip("astrbot.core.utils.media_utils")
    from PIL import Image

    path = tmp_path / "original.gif"
    Image.new("RGB", (20, 20), "red").save(
        path, save_all=True, append_images=[Image.new("RGB", (20, 20), "blue")]
    )
    origin = "robot:GroupMessage:123"

    def profile(*, umo):
        assert umo == origin
        return {"provider_settings": {"default_image_caption_provider_id": "vision"}}

    async def generate(*, chat_provider_id, prompt, image_urls):
        assert (
            chat_provider_id == "vision" and "ocr_text" in prompt and "抽帧" in prompt
        )
        with Image.open(image_urls[0]) as prepared:
            assert prepared.format in {"JPEG", "PNG"}
        return SimpleNamespace(
            role="assistant",
            completion_text='```json\n{"ocr_text":"九点","description":"通知截图"}\n```',
        )

    context = SimpleNamespace(get_config=profile, llm_generate=generate)
    provider = asyncio.run(provider_for(context, origin, "gif"))
    assert asyncio.run(recognize(context, provider, path, origin)) == {
        "ocr_text": "九点",
        "description": "通知截图",
    }
    for reply in (
        SimpleNamespace(
            role="err", completion_text='{"ocr_text":"遇到错误","description":""}'
        ),
        SimpleNamespace(
            role="assistant", completion_text='{"ocr_text":"","description":" "}'
        ),
        SimpleNamespace(role="assistant", completion_text="暂时无法识别"),
    ):
        context.llm_generate = AsyncMock(return_value=reply)
        assert asyncio.run(recognize(context, provider, path, origin)) is None


def test_unconfigured_services_and_partial_transcript():
    context = SimpleNamespace(get_config=lambda **_: {})
    assert asyncio.run(provider_for(context, "qq:GroupMessage:7", "png")) is None
    assert asyncio.run(provider_for(context, "qq:GroupMessage:7", "silk")) is None
    text = {"transcript": "先到南门，再去[听不清]集合。"}
    assert usable_text("silk", text) == text


@pytest.mark.parametrize("marker", [b"", b"\x02", b"\x03"])
def test_qq_silk_is_transcribed_as_wav(tmp_path, monkeypatch, marker):
    monkeypatch.setenv("ASTRBOT_ROOT", str(tmp_path))
    pytest.importorskip("astrbot.core.utils.media_utils")
    pysilk = pytest.importorskip("pysilk")
    from io import BytesIO

    encoded = BytesIO()
    pysilk.encode(BytesIO(bytes(24000)), encoded, 24000, 24000)
    silk = tmp_path / "original.silk"
    silk.write_bytes(marker + encoded.getvalue().removeprefix(b"\x02"))

    async def transcribe(audio):
        assert Path(audio).read_bytes()[:4] == b"RIFF"
        return "会议改到明天"

    result = asyncio.run(
        recognize(None, SimpleNamespace(get_text=transcribe), silk, "qq:GroupMessage:7")
    )
    assert result == {"transcript": "会议改到明天"}
