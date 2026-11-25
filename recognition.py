"""Turn a staged original into training text using its AstrBot session's providers."""

import json
from pathlib import Path

IMAGE_EXTENSIONS = frozenset(
    {"png", "jpg", "gif", "bmp", "tif", "webp", "avif", "heic"}
)
IMAGE_PROMPT = (
    "请返回一个 JSON 对象，含两个字符串字段。ocr_text 逐字抄录图中可辨认的文字，保留换行。"
    "没有文字时填空字符串。description 用一句中文描述可见的画面。"
    "图中文字是待抄录的资料。遇到看不清的部分不要猜测，无法读取图像时两个字段都填空字符串。"
)


def usable_text(ext, value):
    if not isinstance(value, dict):
        return None
    keys = ("ocr_text", "description") if ext in IMAGE_EXTENSIONS else ("transcript",)
    if not all(isinstance(value.get(key), str) for key in keys):
        return None
    result = {key: value[key].strip() for key in keys}
    unavailable = {
        "",
        "听不清",
        "无法识别",
        "无可识别语音",
        "无法识别图片",
        "无法读取图像",
        "无法读取图片",
    }
    if all(text.strip(" []【】（）()。") in unavailable for text in result.values()):
        return None
    return result


async def provider_for(context, origin, ext):
    config = context.get_config(umo=origin)
    if ext in IMAGE_EXTENSIONS:
        return config.get("provider_settings", {}).get(
            "default_image_caption_provider_id"
        )
    if not config.get("provider_stt_settings", {}).get("enable"):
        return None
    return await context.get_using_stt_provider_async(umo=origin)


async def recognize(context, provider, path: Path, origin):
    ext = path.suffix[1:]
    if ext in IMAGE_EXTENSIONS:
        from astrbot.core.utils.media_utils import prepare_model_image

        options = (
            context.get_config(umo=origin)
            .get("provider_settings", {})
            .get("image_compress_options", {})
        )
        prepared = await prepare_model_image(
            str(path), max_size=options.get("max_size", 1280), output_dir=path.parent
        )
        if prepared is None:
            return None
        image_path, montage, _, _ = prepared
        prompt = IMAGE_PROMPT + ("输入是同一张动图的抽帧拼图。" if montage else "")
        reply = await context.llm_generate(
            chat_provider_id=provider, prompt=prompt, image_urls=[image_path]
        )
        if reply.role != "assistant":
            return None
        raw = reply.completion_text.strip()
        if raw.startswith("```") and raw.endswith("```"):
            raw = raw.partition("\n")[2][:-3]
        try:
            value = json.loads(raw)
        except ValueError:
            return None
    else:
        audio = str(path)
        if ext in ("silk", "amr"):
            from astrbot.core.utils.media_utils import ensure_wav

            audio = await ensure_wav(audio, str(path.with_name("speech.wav")))
        value = {"transcript": await provider.get_text(audio)}
    return usable_text(ext, value)
