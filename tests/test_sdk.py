"""Exercise real AstrBot event conversion, configuration and dashboard response APIs."""

import asyncio
import io
import json
import zipfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock
from urllib.parse import urlencode

import pytest


def test_sdk_capture_settings_download_and_reload(tmp_path, monkeypatch):
    monkeypatch.setenv("ASTRBOT_ROOT", str(tmp_path))
    pytest.importorskip("astrbot.api.web", reason="requires the AstrBot runtime")
    from aiocqhttp import Event
    from astrbot.api import AstrBotConfig
    from astrbot.api.star import Context
    from astrbot.api.web import PluginRequest, bind_request_context
    from astrbot.core.platform.platform_metadata import PlatformMetadata
    from astrbot.core.platform.sources.aiocqhttp.aiocqhttp_message_event import (
        AiocqhttpMessageEvent,
    )
    from astrbot.core.platform.sources.aiocqhttp.aiocqhttp_platform_adapter import (
        AiocqhttpAdapter,
    )
    from astrbot_plugin_chat_corpus.main import ChatCorpus, TargetGroup
    from astrbot_plugin_chat_corpus.settings import PLUGIN
    from starlette.requests import Request

    schema = json.loads(
        (Path(__file__).resolve().parents[1] / "_conf_schema.json").read_text("utf-8")
    )
    config_file = tmp_path / "config.json"
    config = AstrBotConfig(str(config_file), schema=schema)
    context = object.__new__(Context)
    context.registered_web_apis = []
    directory_bot = SimpleNamespace(
        call_action=AsyncMock(
            return_value=[
                {"group_id": 12345, "group_name": "离线测试群", "member_count": 17},
            ]
        )
    )
    context.platform_manager = SimpleNamespace(
        get_insts=lambda: [
            SimpleNamespace(
                meta=lambda: SimpleNamespace(name="aiocqhttp", id="qq-test"),
                get_client=lambda: directory_bot,
            )
        ]
    )

    async def request_to(path, *, method="GET", actor="admin", body=None, query=None):
        data = json.dumps(body).encode()

        async def receive():
            return {"type": "http.request", "body": data, "more_body": False}

        native = Request(
            {
                "type": "http",
                "method": method,
                "path": "/" + path,
                "scheme": "http",
                "server": ("testserver", 80),
                "query_string": urlencode(query or {}).encode(),
                "headers": [(b"content-type", b"application/json")],
            },
            receive,
        )
        handler = next(
            h
            for p, h, methods, _ in context.registered_web_apis
            if p == f"/{PLUGIN}/{path}" and method in methods
        )
        with bind_request_context(
            PluginRequest(native, plugin_name=PLUGIN, username=actor)
        ):
            return await handler()

    async def run():
        plugin = ChatCorpus(context, config)
        await plugin.initialize()
        try:
            assert (await request_to("export", actor=None)).status_code == 401
            assert (await request_to("groups", actor=None)).status_code == 401
            listing = json.loads((await request_to("groups")).body)
            assert listing["groups"] == [
                {
                    "group_id": "12345",
                    "group_name": "离线测试群",
                    "member_count": 17,
                    "online": True,
                }
            ]
            defaults = json.loads((await request_to("state")).body)["settings"]
            values = {**defaults, "group_ids": ["12345"], "capacity_mib": 2}
            saved = await request_to(
                "settings",
                method="POST",
                body={"settings": values, "previous": defaults},
            )
            assert (
                saved.status_code == 200
                and json.loads(config_file.read_text("utf-8-sig")) == values
            )
            stale = await request_to(
                "settings",
                method="POST",
                body={"settings": values, "previous": defaults},
            )
            assert stale.status_code == 409
            invalid = await request_to(
                "settings",
                method="POST",
                body={"settings": {**values, "rolling": "false"}, "previous": values},
            )
            assert invalid.status_code == 400
            assert (
                await request_to("export", query={"group_id": "../"})
            ).status_code == 400

            adapter = object.__new__(AiocqhttpAdapter)
            voice = b"ID3" + bytes(1234)
            voice_path = tmp_path / "voice.mp3"
            voice_path.write_bytes(voice)
            adapter.bot = SimpleNamespace(
                call_action=AsyncMock(return_value={"file": str(voice_path)})
            )
            native = Event(
                {
                    "post_type": "message",
                    "message_type": "group",
                    "group_id": 12345,
                    "message_id": 90,
                    "time": 123456,
                    "self_id": 999,
                    "user_id": 42,
                    "sender": {"user_id": 42, "nickname": "测试群友"},
                    "message": [
                        {"type": "text", "data": {"text": "  原文  "}},
                        {"type": "record", "data": {"file": "record.silk"}},
                    ],
                }
            )

            async def deliver(native):
                message = await adapter._convert_handle_message_event(native)
                meta = PlatformMetadata("aiocqhttp", "test", "qq-test")
                event = AiocqhttpMessageEvent(
                    message.message_str, message, meta, message.session_id, adapter.bot
                )
                if TargetGroup(False).filter(event, config):
                    await plugin.record_group(event)

            await deliver(native)
            await deliver(Event({**native, "message_id": 91, "group_id": 9999}))
            await deliver(native)
            stream = await request_to("export", query={"group_id": "12345"})
            data = b"".join([chunk async for chunk in stream.body_iterator])
            assert stream.status_code == 200 and int(
                stream.headers["content-length"]
            ) == len(data)
            assert stream.media_type == "application/zip"
            with zipfile.ZipFile(io.BytesIO(data)) as bundle:
                row = json.loads(bundle.read("messages.jsonl"))
                assert row["segments"][0]["data"]["text"] == "  原文  "
                assert bundle.read(row["segments"][1]["media"]["path"]) == voice
            status = json.loads((await request_to("state")).body)
            assert status["messages"] == status["media"] == 1 and status["errors"] == 0
            assert [g["group_id"] for g in status["groups"]] == ["12345"]

            # A disk error saving settings must leave both the old allowance and archive intact.
            def fail_save():
                raise OSError("disk error")

            monkeypatch.setattr(type(config), "save_config", lambda _self: fail_save())
            failed = await request_to(
                "settings",
                method="POST",
                body={"settings": {**values, "group_ids": []}, "previous": values},
            )
            assert (
                failed.status_code == 500
                and plugin.settings.public() == values
                and dict(config) == values
            )
        finally:
            await plugin.terminate()
        assert not context.registered_web_apis and TargetGroup.active is None
        second = ChatCorpus(context, config)
        await second.initialize()
        try:
            assert json.loads((await request_to("state")).body)["messages"] == 1
        finally:
            await second.terminate()

    asyncio.run(run())
