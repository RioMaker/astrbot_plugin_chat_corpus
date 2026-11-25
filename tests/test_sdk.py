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
    context.get_config = lambda *, umo: {"provider_stt_settings": {"enable": True}}
    transcribe = AsyncMock(return_value="周六三点集合")
    context.get_using_stt_provider_async = AsyncMock(
        return_value=SimpleNamespace(get_text=transcribe)
    )
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
            assert (
                await request_to("export/start", method="POST", actor=None)
            ).status_code == 401
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
            voice = b"ID3" + bytes(600_000)
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
            await asyncio.gather(*tuple(plugin.pending))
            async with asyncio.timeout(5):
                while not plugin.store.status()["recognized"]:
                    await asyncio.sleep(0.01)
            transcribe.assert_awaited_once()
            assert context.get_using_stt_provider_async.await_args.kwargs[
                "umo"
            ].startswith("qq-test:GroupMessage:")

            # A pending OneBot download must neither hold up a ZIP nor get cancelled by it.
            started, release = asyncio.Event(), asyncio.Event()

            async def slow_action(*_args, **_kwargs):
                started.set()
                await release.wait()
                return {"file": str(voice_path)}

            adapter.bot.call_action.side_effect = slow_action
            await deliver(Event({**native, "message_id": 92}))
            try:
                async with asyncio.timeout(5):
                    await started.wait()
                    selection = {"group_id": "12345", "since": 123455, "before": 123457}
                    legacy = await request_to("export", query=selection)
                    assert legacy.status_code == 200
                    with zipfile.ZipFile(
                        io.BytesIO(b"".join([c async for c in legacy.body_iterator]))
                    ) as bundle:
                        assert json.loads(bundle.read("manifest.json"))["messages"] == 1
                    start = await request_to(
                        "export/start", method="POST", body=selection
                    )
                    assert start.status_code == 202
                    job = json.loads(start.body)
                    ticket = {"id": job["id"]}
                    assert (
                        await request_to("export/status", query=ticket, actor="other")
                    ).status_code == 404
                    assert (
                        await request_to("export", query=ticket, actor="other")
                    ).status_code == 404
                    while job["phase"] == "packing":
                        await asyncio.sleep(0.01)
                        job = json.loads(
                            (await request_to("export/status", query=ticket)).body
                        )
                    assert (
                        job["phase"] == "ready"
                        and job["messages"] == job["media_files"] == 1
                    )
                    stream = await request_to("export", query=ticket)
                    chunks = [
                        await anext(stream.body_iterator),
                        await anext(stream.body_iterator),
                    ]
                    sending = json.loads(
                        (await request_to("export/status", query=ticket)).body
                    )
                    assert sending["phase"] == "sending" and sending["sent"] == len(
                        chunks[0]
                    )
                    chunks.extend([chunk async for chunk in stream.body_iterator])
                    data = b"".join(chunks)
                    assert job["size"] == len(data)
                    finished = json.loads(
                        (await request_to("export/status", query=ticket)).body
                    )
                    assert finished["phase"] == "sent" and finished["sent"] == len(data)
                    assert (await request_to("export", query=ticket)).status_code == 409
                    assert len(plugin.pending) == 1
            finally:
                release.set()
                await asyncio.gather(*tuple(plugin.pending))
            assert stream.status_code == 200 and int(
                stream.headers["content-length"]
            ) == len(data)
            assert stream.media_type == "application/zip"
            with zipfile.ZipFile(io.BytesIO(data)) as bundle:
                row = json.loads(bundle.read("messages.jsonl"))
                assert row["segments"][0]["data"]["text"] == "  原文  "
                assert bundle.read(row["segments"][1]["media"]["path"]) == voice
                assert row["segments"][1]["recognition"] == {
                    "transcript": "周六三点集合"
                }
            status = json.loads((await request_to("state")).body)
            assert (
                status["messages"] == 2
                and status["media"] == 1
                and status["errors"] == 0
            )
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
            assert json.loads((await request_to("state")).body)["messages"] == 2
        finally:
            await second.terminate()

    asyncio.run(run())


def test_reload_during_recognition_and_capture_under_pressure(tmp_path, monkeypatch):
    import hashlib

    monkeypatch.setenv("ASTRBOT_ROOT", str(tmp_path))
    pytest.importorskip("astrbot.api.web", reason="requires the AstrBot runtime")
    from astrbot.api.star import Context
    from astrbot_plugin_chat_corpus.main import ChatCorpus
    from astrbot_plugin_chat_corpus.media import Asset
    from astrbot_plugin_chat_corpus.settings import ExportRange

    async def run():
        entered, finish, waiting = asyncio.Event(), asyncio.Event(), asyncio.Event()

        async def transcribe(path):
            assert Path(path).read_bytes() == audio
            entered.set()
            await finish.wait()
            return "明天下午开会"

        context = object.__new__(Context)
        context.registered_web_apis = []
        context.get_config = lambda **_: {"provider_stt_settings": {"enable": True}}
        context.get_using_stt_provider_async = AsyncMock(
            return_value=SimpleNamespace(get_text=transcribe)
        )
        config = {"group_ids": ["123"], "capacity_mib": 1, "rolling": True}
        first = ChatCorpus(context, config)
        await first.initialize()
        audio = b"ID3" + bytes(700_000)
        source = tmp_path / "test.mp3"
        source.write_bytes(audio)
        asset = Asset(source, hashlib.sha256(audio).hexdigest(), "mp3", len(audio))
        origin = "robot:GroupMessage:123"
        record = {
            "platform_id": "robot",
            "self_id": "1",
            "group_id": "123",
            "message_id": "1",
            "timestamp": 10,
            "received_ns": 10,
            "segments": [{"type": "record", "data": {}, "media": asset.reference()}],
        }
        try:
            await first._append(record, {asset.digest: asset}, 0, origin)
            await asyncio.wait_for(entered.wait(), 5)
        finally:
            await asyncio.wait_for(first.terminate(), 5)
        assert first.store.status()["media"] == 1
        entered.clear()
        second = ChatCorpus(context, config)
        await second.initialize()
        try:
            await asyncio.wait_for(entered.wait(), 5)
            loop = asyncio.get_running_loop()
            append = second.store.append

            def observe(*args, **kwargs):
                result = append(*args, **kwargs)
                if result == "waiting":
                    loop.call_soon_threadsafe(waiting.set)
                return result

            monkeypatch.setattr(second.store, "append", observe)
            text = {
                **record,
                "message_id": "2",
                "timestamp": 20,
                "segments": [{"type": "text", "data": {"text": "t" * 700_000}}],
            }
            capture = asyncio.create_task(second._append(text, {}, 0, origin))
            await asyncio.wait_for(waiting.wait(), 5)
            finish.set()
            await asyncio.wait_for(capture, 5)
            with (
                second.store.export(ExportRange()) as snapshot,
                zipfile.ZipFile(snapshot) as bundle,
            ):
                records = [
                    json.loads(line)
                    for line in bundle.read("messages.jsonl").splitlines()
                ]
                assert [r["message_id"] for r in records] == ["1", "2"]
                voice = records[0]["segments"][0]
                assert voice["recognition"]["transcript"] == "明天下午开会"
                assert voice["media"]["missing"] == "capacity_evicted"
                assert asset.reference()["path"] not in bundle.namelist()
        finally:
            finish.set()
            await second.terminate()

    asyncio.run(run())
