import asyncio
import base64
import json
import zipfile
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from astrbot_plugin_chat_corpus.archive import Archive
from astrbot_plugin_chat_corpus.ingest import snapshot
from astrbot_plugin_chat_corpus.media import MediaError, MediaReader, qq_url
from astrbot_plugin_chat_corpus.settings import MIB, ExportRange, Settings

PNG = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+jRZkAAAAASUVORK5CYII="
)
VOICE = b"\x02#!SILK_V3\x01\x00\x00"


def inline(value):
    return "base64://" + base64.b64encode(value).decode("ascii")


def event_with(parts, group="12345", **raw_fields):
    raw = {
        "post_type": "message",
        "message_type": "group",
        "message": parts,
        "group_id": group,
        "time": 123,
        "message_id": 77,
        "sender": {"user_id": 456, "nickname": "原昵称", "card": "群名片"},
        **raw_fields,
    }
    return SimpleNamespace(
        get_platform_name=lambda: "aiocqhttp",
        is_private_chat=lambda: False,
        get_group_id=lambda: group,
        get_platform_id=lambda: "qq-1",
        get_self_id=lambda: "999",
        get_sender_id=lambda: "456",
        get_sender_name=lambda: "群名片",
        get_messages=list,
        message_obj=SimpleNamespace(raw_message=raw, timestamp=999),
    )


def test_only_selected_group_messages_touch_the_body():
    event = event_with([])
    del event.message_obj
    assert snapshot(event, []) is None
    assert snapshot(event, ["54321"]) is None
    event.is_private_chat = lambda: True
    assert snapshot(event, ["12345"]) is None
    event.is_private_chat = lambda: False
    event.get_platform_name = lambda: "telegram"
    assert snapshot(event, ["12345"]) is None
    for keys in ({"post_type": "notice"}, {"message_type": "private"}):
        assert (
            snapshot(
                event_with([{"type": "text", "data": {"text": "x"}}], **keys), ["12345"]
            )
            is None
        )


def test_multimodal_roundtrip_text_order_reply_and_missing(tmp_path):
    shared_path = tmp_path / "adapter-voice.silk"
    shared_path.write_bytes(VOICE)

    async def action(name, **kwargs):
        assert kwargs["self_id"] == "999"
        if name == "get_record":
            assert kwargs["file"] == "qq-voice" and kwargs["out_format"] == "mp3"
            return {"file": str(shared_path)}
        raise OSError("expired")

    original = "  保留换行\n还有 emoji 🥁  "
    parts = [
        {"type": "reply", "data": {"id": "66"}},
        {"type": "text", "data": {"text": original}},
        {"type": "image", "data": {"file": inline(PNG), "summary": "原图"}},
        {"type": "record", "data": {"file": "qq-voice"}},
        {"type": "at", "data": {"qq": "876"}},
        {
            "type": "node",
            "data": {"content": [{"type": "image", "data": {"file": inline(PNG)}}]},
        },
        {"type": "image", "data": {"file": "expired-image"}},
    ]
    incoming = snapshot(event_with(parts), ["12345"])
    parts[1]["data"]["text"] = "changed by another handler"
    assets, missing = asyncio.run(
        incoming.collect(MediaReader(None, action, "999"), tmp_path, MIB)
    )
    assert missing == 1 and len(assets) == 2
    store = Archive(tmp_path / "store")
    store.initialize(Settings.read({}))
    assert store.append(incoming.record, assets, missing, MIB, False) == "stored"
    with store.export(ExportRange()) as file, zipfile.ZipFile(file) as bundle:
        envelope = json.loads(bundle.read("messages.jsonl"))
        assert envelope["timestamp"] == 123 and envelope["message_id"] == "77"
        assert envelope["sender"] == {
            "id": "456",
            "nickname": "原昵称",
            "card": "群名片",
        }
        segments = envelope["segments"]
        assert [p["type"] for p in segments] == [p["type"] for p in parts]
        assert (
            segments[0]["data"]["id"] == "66"
            and segments[1]["data"]["text"] == original
        )
        assert bundle.read(segments[2]["media"]["path"]) == PNG
        assert bundle.read(segments[3]["media"]["path"]) == VOICE
        assert segments[2]["media"] == segments[5]["data"]["content"][0]["media"]
        assert segments[6]["media"]["missing"] == "unavailable"
        assert json.loads(bundle.read("manifest.json"))["missing_media_segments"] == 1
        assert b"base64://" not in bundle.read("messages.jsonl")


def test_failure_and_oversize_leave_other_parts_intact(tmp_path):
    parts = [
        {"type": "record", "data": {"file": inline(VOICE * 300)}},
        {"type": "image", "data": {"file": inline(b"<html>expired</html>")}},
        {"type": "image", "data": {"file": inline(PNG)}},
    ]
    incoming = snapshot(event_with(parts), ["12345"])
    assets, missing = asyncio.run(
        incoming.collect(MediaReader(None, None, "999"), tmp_path, 1000)
    )
    assert missing == 2 and len(assets) == 1
    assert [p["media"].get("missing") for p in incoming.record["segments"]] == [
        "over_limit",
        "unrecognized_format",
        None,
    ]


def test_raw_paths_and_chat_text_are_not_file_sources(tmp_path):
    local = tmp_path / "private.png"
    local.write_bytes(PNG)
    parts = [
        {"type": "text", "data": {"text": str(local)}},
        {"type": "image", "data": {"file": str(local)}},
    ]
    incoming = snapshot(event_with(parts), ["12345"])
    assets, missing = asyncio.run(
        incoming.collect(MediaReader(None, None, ""), tmp_path, MIB)
    )
    assert not assets and missing == 1
    assert incoming.record["segments"][1]["data"]["file_id"] == "private.png"


def test_forwarded_nodes_and_remote_protocol_media(tmp_path):
    async def action(name, **params):
        if name == "get_forward_msg":
            if params["id"] == "expired":
                raise OSError("unavailable")
            return {
                "messages": [
                    {
                        "sender": {"nickname": "群友"},
                        "time": 122,
                        "content": [
                            {"type": "text", "data": {"text": "看图"}},
                            {
                                "type": "image",
                                "data": {"file": "qq.png", "file_id": "opaque"},
                            },
                            {"type": "forward", "data": {"id": "nested"}},
                        ],
                    }
                ]
            }
        if name == "get_image":
            assert params["file"] == "qq.png"
            return {"file": "/remote-container/no-shared-volume/qq.png"}
        assert name == "get_file" and params["file_id"] == "opaque"
        return {"base64": base64.b64encode(PNG).decode()}

    incoming = snapshot(
        event_with(
            [{"type": "forward", "data": {"id": key}} for key in ("ok", "expired")]
        ),
        ["12345"],
    )
    files, missing = asyncio.run(
        incoming.collect(MediaReader(None, action, "999"), tmp_path, MIB)
    )
    first, expired = incoming.record["segments"]
    parts = first["data"]["content"][0]["data"]["content"]
    assert parts[0]["data"]["text"] == "看图" and parts[2]["data"]["id"] == "nested"
    assert files[parts[1]["media"]["sha256"]].path.read_bytes() == PNG and not missing
    assert (
        expired["data"]["id"] == "expired"
        and expired["forward_status"] == "unavailable"
    )


class Response:
    def __init__(self, *, body=b"", redirect="", status=200):
        self.status = status
        self.body = body
        self.headers = {"Location": redirect}
        self.content_length = None
        self.content = self

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False

    async def iter_chunked(self, size):
        yield self.body[:3]
        yield self.body[3:]


def test_cdn_stream_and_redirect_validation(tmp_path):
    url = "https://gchat.qpic.cn/img/signed?token=secret"
    requests = []

    def get(link, *, allow_redirects):
        requests.append(link)
        assert not allow_redirects
        return Response(redirect="https://example.org/x", status=302)

    direct = SimpleNamespace(get=lambda *args, **kwargs: Response(body=PNG))
    item = asyncio.run(
        MediaReader(direct, None, "").fetch({"url": url}, tmp_path / "ok", "image", MIB)
    )
    assert item.path.read_bytes() == PNG
    with pytest.raises(MediaError, match="over_limit"):
        asyncio.run(
            MediaReader(direct, None, "").fetch(
                {"url": url}, tmp_path / "too-big", "image", 5
            )
        )
    reply = AsyncMock(return_value={"data": {"base64": base64.b64encode(PNG).decode()}})
    reader = MediaReader(SimpleNamespace(get=get), reply, "9")
    item = asyncio.run(
        reader.fetch({"url": url, "file_id": "handle"}, tmp_path / "ok2", "image", MIB)
    )
    assert requests == [url] and item.path.read_bytes() == PNG
    reply.assert_awaited_once_with("get_image", file="handle", self_id="9")
    for address in (
        "http://127.0.0.1/x",
        "https://qpic.cn.example.org/x",
        "file:///etc/file",
        "https://user@qpic.cn/x",
    ):
        assert not qq_url(address)


@pytest.mark.parametrize(
    "values",
    [
        {"rolling": "false"},
        {"capacity_mib": False},
        {"capacity_mib": 0},
        {"group_ids": "*"},
        {"group_ids": ["../123"]},
    ],
)
def test_configuration_rejects_invalid_values(values):
    with pytest.raises((TypeError, ValueError)):
        Settings.read(values)


def test_defaults_and_selection_boundaries():
    assert Settings.read({}) == Settings((), 1024, False)
    assert Settings.read({"group_ids": [123, "456", "123"]}).group_ids == ("123", "456")
    assert ExportRange.from_query(
        {"group_id": "123", "since": "10", "before": "20"}
    ) == ExportRange("123", 10, 20)
    for query in (
        {"group_id": "../../"},
        {"since": "bad"},
        {"since": "20", "before": "20"},
    ):
        with pytest.raises(ValueError):
            ExportRange.from_query(query)
