"""Build a stable, ordered copy of a QQ message before other handlers edit it."""

import time
import uuid
from collections.abc import Mapping
from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path

from .media import MediaError, file_token


@dataclass
class Incoming:
    record: dict
    attachments: list
    forwards: list

    async def collect(self, reader, directory, limit):
        for segment in self.forwards:
            data = segment["data"]
            nodes = await reader.forward_nodes(str(data["id"]))
            if nodes is None:
                segment["forward_status"] = "unavailable"
            else:
                data["content"] = _nodes(nodes, self.attachments)
        assets = {}
        missing = 0
        remaining = limit
        # Repeated segments keep their positions while sharing a single file.
        fetched = {}
        for i, (segment, kind, source) in enumerate(self.attachments):
            token = file_token(source)
            identifier = token or next(
                (
                    v
                    for k in ("url", "file", "base64")
                    if isinstance(v := source.get(k), str) and v
                ),
                "",
            )
            key = (kind, identifier)
            target = Path(directory) / str(i)
            if key[1] and key in fetched:
                segment["media"] = fetched[key]
                continue
            try:
                item = await reader.fetch(source, target, kind, remaining)
            except MediaError as exc:
                segment["media"] = {"missing": str(exc)}
                target.unlink(missing_ok=True)
                missing += 1
                continue
            if item.digest not in assets:
                assets[item.digest] = item
                remaining -= item.size
            else:
                target.unlink(missing_ok=True)
            segment["media"] = item.reference()
            if key[1]:
                fetched[key] = segment["media"]
        return assets, missing


def _nodes(nodes, attachments):
    result = []
    for node in nodes:
        if not isinstance(node, Mapping):
            continue
        if "type" in node:
            result.extend(_segments([node], attachments))
            continue
        data = {
            k: deepcopy(v)
            for k, v in node.items()
            if k not in ("content", "message", "raw_message")
        }
        content = node.get("content", node.get("message", []))
        data["content"] = (
            _segments(content, attachments) if isinstance(content, list) else content
        )
        result.append({"type": "node", "data": data})
    return result


def _segments(parts, attachments, forwards=None):
    copied = []
    for part in parts:
        if not isinstance(part, Mapping):
            continue
        kind = str(part.get("type", "unknown"))
        raw = part.get("data", {})
        if not isinstance(raw, Mapping):
            copied.append({"type": kind, "data": deepcopy(raw)})
            continue
        if kind in ("image", "record", "mface"):
            media_kind = "image" if kind == "mface" else kind
            transport = {"base64", "url", "file", "file_id", "path"}
            segment = {
                "type": kind,
                "data": {k: deepcopy(v) for k, v in raw.items() if k not in transport},
            }
            token = file_token(raw)
            if token:
                segment["data"]["file_id"] = token
            attachments.append((segment, media_kind, dict(raw)))
        else:
            data = {}
            for key, value in raw.items():
                if (
                    kind == "forward"
                    and key in ("content", "messages")
                    and isinstance(value, list)
                ):
                    data[key] = _nodes(value, attachments)
                elif (
                    key in ("content", "message")
                    and isinstance(value, list)
                    and all(isinstance(p, Mapping) and "type" in p for p in value)
                ):
                    data[key] = _segments(value, attachments)
                else:
                    data[key] = deepcopy(value)
            segment = {"type": kind, "data": data}
            if (
                kind == "forward"
                and data.get("id")
                and not (data.get("content") or data.get("messages"))
                and forwards is not None
            ):
                forwards.append(segment)
        copied.append(segment)
    return copied


def _components(event):
    parts = []
    for item in event.get_messages():
        kind = type(item).__name__.lower()
        if kind == "plain":
            parts.append({"type": "text", "data": {"text": item.text}})
        else:
            data = {}
            for key in (
                "id",
                "qq",
                "file",
                "url",
                "text",
                "name",
                "sub_type",
                "summary",
            ):
                value = getattr(item, key, None)
                if isinstance(value, (str, int, float, bool)):
                    data[key] = value
            parts.append({"type": kind, "data": data})
    return parts


def snapshot(event, group_ids):
    if event.get_platform_name() != "aiocqhttp" or event.is_private_chat():
        return None
    group = str(event.get_group_id() or "")
    if group not in group_ids:
        return None
    obj = event.message_obj
    raw = getattr(obj, "raw_message", None)
    raw = raw if isinstance(raw, Mapping) else {}
    if (
        raw.get("post_type") not in (None, "message", "message_sent")
        or raw.get("message_type") not in (None, "group")
        or str(raw.get("group_id", group)) != group
    ):
        return None
    parts = raw.get("message")
    if not isinstance(parts, list):
        parts = _components(event)
    if not parts and getattr(obj, "message_str", ""):
        parts = [{"type": "text", "data": {"text": obj.message_str}}]
    if not parts:
        return None
    received = time.time_ns()
    try:
        ts = int(raw.get("time") or getattr(obj, "timestamp", 0))
        if not 0 < ts < 2**63:
            raise ValueError
    except (ValueError, TypeError, OverflowError):
        ts = received // 1_000_000_000
    sender = raw.get("sender")
    sender = sender if isinstance(sender, Mapping) else {}
    message_id = raw.get("message_id", getattr(obj, "message_id", None))
    attachments, forwards = [], []
    record = {
        "platform": "aiocqhttp",
        "platform_id": str(event.get_platform_id()),
        "self_id": str(event.get_self_id()),
        "group_id": group,
        "group_name": raw.get("group_name")
        or getattr(getattr(obj, "group", None), "group_name", "")
        or "",
        "message_id": str(message_id)
        if message_id not in (None, "")
        else "local-" + uuid.uuid4().hex,
        "timestamp": ts,
        "received_ns": received,
        "sender": {
            "id": str(sender.get("user_id") or event.get_sender_id()),
            "nickname": str(sender.get("nickname") or event.get_sender_name() or ""),
            "card": str(sender.get("card") or ""),
        },
        "segments": _segments(parts, attachments, forwards),
    }
    return Incoming(record, attachments, forwards)
