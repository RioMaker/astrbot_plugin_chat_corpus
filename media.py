"""Copy OneBot media while its download reference is still valid."""

import asyncio
import base64
import binascii
import hashlib
import stat
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urljoin, urlsplit
from urllib.request import url2pathname

import aiohttp

from .archive import CHUNK, media_name

QQ_DOMAINS = ("qq.com", "qq.com.cn", "qpic.cn", "qlogo.cn", "gtimg.cn", "gtimg.com")


class MediaError(Exception):
    """A portable reason for an unavailable attachment."""


@dataclass
class Asset:
    path: Path
    digest: str
    ext: str
    size: int

    def reference(self):
        return {
            "path": media_name(self.digest, self.ext),
            "sha256": self.digest,
            "bytes": self.size,
        }


def extension(header, kind):
    if kind == "image":
        for prefix, ext in (
            (b"\x89PNG\r\n\x1a\n", "png"),
            (b"\xff\xd8\xff", "jpg"),
            (b"GIF87a", "gif"),
            (b"GIF89a", "gif"),
            (b"BM", "bmp"),
            (b"II*\x00", "tif"),
            (b"MM\x00*", "tif"),
        ):
            if header.startswith(prefix):
                return ext
        if header[:4] == b"RIFF" and header[8:12] == b"WEBP":
            return "webp"
        if header[4:8] == b"ftyp":
            if b"avif" in header or b"avis" in header:
                return "avif"
            if any(brand in header for brand in (b"heic", b"heif", b"heix", b"mif1")):
                return "heic"
    else:
        for prefix, ext in (
            (b"\x02#!SILK_V3", "silk"),
            (b"\x03#!SILK_V3", "silk"),
            (b"#!SILK_V3", "silk"),
            (b"#!AMR", "amr"),
            (b"OggS", "ogg"),
            (b"fLaC", "flac"),
            (b"ID3", "mp3"),
            (b"\x1a\x45\xdf\xa3", "webm"),
        ):
            if header.startswith(prefix):
                return ext
        if header[:4] == b"RIFF" and header[8:12] == b"WAVE":
            return "wav"
        if header[4:8] == b"ftyp":
            return "m4a"
        if len(header) >= 2 and header[0] == 0xFF and header[1] & 0xE0 == 0xE0:
            return "mp3" if header[1] & 0x06 else "aac"
    raise MediaError("unrecognized_format")


def qq_url(value):
    try:
        parsed = urlsplit(value)
        host = (parsed.hostname or "").lower()
        return (
            parsed.scheme in ("http", "https")
            and not parsed.username
            and not parsed.password
            and parsed.port in (None, 80, 443)
            and any(host == d or host.endswith("." + d) for d in QQ_DOMAINS)
        )
    except ValueError:
        return False


def local_file(value):
    if value.lower().startswith("file:"):
        parsed = urlsplit(value)
        if parsed.netloc not in ("", "localhost"):
            return None
        value = url2pathname(parsed.path)
    if not value or value.replace("\\", "/").startswith("//"):
        return None
    path = Path(value)
    return path if path.is_absolute() else None


def stage_base64(value, target, limit):
    value = value.removeprefix("base64://")
    if len(value) > 4 * ((limit + 2) // 3):
        raise MediaError("over_limit")
    total = 0
    with target.open("wb") as out:
        for i in range(0, len(value), CHUNK):
            try:
                piece = base64.b64decode(value[i : i + CHUNK], validate=True)
            except (ValueError, binascii.Error) as exc:
                raise MediaError("invalid_base64") from exc
            total += len(piece)
            if total > limit:
                raise MediaError("over_limit")
            out.write(piece)


def stage_local(path, target, limit):
    # Only a protocol action can nominate a local file. Chat segments cannot.
    info = path.stat()
    if not stat.S_ISREG(info.st_mode):
        raise MediaError("unavailable")
    if info.st_size > limit:
        raise MediaError("over_limit")
    with path.open("rb") as src, target.open("wb") as dest:
        total = 0
        while chunk := src.read(CHUNK):
            total += len(chunk)
            if total > limit:
                raise MediaError("over_limit")
            dest.write(chunk)


def inspect_file(path, kind):
    digest = hashlib.sha256()
    with path.open("rb") as src:
        ext = extension(src.read(64), kind)
        src.seek(0)
        while chunk := src.read(CHUNK):
            digest.update(chunk)
        return Asset(path, digest.hexdigest(), ext, src.tell())


def file_token(data):
    for key in ("file_id", "file"):
        value = data.get(key)
        if not isinstance(value, str) or not value or len(value) > 4096:
            continue
        if value.startswith(("http:", "https:", "base64:", "data:")):
            continue
        if value.startswith(("/", "file:")) or "\\" in value:
            value = value.replace("\\", "/").rsplit("/", 1)[-1]
        if (
            value
            and not value.startswith(("/", "."))
            and ".." not in value
            and ":" not in value
        ):
            return value
    return ""


class MediaReader:
    def __init__(self, session, call_action, bot_id):
        self.session = session
        self.call_action = call_action
        self.bot_id = bot_id

    async def _action(self, name, **params):
        if not self.call_action:
            return None
        if self.bot_id:
            params["self_id"] = self.bot_id
        try:
            reply = await asyncio.wait_for(self.call_action(name, **params), timeout=15)
        except Exception:  # noqa: BLE001 -- expired IDs and unsupported actions have adapter-specific errors
            return None
        return reply.get("data", reply) if isinstance(reply, dict) else None

    async def forward_nodes(self, forward_id):
        reply = await self._action("get_forward_msg", id=forward_id)
        nodes = reply.get("messages") if isinstance(reply, dict) else None
        return nodes if isinstance(nodes, list) else None

    async def download(self, url, target, limit):
        for _ in range(4):
            if not qq_url(url):
                raise MediaError("unavailable")
            async with self.session.get(url, allow_redirects=False) as response:
                if response.status in (301, 302, 303, 307, 308):
                    url = urljoin(url, response.headers.get("Location", ""))
                    continue
                if response.status != 200:
                    raise MediaError("unavailable")
                if (
                    response.content_length is not None
                    and response.content_length > limit
                ):
                    raise MediaError("over_limit")
                with target.open("wb") as out:
                    total = 0
                    async for chunk in response.content.iter_chunked(CHUNK):
                        total += len(chunk)
                        if total > limit:
                            raise MediaError("over_limit")
                        await asyncio.to_thread(out.write, chunk)
                return
        raise MediaError("unavailable")

    async def _sources(self, data, target, kind, limit, *, from_action):
        errors = []
        for field in ("base64", "url", "file", "path"):
            value = data.get(field)
            if not isinstance(value, str) or not value:
                continue
            try:
                if field == "base64" or value.startswith("base64://"):
                    await asyncio.to_thread(stage_base64, value, target, limit)
                elif qq_url(value):
                    await self.download(value, target, limit)
                elif from_action and (path := local_file(value)) is not None:
                    await asyncio.to_thread(stage_local, path, target, limit)
                else:
                    continue
                return await asyncio.to_thread(inspect_file, target, kind)
            except (MediaError, aiohttp.ClientError, OSError, ValueError) as exc:
                errors.append(
                    str(exc) if isinstance(exc, MediaError) else "unavailable"
                )
        raise MediaError(
            "over_limit"
            if "over_limit" in errors
            else next(iter(errors), "unavailable")
        )

    async def fetch(self, data, target, kind, limit):
        # Leave room for the SQLite record header under its per-BLOB length limit.
        limit = min(limit, 999_990_000)
        try:
            return await self._sources(data, target, kind, limit, from_action=False)
        except MediaError as first:
            reason = str(first)
        token = file_token(data)
        if not token or not self.call_action:
            raise MediaError(reason)
        args = {"file": file_token({"file": data.get("file")}) or token}
        if kind == "record":
            args["out_format"] = "mp3"
        # A remote NapCat container can supply bytes via get_file even when
        # get_image/get_record returned a path on its own filesystem.
        for action, params in (("get_" + kind, args), ("get_file", {"file_id": token})):
            reply = await self._action(action, **params)
            if not isinstance(reply, dict):
                continue
            try:
                return await self._sources(reply, target, kind, limit, from_action=True)
            except MediaError as exc:
                if reason != "over_limit":
                    reason = str(exc)
        raise MediaError(reason)
