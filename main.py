"""AstrBot entry point for the QQ group corpus archive."""

import asyncio
import os
import tempfile
from datetime import UTC, datetime
from pathlib import Path

import aiohttp
from astrbot.api import AstrBotConfig, logger
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.star import Context, Star
from astrbot.api.web import error_response, json_response, request, stream_response
from astrbot.core.utils.astrbot_path import get_astrbot_data_path

from .archive import CHUNK, Archive
from .groups import GroupDirectory
from .ingest import snapshot
from .media import MediaReader
from .settings import PLUGIN, ExportRange, Settings


class TargetGroup(filter.CustomFilter):
    # The waking stage evaluates filters before it dispatches plugin handlers.
    active: Settings | None = None

    def filter(self, event, cfg):
        return (
            self.active is not None
            and event.get_platform_name() == "aiocqhttp"
            and not event.is_private_chat()
            and str(event.get_group_id()) in self.active.group_ids
        )


class ChatCorpus(Star):
    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)
        self.config = config
        self.settings = Settings.read(config)
        self.store = Archive(Path(get_astrbot_data_path()) / "plugin_data" / PLUGIN)
        self.directory = GroupDirectory()
        self.lock = asyncio.Lock()
        self.slots = asyncio.Semaphore(4)
        self.pending = set()
        self.errors = 0
        self.stopped = False
        self.session = None
        self.handlers = []
        for path, handler, method in (
            ("state", self.state, "GET"),
            ("groups", self.groups, "GET"),
            ("settings", self.save, "POST"),
            ("export", self.export, "GET"),
        ):
            view = self._dashboard(handler)
            self.handlers.append(view)
            context.register_web_api(f"/{PLUGIN}/{path}", view, [method], "对话语料库")

    async def initialize(self):
        await asyncio.to_thread(self.store.initialize, self.settings)
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=25, connect=6),
            cookie_jar=aiohttp.DummyCookieJar(),
        )
        TargetGroup.active = self.settings

    def _dashboard(self, handler):
        async def guarded():
            if self.stopped or self.session is None:
                return error_response("插件尚未就绪，请刷新页面", status_code=503)
            if not request.username:
                return error_response("请先登录 AstrBot 管理后台", status_code=401)
            try:
                return await handler()
            except ValueError as exc:
                return error_response(str(exc), status_code=400)
            except Exception as exc:  # noqa: BLE001 -- return a dashboard error without exposing chat content
                logger.error("[对话语料库] 管理页操作失败：%s", type(exc).__name__)
                return error_response(
                    "操作失败，请检查服务器存储空间及读写权限", status_code=500
                )

        return guarded

    @filter.custom_filter(TargetGroup, False, priority=10010)
    async def record_group(self, event: AstrMessageEvent):
        if self.stopped or not self.settings.group_ids or self.session is None:
            return
        try:
            incoming = snapshot(event, self.settings.group_ids)
            if incoming is None:
                return
            # Limit simultaneous temporary copies without dropping a busy group's messages.
            await self.slots.acquire()
            if self.stopped:
                self.slots.release()
                return
            call = getattr(getattr(event, "bot", None), "call_action", None)
            task = asyncio.create_task(self._capture(incoming, call))
            self.pending.add(task)
            task.add_done_callback(self.pending.discard)
        except Exception as exc:  # noqa: BLE001 -- a malformed event must not stop other plugins
            self._capture_error(exc)

    def _capture_error(self, exc):
        self.errors += 1
        logger.error("[对话语料库] 写入消息失败：%s", type(exc).__name__)

    async def _capture(self, incoming, call):
        try:
            record = incoming.record
            if await asyncio.to_thread(self.store.contains, record):
                return
            reader = MediaReader(self.session, call, record["self_id"])
            with tempfile.TemporaryDirectory(prefix="chat-corpus-") as folder:
                assets, missing = await incoming.collect(
                    reader, folder, self.settings.limit
                )
                async with self.lock:
                    # The group or quota may have changed while media was downloading.
                    current = self.settings
                    if record["group_id"] in current.group_ids:
                        await asyncio.to_thread(
                            self.store.append,
                            record,
                            assets,
                            missing,
                            current.limit,
                            current.rolling,
                        )
        except Exception as exc:  # noqa: BLE001 -- surface background failures in archive status
            self._capture_error(exc)
        finally:
            self.slots.release()

    async def state(self):
        async with self.lock:
            data = await asyncio.to_thread(self.store.status)
            data.update(
                settings=self.settings.public(),
                pending=len(self.pending),
                errors=self.errors,
            )
        return json_response(data, headers={"Cache-Control": "no-store"})

    async def groups(self):
        directory = await self.directory.discover(
            self.context.platform_manager, self.settings.group_ids
        )
        return json_response(directory, headers={"Cache-Control": "no-store"})

    async def save(self):
        body = await request.json(default=None)
        required = {"group_ids", "capacity_mib", "rolling"}
        if (
            not isinstance(body, dict)
            or not isinstance(body.get("settings"), dict)
            or set(body["settings"]) != required
        ):
            raise ValueError("请提交完整的群号、容量和动态存储设置")
        try:
            updated = Settings.read(body["settings"])
        except TypeError as exc:
            raise ValueError(str(exc)) from exc
        async with self.lock:
            previous = self.settings.public()
            if body.get("previous") != previous:
                return error_response("设置已变更，请刷新后重新修改", status_code=409)
            self.config.update(updated.public())
            try:
                self.config.save_config()
            except Exception:
                self.config.update(previous)
                raise
            self.settings = updated
            TargetGroup.active = updated
            await asyncio.to_thread(self.store.apply_limit, updated)
        return json_response({"settings": updated.public()})

    async def export(self):
        selection = ExportRange.from_query(request.query)
        if self.pending:
            # Include captures already dispatched at click time. A disconnected
            # browser must not cancel their attachment downloads.
            await asyncio.gather(
                *(asyncio.shield(task) for task in tuple(self.pending))
            )
        bundle = await asyncio.to_thread(self.store.export, selection)
        filename = "chat-corpus-{}-{}.zip".format(
            selection.group_id or "all",
            datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ"),
        )

        async def chunks():
            try:
                while piece := await asyncio.to_thread(bundle.read, CHUNK):
                    yield piece
            finally:
                bundle.close()

        return stream_response(
            chunks(),
            content_type="application/zip",
            headers={
                "Cache-Control": "no-store",
                "Content-Disposition": f'attachment; filename="{filename}"',
                "Content-Length": str(os.fstat(bundle.fileno()).st_size),
            },
        )

    async def terminate(self):
        self.stopped = True
        if TargetGroup.active is self.settings:
            TargetGroup.active = None
        if self.pending:
            await asyncio.gather(*tuple(self.pending))
        if self.session is not None:
            await self.session.close()
        routes = getattr(self.context, "registered_web_apis", None)
        if isinstance(routes, list):
            routes[:] = [
                r
                for r in routes
                if not (isinstance(r, tuple) and r[1] in self.handlers)
            ]
