"""Temporary ZIPs prepared for an authenticated dashboard download."""

import asyncio
import os
from uuid import uuid4

from astrbot.api import logger


class ExportJob:
    def __init__(self, store, selection, expire):
        self.id = uuid4().hex
        self.selection = selection
        self.phase = "packing"
        self.progress = (0, 0, 0)
        self.size = 0
        self.sent = 0
        self.file = None
        self.timer = None
        self._expire = expire
        self.task = asyncio.create_task(self._build(store))

    def _progress(self, messages, media_files, packed_bytes):
        # Publish the worker's three counters together for the dashboard.
        self.progress = (messages, media_files, packed_bytes)

    async def _build(self, store):
        try:
            self.file = await asyncio.to_thread(
                store.export, self.selection, self._progress
            )
            self.size = os.fstat(self.file.fileno()).st_size
            self.phase = "ready"
        except Exception as exc:  # noqa: BLE001 -- report failure to the waiting dashboard
            self.close()
            self.phase = "failed"
            logger.error("[对话语料库] ZIP 打包失败：%s", type(exc).__name__)
        self.expire_later()

    def expire_later(self):
        # Release unclaimed files and short-lived completion status after the page closes.
        self.timer = asyncio.get_running_loop().call_later(600, self._expire, self)

    def state(self):
        messages, media_files, packed_bytes = self.progress
        return {
            "id": self.id,
            "phase": self.phase,
            "messages": messages,
            "media_files": media_files,
            "packed_bytes": packed_bytes,
            "size": self.size,
            "sent": self.sent,
        }

    def take(self):
        self.phase = "sending"
        bundle, self.file = self.file, None
        return bundle

    def close(self):
        if self.timer is not None:
            self.timer.cancel()
        if self.file is not None:
            self.file.close()
            self.file = None
