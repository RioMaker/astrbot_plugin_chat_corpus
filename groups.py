"""Discover joined QQ groups from the connected OneBot accounts."""

import asyncio
from collections.abc import Mapping

from .settings import GROUP_ID


def _count(value):
    if type(value) is int and value > 0:
        return value
    if (
        isinstance(value, str)
        and value.isascii()
        and value.isdigit()
        and len(value) <= 9
    ):
        return int(value) or None
    return None


def _name(value):
    return value.strip() if isinstance(value, str) else ""


def _accounts(manager):
    for adapter in manager.get_insts():
        meta = adapter.meta()
        if meta.name != "aiocqhttp":
            continue
        client = adapter.get_client()
        # aiocqhttp accepts several reverse-WebSocket accounts on one platform.
        # HTTP and wildcard connections use the client's default routing.
        connections = getattr(client, "_wsr_api_clients", None)
        routes = tuple(connections) if isinstance(connections, Mapping) else ()
        for route in routes or (None,):
            bot_id = "" if route is None or routes == ("*",) else str(route)
            yield (str(meta.id), bot_id), client


class GroupDirectory:
    def __init__(self):
        self.snapshots = {}
        self.lock = asyncio.Lock()
        self.calls = asyncio.Semaphore(6)

    async def _query(self, client, bot_id, action, **params):
        if bot_id:
            params["self_id"] = bot_id
        async with self.calls:
            try:
                value = await asyncio.wait_for(
                    client.call_action(action, **params), timeout=8
                )
            except Exception:  # noqa: BLE001 -- one offline account must not hide the other accounts' groups
                return None
        return value.get("data", value) if isinstance(value, dict) else value

    async def _group(self, client, bot_id, group_id, raw):
        name = _name(raw.get("group_name"))
        count = _count(raw.get("member_count"))
        if not name or count is None:
            info = await self._query(
                client, bot_id, "get_group_info", group_id=int(group_id), no_cache=True
            )
            if (
                isinstance(info, dict)
                and str(info.get("group_id", group_id)) == group_id
            ):
                name = _name(info.get("group_name")) or name
                count = _count(info.get("member_count")) or count
        if count is None:
            members = await self._query(
                client, bot_id, "get_group_member_list", group_id=int(group_id)
            )
            if isinstance(members, list) and members:
                count = len(members)
        return {"group_id": group_id, "group_name": name, "member_count": count}

    async def _account(self, key, client):
        rows = await self._query(client, key[1], "get_group_list")
        if not isinstance(rows, list):
            return False
        unique = {
            str(row["group_id"]): row
            for row in rows
            if isinstance(row, dict)
            and GROUP_ID.fullmatch(str(row.get("group_id", "")))
        }
        groups = await asyncio.gather(
            *(self._group(client, key[1], gid, row) for gid, row in unique.items())
        )
        self.snapshots[key] = {g["group_id"]: g for g in groups}
        return True

    async def discover(self, manager, selected):
        async with self.lock:
            accounts = dict(_accounts(manager))
            results = await asyncio.gather(
                *(self._account(key, client) for key, client in accounts.items())
            )
            online = {
                key for key, success in zip(accounts, results, strict=True) if success
            }
            groups = {}
            for key, rows in self.snapshots.items():
                live = key in online
                for gid, row in rows.items():
                    if not live and gid not in selected:
                        continue
                    prior = groups.get(gid)
                    if prior is None or (live and not prior["online"]):
                        groups[gid] = {**row, "online": live}
                    elif prior["online"] == live:
                        prior["group_name"] = prior["group_name"] or row["group_name"]
                        prior["member_count"] = (
                            prior["member_count"] or row["member_count"]
                        )
            return {
                "groups": sorted(
                    groups.values(), key=lambda g: (g["group_name"], g["group_id"])
                ),
                "accounts": len(accounts),
                "failed": len(accounts) - len(online),
            }
