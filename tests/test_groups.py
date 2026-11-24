import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

from astrbot_plugin_chat_corpus.groups import GroupDirectory


def platform(name, instance, client):
    return SimpleNamespace(
        meta=lambda: SimpleNamespace(name=name, id=instance), get_client=lambda: client
    )


def test_discovers_silent_groups_across_bots_and_fills_missing_details():
    calls = []

    async def action(name, **args):
        calls.append((name, args))
        if name == "get_group_list":
            if args["self_id"] == "7":
                return [
                    {"group_id": 123, "group_name": "太鼓群", "member_count": 37},
                    {"group_id": 456, "group_name": "", "member_count": 0},
                ]
            return {
                "data": [
                    {"group_id": "123", "group_name": "太鼓群", "member_count": "37"},
                    {"group_id": 567, "group_name": "安静的群"},
                    {"group_id": "../bad"},
                ]
            }
        if name == "get_group_info":
            assert args["no_cache"]
            if args["group_id"] == 456:
                return {
                    "data": {
                        "group_id": 456,
                        "group_name": "同好会",
                        "member_count": 22,
                    }
                }
            return {"group_id": 567}
        assert (
            name == "get_group_member_list"
            and args["group_id"] == 567
            and args["self_id"] == "8"
        )
        return [{"user_id": 1, "nickname": "不保存名单"}, {"user_id": 2}]

    client = SimpleNamespace(
        call_action=action, _wsr_api_clients={"7": object(), "8": object()}
    )
    manager = SimpleNamespace(
        get_insts=lambda: [
            platform("aiocqhttp", "qq", client),
            platform("telegram", "tg", None),
        ]
    )
    result = asyncio.run(GroupDirectory().discover(manager, []))
    assert result["accounts"] == 2 and result["failed"] == 0
    assert {
        g["group_id"]: (g["group_name"], g["member_count"]) for g in result["groups"]
    } == {
        "123": ("太鼓群", 37),
        "456": ("同好会", 22),
        "567": ("安静的群", 2),
    }
    assert all(
        g["online"] and set(g) == {"online", "group_name", "group_id", "member_count"}
        for g in result["groups"]
    )
    assert sum(action == "get_group_member_list" for action, _ in calls) == 1
    assert not any(args.get("group_id") == 123 for _, args in calls)


def test_refresh_keeps_selected_metadata_when_a_connection_drops():
    first = SimpleNamespace(
        call_action=AsyncMock(
            return_value=[{"group_id": 123, "group_name": "甲", "member_count": 3}]
        )
    )
    second = SimpleNamespace(
        call_action=AsyncMock(
            return_value=[{"group_id": 456, "group_name": "乙", "member_count": 6}]
        )
    )
    manager = SimpleNamespace(
        get_insts=lambda: [
            platform("aiocqhttp", "a", first),
            platform("aiocqhttp", "b", second),
        ]
    )

    async def run():
        directory = GroupDirectory()
        assert len((await directory.discover(manager, []))["groups"]) == 2
        first.call_action.side_effect = OSError("offline")
        second.call_action.return_value = [
            {"group_id": 456, "group_name": "更新群名", "member_count": 9}
        ]
        result = await directory.discover(manager, ["123"])
        groups = {g["group_id"]: g for g in result["groups"]}
        assert result["failed"] == 1 and result["accounts"] == 2
        assert groups["123"] == {
            "group_id": "123",
            "group_name": "甲",
            "member_count": 3,
            "online": False,
        }
        assert (
            groups["456"]["online"]
            and groups["456"]["group_name"] == "更新群名"
            and groups["456"]["member_count"] == 9
        )
        assert [
            g["group_id"] for g in (await directory.discover(manager, []))["groups"]
        ] == ["456"]

    asyncio.run(run())


def test_unavailable_member_count_and_no_connected_adapter():
    async def action(name, **params):
        if name == "get_group_list":
            return [{"group_id": 888, "group_name": "人数未知"}]
        raise OSError("unsupported")

    manager = SimpleNamespace(
        get_insts=lambda: [
            platform("aiocqhttp", "qq", SimpleNamespace(call_action=action))
        ]
    )
    directory = GroupDirectory()
    assert (
        asyncio.run(directory.discover(manager, []))["groups"][0]["member_count"]
        is None
    )
    assert asyncio.run(directory.discover(SimpleNamespace(get_insts=list), [])) == {
        "groups": [],
        "accounts": 0,
        "failed": 0,
    }
