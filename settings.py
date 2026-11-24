"""Shared configuration and export range validation."""

import re
from dataclasses import dataclass

PLUGIN = "astrbot_plugin_chat_corpus"
MIB = 1024 * 1024
GROUP_ID = re.compile(r"[1-9][0-9]{0,19}")


@dataclass(frozen=True)
class Settings:
    group_ids: tuple[str, ...]
    capacity_mib: int
    rolling: bool

    @classmethod
    def read(cls, values):
        groups = values.get("group_ids", [])
        if not isinstance(groups, list):
            raise TypeError("群号需要逐项填写")
        ids = []
        for value in groups:
            group = str(value).strip()
            if not GROUP_ID.fullmatch(group):
                raise ValueError("每个 QQ 群号都应为不以 0 开头的数字")
            ids.append(group)
        capacity = values.get("capacity_mib", 1024)
        if type(capacity) is not int or not 1 <= capacity <= 2**31 - 1:
            raise ValueError("容量上限必须为 1 ～ 2147483647 之间的整数 MiB")
        rolling = values.get("rolling", False)
        if not isinstance(rolling, bool):
            raise TypeError("动态存储需要使用开关")
        return cls(tuple(dict.fromkeys(ids)), capacity, rolling)

    @property
    def limit(self):
        return self.capacity_mib * MIB

    def public(self):
        return {
            "group_ids": list(self.group_ids),
            "capacity_mib": self.capacity_mib,
            "rolling": self.rolling,
        }


@dataclass(frozen=True)
class ExportRange:
    group_id: str = ""
    since: int | None = None
    before: int | None = None

    @classmethod
    def from_query(cls, query):
        group = query.get("group_id", "")
        if group and not GROUP_ID.fullmatch(group):
            raise ValueError("导出群号无效")

        def timestamp(key):
            value = query.get(key, "")
            if value == "":
                return None
            if not re.fullmatch(r"-?[0-9]{1,12}", value):
                raise ValueError("导出日期无效")
            return int(value)

        since, before = timestamp("since"), timestamp("before")
        if since is not None and before is not None and since >= before:
            raise ValueError("结束日期不能早于开始日期")
        return cls(group, since, before)
