import hashlib
import json
import sqlite3
import zipfile
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing

import pytest
from astrbot_plugin_chat_corpus.archive import Archive
from astrbot_plugin_chat_corpus.media import Asset
from astrbot_plugin_chat_corpus.settings import MIB, ExportRange, Settings


@pytest.fixture
def store(tmp_path):
    value = Archive(tmp_path / "data")
    value.initialize(Settings.read({}))
    return value


def message(mid, stamp=100, group="123", text="一条消息", asset=None, arrived=None):
    parts = [{"type": "text", "data": {"text": text}}]
    if asset:
        parts.append({"type": "image", "data": {}, "media": asset.reference()})
    return {
        "platform_id": "qq",
        "self_id": "7",
        "group_id": group,
        "message_id": str(mid),
        "timestamp": stamp,
        "received_ns": arrived or int(mid),
        "segments": parts,
    }


def asset_file(tmp_path, name, size):
    data = name.encode() * size
    path = tmp_path / name
    path.write_bytes(data)
    return Asset(path, hashlib.sha256(data).hexdigest(), "png", len(data))


def add(store, record, asset=None, *, limit=MIB, rolling=False):
    return store.append(
        record, {asset.digest: asset} if asset else {}, 0, limit, rolling
    )


def contents(store, selection=None):
    with (
        store.export(selection or ExportRange()) as file,
        zipfile.ZipFile(file) as bundle,
    ):
        assert bundle.testzip() is None
        return (
            [json.loads(v) for v in bundle.read("messages.jsonl").splitlines()],
            json.loads(bundle.read("manifest.json")),
            {k: bundle.read(k) for k in bundle.namelist() if k.startswith("media/")},
        )


def test_empty_bundle(store):
    records, manifest, media = contents(store)
    assert records == [] and media == {}
    assert (
        manifest["messages"]
        == manifest["media_files"]
        == manifest["missing_media_segments"]
        == 0
    )


def test_filtered_snapshot_and_order(store, tmp_path):
    shared = asset_file(tmp_path, "a", 1234)
    other = asset_file(tmp_path, "b", 1234)
    for mid, stamp, group, item in (
        (2, 10, "123", shared),
        (1, 10, "123", shared),
        (3, 11, "123", shared),
        (4, 10, "456", other),
    ):
        assert add(store, message(mid, stamp, group, asset=item), item) == "stored"
    records, manifest, files = contents(store, ExportRange("123", 10, 11))
    assert [r["message_id"] for r in records] == ["1", "2"]
    assert manifest["group_id"] == "123" and manifest["messages"] == 2
    assert manifest["media_files"] == 1 and manifest["missing_media_segments"] == 0
    assert files == {shared.reference()["path"]: shared.path.read_bytes()}
    assert store.status()["media"] == 2


def test_no_rolling_preserves_history_at_physical_limit(store, tmp_path):
    item = asset_file(tmp_path, "a", 620_000)
    big = asset_file(tmp_path, "b", 600_000)
    assert add(store, message(1, asset=item), item) == "stored"
    assert add(store, message(2, asset=big), big) == "full"
    assert store.status()["skipped"] == 1
    assert [r["message_id"] for r in contents(store)[0]] == ["1"]
    assert store.path.stat().st_size == store.status()["used_bytes"] <= MIB


def test_global_fifo_shared_blobs_and_physical_reclamation(store, tmp_path):
    common = asset_file(tmp_path, "c", 180_000)
    unique = asset_file(tmp_path, "u", 270_000)
    # The newest message was received first.
    add(store, message(3, 300, "456", asset=common), common)
    add(store, message(1, 100, "123", text="x" * 180_000, asset=common), common)
    add(store, message(2, 200, "123", asset=unique), unique)
    baseline = store.path.stat().st_size
    assert (
        add(
            store,
            message(4, 400, "789", text="y" * 120_000),
            limit=baseline,
            rolling=True,
        )
        == "stored"
    )
    records, _, files = contents(store)
    assert [r["message_id"] for r in records] == ["2", "3", "4"]
    assert files[common.reference()["path"]] == common.path.read_bytes()
    assert store.status()["evicted"] == 1
    assert store.path.stat().st_size <= baseline
    # Once the oldest remaining message no longer fits, its exclusive attachment disappears.
    assert (
        add(store, message(5, 500, text="z" * 200_000), limit=baseline, rolling=True)
        == "stored"
    )
    assert unique.reference()["path"] not in contents(store)[2]
    assert common.reference()["path"] in contents(store)[2]
    assert store.path.stat().st_size <= baseline


def test_impossible_or_out_of_order_record_cannot_erase_history(store, tmp_path):
    for mid in range(1, 5):
        add(store, message(mid, 20 + mid, text="A" * 36_000))
    limit = store.path.stat().st_size
    before = contents(store)[0]
    assert (
        add(store, message(5, 1, text="late" * 12_000), limit=limit, rolling=True)
        == "full"
    )
    item = asset_file(tmp_path, "q", limit - 8192)
    assert (
        add(store, message(6, 60, asset=item), item, limit=limit, rolling=True)
        == "full"
    )
    assert contents(store)[0] == before
    assert store.status()["evicted"] == 0 and store.status()["media"] == 0


def test_shrink_restart_and_change_back_to_static(store, tmp_path):
    for mid in range(1, 7):
        add(store, message(mid, mid, text="x" * 320_000), limit=3 * MIB)
    store.apply_limit(Settings.read({"capacity_mib": 1}))
    assert store.status()["messages"] == 6
    limits = Settings.read({"capacity_mib": 1, "rolling": True})
    store.apply_limit(limits)
    after = contents(store)[0]
    assert 0 < len(after) < 6 and after[-1]["message_id"] == "6"
    assert store.path.stat().st_size <= MIB
    reopened = Archive(tmp_path / "data")
    reopened.initialize(limits)
    assert contents(reopened)[0] == after
    assert reopened.status()["evicted"] == 6 - len(after)


def test_writes_are_idempotent_and_failed_copy_rolls_back(store, tmp_path):
    item = asset_file(tmp_path, "a", 1000)
    record = message(1, asset=item)
    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(lambda _: add(store, record, item), range(12)))
    assert results.count("stored") == 1 and results.count("duplicate") == 11
    missing = asset_file(tmp_path, "b", 100)
    missing.path.unlink()
    with pytest.raises(FileNotFoundError):
        add(store, message(2, asset=missing), missing)
    assert store.status()["messages"] == store.status()["media"] == 1
    with closing(sqlite3.connect(store.path)) as db:
        assert db.execute("PRAGMA integrity_check").fetchone() == ("ok",)
        assert db.execute("PRAGMA foreign_key_check").fetchall() == []


def test_export_stays_consistent_during_retention(store, tmp_path):
    item = asset_file(tmp_path, "a", 450_000)
    add(store, message(1, asset=item), item)
    with store.export(ExportRange()) as file:
        add(store, message(2, 200, text="n" * 700_000), rolling=True)
        with zipfile.ZipFile(file) as bundle:
            assert json.loads(bundle.read("messages.jsonl"))["message_id"] == "1"
            assert bundle.read(item.reference()["path"]) == item.path.read_bytes()
    assert store.status()["media"] == 0
