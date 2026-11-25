import hashlib
import json
import sqlite3
import zipfile
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from threading import Event

import pytest
from astrbot_plugin_chat_corpus.archive import SCHEMA, Archive
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


def recognize(store, asset):
    text = {"ocr_text": "南门集合", "description": "群活动通知"}
    assert store.save_recognition(asset.digest, text, Settings.read({}))
    return text


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


def test_files_roll_by_latest_reference_without_losing_text(store, tmp_path):
    common = asset_file(tmp_path, "c", 180_000)
    unique = asset_file(tmp_path, "u", 270_000)
    # The newest message was received first.
    add(store, message(3, 300, "456", asset=common), common)
    add(store, message(1, 100, "123", text="x" * 180_000, asset=common), common)
    add(store, message(2, 200, "123", asset=unique), unique)
    recognize(store, common)
    text = recognize(store, unique)
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
    assert [r["message_id"] for r in records] == ["1", "2", "3", "4"]
    assert unique.reference()["path"] not in files
    segment = records[1]["segments"][1]
    assert (
        segment["recognition"] == text
        and segment["media"]["missing"] == "capacity_evicted"
    )
    assert "path" not in segment["media"]
    assert files[common.reference()["path"]] == common.path.read_bytes()
    assert store.status()["evicted"] == 0 and store.status()["cleared_media"] == 1
    assert store.path.stat().st_size <= baseline
    # More text now needs the space occupied by the shared original.
    assert (
        add(store, message(5, 500, text="z" * 200_000), limit=baseline, rolling=True)
        == "stored"
    )
    records, manifest, files = contents(store)
    assert not files and len(records) == 5
    assert (
        manifest["recognized_media_segments"] == manifest["cleared_media_segments"] == 3
    )
    assert store.status()["cleared_media"] == 2 and store.status()["evicted"] == 0
    assert records[0]["segments"][0]["data"]["text"] == "x" * 180_000
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
    assert (
        add(store, message(6, 60, text="x" * limit), limit=limit, rolling=True)
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
    recognize(store, item)
    with store.export(ExportRange()) as file:
        add(store, message(2, 200, text="n" * 700_000), rolling=True)
        with zipfile.ZipFile(file) as bundle:
            assert json.loads(bundle.read("messages.jsonl"))["message_id"] == "1"
            assert bundle.read(item.reference()["path"]) == item.path.read_bytes()
    assert store.status()["media"] == 0


def test_export_of_existing_archive_allows_concurrent_retention(store, tmp_path):
    item = asset_file(tmp_path, "a", 450_000)
    add(store, message(1, asset=item), item)
    recognize(store, item)
    # Simulate reopening a populated 0.1.0 database in rollback journal mode.
    with closing(sqlite3.connect(store.path)) as db:
        assert db.execute("PRAGMA journal_mode=DELETE").fetchone() == ("delete",)
    store.initialize(Settings.read({}))
    reading, release = Event(), Event()

    def pause_after_messages(*_counts):
        reading.set()
        assert release.wait(5)

    with ThreadPoolExecutor(max_workers=2) as pool:
        exporting = pool.submit(store.export, ExportRange(), pause_after_messages)
        try:
            assert reading.wait(5)
            writing = pool.submit(
                add, store, message(2, 200, text="n" * 700_000), rolling=True
            )
            assert writing.result(timeout=3) == "stored"
            assert store.status()["media"] == 0
        finally:
            release.set()
        with exporting.result(timeout=5) as file, zipfile.ZipFile(file) as bundle:
            assert bundle.testzip() is None
            assert json.loads(bundle.read("messages.jsonl"))["message_id"] == "1"
            assert bundle.read(item.reference()["path"]) == item.path.read_bytes()


def test_failed_original_survives_and_incoming_mixed_message_keeps_text(
    store, tmp_path
):
    first = asset_file(tmp_path, "a", 740_000)
    large = asset_file(tmp_path, "b", 620_000)
    assert add(store, message(1, asset=first), first) == "stored"
    assert not store.save_recognition(first.digest, {}, Settings.read({}))
    lease = store.next_recognition()[3]
    store.release_recognition(first.digest, lease, retry=True)
    mixed = message(2, 200, text="保留正文", asset=large)
    media_part = mixed["segments"][1]
    mixed["segments"].append({"type": "node", "data": {"content": [dict(media_part)]}})
    assert add(store, mixed, large, rolling=True) == "stored"
    pure = message(3, 300, text="", asset=large)
    assert add(store, pure, large, rolling=True) == "full"
    rows, manifest, files = contents(store)
    assert list(files) == [first.reference()["path"]]
    kept, image, node = rows[1]["segments"]
    assert kept["data"]["text"] == "保留正文"
    assert node["data"]["content"][0]["media"] == image["media"]
    assert (
        image["media"]["missing"] == "capacity_skipped" and "path" not in image["media"]
    )
    assert manifest["missing_media_segments"] == 2 and store.status()["evicted"] == 0


def test_pure_recognized_media_survives_as_text_in_forward_and_future_duplicates(
    store, tmp_path
):
    image = asset_file(tmp_path, "i", 650_000)
    record = message(1, text="", asset=image)
    record["segments"] = [
        {
            "type": "forward",
            "data": {
                "content": [
                    {"type": "node", "data": {"content": [record["segments"][1]]}}
                ]
            },
        }
    ]
    add(store, record, image)
    text = recognize(store, image)
    assert add(store, message(2, 110, text="t" * 600_000), rolling=True) == "stored"
    assert (
        add(store, message(3, 120, "456", text="", asset=image), image, rolling=True)
        == "stored"
    )
    records, manifest, files = contents(store)
    nested = records[0]["segments"][0]["data"]["content"][0]["data"]["content"][0]
    assert nested["recognition"] == records[2]["segments"][1]["recognition"] == text
    assert nested["media"]["missing"] == "capacity_evicted" and not files
    assert (
        manifest["recognized_media_segments"] == manifest["missing_media_segments"] == 2
    )
    assert store.next_recognition() is None and store.status()["recognized"] == 1


def test_old_database_waits_for_recognition_before_shrinking(tmp_path):
    folder = tmp_path / "data"
    folder.mkdir()
    store = Archive(folder)
    legacy = asset_file(tmp_path, "l", 1_100_000)
    record = message(1, text="旧库原文", asset=legacy)
    with closing(sqlite3.connect(store.path)) as db:
        db.execute("PRAGMA auto_vacuum=FULL")
        db.executescript(SCHEMA)
        db.execute(
            "INSERT INTO messages VALUES(1,'qq','7','123','1',100,1,?,0)",
            (json.dumps(record),),
        )
        db.execute(
            "INSERT INTO media VALUES(1,?,'png',?)",
            (legacy.digest, legacy.path.read_bytes()),
        )
        db.execute("INSERT INTO refs VALUES(1,1)")
        db.commit()
    store.initialize(Settings.read({}))
    assert store.next_recognition() is None
    reduced = Settings.read({"capacity_mib": 1, "rolling": True})
    store.apply_limit(reduced)
    job = store.next_recognition()
    assert job[:3] == (legacy.digest, "png", "qq:GroupMessage:123")
    assert not store.save_recognition(
        legacy.digest, {"description": "", "ocr_text": "   "}, reduced
    )
    store.release_recognition(legacy.digest, job[3], retry=True)
    store.initialize(reduced)
    assert (
        store.next_recognition() is None and store.status()["retrying_recognition"] == 1
    )
    assert contents(store)[2][legacy.reference()["path"]] == legacy.path.read_bytes()
    assert store.save_recognition(
        legacy.digest, {"ocr_text": "历史", "description": "旧海报"}, reduced
    )
    rows, _, files = contents(store)
    assert rows[0]["segments"][0]["data"]["text"] == "旧库原文"
    assert rows[0]["segments"][1]["recognition"]["ocr_text"] == "历史"
    assert not files and store.path.stat().st_size <= reduced.limit
    assert store.status()["retrying_recognition"] == store.status()["evicted"] == 0


def test_pressure_waits_for_available_recognition_before_evicting_old_text(
    store, tmp_path
):
    item = asset_file(tmp_path, "p", 400_000)
    add(store, message(1, 100, text="x" * 340_000))
    add(store, message(2, 200, asset=item), item)
    incoming = message(3, 300, text="y" * 360_000)
    assert store.append(incoming, {}, 0, MIB, True, wait_for_media=True) == "waiting"
    assert store.status()["messages"] == 2 and store.status()["skipped"] == 0
    recognize(store, item)
    assert store.append(incoming, {}, 0, MIB, True, wait_for_media=True) == "stored"
    assert store.status()["messages"] == 3 and store.status()["media"] == 0


def test_placeholder_messages_are_removed_before_older_text(store):
    add(store, message(1, 100, text="x" * 220_000))
    placeholder = message(2, 200)
    placeholder["segments"] = [
        {
            "type": "image",
            "data": {"file_id": "p" * 220_000},
            "media": {"missing": "unavailable"},
        }
    ]
    store.append(placeholder, {}, 1, MIB, False)
    assert add(store, message(3, 300, text="z" * 660_000), rolling=True) == "stored"
    assert [r["message_id"] for r in contents(store)[0]] == ["1", "3"]


def test_two_workers_claim_once_and_recover_after_crash(store, tmp_path, monkeypatch):
    import time

    asset = asset_file(tmp_path, "p", 2000)
    add(store, message(1, asset=asset), asset)
    other = Archive(store.path.parent)
    with ThreadPoolExecutor(max_workers=2) as pool:
        claims = list(pool.map(lambda s: s.next_recognition(), (store, other)))
    assert claims.count(None) == 1
    first = next(job for job in claims if job is not None)
    assert other.next_recognition() is None
    monkeypatch.setattr(time, "time", lambda: first[3] + 1)
    again = other.next_recognition()
    assert first[:3] == again[:3]
    store.release_recognition(asset.digest, first[3], retry=True)
    assert store.next_recognition() is None
    other.release_recognition(asset.digest, again[3], retry=False)
    assert store.next_recognition()[:3] == first[:3]
