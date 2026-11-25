"""Transactional message and media storage, retention and portable ZIP snapshots."""

import io
import json
import shutil
import sqlite3
import tempfile
import threading
import time
import zipfile
from contextlib import closing, contextmanager
from pathlib import Path

CHUNK = 256 * 1024
SCHEMA = """
CREATE TABLE IF NOT EXISTS messages (
    id INTEGER PRIMARY KEY, platform_id TEXT NOT NULL, self_id TEXT NOT NULL,
    group_id TEXT NOT NULL, message_id TEXT NOT NULL, ts INTEGER NOT NULL,
    arrived INTEGER NOT NULL, payload TEXT NOT NULL, missing INTEGER NOT NULL,
    UNIQUE(platform_id, self_id, group_id, message_id)
);
CREATE INDEX IF NOT EXISTS chronological ON messages(ts, arrived, id);
CREATE INDEX IF NOT EXISTS by_group ON messages(group_id, ts);
CREATE TABLE IF NOT EXISTS media (
    id INTEGER PRIMARY KEY, digest TEXT NOT NULL UNIQUE, ext TEXT NOT NULL, body BLOB NOT NULL
);
CREATE TABLE IF NOT EXISTS refs (
    message INTEGER NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
    asset INTEGER NOT NULL REFERENCES media(id),
    PRIMARY KEY(message, asset)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS by_asset ON refs(asset);
CREATE TRIGGER IF NOT EXISTS collect_media AFTER DELETE ON refs
WHEN NOT EXISTS (SELECT 1 FROM refs WHERE asset = OLD.asset)
BEGIN
    DELETE FROM media WHERE id = OLD.asset;
END;
CREATE TABLE IF NOT EXISTS counters (
    id INTEGER PRIMARY KEY CHECK(id = 1), evicted INTEGER NOT NULL, skipped INTEGER NOT NULL
);
INSERT OR IGNORE INTO counters VALUES (1, 0, 0);
"""


def encode(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def media_name(digest, ext):
    return f"media/{digest}.{ext}"


def used_bytes(db):
    pages = db.execute("PRAGMA page_count").fetchone()[0]
    free = db.execute("PRAGMA freelist_count").fetchone()[0]
    return (pages - free) * db.execute("PRAGMA page_size").fetchone()[0]


def trim(db, limit, protected=None):
    removed = 0
    while used_bytes(db) > limit:
        row = db.execute(
            "SELECT id FROM messages ORDER BY ts, arrived, id LIMIT 1"
        ).fetchone()
        if row is None or row[0] == protected:
            return False
        db.execute("DELETE FROM messages WHERE id = ?", (row[0],))
        removed += 1
    if removed:
        db.execute("UPDATE counters SET evicted = evicted + ? WHERE id = 1", (removed,))
    return True


class Archive:
    def __init__(self, directory):
        self.path = Path(directory) / "corpus.sqlite3"
        # Serialize short operations on the live archive. Exports use a WAL reader.
        self.lock = threading.Lock()

    @contextmanager
    def connection(self):
        with self.lock, closing(sqlite3.connect(self.path, timeout=30)) as db:
            db.execute("PRAGMA foreign_keys = ON")
            with db:
                yield db

    def initialize(self, settings):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.connection() as db:
            db.execute("PRAGMA auto_vacuum = FULL")
            db.execute("PRAGMA journal_mode = WAL")
            db.executescript(SCHEMA)
            if settings.rolling:
                db.execute("BEGIN IMMEDIATE")
                trim(db, settings.limit)

    def apply_limit(self, settings):
        if settings.rolling:
            with self.connection() as db:
                db.execute("BEGIN IMMEDIATE")
                trim(db, settings.limit)

    @staticmethod
    def _key(record):
        return tuple(
            record[k] for k in ("platform_id", "self_id", "group_id", "message_id")
        )

    def contains(self, record):
        with self.connection() as db:
            return (
                db.execute(
                    "SELECT 1 FROM messages WHERE platform_id=? AND self_id=? AND group_id=? AND message_id=?",
                    self._key(record),
                ).fetchone()
                is not None
            )

    @staticmethod
    def _asset(db, item):
        row = db.execute(
            "SELECT id FROM media WHERE digest=?", (item.digest,)
        ).fetchone()
        if row:
            return row[0]
        asset_id = db.execute(
            "INSERT INTO media(digest, ext, body) VALUES(?, ?, zeroblob(?))",
            (item.digest, item.ext, item.size),
        ).lastrowid
        with (
            item.path.open("rb") as src,
            db.blobopen("media", "body", asset_id) as dest,
        ):
            shutil.copyfileobj(src, dest, CHUNK)
        return asset_id

    def append(self, record, assets, missing, limit, rolling):
        payload = encode(record)
        with self.connection() as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute("SAVEPOINT candidate")
            row = db.execute(
                """INSERT INTO messages(platform_id,self_id,group_id,message_id,ts,arrived,payload,missing)
                   VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(platform_id,self_id,group_id,message_id) DO NOTHING""",
                (
                    *self._key(record),
                    record["timestamp"],
                    record["received_ns"],
                    payload,
                    missing,
                ),
            )
            if not row.rowcount:
                return "duplicate"
            msg_id = row.lastrowid
            # If even the content alone is too large, keep the entire existing history.
            fits = (
                sum(item.size for item in assets.values()) + len(payload.encode())
                <= limit
            )
            if fits:
                for item in assets.values():
                    db.execute(
                        "INSERT INTO refs VALUES(?,?)", (msg_id, self._asset(db, item))
                    )
                fits = trim(db, limit, msg_id) if rolling else used_bytes(db) <= limit
            if not fits:
                db.execute("ROLLBACK TO candidate")
                db.execute("UPDATE counters SET skipped = skipped + 1 WHERE id = 1")
            db.execute("RELEASE candidate")
            return "stored" if fits else "full"

    def status(self):
        with self.connection() as db:
            groups = [
                {"group_id": g, "messages": n, "last_timestamp": last}
                for g, n, last in db.execute(
                    "SELECT group_id, count(*), max(ts) FROM messages GROUP BY group_id ORDER BY group_id"
                )
            ]
            evicted, skipped = db.execute(
                "SELECT evicted, skipped FROM counters"
            ).fetchone()
            return {
                "groups": groups,
                "messages": sum(g["messages"] for g in groups),
                "used_bytes": used_bytes(db),
                "media": db.execute("SELECT count(*) FROM media").fetchone()[0],
                "missing": db.execute(
                    "SELECT coalesce(sum(missing),0) FROM messages"
                ).fetchone()[0],
                "evicted": evicted,
                "skipped": skipped,
            }

    def export(self, selection, progress=None):
        filters, args = [], []
        for condition, value in (
            ("m.group_id=?", selection.group_id or None),
            ("m.ts>=?", selection.since),
            ("m.ts<?", selection.before),
        ):
            if value is not None:
                filters.append(condition)
                args.append(value)
        where = " WHERE " + " AND ".join(filters) if filters else ""
        result = tempfile.TemporaryFile(mode="w+b")  # noqa: SIM115 -- the response owns the open snapshot
        try:
            with (
                closing(sqlite3.connect(self.path, timeout=30)) as db,
                zipfile.ZipFile(
                    result, "w", zipfile.ZIP_DEFLATED, compresslevel=1
                ) as bundle,
            ):
                db.execute("PRAGMA query_only = ON")
                db.execute("BEGIN")
                counts = {"messages": 0, "media_files": 0, "missing_media_segments": 0}

                def report():
                    if progress is not None:
                        progress(
                            counts["messages"], counts["media_files"], result.tell()
                        )

                query = (
                    "SELECT m.payload, m.missing FROM messages m"
                    + where
                    + " ORDER BY m.ts,m.arrived,m.id"
                )
                with io.BufferedWriter(
                    bundle.open("messages.jsonl", "w", force_zip64=True), CHUNK
                ) as lines:
                    for payload, missing in db.execute(query, args):
                        lines.write(payload.encode("utf-8") + b"\n")
                        counts["messages"] += 1
                        counts["missing_media_segments"] += missing
                        if counts["messages"] % 256 == 0:
                            report()
                report()
                # Deduplicate integer IDs before looking up rows containing large BLOBs.
                assets = "SELECT asset FROM refs"
                if where:
                    assets = (
                        "SELECT r.asset FROM messages m JOIN refs r ON r.message=m.id"
                        + where
                    )
                query = f"SELECT id,digest,ext FROM media WHERE id IN ({assets})"
                for asset_id, digest, ext in db.execute(query, args):
                    name = media_name(digest, ext)
                    entry = name if ext == "wav" else zipfile.ZipInfo(name)
                    with (
                        db.blobopen("media", "body", asset_id, readonly=True) as src,
                        bundle.open(entry, "w", force_zip64=True) as dest,
                    ):
                        while piece := src.read(CHUNK):
                            dest.write(piece)
                            report()
                    counts["media_files"] += 1
                    report()
                bundle.writestr(
                    "manifest.json",
                    encode(
                        {
                            "schema_version": 1,
                            "exported_at": int(time.time()),
                            "group_id": selection.group_id or None,
                            "since": selection.since,
                            "before": selection.before,
                            "timestamps": "Unix seconds UTC",
                            "order": ["timestamp", "received_ns"],
                            **counts,
                        }
                    ),
                )
            result.seek(0)
            return result
        except BaseException:
            result.close()
            raise
