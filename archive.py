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
from copy import deepcopy
from pathlib import Path

from .recognition import usable_text
from .retention import (
    apply_rolling_limit,
    queue_legacy_media,
    reclaim_files,
    reclaim_messages,
    used_bytes,
)

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
INSERT OR IGNORE INTO counters(id,evicted,skipped) VALUES (1, 0, 0);
"""


def encode(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def media_name(digest, ext):
    return f"media/{digest}.{ext}"


def segments(parts):
    for part in parts:
        if not isinstance(part, dict):
            continue
        yield part
        data = part.get("data")
        if isinstance(data, dict):
            for key in ("content", "messages", "message"):
                if isinstance(children := data.get(key), list):
                    yield from segments(children)


def has_chat_text(record):
    for part in segments(record["segments"]):
        data = part.get("data")
        if not isinstance(data, dict):
            continue
        if part.get("type") == "text" and str(data.get("text") or "").strip():
            return True
        if (
            part.get("type") == "node"
            and isinstance(data.get("content"), str)
            and data["content"].strip()
        ):
            return True
    return False


def migrate(db):
    if db.execute("PRAGMA user_version").fetchone()[0] >= 2:
        return
    # Legacy files become eligible on the first capacity pressure.
    # Keep frequent queue updates off the rows containing the large original BLOBs.
    db.execute("""CREATE TABLE media_text (
        asset INTEGER PRIMARY KEY REFERENCES media(id) ON DELETE CASCADE,
        recognition TEXT, next_try INTEGER, origin TEXT, lease_until INTEGER NOT NULL DEFAULT 0
    )""")
    db.execute("INSERT INTO media_text(asset) SELECT id FROM media")
    db.execute("ALTER TABLE messages ADD COLUMN has_text INTEGER NOT NULL DEFAULT 0")
    db.execute("ALTER TABLE counters ADD COLUMN cleared INTEGER NOT NULL DEFAULT 0")
    for msg_id, payload in db.execute("SELECT id,payload FROM messages ORDER BY id"):
        if has_chat_text(json.loads(payload)):
            db.execute("UPDATE messages SET has_text=1 WHERE id=?", (msg_id,))
    db.execute(
        "CREATE INDEX recognition_queue ON media_text(next_try,asset) WHERE recognition IS NULL"
    )
    db.execute("CREATE INDEX retention_order ON messages(has_text,ts,arrived,id)")
    db.execute("PRAGMA user_version=2")


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
            db.execute("BEGIN IMMEDIATE")
            migrate(db)
            if settings.rolling:
                apply_rolling_limit(db, settings.limit)

    def apply_limit(self, settings):
        if settings.rolling:
            with self.connection() as db:
                db.execute("BEGIN IMMEDIATE")
                apply_rolling_limit(db, settings.limit)

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
    def _reserve_asset(db, item, origin):
        asset_id = db.execute(
            "INSERT INTO media(digest,ext,body) VALUES(?,?,zeroblob(?))",
            (item.digest, item.ext, item.size),
        ).lastrowid
        db.execute(
            "INSERT INTO media_text(asset,next_try,origin) VALUES(?,unixepoch(),?)",
            (asset_id, origin),
        )
        return asset_id

    def _try_append(
        self, db, record, known, new, missing, limit, rolling, origin, wait_for_media
    ):
        payload = encode(record)
        if len(payload.encode()) + sum(item.size for item in new) > limit:
            return False
        has_text = has_chat_text(record) or any(
            recognized for _, recognized in known.values()
        )
        db.execute("SAVEPOINT candidate")
        msg_id = db.execute(
            """INSERT INTO messages(platform_id,self_id,group_id,message_id,ts,arrived,payload,missing,has_text)
               VALUES(?,?,?,?,?,?,?,?,?)""",
            (
                *self._key(record),
                record["timestamp"],
                record["received_ns"],
                payload,
                missing,
                has_text,
            ),
        ).lastrowid
        ids = [asset_id for asset_id, _ in known.values()]
        db.executemany(
            """UPDATE media_text SET next_try=unixepoch(),origin=coalesce(origin,?)
               WHERE asset=? AND recognition IS NULL AND next_try IS NULL""",
            ((origin, asset_id) for asset_id in ids),
        )
        reserved = [(self._reserve_asset(db, item, origin), item) for item in new]
        ids.extend(asset_id for asset_id, _ in reserved)
        db.executemany(
            "INSERT INTO refs VALUES(?,?)", ((msg_id, asset_id) for asset_id in ids)
        )
        fits = reclaim_files(db, limit) if rolling else used_bytes(db) <= limit
        if not fits and rolling:
            allow_text = has_text and not new
            if allow_text and wait_for_media:
                allow_text = not self._recognition_due(db)
            fits = reclaim_messages(db, limit, protected=msg_id, allow_text=allow_text)
        if fits:
            for asset_id, item in reserved:
                with (
                    item.path.open("rb") as src,
                    db.blobopen("media", "body", asset_id) as dest,
                ):
                    shutil.copyfileobj(src, dest, CHUNK)
        else:
            db.execute("ROLLBACK TO candidate")
        db.execute("RELEASE candidate")
        return fits

    @staticmethod
    def _recognition_due(db):
        return (
            db.execute(
                "SELECT 1 FROM media_text WHERE recognition IS NULL AND next_try<=? LIMIT 1",
                (int(time.time()),),
            ).fetchone()
            is not None
        )

    def append(
        self,
        record,
        assets,
        missing,
        limit,
        rolling,
        origin=None,
        *,
        wait_for_media=False,
    ):
        with self.connection() as db:
            db.execute("BEGIN IMMEDIATE")
            if db.execute(
                "SELECT 1 FROM messages WHERE platform_id=? AND self_id=? AND group_id=? AND message_id=?",
                self._key(record),
            ).fetchone():
                return "duplicate"
            known, new = {}, []
            for digest, item in assets.items():
                row = db.execute(
                    "SELECT a.id,t.recognition IS NOT NULL FROM media a JOIN media_text t ON t.asset=a.id WHERE a.digest=?",
                    (digest,),
                ).fetchone()
                if row:
                    known[digest] = row
                else:
                    new.append(item)
            if self._try_append(
                db, record, known, new, missing, limit, rolling, origin, wait_for_media
            ):
                return "stored"
            if rolling:
                queue_legacy_media(db)
                if new and (has_chat_text(record) or known):
                    # Refuse new originals before allowing older text to roll out.
                    partial = deepcopy(record)
                    for part in segments(partial["segments"]):
                        media = part.get("media", {})
                        if "path" in media and media.get("sha256") not in known:
                            part["media"] = {
                                k: v for k, v in media.items() if k != "path"
                            }
                            part["media"]["missing"] = "capacity_skipped"
                            missing += 1
                    if self._try_append(
                        db,
                        partial,
                        known,
                        [],
                        missing,
                        limit,
                        rolling,
                        origin,
                        wait_for_media,
                    ):
                        return "stored"
                if wait_for_media and self._recognition_due(db):
                    return "waiting"
            db.execute("UPDATE counters SET skipped=skipped+1 WHERE id=1")
            return "full"

    def next_recognition(self):
        with self.connection() as db:
            db.execute("BEGIN IMMEDIATE")
            now = int(time.time())
            row = db.execute(
                """
                SELECT a.id,a.digest,a.ext,coalesce(t.origin, (
                    SELECT m.platform_id || ':GroupMessage:' || m.group_id FROM refs r
                    JOIN messages m ON m.id=r.message WHERE r.asset=a.id ORDER BY m.id LIMIT 1
                )) FROM media_text t JOIN media a ON a.id=t.asset
                WHERE t.recognition IS NULL AND t.next_try<=? AND t.lease_until<=?
                ORDER BY t.next_try,t.asset LIMIT 1
            """,
                (now, now),
            ).fetchone()
            if row is None:
                return None
            # A second instance cannot submit this file during a reload. A crashed
            # worker's lease expires after the provider timeout plus staging time.
            lease = now + 300
            db.execute(
                "UPDATE media_text SET lease_until=? WHERE asset=?", (lease, row[0])
            )
            return (*row[1:], lease)

    def copy_original(self, digest, path):
        # Copy using a WAL snapshot so staging a large attachment does not block captures.
        with closing(sqlite3.connect(self.path, timeout=30)) as db:
            db.execute("BEGIN")
            row = db.execute(
                "SELECT a.id FROM media a JOIN media_text t ON t.asset=a.id WHERE a.digest=? AND t.recognition IS NULL",
                (digest,),
            ).fetchone()
            if row is None:
                return False
            with (
                db.blobopen("media", "body", row[0], readonly=True) as src,
                path.open("wb") as out,
            ):
                shutil.copyfileobj(src, out, CHUNK)
            return True

    def release_recognition(self, digest, lease, *, retry):
        with self.connection() as db:
            db.execute(
                """UPDATE media_text SET next_try=?,lease_until=0 WHERE asset=(SELECT id FROM media WHERE digest=?)
                   AND recognition IS NULL AND lease_until=?""",
                (int(time.time()) + (600 if retry else 0), digest, lease),
            )

    def save_recognition(self, digest, result, settings):
        with self.connection() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute(
                "SELECT a.id,a.ext FROM media a JOIN media_text t ON t.asset=a.id WHERE a.digest=? AND t.recognition IS NULL",
                (digest,),
            ).fetchone()
            if row is None:
                return True
            text = usable_text(row[1], result)
            if text is None:
                return False
            annotation = encode(text)
            if len(annotation.encode()) > settings.limit:
                return False
            previous = used_bytes(db)
            db.execute("SAVEPOINT annotation")
            db.execute(
                "UPDATE media_text SET recognition=?,next_try=NULL,lease_until=0 WHERE asset=?",
                (annotation, row[0]),
            )
            db.execute(
                "UPDATE messages SET has_text=1 WHERE id IN (SELECT message FROM refs WHERE asset=?)",
                (row[0],),
            )
            if settings.rolling:
                reclaim_files(db, settings.limit)
            # A previous limit reduction may already be waiting for other originals.
            fits = used_bytes(db) <= max(settings.limit, previous)
            if not fits:
                db.execute("ROLLBACK TO annotation")
            elif settings.rolling and previous > settings.limit:
                apply_rolling_limit(db, settings.limit)
            db.execute("RELEASE annotation")
            return fits

    def status(self):
        with self.connection() as db:
            groups = [
                {"group_id": g, "messages": n, "last_timestamp": last}
                for g, n, last in db.execute(
                    "SELECT group_id, count(*), max(ts) FROM messages GROUP BY group_id ORDER BY group_id"
                )
            ]
            evicted, skipped, cleared = db.execute(
                "SELECT evicted,skipped,cleared FROM counters"
            ).fetchone()
            files, recognized, waiting, retrying = db.execute("""
                SELECT count(*) FILTER (WHERE length(body)>0), count(recognition),
                       count(*) FILTER (WHERE recognition IS NULL),
                       count(*) FILTER (WHERE next_try>unixepoch()) FROM media_text t JOIN media a ON a.id=t.asset
            """).fetchone()
            return {
                "groups": groups,
                "messages": sum(g["messages"] for g in groups),
                "used_bytes": used_bytes(db),
                "media": files,
                "recognized": recognized,
                "awaiting_recognition": waiting,
                "retrying_recognition": retrying,
                "cleared_media": cleared,
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
                counts = {
                    "messages": 0,
                    "media_files": 0,
                    "missing_media_segments": 0,
                    "recognized_media_segments": 0,
                    "cleared_media_segments": 0,
                }

                def report():
                    if progress is not None:
                        progress(
                            counts["messages"], counts["media_files"], result.tell()
                        )

                query = (
                    """SELECT m.payload,m.missing,(
                        SELECT json_group_object(a.digest,json_object(
                            'text',json(t.recognition),'original',length(a.body)>0
                        )) FROM refs r JOIN media a ON a.id=r.asset JOIN media_text t ON t.asset=a.id
                        WHERE r.message=m.id AND t.recognition IS NOT NULL
                    ) FROM messages m"""
                    + where
                    + " ORDER BY m.ts,m.arrived,m.id"
                )
                with io.BufferedWriter(
                    bundle.open("messages.jsonl", "w", force_zip64=True), CHUNK
                ) as lines:
                    for payload, missing, annotations in db.execute(query, args):
                        if annotations != "{}":
                            record = json.loads(payload)
                            by_digest = json.loads(annotations)
                            for part in segments(record["segments"]):
                                ref = part.get("media", {})
                                annotation = by_digest.get(ref.get("sha256"))
                                if "path" not in ref or annotation is None:
                                    continue
                                part["recognition"] = annotation["text"]
                                counts["recognized_media_segments"] += 1
                                if not annotation["original"]:
                                    del ref["path"]
                                    ref["missing"] = "capacity_evicted"
                                    missing += 1
                                    counts["cleared_media_segments"] += 1
                            payload = encode(record)
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
                query = f"SELECT id,digest,ext FROM media WHERE length(body)>0 AND id IN ({assets})"
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
                            "schema_version": 2,
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
