"""Capacity enforcement within the caller's SQLite write transaction."""


def used_bytes(db):
    pages = db.execute("PRAGMA page_count").fetchone()[0]
    free = db.execute("PRAGMA freelist_count").fetchone()[0]
    return (pages - free) * db.execute("PRAGMA page_size").fetchone()[0]


def queue_legacy_media(db):
    db.execute(
        "UPDATE media_text SET next_try=unixepoch() WHERE recognition IS NULL AND next_try IS NULL"
    )


def reclaim_files(db, limit):
    if used_bytes(db) <= limit:
        return True
    queue_legacy_media(db)
    # A shared file is as recent as its newest referencing message across all groups.
    files = db.execute("""
        SELECT a.id FROM media a JOIN media_text t ON t.asset=a.id JOIN messages newest ON newest.id=(
            SELECT m.id FROM refs r JOIN messages m ON m.id=r.message
            WHERE r.asset=a.id ORDER BY m.ts DESC,m.arrived DESC,m.id DESC LIMIT 1
        ) WHERE t.recognition IS NOT NULL AND length(a.body)>0
        ORDER BY newest.ts,newest.arrived,newest.id,a.id
    """)
    cleared = 0
    for (asset_id,) in files:
        db.execute("UPDATE media SET body=X'' WHERE id=?", (asset_id,))
        cleared += 1
        if used_bytes(db) <= limit:
            break
    if cleared:
        db.execute("UPDATE counters SET cleared=cleared+? WHERE id=1", (cleared,))
    return used_bytes(db) <= limit


def reclaim_messages(db, limit, *, protected=None, allow_text):
    # Never delete a message that still owns a pending/failed original.
    eligible = "" if allow_text else "AND m.has_text=0"
    rows = db.execute(f"""
        SELECT m.id FROM messages m WHERE NOT EXISTS (
            SELECT 1 FROM refs r JOIN media_text t ON t.asset=r.asset
            WHERE r.message=m.id AND t.recognition IS NULL
        ) {eligible} ORDER BY m.has_text,m.ts,m.arrived,m.id
    """)
    removed = 0
    for (msg_id,) in rows:
        if msg_id == protected:
            break
        db.execute("DELETE FROM messages WHERE id=?", (msg_id,))
        removed += 1
        if used_bytes(db) <= limit:
            break
    if removed:
        db.execute("UPDATE counters SET evicted=evicted+? WHERE id=1", (removed,))
    return used_bytes(db) <= limit


def apply_rolling_limit(db, limit):
    if reclaim_files(db, limit):
        return
    # After a limit reduction, let recognition release originals before sacrificing text.
    pending = db.execute(
        "SELECT 1 FROM media_text WHERE recognition IS NULL LIMIT 1"
    ).fetchone()
    reclaim_messages(db, limit, allow_text=not pending)
