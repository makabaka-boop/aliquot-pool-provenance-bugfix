from __future__ import annotations

import sqlite3
from pathlib import Path

# tubes holds *current state* (balance, revision) and is mutable.
# splits / split_children / pools / pool_sources / lineage_edges are *history*:
# insert-only, enforced by triggers below so no code path (or manual SQL) can
# rewrite the past.
#
# lineage_edges is one row per (child, parent) *contribution*: a split child
# has one edge (to its mother tube); a pooled tube has one edge per source,
# so the full provenance DAG — and every source's contributed volume — is
# always recoverable.
SCHEMA = """
CREATE TABLE IF NOT EXISTS tubes (
    id          TEXT PRIMARY KEY,
    balance_ul  INTEGER NOT NULL CHECK (balance_ul >= 0),
    revision    INTEGER NOT NULL DEFAULT 0 CHECK (revision >= 0),
    created_at  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS splits (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    parent_id          TEXT NOT NULL REFERENCES tubes (id),
    request_key        TEXT NOT NULL,
    expected_revision  INTEGER NOT NULL,
    total_amount_ul    INTEGER NOT NULL CHECK (total_amount_ul > 0),
    created_at         TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_splits_parent ON splits (parent_id);

CREATE TABLE IF NOT EXISTS split_children (
    split_id   INTEGER NOT NULL REFERENCES splits (id),
    child_id   TEXT NOT NULL REFERENCES tubes (id),
    amount_ul  INTEGER NOT NULL CHECK (amount_ul > 0),
    position   INTEGER NOT NULL,
    PRIMARY KEY (split_id, child_id)
);

CREATE TABLE IF NOT EXISTS pools (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    new_tube_id      TEXT NOT NULL REFERENCES tubes (id),
    request_key      TEXT NOT NULL,
    total_amount_ul  INTEGER NOT NULL CHECK (total_amount_ul > 0),
    created_at       TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_pools_new_tube ON pools (new_tube_id);

CREATE TABLE IF NOT EXISTS pool_sources (
    pool_id            INTEGER NOT NULL REFERENCES pools (id),
    source_id          TEXT NOT NULL REFERENCES tubes (id),
    position           INTEGER NOT NULL,
    expected_revision  INTEGER NOT NULL,
    amount_ul          INTEGER NOT NULL CHECK (amount_ul > 0),
    PRIMARY KEY (pool_id, source_id)
);
CREATE INDEX IF NOT EXISTS idx_pool_sources_source ON pool_sources (source_id);

CREATE TABLE IF NOT EXISTS lineage_edges (
    child_id          TEXT NOT NULL REFERENCES tubes (id),
    parent_id         TEXT NOT NULL REFERENCES tubes (id),
    position          INTEGER NOT NULL DEFAULT 0,
    operation         TEXT NOT NULL CHECK (operation IN ('split', 'pool')),
    split_id          INTEGER REFERENCES splits (id),
    pool_id           INTEGER REFERENCES pools (id),
    amount_ul         INTEGER NOT NULL CHECK (amount_ul > 0),
    total_amount_ul   INTEGER NOT NULL CHECK (total_amount_ul > 0),
    PRIMARY KEY (child_id, parent_id)
);
CREATE INDEX IF NOT EXISTS idx_lineage_parent ON lineage_edges (parent_id);

-- One global idempotency namespace shared by every mutating operation. A key
-- belongs to exactly one operation (split_id XOR pool_id); request_key reuse
-- between splits and pools is therefore a conflict, never a second effect.
CREATE TABLE IF NOT EXISTS idempotency_keys (
    request_key   TEXT PRIMARY KEY,
    operation     TEXT NOT NULL CHECK (operation IN ('split', 'pool')),
    request_hash  TEXT NOT NULL,
    request_body  TEXT NOT NULL,
    response_body TEXT NOT NULL,
    split_id      INTEGER REFERENCES splits (id),
    pool_id       INTEGER REFERENCES pools (id),
    created_at    TEXT NOT NULL,
    CHECK ((split_id IS NOT NULL AND pool_id IS NULL)
           OR (pool_id IS NOT NULL AND split_id IS NULL))
);

CREATE TRIGGER IF NOT EXISTS splits_no_update
BEFORE UPDATE ON splits
BEGIN SELECT RAISE(ABORT, 'splits history is immutable'); END;

CREATE TRIGGER IF NOT EXISTS splits_no_delete
BEFORE DELETE ON splits
BEGIN SELECT RAISE(ABORT, 'splits history is immutable'); END;

CREATE TRIGGER IF NOT EXISTS split_children_no_update
BEFORE UPDATE ON split_children
BEGIN SELECT RAISE(ABORT, 'split children history is immutable'); END;

CREATE TRIGGER IF NOT EXISTS split_children_no_delete
BEFORE DELETE ON split_children
BEGIN SELECT RAISE(ABORT, 'split children history is immutable'); END;

CREATE TRIGGER IF NOT EXISTS pools_no_update
BEFORE UPDATE ON pools
BEGIN SELECT RAISE(ABORT, 'pools history is immutable'); END;

CREATE TRIGGER IF NOT EXISTS pools_no_delete
BEFORE DELETE ON pools
BEGIN SELECT RAISE(ABORT, 'pools history is immutable'); END;

CREATE TRIGGER IF NOT EXISTS pool_sources_no_update
BEFORE UPDATE ON pool_sources
BEGIN SELECT RAISE(ABORT, 'pool sources history is immutable'); END;

CREATE TRIGGER IF NOT EXISTS pool_sources_no_delete
BEFORE DELETE ON pool_sources
BEGIN SELECT RAISE(ABORT, 'pool sources history is immutable'); END;

CREATE TRIGGER IF NOT EXISTS lineage_edges_no_update
BEFORE UPDATE ON lineage_edges
BEGIN SELECT RAISE(ABORT, 'lineage history is immutable'); END;

CREATE TRIGGER IF NOT EXISTS lineage_edges_no_delete
BEFORE DELETE ON lineage_edges
BEGIN SELECT RAISE(ABORT, 'lineage history is immutable'); END;
"""

# user_version stamped once the current schema (pools + DAG lineage +
# operation-tagged idempotency) is in place.
SCHEMA_VERSION = 1


def _columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}


def _migrate(conn: sqlite3.Connection) -> None:
    """Bring databases created by older versions up to the current schema.

    The service may already hold databases containing only split rows (the
    deployed baseline). A development build also wrote a flat
    ``pool_records`` table. Both are upgraded losslessly; all history keeps
    its old request keys and timestamps.

    The upgrade is restart-safe: legacy tables are renamed aside, new ones
    are created, then data is copied with INSERT OR IGNORE inside one
    transaction. A crash mid-copy leaves the *_legacy tables behind and the
    next startup resumes the copy instead of stamping the version early.
    """
    version = conn.execute("PRAGMA user_version").fetchone()[0]
    if version >= SCHEMA_VERSION:
        return

    # First pass: create any table/trigger that is missing. executescript()
    # implicitly commits, so DDL stays outside the copy transaction below.
    conn.executescript(SCHEMA)

    tables = {
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        )
    }

    # Move legacy structures aside (renames are metadata-only and carry the
    # old triggers/indexes with them). Any triggers that moved onto a renamed
    # table must be dropped by name first: otherwise CREATE TRIGGER IF NOT
    # EXISTS considers the name taken, skips creation, and dropping the
    # legacy table would silently remove immutability protection altogether.
    if "idempotency_keys" in tables and "operation" not in _columns(conn, "idempotency_keys"):
        conn.execute("ALTER TABLE idempotency_keys RENAME TO idempotency_keys_legacy")
    if "lineage_edges" in tables and "operation" not in _columns(conn, "lineage_edges"):
        conn.execute("ALTER TABLE lineage_edges RENAME TO lineage_edges_legacy")
        conn.execute("DROP TRIGGER IF EXISTS lineage_edges_no_update")
        conn.execute("DROP TRIGGER IF EXISTS lineage_edges_no_delete")
    if "pool_records" in tables and "child_id" in _columns(conn, "pool_records"):
        conn.execute("ALTER TABLE pool_records RENAME TO pool_records_legacy")

    # Second pass: recreate the renamed-away tables and attach new triggers.
    conn.executescript(SCHEMA)

    legacy_tables = {
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        )
    }

    conn.execute("BEGIN IMMEDIATE")
    try:
        if "idempotency_keys_legacy" in legacy_tables:
            conn.execute(
                "INSERT OR IGNORE INTO idempotency_keys (request_key, operation, request_hash, "
                "request_body, response_body, split_id, pool_id, created_at) "
                "SELECT request_key, 'split', request_hash, request_body, response_body, "
                "split_id, NULL, created_at FROM idempotency_keys_legacy"
            )

        if "lineage_edges_legacy" in legacy_tables:
            conn.execute(
                "INSERT OR IGNORE INTO lineage_edges (child_id, parent_id, position, operation, "
                "split_id, pool_id, amount_ul, total_amount_ul) "
                "SELECT e.child_id, e.parent_id, 0, 'split', e.split_id, NULL, "
                "e.amount_ul, s.total_amount_ul "
                "FROM lineage_edges_legacy e JOIN splits s ON s.id = e.split_id"
            )

        if "pool_records_legacy" in legacy_tables:
            conn.execute(
                "INSERT OR IGNORE INTO pools (id, new_tube_id, request_key, total_amount_ul, created_at) "
                "SELECT rowid, child_id, request_key, total_amount_ul, created_at FROM pool_records_legacy"
            )
            # The flat table only retained the first source; it is preserved
            # at position 0 with the whole pooled volume, exactly as recorded.
            conn.execute(
                "INSERT OR IGNORE INTO pool_sources (pool_id, source_id, position, "
                "expected_revision, amount_ul) "
                "SELECT rowid, primary_source_id, 0, 0, total_amount_ul FROM pool_records_legacy"
            )
            conn.execute(
                "INSERT OR IGNORE INTO lineage_edges (child_id, parent_id, position, operation, "
                "split_id, pool_id, amount_ul, total_amount_ul) "
                "SELECT child_id, primary_source_id, 0, 'pool', NULL, rowid, "
                "total_amount_ul, total_amount_ul FROM pool_records_legacy"
            )

        for legacy in ("idempotency_keys_legacy", "lineage_edges_legacy", "pool_records_legacy"):
            if legacy in legacy_tables:
                conn.execute(f"DROP TABLE {legacy}")

        conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise


def connect(db_path: str) -> sqlite3.Connection:
    # isolation_level=None -> autocommit; transactions are opened explicitly
    # with BEGIN IMMEDIATE so the write lock is taken before any read.
    # check_same_thread=False: FastAPI may run dependency setup/teardown and
    # the endpoint itself on different threadpool threads; each connection is
    # still strictly confined to a single request.
    conn = sqlite3.connect(db_path, timeout=30.0, isolation_level=None, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA busy_timeout = 30000")
    return conn


def init_db(db_path: str) -> None:
    if db_path != ":memory:":
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = connect(db_path)
    try:
        conn.execute("PRAGMA journal_mode = WAL")
        _migrate(conn)
    finally:
        conn.close()
