from __future__ import annotations

import sqlite3
from pathlib import Path

# tubes holds *current state* (balance, revision) and is mutable.
# splits / split_children / lineage_edges are *history*: insert-only, enforced
# by triggers below so no code path (or manual SQL) can rewrite the past.
# pool_records / pool_sources are the same kind of history for merges: a pool
# has 2-5 contributing sources, each its own edge — lineage is a DAG, never a
# single "primary source".
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

CREATE TABLE IF NOT EXISTS lineage_edges (
    child_id   TEXT PRIMARY KEY REFERENCES tubes (id),
    parent_id  TEXT NOT NULL REFERENCES tubes (id),
    split_id   INTEGER NOT NULL REFERENCES splits (id),
    amount_ul  INTEGER NOT NULL CHECK (amount_ul > 0)
);

CREATE TABLE IF NOT EXISTS pool_records (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    child_id         TEXT NOT NULL UNIQUE REFERENCES tubes (id),
    request_key      TEXT NOT NULL,
    total_amount_ul  INTEGER NOT NULL CHECK (total_amount_ul > 0),
    created_at       TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS pool_sources (
    pool_id           INTEGER NOT NULL REFERENCES pool_records (id),
    source_id         TEXT NOT NULL REFERENCES tubes (id),
    expected_revision INTEGER NOT NULL,
    amount_ul         INTEGER NOT NULL CHECK (amount_ul > 0),
    position          INTEGER NOT NULL,
    PRIMARY KEY (pool_id, source_id)
);
CREATE INDEX IF NOT EXISTS idx_pool_sources_source ON pool_sources (source_id);

-- Idempotency receipts are shared by every mutating operation: one global
-- request-key namespace, so a key consumed by a split cannot later power a
-- pool (or vice versa). Exactly one of split_id / pool_id is set.
CREATE TABLE IF NOT EXISTS idempotency_keys (
    request_key   TEXT PRIMARY KEY,
    request_hash  TEXT NOT NULL,
    request_body  TEXT NOT NULL,
    response_body TEXT NOT NULL,
    operation     TEXT NOT NULL CHECK (operation IN ('split', 'pool')),
    split_id      INTEGER REFERENCES splits (id),
    pool_id       INTEGER REFERENCES pool_records (id),
    created_at    TEXT NOT NULL
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

CREATE TRIGGER IF NOT EXISTS lineage_edges_no_update
BEFORE UPDATE ON lineage_edges
BEGIN SELECT RAISE(ABORT, 'lineage history is immutable'); END;

CREATE TRIGGER IF NOT EXISTS lineage_edges_no_delete
BEFORE DELETE ON lineage_edges
BEGIN SELECT RAISE(ABORT, 'lineage history is immutable'); END;

CREATE TRIGGER IF NOT EXISTS pool_records_no_update
BEFORE UPDATE ON pool_records
BEGIN SELECT RAISE(ABORT, 'pool history is immutable'); END;

CREATE TRIGGER IF NOT EXISTS pool_records_no_delete
BEFORE DELETE ON pool_records
BEGIN SELECT RAISE(ABORT, 'pool history is immutable'); END;

CREATE TRIGGER IF NOT EXISTS pool_sources_no_update
BEFORE UPDATE ON pool_sources
BEGIN SELECT RAISE(ABORT, 'pool source history is immutable'); END;

CREATE TRIGGER IF NOT EXISTS pool_sources_no_delete
BEFORE DELETE ON pool_sources
BEGIN SELECT RAISE(ABORT, 'pool source history is immutable'); END;
"""


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


def _table_columns(conn: sqlite3.Connection, table: str) -> list[str]:
    return [row["name"] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()]


def _migrate(conn: sqlite3.Connection) -> None:
    """Upgrade database files written by older code in place.

    Pre-launch databases may already hold split records with the original
    idempotency_keys shape (split_id NOT NULL, no operation/pool_id). Those
    rows are copied verbatim into the generic shape so existing request keys
    keep replaying after the upgrade. A half-shaped pool_records from the
    never-released pool implementation is discarded: it never recorded
    consistent data to preserve.
    """
    idem_cols = _table_columns(conn, "idempotency_keys")
    if idem_cols and ("operation" not in idem_cols or "pool_id" not in idem_cols):
        # The rebuilt table references pool_records, which a split-only legacy
        # database does not have yet — disable FK enforcement for the rebuild
        # (allowed only outside a transaction); it is restored immediately after.
        conn.execute("PRAGMA foreign_keys = OFF")
        conn.execute("BEGIN")
        try:
            conn.execute(
                """
                CREATE TABLE idempotency_keys_v2 (
                    request_key   TEXT PRIMARY KEY,
                    request_hash  TEXT NOT NULL,
                    request_body  TEXT NOT NULL,
                    response_body TEXT NOT NULL,
                    operation     TEXT NOT NULL CHECK (operation IN ('split', 'pool')),
                    split_id      INTEGER REFERENCES splits (id),
                    pool_id       INTEGER REFERENCES pool_records (id),
                    created_at    TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                INSERT INTO idempotency_keys_v2
                    (request_key, request_hash, request_body, response_body,
                     operation, split_id, pool_id, created_at)
                SELECT request_key, request_hash, request_body, response_body,
                       'split', split_id, NULL, created_at
                FROM idempotency_keys
                """
            )
            conn.execute("DROP TABLE idempotency_keys")
            conn.execute("ALTER TABLE idempotency_keys_v2 RENAME TO idempotency_keys")
            conn.execute("COMMIT")
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.execute("PRAGMA foreign_keys = ON")

    pool_cols = _table_columns(conn, "pool_records")
    if pool_cols and ("id" not in pool_cols or "primary_source_id" in pool_cols):
        # Legacy table from the unfinished pool work; nothing references it.
        conn.execute("DROP TABLE IF EXISTS pool_records")


def init_db(db_path: str) -> None:
    if db_path != ":memory:":
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = connect(db_path)
    try:
        conn.execute("PRAGMA journal_mode = WAL")
        _migrate(conn)
        conn.executescript(SCHEMA)
    finally:
        conn.close()
