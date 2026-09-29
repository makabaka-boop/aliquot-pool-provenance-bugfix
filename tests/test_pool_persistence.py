"""Pools across restarts: balances, revisions, history, lineage and request
receipts must cross-check after reopening the same SQLite file. Also covers
migration of databases created before pools existed."""

import sqlite3

import pytest
from fastapi.testclient import TestClient

from app.main import create_app
from conftest import make_pool, make_split, total_balance


def test_pool_state_lineage_and_receipt_survive_restart(db_path):
    pool_body = make_pool("mix", "p1", [("a", 0, 40), ("b", 0, 30), ("c", 0, 20)])
    with TestClient(create_app(db_path)) as c1:
        for tid, bal in (("a", 100), ("b", 100), ("c", 100)):
            c1.post("/tubes", json={"id": tid, "balance_ul": bal})
        first = c1.post("/pools", json=pool_body)
        assert first.status_code == 201
        # pooled tube is itself split afterwards
        assert c1.post(
            "/splits", json=make_split("mix", 0, "s1", [("leaf", 25)])
        ).status_code == 201

    with TestClient(create_app(db_path)) as c2:
        # current state
        for tid, bal, rev in (("a", 60, 1), ("b", 70, 1), ("c", 80, 1)):
            got = c2.get(f"/tubes/{tid}").json()
            assert got["balance_ul"] == bal and got["revision"] == rev
        assert c2.get("/tubes/mix").json()["balance_ul"] == 65
        assert c2.get("/tubes/leaf").json()["balance_ul"] == 25
        assert total_balance(c2) == 300

        # idempotent replay after restart returns the exact first receipt
        replay = c2.post("/pools", json=pool_body)
        assert replay.status_code == 201
        assert replay.json() == first.json()
        # and does not deduct twice
        assert c2.get("/tubes/a").json()["balance_ul"] == 60
        assert c2.get("/tubes/a").json()["revision"] == 1

        # the stale source revision is rejected; the bumped one works
        bad = c2.post("/pools", json=make_pool("m2", "p2", [("a", 0, 5), ("b", 1, 5)]))
        assert bad.status_code == 412
        good = c2.post("/pools", json=make_pool("m2", "p2", [("a", 1, 5), ("b", 1, 5)]))
        assert good.status_code == 201
        assert total_balance(c2) == 300

        # full merged lineage survives the restart, including every ancestor
        anc = c2.get("/tubes/leaf/ancestry").json()
        assert anc["lineage"] == "merged"
        assert anc["graph"]["roots"] == ["a", "b", "c"]
        paths = {tuple(h["tube"]["id"] for h in chain) for chain in anc["chains"]}
        assert paths == {
            ("a", "mix", "leaf"), ("b", "mix", "leaf"), ("c", "mix", "leaf")
        }
        # contributions preserved
        edges = {(e["parent_id"], e["child_id"]): e["amount_ul"] for e in anc["graph"]["edges"]}
        assert edges[("a", "mix")] == 40
        assert edges[("b", "mix")] == 30
        assert edges[("c", "mix")] == 20
        assert edges[("mix", "leaf")] == 25

        # history endpoints cross-check the receipt
        record = c2.get(f"/pools/{first.json()['pool_id']}").json()
        assert record["request_key"] == "p1"
        assert sum(s["amount_ul"] for s in record["sources"]) == record["total_amount_ul"] == 90


def test_pool_history_tables_are_immutable(db_path):
    with TestClient(create_app(db_path)) as client:
        for tid in ("a", "b"):
            client.post("/tubes", json={"id": tid, "balance_ul": 100})
        client.post("/pools", json=make_pool("mix", "k1", [("a", 0, 10), ("b", 0, 20)]))

    conn = sqlite3.connect(db_path)
    for table in ("pools", "pool_sources", "lineage_edges"):
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(f"UPDATE {table} SET rowid = rowid")
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(f"DELETE FROM {table}")
    conn.close()


def test_preexisting_split_only_database_migrates_and_keeps_working(tmp_path):
    # Build a database with the ORIGINAL deployed schema (split-only, old
    # idempotency_keys, old lineage_edges) — what is on disk before this
    # release is deployed.
    db_path = str(tmp_path / "old.db")
    conn = sqlite3.connect(db_path, isolation_level=None)
    conn.execute("PRAGMA journal_mode = WAL")
    conn.executescript(
        """
        CREATE TABLE tubes (
            id TEXT PRIMARY KEY, balance_ul INTEGER NOT NULL CHECK (balance_ul >= 0),
            revision INTEGER NOT NULL DEFAULT 0 CHECK (revision >= 0), created_at TEXT NOT NULL);
        CREATE TABLE splits (
            id INTEGER PRIMARY KEY AUTOINCREMENT, parent_id TEXT NOT NULL REFERENCES tubes (id),
            request_key TEXT NOT NULL, expected_revision INTEGER NOT NULL,
            total_amount_ul INTEGER NOT NULL CHECK (total_amount_ul > 0), created_at TEXT NOT NULL);
        CREATE TABLE split_children (
            split_id INTEGER NOT NULL REFERENCES splits (id), child_id TEXT NOT NULL REFERENCES tubes (id),
            amount_ul INTEGER NOT NULL CHECK (amount_ul > 0), position INTEGER NOT NULL,
            PRIMARY KEY (split_id, child_id));
        CREATE TABLE lineage_edges (
            child_id TEXT PRIMARY KEY REFERENCES tubes (id), parent_id TEXT NOT NULL REFERENCES tubes (id),
            split_id INTEGER NOT NULL REFERENCES splits (id), amount_ul INTEGER NOT NULL CHECK (amount_ul > 0));
        CREATE TABLE idempotency_keys (
            request_key TEXT PRIMARY KEY, request_hash TEXT NOT NULL, request_body TEXT NOT NULL,
            response_body TEXT NOT NULL, split_id INTEGER NOT NULL REFERENCES splits (id),
            created_at TEXT NOT NULL);
        CREATE TRIGGER splits_no_update BEFORE UPDATE ON splits
        BEGIN SELECT RAISE(ABORT, 'splits history is immutable'); END;
        CREATE TRIGGER splits_no_delete BEFORE DELETE ON splits
        BEGIN SELECT RAISE(ABORT, 'splits history is immutable'); END;
        CREATE TRIGGER split_children_no_update BEFORE UPDATE ON split_children
        BEGIN SELECT RAISE(ABORT, 'split children history is immutable'); END;
        CREATE TRIGGER split_children_no_delete BEFORE DELETE ON split_children
        BEGIN SELECT RAISE(ABORT, 'split children history is immutable'); END;
        CREATE TRIGGER lineage_edges_no_update BEFORE UPDATE ON lineage_edges
        BEGIN SELECT RAISE(ABORT, 'lineage history is immutable'); END;
        CREATE TRIGGER lineage_edges_no_delete BEFORE DELETE ON lineage_edges
        BEGIN SELECT RAISE(ABORT, 'lineage history is immutable'); END;
        INSERT INTO tubes VALUES ('root', 900, 1, '2026-01-01T00:00:00.000+00:00');
        INSERT INTO tubes VALUES ('a', 300, 0, '2026-01-01T00:00:00.000+00:00');
        INSERT INTO tubes VALUES ('b', 100, 0, '2026-01-01T00:00:00.000+00:00');
        INSERT INTO splits (id, parent_id, request_key, expected_revision, total_amount_ul, created_at)
            VALUES (1, 'root', 'k1', 0, 400, '2026-01-01T00:00:00.000+00:00');
        INSERT INTO split_children VALUES (1, 'a', 300, 0), (1, 'b', 100, 1);
        INSERT INTO lineage_edges VALUES ('a', 'root', 1, 300), ('b', 'root', 1, 100);
        INSERT INTO idempotency_keys VALUES ('k1', 'h', '{}', '{"split_id": 1}', 1,
            '2026-01-01T00:00:00.000+00:00');
        """
    )
    conn.close()

    with TestClient(create_app(db_path)) as client:
        # old reads keep working
        assert client.get("/tubes/root").json()["balance_ul"] == 900
        chain = client.get("/tubes/a/ancestry").json()
        assert [h["tube"]["id"] for h in chain["chain"]] == ["root", "a"]
        assert chain["lineage"] == "single"
        assert chain["chain"][1]["via"]["operation"] == "split"
        assert client.get("/tubes/a").json()["parent_id"] == "root"
        # old splits history intact and immutable-table evidence intact
        assert client.get("/tubes/root/splits").json()["splits"][0]["total_amount_ul"] == 400
        # new operations work on the migrated file
        assert client.post(
            "/splits", json=make_split("root", 1, "k2", [("c", 50)])
        ).status_code == 201
        assert client.post(
            "/pools", json=make_pool("mix", "p1", [("a", 0, 100), ("b", 0, 50)])
        ).status_code == 201
        mix_anc = client.get("/tubes/mix/ancestry").json()
        assert mix_anc["lineage"] == "merged"
        assert mix_anc["graph"]["roots"] == ["root"]  # a and b both descend from root
        assert total_balance(client) == 1300  # 900 + 300 + 100 registered, conserved
        # the old request key was migrated as a split key and now conflicts
        # with any different body reuse
        assert client.post(
            "/splits", json=make_split("root", 2, "k1", [("x", 1)])
        ).status_code == 409

    # history-table triggers must have survived the table rebuild: the
    # renamed-away legacy triggers would otherwise have been dropped silently.
    conn = sqlite3.connect(db_path)
    for table in ("splits", "split_children", "lineage_edges", "pools", "pool_sources"):
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(f"UPDATE {table} SET rowid = rowid")
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(f"DELETE FROM {table}")
    conn.close()


def test_legacy_flat_pool_records_table_migrates_lossily_but_readably(tmp_path):
    # The development build could leave a flat pool_records table.
    db_path = str(tmp_path / "dev.db")
    conn = sqlite3.connect(db_path, isolation_level=None)
    conn.execute("PRAGMA journal_mode = WAL")
    conn.executescript(
        """
        CREATE TABLE tubes (id TEXT PRIMARY KEY, balance_ul INTEGER NOT NULL,
            revision INTEGER NOT NULL DEFAULT 0, created_at TEXT NOT NULL);
        CREATE TABLE pool_records (
            child_id TEXT PRIMARY KEY, request_key TEXT NOT NULL,
            primary_source_id TEXT NOT NULL, total_amount_ul INTEGER NOT NULL, created_at TEXT NOT NULL);
        INSERT INTO tubes VALUES ('a', 90, 1, 't'), ('mix', 10, 0, 't');
        INSERT INTO pool_records VALUES ('mix', 'pk', 'a', 10, 't');
        """
    )
    conn.close()

    with TestClient(create_app(db_path)) as client:
        record = client.get("/tubes/a/pools").json()
        assert len(record["pools"]) == 1
        assert record["pools"][0]["new_tube_id"] == "mix"
        assert record["pools"][0]["sources"][0] == {
            "id": "a", "amount_ul": 10, "expected_revision": 0, "position": 0
        }
        anc = client.get("/tubes/mix/ancestry").json()
        assert [h["tube"]["id"] for h in anc["chains"][0]] == ["a", "mix"]
        assert anc["graph"]["edges"][0]["operation"] == "pool"
        # and the migrated pool is immutable history
        assert client.post(
            "/pools", json=make_pool("m2", "p2", [("a", 1, 1), ("mix", 0, 1)])
        ).status_code == 201
