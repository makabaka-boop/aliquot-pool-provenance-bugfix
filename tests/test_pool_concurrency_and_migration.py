"""Pool concurrency against a real uvicorn server and upgrade of database
files written by older versions of the service."""

from __future__ import annotations

import sqlite3
import threading

import httpx
from fastapi.testclient import TestClient

from app import db as dbmod
from app.main import create_app
from conftest import make_pool


def _fire(url, path, payload, barrier, outcomes):
    barrier.wait(timeout=10)
    resp = httpx.post(f"{url}{path}", json=payload, timeout=30)
    outcomes.append((resp.status_code, resp.json()))


def _race(url, path, payloads):
    barrier = threading.Barrier(len(payloads))
    outcomes = []
    threads = [
        threading.Thread(target=_fire, args=(url, path, p, barrier, outcomes)) for p in payloads
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)
    return outcomes


def test_concurrent_identical_pool_retry_is_applied_once(server):
    httpx.post(f"{server}/tubes", json={"id": "s1", "balance_ul": 500}).raise_for_status()
    httpx.post(f"{server}/tubes", json={"id": "s2", "balance_ul": 500}).raise_for_status()
    payload = make_pool("m", "dup-key", [("s1", 0, 100), ("s2", 0, 200)])
    outcomes = _race(server, "/pools", [payload, dict(payload)])

    statuses = sorted(s for s, _ in outcomes)
    assert statuses == [201, 201]
    bodies = [b for _, b in outcomes]
    assert bodies[0] == bodies[1]

    s1 = httpx.get(f"{server}/tubes/s1").json()
    assert s1["balance_ul"] == 400 and s1["revision"] == 1
    s2 = httpx.get(f"{server}/tubes/s2").json()
    assert s2["balance_ul"] == 300 and s2["revision"] == 1
    m = httpx.get(f"{server}/tubes/m").json()
    assert m["balance_ul"] == 300
    assert sum(t["balance_ul"] for t in httpx.get(f"{server}/tubes").json()["tubes"]) == 1000


def test_concurrent_pools_on_the_same_source_serialize_at_most_one_per_revision(server):
    httpx.post(f"{server}/tubes", json={"id": "s1", "balance_ul": 500}).raise_for_status()
    httpx.post(f"{server}/tubes", json={"id": "s2", "balance_ul": 500}).raise_for_status()
    httpx.post(f"{server}/tubes", json={"id": "s3", "balance_ul": 500}).raise_for_status()
    p1 = make_pool("m1", "k1", [("s1", 0, 100), ("s2", 0, 100)])
    p2 = make_pool("m2", "k2", [("s1", 0, 100), ("s3", 0, 100)])
    outcomes = _race(server, "/pools", [p1, p2])
    statuses = sorted(s for s, _ in outcomes)
    assert statuses == [201, 412]
    loser = next(b for s, b in outcomes if s == 412)
    assert loser["error"]["code"] == "REVISION_CONFLICT"
    s1 = httpx.get(f"{server}/tubes/s1").json()
    assert s1["revision"] == 1 and s1["balance_ul"] == 400
    assert sum(t["balance_ul"] for t in httpx.get(f"{server}/tubes").json()["tubes"]) == 1500


def test_legacy_split_only_database_upgrades_and_split_keys_still_replay(db_path):
    from app.main import _canonical

    # Build a database with the *old* schema: no pool tables, idempotency_keys
    # with the original (split_id NOT NULL) shape.
    split_body = {
        "parent_id": "root",
        "expected_revision": 0,
        "request_key": "legacy-key",
        "children": [{"id": "a", "amount_ul": 400}],
    }
    import hashlib
    import json

    digest = hashlib.sha256(_canonical(split_body).encode("utf-8")).hexdigest()
    original_response = {
        "split_id": 1,
        "request_key": "legacy-key",
        "parent": {"id": "root", "balance_ul": 500, "revision": 1},
        "children": [{"id": "a", "balance_ul": 400, "revision": 0}],
        "total_amount_ul": 400,
        "created_at": "2026-09-01T00:00:00.000+00:00",
    }
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE tubes (
            id TEXT PRIMARY KEY,
            balance_ul INTEGER NOT NULL CHECK (balance_ul >= 0),
            revision INTEGER NOT NULL DEFAULT 0 CHECK (revision >= 0),
            created_at TEXT NOT NULL
        );
        CREATE TABLE splits (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            parent_id TEXT NOT NULL REFERENCES tubes (id),
            request_key TEXT NOT NULL,
            expected_revision INTEGER NOT NULL,
            total_amount_ul INTEGER NOT NULL CHECK (total_amount_ul > 0),
            created_at TEXT NOT NULL
        );
        CREATE TABLE split_children (
            split_id INTEGER NOT NULL REFERENCES splits (id),
            child_id TEXT NOT NULL REFERENCES tubes (id),
            amount_ul INTEGER NOT NULL CHECK (amount_ul > 0),
            position INTEGER NOT NULL,
            PRIMARY KEY (split_id, child_id)
        );
        CREATE TABLE lineage_edges (
            child_id TEXT PRIMARY KEY REFERENCES tubes (id),
            parent_id TEXT NOT NULL REFERENCES tubes (id),
            split_id INTEGER NOT NULL REFERENCES splits (id),
            amount_ul INTEGER NOT NULL CHECK (amount_ul > 0)
        );
        CREATE TABLE idempotency_keys (
            request_key TEXT PRIMARY KEY,
            request_hash TEXT NOT NULL,
            request_body TEXT NOT NULL,
            response_body TEXT NOT NULL,
            split_id INTEGER NOT NULL REFERENCES splits (id),
            created_at TEXT NOT NULL
        );
        INSERT INTO tubes (id, balance_ul, revision, created_at)
        VALUES ('root', 500, 1, '2026-09-01T00:00:00.000+00:00');
        INSERT INTO tubes (id, balance_ul, revision, created_at)
        VALUES ('a', 400, 0, '2026-09-01T00:00:00.000+00:00');
        INSERT INTO splits (id, parent_id, request_key, expected_revision, total_amount_ul, created_at)
        VALUES (1, 'root', 'legacy-key', 0, 400, '2026-09-01T00:00:00.000+00:00');
        INSERT INTO split_children (split_id, child_id, amount_ul, position)
        VALUES (1, 'a', 400, 0);
        INSERT INTO lineage_edges (child_id, parent_id, split_id, amount_ul)
        VALUES ('a', 'root', 1, 400);
        """
    )
    conn.execute(
        """
        INSERT INTO idempotency_keys
            (request_key, request_hash, request_body, response_body, split_id, created_at)
        VALUES ('legacy-key', ?, ?, ?, 1, '2026-09-01T00:00:00.000+00:00')
        """,
        (digest, _canonical(split_body), json.dumps(original_response)),
    )
    conn.commit()
    conn.close()

    with TestClient(create_app(db_path)) as client:
        # balances and revision survived the upgrade untouched
        root = client.get("/tubes/root").json()
        assert root["balance_ul"] == 500 and root["revision"] == 1
        # split lineage still walks normally
        chain = client.get("/tubes/a/ancestry").json()["chain"]
        assert [h["tube"]["id"] for h in chain] == ["root", "a"]
        # old split request key still replays its original receipt
        replay = client.post(
            "/splits",
            json={
                "parent_id": "root",
                "expected_revision": 0,
                "request_key": "legacy-key",
                "children": [{"id": "a", "amount_ul": 400}],
            },
        )
        assert replay.status_code == 201
        assert replay.json() == original_response
        assert client.get("/tubes/root").json()["revision"] == 1  # no second deduction

        # and the upgraded database supports pools normally
        client.post("/tubes", json={"id": "b", "balance_ul": 100})
        pool = client.post(
            "/pools", json=make_pool("m", "pool-key", [("root", 1, 50), ("b", 0, 50)])
        )
        assert pool.status_code == 201
        assert pool.json()["tube"]["balance_ul"] == 100
        # old key and new pool keys coexist in one global namespace
        conflict = client.post(
            "/pools", json=make_pool("m2", "legacy-key", [("root", 2, 1), ("b", 1, 1)])
        )
        assert conflict.status_code == 409


def test_legacy_half_shaped_pool_table_is_discarded_on_upgrade(db_path):
    # A database produced by the never-released pool code: pool_records with
    # primary_source_id but no pool_sources. Upgrade drops it; data stays usable.
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE tubes (
            id TEXT PRIMARY KEY,
            balance_ul INTEGER NOT NULL CHECK (balance_ul >= 0),
            revision INTEGER NOT NULL DEFAULT 0 CHECK (revision >= 0),
            created_at TEXT NOT NULL
        );
        CREATE TABLE pool_records (
            child_id TEXT PRIMARY KEY,
            request_key TEXT NOT NULL,
            primary_source_id TEXT NOT NULL,
            total_amount_ul INTEGER NOT NULL,
            created_at TEXT NOT NULL
        );
        INSERT INTO tubes (id, balance_ul, revision, created_at)
        VALUES ('root', 100, 0, '2026-09-01T00:00:00.000+00:00');
        """
    )
    conn.commit()
    conn.close()

    with TestClient(create_app(db_path)) as client:
        assert client.get("/tubes/root").status_code == 200
        client.post("/tubes", json={"id": "other", "balance_ul": 50})
        resp = client.post(
            "/pools", json=make_pool("fresh", "pk", [("root", 0, 10), ("other", 0, 10)])
        )
        assert resp.status_code == 201
        assert {p["id"] for p in client.get("/tubes/fresh").json()["parents"]} == {"root", "other"}
