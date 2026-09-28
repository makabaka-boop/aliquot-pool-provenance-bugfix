"""POST /pools: atomic multi-source merges with complete lineage."""

from conftest import make_pool, make_split, total_balance


def _register_sources(client, specs: dict[str, int]) -> None:
    for tube_id, balance in specs.items():
        client.post("/tubes", json={"id": tube_id, "balance_ul": balance})


def test_basic_pool_deducts_every_source_and_creates_new_tube(client):
    _register_sources(client, {"s1": 300, "s2": 200, "s3": 100})
    resp = client.post(
        "/pools", json=make_pool("merged", "p1", [("s1", 0, 100), ("s2", 0, 150), ("s3", 0, 50)])
    )
    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert body["tube"] == {"id": "merged", "balance_ul": 300, "revision": 0}
    assert body["total_amount_ul"] == 300
    assert body["request_key"] == "p1"
    assert [(s["id"], s["balance_ul"], s["revision"], s["amount_ul"]) for s in body["sources"]] == [
        ("s1", 200, 1, 100),
        ("s2", 50, 1, 150),
        ("s3", 50, 1, 50),
    ]

    actual = {t["id"]: (t["balance_ul"], t["revision"]) for t in client.get("/tubes").json()["tubes"]}
    assert actual["s1"] == (200, 1)
    assert actual["s2"] == (50, 1)
    assert actual["s3"] == (50, 1)
    assert actual["merged"] == (300, 0)
    assert total_balance(client) == 600


def test_pool_accepts_two_and_five_sources(client):
    _register_sources(client, {f"s{i}": 100 for i in range(5)})
    r = client.post("/pools", json=make_pool("p2", "k2", [("s0", 0, 10), ("s1", 0, 10)]))
    assert r.status_code == 201, r.text
    r = client.post(
        "/pools",
        json=make_pool(
            "p5",
            "k5",
            [("s0", 1, 5), ("s1", 1, 5)] + [(f"s{i}", 0, 5) for i in range(2, 5)],
        ),
    )
    assert r.status_code == 201, r.text
    assert r.json()["tube"]["balance_ul"] == 25


def test_stale_revision_on_a_later_source_leaves_everything_untouched(client):
    _register_sources(client, {"s1": 300, "s2": 200})
    # s2 is actually at revision 1 after a prior split
    client.post("/splits", json=make_split("s2", 0, "pre", [("x", 50)]))

    resp = client.post(
        "/pools", json=make_pool("m", "p-bad", [("s1", 0, 100), ("s2", 0, 80)])
    )
    assert resp.status_code == 412
    assert resp.json()["error"]["code"] == "REVISION_CONFLICT"

    # the earlier source was never deducted, no revision bump, no new tube
    s1 = client.get("/tubes/s1").json()
    assert s1["balance_ul"] == 300 and s1["revision"] == 0
    s2 = client.get("/tubes/s2").json()
    assert s2["balance_ul"] == 150 and s2["revision"] == 1
    assert client.get("/tubes/m").status_code == 404
    assert total_balance(client) == 500
    # the failed request did not burn the key
    ok = client.post("/pools", json=make_pool("m", "p-bad", [("s1", 0, 100), ("s2", 1, 80)]))
    assert ok.status_code == 201, ok.text


def test_insufficient_balance_on_a_later_source_rolls_back_the_first(client):
    _register_sources(client, {"s1": 300, "s2": 5})
    resp = client.post(
        "/pools", json=make_pool("m", "p-bad", [("s1", 0, 100), ("s2", 0, 10)])
    )
    assert resp.status_code == 422
    assert resp.json()["error"]["code"] == "INSUFFICIENT_BALANCE"
    s1 = client.get("/tubes/s1").json()
    assert s1["balance_ul"] == 300 and s1["revision"] == 0
    assert client.get("/tubes/m").status_code == 404


def test_unknown_source_404_and_no_partial_deduction(client):
    _register_sources(client, {"s1": 300})
    resp = client.post(
        "/pools", json=make_pool("m", "p-bad", [("ghost", 0, 10), ("s1", 0, 10)])
    )
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "SOURCE_NOT_FOUND"
    resp = client.post(
        "/pools", json=make_pool("m", "p-bad2", [("s1", 0, 10), ("ghost2", 0, 10)])
    )
    assert resp.status_code == 404
    assert client.get("/tubes/s1").json()["revision"] == 0


def test_new_id_collision_409_and_no_deductions(client):
    _register_sources(client, {"s1": 300, "s2": 200, "taken": 1})
    resp = client.post(
        "/pools", json=make_pool("taken", "p-bad", [("s1", 0, 100), ("s2", 0, 50)])
    )
    assert resp.status_code == 409
    assert resp.json()["error"]["code"] == "CHILD_ID_EXISTS"
    assert client.get("/tubes/s1").json()["revision"] == 0
    assert client.get("/tubes/s2").json()["revision"] == 0


def test_total_above_int64_is_422_without_touching_state(client):
    INT64_MAX = 2**63 - 1
    _register_sources(client, {"s1": INT64_MAX, "s2": INT64_MAX})
    resp = client.post(
        "/pools",
        json=make_pool("m", "p-big", [("s1", 0, INT64_MAX), ("s2", 0, 1)]),
    )
    assert resp.status_code == 422
    assert resp.json()["error"]["code"] == "VALIDATION_ERROR"
    s1 = client.get("/tubes/s1").json()
    assert s1["balance_ul"] == INT64_MAX and s1["revision"] == 0
    s2 = client.get("/tubes/s2").json()
    assert s2["balance_ul"] == INT64_MAX and s2["revision"] == 0
    assert client.get("/tubes/m").status_code == 404
    # key not burned: a corrected request works
    ok = client.post("/pools", json=make_pool("m", "p-big", [("s1", 0, 1), ("s2", 0, 1)]))
    assert ok.status_code == 201


def test_total_exactly_int64_max_is_legal(client):
    INT64_MAX = 2**63 - 1
    _register_sources(client, {"s1": INT64_MAX, "s2": INT64_MAX})
    resp = client.post(
        "/pools",
        json=make_pool("m", "p-max", [("s1", 0, INT64_MAX - 1), ("s2", 0, 1)]),
    )
    assert resp.status_code == 201
    assert resp.json()["tube"]["balance_ul"] == INT64_MAX


def test_pool_validation_errors(client):
    _register_sources(client, {"s1": 100})
    base = make_pool("m", "k", [("s1", 0, 10)])  # only one source
    cases = [
        base,
        make_pool("m", "k", [(f"s{i}", 0, 1) for i in range(6)]),  # six sources
        make_pool("m", "k", [("s1", 0, 10), ("s1", 0, 5)]),        # duplicate sources
        make_pool("s1", "k", [("s1", 0, 5)]),                      # new id is a source
        make_pool("m", "k", [("s1", -1, 10)]),                     # negative revision
        make_pool("m", "k", [("s1", 0, 0)]),                       # zero amount
        make_pool("m", "k", [("s1", 0, -3)]),                      # negative amount
        make_pool("m", "k", [("s1", 0, 2.5)]),                     # fractional
        make_pool("m", "k", [("s1", 0, "10")]),                    # string amount
        dict(base, unexpected=1),                                  # unknown field
    ]
    for payload in cases:
        resp = client.post("/pools", json=payload)
        assert resp.status_code == 422, payload
        assert resp.json()["error"]["code"] == "VALIDATION_ERROR"
    assert client.get("/tubes/s1").json()["revision"] == 0


# ---------------------------------------------------------------- idempotency


def test_pool_retry_same_key_same_body_returns_original(client):
    _register_sources(client, {"s1": 300, "s2": 200})
    payload = make_pool("m", "dup", [("s1", 0, 100), ("s2", 0, 50)])
    first = client.post("/pools", json=payload)
    second = client.post("/pools", json=payload)
    assert first.status_code == 201 and second.status_code == 201
    assert first.json() == second.json()
    s1 = client.get("/tubes/s1").json()
    assert s1["balance_ul"] == 200 and s1["revision"] == 1
    m = client.get("/tubes/m").json()
    assert m["balance_ul"] == 150
    assert total_balance(client) == 500


def test_pool_same_key_different_body_409_and_no_effect(client):
    _register_sources(client, {"s1": 300, "s2": 200})
    first = client.post("/pools", json=make_pool("m", "dup", [("s1", 0, 100), ("s2", 0, 50)]))
    assert first.status_code == 201
    mutated = make_pool("m", "dup", [("s1", 0, 101), ("s2", 0, 50)])
    resp = client.post("/pools", json=mutated)
    assert resp.status_code == 409
    assert resp.json()["error"]["code"] == "REQUEST_KEY_CONFLICT"
    s1 = client.get("/tubes/s1").json()
    assert s1["balance_ul"] == 200 and s1["revision"] == 1
    # different new_id under the same key is also a conflict
    resp = client.post("/pools", json=make_pool("other", "dup", [("s1", 1, 1), ("s2", 1, 1)]))
    assert resp.status_code == 409
    assert client.get("/tubes/other").status_code == 404


def test_request_key_is_shared_between_splits_and_pools(client):
    _register_sources(client, {"s1": 300, "s2": 200})
    split = client.post("/splits", json=make_split("s1", 0, "shared", [("c", 10)]))
    assert split.status_code == 201
    pool = client.post("/pools", json=make_pool("m", "shared", [("s1", 1, 5), ("s2", 0, 5)]))
    assert pool.status_code == 409
    assert pool.json()["error"]["code"] == "REQUEST_KEY_CONFLICT"
    # the pool did not run
    assert client.get("/tubes/m").status_code == 404
    assert client.get("/tubes/s2").json()["balance_ul"] == 200

    # and the reverse: a pool key cannot drive a split
    pool2 = client.post("/pools", json=make_pool("m2", "also-shared", [("s1", 1, 5), ("s2", 0, 5)]))
    assert pool2.status_code == 201
    split2 = client.post("/splits", json=make_split("s2", 1, "also-shared", [("c2", 1)]))
    assert split2.status_code == 409
    assert client.get("/tubes/c2").status_code == 404


# ----------------------------------------------------------------- lineage


def test_pooled_tube_lineage_lists_every_ancestor_and_contribution(client):
    _register_sources(client, {"s1": 300, "s2": 200, "s3": 100})
    client.post(
        "/pools",
        json=make_pool("m", "p1", [("s1", 0, 100), ("s2", 0, 150), ("s3", 0, 50)]),
    )

    m = client.get("/tubes/m").json()
    assert m["parent_id"] is None
    assert {p["id"]: p["amount_ul"] for p in m["parents"]} == {"s1": 100, "s2": 150, "s3": 50}

    anc = client.get("/tubes/m/ancestry").json()
    assert anc["depth"] == 1
    assert "chain" not in anc  # pooled ancestry is expressed as a DAG
    graph = anc["graph"]
    assert {n["id"] for n in graph["nodes"]} == {"m", "s1", "s2", "s3"}
    edges = {(e["parent_id"], e["child_id"]): e for e in graph["edges"]}
    assert set(edges) == {("s1", "m"), ("s2", "m"), ("s3", "m")}
    assert edges[("s2", "m")]["amount_ul"] == 150
    assert edges[("s2", "m")]["kind"] == "pool"
    assert edges[("s2", "m")]["expected_revision"] == 0
    assert len(graph["pools"]) == 1
    pool = graph["pools"][0]
    assert pool["request_key"] == "p1" and pool["total_amount_ul"] == 300
    assert [(s["id"], s["amount_ul"], s["expected_revision"]) for s in pool["sources"]] == [
        ("s1", 100, 0),
        ("s2", 150, 0),
        ("s3", 50, 0),
    ]

    prov = client.get("/tubes/m/provenance").json()
    assert {n["id"] for n in prov["nodes"]} == {"m", "s1", "s2", "s3"}
    assert len(prov["edges"]) == 3

    # per-source contribution history
    s2_pools = client.get("/tubes/s2/pools").json()["pools"]
    assert len(s2_pools) == 1
    assert s2_pools[0]["child_id"] == "m"


def test_descendant_of_pooled_tube_traces_all_grandparents(client):
    _register_sources(client, {"s1": 300, "s2": 200})
    client.post("/pools", json=make_pool("m", "p1", [("s1", 0, 100), ("s2", 0, 80)]))
    # split the pooled tube, then split one child — a pooled ancestor sits in
    # the middle of the chain
    client.post("/splits", json=make_split("m", 0, "sp1", [("leaf", 60), ("leaf2", 40)]))
    client.post("/splits", json=make_split("leaf", 0, "sp2", [("tiny", 20)]))

    anc = client.get("/tubes/tiny/ancestry").json()
    graph = anc["graph"]
    ids = {n["id"] for n in graph["nodes"]}
    assert ids == {"tiny", "leaf", "m", "s1", "s2"}
    parents = {e["child_id"]: [] for e in graph["edges"]}
    for e in graph["edges"]:
        parents.setdefault(e["child_id"], []).append(e["parent_id"])
    assert parents["tiny"] == ["leaf"]
    assert parents["leaf"] == ["m"]
    assert sorted(parents["m"]) == ["s1", "s2"]
    # longest root path: s1/s2 -> m -> leaf -> tiny = depth 3
    assert anc["depth"] == 3
    kinds = {(e["parent_id"], e["child_id"]): e["kind"] for e in graph["edges"]}
    assert kinds[("s1", "m")] == "pool" and kinds[("leaf", "tiny")] == "split"
    # conservation still holds with pooled ancestry
    assert total_balance(client) == 500


def test_pool_sources_can_include_descendant_tubes(client):
    # two tubes each split, then descendants merged — the DAG must capture
    # both original roots
    _register_sources(client, {"r1": 100, "r2": 100})
    client.post("/splits", json=make_split("r1", 0, "a", [("a1", 40)]))
    client.post("/splits", json=make_split("r2", 0, "b", [("b1", 30)]))
    client.post("/pools", json=make_pool("m", "p1", [("a1", 0, 20), ("b1", 0, 15)]))
    graph = client.get("/tubes/m/ancestry").json()["graph"]
    ids = {n["id"] for n in graph["nodes"]}
    assert ids == {"m", "a1", "b1", "r1", "r2"}
    edges = {(e["parent_id"], e["child_id"]) for e in graph["edges"]}
    assert ("r1", "a1") in edges and ("a1", "m") in edges
    assert ("r2", "b1") in edges and ("b1", "m") in edges


# ------------------------------------------------------------- persistence


def test_restart_preserves_pool_state_lineage_and_receipt(db_path):
    from fastapi.testclient import TestClient

    from app.main import create_app

    payload = make_pool("m", "p1", [("s1", 0, 100), ("s2", 0, 80)])
    with TestClient(create_app(db_path)) as c1:
        c1.post("/tubes", json={"id": "s1", "balance_ul": 300})
        c1.post("/tubes", json={"id": "s2", "balance_ul": 200})
        first = c1.post("/pools", json=payload)
        assert first.status_code == 201

    with TestClient(create_app(db_path)) as c2:
        s1 = c2.get("/tubes/s1").json()
        assert s1["balance_ul"] == 200 and s1["revision"] == 1
        m = c2.get("/tubes/m").json()
        assert m["balance_ul"] == 180 and {p["id"] for p in m["parents"]} == {"s1", "s2"}

        replay = c2.post("/pools", json=payload)
        assert replay.status_code == 201
        assert replay.json() == first.json()
        assert c2.get("/tubes/s1").json()["balance_ul"] == 200  # not deducted twice

        graph = c2.get("/tubes/m/ancestry").json()["graph"]
        assert len(graph["edges"]) == 2
        assert total_balance(c2) == 500


def test_history_tables_reject_updates_and_deletes(client, db_path):
    import sqlite3

    import pytest

    _register_sources(client, {"s1": 300, "s2": 200})
    client.post("/pools", json=make_pool("m", "p1", [("s1", 0, 100), ("s2", 0, 80)]))
    conn = sqlite3.connect(db_path)
    for table in ("pool_records", "pool_sources"):
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(f"UPDATE {table} SET rowid = rowid")
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(f"DELETE FROM {table}")
    conn.close()
