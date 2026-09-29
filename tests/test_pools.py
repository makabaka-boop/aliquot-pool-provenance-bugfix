"""Multi-source pooling (/pools): atomic merge of 2-5 source tubes into one
new tube, with complete provenance and the same idempotency guarantees as
single-source splits."""

from conftest import make_pool, make_split, total_balance

INT64_MAX = 2**63 - 1


def _register(client, specs: dict[str, int]):
    for tid, balance in specs.items():
        assert client.post("/tubes", json={"id": tid, "balance_ul": balance}).status_code == 201


def test_basic_pool_three_sources(client):
    _register(client, {"a": 100, "b": 200, "c": 300})
    resp = client.post("/pools", json=make_pool("mix", "p1", [("a", 0, 40), ("b", 0, 80), ("c", 0, 120)]))
    assert resp.status_code == 201
    body = resp.json()
    assert body["pool_id"] >= 1
    assert body["request_key"] == "p1"
    assert body["tube"] == {"id": "mix", "balance_ul": 240, "revision": 0}
    assert body["total_amount_ul"] == 240
    assert body["sources"] == [
        {"id": "a", "amount_ul": 40, "balance_ul": 60, "revision": 1},
        {"id": "b", "amount_ul": 80, "balance_ul": 120, "revision": 1},
        {"id": "c", "amount_ul": 120, "balance_ul": 180, "revision": 1},
    ]

    # every source was deducted exactly once, revision bumped exactly once
    for tid, balance in (("a", 60), ("b", 120), ("c", 180)):
        tube = client.get(f"/tubes/{tid}").json()
        assert tube["balance_ul"] == balance and tube["revision"] == 1
    assert total_balance(client) == 600


def test_pool_supports_two_to_five_sources(client):
    _register(client, {f"s{i}": 100 for i in range(5)})
    assert client.post(
        "/pools", json=make_pool("two", "k2", [("s0", 0, 10), ("s1", 0, 10)])
    ).status_code == 201
    five = make_pool("five", "k5", [(f"s{i}", 0, 1) for i in range(2, 5)])
    five["sources"] += [{"id": "s0", "expected_revision": 1, "amount_ul": 1},
                        {"id": "s1", "expected_revision": 1, "amount_ul": 1}]
    assert client.post("/pools", json=five).status_code == 201

    for bad in ([], [("s0", 0, 1)], [(f"s{i}", 0, 1) for i in range(6)]):
        resp = client.post("/pools", json=make_pool("bad", "kb", bad))
        assert resp.status_code == 422
        assert resp.json()["error"]["code"] == "VALIDATION_ERROR"


def test_failure_on_later_source_leaves_no_partial_change(client):
    _register(client, {"a": 100, "b": 100, "c": 100})
    # c is consumed first by another operation -> revision 1, balance 90.
    assert client.post(
        "/pools", json=make_pool("primer", "prime", [("c", 0, 10), ("a", 0, 10)])
    ).status_code == 201
    # a and c are now at revision 1, b still at revision 0.
    # New request: earlier sources a (rev 1, ok) and b (rev 0, ok), later
    # source c presents stale revision 0 -> 412.
    resp = client.post(
        "/pools",
        json=make_pool("mix", "fail1", [("a", 1, 50), ("b", 0, 50), ("c", 0, 50)]),
    )
    assert resp.status_code == 412
    assert resp.json()["error"]["code"] == "REVISION_CONFLICT"

    # nothing moved: no deductions, no revisions, no new tube, no history
    a, b, c = (client.get(f"/tubes/{t}").json() for t in ("a", "b", "c"))
    assert (a["balance_ul"], a["revision"]) == (90, 1)
    assert (b["balance_ul"], b["revision"]) == (100, 0)
    assert (c["balance_ul"], c["revision"]) == (90, 1)
    assert client.get("/tubes/mix").status_code == 404
    assert client.get("/tubes/a/pools").json()["pools"]
    # exactly one pool (the primer) references each surviving source
    assert len(client.get("/tubes/b/pools").json()["pools"]) == 0
    assert total_balance(client) == 300

    # the failed key is not burned: corrected request with the same key works
    ok = client.post(
        "/pools",
        json=make_pool("mix", "fail1", [("a", 1, 50), ("b", 0, 50), ("c", 1, 50)]),
    )
    assert ok.status_code == 201
    assert client.get("/tubes/mix").json()["balance_ul"] == 150


def test_insufficient_balance_on_later_source_is_atomic(client):
    _register(client, {"a": 100, "b": 10})
    resp = client.post(
        "/pools", json=make_pool("mix", "k1", [("a", 0, 50), ("b", 0, 50)])
    )
    assert resp.status_code == 422
    assert resp.json()["error"]["code"] == "INSUFFICIENT_BALANCE"
    a = client.get("/tubes/a").json()
    assert a["balance_ul"] == 100 and a["revision"] == 0
    assert client.get("/tubes/mix").status_code == 404
    assert total_balance(client) == 110


def test_missing_source_404_and_no_change(client):
    _register(client, {"a": 100})
    resp = client.post("/pools", json=make_pool("mix", "k1", [("a", 0, 10), ("ghost", 0, 10)]))
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "TUBE_NOT_FOUND"
    assert client.get("/tubes/a").json()["revision"] == 0
    assert client.get("/tubes/mix").status_code == 404


def test_new_id_collision_409(client):
    _register(client, {"a": 100, "b": 100, "taken": 1})
    resp = client.post("/pools", json=make_pool("taken", "k1", [("a", 0, 10), ("b", 0, 10)]))
    assert resp.status_code == 409
    assert resp.json()["error"]["code"] == "CHILD_ID_EXISTS"
    assert client.get("/tubes/a").json()["revision"] == 0


def test_pool_validation(client):
    _register(client, {"a": 100, "b": 100})
    base = make_pool("mix", "k1", [("a", 0, 10), ("b", 0, 10)])

    bad = [
        dict(base, sources=[{"id": "a", "expected_revision": 0, "amount_ul": 10},
                            {"id": "a", "expected_revision": 0, "amount_ul": 10}]),  # dup source
        dict(base, new_id="a"),                                                         # new == source
        make_pool("m", "k", [("a", 0, 0), ("b", 0, 10)]),                              # zero amount
        make_pool("m", "k", [("a", -1, 10), ("b", 0, 10)]),                            # bad revision
        make_pool("m", "k", [("a", 0, 2.5), ("b", 0, 10)]),                            # fractional
        dict(base, unexpected=1),                                                      # unknown field
        dict(base, request_key=""),                                                    # empty key
    ]
    for payload in bad:
        resp = client.post("/pools", json=payload)
        assert resp.status_code == 422, payload
        assert resp.json()["error"]["code"] == "VALIDATION_ERROR", payload
    assert client.get("/tubes/a").json()["revision"] == 0


def test_pool_total_above_int64_is_stable_422_without_partial_change(client):
    # Each amount is legal; together they overflow the storable integer.
    sources = [("a", 0, INT64_MAX), ("b", 0, 2)]
    _register(client, {"a": INT64_MAX, "b": 100})
    resp = client.post("/pools", json=make_pool("big", "k1", sources))
    assert resp.status_code == 422
    assert resp.json()["error"]["code"] == "VALIDATION_ERROR"
    # rejected at the schema boundary: no deductions, no revisions, no tube
    assert client.get("/tubes/a").json()["balance_ul"] == INT64_MAX
    assert client.get("/tubes/a").json()["revision"] == 0
    assert client.get("/tubes/b").json()["balance_ul"] == 100
    assert client.get("/tubes/big").status_code == 404
    # and the key was not burned
    fixed = client.post("/pools", json=make_pool("big", "k1", [("a", 0, 1), ("b", 0, 2)]))
    assert fixed.status_code == 201
    assert fixed.json()["tube"]["balance_ul"] == 3


def test_pool_idempotent_replay_returns_original_receipt(client):
    _register(client, {"a": 100, "b": 200})
    payload = make_pool("mix", "rk", [("a", 0, 30), ("b", 0, 70)])
    first = client.post("/pools", json=payload)
    second = client.post("/pools", json=payload)
    assert first.status_code == 201 and second.status_code == 201
    assert first.json() == second.json()
    assert client.get("/tubes/a").json()["revision"] == 1
    assert client.get("/tubes/b").json()["revision"] == 1
    assert client.get("/tubes/mix").json()["balance_ul"] == 100
    assert len(client.get("/tubes/a/pools").json()["pools"]) == 1
    assert total_balance(client) == 300


def test_pool_same_key_different_body_is_conflict(client):
    _register(client, {"a": 100, "b": 100})
    assert client.post(
        "/pools", json=make_pool("mix", "dup", [("a", 0, 10), ("b", 0, 10)])
    ).status_code == 201
    mutated = make_pool("mix2", "dup", [("a", 0, 11), ("b", 0, 10)])
    resp = client.post("/pools", json=mutated)
    assert resp.status_code == 409
    assert resp.json()["error"]["code"] == "REQUEST_KEY_CONFLICT"
    # a key reused between the two operation kinds is a conflict as well
    split_reuse = make_split("a", 1, "dup", [("child-x", 5)])
    resp = client.post("/splits", json=split_reuse)
    assert resp.status_code == 409
    assert resp.json()["error"]["code"] == "REQUEST_KEY_CONFLICT"
    # nothing extra applied
    assert client.get("/tubes/a").json()["balance_ul"] == 90
    assert client.get("/tubes/mix2").status_code == 404
    assert client.get("/tubes/child-x").status_code == 404


def test_split_key_reused_by_pool_is_conflict(client):
    _register(client, {"a": 100, "b": 100})
    assert client.post(
        "/splits", json=make_split("a", 0, "shared", [("kid", 10)])
    ).status_code == 201
    resp = client.post("/pools", json=make_pool("mix", "shared", [("a", 1, 5), ("b", 0, 5)]))
    assert resp.status_code == 409
    assert resp.json()["error"]["code"] == "REQUEST_KEY_CONFLICT"
    assert client.get("/tubes/mix").status_code == 404
    assert client.get("/tubes/b").json()["revision"] == 0


def test_merged_tube_lineage_contains_every_ancestor_and_contribution(client):
    _register(client, {"a": 100, "b": 100, "c": 100})
    pool = client.post(
        "/pools", json=make_pool("mix", "p1", [("a", 0, 40), ("b", 0, 30), ("c", 0, 20)])
    )
    assert pool.status_code == 201

    # tube view exposes every parent
    mix = client.get("/tubes/mix").json()
    assert mix["parent_ids"] == ["a", "b", "c"]
    assert mix["parent_id"] is None  # ambiguous: more than one parent

    anc = client.get("/tubes/mix/ancestry").json()
    assert anc["lineage"] == "merged"
    assert anc["chain"] is None
    assert anc["depth"] == 1
    graph = anc["graph"]
    assert sorted(n["id"] for n in graph["nodes"]) == ["a", "b", "c", "mix"]
    assert graph["roots"] == ["a", "b", "c"]
    edges = {(e["parent_id"], e["child_id"]): e for e in graph["edges"]}
    assert set(edges) == {("a", "mix"), ("b", "mix"), ("c", "mix")}
    assert edges[("a", "mix")]["amount_ul"] == 40
    assert edges[("b", "mix")]["amount_ul"] == 30
    assert edges[("c", "mix")]["amount_ul"] == 20
    assert all(e["operation"] == "pool" and e["pool_id"] == pool.json()["pool_id"]
               for e in edges.values())

    # one root-first path per real ancestor, each carrying the pool evidence
    path_parents = [[hop["tube"]["id"] for hop in chain] for chain in anc["chains"]]
    assert sorted(path_parents) == [["a", "mix"], ["b", "mix"], ["c", "mix"]]
    for chain in anc["chains"]:
        assert chain[0]["via"] is None
        via = chain[1]["via"]
        assert via["operation"] == "pool"
        # the full contribution list rides along on every pool hop
        assert [(s["id"], s["amount_ul"]) for s in via["pool"]["sources"]] == [
            ("a", 40), ("b", 30), ("c", 20)
        ]
        assert via["pool"]["request_key"] == "p1"


def test_ancestry_dag_after_pooling_descendants_of_earlier_pools(client):
    # a <- x (split); b, c; pool(x,b) -> m1; pool(m1,c) -> m2; split m2 -> leaf
    _register(client, {"a": 1000, "b": 100, "c": 100})
    assert client.post("/splits", json=make_split("a", 0, "s1", [("x", 400)])).status_code == 201
    assert client.post(
        "/pools", json=make_pool("m1", "p1", [("x", 0, 100), ("b", 0, 50)])
    ).status_code == 201
    assert client.post(
        "/pools", json=make_pool("m2", "p2", [("m1", 0, 120), ("c", 0, 30)])
    ).status_code == 201
    assert client.post("/splits", json=make_split("m2", 0, "s2", [("leaf", 60)])).status_code == 201

    anc = client.get("/tubes/leaf/ancestry").json()
    assert anc["lineage"] == "merged"
    # roots are the registered tubes that actually contributed
    assert anc["graph"]["roots"] == ["a", "b", "c"]
    paths = {tuple(hop["tube"]["id"] for hop in chain) for chain in anc["chains"]}
    assert paths == {
        ("a", "x", "m1", "m2", "leaf"),
        ("b", "m1", "m2", "leaf"),
        ("c", "m2", "leaf"),
    }
    assert anc["depth"] == 4
    # edge operations are recorded per hop
    op_by_child_parent = {
        (hop["tube"]["id"], hop["via"]["parent_id"]): hop["via"]["operation"]
        for chain in anc["chains"] for hop in chain[1:]
    }
    assert op_by_child_parent[("x", "a")] == "split"
    assert op_by_child_parent[("m1", "x")] == "pool"
    assert op_by_child_parent[("m2", "m1")] == "pool"
    assert op_by_child_parent[("leaf", "m2")] == "split"
    # the deepest chain is snapshot-consistent and evidence-pinned
    deepest = max(anc["chains"], key=len)
    assert deepest[1]["via"]["split"]["expected_revision"] == 0
    assert total_balance(client) == 1200


def test_pool_history_endpoints_and_record(client):
    _register(client, {"a": 100, "b": 100})
    body = client.post(
        "/pools", json=make_pool("mix", "p1", [("a", 0, 40), ("b", 0, 30)])
    ).json()
    pool_id = body["pool_id"]
    record = client.get(f"/pools/{pool_id}").json()
    assert record["new_tube_id"] == "mix"
    assert record["total_amount_ul"] == 70
    assert [(s["id"], s["expected_revision"], s["amount_ul"], s["position"]) for s in record["sources"]] == [
        ("a", 0, 40, 0), ("b", 0, 30, 1)
    ]
    # both sources list the pool in their contribution history
    for tid in ("a", "b"):
        pools = client.get(f"/tubes/{tid}/pools").json()["pools"]
        assert [p["pool_id"] for p in pools] == [pool_id]
    assert client.get("/pools/9999").status_code == 404


def test_pool_then_split_keeps_conservation_and_full_recompute(client):
    _register(client, {"a": 300, "b": 200})
    assert client.post(
        "/pools", json=make_pool("mix", "p1", [("a", 0, 100), ("b", 0, 50)])
    ).status_code == 201
    assert client.post("/splits", json=make_split("mix", 0, "s1", [("q1", 90), ("q2", 30)])).status_code == 201
    balances = {t["id"]: t["balance_ul"] for t in client.get("/tubes").json()["tubes"]}
    assert balances == {"a": 200, "b": 150, "mix": 30, "q1": 90, "q2": 30}
    assert total_balance(client) == 500

    # source audit: current balance + everything it dispensed == registered
    pools_a = client.get("/tubes/a/pools").json()["pools"]
    assert sum(s["amount_ul"] for p in pools_a for s in p["sources"] if s["id"] == "a") == 100
