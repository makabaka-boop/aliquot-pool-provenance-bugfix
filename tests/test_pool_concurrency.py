"""Concurrency for multi-source pools, against a real uvicorn server with
genuinely independent SQLite connections (same harness as split races)."""

import threading
import time

import httpx

from conftest import make_pool, make_split


def _fire(url, path, payload, barrier, outcomes):
    barrier.wait(timeout=10)
    resp = httpx.post(f"{url}{path}", json=payload, timeout=30)
    outcomes.append((resp.status_code, resp.json()))


def _race(url, calls):
    barrier = threading.Barrier(len(calls))
    outcomes = []
    threads = [
        threading.Thread(target=_fire, args=(url, path, payload, barrier, outcomes))
        for path, payload in calls
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)
    return outcomes


def _reg(url, tid, balance):
    httpx.post(f"{url}/tubes", json={"id": tid, "balance_ul": balance}).raise_for_status()


def test_concurrent_pools_on_overlapping_sources_only_one_revision_wins(server):
    for tid in ("a", "b"):
        _reg(server, tid, 1000)
    calls = [
        ("/pools", make_pool("m1", "k1", [("a", 0, 400), ("b", 0, 400)])),
        ("/pools", make_pool("m2", "k2", [("a", 0, 400), ("b", 0, 400)])),
    ]
    outcomes = _race(server, calls)
    statuses = sorted(s for s, _ in outcomes)
    assert statuses == [201, 412]
    loser = next(b for s, b in outcomes if s == 412)
    assert loser["error"]["code"] == "REVISION_CONFLICT"

    a = httpx.get(f"{server}/tubes/a").json()
    b = httpx.get(f"{server}/tubes/b").json()
    assert a["revision"] == 1 and b["revision"] == 1
    assert a["balance_ul"] == 600 and b["balance_ul"] == 600
    tubes = httpx.get(f"{server}/tubes").json()["tubes"]
    assert sum(t["balance_ul"] for t in tubes) == 2000
    # exactly one of the two new tubes exists — which request wins the lock
    # race is nondeterministic
    existing = [m for m in ("m1", "m2") if httpx.get(f"{server}/tubes/{m}").status_code == 200]
    assert existing == ["m1"] or existing == ["m2"]


def test_concurrent_identical_pool_replay_is_applied_once(server):
    for tid in ("a", "b"):
        _reg(server, tid, 500)
    payload = make_pool("mix", "dup", [("a", 0, 100), ("b", 0, 100)])
    outcomes = _race(server, [("/pools", payload), ("/pools", dict(payload))])
    statuses = sorted(s for s, _ in outcomes)
    assert statuses == [201, 201]
    bodies = [b for _, b in outcomes]
    assert bodies[0] == bodies[1]

    a = httpx.get(f"{server}/tubes/a").json()
    assert a["revision"] == 1 and a["balance_ul"] == 400
    assert len(httpx.get(f"{server}/tubes/a/pools").json()["pools"]) == 1
    tubes = httpx.get(f"{server}/tubes").json()["tubes"]
    assert sum(t["balance_ul"] for t in tubes) == 1000


def test_split_and_pool_racing_same_tube_are_serialised(server):
    for tid in ("a", "b"):
        _reg(server, tid, 1000)
    calls = [
        ("/splits", make_split("a", 0, "sk", [("kid", 100)])),
        ("/pools", make_pool("mix", "pk", [("a", 0, 200), ("b", 0, 200)])),
    ]
    outcomes = _race(server, calls)
    statuses = sorted(s for s, _ in outcomes)
    assert statuses == [201, 412], outcomes
    winner = next(b for s, b in outcomes if s == 201)
    # whichever wins, exactly one revision step landed on a and volume is conserved
    a = httpx.get(f"{server}/tubes/a").json()
    assert a["revision"] == 1
    if winner.get("tube", {}).get("id") == "mix":
        assert a["balance_ul"] == 800
        assert httpx.get(f"{server}/tubes/kid").status_code == 404
    else:
        assert a["balance_ul"] == 900
        assert httpx.get(f"{server}/tubes/mix").status_code == 404
    tubes = httpx.get(f"{server}/tubes").json()["tubes"]
    assert sum(t["balance_ul"] for t in tubes) == 2000


def test_second_writer_blocks_until_pool_commits_then_sees_revisions(gated_server):
    # Park the pool after its first source UPDATE (a already deducted inside
    # the open write transaction). A second writer must block on the write
    # lock — it can never observe or interfere with the half-applied state —
    # and after release it sees the bumped revisions and fails 412.
    url, gate = gated_server
    for tid in ("a", "b", "c"):
        httpx.post(f"{url}/tubes", json={"id": tid, "balance_ul": 100}).raise_for_status()

    gate.arm_once(lambda conn, sql, params: sql.startswith("UPDATE tubes") and params[1] == "a")
    box = {}

    def pooled():
        box["first"] = httpx.post(
            f"{url}/pools",
            json=make_pool("mix", "p1", [("a", 0, 50), ("b", 0, 50), ("c", 0, 50)]),
            timeout=30,
        )

    t1 = threading.Thread(target=pooled)
    t1.start()
    assert gate.entered.wait(timeout=15)

    interleaved = {}

    def bump():
        interleaved["second"] = httpx.post(
            f"{url}/pools",
            json=make_pool("m0", "p0", [("c", 0, 10), ("b", 0, 10)]),
            timeout=30,
        )

    t2 = threading.Thread(target=bump)
    t2.start()
    time.sleep(0.5)
    assert "second" not in interleaved  # queued behind BEGIN IMMEDIATE
    gate.release.set()
    t1.join(timeout=15)
    t2.join(timeout=15)

    assert box["first"].status_code == 201
    assert interleaved["second"].status_code == 412
    assert interleaved["second"].json()["error"]["code"] == "REVISION_CONFLICT"
    # the first pool alone is visible, complete and conserved
    for tid, bal in (("a", 50), ("b", 50), ("c", 50)):
        got = httpx.get(f"{url}/tubes/{tid}").json()
        assert got["balance_ul"] == bal and got["revision"] == 1
    assert httpx.get(f"{url}/tubes/m0").status_code == 404
    tubes = httpx.get(f"{url}/tubes").json()["tubes"]
    assert sum(t["balance_ul"] for t in tubes) == 300
