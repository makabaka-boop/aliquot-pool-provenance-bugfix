from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from datetime import datetime, timezone

from fastapi import Depends, FastAPI, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from . import db as dbmod
from .errors import ApiError
from .schemas import PoolRequest, RegisterTubeRequest, SplitRequest, SQLITE_INT64_MAX


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def _canonical(payload: dict) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _tube_view(row: sqlite3.Row) -> dict:
    return {
        "id": row["id"],
        "balance_ul": row["balance_ul"],
        "revision": row["revision"],
        "created_at": row["created_at"],
    }


def _split_record(conn: sqlite3.Connection, split_row: sqlite3.Row) -> dict:
    children = conn.execute(
        "SELECT child_id AS id, amount_ul, position FROM split_children "
        "WHERE split_id = ? ORDER BY position",
        (split_row["id"],),
    ).fetchall()
    return {
        "split_id": split_row["id"],
        "parent_id": split_row["parent_id"],
        "request_key": split_row["request_key"],
        "expected_revision": split_row["expected_revision"],
        "total_amount_ul": split_row["total_amount_ul"],
        "created_at": split_row["created_at"],
        "children": [dict(c) for c in children],
    }


def _pool_record(conn: sqlite3.Connection, pool_id: int) -> dict:
    pool = conn.execute(
        "SELECT * FROM pool_records WHERE id = ?", (pool_id,)
    ).fetchone()
    sources = conn.execute(
        "SELECT source_id AS id, amount_ul, expected_revision, position "
        "FROM pool_sources WHERE pool_id = ? ORDER BY position",
        (pool_id,),
    ).fetchall()
    return {
        "pool_id": pool["id"],
        "child_id": pool["child_id"],
        "request_key": pool["request_key"],
        "total_amount_ul": pool["total_amount_ul"],
        "created_at": pool["created_at"],
        "sources": [dict(s) for s in sources],
    }


def get_db(request: Request):
    conn = dbmod.connect(request.app.state.db_path)
    try:
        yield conn
    finally:
        conn.close()


def _replay_or_conflict(conn: sqlite3.Connection, request_key: str, digest: str) -> JSONResponse | None:
    """Look up a stored idempotency receipt. The key namespace is global:
    splits and pools share idempotency_keys, so a key reused with a different
    body — even across the two endpoints — is a 409, never a second effect."""
    row = conn.execute(
        "SELECT request_hash, response_body FROM idempotency_keys WHERE request_key = ?",
        (request_key,),
    ).fetchone()
    if row is None:
        return None
    if row["request_hash"] != digest:
        raise ApiError(
            409,
            "REQUEST_KEY_CONFLICT",
            f"request_key {request_key!r} was already used with a different body",
        )
    return JSONResponse(status_code=201, content=json.loads(row["response_body"]))


def _collect_lineage(conn: sqlite3.Connection, tube_id: str):
    """Walk the full ancestor DAG of tube_id in the caller's snapshot.

    A tube is created either by a split (exactly one parent, lineage_edges)
    or by a pool (2-5 parents, pool_records + pool_sources). Every edge is
    returned — there is no such thing as a 'primary' source: all ancestors
    and their contributed volumes must remain traceable.

    Returns (tube_views, edges, pool_ids):
      tube_views: {id: view} for every tube on the DAG (subject included)
      edges:     bottom-up traversal order, each a dict with kind
                 "split" (split_id) or "pool" (pool_id, expected_revision)
      pool_ids:  pool_records encountered, traversal order
    Raises TUBE_NOT_FOUND for any missing tube in the walk.
    """
    tube_views: dict[str, dict] = {}
    edges: list[dict] = []
    pool_ids: list[int] = []

    stack = [tube_id]
    seen: set[str] = set()
    while stack:
        current_id = stack.pop()
        if current_id in seen:
            continue
        seen.add(current_id)

        row = conn.execute("SELECT * FROM tubes WHERE id = ?", (current_id,)).fetchone()
        if row is None:
            raise ApiError(404, "TUBE_NOT_FOUND", f"tube {current_id!r} does not exist")
        tube_views[current_id] = _tube_view(row)

        split_edges = conn.execute(
            "SELECT parent_id, split_id, amount_ul FROM lineage_edges WHERE child_id = ?",
            (current_id,),
        ).fetchall()
        if split_edges:
            for edge in split_edges:
                edges.append(
                    {
                        "child_id": current_id,
                        "parent_id": edge["parent_id"],
                        "amount_ul": edge["amount_ul"],
                        "kind": "split",
                        "split_id": edge["split_id"],
                    }
                )
                stack.append(edge["parent_id"])
            continue

        pool = conn.execute(
            "SELECT id FROM pool_records WHERE child_id = ?", (current_id,)
        ).fetchone()
        if pool is not None:
            pool_ids.append(pool["id"])
            sources = conn.execute(
                "SELECT source_id, amount_ul, expected_revision "
                "FROM pool_sources WHERE pool_id = ? ORDER BY position",
                (pool["id"],),
            ).fetchall()
            for source in sources:
                edges.append(
                    {
                        "child_id": current_id,
                        "parent_id": source["source_id"],
                        "amount_ul": source["amount_ul"],
                        "kind": "pool",
                        "pool_id": pool["id"],
                        "expected_revision": source["expected_revision"],
                    }
                )
                stack.append(source["source_id"])

    return tube_views, edges, pool_ids


def _lineage_graph(conn: sqlite3.Connection, tube_id: str) -> dict:
    """Build the complete, self-describing ancestor DAG of tube_id from the
    data collected by _collect_lineage. Runs in the caller's snapshot."""
    tube_views, edges, pool_ids = _collect_lineage(conn, tube_id)

    parents_of: dict[str, set[str]] = {tid: set() for tid in tube_views}
    for edge in edges:
        parents_of[edge["child_id"]].add(edge["parent_id"])

    # Parents-first node order via iterative post-order DFS; deterministic
    # tie-break by id. Every node's parents are part of the DAG by construction.
    ordered: list[str] = []
    visited: set[str] = set()
    work: list[tuple[str, int]] = [(tube_id, 0)]
    while work:
        current, state = work.pop()
        if state == 1:
            if current not in ordered:
                ordered.append(current)
            continue
        if current in visited:
            continue
        visited.add(current)
        work.append((current, 1))
        for parent in sorted(parents_of[current], reverse=True):
            work.append((parent, 0))

    # Edges keyed by child, then emitted node by node in parents-first order.
    by_child: dict[str, list[dict]] = {tid: [] for tid in tube_views}
    for edge in edges:
        by_child[edge["child_id"]].append(edge)

    out_edges: list[dict] = []
    seen_pools: set[int] = set()
    pools: list[dict] = []
    for current in ordered:
        for edge in sorted(by_child[current], key=lambda e: (e["kind"], e["parent_id"])):
            out = {
                "child_id": edge["child_id"],
                "parent_id": edge["parent_id"],
                "amount_ul": edge["amount_ul"],
                "kind": edge["kind"],
            }
            if edge["kind"] == "split":
                split_row = conn.execute(
                    "SELECT * FROM splits WHERE id = ?", (edge["split_id"],)
                ).fetchone()
                out["split_id"] = edge["split_id"]
                out["split"] = _split_record(conn, split_row)
            else:
                out["pool_id"] = edge["pool_id"]
                out["expected_revision"] = edge["expected_revision"]
                if edge["pool_id"] not in seen_pools:
                    seen_pools.add(edge["pool_id"])
                    pools.append(_pool_record(conn, edge["pool_id"]))
            out_edges.append(out)

    # depth: longest root-to-node path in edges (0 for a registered root).
    # `ordered` is parents-first, so every parent's depth is known already.
    depth: dict[str, int] = {}
    for current in ordered:
        depth[current] = (
            0 if not parents_of[current] else 1 + max(depth[p] for p in parents_of[current])
        )

    return {
        "tube_id": tube_id,
        "depth": depth[tube_id],
        "nodes": [tube_views[tid] for tid in ordered],
        "edges": out_edges,
        "pools": pools,
        "_parents_of": parents_of,
        "_by_child": by_child,
    }


def create_app(db_path: str) -> FastAPI:
    dbmod.init_db(db_path)
    app = FastAPI(title="Sample Split Service", version="1.0.0")
    app.state.db_path = db_path

    @app.exception_handler(ApiError)
    async def api_error_handler(_request: Request, exc: ApiError) -> JSONResponse:
        return JSONResponse(
            status_code=exc.status_code,
            content={"error": {"code": exc.code, "message": exc.message}},
        )

    @app.exception_handler(RequestValidationError)
    async def validation_error_handler(_request: Request, exc: RequestValidationError) -> JSONResponse:
        return JSONResponse(
            status_code=422,
            content={
                "error": {
                    "code": "VALIDATION_ERROR",
                    "message": "request failed validation",
                    "details": jsonable_encoder(exc.errors()),
                }
            },
        )

    @app.get("/healthz")
    def healthz() -> dict:
        return {"status": "ok"}

    # ---------------------------------------------------------------- tubes

    @app.post("/tubes", status_code=201)
    def register_tube(req: RegisterTubeRequest, conn: sqlite3.Connection = Depends(get_db)) -> dict:
        now = _utcnow()
        try:
            conn.execute(
                "INSERT INTO tubes (id, balance_ul, revision, created_at) VALUES (?, ?, 0, ?)",
                (req.id, req.balance_ul, now),
            )
        except sqlite3.IntegrityError:
            raise ApiError(409, "TUBE_ALREADY_EXISTS", f"tube {req.id!r} already exists")
        # The receipt is the state this insert committed — revision 0 and the
        # full registered volume. It must not be re-read from the database:
        # a split from another request could commit between the INSERT and a
        # re-SELECT, and the response would then describe a state this
        # creation never produced.
        return {"id": req.id, "balance_ul": req.balance_ul, "revision": 0, "created_at": now}

    @app.get("/tubes")
    def list_tubes(conn: sqlite3.Connection = Depends(get_db)) -> dict:
        rows = conn.execute("SELECT * FROM tubes ORDER BY rowid").fetchall()
        return {"tubes": [_tube_view(r) for r in rows]}

    @app.get("/tubes/{tube_id}")
    def get_tube(tube_id: str, conn: sqlite3.Connection = Depends(get_db)) -> dict:
        row = conn.execute("SELECT * FROM tubes WHERE id = ?", (tube_id,)).fetchone()
        if row is None:
            raise ApiError(404, "TUBE_NOT_FOUND", f"tube {tube_id!r} does not exist")
        view = _tube_view(row)

        # Every creation edge, split or pool. parent_id stays the unique split
        # parent for backwards compatibility; pooled tubes (many parents) expose
        # the full list under parents and report parent_id = None.
        parents: list[dict] = []
        edge = conn.execute(
            "SELECT parent_id, amount_ul FROM lineage_edges WHERE child_id = ?", (tube_id,)
        ).fetchone()
        if edge is not None:
            parents = [{"id": edge["parent_id"], "amount_ul": edge["amount_ul"], "kind": "split"}]
        else:
            sources = conn.execute(
                "SELECT source_id, amount_ul FROM pool_sources ps "
                "JOIN pool_records pr ON pr.id = ps.pool_id "
                "WHERE pr.child_id = ? ORDER BY ps.position",
                (tube_id,),
            ).fetchall()
            parents = [
                {"id": s["source_id"], "amount_ul": s["amount_ul"], "kind": "pool"}
                for s in sources
            ]
        view["parent_id"] = parents[0]["id"] if len(parents) == 1 else None
        view["parents"] = parents
        return view

    @app.get("/tubes/{tube_id}/ancestry")
    def get_ancestry(tube_id: str, conn: sqlite3.Connection = Depends(get_db)) -> dict:
        # The whole walk runs inside one explicit read transaction, so every
        # node and edge of the DAG is read from the same snapshot. Without it
        # each SELECT is its own snapshot (autocommit) and concurrent splits or
        # pools could be stitched into an ancestry that never existed at any
        # single moment. In WAL mode a read transaction does not block writers.
        # Each edge pins the exact parent revision the operation consumed
        # (split.expected_revision on the split record, expected_revision on a
        # pool edge).
        conn.execute("BEGIN")
        try:
            graph = _lineage_graph(conn, tube_id)
            parents_of = graph.pop("_parents_of")
            by_child = graph.pop("_by_child")

            is_dag = any(e["kind"] == "pool" for e in graph["edges"])
            chain = None
            if not is_dag:
                # Pure split lineage is a single root-first chain; preserve the
                # historical response shape exactly, hop evidence included.
                path = [tube_id]
                while parents_of[path[-1]]:
                    path.append(next(iter(parents_of[path[-1]])))
                path.reverse()
                chain = []
                for current in path:
                    node = next(n for n in graph["nodes"] if n["id"] == current)
                    via = None
                    incoming = by_child[current]
                    if incoming:
                        edge = incoming[0]
                        split_row = conn.execute(
                            "SELECT * FROM splits WHERE id = ?", (edge["split_id"],)
                        ).fetchone()
                        via = {
                            "parent_id": edge["parent_id"],
                            "amount_ul": edge["amount_ul"],
                            "split": _split_record(conn, split_row),
                        }
                    chain.append({"tube": node, "via": via})
                graph["depth"] = len(path) - 1
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        conn.execute("COMMIT")

        payload = {"tube_id": tube_id, "depth": graph["depth"], "graph": graph}
        # "chain" is only emitted for purely linear (split-only) ancestry;
        # pooled ancestry is expressed by the DAG graph.
        if chain is not None:
            payload["chain"] = chain
        return payload

    @app.get("/tubes/{tube_id}/splits")
    def get_tube_splits(tube_id: str, conn: sqlite3.Connection = Depends(get_db)) -> dict:
        row = conn.execute("SELECT id FROM tubes WHERE id = ?", (tube_id,)).fetchone()
        if row is None:
            raise ApiError(404, "TUBE_NOT_FOUND", f"tube {tube_id!r} does not exist")
        splits = conn.execute(
            "SELECT * FROM splits WHERE parent_id = ? ORDER BY id", (tube_id,)
        ).fetchall()
        return {"tube_id": tube_id, "splits": [_split_record(conn, s) for s in splits]}

    @app.get("/tubes/{tube_id}/pools")
    def get_tube_pools(tube_id: str, conn: sqlite3.Connection = Depends(get_db)) -> dict:
        row = conn.execute("SELECT id FROM tubes WHERE id = ?", (tube_id,)).fetchone()
        if row is None:
            raise ApiError(404, "TUBE_NOT_FOUND", f"tube {tube_id!r} does not exist")
        pool_ids = conn.execute(
            "SELECT pr.id FROM pool_records pr "
            "JOIN pool_sources ps ON ps.pool_id = pr.id "
            "WHERE ps.source_id = ? ORDER BY pr.id",
            (tube_id,),
        ).fetchall()
        return {
            "tube_id": tube_id,
            "pools": [_pool_record(conn, r["id"]) for r in pool_ids],
        }

    @app.get("/tubes/{tube_id}/provenance")
    def get_provenance(tube_id: str, conn: sqlite3.Connection = Depends(get_db)) -> dict:
        conn.execute("BEGIN")
        try:
            graph = _lineage_graph(conn, tube_id)
            graph.pop("_parents_of")
            graph.pop("_by_child")
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        conn.execute("COMMIT")
        return graph

    # ---------------------------------------------------------------- split

    @app.post("/splits", status_code=201)
    def split_tube(req: SplitRequest, conn: sqlite3.Connection = Depends(get_db)):
        body = req.model_dump(mode="json")
        canonical = _canonical(body)
        digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
        try:
            # BEGIN IMMEDIATE takes the database write lock up front, so the
            # check-then-act sequence below is serialised against every other
            # writer: a concurrent request on the same parent either waits and
            # then sees the bumped revision (412), or replays the stored
            # idempotent response.
            conn.execute("BEGIN IMMEDIATE")

            replay = _replay_or_conflict(conn, req.request_key, digest)
            if replay is not None:
                conn.execute("COMMIT")
                return replay

            parent = conn.execute(
                "SELECT * FROM tubes WHERE id = ?", (req.parent_id,)
            ).fetchone()
            if parent is None:
                raise ApiError(404, "PARENT_NOT_FOUND", f"parent tube {req.parent_id!r} does not exist")
            if parent["revision"] != req.expected_revision:
                raise ApiError(
                    412,
                    "REVISION_CONFLICT",
                    f"parent {req.parent_id!r} is at revision {parent['revision']}, "
                    f"not {req.expected_revision}",
                )

            total = sum(c.amount_ul for c in req.children)
            if total > parent["balance_ul"]:
                raise ApiError(
                    422,
                    "INSUFFICIENT_BALANCE",
                    f"children total {total} uL exceeds parent balance "
                    f"{parent['balance_ul']} uL",
                )

            placeholders = ", ".join("?" for _ in req.children)
            clashes = conn.execute(
                f"SELECT id FROM tubes WHERE id IN ({placeholders})",
                [c.id for c in req.children],
            ).fetchall()
            if clashes:
                taken = ", ".join(sorted(r["id"] for r in clashes))
                raise ApiError(409, "CHILD_ID_EXISTS", f"child id(s) already exist: {taken}")

            now = _utcnow()
            new_balance = parent["balance_ul"] - total
            cur = conn.execute(
                "UPDATE tubes SET balance_ul = ?, revision = revision + 1 "
                "WHERE id = ? AND revision = ?",
                (new_balance, req.parent_id, req.expected_revision),
            )
            if cur.rowcount != 1:  # unreachable under the write lock; defence in depth
                raise ApiError(412, "REVISION_CONFLICT", f"parent {req.parent_id!r} changed concurrently")

            cur = conn.execute(
                "INSERT INTO splits (parent_id, request_key, expected_revision, "
                "total_amount_ul, created_at) VALUES (?, ?, ?, ?, ?)",
                (req.parent_id, req.request_key, req.expected_revision, total, now),
            )
            split_id = cur.lastrowid
            for position, child in enumerate(req.children):
                conn.execute(
                    "INSERT INTO tubes (id, balance_ul, revision, created_at) VALUES (?, ?, 0, ?)",
                    (child.id, child.amount_ul, now),
                )
                conn.execute(
                    "INSERT INTO split_children (split_id, child_id, amount_ul, position) "
                    "VALUES (?, ?, ?, ?)",
                    (split_id, child.id, child.amount_ul, position),
                )
                conn.execute(
                    "INSERT INTO lineage_edges (child_id, parent_id, split_id, amount_ul) "
                    "VALUES (?, ?, ?, ?)",
                    (child.id, req.parent_id, split_id, child.amount_ul),
                )

            response = {
                "split_id": split_id,
                "request_key": req.request_key,
                "parent": {
                    "id": parent["id"],
                    "balance_ul": new_balance,
                    "revision": req.expected_revision + 1,
                },
                "children": [
                    {"id": c.id, "balance_ul": c.amount_ul, "revision": 0} for c in req.children
                ],
                "total_amount_ul": total,
                "created_at": now,
            }
            conn.execute(
                "INSERT INTO idempotency_keys (request_key, request_hash, request_body, "
                "response_body, operation, split_id, pool_id, created_at) "
                "VALUES (?, ?, ?, ?, 'split', ?, NULL, ?)",
                (req.request_key, digest, canonical, json.dumps(response, ensure_ascii=False),
                 split_id, now),
            )
            conn.execute("COMMIT")
            return response
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise

    # ---------------------------------------------------------------- pool

    @app.post("/pools", status_code=201)
    def pool_tubes(req: PoolRequest, conn: sqlite3.Connection = Depends(get_db)):
        body = req.model_dump(mode="json")
        canonical = _canonical(body)
        digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
        try:
            # One write transaction around the whole merge. Every check runs
            # before any UPDATE: if a later source is stale, short, or a
            # history insert fails, the rollback undoes even the earliest
            # source's deduction — a failed pool never leaves partial changes.
            conn.execute("BEGIN IMMEDIATE")

            replay = _replay_or_conflict(conn, req.request_key, digest)
            if replay is not None:
                conn.execute("COMMIT")
                return replay

            # --- validate every source first, mutate nothing ---------------
            checked: list[tuple] = []  # (PoolSource, current_row)
            total = 0
            for source in req.sources:
                row = conn.execute(
                    "SELECT * FROM tubes WHERE id = ?", (source.id,)
                ).fetchone()
                if row is None:
                    raise ApiError(
                        404, "SOURCE_NOT_FOUND", f"source tube {source.id!r} does not exist"
                    )
                if row["revision"] != source.expected_revision:
                    raise ApiError(
                        412,
                        "REVISION_CONFLICT",
                        f"source {source.id!r} is at revision {row['revision']}, "
                        f"not {source.expected_revision}",
                    )
                if row["balance_ul"] < source.amount_ul:
                    raise ApiError(
                        422,
                        "INSUFFICIENT_BALANCE",
                        f"source {source.id!r} balance {row['balance_ul']} uL cannot "
                        f"contribute {source.amount_ul} uL",
                    )
                total += source.amount_ul
                if total > SQLITE_INT64_MAX:
                    # Schema validation normally rejects this before the
                    # request arrives; this keeps the boundary enforced inside
                    # the serialized transaction as defence in depth.
                    raise ApiError(
                        422,
                        "VALIDATION_ERROR",
                        f"sources total {total} uL exceeds the storable limit "
                        f"{SQLITE_INT64_MAX} uL",
                    )
                checked.append((source, row))

            if conn.execute(
                "SELECT 1 FROM tubes WHERE id = ?", (req.new_id,)
            ).fetchone() is not None:
                raise ApiError(409, "CHILD_ID_EXISTS", f"tube {req.new_id!r} already exists")

            # --- all inputs valid: apply the whole operation atomically -----
            now = _utcnow()
            updated = []
            for source, row in checked:
                remaining = row["balance_ul"] - source.amount_ul
                new_revision = source.expected_revision + 1
                cur = conn.execute(
                    "UPDATE tubes SET balance_ul = ?, revision = revision + 1 "
                    "WHERE id = ? AND revision = ?",
                    (remaining, source.id, source.expected_revision),
                )
                if cur.rowcount != 1:  # unreachable under the write lock
                    raise ApiError(
                        412, "REVISION_CONFLICT", f"source {source.id!r} changed concurrently"
                    )
                updated.append(
                    {
                        "id": source.id,
                        "amount_ul": source.amount_ul,
                        "balance_ul": remaining,
                        "revision": new_revision,
                        "expected_revision": source.expected_revision,
                    }
                )

            try:
                conn.execute(
                    "INSERT INTO tubes (id, balance_ul, revision, created_at) "
                    "VALUES (?, ?, 0, ?)",
                    (req.new_id, total, now),
                )
            except sqlite3.IntegrityError:
                raise ApiError(409, "CHILD_ID_EXISTS", f"tube {req.new_id!r} already exists")

            cur = conn.execute(
                "INSERT INTO pool_records (child_id, request_key, total_amount_ul, created_at) "
                "VALUES (?, ?, ?, ?)",
                (req.new_id, req.request_key, total, now),
            )
            pool_id = cur.lastrowid
            for position, source in enumerate(req.sources):
                conn.execute(
                    "INSERT INTO pool_sources (pool_id, source_id, expected_revision, "
                    "amount_ul, position) VALUES (?, ?, ?, ?, ?)",
                    (pool_id, source.id, source.expected_revision, source.amount_ul, position),
                )

            response = {
                "pool_id": pool_id,
                "request_key": req.request_key,
                "tube": {"id": req.new_id, "balance_ul": total, "revision": 0},
                "sources": updated,
                "total_amount_ul": total,
                "created_at": now,
            }
            conn.execute(
                "INSERT INTO idempotency_keys (request_key, request_hash, request_body, "
                "response_body, operation, split_id, pool_id, created_at) "
                "VALUES (?, ?, ?, ?, 'pool', NULL, ?, ?)",
                (req.request_key, digest, canonical, json.dumps(response, ensure_ascii=False),
                 pool_id, now),
            )
            conn.execute("COMMIT")
            return response
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise

    return app


app = create_app(os.environ.get("DATABASE_PATH", "data/lab.db"))
