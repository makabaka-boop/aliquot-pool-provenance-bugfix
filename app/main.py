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
from .schemas import SQLITE_INT64_MAX, PoolRequest, RegisterTubeRequest, SplitRequest


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
    pool_row = conn.execute("SELECT * FROM pools WHERE id = ?", (pool_id,)).fetchone()
    sources = conn.execute(
        "SELECT source_id AS id, amount_ul, expected_revision, position "
        "FROM pool_sources WHERE pool_id = ? ORDER BY position",
        (pool_id,),
    ).fetchall()
    return {
        "pool_id": pool_row["id"],
        "new_tube_id": pool_row["new_tube_id"],
        "request_key": pool_row["request_key"],
        "total_amount_ul": pool_row["total_amount_ul"],
        "created_at": pool_row["created_at"],
        "sources": [dict(s) for s in sources],
    }


def _edge_view(edge: sqlite3.Row) -> dict:
    return {
        "child_id": edge["child_id"],
        "parent_id": edge["parent_id"],
        "operation": edge["operation"],
        "position": edge["position"],
        "amount_ul": edge["amount_ul"],
        "total_amount_ul": edge["total_amount_ul"],
        "split_id": edge["split_id"],
        "pool_id": edge["pool_id"],
    }


def get_db(request: Request):
    conn = dbmod.connect(request.app.state.db_path)
    try:
        yield conn
    finally:
        conn.close()


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
        # A tube is created by exactly one operation: a split gives one
        # parent, a pool gives one parent per contributing source.
        parents = conn.execute(
            "SELECT parent_id FROM lineage_edges WHERE child_id = ? "
            "ORDER BY position, parent_id",
            (tube_id,),
        ).fetchall()
        parent_ids = [e["parent_id"] for e in parents]
        view["parent_id"] = parent_ids[0] if len(parent_ids) == 1 else None
        view["parent_ids"] = parent_ids
        return view

    def _load_ancestor_graph(conn: sqlite3.Connection, tube_id: str) -> tuple[dict, dict]:
        """Read the full ancestor DAG of tube_id — tubes and contribution
        edges — using only the caller's current snapshot."""
        nodes: dict[str, sqlite3.Row] = {}
        edges: dict[tuple[str, str], sqlite3.Row] = {}
        frontier = [tube_id]
        seen: set[str] = set()
        while frontier:
            current_id = frontier.pop()
            if current_id in seen:
                continue
            seen.add(current_id)
            row = conn.execute(
                "SELECT * FROM tubes WHERE id = ?", (current_id,)
            ).fetchone()
            if row is None:
                raise ApiError(404, "TUBE_NOT_FOUND", f"tube {current_id!r} does not exist")
            nodes[current_id] = row
            for edge in conn.execute(
                "SELECT * FROM lineage_edges WHERE child_id = ? ORDER BY position, parent_id",
                (current_id,),
            ).fetchall():
                edges[(edge["child_id"], edge["parent_id"])] = edge
                if edge["parent_id"] not in seen:
                    frontier.append(edge["parent_id"])
        return nodes, edges

    def _via_for_edge(conn: sqlite3.Connection, edge: sqlite3.Row) -> dict:
        via = {
            "parent_id": edge["parent_id"],
            "position": edge["position"],
            "amount_ul": edge["amount_ul"],
        }
        if edge["operation"] == "split":
            split_row = conn.execute(
                "SELECT * FROM splits WHERE id = ?", (edge["split_id"],)
            ).fetchone()
            via["operation"] = "split"
            via["split"] = _split_record(conn, split_row)
        else:
            via["operation"] = "pool"
            via["pool"] = _pool_record(conn, edge["pool_id"])
        return via

    def _hop_for(conn: sqlite3.Connection, nodes: dict, edges: dict, child_id: str, parent_id: str) -> dict:
        return {"tube": _tube_view(nodes[child_id]),
                "via": _via_for_edge(conn, edges[(child_id, parent_id)])}

    @app.get("/tubes/{tube_id}/ancestry")
    def get_ancestry(tube_id: str, conn: sqlite3.Connection = Depends(get_db)) -> dict:
        # The whole walk runs inside one explicit read transaction, so every
        # node and edge of the graph is read from the same snapshot. Without
        # it each SELECT is its own snapshot (autocommit) and concurrent
        # splits/pools could be stitched into a combination that never existed
        # at any single moment. In WAL mode a read transaction does not block
        # writers. Each edge's evidence carries the expected revision the
        # operation consumed, pinning the exact source-balance versions.
        conn.execute("BEGIN")
        try:
            nodes, edges = _load_ancestor_graph(conn, tube_id)
            parents: dict[str, list[str]] = {}
            for (child_id, parent_id), edge in edges.items():
                parents.setdefault(child_id, []).append(parent_id)

            root_ids = sorted(node_id for node_id in nodes if node_id not in parents)

            # Enumerate every root -> tube_id path. A non-merged tube has
            # exactly one (the historical linear chain); pooling can open
            # several, one per root that contributed.
            def paths_up(child_id: str) -> list[list[str]]:
                up = parents.get(child_id, [])
                if not up:
                    return [[child_id]]
                result = []
                for parent_id in up:
                    for path in paths_up(parent_id):
                        result.append(path + [child_id])
                return result

            path_ids = sorted(paths_up(tube_id), key=lambda p: p)
            chains = []
            for path in path_ids:
                hops = [{"tube": _tube_view(nodes[path[0]]), "via": None}]
                for child_id, parent_id in zip(path[1:], path[:-1]):
                    hops.append(_hop_for(conn, nodes, edges, child_id, parent_id))
                chains.append(hops)

            graph = {
                "nodes": [_tube_view(nodes[nid]) for nid in sorted(nodes)],
                "edges": [_edge_view(edges[key]) for key in sorted(edges)],
                "roots": root_ids,
            }
            merged = any(len(up) > 1 for up in parents.values())
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        conn.execute("COMMIT")

        depth = max((len(path) - 1 for path in path_ids), default=0)
        payload = {
            "tube_id": tube_id,
            "depth": depth,
            "lineage": "merged" if merged else "single",
            # Backwards-compatible linear chain; present only when the tube
            # has exactly one root-to-tube path (every split-only database).
            "chain": chains[0] if len(chains) == 1 else None,
            "chains": chains,
            "graph": graph,
        }
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
        pool_ids = [
            r["pool_id"]
            for r in conn.execute(
                "SELECT DISTINCT pool_id FROM pool_sources WHERE source_id = ? ORDER BY pool_id",
                (tube_id,),
            ).fetchall()
        ]
        return {"tube_id": tube_id, "pools": [_pool_record(conn, pid) for pid in pool_ids]}

    @app.get("/pools/{pool_id}")
    def get_pool(pool_id: int, conn: sqlite3.Connection = Depends(get_db)) -> dict:
        row = conn.execute("SELECT id FROM pools WHERE id = ?", (pool_id,)).fetchone()
        if row is None:
            raise ApiError(404, "POOL_NOT_FOUND", f"pool {pool_id} does not exist")
        return _pool_record(conn, pool_id)

    @app.get("/tubes/{tube_id}/provenance")
    def get_provenance(tube_id: str, conn: sqlite3.Connection = Depends(get_db)) -> dict:
        conn.execute("BEGIN")
        try:
            nodes, edges = _load_ancestor_graph(conn, tube_id)
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        conn.execute("COMMIT")
        return {
            "tube_id": tube_id,
            "nodes": [_tube_view(nodes[nid]) for nid in sorted(nodes)],
            "edges": [_edge_view(edges[key]) for key in sorted(edges)],
            "roots": sorted(nid for nid in nodes if all(c != nid for (c, _p) in edges)),
        }

    # ---------------------------------------------------------------- split

    @app.post("/splits", status_code=201)
    def split_tube(req: SplitRequest, conn: sqlite3.Connection = Depends(get_db)):
        body = req.model_dump(mode="json")
        canonical = _canonical(body)
        digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
        try:
            # BEGIN IMMEDIATE takes the database write lock up front, so the
            # check-then-act sequence below is serialised against every other
            # mutation: a concurrent request on the same parent either waits
            # and then sees the bumped revision (412), or replays the stored
            # idempotent response.
            conn.execute("BEGIN IMMEDIATE")

            replay = conn.execute(
                "SELECT request_hash, response_body FROM idempotency_keys WHERE request_key = ?",
                (req.request_key,),
            ).fetchone()
            if replay is not None:
                if replay["request_hash"] != digest:
                    raise ApiError(
                        409,
                        "REQUEST_KEY_CONFLICT",
                        f"request_key {req.request_key!r} was already used with a different body",
                    )
                conn.execute("COMMIT")
                return JSONResponse(status_code=201, content=json.loads(replay["response_body"]))

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
                    "INSERT INTO lineage_edges (child_id, parent_id, position, operation, "
                    "split_id, pool_id, amount_ul, total_amount_ul) "
                    "VALUES (?, ?, 0, 'split', ?, NULL, ?, ?)",
                    (child.id, req.parent_id, split_id, child.amount_ul, total),
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
                "INSERT INTO idempotency_keys (request_key, operation, request_hash, request_body, "
                "response_body, split_id, pool_id, created_at) "
                "VALUES (?, 'split', ?, ?, ?, ?, NULL, ?)",
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
            # Everything this operation can observe or change happens in one
            # serialised transaction. BEGIN IMMEDIATE takes the write lock
            # before any check, so validation, deductions, revisions, the new
            # tube, every contribution edge and the receipt either all commit
            # together or are all rolled back — no early source can be left
            # deducted when a later source fails.
            conn.execute("BEGIN IMMEDIATE")

            replay = conn.execute(
                "SELECT request_hash, response_body FROM idempotency_keys WHERE request_key = ?",
                (req.request_key,),
            ).fetchone()
            if replay is not None:
                if replay["request_hash"] != digest:
                    raise ApiError(
                        409,
                        "REQUEST_KEY_CONFLICT",
                        f"request_key {req.request_key!r} was already used with a different body",
                    )
                conn.execute("COMMIT")
                return JSONResponse(status_code=201, content=json.loads(replay["response_body"]))

            # Validate every source before writing anything. Sources are
            # checked in request order so the first problem is deterministic.
            total = 0
            source_views = []
            source_rows = []
            for source in req.sources:
                total += source.amount_ul
                row = conn.execute(
                    "SELECT * FROM tubes WHERE id = ?", (source.id,)
                ).fetchone()
                if row is None:
                    raise ApiError(
                        404, "TUBE_NOT_FOUND", f"source tube {source.id!r} does not exist"
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
                        f"source {source.id!r} balance {row['balance_ul']} uL cannot provide "
                        f"{source.amount_ul} uL",
                    )
                source_rows.append(row)

            # The schema layer already rejects this; keep the guard next to
            # the column that would overflow so a future caller can never
            # surface a 500 from SQLite integer overflow.
            if total > SQLITE_INT64_MAX:
                raise ApiError(
                    422,
                    "POOL_TOTAL_EXCEEDS_LIMIT",
                    f"sources total {total} uL exceeds the storable integer limit {SQLITE_INT64_MAX}",
                )

            if conn.execute(
                "SELECT 1 FROM tubes WHERE id = ?", (req.new_id,)
            ).fetchone() is not None:
                raise ApiError(
                    409, "CHILD_ID_EXISTS", f"tube {req.new_id!r} already exists"
                )

            now = _utcnow()

            # Deduct every source with a conditional UPDATE on the revision
            # observed above (defence in depth alongside the write lock).
            for source, row in zip(req.sources, source_rows):
                remaining = row["balance_ul"] - source.amount_ul
                cur = conn.execute(
                    "UPDATE tubes SET balance_ul = ?, revision = revision + 1 "
                    "WHERE id = ? AND revision = ?",
                    (remaining, source.id, source.expected_revision),
                )
                if cur.rowcount != 1:  # unreachable under the write lock
                    raise ApiError(
                        412,
                        "REVISION_CONFLICT",
                        f"source {source.id!r} changed concurrently",
                    )
                source_views.append(
                    {
                        "id": source.id,
                        "amount_ul": source.amount_ul,
                        "balance_ul": remaining,
                        "revision": source.expected_revision + 1,
                    }
                )

            conn.execute(
                "INSERT INTO tubes (id, balance_ul, revision, created_at) VALUES (?, ?, 0, ?)",
                (req.new_id, total, now),
            )
            cur = conn.execute(
                "INSERT INTO pools (new_tube_id, request_key, total_amount_ul, created_at) "
                "VALUES (?, ?, ?, ?)",
                (req.new_id, req.request_key, total, now),
            )
            pool_id = cur.lastrowid
            for position, source in enumerate(req.sources):
                conn.execute(
                    "INSERT INTO pool_sources (pool_id, source_id, position, expected_revision, "
                    "amount_ul) VALUES (?, ?, ?, ?, ?)",
                    (pool_id, source.id, position, source.expected_revision, source.amount_ul),
                )
                conn.execute(
                    "INSERT INTO lineage_edges (child_id, parent_id, position, operation, "
                    "split_id, pool_id, amount_ul, total_amount_ul) "
                    "VALUES (?, ?, ?, 'pool', NULL, ?, ?, ?)",
                    (req.new_id, source.id, position, pool_id, source.amount_ul, total),
                )

            response = {
                "pool_id": pool_id,
                "request_key": req.request_key,
                "tube": {"id": req.new_id, "balance_ul": total, "revision": 0},
                "sources": source_views,
                "total_amount_ul": total,
                "created_at": now,
            }
            conn.execute(
                "INSERT INTO idempotency_keys (request_key, operation, request_hash, request_body, "
                "response_body, split_id, pool_id, created_at) "
                "VALUES (?, 'pool', ?, ?, ?, NULL, ?, ?)",
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
