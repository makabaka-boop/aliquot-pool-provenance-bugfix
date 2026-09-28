# Sample Split Service

可追溯、不超分的样本分装库存服务。Python 3.12 / FastAPI 纯后端，SQLite 文件持久化，Docker Compose 一键运行。

## 快速开始

```bash
docker compose up --build      # 服务监听 http://localhost:8000
```

本地开发（Python 3.12+）：

```bash
pip install -r requirements-dev.txt
uvicorn app.main:app --reload          # DATABASE_PATH 环境变量可指定 SQLite 文件，默认 data/lab.db
pytest                                 # 运行全部测试（含双连接并发竞争）
```

## 数据模型与不变量

- `tubes`：**当前状态**（余额 `balance_ul`、修订号 `revision`），唯一可变的表。
- `splits` / `split_children` / `lineage_edges`：**分装历史事实**，只插入；数据库触发器拒绝任何 UPDATE/DELETE，历史不可改写。
- `pool_records` / `pool_sources`：**合并历史事实**：一支新管由 2–5 支来源管合并而成，每支来源是一条独立谱系边（贡献体积 + 消费的来源修订号），不存在“主来源”。同样只插入、触发器拒绝改写。
- `idempotency_keys`：请求键 → 规范化正文哈希 + 原始响应 + 操作类型（`split`/`pool`），分装与合并**共用全局请求键命名空间**，与业务变更同事务写入。
- 谱系因此是一个 DAG：一支管由一次分装（单一母管）或一次合并（多个来源）创建；`/ancestry` 与 `/provenance` 能追到全部祖先及各自贡献体积。
- 不变量：任意时刻 `Σ 所有管的余额 == Σ 登记的初始量`（分装与合并都只在管之间搬运微升量）。

## API

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/tubes` | 登记初始样本管 `{id, balance_ul}`（正整数微升，上限 2^63-1），修订号从 0 开始；回执即本次创建提交的状态（余额=登记量、修订号=0），不回读库，不受并发操作影响 |
| GET | `/tubes` | 列出全部管的当前余额与修订号 |
| GET | `/tubes/{id}` | 单管当前余额、修订号；`parent_id`（仅单来源创建时）与 `parents`（全部创建来源及贡献体积） |
| POST | `/splits` | 分装（见下） |
| POST | `/pools` | 合并 2–5 支来源管为一支新管（见下） |
| GET | `/tubes/{id}/ancestry` | 完整祖先 DAG（`graph`：`nodes`/`edges`/`pools`，整个查询在单个读事务快照中读取）；纯分装谱系另附根到该管的 `chain`，每跳附原始分装记录 |
| GET | `/tubes/{id}/provenance` | 与 ancestry 的 `graph` 相同的完整祖先 DAG |
| GET | `/tubes/{id}/splits` | 该管作为母管的全部历史分装记录 |
| GET | `/tubes/{id}/pools` | 该管作为来源贡献过的全部合并记录 |
| GET | `/healthz` | 健康检查 |

### POST /splits

```json
{
  "parent_id": "master-001",
  "expected_revision": 0,
  "request_key": "req-42",
  "children": [{"id": "aliquot-a", "amount_ul": 300}, {"id": "aliquot-b", "amount_ul": 150}]
}
```

- `children` 1–20 支，id 在请求内唯一且不得与任何现存管重复，`amount_ul` 为正整数微升。
- 子管总量不得超过母管当前余额（可以恰好分完，母管余额归 0）。
- **单事务**：母管扣减、子管建立、谱系边、修订号 +1、幂等记录在同一事务提交，失败整体回滚。

### POST /pools

```json
{
  "new_id": "pooled-001",
  "request_key": "pool-42",
  "sources": [
    {"id": "tube-a", "expected_revision": 2, "amount_ul": 300},
    {"id": "tube-b", "expected_revision": 0, "amount_ul": 150}
  ]
}
```

- `sources` 2–5 支，id 互不相同且与 `new_id` 不同；每支携带各自当前修订号 `expected_revision` 与贡献体积。
- 每个贡献量为正整数微升；**合计体积不得超过 2^63-1**，否则在输入校验阶段返回 **422 VALIDATION_ERROR**，不扣任何来源。
- 各来源余额必须足以贡献其体积。
- **单事务、先验后写**：`BEGIN IMMEDIATE` 先在事务内校验全部来源（存在性、修订号、余额）与新管 id，全部通过后才统一扣减、建新管、写 `pool_records` + 每来源一条 `pool_sources` 及幂等记录。较后的来源修订过期、余额不足或任何历史写入失败时整体回滚，较早的来源不会留下扣量或修订 +1。
- 新管谱系包含**全部**来源：每支来源及其贡献体积、消费的修订号都永久可查（祖先 DAG、`/tubes/{new_id}/ancestry`）。

### 幂等与并发

- 相同 `request_key` + 完全相同正文（JSON 语义相同，键序无关）→ 返回首次的原始结果，不重复扣减；**重试在重启后同样稳定取回首次回执**（回执与历史同事务落盘）。
- 请求键为**全局命名空间**：同一键被不同正文复用（无论分装还是合并，也包括分装与合并之间复用）→ **409 REQUEST_KEY_CONFLICT**。
- 两个终端同时按同一旧修订号操作同一管：`BEGIN IMMEDIATE` 在事务入口即取写锁，后到者看到修订号已变 → **412 REVISION_CONFLICT**，最多一笔成功。同键同正文并发则只执行一次，双方都收到首次结果。
- 失败的请求不占用请求键，修正后可用原键重发。

### 错误码

| HTTP | `error.code` | 场景 |
|---|---|---|
| 422 | `VALIDATION_ERROR` | 未知字段、非法量（0/负数/小数/字符串/超出 int64 范围）、合并合计超 int64、来源/子管数越界、请求内 id 重复等 |
| 422 | `INSUFFICIENT_BALANCE` | 分装子管总量超过母管余额，或某来源不足以贡献其体积 |
| 404 | `TUBE_NOT_FOUND` / `PARENT_NOT_FOUND` / `SOURCE_NOT_FOUND` | 管不存在 |
| 409 | `TUBE_ALREADY_EXISTS` | 重复登记同一管 id |
| 409 | `CHILD_ID_EXISTS` | 新管/子管 id 已被占用 |
| 409 | `REQUEST_KEY_CONFLICT` | 请求键被不同正文（或不同操作）复用 |
| 412 | `REVISION_CONFLICT` | `expected_revision` 与当前修订号不符 |

错误体统一为 `{"error": {"code", "message", ...}}`。

## 测试

`pytest` 覆盖：登记与校验（含超出 int64 范围的体积返回 422 且不落库）、分装规则、**合并**（多来源成功、后来的来源修订过期/余额不足/不存在/新管 id 冲突时整体回滚无部分扣量、合计超 int64 返回 422、同键重放只生效一次、分装与合并跨操作复用键返回 409、合并谱系包含全部来源及其贡献、合并管后代能追到全部根、重启后余额/历史/回执一致）、**两个独立连接经真实 uvicorn 服务竞争同一管**（同键并发只执行一次、不同键按修订号串行恰一笔成功）、逐层操作后的总量守恒、历史表触发器拒绝改写。

`tests/test_pool_concurrency_and_migration.py` 还直接构造**旧版数据库文件**（仅分装记录 + 旧形状 `idempotency_keys`，以及未上线版本的半成品 `pool_records`）验证启动迁移：旧分装键升级后仍重放首次回执、旧数据不重复扣减、新合并功能可在升级后的库上正常使用。

`tests/test_acceptance.py` 用语句闸门（`gated_server`，在真实 uvicorn + SQLite 文件上把指定 SQL 停在请求中途）确定性交错：登记 INSERT 提交后、响应生成前插入一笔分装，回执仍为修订号 0 与全额；谱系查询读到后代后依次分装后代与各级祖先，返回链仍是查询开始时的同一快照，守恒可复算、`expected_revision` 可对版，交错请求的重放返回首次结果。

## 数据库升级

`init_db` 在启动时就地迁移旧库：旧形状的 `idempotency_keys`（`split_id NOT NULL`、无 `operation`/`pool_id`）在一个事务内重建为通用形状，旧分装记录原样保留；未上线版本遗留的半成品 `pool_records`（带 `primary_source_id`、无 `pool_sources`）被丢弃，随后建立新表。仅含分装记录的数据库升级后行为不变。

## 项目结构

```
app/
  main.py      # 路由、事务编排、异常映射（create_app 工厂）
  db.py        # 连接、建表、不可变触发器
  schemas.py   # Pydantic 请求模型（extra=forbid，严格正整数）
  errors.py    # ApiError
tests/         # pytest 套件
Dockerfile     # python:3.12-slim
docker-compose.yml
```
