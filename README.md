# Sample Split Service

可追溯、不超分的样本分装 / 合管库存服务。Python 3.12 / FastAPI 纯后端，SQLite 文件持久化，Docker Compose 一键运行。

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
- `splits` / `split_children`：单来源分装历史；`pools` / `pool_sources`：多来源合管历史（2–5 支来源，按请求序记录各自的 `expected_revision` 与贡献体积）。两者只插入；数据库触发器拒绝任何 UPDATE/DELETE，历史不可改写。
- `lineage_edges`：谱系 DAG，每个 **(子管, 父管) 贡献** 一行。分装子管只有一条边；合管新管对每支来源各有一条边，因此新管及其后代可以追到全部真实祖先与各自贡献体积。
- `idempotency_keys`：请求键 → 操作类型（`split`/`pool`）+ 规范化正文哈希 + 原始响应，与对应操作在**同一事务**写入。请求键在分装与合管之间共享一个命名空间。
- 不变量：任意时刻 `Σ 所有管的余额 == Σ 登记的初始量`（分装在母管与子管间搬运、合管在各来源与新管间搬运微升量）。

## API

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/tubes` | 登记初始样本管 `{id, balance_ul}`（正整数微升，上限 2^63-1），修订号从 0 开始；回执即本次创建提交的状态（余额=登记量、修订号=0），不回读库，不受并发操作影响 |
| GET | `/tubes` | 列出全部管的当前余额与修订号 |
| GET | `/tubes/{id}` | 单管当前余额、修订号；`parent_id`（唯一父管时）与 `parent_ids`（全部直接来源，合管时为多支） |
| POST | `/splits` | 单来源分装（见下） |
| POST | `/pools` | 多来源合管（2–5 支 → 一支新管，见下） |
| GET | `/tubes/{id}/ancestry` | 祖先 DAG：`graph`（节点+全部贡献边）、`chains`（每条根→该管路径，合管时多条）、单路径时额外提供根到该管的线性 `chain`；整条 DAG 在单个读事务快照中读取，各跳附原始分装/合管记录，记录中的 `expected_revision` 标明它消费的来源修订号 |
| GET | `/tubes/{id}/splits` | 该管作为母管的全部分装记录 |
| GET | `/tubes/{id}/pools` | 该管作为来源参与的全部合管记录 |
| GET | `/pools/{id}` | 单笔合管记录（全部来源及贡献体积） |
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
  "new_id": "pool-007",
  "request_key": "req-77",
  "sources": [
    {"id": "src-a", "expected_revision": 2, "amount_ul": 300},
    {"id": "src-b", "expected_revision": 0, "amount_ul": 150}
  ]
}
```

- `sources` 2–5 支、互不相同且不得与 `new_id` 相同；每支 `amount_ul` 为正整数微升。
- 校验顺序固定（按请求序逐支检查存在性、修订号、余额），**所有来源先全部校验通过，再统一写入**。
- **单事务**：`BEGIN IMMEDIATE` 在入口即取写锁，全部来源的扣减与修订 +1、新管建立（余额=贡献合计、修订 0）、每支来源的贡献边与合管记录、幂等回执要么一起提交，要么整体回滚——较后的来源修订过期/余额不足/写入失败时，较早的来源不会留下扣量。
- 多支合法贡献合计超过 2^63-1 时在请求校验阶段返回稳定的 **422 VALIDATION_ERROR**，不触碰数据库、不占用请求键。
- 回执含新管状态与每支来源扣减后的余额/修订，与库内余额、修订、历史、谱系边、请求键回执互为同一操作的一致视图。

### 幂等与并发

- 相同 `request_key` + 完全相同正文（JSON 语义相同，键序无关）→ 返回首次的原始结果，不重复扣减；重启后同样取回首次回执。
- 相同 `request_key` + 正文不同 → **409 REQUEST_KEY_CONFLICT**。请求键在分装与合管间共用，跨操作复用同键同样冲突，不会各生效一次。
- 并发写操作（分装-分装、合管-合管、分装-合管）经 `BEGIN IMMEDIATE` 串行化：后到者看到修订号已变 → **412 REVISION_CONFLICT**，最多一笔成功；同键并发重放只执行一次。
- 失败的请求不占用请求键，修正后可用原键重发。

### 错误码

| HTTP | `error.code` | 场景 |
|---|---|---|
| 422 | `VALIDATION_ERROR` | 未知字段、非法量（0/负数/小数/字符串/超出 int64 范围）、子管/来源数越界、请求内 id 重复、**合管贡献合计超出 int64** 等 |
| 422 | `INSUFFICIENT_BALANCE` | 分装总量超过母管余额，或某合管来源贡献超过其余额 |
| 404 | `TUBE_NOT_FOUND` / `PARENT_NOT_FOUND` / `POOL_NOT_FOUND` | 管 / 母管 / 合管不存在 |
| 409 | `TUBE_ALREADY_EXISTS` | 重复登记同一管 id |
| 409 | `CHILD_ID_EXISTS` | 子管 / 合管新管 id 已被占用 |
| 409 | `REQUEST_KEY_CONFLICT` | 请求键被不同正文（含跨分装/合管）复用 |
| 412 | `REVISION_CONFLICT` | `expected_revision` 与当前修订号不符 |

错误体统一为 `{"error": {"code", "message", ...}}`。

## 升级与兼容

启动时自动迁移旧库（以 `PRAGMA user_version` 标记）：仅含分装记录的上线数据库无损升级，旧的线性谱系与幂等回执继续可用；旧开发版本可能留下的扁平 `pool_records` 表也会升级为 `pools`/`pool_sources`（其仅保留的首来源按原样迁移）。迁移中途崩溃可安全重入。

## 测试

`pytest` 覆盖：登记与校验（含超出 int64 范围的体积返回 422 且不落库）、分装规则、合管规则（2–5 支来源、修订冲突/余额不足/来源缺失/新管冲突均整体回滚且不烧键、合计溢出 int64 为稳定 422）、合管幂等与跨操作请求键冲突、合管后多祖先 DAG 谱系（含合管后代再次合管/分装）、**真实 uvicorn 双连接竞争**（分装-分装、合管-合管、分装-合管、同键并发重放、语句闸门确定性交错）、重启后余额/修订/历史/谱系/回执交叉复核、旧分装库与旧扁平合管表迁移、逐层操作后的总量守恒、历史表触发器拒绝改写。

`tests/test_acceptance.py` 用语句闸门（`gated_server`，在真实 uvicorn + SQLite 文件上把指定 SQL 停在请求中途）确定性交错：登记 INSERT 提交后、响应生成前插入一笔分装，回执仍为修订号 0 与全额；谱系查询读到后代后依次分装后代与各级祖先，返回链仍是查询开始时的同一快照，守恒可复算、`expected_revision` 可对版，交错请求的重放返回首次结果。

## 项目结构

```
app/
  main.py      # 路由、事务编排、异常映射、谱系 DAG 遍历（create_app 工厂）
  db.py        # 连接、建表、不可变触发器、旧库迁移
  schemas.py   # Pydantic 请求模型（extra=forbid，严格正整数，合管合计上限）
  errors.py    # ApiError
tests/         # pytest 套件
Dockerfile     # python:3.12-slim
docker-compose.yml
```
