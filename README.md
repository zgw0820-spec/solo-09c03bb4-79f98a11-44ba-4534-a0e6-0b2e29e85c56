# Device Fault Log Ops — 在线建索引平台

设备故障日志持续写入，运维通过 API 在线构建 `(device_id, occurred_at)` 索引（`CREATE INDEX CONCURRENTLY`），构建期间日志照常写入。两个无状态 API 实例通过 PostgreSQL 自身协调，保证同一索引的并发申请只启动一次构建。

## 架构

```
                ┌────────────┐        ┌────────────┐
   客户端 ─────▶│  api1      │        │  api2      │◀───── 客户端
   (:8001/docs) │ FastAPI    │        │ FastAPI    │ (:8002/docs)
                └─────┬──────┘        └─────┬──────┘
                      │      同一个数据库    │
                      └─────────┬───────────┘
                                ▼
                     ┌─────────────────────┐
                     │  PostgreSQL 16 (db) │
                     │  device_fault_logs  │  预置 400 万行故障日志
                     │  index_build_jobs   │  构建任务台账（唯一约束）
                     └─────────────────────┘
```

协调机制（全部落在数据库内，实例无状态、无领导者）：

| 需求 | 机制 |
| --- | --- |
| 并发申请只构建一次 | `INSERT ... ON CONFLICT (index_name) DO NOTHING`（唯一约束保证跨实例只有一个赢家）；临界区再套一层会话级 advisory lock |
| 构建期间日志照常写入 | `CREATE INDEX CONCURRENTLY`（不阻塞 DML），构建在独立后台线程 + 独立连接上执行 |
| 构建中的无效目录项不得误删 | 清理前检查 `pg_stat_progress_create_index` / `pg_stat_activity` 是否有活跃构建；有则绝不动无效索引；新认领的任务在宽限期内同样受保护 |
| 确认有效才报告就绪 | 构建完成后校验 `pg_index.indisvalid && indisready` 且 `pg_get_indexdef` 定义匹配 `(device_id, occurred_at)`，才置 `ready`；`GET /indexes/status` 每次也实时核对目录 |
| 失败/崩溃后清理重试 | 宽限期后无活跃构建的 `building` 记录判定为孤儿 → `failed`；重试时先 `DROP INDEX CONCURRENTLY IF EXISTS` 清掉无效残件再重新认领 |
| 重启状态一致 | 实例启动时 `reconcile()`：把台账与 `pg_catalog` 实际状态对齐（孤儿→failed、已完成→ready、目录矛盾→failed） |
| 迟到旧作业不得覆盖新结果 | 所有终态写入带围栏条件 `WHERE id = <job> AND status = 'building'`；任务行被清理重建后旧作业的 UPDATE 命中 0 行 |

已知限制：实例崩溃后，其 `building` 记录需等 `ORPHAN_GRACE_SECONDS`（默认 20s，compose 中设 15s）宽限期过后才被判定为孤儿——这是区分“认领后尚未在 `pg_stat_activity` 可见”与“构建者已死”的必要窗口。

## 运行（Docker Compose）

```bash
docker compose up -d --build
# Swagger UI: http://localhost:8001/docs  或  http://localhost:8002/docs
# PostgreSQL: localhost:5433 (ops/ops, db=faultlogs)
```

首次启动时 `db/init.sql` 预置 400 万行日志（约需几十秒），db 健康检查会等到 schema 就绪后才放行 API 容器。

## API

| 方法/路径 | 说明 |
| --- | --- |
| `POST /logs` | 写入一条故障日志（`device_id`、`message` 必填；`severity`、`occurred_at` 可选） |
| `GET /logs` | 按 `device_id` + 时间范围查询（走新索引） |
| `GET /logs/plan` | `EXPLAIN` 上述查询，验证索引被使用 |
| `GET /events` | 按设备 + UTC 时间段（最长 24 小时）查询故障事件，含起止时间、日志 ID、确认状态、事件标识 |
| `POST /events/ack` | 确认"所见的那组日志"；同一事件重复确认为幂等 |
| `GET /events/plan` | `EXPLAIN` 事件分组使用的 critical 日志扫描 |
| `POST /indexes/build` | 幂等申请建索引；并发调用只会有一个 `started`，其余 `building`；已就绪返回 `ready` |
| `GET /indexes/status` | 任务状态 + 实时目录核对（`catalog.exists/valid/ready/definition`、活跃构建后端数） |
| `POST /indexes/cleanup` | 确认无活跃构建后，删除无效残件索引并清理失败任务，允许重试 |
| `GET /health` | 健康检查 |

`POST /indexes/build` 返回的 `state`：`started`（本次认领成功）、`building`（他处已在构建）、`ready`（目录已确认有效）、`conflict`（同名索引定义不符，需人工处理）。

## 故障事件与确认

### 事件的判定

同一设备、`severity = 'critical'` 的日志按时间排序：

* 相邻两条日志的时间间隔 **≤ 5 分钟** 即连成一段；
* 一段内 **至少 3 条** 日志才构成事件（1～2 条不是事件）；
* 非 critical 日志不参与分组，也不会切断或连接事件。

`GET /events?device_id=...&start=...&end=...` 返回至少有一条日志落在窗口 `[start, end]` 内的完整事件。跨窗口边界的事件会被完整返回：返回的 `start` 可能早于请求的 `start`，`end` 可能晚于请求的 `end`，这样 `log_ids` 始终是构成一个完整事件的确切集合，`event_id` 也不随查询窗口变化。时间窗最大 24 小时（超出返回 422）。

```json
{
  "device_id": "dev-00042",
  "start": "2026-01-01T00:00:00Z", "end": "2026-01-01T01:00:00Z",
  "count": 1,
  "events": [{
    "event_id": "evt_7f3a...",
    "device_id": "dev-00042",
    "start": "2026-01-01T00:12:00Z", "end": "2026-01-01T00:21:00Z",
    "first_log_id": 4000123, "last_log_id": 4000131,
    "log_count": 4, "log_ids": [4000123, 4000125, 4000128, 4000131],
    "acknowledged": false, "acknowledged_at": null, "acknowledged_by": null
  }]
}
```

### 事件标识与确认失效

事件标识由**日志集合本身**决定：

```
event_id = "evt_" + sha256(device_id, 按时间排序的 log_ids)[:32]
```

不引入可变的事件行。回填一条旧时间的日志只要扩展或合并了事件，日志集合就变了，`event_id` 随之改变：旧确认按新 id 查不到，事件立即显示为未确认；用旧 `event_id` 调用确认会校验失败（409），不可能作用到变化后的事件。无需后台失效任务，两个实例的判定天然一致。

`POST /events/ack` 必须同时提交 `device_id`、`event_id` 和当时看到的完整 `log_ids`：

```json
{"device_id": "dev-00042", "event_id": "evt_7f3a...",
 "log_ids": [4000123, 4000125, 4000128, 4000131], "acknowledged_by": "op-1"}
```

* 服务端重新读取这些日志并校验：都属于该设备、都是 critical、构成一段时间间隔不超 5 分钟的连续运行、且前后 5 分钟内没有遗漏的 critical 日志；`event_id` 必须等于这组日志的哈希。
* 校验通过后写入 `fault_event_acks`（主键 `(device_id, event_id)`），重复确认走 `ON CONFLICT DO NOTHING` 并回读，返回 `already_acknowledged: true`，幂等。
* 409 表示事件已变化或参数与实际日志不符，此时不会产生任何确认记录。

### 并发与失败语义

critical 日志写入和确认都会对 `hashtextextended(device_id)` 取**事务级 advisory lock**：

* 确认先取锁、再校验、再写入。取锁时同一设备所有 critical 写入要么已提交（可见），要么仍在进行（此刻睡在锁上），因此确认依据的就是"当时已提交的日志"，不会看到未提交的写入。
* 写入的锁和 INSERT 在同一事务：写入失败（校验拒绝、数据库错误、连接中断、事务回滚）时锁与日志一起消失，事件保持不变，后续确认/写入照常。
* 锁按设备隔离，其他设备不需要等待。非 critical 写入不取该锁，不影响事件。

`fault_event_acks` 建表语句同时写在 `db/init.sql` 和 API 启动时的 `ensure_schema()` 中，已有数据库升级后也能直接获得该表。

## 自测

`tests/selftest.py` 端到端验证：并发去重、构建期写入、无效目录项保护、崩溃→孤儿检测→清理→重试、迟到作业围栏、重启一致性、查询计划命中索引，以及故障事件分组/确认/失效与双实例并发写入交错。

```bash
# Docker 模式（需要本机有 docker）：
docker compose up -d --build
python3 tests/selftest.py --mode docker \
  --dsn "postgresql://ops:ops@localhost:5433/faultlogs"

# 本地模式（无 docker，使用 pgserver 提供的 PostgreSQL 二进制）：
python3 -m venv /tmp/ops-venv && /tmp/ops-venv/bin/pip install -r requirements.txt pgserver
tests/run_local.sh
```

## 验证状态

`tests/selftest.py` 已对两个真实 API 实例 + 真实 PostgreSQL 16.2（400 万行预置数据）完整跑通，**110/110 检查通过**。

* P0–P6（原有）：并发申请去重（12 并发仅 1 次构建、活跃构建后端数恒为 1）、构建期 26 次写入零失败、构建中的无效目录项全程未被删除、就绪前目录校验、崩溃后服务端完成构建的自愈、构建被中断后的无效残件清理与重试、迟到旧作业围栏（UPDATE 命中 0 行）、启动 reconcile 与重启一致性、查询计划命中索引。
* P7（事件与确认）：2 条不成事件 / 3 条成事件、>5 分钟断段、非 critical 不参与；确认成功与跨实例重复确认幂等；回填扩展事件后旧确认立即失效、旧 `event_id` 确认被 409 拒绝；回填桥接合并两个事件后两个旧确认都不再生效；24 小时上限与倒置区间 422；跨窗口边界返回完整事件；8 个并发确认与一次回填写入交错时结果自洽（成功/幂等/409 均不破坏状态）；未提交的 critical 写入会让确认等待、回滚后旧事件可正常确认；已提交的扩展写入使旧确认 409；请求校验失败与数据库级插入失败都不改变事件；设备锁隔离（其他设备不阻塞）；`/logs`、`/logs/plan`、事件查询计划与在线索引状态仍正常。

说明：本交付环境的沙箱没有 Docker，`docker-compose.yml`/`Dockerfile` 未实际构建运行（已做 YAML 语法校验和端口/服务/DSN 检查）；上述验证以 local 模式（pgserver 提供的 PostgreSQL 16.2 + 两个本地 uvicorn 实例）完成，应用代码路径与 docker 模式完全一致。

## 目录

```
app/main.py           FastAPI 接口
app/events.py         故障事件分组、查询与确认（事件标识、设备锁、失效校验）
app/index_manager.py  建索引协调核心（认领/围栏/清理/核对/重启对齐）
app/db.py             连接与 schema 等待
db/init.sql           建表 + 400 万行预置 + 任务台账表 + 确认表
tests/selftest.py     端到端自测（local/docker 两种编排）
tests/local_db.py     本地 pgserver 数据库引导
docker-compose.yml    db(postgres:16) + api1 + api2
```
