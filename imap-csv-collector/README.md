# 仪器邮件 CSV 采集与统一结果服务

检测中心资料员无需逐封下载邮件：服务通过 IMAP 自动采集仪器邮件中的 CSV 附件，
解析后写入 SQLite 样品结果库；资料员发布“统一规则”后，不同仪器的
`铅Pb` / `Pb`、`mg/kg` / `μg/kg` 等差异被精确换算为标准项目、标准单位，
统计脚本直接查询统一结果。

## 核心保证

### 采集

| 需求 | 实现方式 |
| --- | --- |
| 整封邮件原子入库 | 一封邮件的**全部** CSV 解析成功后，原始检测记录、统一结果、投递记录、UID 进度才在同一个 SQLite 事务提交；任一附件损坏则整封放弃，无部分结果 |
| 失败停在该邮件并返回标识 | 坏附件 / 断线 / 冲突都通过响应的 `stopped` 字段返回 UID、Message-ID、主题和原因，进度不推进 |
| 重启可续采 | 进度持久化在 `ingest_state`，下次从 `last_uid + 1` 开始 |
| UIDVALIDITY 改变后重扫 | 检测到变化即清零进度重新扫描，历史投递记录保留用于去重 |
| 重投识别 | Message-ID + 全部附件的 SHA-256 摘要：同 ID 同摘要跳过；同 ID 不同摘要停采待核实，原结果原样保留 |

### 统一规则

| 需求 | 实现方式 |
| --- | --- |
| 规则键 | 原检测项目 + 原单位 -> 标准项目 + 标准单位 + **正十进制倍率**；同一规则集内键唯一 |
| 精确换算 | `Decimal` 高精度上下文中相乘，不做二进制浮点近似：`0.021 * 1000 = 21`、`1.005 * 1000 = 1005` |
| 待核实 | 没有匹配规则（`no_rule`）、项目存在但单位未映射（`unit_unmapped`）、结果非数值（`non_numeric`）三类记录 `status=pending`，**统一字段保留原项目/原值/原单位**，并保留 `message_id` / `uid` / `mailbox_key` / `ingested_at` 邮件来源 |
| 发布即重算 | 每次发布生成新规则集版本，后台分块重算全库历史原始记录，接口立即返回 |
| 采集不被重算阻塞 | 重算期间采集照常入库，统一结果按**当前已启用（旧）规则**写入并立即可查；采集只加自己的短事务，不与整库重算抢长锁 |
| 启用覆盖期间新增 | 重算结束后，在**同一个写事务**中补扫期间新增记录并切换 `rule_state` 指针；此后入库的记录按新规则换算 |
| 一次查询一套规则 | 查询在 SQLite 读事务快照中读取“启用规则集指针 + 规则 + 结果”，切换瞬间也不会混用两套规则 |
| 失败保持旧视图 | 新规则集重算失败标记 `failed`（含 `last_error`）；旧规则集保持 `active`，其完整结果继续可查 |
| 重启保持状态 | 规则集、规则、统一结果全部持久化；启动时把上次进程遗留的 `building/recomputing` 版本标记为 `failed`，已启用版本不受影响 |
| 原始记录不变 | 重算/发布只读 `test_result`，不改写原始记录、`mail_delivery` 与 `ingest_state`，重投识别语义不变 |

内置 **0 号空规则集**：首次发布前处于启用状态，所有记录都以 `pending/no_rule`
挂在它下面。统一查询从第一天起就是完整、可解释的视图，而不是空表。

## 目录结构

```
app/                 FastAPI + 采集核心 + 规则引擎 + SQLite 持久层
  main.py            HTTP 路由（采集 / 规则发布 / 统一结果查询）
  collector.py       IMAP 采集、重投识别、原子事务边界
  rules.py           规则校验、Decimal 精确换算、分块重算与原子启用
  csv_parse.py       CSV 校验解析（表头：样品编号,检测项目,结果,单位）
  db.py              SQLite 建表、迁移与查询
imap_server/         本地 IMAP 样例服务（纯标准库实现 IMAP4rev1 子集）
  server.py          IMAP 协议服务，支持 FLAKY_FETCH_AFTER 断线注入
  mailbox.py         文件邮箱、附件替换、邮箱重建
  seed.py            演示种子：两封正常邮件 + 一封多附件坏 CSV 邮件
  cli.py             邮箱控制 CLI（重投/重建/新增邮件）
scripts/
  seed_unified.py    imap 容器启动时追加“项目名/单位不统一”的演示邮件
  demo.sh            原有采集/重投/断线端到端演示
  demo_unified.sh    统一规则发布与查询演示
tests/               33 个 pytest 用例
docker-compose.yml   api + imap 两个服务 + mailctl 控制容器
```

## 一键启动（Docker Compose）

```bash
docker compose up -d --build
```

- API 文档： http://localhost:8000/docs
- IMAP 样例服务： localhost:10143（明文 IMAP，账号 `instrument@lab.local` / `labpass`）
- SQLite 数据与邮箱分别持久化在 `db_data`、`mail_data` 卷

统一规则演示（重算期间采集、期间新增记录补扫、重启保持）：

```bash
bash scripts/demo_unified.sh
```

原有采集/重投/断线演示：

```bash
bash scripts/demo.sh
```

演示编排给 api 设了 `RULE_RECOMPUTE_CHUNK_SIZE=1` 与
`RULE_RECOMPUTE_PAUSE_SECONDS=1`，把重算放慢到肉眼可见；
生产环境删除这两个变量即可（默认每批 500 条、无暂停）。

## API

### POST /collect — 触发一轮采集

```bash
curl -X POST localhost:8000/collect -H 'Content-Type: application/json' -d '{
  "host": "imap",
  "port": 10143,
  "folder": "INBOX",
  "username": "instrument@lab.local",
  "password": "labpass",
  "use_ssl": false
}'
```

响应（正常完成 `status=completed`；停采时 `status=stopped` 并带 `stopped` 邮件标识）：

```json
{
  "status": "stopped",
  "mailbox_key": "imap|10143|INBOX",
  "uidvalidity": 1794581572,
  "resume_from_uid": 3,
  "scanned_uids": [1, 2, 3],
  "ingested": [ {"uid": 1, "message_id": "inst-0001@lab-instrument", "record_count": 3, "...": "..."} ],
  "skipped_duplicates": [],
  "stopped": {
    "reason": "坏 CSV 附件，整封邮件不入库: 附件 batch_b3_bad.csv: 第 3 行结果为空",
    "uid": 3,
    "message_id": "inst-0003@lab-instrument",
    "subject": "仪器结果 批次 B3（含损坏附件）"
  },
  "total_new_records": 5
}
```

### POST /rules/publish — 发布统一规则

```bash
curl -X POST localhost:8000/rules/publish -H 'Content-Type: application/json' -d '{
  "rules": [
    {"source_item": "铅Pb", "source_unit": "mg/kg", "target_item": "铅", "target_unit": "μg/kg", "factor": "1000"},
    {"source_item": "Pb",   "source_unit": "mg/kg", "target_item": "铅", "target_unit": "μg/kg", "factor": "1000"}
  ],
  "wait": false
}'
```

- `factor` 接受字符串或数字，必须是正的有限十进制数；按十进制精确相乘。
- 每次发布是一套**完整**新规则集（不是增量补丁），同名同单位不允许重复。
- `wait=true` 时同步等待重算完成，响应中可直接看到 `status=active`；
  默认 `wait=false` 后台重算，立即返回 `building`，重算期间采集/查询不受影响。
- 已有规则集正在 `building/recomputing` 时再次发布返回 `409`。
- 重算失败时该规则集为 `failed`（`last_error` 说明原因），其部分结果会被
  清除，避免出现“半套规则”的假统一视图；旧规则集仍是唯一完整视图。
  `wait=true` 的发布依然返回 `200`，请以响应中的 `status` 判断结果。

响应：

```json
{
  "rule_set_id": 1,
  "status": "building",
  "rule_count": 2,
  "active_rule_set_id": 0,
  "is_active": false,
  "rule_set": {"id": 1, "status": "building", "processed_rows": 0, "total_rows": 12, "...": "..."}
}
```

### GET /unified/results/{样品编号} — 统一结果

```bash
curl localhost:8000/unified/results/S-1001
```

```json
{
  "sample_no": "S-1001",
  "rule_set_id": 1,
  "rule_set_status": "active",
  "count": 3,
  "unified_count": 2,
  "pending_count": 1,
  "results": [
    {
      "sample_no": "S-1001",
      "item": "铅",
      "result": "21",
      "unit": "μg/kg",
      "status": "unified",
      "reason": null,
      "source_item": "铅Pb",
      "source_unit": "mg/kg",
      "source_result": "0.021",
      "message_id": "inst-0001@lab-instrument",
      "uid": 1,
      "mailbox_key": "imap|10143|INBOX",
      "ingested_at": "2026-11-13T10:00:00+00:00"
    },
    {
      "sample_no": "S-1001",
      "item": "砷As",
      "result": "0.012",
      "unit": "mg/kg",
      "status": "pending",
      "reason": "no_rule",
      "source_item": "砷As",
      "source_unit": "mg/kg",
      "source_result": "0.012",
      "message_id": "inst-0001@lab-instrument",
      "uid": 1,
      "mailbox_key": "imap|10143|INBOX",
      "ingested_at": "2026-11-13T10:00:00+00:00"
    }
  ]
}
```

`reason` 取值：`no_rule`（没有该项目规则）、`unit_unmapped`（该项目有规则
但没有当前单位的映射）、`non_numeric`（结果不是数值）。待核实记录的
`result` / `unit` / `item` 保留原始值，绝不猜测换算。

### 其他端点

| 端点 | 说明 |
| --- | --- |
| `GET /unified/results?status=unified\|pending` | 当前规则集下的统一结果列表 |
| `GET /unified/samples` | 当前规则集下各样品统一/待核实数量 |
| `GET /rulesets` | 规则集版本、状态、重算进度与当前启用版本 |
| `GET /rulesets/{id}` | 单个规则集及其规则明细 |
| `GET /results/{样品编号}` | **原始**检测记录（不换算，保留邮件来源） |
| `GET /samples` | 已入库样品汇总（原始记录） |
| `GET /ingest/state` | 各邮箱 UIDVALIDITY 与 last_uid 进度 |
| `GET /ingest/deliveries` | 已处理投递（Message-ID、附件摘要） |
| `GET /health` | 健康检查 |
| `POST /admin/reset` | 清空结果库（演示用） |

## CSV 约定

UTF-8（可带 BOM），首行表头固定为 `样品编号,检测项目,结果,单位`，
四个字段均不允许为空，否则该附件所在整封邮件判定失败。
`结果` 可以是数值或 `<0.01` 这类检出限表述；后者在规则换算时标待核实，
原值仍完整保留。

## 本地无 Docker 运行 / 测试

```bash
python -m venv .venv && source .venv/bin/activate   # 或使用已有解释器
pip install -r requirements.txt pytest httpx
PYTHONPATH=. pytest -q

# 手动起两个进程
python -m imap_server.cli serve --host 127.0.0.1 --port 10143
COLLECTOR_DB=./data/collector.db uvicorn app.main:app --port 8000
```

## 设计说明与边界

- 采集按邮箱文件夹维度维护进度，`mailbox_key = host|port|folder`。
- 附件摘要对「文件名 + 文件内容 SHA-256」排序后再做一次 SHA-256，
  附件顺序变化不影响判定，内容或文件名变化即视为不同投递。
- 统一结果按规则集版本单独存储（`unified_result.rule_set_id`），旧版本结果
  在版本被取代后仍保留在库中，便于审计；未启用版本的结果不会被查询读到。
- 重算按 `RULE_RECOMPUTE_CHUNK_SIZE` 分块，每块一个短写事务；不会长时间
  锁库，采集写入可穿插进行。
- 发布期间（`building/recomputing`）拒绝再次发布，避免两套版本竞争启用。
- 进程内用线程锁串行化重算；多副本部署时请只让一个 worker 执行发布，
  或改用外部锁（`rule_set` 的状态行可作为乐观锁基础）。
- 演示故障注入：`RULE_RECOMPUTE_FAIL_AFTER=N` 在第 N 批后中断重算，
  可直观看到旧规则集继续服务；`RULE_RECOMPUTE_PAUSE_SECONDS` 用于放慢重算。
- 样例 IMAP 服务仅实现采集所需命令（LOGIN/SELECT/STATUS/UID SEARCH/UID FETCH 等），
  面向演示与测试，不可作为生产邮箱使用。
- `/collect` 在单进程内加锁串行化；生产多副本部署时应改用分布式锁或单 worker。
