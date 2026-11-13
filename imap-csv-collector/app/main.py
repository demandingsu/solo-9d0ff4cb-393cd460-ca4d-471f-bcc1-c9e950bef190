"""FastAPI 应用：采集触发、统一规则发布与统一结果查询。"""
from __future__ import annotations

from typing import Any

from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from . import db, rules
from .collector import collect


@asynccontextmanager
async def lifespan(_app: FastAPI):
    conn = db.connect()
    db.init_db(conn)
    conn.close()
    # 上次进程若在重算中途退出，标记为 failed；已启用规则集与结果不受影响
    rules.recover_interrupted()
    yield


app = FastAPI(
    title="仪器邮件 CSV 采集与统一结果服务",
    description=(
        "从 IMAP 邮箱采集仪器 CSV 附件并入库，按可发布的统一规则"
        "（原检测项目/单位 -> 标准项目/单位 + 正十进制倍率）精确换算结果，"
        "支持按样品编号查询统一结果。"
    ),
    version="2.0.0",
    lifespan=lifespan,
)


class CollectRequest(BaseModel):
    host: str = Field(..., description="IMAP 服务器地址")
    port: int = Field(..., ge=1, le=65535, description="IMAP 端口")
    folder: str = Field(..., description="邮箱文件夹，如 INBOX")
    username: str
    password: str
    use_ssl: bool = Field(False, description="是否使用 IMAP over SSL")


class CollectResponse(BaseModel):
    status: str
    mailbox_key: str
    folder: str
    uidvalidity: int
    resume_from_uid: int
    scanned_uids: list[int]
    ingested: list[dict[str, Any]]
    skipped_duplicates: list[dict[str, Any]]
    stopped: dict[str, Any] | None
    total_new_records: int


class RuleIn(BaseModel):
    source_item: str = Field(..., description="仪器原始检测项目，如 铅Pb")
    source_unit: str = Field(..., description="仪器原始单位，如 mg/kg")
    target_item: str = Field(..., description="标准检测项目，如 铅")
    target_unit: str = Field(..., description="标准单位，如 mg/kg")
    factor: Any = Field(
        1,
        description=(
            "正十进制倍率：标准结果 = 原结果 × 倍率（十进制精确相乘）。"
            "建议传字符串以避免浮点表示误差，如 \"0.001\""
        ),
        examples=["1000", "0.001"],
    )


class PublishRulesRequest(BaseModel):
    rules: list[RuleIn] = Field(..., description="完整规则集；同名同单位不允许重复")
    wait: bool = Field(
        False, description="true 时等待重算完成再返回（演示/测试用，便于确定时序）"
    )


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/collect", response_model=CollectResponse)
def trigger_collect(req: CollectRequest) -> dict[str, Any]:
    report = collect(
        host=req.host,
        port=req.port,
        folder=req.folder,
        username=req.username,
        password=req.password,
        use_ssl=req.use_ssl,
    )
    return report.to_dict()


@app.get("/results/{sample_no}")
def get_results(sample_no: str) -> dict[str, Any]:
    """原始检测记录（永不换算、永不改写，保留邮件来源）。"""
    with db.closing_ready() as conn:
        rows = db.query_results_by_sample(conn, sample_no)
    if not rows:
        raise HTTPException(status_code=404, detail=f"未找到样品 {sample_no} 的结果")
    return {
        "sample_no": sample_no,
        "count": len(rows),
        "results": [dict(row) for row in rows],
    }


@app.get("/samples")
def get_samples() -> dict[str, Any]:
    with db.closing_ready() as conn:
        rows = db.list_samples(conn)
    return {"count": len(rows), "samples": [dict(row) for row in rows]}


@app.get("/ingest/state")
def get_ingest_state() -> dict[str, Any]:
    with db.closing_ready() as conn:
        states = [
            dict(row)
            for row in conn.execute("SELECT * FROM ingest_state ORDER BY mailbox_key")
        ]
    return {"count": len(states), "states": states}


@app.get("/ingest/deliveries")
def get_deliveries() -> dict[str, Any]:
    with db.closing_ready() as conn:
        rows = db.list_deliveries(conn)
    return {"count": len(rows), "deliveries": [dict(row) for row in rows]}


# ---------------------------------------------------------------------------
# 统一规则
# ---------------------------------------------------------------------------


@app.post("/rules/publish")
def publish_rules(req: PublishRulesRequest) -> dict[str, Any]:
    """发布一套新规则并触发全库重算。

    默认后台重算、立即返回，期间采集接口照常可用（按旧规则入库）；
    重算完成并补扫期间新增记录后，才原子切换到新规则集。
    """
    try:
        rule_set_id = rules.publish_rule_set(
            [rule.model_dump() for rule in req.rules],
            wait=req.wait,
        )
    except rules.RuleConflict as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except rules.RuleError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    with db.closing_ready() as conn:
        active_id = db.get_active_rule_set_id(conn)
        summary = rules.rule_set_summary(conn, rule_set_id)
    return {
        "rule_set_id": rule_set_id,
        "status": summary["status"] if summary else "building",
        "rule_count": summary["rule_count"] if summary else len(req.rules),
        "active_rule_set_id": active_id,
        "is_active": rule_set_id == active_id,
        "rule_set": summary,
    }


@app.get("/rulesets")
def get_rule_sets() -> dict[str, Any]:
    with db.closing_ready() as conn:
        active_id = db.get_active_rule_set_id(conn)
        rows = db.list_rule_sets(conn)
    return {
        "active_rule_set_id": active_id,
        "count": len(rows),
        "rule_sets": [
            {**dict(row), "is_active": row["id"] == active_id} for row in rows
        ],
    }


@app.get("/rulesets/{rule_set_id}")
def get_rule_set(rule_set_id: int) -> dict[str, Any]:
    with db.closing_ready() as conn:
        summary = rules.rule_set_summary(conn, rule_set_id)
        if summary is None:
            raise HTTPException(status_code=404, detail=f"规则集 {rule_set_id} 不存在")
        rule_rows = [dict(row) for row in db.get_rules(conn, rule_set_id)]
        active_id = db.get_active_rule_set_id(conn)
    return {
        **summary,
        "is_active": rule_set_id == active_id,
        "rules": rule_rows,
    }


# ---------------------------------------------------------------------------
# 统一结果查询
# ---------------------------------------------------------------------------


def _open_snapshot(conn):
    """建立读事务，保证规则集指针、规则与结果来自同一数据库快照。"""
    conn.execute("BEGIN")
    return conn


@app.get("/unified/results/{sample_no}")
def get_unified_results(sample_no: str) -> dict[str, Any]:
    """按样品查询统一结果。

    只使用当前启用的单一规则集版本；没有规则、缺少映射或结果非数值的记录
    以 status=pending 返回，保留原值及邮件来源（message_id / uid /
    mailbox_key / ingested_at）。
    """
    conn = db.open_ready()
    try:
        _open_snapshot(conn)
        rule_set_id = db.get_active_rule_set_id(conn)
        rows = db.query_unified_results(conn, rule_set_id, sample_no=sample_no)
        summary = rules.rule_set_summary(conn, rule_set_id)
        conn.commit()
    finally:
        conn.close()
    if not rows:
        raise HTTPException(
            status_code=404, detail=f"未找到样品 {sample_no} 的统一结果"
        )
    payload = [dict(row) for row in rows]
    return {
        "sample_no": sample_no,
        "rule_set_id": rule_set_id,
        "rule_set_status": summary["status"] if summary else None,
        "count": len(payload),
        "unified_count": sum(1 for row in payload if row["status"] == "unified"),
        "pending_count": sum(1 for row in payload if row["status"] == "pending"),
        "results": payload,
    }


@app.get("/unified/results")
def list_unified_results(
    status: str | None = None,
    limit: int = 200,
) -> dict[str, Any]:
    """列出当前启用规则集下的统一结果（可按 unified / pending 过滤）。"""
    if status not in (None, rules.STATUS_UNIFIED, rules.STATUS_PENDING):
        raise HTTPException(
            status_code=422, detail="status 只能为 unified 或 pending"
        )
    conn = db.open_ready()
    try:
        _open_snapshot(conn)
        rule_set_id = db.get_active_rule_set_id(conn)
        rows = db.query_unified_results(
            conn, rule_set_id, status=status, limit=limit
        )
        total = db.count_unified_results(conn, rule_set_id, status=status)
        conn.commit()
    finally:
        conn.close()
    return {
        "rule_set_id": rule_set_id,
        "total": total,
        "count": len(rows),
        "results": [dict(row) for row in rows],
    }


@app.get("/unified/samples")
def get_unified_samples() -> dict[str, Any]:
    conn = db.open_ready()
    try:
        _open_snapshot(conn)
        rule_set_id = db.get_active_rule_set_id(conn)
        rows = db.list_unified_samples(conn, rule_set_id)
        conn.commit()
    finally:
        conn.close()
    return {
        "rule_set_id": rule_set_id,
        "count": len(rows),
        "samples": [dict(row) for row in rows],
    }


@app.post("/admin/reset")
def reset_database() -> dict[str, str]:
    """清空全部业务数据（演示/测试用，生产环境请经网关限制访问）。"""
    db.reset_all(db.get_db_path())
    return {"status": "reset"}
