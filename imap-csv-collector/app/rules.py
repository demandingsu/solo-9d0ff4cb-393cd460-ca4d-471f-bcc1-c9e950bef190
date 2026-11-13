"""统一规则集：发布、精确换算、分块重算与启用。

语义要点
--------
* 规则以（原检测项目, 原单位）为键，映射到（标准项目, 标准单位）并给出
  正十进制倍率；换算使用 Decimal 精确相乘，不做浮点舍入。
* 每次发布生成新的规则集版本。先写入规则集（building），再分块为全库
  历史原始记录生成统一结果；全部完成后在**同一个写事务**里补扫期间新增
  记录并切换 rule_state 指针，因此任何一次查询只会看到一套规则。
* 重算期间 rule_state 仍指向旧规则集：采集写入统一结果时读取的是旧规则；
  旧规则集的结果完整可查。重算失败只把新规则集标记为 failed，旧规则集
  及其结果原样保留。
* 原始记录（test_result）、投递识别（mail_delivery）、采集进度
  （ingest_state）在重算过程中不被修改。
"""
from __future__ import annotations

import os
import re
import threading
from decimal import Decimal, InvalidOperation, localcontext
from typing import Iterable, Sequence

from . import db

STATUS_UNIFIED = "unified"
STATUS_PENDING = "pending"

REASON_NO_RULE = "no_rule"
REASON_UNMAPPED_UNIT = "unit_unmapped"
REASON_NON_NUMERIC = "non_numeric"

REASON_TEXT = {
    REASON_NO_RULE: "没有该检测项目的统一规则，保留原值待核实",
    REASON_UNMAPPED_UNIT: "该项目缺少当前单位的映射，保留原值待核实",
    REASON_NON_NUMERIC: "结果不是数值，无法按规则换算，保留原值待核实",
}

_NUMERIC_RE = re.compile(r"^[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?$")

# 防止超长数字串造成 Decimal 运算量爆炸；超长一律按“非数值”待核实
MAX_NUMERIC_LENGTH = 64

_RECOMPUTE_LOCK = threading.Lock()
# 本进程内正在重算的规则集：重启恢复时不能把它们误判为“中断遗留”
_ACTIVE_RECOMPUTES: set[int] = set()
_ACTIVE_RECOMPUTES_LOCK = threading.Lock()

# 仅用于演示/测试的故障与节奏注入（与 IMAP 的 FLAKY_FETCH_AFTER 同风格）：
#   RULE_RECOMPUTE_CHUNK_SIZE  每批重算的记录数，默认 500
#   RULE_RECOMPUTE_PAUSE_SECONDS  每批之间暂停秒数，默认 0
#   RULE_RECOMPUTE_FAIL_AFTER  第 N 批后抛错，模拟重算失败，默认 0（不注入）
DEFAULT_CHUNK_SIZE = 500


class RuleError(ValueError):
    """规则定义或发布流程错误。"""


class RuleConflict(RuleError):
    """已有规则集正在重算，拒绝并发发布。"""


def parse_numeric(text: str) -> Decimal | None:
    stripped = (text or "").strip()
    if len(stripped) > MAX_NUMERIC_LENGTH:
        return None
    if not _NUMERIC_RE.match(stripped):
        return None
    try:
        value = Decimal(stripped)
    except InvalidOperation:
        return None
    if not value.is_finite():
        return None
    return value


def multiply_exact(left: Decimal, right: Decimal) -> Decimal:
    """高精度上下文下相乘，保证十进制结果为精确值而非舍入值。"""
    lt = left.as_tuple()
    rt = right.as_tuple()
    prec = (
        max(34, len(lt.digits) + len(rt.digits))
        + abs(lt.exponent)
        + abs(rt.exponent)
        + 10
    )
    with localcontext() as ctx:
        ctx.prec = prec
        return left * right


def format_decimal(value: Decimal) -> str:
    """定点格式输出，去掉无意义的尾随零（21.000 -> 21）。"""
    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    if text in ("", "-0"):
        text = "0"
    return text


def normalize_factor(raw: object) -> str:
    """校验并规范化倍率：必须是正的有限十进制数。"""
    if isinstance(raw, bool):
        raise RuleError(f"倍率必须是十进制数，收到 {raw!r}")
    if isinstance(raw, Decimal):
        value = raw
    elif isinstance(raw, int):
        value = Decimal(raw)
    elif isinstance(raw, float):
        # 浮点入参按最短可回读表示转换，避免 0.1 -> 0.1000000000000000055...
        value = Decimal(repr(raw))
    elif isinstance(raw, str):
        try:
            value = Decimal(raw.strip())
        except InvalidOperation as exc:
            raise RuleError(f"倍率不是合法的十进制数: {raw!r}") from exc
    else:
        raise RuleError(f"倍率类型不支持: {type(raw).__name__}")
    if not value.is_finite():
        raise RuleError("倍率必须是有限十进制数")
    if value <= 0:
        raise RuleError("倍率必须大于 0")
    if value.adjusted() > 100 or value.adjusted() < -100:
        raise RuleError("倍率数量级超出合理范围")
    return format_decimal(value)


def normalize_rules(rules: Iterable[dict]) -> list[tuple[str, str, str, str, str]]:
    """把 API 输入规范化为 (source_item, source_unit, target_item, target_unit, factor)。"""
    normalized: list[tuple[str, str, str, str, str]] = []
    seen: dict[tuple[str, str], int] = {}
    for index, raw in enumerate(rules):
        if not isinstance(raw, dict):
            raise RuleError(f"第 {index + 1} 条规则格式错误")
        source_item = str(raw.get("source_item") or "").strip()
        source_unit = str(raw.get("source_unit") or "").strip()
        target_item = str(raw.get("target_item") or "").strip()
        target_unit = str(raw.get("target_unit") or "").strip()
        if not source_item:
            raise RuleError(f"第 {index + 1} 条规则缺少原检测项目")
        if not source_unit:
            raise RuleError(f"第 {index + 1} 条规则缺少原单位")
        if not target_item:
            raise RuleError(f"第 {index + 1} 条规则缺少标准项目")
        if not target_unit:
            raise RuleError(f"第 {index + 1} 条规则缺少标准单位")
        key = (source_item, source_unit)
        if key in seen:
            raise RuleError(
                f"第 {index + 1} 条规则与第 {seen[key]} 条的"
                f"原检测项目+原单位重复: {source_item}/{source_unit}"
            )
        seen[key] = index + 1
        factor = normalize_factor(raw.get("factor"))
        normalized.append(
            (source_item, source_unit, target_item, target_unit, factor)
        )
    return normalized


def load_rule_map(
    conn, rule_set_id: int
) -> dict[tuple[str, str], tuple[str, str, Decimal]]:
    result: dict[tuple[str, str], tuple[str, str, Decimal]] = {}
    for row in db.get_rules(conn, rule_set_id):
        result[(row["source_item"], row["source_unit"])] = (
            row["target_item"],
            row["target_unit"],
            Decimal(row["factor"]),
        )
    return result


def build_unified_rows(
    rule_set_id: int,
    raw_rows: Sequence,
    rule_map: dict[tuple[str, str], tuple[str, str, Decimal]],
) -> list[tuple]:
    """把原始记录换算为统一结果行（含待核实行）。"""
    rows: list[tuple] = []
    known_items = {item for item, _unit in rule_map}
    for raw in raw_rows:
        source_item = raw["test_item"]
        source_unit = raw["unit"]
        source_result = raw["result"]
        base = (
            rule_set_id,
            raw["id"],
            raw["sample_no"],
            source_item,
            source_unit,
            source_result,
            raw["message_id"],
            raw["uid"],
            raw["mailbox_key"],
            raw["ingested_at"],
        )
        rule = rule_map.get((source_item, source_unit))
        if rule is None:
            # 无精确映射：统一字段整体保留原项目/原值/原单位，标待核实。
            # 项目已存在但单位未映射时给出更精确的原因。
            reason = (
                REASON_UNMAPPED_UNIT
                if source_item in known_items
                else REASON_NO_RULE
            )
            rows.append(
                base
                + (
                    source_item,
                    source_result,
                    source_unit,
                    STATUS_PENDING,
                    reason,
                )
            )
            continue
        target_item, target_unit, factor = rule
        numeric = parse_numeric(source_result)
        if numeric is None:
            # 有映射但结果非数值：不臆造换算值，原值/原单位保留待核实
            rows.append(
                base
                + (
                    source_item,
                    source_result,
                    source_unit,
                    STATUS_PENDING,
                    REASON_NON_NUMERIC,
                )
            )
            continue
        converted = format_decimal(multiply_exact(numeric, factor))
        rows.append(
            base + (target_item, converted, target_unit, STATUS_UNIFIED, None)
        )
    return rows


def _recompute_settings() -> tuple[int, float, int]:
    chunk = int(os.environ.get("RULE_RECOMPUTE_CHUNK_SIZE", "0") or 0)
    if chunk <= 0:
        chunk = DEFAULT_CHUNK_SIZE
    try:
        pause = float(os.environ.get("RULE_RECOMPUTE_PAUSE_SECONDS", "0") or 0)
    except ValueError:
        pause = 0.0
    fail_after = int(os.environ.get("RULE_RECOMPUTE_FAIL_AFTER", "0") or 0)
    return chunk, max(0.0, pause), max(0, fail_after)


def _sweep_chunk(
    conn, rule_set_id: int, rule_map, chunk_size: int, maximum_id: int
) -> int:
    """在独立的短写事务中处理一批未换算记录，返回本批条数。"""
    with db.transaction(conn, immediate=True):
        pending = db.fetch_unprocessed_raw_results(
            conn, rule_set_id, limit=chunk_size, maximum_id=maximum_id
        )
        if pending:
            db.insert_unified_results(
                conn, build_unified_rows(rule_set_id, pending, rule_map)
            )
    return len(pending)


def recompute_rule_set(
    rule_set_id: int,
    *,
    db_path: str | None = None,
    chunk_size: int | None = None,
    pause_seconds: float | None = None,
    fail_after_chunks: int | None = None,
) -> None:
    """为指定规则集重算全库统一结果并启用。

    失败时抛出异常并把规则集标记为 failed；旧规则集保持 active，
    其统一结果完整可查。成功与最终补扫/切换在同一写事务内完成。
    """
    env_chunk, env_pause, env_fail = _recompute_settings()
    chunk = chunk_size if chunk_size and chunk_size > 0 else env_chunk
    pause = env_pause if pause_seconds is None else max(0.0, pause_seconds)
    fail_after = env_fail if fail_after_chunks is None else max(0, fail_after_chunks)

    conn = db.connect(db_path)
    db.init_db(conn)
    with _ACTIVE_RECOMPUTES_LOCK:
        _ACTIVE_RECOMPUTES.add(rule_set_id)
    try:
        rule_map = load_rule_map(conn, rule_set_id)
        total = db.count_raw_results(conn)
        # 分块阶段只扫发布时已存在的记录；期间新增记录由最终补扫覆盖，
        # 这样即使采集持续不断，重算循环也一定能结束。
        maximum_id = db.max_raw_result_id(conn)
        with db.transaction(conn, immediate=True):
            db.mark_rule_set_status(
                conn,
                rule_set_id,
                "recomputing",
                started_at=db._utcnow(),
                error="",
                finished_at="",
            )
            db.update_rule_set_progress(
                conn, rule_set_id, processed_rows=0, total_rows=total
            )

        processed = 0
        batches = 0
        while True:
            done = _sweep_chunk(
                conn, rule_set_id, rule_map, chunk, maximum_id
            )
            if done == 0:
                break
            processed += done
            batches += 1
            with db.transaction(conn, immediate=True):
                db.update_rule_set_progress(
                    conn,
                    rule_set_id,
                    processed_rows=min(processed, total),
                    total_rows=total,
                )
            if fail_after and batches >= fail_after:
                raise RuntimeError(
                    f"重算在第 {batches} 批后按注入要求中断（演示/测试）"
                )
            if pause:
                threading.Event().wait(pause)

        # 最终补扫 + 启用：同一写事务，期间新增记录不会漏掉；
        # 采集要么在事务前写入旧规则集（这里补扫），要么在事务后按新规则入库。
        with db.transaction(conn, immediate=True):
            remaining = db.fetch_unprocessed_raw_results(conn, rule_set_id)
            if remaining:
                db.insert_unified_results(
                    conn, build_unified_rows(rule_set_id, remaining, rule_map)
                )
                processed += len(remaining)
            total = db.count_raw_results(conn)
            db.update_rule_set_progress(
                conn,
                rule_set_id,
                processed_rows=total,
                total_rows=total,
            )
            db.activate_rule_set(conn, rule_set_id)
            db.mark_rule_set_status(
                conn,
                rule_set_id,
                "active",
                error="",
                finished_at=db._utcnow(),
            )
    except Exception as exc:
        try:
            with db.transaction(conn, immediate=True):
                # 未启用版本的部分结果一律清掉：失败版本不应留下
                # 看似可用的“半套规则”结果，旧版本才是唯一完整视图
                db.delete_unified_results_for_set(conn, rule_set_id)
                db.mark_rule_set_status(
                    conn,
                    rule_set_id,
                    "failed",
                    error=str(exc) or exc.__class__.__name__,
                    finished_at=db._utcnow(),
                )
        except Exception:
            pass
        raise
    finally:
        with _ACTIVE_RECOMPUTES_LOCK:
            _ACTIVE_RECOMPUTES.discard(rule_set_id)
        conn.close()


def publish_rule_set(
    rules: Iterable[dict],
    *,
    db_path: str | None = None,
    wait: bool = False,
) -> int:
    """发布新规则集并（可选）同步重算，返回规则集 id。

    重算默认在后台线程进行：HTTP 请求立即返回，采集接口不受影响。
    """
    normalized = normalize_rules(rules)
    conn = db.connect(db_path)
    db.init_db(conn)
    try:
        with db.transaction(conn, immediate=True):
            busy = conn.execute(
                """
                SELECT id, status FROM rule_set
                WHERE status IN ('building', 'recomputing')
                ORDER BY id DESC LIMIT 1
                """
            ).fetchone()
            if busy is not None:
                raise RuleConflict(
                    f"规则集 {busy['id']} 正在{busy['status']}，"
                    "请等待完成后再发布"
                )
            rule_set_id = db.insert_rule_set(conn, status="building")
            db.insert_rules(conn, rule_set_id, normalized)
    finally:
        conn.close()

    if wait:
        try:
            with _RECOMPUTE_LOCK:
                recompute_rule_set(rule_set_id, db_path=db_path)
        except Exception:
            # 失败原因已持久化到 rule_set.last_error；旧规则集保持启用
            pass
    else:
        thread = threading.Thread(
            target=_recompute_in_background,
            args=(rule_set_id, db_path),
            name=f"rule-recompute-{rule_set_id}",
            daemon=True,
        )
        thread.start()
    return rule_set_id


def _recompute_in_background(rule_set_id: int, db_path: str | None) -> None:
    try:
        with _RECOMPUTE_LOCK:
            recompute_rule_set(rule_set_id, db_path=db_path)
    except Exception:
        # 失败状态已由 recompute_rule_set 持久化，旧规则集不受影响
        pass


def recompute_async(
    rule_set_id: int, *, db_path: str | None = None
) -> threading.Thread:
    thread = threading.Thread(
        target=_recompute_in_background,
        args=(rule_set_id, db_path),
        name=f"rule-recompute-{rule_set_id}",
        daemon=True,
    )
    thread.start()
    return thread


def recover_interrupted(db_path: str | None = None) -> int:
    """服务重启后，把不可能再继续的重算标记为 failed。

    旧（已启用）规则集与统一结果保持不变；新规则集需重新发布。
    """
    conn = db.connect(db_path)
    db.init_db(conn)
    recovered = 0
    try:
        with _ACTIVE_RECOMPUTES_LOCK:
            active_now = set(_ACTIVE_RECOMPUTES)
        with db.transaction(conn, immediate=True):
            rows = conn.execute(
                """
                SELECT id FROM rule_set
                WHERE status IN ('building', 'recomputing')
                """
            ).fetchall()
            for row in rows:
                if row["id"] in active_now:
                    continue
                db.mark_rule_set_status(
                    conn,
                    row["id"],
                    "failed",
                    error="服务重启，重算中断；请重新发布规则",
                    finished_at=db._utcnow(),
                )
                recovered += 1
    finally:
        conn.close()
    return recovered


def rule_set_summary(conn, rule_set_id: int) -> dict | None:
    if rule_set_id is None:
        return None
    row = db.get_rule_set(conn, rule_set_id)
    if row is None:
        return None
    return {
        "id": row["id"],
        "status": row["status"],
        "created_at": row["created_at"],
        "activated_at": row["activated_at"],
        "recompute_started_at": row["recompute_started_at"],
        "recompute_finished_at": row["recompute_finished_at"],
        "last_error": row["last_error"],
        "rule_count": row["rule_count"],
        "processed_rows": row["processed_rows"],
        "total_rows": row["total_rows"],
    }
