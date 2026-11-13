"""SQLite 持久层。

表说明：
  ingest_state     每个邮箱文件夹的采集进度（UIDVALIDITY / 最后处理 UID）
  mail_delivery    已处理邮件投递（Message-ID + 附件摘要，去重/冲突识别）
  test_result      原始检测记录（仪器原始项目/结果/单位，永不改写）
  rule_set         统一规则集版本及其重算进度/状态
  rule             规则集内的映射：原项目+原单位 -> 标准项目/单位/倍率
  unified_result   已换算的统一结果（按规则集版本持久化，引用原始记录）
  rule_state       当前启用的规则集（单行）
"""
from __future__ import annotations

import json
import os
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Iterable, Iterator, Sequence

DEFAULT_DB_PATH = "/workspace/imap-csv-collector/data/collector.db"

# 单进程内串行化采集，避免两个请求并发推进同一邮箱进度
ingest_lock = threading.Lock()


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def get_db_path() -> str:
    return os.environ.get("COLLECTOR_DB", DEFAULT_DB_PATH)


def connect(db_path: str | None = None) -> sqlite3.Connection:
    path = db_path or get_db_path()
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    conn = sqlite3.connect(path, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    return conn


def open_ready(db_path: str | None = None) -> sqlite3.Connection:
    """建立连接并确保表结构就绪（幂等）；读取端点可安全使用。"""
    conn = connect(db_path)
    init_db(conn)
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS ingest_state (
            mailbox_key   TEXT PRIMARY KEY,
            uidvalidity   INTEGER NOT NULL,
            last_uid      INTEGER NOT NULL,
            updated_at    TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS mail_delivery (
            id                INTEGER PRIMARY KEY AUTOINCREMENT,
            mailbox_key       TEXT NOT NULL,
            message_id        TEXT NOT NULL,
            attachment_digest TEXT NOT NULL,
            first_uid         INTEGER NOT NULL,
            uidvalidity       INTEGER NOT NULL,
            ingested_at       TEXT NOT NULL,
            UNIQUE (mailbox_key, message_id)
        );

        CREATE TABLE IF NOT EXISTS test_result (
            id                INTEGER PRIMARY KEY AUTOINCREMENT,
            mailbox_key       TEXT NOT NULL,
            message_id        TEXT NOT NULL,
            uid               INTEGER NOT NULL,
            sample_no         TEXT NOT NULL,
            test_item         TEXT NOT NULL,
            result            TEXT NOT NULL,
            unit              TEXT NOT NULL,
            ingested_at       TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS rule_set (
            id                INTEGER PRIMARY KEY AUTOINCREMENT,
            status            TEXT NOT NULL DEFAULT 'building',
            created_at        TEXT NOT NULL,
            activated_at      TEXT,
            recompute_started_at TEXT,
            recompute_finished_at TEXT,
            last_error        TEXT,
            rule_count        INTEGER NOT NULL DEFAULT 0,
            processed_rows    INTEGER NOT NULL DEFAULT 0,
            total_rows        INTEGER NOT NULL DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS rule (
            id                INTEGER PRIMARY KEY AUTOINCREMENT,
            rule_set_id       INTEGER NOT NULL REFERENCES rule_set(id),
            source_item       TEXT NOT NULL,
            source_unit       TEXT NOT NULL,
            target_item       TEXT NOT NULL,
            target_unit       TEXT NOT NULL,
            factor            TEXT NOT NULL,
            UNIQUE (rule_set_id, source_item, source_unit)
        );

        CREATE TABLE IF NOT EXISTS unified_result (
            id                INTEGER PRIMARY KEY AUTOINCREMENT,
            rule_set_id       INTEGER NOT NULL REFERENCES rule_set(id),
            test_result_id    INTEGER NOT NULL REFERENCES test_result(id),
            sample_no         TEXT NOT NULL,
            source_item       TEXT NOT NULL,
            source_unit       TEXT NOT NULL,
            source_result     TEXT NOT NULL,
            message_id        TEXT NOT NULL,
            uid               INTEGER NOT NULL,
            mailbox_key       TEXT NOT NULL,
            ingested_at       TEXT NOT NULL,
            item              TEXT NOT NULL,
            result            TEXT,
            unit              TEXT,
            status            TEXT NOT NULL,
            reason            TEXT,
            UNIQUE (rule_set_id, test_result_id)
        );

        CREATE TABLE IF NOT EXISTS rule_state (
            id            INTEGER PRIMARY KEY CHECK (id = 1),
            rule_set_id   INTEGER NOT NULL REFERENCES rule_set(id),
            activated_at  TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_test_result_sample
            ON test_result (sample_no);
        CREATE INDEX IF NOT EXISTS idx_test_result_message
            ON test_result (mailbox_key, message_id);
        CREATE INDEX IF NOT EXISTS idx_delivery_mailbox
            ON mail_delivery (mailbox_key);
        CREATE INDEX IF NOT EXISTS idx_unified_rule_set
            ON unified_result (rule_set_id);
        CREATE INDEX IF NOT EXISTS idx_unified_rule_sample
            ON unified_result (rule_set_id, sample_no);
        CREATE INDEX IF NOT EXISTS idx_unified_rule_status
            ON unified_result (rule_set_id, status);
        """
    )
    _migrate_schema(conn)
    _ensure_bootstrap_rule_set(conn)
    _backfill_legacy_results(conn)
    conn.commit()


def _migrate_schema(conn: sqlite3.Connection) -> None:
    """为已存在的旧库补齐新增列（新库建表时已包含）。"""
    columns = {
        row["name"]
        for row in conn.execute("PRAGMA table_info(rule_set)")
    }
    for name, ddl in (
        ("processed_rows", "INTEGER NOT NULL DEFAULT 0"),
        ("total_rows", "INTEGER NOT NULL DEFAULT 0"),
        ("recompute_started_at", "TEXT"),
        ("recompute_finished_at", "TEXT"),
        ("last_error", "TEXT"),
        ("rule_count", "INTEGER NOT NULL DEFAULT 0"),
    ):
        if name not in columns:
            conn.execute(f"ALTER TABLE rule_set ADD COLUMN {name} {ddl}")


def _ensure_bootstrap_rule_set(conn: sqlite3.Connection) -> None:
    """建立 0 号空规则集并作为初始启用版本。

    它没有任何规则，因此所有既有/新增记录都会以“待核实（无匹配规则）”
    出现在统一结果中——这是可查询的完整视图，而不是空表。
    """
    now = _utcnow()
    conn.execute(
        """
        INSERT OR IGNORE INTO rule_set
            (id, status, created_at, activated_at,
             recompute_started_at, recompute_finished_at,
             rule_count, processed_rows, total_rows)
        VALUES (0, 'active', ?, ?, ?, ?, 0, 0, 0)
        """,
        (now, now, now, now),
    )
    conn.execute(
        "INSERT OR IGNORE INTO rule_state (id, rule_set_id, activated_at)"
        " VALUES (1, 0, ?)",
        (now,),
    )


def _backfill_legacy_results(conn: sqlite3.Connection) -> None:
    """旧库升级：为历史原始记录补上当前启用规则集下的待核实行。

    仅对“从未写入任何统一结果”的记录生效；正常情况下采集与重算始终在
    同一事务写入原始记录与统一结果。使用 LIMIT 分批，避免启动时长时间
    锁库（只用于一次性升级）。
    """
    rule_set_id = get_active_rule_set_id(conn)
    # 常见路径完全没有待补记录：先用只读查询判定，避免每次建连都抢写锁
    probe = conn.execute(
        """
        SELECT 1
        FROM test_result AS t
        WHERE NOT EXISTS (
            SELECT 1 FROM unified_result AS u
            WHERE u.rule_set_id = ? AND u.test_result_id = t.id
        )
        LIMIT 1
        """,
        (rule_set_id,),
    ).fetchone()
    if probe is None:
        return
    while True:
        inserted = conn.execute(
            """
            INSERT INTO unified_result
                (rule_set_id, test_result_id, sample_no, source_item, source_unit,
                 source_result, message_id, uid, mailbox_key, ingested_at,
                 item, result, unit, status, reason)
            SELECT ?, id, sample_no, test_item, unit, result, message_id, uid,
                   mailbox_key, ingested_at, test_item, result, unit,
                   'pending', 'no_rule'
            FROM test_result AS t
            WHERE NOT EXISTS (
                SELECT 1 FROM unified_result AS u
                WHERE u.rule_set_id = ? AND u.test_result_id = t.id
            )
            LIMIT 500
            """,
            (rule_set_id, rule_set_id),
        )
        if inserted.rowcount <= 0:
            break


@contextmanager
def closing_ready(db_path: str | None = None) -> Iterator[sqlite3.Connection]:
    """表结构就绪的短连接（读取端点用，结束后关闭）。"""
    conn = open_ready(db_path)
    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def transaction(
    conn: sqlite3.Connection, *, immediate: bool = False
) -> Iterator[sqlite3.Connection]:
    """显式事务边界。

    immediate=True 时先取写锁（BEGIN IMMEDIATE），避免
    “先读后写”在并发下发生快照升级失败；重算分块与采集入库都使用它。
    """
    started = False
    if immediate and not conn.in_transaction:
        conn.execute("BEGIN IMMEDIATE")
        started = True
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def get_state(conn: sqlite3.Connection, mailbox_key: str) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM ingest_state WHERE mailbox_key = ?", (mailbox_key,)
    ).fetchone()


def upsert_state(
    conn: sqlite3.Connection,
    mailbox_key: str,
    uidvalidity: int,
    last_uid: int,
) -> None:
    conn.execute(
        """
        INSERT INTO ingest_state (mailbox_key, uidvalidity, last_uid, updated_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(mailbox_key) DO UPDATE SET
            uidvalidity = excluded.uidvalidity,
            last_uid    = excluded.last_uid,
            updated_at  = excluded.updated_at
        """,
        (mailbox_key, uidvalidity, last_uid, _utcnow()),
    )


def find_delivery(
    conn: sqlite3.Connection, mailbox_key: str, message_id: str
) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM mail_delivery WHERE mailbox_key = ? AND message_id = ?",
        (mailbox_key, message_id),
    ).fetchone()


def insert_delivery(
    conn: sqlite3.Connection,
    mailbox_key: str,
    message_id: str,
    attachment_digest: str,
    uid: int,
    uidvalidity: int,
) -> None:
    conn.execute(
        """
        INSERT INTO mail_delivery
            (mailbox_key, message_id, attachment_digest, first_uid,
             uidvalidity, ingested_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            mailbox_key,
            message_id,
            attachment_digest,
            uid,
            uidvalidity,
            _utcnow(),
        ),
    )


def insert_results(
    conn: sqlite3.Connection,
    mailbox_key: str,
    message_id: str,
    uid: int,
    records: Iterable[tuple[str, str, str, str]],
) -> list[int]:
    """写入原始检测记录，返回对应自增 id（与传入顺序一致）。"""
    now = _utcnow()
    ids: list[int] = []
    for sample_no, test_item, result, unit in records:
        cur = conn.execute(
            """
            INSERT INTO test_result
                (mailbox_key, message_id, uid, sample_no, test_item,
                 result, unit, ingested_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (mailbox_key, message_id, uid, sample_no, test_item, result, unit, now),
        )
        ids.append(int(cur.lastrowid))
    return ids


def query_results_by_sample(
    conn: sqlite3.Connection, sample_no: str
) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT id, sample_no, test_item, result, unit, message_id, uid,
               mailbox_key, ingested_at
        FROM test_result
        WHERE sample_no = ?
        ORDER BY id
        """,
        (sample_no,),
    ).fetchall()


def list_samples(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT sample_no,
               COUNT(*)        AS result_count,
               MIN(ingested_at) AS first_ingested_at,
               MAX(ingested_at) AS last_ingested_at
        FROM test_result
        GROUP BY sample_no
        ORDER BY sample_no
        """,
    ).fetchall()


def list_deliveries(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT mailbox_key, message_id, attachment_digest, first_uid,
               uidvalidity, ingested_at
        FROM mail_delivery
        ORDER BY id
        """,
    ).fetchall()


# ---------------------------------------------------------------------------
# 规则集
# ---------------------------------------------------------------------------


def get_active_rule_set_id(conn: sqlite3.Connection) -> int:
    row = conn.execute("SELECT rule_set_id FROM rule_state WHERE id = 1").fetchone()
    if row is None:
        # 正常路径由 init_db 保证存在；这里兜底且不提交，避免打断调用方事务
        _ensure_bootstrap_rule_set(conn)
        row = conn.execute(
            "SELECT rule_set_id FROM rule_state WHERE id = 1"
        ).fetchone()
    return int(row["rule_set_id"]) if row is not None else 0


def get_rule_set(conn: sqlite3.Connection, rule_set_id: int) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM rule_set WHERE id = ?", (rule_set_id,)
    ).fetchone()


def get_rules(conn: sqlite3.Connection, rule_set_id: int) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT id, source_item, source_unit, target_item, target_unit, factor
        FROM rule
        WHERE rule_set_id = ?
        ORDER BY source_item, source_unit
        """,
        (rule_set_id,),
    ).fetchall()


def insert_rule_set(
    conn: sqlite3.Connection, status: str = "building"
) -> int:
    cur = conn.execute(
        "INSERT INTO rule_set (status, created_at) VALUES (?, ?)",
        (status, _utcnow()),
    )
    return int(cur.lastrowid)


def insert_rules(
    conn: sqlite3.Connection,
    rule_set_id: int,
    rules: Sequence[tuple[str, str, str, str, float]],
) -> None:
    conn.executemany(
        """
        INSERT INTO rule
            (rule_set_id, source_item, source_unit,
             target_item, target_unit, factor)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        [(rule_set_id, *rule) for rule in rules],
    )
    conn.execute(
        "UPDATE rule_set SET rule_count = ? WHERE id = ?",
        (len(rules), rule_set_id),
    )


def mark_rule_set_status(
    conn: sqlite3.Connection,
    rule_set_id: int,
    status: str,
    *,
    error: str | None = None,
    started_at: str | None = None,
    finished_at: str | None = None,
    activated_at: str | None = None,
) -> None:
    fields = ["status = ?"]
    params: list[object] = [status]
    if error is not None:
        fields.append("last_error = ?")
        params.append(error)
    if started_at is not None:
        fields.append("recompute_started_at = ?")
        params.append(started_at)
    if finished_at is not None:
        fields.append("recompute_finished_at = ?")
        params.append(finished_at)
    if activated_at is not None:
        fields.append("activated_at = ?")
        params.append(activated_at)
    params.append(rule_set_id)
    conn.execute(
        f"UPDATE rule_set SET {', '.join(fields)} WHERE id = ?", tuple(params)
    )


def activate_rule_set(conn: sqlite3.Connection, rule_set_id: int) -> None:
    now = _utcnow()
    conn.execute(
        """
        UPDATE rule_state
        SET rule_set_id = ?, activated_at = ?
        WHERE id = 1
        """,
        (rule_set_id, now),
    )
    conn.execute(
        "UPDATE rule_set SET status = 'active', activated_at = ? WHERE id = ?",
        (now, rule_set_id),
    )
    conn.execute(
        """
        UPDATE rule_set SET status = 'superseded'
        WHERE id != ? AND status = 'active'
        """,
        (rule_set_id,),
    )


def list_rule_sets(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT id, status, created_at, activated_at, recompute_started_at,
               recompute_finished_at, last_error, rule_count,
               processed_rows, total_rows
        FROM rule_set
        ORDER BY id DESC
        """
    ).fetchall()


def insert_unified_results(
    conn: sqlite3.Connection,
    rows: Sequence[tuple],
) -> None:
    """rows: (rule_set_id, test_result_id, sample_no, source_item, source_unit,
    source_result, message_id, uid, mailbox_key, ingested_at, item, result,
    unit, status, reason)"""
    conn.executemany(
        """
        INSERT INTO unified_result
            (rule_set_id, test_result_id, sample_no, source_item, source_unit,
             source_result, message_id, uid, mailbox_key, ingested_at,
             item, result, unit, status, reason)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )


RAW_RESULT_COLUMNS = (
    "id, sample_no, test_item, result, unit, message_id, uid, "
    "mailbox_key, ingested_at"
)


def fetch_raw_results(
    conn: sqlite3.Connection,
    *,
    minimum_id: int = 0,
    test_result_ids: Sequence[int] | None = None,
    limit: int | None = None,
) -> list[sqlite3.Row]:
    if test_result_ids is not None:
        if not test_result_ids:
            return []
        placeholders = ",".join("?" for _ in test_result_ids)
        return conn.execute(
            f"""
            SELECT {RAW_RESULT_COLUMNS}
            FROM test_result
            WHERE id IN ({placeholders})
            ORDER BY id
            """,
            tuple(test_result_ids),
        ).fetchall()
    sql = f"""
        SELECT {RAW_RESULT_COLUMNS}
        FROM test_result
        WHERE id > ?
        ORDER BY id
    """
    params: list[object] = [minimum_id]
    if limit is not None:
        sql += " LIMIT ?"
        params.append(limit)
    return conn.execute(sql, tuple(params)).fetchall()


def fetch_unprocessed_raw_results(
    conn: sqlite3.Connection,
    rule_set_id: int,
    limit: int | None = None,
    maximum_id: int | None = None,
) -> list[sqlite3.Row]:
    """尚未写入指定规则集统一结果的原始记录。

    maximum_id 限定分块阶段的扫描上界，使持续采集时重算仍能推进；
    最后的补扫事务不传该参数，覆盖期间新增的全部记录。
    """
    sql = f"""
        SELECT {RAW_RESULT_COLUMNS}
        FROM test_result AS t
        WHERE NOT EXISTS (
            SELECT 1 FROM unified_result AS u
            WHERE u.rule_set_id = ? AND u.test_result_id = t.id
        )
    """
    params: list[object] = [rule_set_id]
    if maximum_id is not None:
        sql += " AND t.id <= ?"
        params.append(maximum_id)
    sql += " ORDER BY t.id"
    if limit is not None:
        sql += " LIMIT ?"
        params.append(limit)
    return conn.execute(sql, tuple(params)).fetchall()


def max_raw_result_id(conn: sqlite3.Connection) -> int:
    row = conn.execute("SELECT MAX(id) AS m FROM test_result").fetchone()
    return int(row["m"] or 0)


def fetch_raw_results_by_message(
    conn: sqlite3.Connection, mailbox_key: str, message_id: str
) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT id, sample_no, test_item, result, unit, message_id, uid,
               mailbox_key, ingested_at
        FROM test_result
        WHERE mailbox_key = ? AND message_id = ?
        ORDER BY id
        """,
        (mailbox_key, message_id),
    ).fetchall()


def delete_unified_results_for_set(
    conn: sqlite3.Connection, rule_set_id: int
) -> None:
    conn.execute(
        "DELETE FROM unified_result WHERE rule_set_id = ?", (rule_set_id,)
    )


def update_rule_set_progress(
    conn: sqlite3.Connection,
    rule_set_id: int,
    *,
    processed_rows: int,
    total_rows: int,
) -> None:
    conn.execute(
        """
        UPDATE rule_set
        SET processed_rows = ?, total_rows = ?
        WHERE id = ?
        """,
        (processed_rows, total_rows, rule_set_id),
    )


def count_raw_results(conn: sqlite3.Connection) -> int:
    row = conn.execute("SELECT COUNT(*) AS n FROM test_result").fetchone()
    return int(row["n"])


def count_unified_results(
    conn: sqlite3.Connection, rule_set_id: int, *, status: str | None = None
) -> int:
    if status is None:
        row = conn.execute(
            "SELECT COUNT(*) AS n FROM unified_result WHERE rule_set_id = ?",
            (rule_set_id,),
        ).fetchone()
    else:
        row = conn.execute(
            """
            SELECT COUNT(*) AS n FROM unified_result
            WHERE rule_set_id = ? AND status = ?
            """,
            (rule_set_id, status),
        ).fetchone()
    return int(row["n"])


def query_unified_results(
    conn: sqlite3.Connection,
    rule_set_id: int,
    *,
    sample_no: str | None = None,
    status: str | None = None,
    limit: int | None = None,
) -> list[sqlite3.Row]:
    clauses = ["rule_set_id = ?"]
    params: list[object] = [rule_set_id]
    if sample_no is not None:
        clauses.append("sample_no = ?")
        params.append(sample_no)
    if status is not None:
        clauses.append("status = ?")
        params.append(status)
    sql = f"""
        SELECT id, sample_no, item, result, unit, status, reason,
               source_item, source_unit, source_result,
               message_id, uid, mailbox_key, ingested_at
        FROM unified_result
        WHERE {' AND '.join(clauses)}
        ORDER BY sample_no, id
    """
    if limit is not None:
        sql += " LIMIT ?"
        params.append(limit)
    return conn.execute(sql, tuple(params)).fetchall()


def list_unified_samples(
    conn: sqlite3.Connection, rule_set_id: int
) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT sample_no,
               COUNT(*) AS result_count,
               SUM(CASE WHEN status = 'unified' THEN 1 ELSE 0 END) AS unified_count,
               SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) AS pending_count
        FROM unified_result
        WHERE rule_set_id = ?
        GROUP BY sample_no
        ORDER BY sample_no
        """,
        (rule_set_id,),
    ).fetchall()


def reset_all(db_path: str) -> None:
    """清空全部业务数据（演示/测试用），回到 0 号空规则集。"""
    conn = connect(db_path)
    try:
        init_db(conn)
        conn.execute("BEGIN IMMEDIATE")
        conn.execute("DELETE FROM unified_result")
        conn.execute("DELETE FROM rule")
        conn.execute("DELETE FROM rule_state")
        conn.execute("DELETE FROM rule_set")
        conn.execute("DELETE FROM test_result")
        conn.execute("DELETE FROM mail_delivery")
        conn.execute("DELETE FROM ingest_state")
        _ensure_bootstrap_rule_set(conn)
        conn.commit()
    finally:
        conn.close()


def dump_state_json(db_path: str) -> str:
    with connect(db_path) as conn:
        payload = {
            "states": [dict(r) for r in conn.execute("SELECT * FROM ingest_state")],
            "deliveries": [dict(r) for r in list_deliveries(conn)],
            "results": [
                dict(r)
                for r in conn.execute(
                    "SELECT id, mailbox_key, message_id, uid, sample_no,"
                    " test_item, result, unit, ingested_at FROM test_result"
                )
            ],
            "rule_sets": [dict(r) for r in list_rule_sets(conn)],
            "active_rule_set_id": get_active_rule_set_id(conn),
        }
    return json.dumps(payload, ensure_ascii=False, indent=2)
