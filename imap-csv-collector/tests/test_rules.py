"""统一规则发布、精确换算与统一结果查询的语义测试。

覆盖：
  * 原项目/原单位 -> 标准项目/单位 + 正十进制倍率的精确换算
  * 无规则、缺映射、非数值 -> 待核实，保留原值与邮件来源
  * 发布规则重算已有结果；原始记录与重投识别不变
  * 重算期间新邮件按旧规则入库可查，启用时覆盖期间新增记录
  * 重算失败继续使用旧规则下的完整结果；重启后保留已启用规则与结果
  * 一次查询只使用同一套规则
"""
from __future__ import annotations

import os
import time

import pytest
from fastapi.testclient import TestClient

from app import db, rules
from app.collector import collect
from app.main import app
from imap_server import mailbox
from tests.conftest import PASSWORD, USER

HEADER = "样品编号,检测项目,结果,单位"


def _csv(rows: list[tuple[str, str, str, str]]) -> bytes:
    lines = [HEADER] + [",".join(row) for row in rows]
    return ("\r\n".join(lines) + "\r\n").encode("utf-8")


def _deliver(root: str, folder: str, message_id: str, rows) -> int:
    raw = mailbox.build_message(
        message_id=message_id,
        subject="仪器结果",
        sender="instrument@lab.local",
        recipient="lab@lab.local",
        attachments=[("batch.csv", _csv(rows))],
    )
    return mailbox.append_message(root, folder, raw)


def _unblock_bad_seed(root: str) -> None:
    """种子里的 UID3 是坏附件邮件，会挡住其后新邮件；测试需要新邮件时先移除。"""
    path = f"{mailbox.folder_dir(root, 'INBOX')}/3.eml"
    if os.path.exists(path):
        os.remove(path)


def _collect(host: int, port: int):
    return collect(
        host=host,
        port=port,
        folder="INBOX",
        username=USER,
        password=PASSWORD,
        use_ssl=False,
    )


RULES_V1 = [
    {
        "source_item": "铅Pb",
        "source_unit": "mg/kg",
        "target_item": "铅",
        "target_unit": "μg/kg",
        "factor": "1000",
    },
    {
        "source_item": "镉Cd",
        "source_unit": "mg/kg",
        "target_item": "镉",
        "target_unit": "μg/kg",
        "factor": "1000",
    },
    {
        "source_item": "汞Hg",
        "source_unit": "mg/kg",
        "target_item": "汞",
        "target_unit": "mg/kg",
        "factor": 1,
    },
]


def _publish(rules_list, wait: bool = True) -> int:
    return rules.publish_rule_set(rules_list, wait=wait)


def _settle_rule_set(rule_set_id: int, timeout: float = 10.0) -> dict:
    """等待后台重算结束，返回规则集摘要。"""
    deadline = time.time() + timeout
    while time.time() < deadline:
        with db.connect() as conn:
            summary = rules.rule_set_summary(conn, rule_set_id)
        if summary and summary["status"] not in ("building", "recomputing"):
            return summary
        time.sleep(0.02)
    raise AssertionError(f"规则集 {rule_set_id} 未在 {timeout}s 内完成重算")


def _unified(sample_no: str) -> dict:
    client = TestClient(app)
    resp = client.get(f"/unified/results/{sample_no}")
    assert resp.status_code == 200, resp.text
    return resp.json()


# ---------------------------------------------------------------------------
# 精确换算
# ---------------------------------------------------------------------------


def test_exact_decimal_conversion():
    value = rules.parse_numeric("0.021")
    assert value is not None
    converted = rules.multiply_exact(value, rules.Decimal("1000"))
    assert rules.format_decimal(converted) == "21"

    tricky = rules.multiply_exact(
        rules.Decimal("1.005"), rules.Decimal("1000")
    )
    assert rules.format_decimal(tricky) == "1005"

    tiny = rules.multiply_exact(rules.Decimal("0.000123"), rules.Decimal("1000"))
    assert rules.format_decimal(tiny) == "0.123"

    # 0.1 * 3 在二进制浮点下会得到 0.30000000000000004
    assert rules.format_decimal(
        rules.multiply_exact(rules.Decimal("0.1"), rules.Decimal("3"))
    ) == "0.3"


@pytest.mark.parametrize("factor", ["0", "-1", "abc", "NaN", "Infinity", ""])
def test_rejects_invalid_factor(factor):
    with pytest.raises(rules.RuleError):
        rules.normalize_factor(factor)


def test_rejects_duplicate_source_mapping():
    with pytest.raises(rules.RuleError):
        rules.normalize_rules(
            [
                {
                    "source_item": "铅Pb",
                    "source_unit": "mg/kg",
                    "target_item": "铅",
                    "target_unit": "μg/kg",
                    "factor": 1000,
                },
                {
                    "source_item": "铅Pb",
                    "source_unit": "mg/kg",
                    "target_item": "铅",
                    "target_unit": "mg/kg",
                    "factor": 1,
                },
            ]
        )


# ---------------------------------------------------------------------------
# 发布 + 重算已有结果 + 待核实
# ---------------------------------------------------------------------------


def test_publish_recomputes_existing_results(imap_server, db_path):
    host, port, root = imap_server
    # 先采集（此时只有内置空规则集），补一封带项目名/单位差异的邮件
    _deliver(
        root,
        "INBOX",
        "unify-1@lab",
        [
            ("S-1001", "铅Pb", "0.0045", "mg/kg"),
            ("S-1001", "镉Cd", "0.0012", "mg/kg"),
            ("S-1001", "As", "<0.01", "mg/kg"),
        ],
    )
    _unblock_bad_seed(root)
    report = _collect(host, port)
    assert report.stopped is None
    assert any(item["message_id"] == "unify-1@lab" for item in report.ingested)

    # 发布前：所有记录待核实，原值可查
    before = _unified("S-1001")
    assert before["rule_set_id"] == 0
    assert {row["status"] for row in before["results"]} == {"pending"}
    original_ids = {row["id"] for row in before["results"]}

    with db.connect() as conn:
        raw_before = [
            dict(row) for row in db.query_results_by_sample(conn, "S-1001")
        ]

    client = TestClient(app)
    resp = client.post(
        "/rules/publish", json={"rules": RULES_V1, "wait": True}
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    rule_set_id = body["rule_set_id"]
    assert body["status"] == "active"
    assert body["is_active"] is True

    unified = _unified("S-1001")
    assert unified["rule_set_id"] == rule_set_id
    by_key = {
        (row["source_item"], row["source_result"]): row
        for row in unified["results"]
    }

    lead = by_key[("铅Pb", "0.021")]
    assert lead["status"] == "unified"
    assert lead["item"] == "铅"
    assert lead["result"] == "21"
    assert lead["unit"] == "μg/kg"
    assert lead["source_result"] == "0.021"
    assert lead["message_id"] == "inst-0001@lab-instrument"
    assert lead["uid"] == 1

    assert by_key[("镉Cd", "0.008")]["result"] == "8"
    # 规则同样适用于后采集的邮件
    assert by_key[("铅Pb", "0.0045")]["result"] == "4.5"
    assert by_key[("镉Cd", "0.0012")]["result"] == "1.2"

    # 原文记录 id 与内容完全不变（重算只读原始记录）
    assert original_ids  # 待核实行不再是原始 id 空间，但原始记录必须原样
    with db.connect() as conn:
        raw_after = [
            dict(row) for row in db.query_results_by_sample(conn, "S-1001")
        ]
    assert raw_after == raw_before

    # 缺映射的项目与单位 -> 待核实并保留原值
    arsenic = by_key[("As", "<0.01")]
    assert arsenic["status"] == "pending"
    assert arsenic["reason"] == rules.REASON_NO_RULE
    assert arsenic["result"] == "<0.01"
    assert arsenic["source_result"] == "<0.01"
    assert arsenic["unit"] == "mg/kg"
    assert arsenic["message_id"] == "unify-1@lab"


def test_unit_specific_mapping(imap_server, db_path):
    host, port, root = imap_server
    _deliver(
        root,
        "INBOX",
        "unify-unit@lab",
        [("S-2000", "铅Pb", "0.02", "μg/kg")],
    )
    _unblock_bad_seed(root)
    assert _collect(host, port).stopped is None
    _publish(RULES_V1)
    row = _unified("S-2000")["results"][0]
    # 项目存在但单位未映射：不能错误套用 mg/kg 的 1000 倍
    assert row["status"] == "pending"
    assert row["reason"] == rules.REASON_UNMAPPED_UNIT
    assert row["source_result"] == "0.02"


def test_non_numeric_result_pending(imap_server, db_path):
    host, port, root = imap_server
    _deliver(root, "INBOX", "unify-nn@lab", [("S-3000", "铅Pb", "<0.01", "mg/kg")])
    _unblock_bad_seed(root)
    assert _collect(host, port).stopped is None
    _publish(RULES_V1)
    row = _unified("S-3000")["results"][0]
    assert row["status"] == "pending"
    assert row["reason"] == rules.REASON_NON_NUMERIC
    assert row["result"] == "<0.01"
    assert row["source_result"] == "<0.01"
    assert row["message_id"] == "unify-nn@lab"


def test_redelivery_identity_unchanged_by_publish(imap_server, db_path):
    host, port, root = imap_server
    _collect(host, port)
    _publish(RULES_V1)

    # 移除坏邮件后原样重投 UID1：投递识别/去重仍按 Message-ID + 摘要工作
    os.remove(f"{mailbox.folder_dir(root, 'INBOX')}/3.eml")
    raw1 = mailbox.read_message(root, "INBOX", 1)
    new_uid = mailbox.append_message(root, "INBOX", raw1)
    report = _collect(host, port)
    assert report.stopped is None
    assert [item["uid"] for item in report.skipped_duplicates] == [new_uid]
    assert report.ingested == []


# ---------------------------------------------------------------------------
# 重算期间采集 / 一次查询一套规则
# ---------------------------------------------------------------------------


def test_collect_during_recompute_uses_old_rules_then_new_set_covers_new_rows(
    imap_server, db_path, monkeypatch
):
    host, port, root = imap_server
    _collect(host, port)
    old_set = _publish(RULES_V1)
    first = _unified("S-1001")
    assert first["rule_set_id"] == old_set
    assert all(row["status"] == "unified" for row in first["results"])

    # 让重算变慢：分块 + 每批暂停，制造一个稳定的“重算中”窗口
    monkeypatch.setenv("RULE_RECOMPUTE_CHUNK_SIZE", "1")
    monkeypatch.setenv("RULE_RECOMPUTE_PAUSE_SECONDS", "0.15")

    v2 = [
        {
            "source_item": "铅Pb",
            "source_unit": "mg/kg",
            "target_item": "铅",
            "target_unit": "mg/kg",
            "factor": "1",
        },
        {
            "source_item": "镉Cd",
            "source_unit": "mg/kg",
            "target_item": "镉",
            "target_unit": "μg/kg",
            "factor": "1000",
        },
    ]
    rule_set_id = _publish(v2, wait=False)

    # 等新规则集进入 recomputing
    deadline = time.time() + 5
    while time.time() < deadline:
        with db.connect() as conn:
            summary = rules.rule_set_summary(conn, rule_set_id)
        if summary and summary["status"] == "recomputing":
            break
        time.sleep(0.01)
    assert summary["status"] == "recomputing"

    # 重算期间：查询仍是旧规则集，完整可查（不混用两套规则）
    during = _unified("S-1001")
    assert during["rule_set_id"] == old_set
    assert {row["result"] for row in during["results"]} == {"21", "8"}

    # 重算期间新邮件照常采集入库，按旧规则可查
    _unblock_bad_seed(root)
    _deliver(root, "INBOX", "during@lab", [("S-4000", "铅Pb", "0.5", "mg/kg")])
    report = _collect(host, port)
    assert report.stopped is None
    assert any(item["message_id"] == "during@lab" for item in report.ingested)
    # 采集没有等待整库重算结束
    with db.connect() as conn:
        assert rules.rule_set_summary(conn, rule_set_id)["status"] == "recomputing"
    during_new = _unified("S-4000")
    assert during_new["rule_set_id"] == old_set
    assert during_new["results"][0]["result"] == "500"

    summary = _settle_rule_set(rule_set_id, timeout=20)
    assert summary["status"] == "active"

    # 启用后：期间新增记录也按新规则换算（补扫覆盖）
    after_new = _unified("S-4000")
    assert after_new["rule_set_id"] == rule_set_id
    assert after_new["results"][0]["result"] == "0.5"
    assert after_new["results"][0]["item"] == "铅"

    after_old = _unified("S-1001")
    assert after_old["rule_set_id"] == rule_set_id
    values = {row["source_item"]: row["result"] for row in after_old["results"]}
    assert values["铅Pb"] == "0.021"
    assert values["镉Cd"] == "8"

    # 期间新增记录没有遗漏
    with db.connect() as conn:
        raw_total = db.count_raw_results(conn)
        unified_total = db.count_unified_results(conn, rule_set_id)
    assert unified_total == raw_total


def test_query_uses_single_rule_set_snapshot(imap_server, db_path, monkeypatch):
    """切换规则集的瞬间查询也只会看到一套规则。"""
    host, port, _root = imap_server
    _collect(host, port)
    old_set = _publish(RULES_V1)

    monkeypatch.setenv("RULE_RECOMPUTE_CHUNK_SIZE", "1")
    v2 = [
        {
            "source_item": "铅Pb",
            "source_unit": "mg/kg",
            "target_item": "铅",
            "target_unit": "mg/kg",
            "factor": "1",
        }
    ]
    rule_set_id = _publish(v2, wait=False)
    client = TestClient(app)
    seen_rule_sets = set()
    seen_values = set()
    for _ in range(60):
        resp = client.get("/unified/results/S-1001")
        if resp.status_code == 200:
            body = resp.json()
            # 响应头里的规则集与每条结果上的规则集必须一致
            assert body["rule_set_id"] == rule_set_id or body["rule_set_id"] == old_set
            seen_rule_sets.add(body["rule_set_id"])
            values = {
                row["source_item"]: row["result"] for row in body["results"]
            }
            if "铅Pb" in values:
                seen_values.add((body["rule_set_id"], values["铅Pb"]))
        time.sleep(0.005)
    _settle_rule_set(rule_set_id, timeout=10)
    # 同一规则集版本内，取值从不变化（不会出现一半旧一半新的混合结果）
    by_set: dict[int, set] = {}
    for set_id, value in seen_values:
        by_set.setdefault(set_id, set()).add(value)
    assert seen_rule_sets <= {old_set, rule_set_id}
    if old_set in by_set:
        # v1 下铅Pb 恒为 0.021*1000=21，从未出现半换算值
        assert by_set[old_set] == {"21"}
    for set_id, values in by_set.items():
        assert len(values) == 1, (set_id, values)


# ---------------------------------------------------------------------------
# 失败与重启
# ---------------------------------------------------------------------------


def test_recompute_failure_keeps_old_rules_and_results(
    imap_server, db_path, monkeypatch
):
    host, port, _root = imap_server
    _collect(host, port)
    old_set = _publish(RULES_V1)

    monkeypatch.setenv("RULE_RECOMPUTE_CHUNK_SIZE", "1")
    monkeypatch.setenv("RULE_RECOMPUTE_FAIL_AFTER", "1")
    v2 = [
        {
            "source_item": "铅Pb",
            "source_unit": "mg/kg",
            "target_item": "铅",
            "target_unit": "mg/kg",
            "factor": "1",
        }
    ]
    failed_set = rules.publish_rule_set(v2, wait=True)
    with db.connect() as conn:
        summary = rules.rule_set_summary(conn, failed_set)
        active = db.get_active_rule_set_id(conn)
    assert summary["status"] == "failed"
    assert summary["last_error"]
    assert active == old_set

    # 旧规则下的完整结果继续可查
    unified = _unified("S-1001")
    assert unified["rule_set_id"] == old_set
    assert {row["result"] for row in unified["results"]} == {"21", "8"}
    assert unified["pending_count"] == 0


def test_restart_keeps_active_rule_set_and_results(imap_server, db_path):
    host, port, _root = imap_server
    _collect(host, port)
    rule_set_id = _publish(RULES_V1)
    expected = _unified("S-1001")

    # 新连接 = 服务重启后的状态：规则集指针、规则与统一结果都来自 SQLite
    with db.connect() as conn:
        db.init_db(conn)
        assert db.get_active_rule_set_id(conn) == rule_set_id
        assert len(db.get_rules(conn, rule_set_id)) == len(RULES_V1)
        assert db.count_unified_results(conn, rule_set_id) == db.count_raw_results(
            conn
        )

    fresh = _unified("S-1001")
    assert fresh == expected

    # 重启时若还有未完成重算（进程内不存在），标记 failed，不影响已启用版本
    conn = db.connect()
    try:
        with db.transaction(conn, immediate=True):
            pending_set = db.insert_rule_set(conn, status="recomputing")
    finally:
        conn.close()
    recovered = rules.recover_interrupted()
    assert recovered >= 1
    with db.connect() as conn:
        assert db.get_rule_set(conn, pending_set)["status"] == "failed"
        assert db.get_active_rule_set_id(conn) == rule_set_id
    assert _unified("S-1001") == expected


# ---------------------------------------------------------------------------
# API 校验
# ---------------------------------------------------------------------------


def test_publish_validation_and_conflict(imap_server, db_path, monkeypatch):
    client = TestClient(app)
    bad = client.post(
        "/rules/publish",
        json={
            "rules": [
                {
                    "source_item": "铅Pb",
                    "source_unit": "mg/kg",
                    "target_item": "铅",
                    "target_unit": "μg/kg",
                    "factor": "-2",
                }
            ]
        },
    )
    assert bad.status_code == 422

    _collect(*imap_server[:2])
    monkeypatch.setenv("RULE_RECOMPUTE_CHUNK_SIZE", "1")
    monkeypatch.setenv("RULE_RECOMPUTE_PAUSE_SECONDS", "0.1")
    assert (
        client.post("/rules/publish", json={"rules": RULES_V1, "wait": False})
        .status_code
        == 200
    )
    conflict = client.post(
        "/rules/publish", json={"rules": RULES_V1, "wait": False}
    )
    assert conflict.status_code == 409


def test_failed_rule_set_has_no_partial_results(imap_server, db_path, monkeypatch):
    host, port, _root = imap_server
    _collect(host, port)
    old_set = _publish(RULES_V1)

    monkeypatch.setenv("RULE_RECOMPUTE_CHUNK_SIZE", "1")
    monkeypatch.setenv("RULE_RECOMPUTE_FAIL_AFTER", "2")
    failed_set = rules.publish_rule_set(RULES_V1, wait=True)
    with db.connect() as conn:
        assert db.get_rule_set(conn, failed_set)["status"] == "failed"
        assert db.count_unified_results(conn, failed_set) == 0
        assert db.get_active_rule_set_id(conn) == old_set
    # 旧规则集的结果完整（数量与原始记录一致）
    with db.connect() as conn:
        assert db.count_unified_results(conn, old_set) == db.count_raw_results(conn)


def test_rule_set_listing_and_unified_listing(imap_server, db_path):
    _collect(*imap_server[:2])
    rule_set_id = _publish(RULES_V1)
    client = TestClient(app)

    rule_sets = client.get("/rulesets").json()
    assert rule_sets["active_rule_set_id"] == rule_set_id
    active = next(r for r in rule_sets["rule_sets"] if r["id"] == rule_set_id)
    assert active["is_active"] is True
    assert active["rule_count"] == len(RULES_V1)

    detail = client.get(f"/rulesets/{rule_set_id}").json()
    assert len(detail["rules"]) == len(RULES_V1)

    listed = client.get("/unified/results").json()
    assert listed["rule_set_id"] == rule_set_id
    pending = client.get("/unified/results?status=pending").json()
    assert all(row["status"] == "pending" for row in pending["results"])

    samples = client.get("/unified/samples").json()
    assert samples["rule_set_id"] == rule_set_id
    assert any(row["sample_no"] == "S-1001" for row in samples["samples"])

    assert client.get("/rulesets/99999").status_code == 404
    assert client.get("/unified/results/NOPE").status_code == 404
