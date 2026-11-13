from fastapi.testclient import TestClient

from app import db
from app.main import app
from tests.conftest import PASSWORD, USER


def test_full_api_flow(imap_server, db_path):
    host, port, _root = imap_server
    client = TestClient(app)

    assert client.get("/health").json() == {"status": "ok"}

    payload = {
        "host": host,
        "port": port,
        "folder": "INBOX",
        "username": USER,
        "password": PASSWORD,
        "use_ssl": False,
    }
    resp = client.post("/collect", json=payload)
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "stopped"
    assert body["stopped"]["uid"] == 3

    # 按样品查询
    resp = client.get("/results/S-1001")
    assert resp.status_code == 200
    data = resp.json()
    assert data["count"] == 2
    assert {item["test_item"] for item in data["results"]} == {"铅Pb", "镉Cd"}

    resp = client.get("/results/NO-SUCH")
    assert resp.status_code == 404

    # 样品列表 / 进度 / 投递
    samples = client.get("/samples").json()
    assert samples["count"] >= 2
    state = client.get("/ingest/state").json()
    assert state["states"][0]["last_uid"] == 2
    deliveries = client.get("/ingest/deliveries").json()
    assert deliveries["count"] == 2

    # 请求体校验：端口越界
    bad = dict(payload, port=70000)
    assert client.post("/collect", json=bad).status_code == 422


def test_unified_endpoint_available_before_any_rule(imap_server, db_path):
    host, port, _root = imap_server
    client = TestClient(app)
    payload = {
        "host": host,
        "port": port,
        "folder": "INBOX",
        "username": USER,
        "password": PASSWORD,
        "use_ssl": False,
    }
    client.post("/collect", json=payload)

    # 尚未发布规则：统一查询仍返回完整视图（0 号空规则集，全部待核实）
    body = client.get("/unified/results/S-1001").json()
    assert body["rule_set_id"] == 0
    assert body["pending_count"] == body["count"]
    assert {row["reason"] for row in body["results"]} == {"no_rule"}
    assert all(row["source_result"] == row["result"] for row in body["results"])
    assert all(row["message_id"] for row in body["results"])
