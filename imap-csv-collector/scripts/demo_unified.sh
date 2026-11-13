#!/usr/bin/env bash
# 统一规则演示：docker compose up -d --build 后执行。
# 场景覆盖：
#   1. 无规则时采集：统一结果全部“待核实”，保留原值与邮件来源
#   2. 发布规则集 v1 并重算已有结果（铅Pb/镉Cd/汞Hg/Cr 统一）
#   3. 发布规则集 v2（补充 Pb/Cd/Hg 写法），后台重算、接口立即返回
#   4. 重算期间：查询仍使用 v1、结果完整；新邮件照常采集并按 v1 查询
#   5. 启用 v2：期间新增记录被补扫覆盖，一次查询只见一套规则
#   6. 未映射单位 / 缺映射项目 / 非数值结果 -> 待核实
#   7. 重启 api：已启用规则与统一结果保留
# 用法： bash scripts/demo_unified.sh
set -euo pipefail

API="${API:-http://localhost:8000}"

show() { python3 -c "import json,sys; print(json.dumps(json.load(sys.stdin), ensure_ascii=False, indent=2))"; }
line() { printf '\n========== %s ==========\n' "$1"; }

collect() {
  curl -sS -X POST "$API/collect" -H 'Content-Type: application/json' -d '{
    "host": "imap", "port": 10143, "folder": "INBOX",
    "username": "instrument@lab.local", "password": "labpass",
    "use_ssl": false
  }'
}

publish() {  # $1 = rules json array, $2 = wait
  curl -sS -X POST "$API/rules/publish" -H 'Content-Type: application/json' \
    -d "{\"rules\": $1, \"wait\": $2}"
}

rules_v1() {
  cat <<'JSON'
[
  {"source_item": "铅Pb", "source_unit": "mg/kg", "target_item": "铅", "target_unit": "μg/kg", "factor": "1000"},
  {"source_item": "镉Cd", "source_unit": "mg/kg", "target_item": "镉", "target_unit": "μg/kg", "factor": "1000"},
  {"source_item": "汞Hg", "source_unit": "mg/kg", "target_item": "汞", "target_unit": "mg/kg", "factor": "1"},
  {"source_item": "Cr",   "source_unit": "μg/kg", "target_item": "铬", "target_unit": "μg/kg", "factor": "1"}
]
JSON
}

rules_v2() {
  cat <<'JSON'
[
  {"source_item": "铅Pb", "source_unit": "mg/kg", "target_item": "铅", "target_unit": "μg/kg", "factor": "1000"},
  {"source_item": "Pb",   "source_unit": "mg/kg", "target_item": "铅", "target_unit": "μg/kg", "factor": "1000"},
  {"source_item": "镉Cd", "source_unit": "mg/kg", "target_item": "镉", "target_unit": "μg/kg", "factor": "1000"},
  {"source_item": "Cd",   "source_unit": "mg/kg", "target_item": "镉", "target_unit": "μg/kg", "factor": "1000"},
  {"source_item": "汞Hg", "source_unit": "mg/kg", "target_item": "汞", "target_unit": "mg/kg", "factor": "1"},
  {"source_item": "Hg",   "source_unit": "mg/kg", "target_item": "汞", "target_unit": "mg/kg", "factor": "1"},
  {"source_item": "Cr",   "source_unit": "μg/kg", "target_item": "铬", "target_unit": "μg/kg", "factor": "1"}
]
JSON
}

wait_active() {
  for _ in $(seq 1 120); do
    status=$(curl -sS "$API/rulesets" | python3 -c \
      'import json,sys; print(json.load(sys.stdin)["rule_sets"][0]["status"])')
    [ "$status" = "active" ] && return 0
    [ "$status" = "failed" ] && return 1
    sleep 0.5
  done
  return 1
}

line "0. 重置结果库，保证演示从干净状态开始"
curl -sS -X POST "$API/admin/reset" | show

line "0a. 首轮采集：停在坏附件邮件 UID3；统一结果（0 号空规则集）全部待核实"
collect | show

line "1. 移除坏邮件后续采，把“项目名/单位不统一”的演示邮件采集入库"
docker compose exec -T imap rm -f /srv/mail/INBOX/3.eml || true
collect | show

line "2. 发布规则前查询 S-1001：待核实、保留原值与邮件来源"
curl -sS "$API/unified/results/S-1001" | show

line "3. 发布规则集 v1（wait=true，重算已有结果后启用）"
publish "$(rules_v1)" true | show
curl -sS "$API/unified/results/S-1001" | show

line "4. 发布规则集 v2（补 Pb/Cd/Hg 写法，后台重算，HTTP 立即返回）"
publish "$(rules_v2)" false | show
curl -sS "$API/rulesets" | show

line "5. 重算期间的查询：仍使用 v1（rule_set_id 不变），结果完整"
curl -sS "$API/unified/results/S-1001" | show

line "6. 重算期间投递并采集新邮件：照常入库，可按 v1 查询"
cat > /tmp/during_unified.csv <<'CSV'
样品编号,检测项目,结果,单位
S-1001,Pb,0.050,mg/kg
S-1003,Hg,2.5,μg/kg
S-1007,As,0.02,mg/kg
CSV
docker compose run --rm -v /tmp/during_unified.csv:/tmp/during_unified.csv mailctl \
  redeliver-new --message-id during-unified@lab-instrument \
  --attachment batch_during.csv --csv-path /tmp/during_unified.csv
collect | show
curl -sS "$API/unified/results/S-1001" | show

line "7. 等待 v2 启用：期间新增记录已被补扫覆盖"
wait_active
curl -sS "$API/rulesets" | show
curl -sS "$API/unified/results/S-1001" | show
curl -sS "$API/unified/results/S-1007" | show

line "8. 未映射单位 / 缺映射项目 / 非数值结果：待核实并保留原值"
curl -sS "$API/unified/results?status=pending" | show

line "9. 重启 api 后再次查询：已启用规则集与统一结果保留"
docker compose restart api >/dev/null
sleep 2
curl -sS "$API/rulesets" | show
curl -sS "$API/unified/results/S-1001" | show

line "演示完成（curl -X POST $API/admin/reset 可重置结果库）"
