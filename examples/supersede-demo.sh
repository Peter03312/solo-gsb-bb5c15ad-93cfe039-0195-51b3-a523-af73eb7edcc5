#!/usr/bin/env bash
# 复测报告接替端到端示例：发布 -> 复测接替 -> 沿革查询 -> 冲突与并发约定
# 依赖：curl（jq 可选，没有时用 python -m json.tool）
set -euo pipefail

BASE="${BASE_URL:-http://localhost:8000}"
pp() { if command -v jq >/dev/null 2>&1; then jq .; else python3 -m json.tool; fi; }

echo "== 1. 登记仪器 HPLC-01 的校准（2026 全年，通过） =="
curl -sS -X POST "$BASE/calibrations" -H 'Content-Type: application/json' -d '{
  "instrument": "HPLC-01",
  "valid_from": "2026-01-01T00:00:00Z",
  "valid_to":   "2027-01-01T00:00:00Z",
  "passed": true
}' | pp

echo "== 2. 发布原始报告 R-2026-0001 =="
PUB=$(curl -sS -X POST "$BASE/reports" -H 'Content-Type: application/json' -d '{
  "instrument": "HPLC-01",
  "tested_at": "2026-03-15T08:00:00Z",
  "reference": "R-2026-0001"
}')
echo "$PUB" | pp
RID=$(echo "$PUB" | python3 -c 'import sys,json;print(json.load(sys.stdin)["report_id"])')

echo "== 3. 复测接替：以 R-2026-0001-RETEST 接替报告 $RID（非空接替原因） =="
curl -sS -X POST "$BASE/reports/$RID/supersede" -H 'Content-Type: application/json' -d '{
  "instrument": "HPLC-01",
  "tested_at": "2026-03-20T09:00:00Z",
  "reference": "R-2026-0001-RETEST",
  "reason": "初测色谱峰异常，复测确认"
}' | pp

echo "== 4. 按旧版报告 ID 查询完整沿革（新版 ID 查询结果相同） =="
curl -sS "$BASE/reports/$RID/history" | pp

echo "== 5. 重复接替同一旧版 -> 409 REPORT_ALREADY_SUPERSEDED（附现行报告标识） =="
curl -sS -o /dev/stderr -w 'HTTP %{http_code}\n' -X POST "$BASE/reports/$RID/supersede" \
  -H 'Content-Type: application/json' -d '{
    "instrument": "HPLC-01",
    "tested_at": "2026-03-25T09:00:00Z",
    "reference": "R-2026-0002",
    "reason": "重复接替"
  }'

echo "== 6. 复用旧版编号 -> 409 REPORT_REFERENCE_EXISTS（旧编号不得转移或复用） =="
curl -sS -o /dev/stderr -w 'HTTP %{http_code}\n' -X POST "$BASE/reports/2/supersede" \
  -H 'Content-Type: application/json' -d '{
    "instrument": "HPLC-01",
    "tested_at": "2026-03-25T09:00:00Z",
    "reference": "R-2026-0001",
    "reason": "尝试复用旧编号"
  }'

echo "== 7. 复测时点命中禁用区间 -> 422 INSTRUMENT_DISABLED（旧报告仍有效，不留接替记录） =="
curl -sS -X POST "$BASE/disables" -H 'Content-Type: application/json' -d '{
  "event_no": "DIS-2026-100",
  "instrument": "HPLC-01",
  "disabled_from": "2026-04-01T00:00:00Z",
  "disabled_to":   "2026-05-01T00:00:00Z",
  "reason": "进样阀故障待修"
}' | pp
curl -sS -o /dev/stderr -w 'HTTP %{http_code}\n' -X POST "$BASE/reports/2/supersede" \
  -H 'Content-Type: application/json' -d '{
    "instrument": "HPLC-01",
    "tested_at": "2026-04-15T09:00:00Z",
    "reference": "R-2026-0003",
    "reason": "复测遇禁用"
  }'
echo "-- 阻断后旧版（报告 2）仍为现行有效，沿革无变化 --"
curl -sS "$BASE/reports/2/history" | pp

echo "== 8. 撤销禁用后同一复测接替成功，现行报告再次切换 =="
curl -sS -X POST "$BASE/disables/DIS-2026-100/revoke" \
  -H 'Content-Type: application/json' -d '{"reason": "进样阀更换完成"}' | pp
curl -sS -X POST "$BASE/reports/2/supersede" -H 'Content-Type: application/json' -d '{
  "instrument": "HPLC-01",
  "tested_at": "2026-04-15T09:00:00Z",
  "reference": "R-2026-0003",
  "reason": "复测遇禁用"
}' | pp

echo "== 9. 最终沿革：链 A->B->C，每次接替原因留痕，当前有效报告为 C =="
curl -sS "$BASE/reports/$RID/history" | pp
