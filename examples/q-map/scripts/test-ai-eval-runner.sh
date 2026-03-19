#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

TMP_DIR="$(mktemp -d)"
cleanup() {
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

assert_report() {
  local report_path="$1"
  local expr="$2"
  node -e "const fs=require('fs'); const report=JSON.parse(fs.readFileSync(process.argv[1],'utf8')); const expr=String(process.argv[2]||''); if (!(eval(expr))) { process.stderr.write('assertion failed: ' + expr + '\\n'); process.exit(1); }" "$report_path" "$expr"
}

run_preflight_case() {
  local run_id="unit-preflight-$(date +%s%N)"
  local out_dir="$TMP_DIR/preflight"
  mkdir -p "$out_dir"

  set +e
  node scripts/run-ai-eval.mjs \
    --cases tests/ai-eval/cases.sample.json \
    --base-url http://127.0.0.1:1 \
    --request-timeout-ms 1000 \
    --transport-failure-threshold 1 \
    --run-id "$run_id" \
    --out-dir "$out_dir" >"$out_dir/stdout.log" 2>&1
  local status=$?
  set -e
  if [[ "$status" -eq 0 ]]; then
    echo "[test-ai-eval-runner] expected preflight run to fail, got status=0" >&2
    exit 1
  fi

  grep -q '\[ai-eval\]\[abort\] preflight /health failed:' "$out_dir/stdout.log"
  local report_json="$out_dir/report-$run_id.json"
  assert_report "$report_json" "report.transport && report.transport.aborted === true"
  assert_report "$report_json" "String(report.transport.reason || '').includes('preflight /health failed:')"
  assert_report "$report_json" "report.summary.failed === report.summary.totalCases && report.summary.totalCases > 0"
  assert_report "$report_json" "report.areaGates && report.areaGates.pass === false"
  assert_report "$report_json" "Array.isArray(report.areaGates && report.areaGates.areas) && report.areaGates.areas.length > 0"
  assert_report "$report_json" "Array.isArray(report.cases) && report.cases.every(c => Array.isArray(c.gates && c.gates.failed) && c.gates.failed.includes('transport-error-abort'))"
}

run_threshold_case() {
  local run_id="unit-threshold-$(date +%s%N)"
  local out_dir="$TMP_DIR/threshold"
  mkdir -p "$out_dir"

  set +e
  node scripts/run-ai-eval.mjs \
    --cases tests/ai-eval/cases.sample.json \
    --base-url http://127.0.0.1:1 \
    --request-timeout-ms 1000 \
    --transport-failure-threshold 1 \
    --skip-transport-preflight \
    --run-id "$run_id" \
    --out-dir "$out_dir" >"$out_dir/stdout.log" 2>&1
  local status=$?
  set -e
  if [[ "$status" -eq 0 ]]; then
    echo "[test-ai-eval-runner] expected threshold run to fail, got status=0" >&2
    exit 1
  fi

  grep -q '\[ai-eval\]\[preflight\] skipped by option' "$out_dir/stdout.log"
  grep -q '\[ai-eval\]\[abort\] transport errors threshold reached' "$out_dir/stdout.log"
  local report_json="$out_dir/report-$run_id.json"
  assert_report "$report_json" "report.transport && report.transport.aborted === true"
  assert_report "$report_json" "report.transport && report.transport.preflightSkipped === true"
  assert_report "$report_json" "String(report.transport.reason || '').includes('transport errors threshold reached')"
  assert_report "$report_json" "Array.isArray(report.cases) && report.cases.length > 1"
  assert_report "$report_json" "String(report.cases[0].transportError || '').length > 0"
  assert_report "$report_json" "report.cases.slice(1).every(c => Array.isArray(c.gates && c.gates.failed) && c.gates.failed.includes('transport-error-abort'))"
}

run_threshold_two_case() {
  local run_id="unit-threshold-two-$(date +%s%N)"
  local out_dir="$TMP_DIR/threshold-two"
  mkdir -p "$out_dir"

  set +e
  node scripts/run-ai-eval.mjs \
    --cases tests/ai-eval/cases.sample.json \
    --base-url http://127.0.0.1:1 \
    --request-timeout-ms 1000 \
    --transport-failure-threshold 2 \
    --skip-transport-preflight \
    --run-id "$run_id" \
    --out-dir "$out_dir" >"$out_dir/stdout.log" 2>&1
  local status=$?
  set -e
  if [[ "$status" -eq 0 ]]; then
    echo "[test-ai-eval-runner] expected threshold-two run to fail, got status=0" >&2
    exit 1
  fi

  grep -q '\[ai-eval\]\[abort\] transport errors threshold reached (2/2)' "$out_dir/stdout.log"
  local report_json="$out_dir/report-$run_id.json"
  assert_report "$report_json" "report.transport && report.transport.aborted === true"
  assert_report "$report_json" "String(report.transport.reason || '').includes('(2/2)')"
  assert_report "$report_json" "Array.isArray(report.cases) && report.cases.length > 2"
  assert_report "$report_json" "String(report.cases[0].transportError || '').length > 0"
  assert_report "$report_json" "String(report.cases[1].transportError || '').length > 0"
  assert_report "$report_json" "report.cases.slice(2).every(c => Array.isArray(c.gates && c.gates.failed) && c.gates.failed.includes('transport-error-abort'))"
}

run_trace_grade_case() {
  local tmp_dir="$TMP_DIR/trace-grade"
  mkdir -p "$tmp_dir/results" "$tmp_dir/audit"
  local report_json="$tmp_dir/results/report-trace-grade.json"
  local audit_jsonl="$tmp_dir/audit/session-trace-grade.jsonl"

  cat >"$report_json" <<'JSON'
{
  "casesPath": "tests/ai-eval/cases.functional.json",
  "cases": [
    {
      "id": "arch_cloud_postload_validation",
      "criticality": "critical",
      "toolCalls": ["loadCloudMapAndWait", "waitForQMapDataset"],
      "requestIds": ["req-trace-1"]
    },
    {
      "id": "arch_superlative_admin_unit_show_on_map",
      "criticality": "critical",
      "toolCalls": ["queryQCumberTerritorialUnits", "fitQMapToDataset"],
      "requestIds": ["req-trace-2"]
    }
  ]
}
JSON

  cat >"$audit_jsonl" <<'JSONL'
{"requestId":"req-trace-1","ts":"2026-03-12T16:00:00Z","status":200,"outcome":"success","qualityMetrics":{"workflowScore":91,"falseSuccessClaimCount":0,"contractSchemaMismatchCount":0,"hasDatasetMutation":false,"postCreateWaitOk":true,"postCreateWaitCountOk":true,"postCreateWaitCountRankOk":false}}
{"requestId":"req-trace-2","ts":"2026-03-12T16:00:02Z","status":200,"outcome":"success","qualityMetrics":{"workflowScore":88,"falseSuccessClaimCount":0,"contractSchemaMismatchCount":0,"hasDatasetMutation":false,"postCreateWaitOk":false,"postCreateWaitCountOk":false,"postCreateWaitCountRankOk":false}}
JSONL

  node scripts/audit-ai-trace-grades.mjs --report "$report_json" --audit-dirs "$tmp_dir/audit" >"$tmp_dir/pass.log"
  grep -q '\[ai-trace-grade\] OK:' "$tmp_dir/pass.log"

  cat >"$audit_jsonl" <<'JSONL'
{"requestId":"req-trace-1","ts":"2026-03-12T16:00:00Z","status":200,"outcome":"success","qualityMetrics":{"workflowScore":91,"falseSuccessClaimCount":0,"contractSchemaMismatchCount":0,"hasDatasetMutation":false,"postCreateWaitOk":true,"postCreateWaitCountOk":true,"postCreateWaitCountRankOk":false}}
{"requestId":"req-trace-2","ts":"2026-03-12T16:00:02Z","status":200,"outcome":"success","qualityMetrics":{"workflowScore":88,"falseSuccessClaimCount":1,"contractSchemaMismatchCount":0,"hasDatasetMutation":false,"postCreateWaitOk":false,"postCreateWaitCountOk":false,"postCreateWaitCountRankOk":false}}
JSONL

  set +e
  node scripts/audit-ai-trace-grades.mjs --report "$report_json" --audit-dirs "$tmp_dir/audit" >"$tmp_dir/fail.log" 2>&1
  local status=$?
  set -e
  if [[ "$status" -eq 0 ]]; then
    echo "[test-ai-eval-runner] expected trace-grade audit to fail, got status=0" >&2
    exit 1
  fi
  grep -q '\[ai-trace-grade\] FAIL' "$tmp_dir/fail.log"
  grep -q 'falseSuccessClaimCount=1' "$tmp_dir/fail.log"
}

run_deterministic_constraints_case() {
  local tmp_dir="$TMP_DIR/deterministic-constraints"
  mkdir -p "$tmp_dir/out"
  local cases_json="$tmp_dir/cases.json"
  local run_id="unit-constraints-$(date +%s%N)"

  cat >"$cases_json" <<'JSON'
[
  {
    "id": "default_constraints_case",
    "area": "ai_tool_orchestration",
    "kpi_id": "deterministic_discovery_routing",
    "user_prompt": "Fammi un inventario degli strumenti disponibili.",
    "required_tools_any": ["listQMapToolCategories"],
    "expected_keywords_any": ["strumenti"]
  },
  {
    "id": "disabled_constraints_case",
    "area": "data_pipeline",
    "kpi_id": "provider_dataset_resolution_accuracy",
    "deterministic_constraints": false,
    "user_prompt": "Mostrami i dataset disponibili prima di chiedermi il datasetId.",
    "required_tools_any": ["listQCumberDatasets"],
    "expected_keywords_any": ["dataset"]
  }
]
JSON

  set +e
  node scripts/run-ai-eval.mjs \
    --cases "$cases_json" \
    --dry-run \
    --run-id "$run_id" \
    --out-dir "$tmp_dir/out" >"$tmp_dir/stdout.log" 2>&1
  local status=$?
  set -e
  if [[ "$status" -ne 0 ]]; then
    echo "[test-ai-eval-runner] expected dry-run constraints case to succeed, got status=$status" >&2
    exit 1
  fi

  local report_json="$tmp_dir/out/report-$run_id.json"
  assert_report "$report_json" "report.summary.avgDurationMs === 0"
  assert_report "$report_json" "report.summary.transportErrorRate === 0"
  assert_report "$report_json" "report.cases.find(c => c.id === 'default_constraints_case').deterministicConstraintsApplied === true"
  assert_report "$report_json" "report.cases.find(c => c.id === 'disabled_constraints_case').deterministicConstraintsApplied === false"
}

run_mock_tool_override_case() {
  local tmp_dir="$TMP_DIR/mock-tool-override"
  mkdir -p "$tmp_dir/out"
  local cases_json="$tmp_dir/cases.json"
  local server_js="$tmp_dir/mock-server.js"
  local port=31881
  local run_id="unit-mock-tool-override-$(date +%s%N)"

  cat >"$cases_json" <<'JSON'
[
  {
    "id": "flat_metric_limitation_case",
    "area": "map_ux",
    "kpi_id": "style_application_accuracy",
    "criticality": "critical",
    "max_extra_tool_calls": 0,
    "expected_response_mode": "limitation",
    "response_mode_markers_any": ["non e possibile", "non applicabile"],
    "user_prompt": "Applica un gradiente sul campo flat_metric del layer Admin Boundaries. Se non e possibile, dichiaralo chiaramente.",
    "required_tools_all": ["setQMapLayerColorByField"],
    "expected_keywords_any": ["gradiente", "non possibile"],
    "mock_tool_results": {
      "setQMapLayerColorByField": {
        "success": false,
        "details": "Metric is flat (distinct=1)."
      }
    }
  }
]
JSON

  cat >"$server_js" <<'JS'
const http = require('http');

const port = Number(process.env.MOCK_EVAL_PORT || 31881);
const server = http.createServer((req, res) => {
  if (req.method === 'GET' && req.url === '/health') {
    res.writeHead(200, {'content-type': 'application/json'});
    res.end(JSON.stringify({ok: true}));
    return;
  }

  if (req.method !== 'POST' || req.url !== '/chat/completions') {
    res.writeHead(404, {'content-type': 'application/json'});
    res.end(JSON.stringify({error: 'not found'}));
    return;
  }

  let raw = '';
  req.on('data', chunk => {
    raw += chunk;
  });
  req.on('end', () => {
    const body = JSON.parse(raw || '{}');
    const messages = Array.isArray(body.messages) ? body.messages : [];
    const hasToolResult = messages.some(message => message && message.role === 'tool');
    const payload = hasToolResult
      ? {
          qAssistant: {
            qualityMetrics: {
              workflowScore: 94,
              responseModeHint: 'limitation',
              customRuntimeFlag: true,
              latestRecoveryStage: 'finalized',
              recoveryTrace: ['finalized']
            }
          },
          choices: [
            {
              message: {
                role: 'assistant',
                content: 'Il gradiente metrico richiesto non e applicabile perche la metrica e piatta.'
              }
            }
          ]
        }
      : {
          qAssistant: {
            qualityMetrics: {
              workflowScore: 91,
              recoveryTrace: ['tool-planned']
            }
          },
          choices: [
            {
              message: {
                role: 'assistant',
                content: '',
                tool_calls: [
                  {
                    id: 'tool-flat-metric-1',
                    type: 'function',
                    function: {
                      name: 'setQMapLayerColorByField',
                      arguments: JSON.stringify({
                        datasetName: 'Admin Boundaries',
                        fieldName: 'flat_metric'
                      })
                    }
                  }
                ]
              }
            }
          ]
        };
    res.writeHead(200, {
      'content-type': 'application/json',
      'x-q-assistant-request-id': hasToolResult ? 'req-mock-tool-2' : 'req-mock-tool-1'
    });
    res.end(JSON.stringify(payload));
  });
});

server.listen(port, '127.0.0.1');
JS

  node "$server_js" &
  local server_pid=$!
  trap 'kill "$server_pid" 2>/dev/null || true' RETURN
  sleep 1

  node scripts/run-ai-eval.mjs \
    --cases "$cases_json" \
    --base-url "http://127.0.0.1:$port" \
    --request-timeout-ms 3000 \
    --run-id "$run_id" \
    --out-dir "$tmp_dir/out" >"$tmp_dir/stdout.log" 2>&1

  local report_json="$tmp_dir/out/report-$run_id.json"
  assert_report "$report_json" "report.summary.totalCases === 1"
  assert_report "$report_json" "report.summary.escalationEvaluatedCases === 1"
  assert_report "$report_json" "report.summary.escalationComplianceRate === 1"
  assert_report "$report_json" "report.cases[0].responseModeChecks && report.cases[0].responseModeChecks.observedMode === 'limitation'"
  assert_report "$report_json" "report.cases[0].responseModeChecks && report.cases[0].responseModeChecks.failedToolEvidence === true"
  assert_report "$report_json" "Array.isArray(report.cases[0].toolCalls) && report.cases[0].toolCalls.includes('setQMapLayerColorByField')"
  assert_report "$report_json" "report.cases[0].evalDiagnostics && report.cases[0].evalDiagnostics.qualityMetrics && report.cases[0].evalDiagnostics.qualityMetrics.customRuntimeFlag === true"
  assert_report "$report_json" "report.cases[0].evalDiagnostics && report.cases[0].evalDiagnostics.qualityMetrics && report.cases[0].evalDiagnostics.qualityMetrics.latestRecoveryStage === 'finalized'"
  assert_report "$report_json" "Array.isArray(report.cases[0].evalDiagnostics && report.cases[0].evalDiagnostics.qualityMetrics && report.cases[0].evalDiagnostics.qualityMetrics.recoveryTrace) && report.cases[0].evalDiagnostics.qualityMetrics.recoveryTrace.includes('tool-planned') && report.cases[0].evalDiagnostics.qualityMetrics.recoveryTrace.includes('finalized')"

  kill "$server_pid" 2>/dev/null || true
  trap - RETURN
}

run_operational_audit_case() {
  local tmp_dir="$TMP_DIR/operational-audit"
  mkdir -p "$tmp_dir/results"
  local report_json="$tmp_dir/results/report-operational.json"

  cat >"$report_json" <<'JSON'
{
  "casesPath": "tests/ai-eval/cases.functional.json",
  "transport": {
    "aborted": false
  },
  "cases": [
    {"id": "ok-1", "durationMs": 12000, "transportError": ""},
    {"id": "ok-2", "durationMs": 18000, "transportError": ""},
    {"id": "ok-3", "durationMs": 22000, "transportError": ""}
  ]
}
JSON

  node scripts/audit-ai-operational-kpis.mjs --report "$report_json" --max-avg-duration-ms 25000 --max-p95-duration-ms 25000 --max-max-duration-ms 30000 >"$tmp_dir/pass.log"
  grep -q '\[ai-operational\] OK:' "$tmp_dir/pass.log"

  cat >"$report_json" <<'JSON'
{
  "casesPath": "tests/ai-eval/cases.functional.json",
  "transport": {
    "aborted": true
  },
  "cases": [
    {"id": "slow-1", "durationMs": 12000, "transportError": ""},
    {"id": "slow-2", "durationMs": 18000, "transportError": "timeout"},
    {"id": "slow-3", "durationMs": 52000, "transportError": ""}
  ]
}
JSON

  set +e
  node scripts/audit-ai-operational-kpis.mjs --report "$report_json" --max-avg-duration-ms 20000 --max-p95-duration-ms 40000 --max-max-duration-ms 50000 >"$tmp_dir/fail.log" 2>&1
  local status=$?
  set -e
  if [[ "$status" -eq 0 ]]; then
    echo "[test-ai-eval-runner] expected operational audit to fail, got status=0" >&2
    exit 1
  fi
  grep -q '\[ai-operational\] FAIL' "$tmp_dir/fail.log"
  grep -q 'transport.aborted=true' "$tmp_dir/fail.log"
}

run_response_quality_audit_case() {
  local tmp_dir="$TMP_DIR/response-quality-audit"
  local report_json="$tmp_dir/report.json"
  mkdir -p "$tmp_dir"

  cat >"$report_json" <<'JSON'
{
  "casesPath": "tests/ai-eval/cases.functional.json",
  "summary": {
    "totalCases": 6,
    "totalFalseSuccessClaims": 0,
    "falseSuccessClaimRate": 0,
    "groundedAnswerEvaluatedCases": 3,
    "groundedAnswerPassingCases": 3,
    "groundedFinalAnswerRate": 1,
    "escalationEvaluatedCases": 2,
    "escalationPassingCases": 2,
    "escalationComplianceRate": 1
  }
}
JSON

  node scripts/audit-ai-response-quality.mjs --report "$report_json" >"$tmp_dir/pass.log"
  grep -q '\[ai-response-quality\] OK:' "$tmp_dir/pass.log"

  cat >"$report_json" <<'JSON'
{
  "casesPath": "tests/ai-eval/cases.functional.json",
  "summary": {
    "totalCases": 6,
    "totalFalseSuccessClaims": 1,
    "falseSuccessClaimRate": 0.167,
    "groundedAnswerEvaluatedCases": 1,
    "groundedAnswerPassingCases": 0,
    "groundedFinalAnswerRate": 0,
    "escalationEvaluatedCases": 1,
    "escalationPassingCases": 0,
    "escalationComplianceRate": 0
  }
}
JSON

  set +e
  node scripts/audit-ai-response-quality.mjs --report "$report_json" >"$tmp_dir/fail.log" 2>&1
  local status=$?
  set -e
  if [[ "$status" -eq 0 ]]; then
    echo "[test-ai-eval-runner] expected response-quality audit to fail, got status=0" >&2
    exit 1
  fi
  grep -q '\[ai-response-quality\] FAIL' "$tmp_dir/fail.log"
  grep -q 'falseSuccessClaimRate=0.167 exceeds maxFalseSuccessClaimRate=0' "$tmp_dir/fail.log"
  grep -q 'groundedFinalAnswerRate=0 below minGroundedFinalAnswerRate=1' "$tmp_dir/fail.log"
  grep -q 'escalationComplianceRate=0 below minEscalationComplianceRate=1' "$tmp_dir/fail.log"

  mkdir -p "$tmp_dir/results"
  cat >"$tmp_dir/results/report-older-compatible.json" <<'JSON'
{
  "createdAt": "2026-03-13T09:55:00Z",
  "casesPath": "tests/ai-eval/cases.functional.json",
  "summary": {
    "totalCases": 6,
    "totalFalseSuccessClaims": 0,
    "falseSuccessClaimRate": 0,
    "groundedAnswerEvaluatedCases": 2,
    "groundedAnswerPassingCases": 2,
    "groundedFinalAnswerRate": 1,
    "escalationEvaluatedCases": 1,
    "escalationPassingCases": 1,
    "escalationComplianceRate": 1
  }
}
JSON
  cat >"$tmp_dir/results/report-newer-incompatible.json" <<'JSON'
{
  "createdAt": "2026-03-13T10:00:00Z",
  "casesPath": "tests/ai-eval/cases.functional.json",
  "summary": {
    "totalCases": 6,
    "passRate": 1
  }
}
JSON

  node scripts/audit-ai-response-quality.mjs --results-dir "$tmp_dir/results" >"$tmp_dir/fallback.log"
  grep -q '\[ai-response-quality\] OK:' "$tmp_dir/fallback.log"
  grep -q 'report=.*report-older-compatible.json' "$tmp_dir/fallback.log"
  grep -q 'latestIncompatibleReport=.*report-newer-incompatible.json' "$tmp_dir/fallback.log"
}

run_trace_quality_audit_case() {
  local tmp_dir="$TMP_DIR/trace-quality-audit"
  local report_json="$tmp_dir/report.json"
  local audit_jsonl="$tmp_dir/session-trace-quality.jsonl"
  mkdir -p "$tmp_dir"

  cat >"$report_json" <<'JSON'
{
  "casesPath": "tests/ai-eval/cases.functional.json",
  "cases": [
    {
      "id": "clarify-dataset-id",
      "requestIds": ["req-trace-quality-1"],
      "responseModeChecks": {
        "evaluated": true,
        "pass": true,
        "expectedMode": "clarification",
        "observedMode": "clarification",
        "requiredMarkers": ["datasetId"],
        "matchedMarkers": ["datasetId"]
      },
      "groundedFinalAnswerChecks": {
        "evaluated": false,
        "pass": true
      }
    },
    {
      "id": "grounded-postcreate",
      "requestIds": ["req-trace-quality-2"],
      "responseModeChecks": {
        "evaluated": false,
        "pass": true
      },
      "groundedFinalAnswerChecks": {
        "evaluated": true,
        "pass": true,
        "hasDatasetMutation": true,
        "postCreateWaitCountOk": true,
        "requiredToolsAll": ["createDatasetFromFilter", "countQMapRows"]
      }
    }
  ]
}
JSON

  cat >"$audit_jsonl" <<'JSONL'
{"requestId":"req-trace-quality-1","ts":"2026-03-13T11:00:00Z","status":200,"outcome":"success","responseText":"Quale datasetId vuoi usare per continuare?","requestToolResults":[],"qualityMetrics":{"falseSuccessClaimCount":0,"contractSchemaMismatchCount":0,"hasDatasetMutation":false,"postCreateWaitCountOk":false,"workflowScore":95}}
{"requestId":"req-trace-quality-2","ts":"2026-03-13T11:00:02Z","status":200,"outcome":"success","responseText":"Ho creato il dataset derivato e validato 42 righe prima di concludere.","requestToolResults":[{"toolName":"createDatasetFromFilter","success":true},{"toolName":"waitForQMapDataset","success":true},{"toolName":"countQMapRows","success":true}],"qualityMetrics":{"falseSuccessClaimCount":0,"contractSchemaMismatchCount":0,"hasDatasetMutation":true,"postCreateWaitCountOk":true,"workflowScore":92}}
JSONL

  node scripts/audit-ai-trace-quality.mjs --report "$report_json" --audit-dirs "$tmp_dir" >"$tmp_dir/pass.log"
  grep -q '\[ai-trace-quality\] OK:' "$tmp_dir/pass.log"
  grep -q 'groundedFinalAnswerRate=1' "$tmp_dir/pass.log"
  grep -q 'escalationComplianceRate=1' "$tmp_dir/pass.log"

  cat >"$audit_jsonl" <<'JSONL'
{"requestId":"req-trace-quality-1","ts":"2026-03-13T11:00:00Z","status":200,"outcome":"success","responseText":"Posso procedere appena mi confermi i dettagli.","requestToolResults":[],"qualityMetrics":{"falseSuccessClaimCount":0,"contractSchemaMismatchCount":0,"hasDatasetMutation":false,"postCreateWaitCountOk":false,"workflowScore":95}}
{"requestId":"req-trace-quality-2","ts":"2026-03-13T11:00:02Z","status":200,"outcome":"success","responseText":"Ho creato il dataset derivato e validato 42 righe prima di concludere.","requestToolResults":[{"toolName":"createDatasetFromFilter","success":true},{"toolName":"waitForQMapDataset","success":true}],"qualityMetrics":{"falseSuccessClaimCount":1,"contractSchemaMismatchCount":0,"hasDatasetMutation":true,"postCreateWaitCountOk":true,"workflowScore":92}}
JSONL

  set +e
  node scripts/audit-ai-trace-quality.mjs --report "$report_json" --audit-dirs "$tmp_dir" >"$tmp_dir/fail.log" 2>&1
  local status=$?
  set -e
  if [[ "$status" -eq 0 ]]; then
    echo "[test-ai-eval-runner] expected trace-quality audit to fail, got status=0" >&2
    exit 1
  fi
  grep -q '\[ai-trace-quality\] FAIL' "$tmp_dir/fail.log"
  grep -q 'trace escalation mismatch expected=clarification observed=' "$tmp_dir/fail.log"
  grep -q 'grounded trace falseSuccessClaimCount=1' "$tmp_dir/fail.log"
  grep -q 'grounded trace missing required tool evidence=countqmaprows' "$tmp_dir/fail.log"

  mkdir -p "$tmp_dir/results"
  cat >"$tmp_dir/results/report-older-compatible.json" <<'JSON'
{
  "createdAt": "2026-03-13T09:55:00Z",
  "casesPath": "tests/ai-eval/cases.functional.json",
  "cases": [
    {
      "id": "clarify-dataset-id",
      "requestIds": ["req-fallback-trace-quality-1"],
      "responseModeChecks": {
        "evaluated": true,
        "pass": true,
        "expectedMode": "clarification",
        "observedMode": "clarification",
        "requiredMarkers": ["datasetId"],
        "matchedMarkers": ["datasetId"]
      },
      "groundedFinalAnswerChecks": {
        "evaluated": false,
        "pass": true
      }
    },
    {
      "id": "grounded-fallback",
      "requestIds": ["req-fallback-trace-quality-2"],
      "responseModeChecks": {
        "evaluated": false,
        "pass": true
      },
      "groundedFinalAnswerChecks": {
        "evaluated": true,
        "pass": true,
        "hasDatasetMutation": true,
        "postCreateWaitCountOk": true,
        "requiredToolsAll": ["createDatasetFromFilter", "countQMapRows"]
      }
    }
  ]
}
JSON
  cat >"$tmp_dir/results/report-newer-incompatible.json" <<'JSON'
{
  "createdAt": "2026-03-13T10:00:00Z",
  "casesPath": "tests/ai-eval/cases.functional.json",
  "cases": [
    {
      "id": "legacy-row",
      "requestIds": ["req-newer-legacy"]
    }
  ]
}
JSON
  cat >"$tmp_dir/session-fallback-trace-quality.jsonl" <<'JSONL'
{"requestId":"req-fallback-trace-quality-1","ts":"2026-03-13T09:55:02Z","status":200,"outcome":"success","responseText":"Quale datasetId vuoi usare per continuare?","requestToolResults":[],"qualityMetrics":{"falseSuccessClaimCount":0,"contractSchemaMismatchCount":0,"hasDatasetMutation":false,"postCreateWaitCountOk":false,"workflowScore":95}}
{"requestId":"req-fallback-trace-quality-2","ts":"2026-03-13T09:55:03Z","status":200,"outcome":"success","responseText":"Ho creato il dataset derivato e validato 42 righe prima di concludere.","requestToolResults":[{"toolName":"createDatasetFromFilter","success":true},{"toolName":"waitForQMapDataset","success":true},{"toolName":"countQMapRows","success":true}],"qualityMetrics":{"falseSuccessClaimCount":0,"contractSchemaMismatchCount":0,"hasDatasetMutation":true,"postCreateWaitCountOk":true,"workflowScore":92}}
JSONL

  node scripts/audit-ai-trace-quality.mjs --results-dir "$tmp_dir/results" --audit-dirs "$tmp_dir" >"$tmp_dir/fallback.log"
  grep -q '\[ai-trace-quality\] OK:' "$tmp_dir/fallback.log"
  grep -q 'report=.*report-older-compatible.json' "$tmp_dir/fallback.log"
  grep -q 'latestIncompatibleReport=.*report-newer-incompatible.json' "$tmp_dir/fallback.log"
}

run_passk_audit_case() {
  local tmp_dir="$TMP_DIR/passk-audit"
  mkdir -p "$tmp_dir/results"

  cat >"$tmp_dir/results/report-1.json" <<'JSON'
{
  "runId": "adv-run-1",
  "runType": "adversarial",
  "createdAt": "2026-03-12T18:00:00Z",
  "casesPath": "tests/ai-eval/cases.adversarial.json",
  "cases": [
    {"id": "critical_case", "criticality": "critical", "pass": false, "metrics": {"caseScore": 0.4}},
    {"id": "standard_case", "criticality": "standard", "pass": false, "metrics": {"caseScore": 0.3}}
  ]
}
JSON

  cat >"$tmp_dir/results/report-2.json" <<'JSON'
{
  "runId": "adv-run-2",
  "runType": "adversarial",
  "createdAt": "2026-03-12T18:01:00Z",
  "casesPath": "tests/ai-eval/cases.adversarial.json",
  "cases": [
    {"id": "critical_case", "criticality": "critical", "pass": true, "metrics": {"caseScore": 0.9}},
    {"id": "standard_case", "criticality": "standard", "pass": false, "metrics": {"caseScore": 0.35}}
  ]
}
JSON

  cat >"$tmp_dir/results/report-3.json" <<'JSON'
{
  "runId": "adv-run-3",
  "runType": "adversarial",
  "createdAt": "2026-03-12T18:02:00Z",
  "casesPath": "tests/ai-eval/cases.adversarial.json",
  "cases": [
    {"id": "critical_case", "criticality": "critical", "pass": false, "metrics": {"caseScore": 0.45}},
    {"id": "standard_case", "criticality": "standard", "pass": true, "metrics": {"caseScore": 0.88}}
  ]
}
JSON

  node scripts/audit-ai-passk-reliability.mjs --results-dir "$tmp_dir/results" >"$tmp_dir/pass.log"
  grep -q '\[ai-passk\] OK:' "$tmp_dir/pass.log"

  cat >"$tmp_dir/results/report-2.json" <<'JSON'
{
  "runId": "adv-run-2",
  "runType": "adversarial",
  "createdAt": "2026-03-12T18:01:00Z",
  "casesPath": "tests/ai-eval/cases.adversarial.json",
  "cases": [
    {"id": "critical_case", "criticality": "critical", "pass": false, "metrics": {"caseScore": 0.2}},
    {"id": "standard_case", "criticality": "standard", "pass": false, "metrics": {"caseScore": 0.35}}
  ]
}
JSON

  set +e
  node scripts/audit-ai-passk-reliability.mjs --results-dir "$tmp_dir/results" >"$tmp_dir/fail.log" 2>&1
  local status=$?
  set -e
  if [[ "$status" -eq 0 ]]; then
    echo "[test-ai-eval-runner] expected pass^k audit to fail, got status=0" >&2
    exit 1
  fi
  grep -q '\[ai-passk\] FAIL' "$tmp_dir/fail.log"
  grep -q 'critical_case' "$tmp_dir/fail.log"
}

run_preflight_case
run_threshold_case
run_threshold_two_case
run_trace_grade_case
run_deterministic_constraints_case
run_mock_tool_override_case
run_operational_audit_case
run_response_quality_audit_case
run_trace_quality_audit_case
run_passk_audit_case
echo "[test-ai-eval-runner] OK"
