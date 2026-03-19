#!/usr/bin/env node

import fs from 'node:fs';
import path from 'node:path';
import {fileURLToPath} from 'node:url';

const QMAP_ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');

function resolveQMapPath(target) {
  return path.isAbsolute(target) ? target : path.resolve(QMAP_ROOT, target);
}

function fail(message) {
  console.error(`[ai-trace-grade] FAIL: ${message}`);
  process.exit(2);
}

function parseArgs(argv) {
  const out = {
    resultsDir: resolveQMapPath('tests/ai-eval/results'),
    reportPath: String(process.env.QMAP_TRACE_GRADE_REPORT || '').trim(),
    minWorkflowScore: Number(process.env.QMAP_TRACE_GRADE_MIN_WORKFLOW_SCORE || 75),
    minCloudWorkflowScore: Number(process.env.QMAP_TRACE_GRADE_MIN_CLOUD_WORKFLOW_SCORE || 85),
    requireMutationValidation:
      String(process.env.QMAP_TRACE_GRADE_REQUIRE_MUTATION_VALIDATION || 'false').trim().toLowerCase() === 'true',
    requireRankingValidation:
      String(process.env.QMAP_TRACE_GRADE_REQUIRE_RANKING_VALIDATION || 'false').trim().toLowerCase() === 'true',
    auditDirs: String(process.env.QMAP_TRACE_GRADE_AUDIT_DIRS || '')
      .split(/[,;\n]+/)
      .map(value => value.trim())
      .filter(Boolean)
  };

  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    const next = argv[i + 1];
    if (arg === '--results-dir' && next) {
      out.resultsDir = resolveQMapPath(next);
      i += 1;
      continue;
    }
    if (arg === '--report' && next) {
      out.reportPath = String(next).trim();
      i += 1;
      continue;
    }
    if (arg === '--min-workflow-score' && next) {
      out.minWorkflowScore = Number(next);
      i += 1;
      continue;
    }
    if (arg === '--min-cloud-workflow-score' && next) {
      out.minCloudWorkflowScore = Number(next);
      i += 1;
      continue;
    }
    if (arg === '--audit-dirs' && next) {
      out.auditDirs.push(
        ...String(next)
          .split(/[,;\n]+/)
          .map(value => value.trim())
          .filter(Boolean)
      );
      i += 1;
    }
  }

  const defaultAuditDirs = [
    resolveQMapPath('backends/logs/q-assistant/chat-audit'),
    resolveQMapPath('test-results/assistant-live/chat-audit'),
    resolveQMapPath('backends/logs')
  ];
  out.auditDirs = Array.from(
    new Set((out.auditDirs.length ? out.auditDirs : defaultAuditDirs).map(value => resolveQMapPath(value)))
  );
  return out;
}

function readJson(filePath) {
  return JSON.parse(fs.readFileSync(filePath, 'utf8'));
}

function resolveLatestReportPath(resultsDir) {
  let names = [];
  try {
    names = fs.readdirSync(resultsDir);
  } catch {
    fail(`results dir not found: ${resultsDir}`);
  }
  const candidates = names
    .filter(name => /^report-.*\.json$/i.test(name))
    .map(name => {
      const fullPath = path.join(resultsDir, name);
      const stat = fs.statSync(fullPath);
      return {fullPath, mtimeMs: stat.mtimeMs};
    })
    .sort((a, b) => b.mtimeMs - a.mtimeMs);
  if (!candidates.length) {
    fail(`no report-*.json found in ${resultsDir}`);
  }
  return candidates[0].fullPath;
}

function parseJsonLines(raw) {
  return String(raw || '')
    .split(/\r?\n/)
    .map(line => line.trim())
    .filter(Boolean)
    .map(line => {
      try {
        return JSON.parse(line);
      } catch {
        return null;
      }
    })
    .filter(Boolean);
}

function collectSessionAuditFiles(rootPath) {
  let stats = null;
  try {
    stats = fs.statSync(rootPath);
  } catch {
    return [];
  }

  if (stats.isFile()) {
    return /^session-.*\.jsonl$/i.test(path.basename(rootPath)) ? [rootPath] : [];
  }
  if (!stats.isDirectory()) return [];

  let names = [];
  try {
    names = fs.readdirSync(rootPath);
  } catch {
    return [];
  }
  return names
    .filter(name => /^session-.*\.jsonl$/i.test(name))
    .map(name => path.join(rootPath, name));
}

function collectAuditEventsByRequestId(requestIds, auditDirs) {
  const wanted = new Set(requestIds);
  const byRequestId = new Map();
  for (const dir of auditDirs) {
    for (const filePath of collectSessionAuditFiles(dir)) {
      let raw = '';
      try {
        raw = fs.readFileSync(filePath, 'utf8');
      } catch {
        continue;
      }
      for (const event of parseJsonLines(raw)) {
        const requestId = String(event?.requestId || '').trim();
        if (!requestId || !wanted.has(requestId)) continue;
        const existing = byRequestId.get(requestId);
        if (!existing || String(event?.ts || '') > String(existing?.ts || '')) {
          byRequestId.set(requestId, event);
        }
      }
    }
  }
  return byRequestId;
}

function uniqueStrings(values) {
  return Array.from(new Set(values.map(value => String(value || '').trim()).filter(Boolean)));
}

function intersects(values, candidates) {
  const set = new Set(values.map(value => String(value || '').trim()));
  return candidates.some(candidate => set.has(candidate));
}

function average(values) {
  if (!values.length) return null;
  return values.reduce((sum, value) => sum + value, 0) / values.length;
}

function isFiniteNumber(value) {
  return Number.isFinite(Number(value));
}

function evaluateCaseTrace(caseRow, eventMap, opts) {
  const issues = [];
  const requestIds = uniqueStrings(caseRow?.requestIds || []);
  const matchedEvents = requestIds.map(requestId => eventMap.get(requestId)).filter(Boolean);
  const missingRequestIds = requestIds.filter(requestId => !eventMap.has(requestId));

  if (!requestIds.length) {
    issues.push('missing requestIds in ai-eval report');
  }
  if (missingRequestIds.length) {
    issues.push(`missing chat-audit events for requestIds=${missingRequestIds.join(', ')}`);
  }

  const toolCalls = Array.isArray(caseRow?.toolCalls) ? caseRow.toolCalls : [];
  const workflowScores = matchedEvents
    .map(event => Number(event?.qualityMetrics?.workflowScore))
    .filter(value => Number.isFinite(value));
  const avgWorkflowScore = average(workflowScores);
  const falseSuccessClaimCount = matchedEvents.reduce(
    (sum, event) => sum + Number(event?.qualityMetrics?.falseSuccessClaimCount || 0),
    0
  );
  const contractSchemaMismatchCount = matchedEvents.reduce(
    (sum, event) => sum + Number(event?.qualityMetrics?.contractSchemaMismatchCount || 0),
    0
  );
  const hasDatasetMutation =
    matchedEvents.some(event => event?.qualityMetrics?.hasDatasetMutation === true) ||
    intersects(toolCalls, [
      'createDatasetFromFilter',
      'createDatasetFromCurrentFilters',
      'createDatasetWithGeometryArea',
      'createDatasetWithNormalizedField',
      'mergeQMapDatasets',
      'clipQMapDatasetByGeometry',
      'clipDatasetByBoundary',
      'overlayDifference',
      'overlayUnion',
      'overlayIntersection',
      'overlaySymmetricDifference',
      'spatialJoinByPredicate',
      'zonalStatsByAdmin',
      'tassellateSelectedGeometry',
      'tassellateDatasetLayer',
      'aggregateDatasetToH3',
      'joinQMapDatasetsOnH3',
      'populateTassellationFromAdminUnits',
      'populateTassellationFromAdminUnitsAreaWeighted',
      'populateTassellationFromAdminUnitsDiscrete',
      'loadData'
    ]);
  const hasMutationValidation = matchedEvents.some(
    event =>
      event?.qualityMetrics?.postCreateWaitOk === true ||
      event?.qualityMetrics?.postCreateWaitCountOk === true ||
      event?.qualityMetrics?.postCreateWaitCountRankOk === true
  );
  const needsRankingValidation = toolCalls.includes('rankQMapDatasetRows') && hasDatasetMutation;
  const hasRankingValidation = matchedEvents.some(
    event => event?.qualityMetrics?.postCreateWaitCountRankOk === true
  );

  if (matchedEvents.some(event => Number(event?.status || 0) >= 400)) {
    issues.push('chat-audit includes non-2xx status');
  }
  if (matchedEvents.some(event => String(event?.outcome || '').trim() === 'error')) {
    issues.push('chat-audit outcome=error present in case trace');
  }
  if (falseSuccessClaimCount > 0) {
    issues.push(`falseSuccessClaimCount=${falseSuccessClaimCount}`);
  }
  if (contractSchemaMismatchCount > 0) {
    issues.push(`contractSchemaMismatchCount=${contractSchemaMismatchCount}`);
  }
  if (opts.requireMutationValidation && hasDatasetMutation && !hasMutationValidation) {
    issues.push('dataset mutation trace missing post-create validation evidence');
  }
  if (opts.requireRankingValidation && needsRankingValidation && !hasRankingValidation) {
    issues.push('ranking trace missing postCreateWaitCountRankOk evidence');
  }

  const workflowFloor = String(caseRow?.id || '').startsWith('arch_cloud_')
    ? opts.minCloudWorkflowScore
    : opts.minWorkflowScore;
  if (isFiniteNumber(workflowFloor)) {
    if (!workflowScores.length) {
      issues.push('missing workflowScore in matched trace events');
    } else if (Number(avgWorkflowScore) < Number(workflowFloor)) {
      issues.push(`avgWorkflowScore=${Number(avgWorkflowScore).toFixed(2)} < minWorkflowScore=${Number(workflowFloor)}`);
    }
  }

  return {
    id: String(caseRow?.id || ''),
    criticality: String(caseRow?.criticality || 'standard'),
    requestIds,
    matchedEvents: matchedEvents.length,
    avgWorkflowScore: avgWorkflowScore === null ? null : Number(avgWorkflowScore.toFixed(2)),
    falseSuccessClaimCount,
    contractSchemaMismatchCount,
    hasDatasetMutation,
    hasMutationValidation,
    needsRankingValidation,
    hasRankingValidation,
    issues,
    pass: issues.length === 0
  };
}

function main() {
  const opts = parseArgs(process.argv.slice(2));
  const reportPath = opts.reportPath
    ? resolveQMapPath(opts.reportPath)
    : resolveLatestReportPath(opts.resultsDir);
  const report = readJson(reportPath);
  const cases = Array.isArray(report?.cases) ? report.cases : null;
  if (!cases) {
    fail(`invalid ai-eval report: ${reportPath}`);
  }
  if (!String(report?.casesPath || '').includes('cases.functional.json')) {
    fail(`trace grading requires a functional ai-eval report, got casesPath=${String(report?.casesPath || '-')}`);
  }

  const highRiskCases = cases.filter(caseRow => String(caseRow?.criticality || 'standard') === 'critical');
  if (!highRiskCases.length) {
    fail(`no critical cases found in report: ${reportPath}`);
  }

  const allRequestIds = uniqueStrings(highRiskCases.flatMap(caseRow => caseRow?.requestIds || []));
  const eventMap = collectAuditEventsByRequestId(allRequestIds, opts.auditDirs);
  const evaluatedCases = highRiskCases.map(caseRow => evaluateCaseTrace(caseRow, eventMap, opts));
  const failedCases = evaluatedCases.filter(caseRow => !caseRow.pass);
  const tracedCases = evaluatedCases.filter(caseRow => caseRow.matchedEvents > 0).length;

  if (failedCases.length) {
    console.error('[ai-trace-grade] FAIL');
    console.error(
      `[ai-trace-grade] report=${path.relative(QMAP_ROOT, reportPath)} criticalCases=${highRiskCases.length} tracedCases=${tracedCases}`
    );
    for (const caseRow of failedCases) {
      console.error(
        `[ai-trace-grade] case=${caseRow.id} requestIds=${caseRow.requestIds.join(',') || '-'} issues=${caseRow.issues.join(' | ')}`
      );
    }
    process.exit(2);
  }

  console.log(
    `[ai-trace-grade] OK: report=${path.relative(QMAP_ROOT, reportPath)} criticalCases=${highRiskCases.length} tracedCases=${tracedCases} avgWorkflowScore=${Number(average(evaluatedCases.map(caseRow => Number(caseRow.avgWorkflowScore || 0))) || 0).toFixed(2)}`
  );
}

main();
