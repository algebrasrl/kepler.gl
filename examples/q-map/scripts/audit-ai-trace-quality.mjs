#!/usr/bin/env node

import fs from 'node:fs';
import path from 'node:path';
import {fileURLToPath} from 'node:url';

const QMAP_ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');

const CLARIFICATION_HINTS = [
  'quale',
  'which',
  'specifica il',
  'specifica la',
  'specifica quale',
  'specificare',
  'specify',
  'chiarisci',
  'chiarimento',
  'clarif',
  'scegli',
  'choose',
  'indica il',
  'indica la',
  'indica quale',
  'indicare',
  'select',
  'datasetid',
  'vuoi',
  'preferisci'
];

const LIMITATION_HINTS = [
  'non posso',
  'non e possibile',
  'non e stato possibile',
  'non applicabile',
  'impossibile',
  'unable',
  'cannot',
  'limite',
  'limitazione',
  'ambigu',
  'serve un chiarimento',
  'need clarification'
];

function resolveQMapPath(target) {
  return path.isAbsolute(target) ? target : path.resolve(QMAP_ROOT, target);
}

function fail(message) {
  console.error(`[ai-trace-quality] FAIL: ${message}`);
}

function readJson(filePath) {
  return JSON.parse(fs.readFileSync(filePath, 'utf8'));
}

function round(value) {
  return Number(Number(value || 0).toFixed(3));
}

function optionalNumber(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : undefined;
}

function asFiniteNumber(value, fallback = undefined) {
  if (value === null || value === undefined || value === '') return fallback;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function isPlainObject(value) {
  return Boolean(value) && typeof value === 'object' && !Array.isArray(value);
}

function normalizeForMatch(text) {
  return String(text || '')
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function includesAnyPhrase(normalizedContent, phrases) {
  return (Array.isArray(phrases) ? phrases : []).some(phrase => {
    const normalized = normalizeForMatch(phrase);
    return normalized ? normalizedContent.includes(normalized) : false;
  });
}

function textAcknowledgesNonSuccessOutcome(text) {
  const normalized = String(text || '').trim();
  if (!normalized) return false;
  return /(\bworkflow\s+completat\w*\s+parzial\w*\b|\bworkflow\s+non\s+completat\w*\b|\bparzial\w*\b|\bnon\s+completat\w*\b|\bfallit\w*\b|\bfailed\b|\berror\w*\b|\berrore\w*\b|\bnon\s+riuscit\w*\b|\blimite\s+rilevat\w*\b|\bnon\s+applicabil\w*\b|\bnon\s+disponibil\w*\b)/i.test(
    normalized
  );
}

function uniqueStrings(values) {
  return Array.from(new Set((Array.isArray(values) ? values : []).map(value => String(value || '').trim()).filter(Boolean)));
}

function average(values) {
  if (!values.length) return null;
  return values.reduce((sum, value) => sum + value, 0) / values.length;
}

function normalizeQualityMetrics(value) {
  if (!isPlainObject(value)) return null;
  const workflowScore = optionalNumber(value.workflowScore);
  const falseSuccessClaimCount = Math.max(0, Math.trunc(Number(value.falseSuccessClaimCount || 0)));
  const contractSchemaMismatchCount = Math.max(
    0,
    Math.trunc(Number(value.contractSchemaMismatchCount || 0))
  );
  const contractResponseMismatchCount = Math.max(
    0,
    Math.trunc(Number(value.contractResponseMismatchCount || 0))
  );
  const clarificationReason = String(value.clarificationReason || '').trim() || undefined;
  const clarificationOptionsCount = optionalNumber(value.clarificationOptionsCount);
  return {
    falseSuccessClaimCount,
    contractSchemaMismatchCount,
    contractResponseMismatchCount,
    hasDatasetMutation: value.hasDatasetMutation === true,
    postCreateWaitCountOk: value.postCreateWaitCountOk === true,
    postCreateWaitCountRankOk: value.postCreateWaitCountRankOk === true,
    cloudFailureSeen: value.cloudFailureSeen === true,
    cloudFailureExhausted: value.cloudFailureExhausted === true,
    cloudRecoveryValidated: value.cloudRecoveryValidated === true,
    clarificationPending: value.clarificationPending === true,
    clarificationReason,
    clarificationQuestionSeen: value.clarificationQuestionSeen === true,
    clarificationOptionsCount: clarificationOptionsCount === undefined ? undefined : round(clarificationOptionsCount),
    responseModeHint: ['clarification', 'limitation'].includes(
      String(value.responseModeHint || '').trim().toLowerCase()
    )
      ? String(value.responseModeHint || '').trim().toLowerCase()
      : undefined,
    workflowScore: workflowScore === undefined ? undefined : round(workflowScore)
  };
}

function parseArgs(argv) {
  const out = {
    resultsDir: resolveQMapPath('tests/ai-eval/results'),
    reportPath: String(process.env.QMAP_TRACE_QUALITY_REPORT || '').trim(),
    casesSuffix: String(process.env.QMAP_TRACE_QUALITY_CASES_SUFFIX || 'tests/ai-eval/cases.functional.json').trim(),
    maxFalseSuccessClaimRate: asFiniteNumber(process.env.QMAP_AI_EVAL_MAX_FALSE_SUCCESS_CLAIM_RATE, 0),
    minGroundedFinalAnswerRate: asFiniteNumber(process.env.QMAP_AI_EVAL_MIN_GROUNDED_FINAL_ANSWER_RATE, 1),
    minEscalationComplianceRate: asFiniteNumber(process.env.QMAP_AI_EVAL_MIN_ESCALATION_COMPLIANCE_RATE, 1),
    minGroundedAnswerEvaluatedCases: Math.max(
      0,
      Math.trunc(asFiniteNumber(process.env.QMAP_AI_EVAL_MIN_GROUNDED_ANSWER_EVALUATED_CASES, 1))
    ),
    minEscalationEvaluatedCases: Math.max(
      0,
      Math.trunc(asFiniteNumber(process.env.QMAP_AI_EVAL_MIN_ESCALATION_EVALUATED_CASES, 1))
    ),
    auditDirs: String(process.env.QMAP_TRACE_QUALITY_AUDIT_DIRS || '')
      .split(/[,;\n]+/)
      .map(value => value.trim())
      .filter(Boolean)
  };

  for (let i = 0; i < argv.length; i += 1) {
    const arg = String(argv[i] || '');
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
    if (arg === '--cases-suffix' && next) {
      out.casesSuffix = String(next).trim();
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
      continue;
    }
    if (arg === '--max-false-success-claim-rate' && next) {
      out.maxFalseSuccessClaimRate = asFiniteNumber(next, out.maxFalseSuccessClaimRate);
      i += 1;
      continue;
    }
    if (arg === '--min-grounded-final-answer-rate' && next) {
      out.minGroundedFinalAnswerRate = asFiniteNumber(next, out.minGroundedFinalAnswerRate);
      i += 1;
      continue;
    }
    if (arg === '--min-escalation-compliance-rate' && next) {
      out.minEscalationComplianceRate = asFiniteNumber(next, out.minEscalationComplianceRate);
      i += 1;
      continue;
    }
    if (arg === '--min-grounded-answer-evaluated-cases' && next) {
      out.minGroundedAnswerEvaluatedCases = Math.max(
        0,
        Math.trunc(asFiniteNumber(next, out.minGroundedAnswerEvaluatedCases))
      );
      i += 1;
      continue;
    }
    if (arg === '--min-escalation-evaluated-cases' && next) {
      out.minEscalationEvaluatedCases = Math.max(
        0,
        Math.trunc(asFiniteNumber(next, out.minEscalationEvaluatedCases))
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

function hasTraceQualityChecks(caseRow) {
  return Boolean(caseRow?.responseModeChecks?.evaluated || caseRow?.groundedFinalAnswerChecks?.evaluated);
}

function isCompatibleTraceQualityReport(report, casesSuffix) {
  if (!String(report?.casesPath || '').endsWith(casesSuffix)) return false;
  const cases = Array.isArray(report?.cases) ? report.cases : [];
  return cases.some(caseRow => {
    const requestIds = uniqueStrings(caseRow?.requestIds || []);
    return requestIds.length > 0 && hasTraceQualityChecks(caseRow);
  });
}

function resolveLatestReport(resultsDir, casesSuffix) {
  if (!fs.existsSync(resultsDir)) {
    fail(`results dir not found: ${resultsDir}`);
    process.exit(1);
  }
  const rows = fs
    .readdirSync(resultsDir)
    .filter(name => /^report-.*\.json$/i.test(name))
    .map(name => {
      const fullPath = path.join(resultsDir, name);
      try {
        const report = readJson(fullPath);
        const createdAtMs = Date.parse(String(report?.createdAt || '')) || fs.statSync(fullPath).mtimeMs || 0;
        return {
          fullPath,
          createdAtMs,
          compatible: isCompatibleTraceQualityReport(report, casesSuffix)
        };
      } catch {
        return null;
      }
    })
    .filter(Boolean)
    .sort((a, b) => b.createdAtMs - a.createdAtMs);

  if (!rows.length) {
    fail(`no report-*.json found in ${resultsDir} for casesSuffix=${casesSuffix}`);
    process.exit(1);
  }

  const compatibleRow = rows.find(row => row.compatible);
  return {
    selectedPath: compatibleRow?.fullPath || rows[0].fullPath,
    latestPath: rows[0].fullPath,
    compatible: Boolean(compatibleRow),
    selectedIsLatest: Boolean(compatibleRow && compatibleRow.fullPath === rows[0].fullPath)
  };
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

function deriveObservedMode(content, options = {}) {
  const runtimeResponseModeHint = String(options.runtimeResponseModeHint || '').trim().toLowerCase();
  if (runtimeResponseModeHint === 'clarification' || runtimeResponseModeHint === 'limitation') {
    return runtimeResponseModeHint;
  }
  const rawContent = String(content || '');
  const normalizedContent = normalizeForMatch(rawContent);
  const hasQuestionMark = rawContent.includes('?');
  const clarificationCue =
    hasQuestionMark || includesAnyPhrase(normalizedContent, CLARIFICATION_HINTS);
  const limitationCue =
    includesAnyPhrase(normalizedContent, LIMITATION_HINTS) || textAcknowledgesNonSuccessOutcome(rawContent);
  const failedToolEvidence = options.failedToolEvidence === true;
  if ((limitationCue || failedToolEvidence) && !hasQuestionMark) return 'limitation';
  if (clarificationCue) return 'clarification';
  if (limitationCue) return 'limitation';
  return '';
}

function evaluateExpectedResponseMode(checks, content, options = {}) {
  const expectedMode = String(checks?.expectedMode || '').trim().toLowerCase();
  const normalizedContent = normalizeForMatch(content);
  const requiredMarkers = uniqueStrings(checks?.requiredMarkers || []);
  const matchedMarkers = requiredMarkers.filter(marker => includesAnyPhrase(normalizedContent, [marker]));
  const observedMode = deriveObservedMode(content, options);
  const nonSuccessAcknowledged = textAcknowledgesNonSuccessOutcome(content);
  const structuralLimitationPass =
    expectedMode === 'limitation' && (options.failedToolEvidence === true || nonSuccessAcknowledged);
  const structuralClarificationPass =
    expectedMode === 'clarification' &&
    String(options.runtimeResponseModeHint || '').trim().toLowerCase() === 'clarification';
  const markersPass =
    requiredMarkers.length === 0 ||
    matchedMarkers.length > 0 ||
    structuralLimitationPass ||
    structuralClarificationPass;
  return {
    expectedMode,
    observedMode,
    requiredMarkers,
    matchedMarkers,
    runtimeResponseModeHint: String(options.runtimeResponseModeHint || '').trim().toLowerCase(),
    failedToolEvidence: options.failedToolEvidence === true,
    nonSuccessAcknowledged,
    structuralClarificationPass,
    structuralLimitationPass,
    pass: Boolean(expectedMode) && observedMode === expectedMode && markersPass
  };
}

function getToolResultName(row) {
  return String(
    row?.toolName ??
      row?.tool_name ??
      row?.name ??
      row?.tool ??
      row?.function?.name ??
      row?.functionName ??
      ''
  )
    .trim()
    .toLowerCase();
}

function isSuccessfulToolResult(row) {
  if (!isPlainObject(row)) return false;
  if (row.success === true || row.ok === true) return true;
  const status = optionalNumber(row.status);
  if (status !== undefined) return status < 400;
  const outcome = String(row.outcome || '').trim().toLowerCase();
  return outcome === 'success';
}

function collectSuccessfulTraceTools(events) {
  const out = new Set();
  for (const event of events) {
    const results = Array.isArray(event?.requestToolResults) ? event.requestToolResults : [];
    for (const row of results) {
      const toolName = getToolResultName(row);
      if (!toolName || !isSuccessfulToolResult(row)) continue;
      out.add(toolName);
    }
  }
  return out;
}

function collectTraceToolResults(events) {
  const out = [];
  for (const event of events) {
    const results = Array.isArray(event?.requestToolResults) ? event.requestToolResults : [];
    for (const row of results) out.push(row);
  }
  return out;
}

function sortMatchedEvents(events, requestIds) {
  const indexMap = new Map(requestIds.map((requestId, index) => [requestId, index]));
  return [...events].sort((a, b) => {
    const tsA = String(a?.ts || '');
    const tsB = String(b?.ts || '');
    if (tsA && tsB && tsA !== tsB) return tsA.localeCompare(tsB);
    return (indexMap.get(String(a?.requestId || '')) ?? 0) - (indexMap.get(String(b?.requestId || '')) ?? 0);
  });
}

function evaluateCaseTrace(caseRow, eventMap) {
  const issues = [];
  const requestIds = uniqueStrings(caseRow?.requestIds || []);
  const matchedEvents = sortMatchedEvents(
    requestIds.map(requestId => eventMap.get(requestId)).filter(Boolean),
    requestIds
  );
  const missingRequestIds = requestIds.filter(requestId => !eventMap.has(requestId));
  const lastEvent = matchedEvents[matchedEvents.length - 1] || null;
  const finalTextEvent = [...matchedEvents].reverse().find(event => String(event?.responseText || '').trim()) || null;
  const finalText = String(finalTextEvent?.responseText || '').trim();
  const qualityMetrics = matchedEvents.map(event => normalizeQualityMetrics(event?.qualityMetrics)).filter(Boolean);
  const runtimeResponseModeHint =
    [...qualityMetrics]
      .reverse()
      .map(metrics => String(metrics?.responseModeHint || '').trim().toLowerCase())
      .find(value => value === 'clarification' || value === 'limitation') || '';
  const totalFalseSuccessClaimCount = qualityMetrics.reduce(
    (sum, metrics) => sum + Number(metrics?.falseSuccessClaimCount || 0),
    0
  );
  const totalContractSchemaMismatchCount = qualityMetrics.reduce(
    (sum, metrics) => sum + Number(metrics?.contractSchemaMismatchCount || 0),
    0
  );
  const totalContractResponseMismatchCount = qualityMetrics.reduce(
    (sum, metrics) => sum + Number(metrics?.contractResponseMismatchCount || 0),
    0
  );
  const hasDatasetMutation =
    qualityMetrics.some(metrics => metrics?.hasDatasetMutation === true) ||
    caseRow?.groundedFinalAnswerChecks?.hasDatasetMutation === true;
  const hasPostCreateWaitCountOk =
    qualityMetrics.some(metrics => metrics?.postCreateWaitCountOk === true) ||
    caseRow?.groundedFinalAnswerChecks?.postCreateWaitCountOk === true;
  const hasCloudFailureSeen =
    qualityMetrics.some(metrics => metrics?.cloudFailureSeen === true) ||
    caseRow?.groundedFinalAnswerChecks?.cloudFailureSeen === true;
  const hasCloudRecoveryValidated =
    qualityMetrics.some(metrics => metrics?.cloudRecoveryValidated === true) ||
    caseRow?.groundedFinalAnswerChecks?.cloudRecoveryValidated === true;
  const avgWorkflowScore = average(
    qualityMetrics
      .map(metrics => optionalNumber(metrics?.workflowScore))
      .filter(value => value !== undefined)
  );
  const traceToolResults = collectTraceToolResults(matchedEvents);
  const successfulTraceTools = collectSuccessfulTraceTools(matchedEvents);

  if (!requestIds.length) {
    issues.push('missing requestIds in ai-eval report');
  }
  if (missingRequestIds.length) {
    issues.push(`missing chat-audit events for requestIds=${missingRequestIds.join(', ')}`);
  }
  if (matchedEvents.some(event => Number(event?.status || 0) >= 400)) {
    issues.push('chat-audit includes non-2xx status');
  }
  if (matchedEvents.some(event => String(event?.outcome || '').trim().toLowerCase() === 'error')) {
    issues.push('chat-audit outcome=error present in case trace');
  }

  let escalationEvaluated = false;
  let escalationPass = true;
  let escalationTrace = null;
  const responseModeChecks = isPlainObject(caseRow?.responseModeChecks) ? caseRow.responseModeChecks : null;
  if (responseModeChecks?.evaluated) {
    escalationEvaluated = true;
    if (!finalText) {
      issues.push('escalation trace missing final responseText');
      escalationPass = false;
    } else {
      escalationTrace = evaluateExpectedResponseMode(responseModeChecks, finalText, {
        failedToolEvidence: traceToolResults.some(row => !isSuccessfulToolResult(row)),
        runtimeResponseModeHint
      });
      escalationPass = escalationTrace.pass;
      if (!escalationTrace.pass) {
        issues.push(
          `trace escalation mismatch expected=${escalationTrace.expectedMode || '-'} observed=${escalationTrace.observedMode || '-'} markers=${escalationTrace.matchedMarkers.join(',') || '-'}`
        );
      }
      if (responseModeChecks.pass !== escalationTrace.pass) {
        issues.push(
          `report/trace escalation mismatch reportPass=${responseModeChecks.pass === true} tracePass=${escalationTrace.pass}`
        );
      }
    }
  }

  let groundedEvaluated = false;
  let groundedPass = true;
  let groundedTrace = null;
  const groundedChecks = isPlainObject(caseRow?.groundedFinalAnswerChecks)
    ? caseRow.groundedFinalAnswerChecks
    : null;
  if (groundedChecks?.evaluated) {
    groundedEvaluated = true;
    const requiredToolsAll = uniqueStrings(groundedChecks?.requiredToolsAll || []).map(value =>
      value.toLowerCase()
    );
    const matchedRequiredToolsAll = requiredToolsAll.filter(tool => successfulTraceTools.has(tool));
    const requiredToolsPass =
      requiredToolsAll.length === 0 || matchedRequiredToolsAll.length === requiredToolsAll.length;
    groundedTrace = {
      hasText: Boolean(finalText),
      falseSuccessClaimCount: totalFalseSuccessClaimCount,
      contractSchemaMismatchCount: totalContractSchemaMismatchCount,
      contractResponseMismatchCount: totalContractResponseMismatchCount,
      hasDatasetMutation,
      postCreateWaitCountOk: hasPostCreateWaitCountOk,
      cloudFailureSeen: hasCloudFailureSeen,
      cloudRecoveryValidated: hasCloudRecoveryValidated,
      requiredToolsAll,
      matchedRequiredToolsAll,
      workflowScore: avgWorkflowScore === null ? null : Number(avgWorkflowScore.toFixed(2))
    };
    const requiresCloudValidatedRecovery =
      requiredToolsAll.includes('loadcloudmapandwait') && requiredToolsAll.includes('waitforqmapdataset');
    groundedPass =
      groundedTrace.hasText &&
      groundedTrace.falseSuccessClaimCount === 0 &&
      groundedTrace.contractSchemaMismatchCount === 0 &&
      groundedTrace.contractResponseMismatchCount === 0 &&
      (!groundedTrace.hasDatasetMutation || groundedTrace.postCreateWaitCountOk) &&
      (!requiresCloudValidatedRecovery || !groundedTrace.cloudFailureSeen || groundedTrace.cloudRecoveryValidated) &&
      requiredToolsPass;
    if (!groundedPass) {
      if (!groundedTrace.hasText) issues.push('grounded trace missing final responseText');
      if (groundedTrace.falseSuccessClaimCount > 0) {
        issues.push(`grounded trace falseSuccessClaimCount=${groundedTrace.falseSuccessClaimCount}`);
      }
      if (groundedTrace.contractSchemaMismatchCount > 0) {
        issues.push(
          `grounded trace contractSchemaMismatchCount=${groundedTrace.contractSchemaMismatchCount}`
        );
      }
      if (groundedTrace.contractResponseMismatchCount > 0) {
        issues.push(
          `grounded trace contractResponseMismatchCount=${groundedTrace.contractResponseMismatchCount}`
        );
      }
      if (groundedTrace.hasDatasetMutation && !groundedTrace.postCreateWaitCountOk) {
        issues.push('grounded trace missing postCreateWaitCountOk evidence');
      }
      if (requiresCloudValidatedRecovery && groundedTrace.cloudFailureSeen && !groundedTrace.cloudRecoveryValidated) {
        issues.push('grounded trace missing cloudRecoveryValidated evidence');
      }
      if (!requiredToolsPass) {
        issues.push(
          `grounded trace missing required tool evidence=${requiredToolsAll.filter(tool => !matchedRequiredToolsAll.includes(tool)).join(',')}`
        );
      }
    }
    if (groundedChecks.pass !== groundedPass) {
      issues.push(
        `report/trace grounded mismatch reportPass=${groundedChecks.pass === true} tracePass=${groundedPass}`
      );
    }
  }

  return {
    id: String(caseRow?.id || ''),
    requestIds,
    matchedEvents: matchedEvents.length,
    finalText,
    avgWorkflowScore: avgWorkflowScore === null ? null : Number(avgWorkflowScore.toFixed(2)),
    totalFalseSuccessClaimCount,
    escalationEvaluated,
    escalationPass,
    escalationTrace,
    groundedEvaluated,
    groundedPass,
    groundedTrace,
    issues,
    pass: issues.length === 0
  };
}

function main() {
  const opts = parseArgs(process.argv.slice(2));
  let reportPath = opts.reportPath ? resolveQMapPath(opts.reportPath) : '';
  let latestReportPath = reportPath;
  let selectedIsLatest = true;

  if (!reportPath) {
    const resolved = resolveLatestReport(opts.resultsDir, opts.casesSuffix);
    reportPath = resolved.selectedPath;
    latestReportPath = resolved.latestPath;
    selectedIsLatest = resolved.selectedIsLatest;
  }

  const report = readJson(reportPath);
  const cases = Array.isArray(report?.cases) ? report.cases : null;
  if (!cases) {
    fail(`invalid ai-eval report: ${path.relative(QMAP_ROOT, reportPath)}`);
    process.exit(1);
  }
  if (!String(report?.casesPath || '').endsWith(opts.casesSuffix)) {
    fail(`trace quality audit requires casesPath suffix=${opts.casesSuffix}, got ${String(report?.casesPath || '-')}`);
    process.exit(1);
  }

  const qualityCases = cases.filter(hasTraceQualityChecks);
  if (!qualityCases.length) {
    if (!opts.reportPath && latestReportPath === reportPath) {
      fail(`latest functional report predates trace-quality case checks: ${path.relative(QMAP_ROOT, reportPath)}`);
      fail('run ai-eval-functional to regenerate a compatible report before enforcing this gate');
      process.exit(1);
    }
    fail(`report contains no trace-quality annotated cases: ${path.relative(QMAP_ROOT, reportPath)}`);
    process.exit(1);
  }

  const allRequestIds = uniqueStrings(qualityCases.flatMap(caseRow => caseRow?.requestIds || []));
  const eventMap = collectAuditEventsByRequestId(allRequestIds, opts.auditDirs);
  const evaluatedCases = qualityCases.map(caseRow => evaluateCaseTrace(caseRow, eventMap));
  const failedCases = evaluatedCases.filter(caseRow => !caseRow.pass);
  const groundedCases = evaluatedCases.filter(caseRow => caseRow.groundedEvaluated);
  const groundedPassingCases = groundedCases.filter(caseRow => caseRow.groundedPass);
  const escalationCases = evaluatedCases.filter(caseRow => caseRow.escalationEvaluated);
  const escalationPassingCases = escalationCases.filter(caseRow => caseRow.escalationPass);
  const tracedCases = evaluatedCases.filter(caseRow => caseRow.matchedEvents > 0).length;
  const totalFalseSuccessClaimCount = groundedCases.reduce(
    (sum, caseRow) => sum + Number(caseRow.totalFalseSuccessClaimCount || 0),
    0
  );
  const falseSuccessClaimRate = groundedCases.length
    ? totalFalseSuccessClaimCount > 0
      ? groundedCases.filter(caseRow => Number(caseRow.totalFalseSuccessClaimCount || 0) > 0).length /
        groundedCases.length
      : 0
    : 0;
  const groundedFinalAnswerRate = groundedCases.length ? groundedPassingCases.length / groundedCases.length : null;
  const escalationComplianceRate = escalationCases.length
    ? escalationPassingCases.length / escalationCases.length
    : null;

  const violations = [];
  if (groundedCases.length < opts.minGroundedAnswerEvaluatedCases) {
    violations.push(
      `groundedAnswerEvaluatedCases=${groundedCases.length} below minGroundedAnswerEvaluatedCases=${opts.minGroundedAnswerEvaluatedCases}`
    );
  }
  if (escalationCases.length < opts.minEscalationEvaluatedCases) {
    violations.push(
      `escalationEvaluatedCases=${escalationCases.length} below minEscalationEvaluatedCases=${opts.minEscalationEvaluatedCases}`
    );
  }
  if (groundedCases.length && falseSuccessClaimRate > opts.maxFalseSuccessClaimRate + 1e-9) {
    violations.push(
      `trace falseSuccessClaimRate=${round(falseSuccessClaimRate)} exceeds maxFalseSuccessClaimRate=${round(opts.maxFalseSuccessClaimRate)}`
    );
  }
  if (
    groundedCases.length &&
    groundedFinalAnswerRate !== null &&
    groundedFinalAnswerRate < opts.minGroundedFinalAnswerRate - 1e-9
  ) {
    violations.push(
      `trace groundedFinalAnswerRate=${round(groundedFinalAnswerRate)} below minGroundedFinalAnswerRate=${round(opts.minGroundedFinalAnswerRate)}`
    );
  }
  if (
    escalationCases.length &&
    escalationComplianceRate !== null &&
    escalationComplianceRate < opts.minEscalationComplianceRate - 1e-9
  ) {
    violations.push(
      `trace escalationComplianceRate=${round(escalationComplianceRate)} below minEscalationComplianceRate=${round(opts.minEscalationComplianceRate)}`
    );
  }

  if (failedCases.length || violations.length) {
    console.error('[ai-trace-quality] FAIL');
    console.error(
      `[ai-trace-quality] report=${path.relative(QMAP_ROOT, reportPath)} tracedCases=${tracedCases} groundedCases=${groundedCases.length} escalationCases=${escalationCases.length}`
    );
    for (const violation of violations) {
      console.error(`[ai-trace-quality] violation=${violation}`);
    }
    for (const caseRow of failedCases) {
      console.error(
        `[ai-trace-quality] case=${caseRow.id} requestIds=${caseRow.requestIds.join(',') || '-'} issues=${caseRow.issues.join(' | ')}`
      );
    }
    process.exit(2);
  }

  let suffix = '';
  if (!selectedIsLatest && latestReportPath && latestReportPath !== reportPath) {
    suffix = ` latestIncompatibleReport=${path.relative(QMAP_ROOT, latestReportPath)}`;
  }

  console.log(
    `[ai-trace-quality] OK: report=${path.relative(QMAP_ROOT, reportPath)} tracedCases=${tracedCases} groundedCases=${groundedCases.length} escalationCases=${escalationCases.length} totalFalseSuccessClaims=${totalFalseSuccessClaimCount} groundedFinalAnswerRate=${round(groundedFinalAnswerRate || 0)} escalationComplianceRate=${round(escalationComplianceRate || 0)}${suffix}`
  );
}

main();
