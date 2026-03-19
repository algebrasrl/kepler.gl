#!/usr/bin/env node

import fs from 'node:fs';
import path from 'node:path';
import {fileURLToPath} from 'node:url';

const QMAP_ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');

function resolveQMapPath(target) {
  return path.isAbsolute(target) ? target : path.resolve(QMAP_ROOT, target);
}

function asFiniteNumber(value, fallback = undefined) {
  if (value === null || value === undefined || value === '') return fallback;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function readJson(filePath) {
  return JSON.parse(fs.readFileSync(filePath, 'utf8'));
}

function round(value) {
  return Number(Number(value || 0).toFixed(3));
}

function fail(message) {
  console.error(`[ai-response-quality] FAIL: ${message}`);
}

const REQUIRED_RESPONSE_QUALITY_KEYS = [
  'totalCases',
  'falseSuccessClaimRate',
  'totalFalseSuccessClaims',
  'groundedAnswerEvaluatedCases',
  'groundedAnswerPassingCases',
  'escalationEvaluatedCases',
  'escalationPassingCases'
];

function parseArgs(argv) {
  const out = {
    resultsDir: resolveQMapPath('tests/ai-eval/results'),
    reportPath: String(process.env.QMAP_AI_RESPONSE_QUALITY_REPORT || '').trim(),
    casesSuffix: String(process.env.QMAP_AI_RESPONSE_QUALITY_CASES_SUFFIX || 'tests/ai-eval/cases.functional.json')
      .trim(),
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
    )
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

  return out;
}

function isCompatibleResponseQualitySummary(summary) {
  return REQUIRED_RESPONSE_QUALITY_KEYS.every(key => hasSummaryMetric(summary, key));
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
        const reportCasesSuffix = String(report?.casesPath || '');
        if (casesSuffix && !reportCasesSuffix.endsWith(casesSuffix)) return null;
        const createdAtMs = Date.parse(String(report?.createdAt || '')) || fs.statSync(fullPath).mtimeMs || 0;
        const summary = report?.summary && typeof report.summary === 'object' ? report.summary : null;
        return {fullPath, createdAtMs, compatible: isCompatibleResponseQualitySummary(summary)};
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

function hasSummaryMetric(summary, key) {
  return Boolean(summary) && Object.prototype.hasOwnProperty.call(summary, key);
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
  const summary = report?.summary && typeof report.summary === 'object' ? report.summary : null;

  if (!summary) {
    fail(`report missing summary: ${path.relative(QMAP_ROOT, reportPath)}`);
    process.exit(1);
  }

  const metrics = {
    totalCases: asFiniteNumber(summary?.totalCases),
    falseSuccessClaimRate: asFiniteNumber(summary?.falseSuccessClaimRate),
    totalFalseSuccessClaims: asFiniteNumber(summary?.totalFalseSuccessClaims),
    groundedFinalAnswerRate: asFiniteNumber(summary?.groundedFinalAnswerRate),
    groundedAnswerEvaluatedCases: asFiniteNumber(summary?.groundedAnswerEvaluatedCases),
    groundedAnswerPassingCases: asFiniteNumber(summary?.groundedAnswerPassingCases),
    escalationComplianceRate: asFiniteNumber(summary?.escalationComplianceRate),
    escalationEvaluatedCases: asFiniteNumber(summary?.escalationEvaluatedCases),
    escalationPassingCases: asFiniteNumber(summary?.escalationPassingCases)
  };

  const missing = REQUIRED_RESPONSE_QUALITY_KEYS
    .filter(key => !hasSummaryMetric(summary, key) || metrics[key] === undefined);
  if (missing.length) {
    if (!opts.reportPath && latestReportPath === reportPath) {
      fail(
        `latest functional report predates response-quality metrics: ${path.relative(QMAP_ROOT, reportPath)}`
      );
      fail('run ai-eval-functional to regenerate a compatible report before enforcing this gate');
      process.exit(1);
    }
    fail(`report summary missing response-quality metrics: ${missing.join(', ')}`);
    fail(`report=${path.relative(QMAP_ROOT, reportPath)}`);
    process.exit(1);
  }

  const violations = [];

  if (metrics.falseSuccessClaimRate > opts.maxFalseSuccessClaimRate + 1e-9) {
    violations.push(
      `falseSuccessClaimRate=${metrics.falseSuccessClaimRate} exceeds maxFalseSuccessClaimRate=${round(opts.maxFalseSuccessClaimRate)}`
    );
  }
  if (metrics.groundedAnswerEvaluatedCases < opts.minGroundedAnswerEvaluatedCases) {
    violations.push(
      `groundedAnswerEvaluatedCases=${metrics.groundedAnswerEvaluatedCases} below minGroundedAnswerEvaluatedCases=${opts.minGroundedAnswerEvaluatedCases}`
    );
  }
  if (
    metrics.groundedAnswerEvaluatedCases > 0 &&
    !hasSummaryMetric(summary, 'groundedFinalAnswerRate')
  ) {
    violations.push('groundedFinalAnswerRate missing despite groundedAnswerEvaluatedCases > 0');
  }
  if (metrics.escalationEvaluatedCases < opts.minEscalationEvaluatedCases) {
    violations.push(
      `escalationEvaluatedCases=${metrics.escalationEvaluatedCases} below minEscalationEvaluatedCases=${opts.minEscalationEvaluatedCases}`
    );
  }
  if (
    metrics.groundedAnswerEvaluatedCases > 0 &&
    metrics.groundedFinalAnswerRate !== undefined &&
    metrics.groundedFinalAnswerRate < opts.minGroundedFinalAnswerRate - 1e-9
  ) {
    violations.push(
      `groundedFinalAnswerRate=${metrics.groundedFinalAnswerRate} below minGroundedFinalAnswerRate=${round(opts.minGroundedFinalAnswerRate)}`
    );
  }
  if (
    metrics.escalationEvaluatedCases > 0 &&
    !hasSummaryMetric(summary, 'escalationComplianceRate')
  ) {
    violations.push('escalationComplianceRate missing despite escalationEvaluatedCases > 0');
  }
  if (
    metrics.escalationEvaluatedCases > 0 &&
    metrics.escalationComplianceRate !== undefined &&
    metrics.escalationComplianceRate < opts.minEscalationComplianceRate - 1e-9
  ) {
    violations.push(
      `escalationComplianceRate=${metrics.escalationComplianceRate} below minEscalationComplianceRate=${round(opts.minEscalationComplianceRate)}`
    );
  }

  if (violations.length) {
    for (const violation of violations) fail(violation);
    fail(`report=${path.relative(QMAP_ROOT, reportPath)} totalCases=${metrics.totalCases}`);
    process.exit(1);
  }

  console.log(
    `[ai-response-quality] OK: report=${path.relative(QMAP_ROOT, reportPath)} totalCases=${metrics.totalCases} ` +
      `falseSuccessClaimRate=${metrics.falseSuccessClaimRate} totalFalseSuccessClaims=${metrics.totalFalseSuccessClaims} ` +
      `groundedFinalAnswerRate=${metrics.groundedFinalAnswerRate} groundedAnswerEvaluatedCases=${metrics.groundedAnswerEvaluatedCases} ` +
      `escalationComplianceRate=${metrics.escalationComplianceRate} escalationEvaluatedCases=${metrics.escalationEvaluatedCases}` +
      (selectedIsLatest ? '' : ` latestIncompatibleReport=${path.relative(QMAP_ROOT, latestReportPath)}`)
  );
}

main();
