#!/usr/bin/env node

import fs from 'node:fs';
import path from 'node:path';
import {fileURLToPath} from 'node:url';

const QMAP_ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');

function resolveQMapPath(target) {
  return path.isAbsolute(target) ? target : path.resolve(QMAP_ROOT, target);
}

function asFiniteNumber(value, fallback) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function parseArgs(argv) {
  const out = {
    resultsDir: resolveQMapPath('tests/ai-eval/results'),
    reportPath: String(process.env.QMAP_AI_OPERATIONAL_REPORT || '').trim(),
    maxAvgDurationMs: asFiniteNumber(process.env.QMAP_AI_EVAL_MAX_AVG_DURATION_MS, 25000),
    maxP95DurationMs: asFiniteNumber(process.env.QMAP_AI_EVAL_MAX_P95_DURATION_MS, 45000),
    maxMaxDurationMs: asFiniteNumber(process.env.QMAP_AI_EVAL_MAX_MAX_DURATION_MS, 90000),
    maxTransportErrorRate: asFiniteNumber(process.env.QMAP_AI_EVAL_MAX_TRANSPORT_ERROR_RATE, 0),
    maxContractResponseMismatchRate: asFiniteNumber(
      process.env.QMAP_AI_EVAL_MAX_CONTRACT_RESPONSE_MISMATCH_RATE,
      0
    ),
    requireNoTransportAbort:
      !['0', 'false', 'no', 'off'].includes(
        String(process.env.QMAP_AI_EVAL_REQUIRE_NO_TRANSPORT_ABORT || 'true')
          .trim()
          .toLowerCase()
      ),
    casesSuffix: String(process.env.QMAP_AI_OPERATIONAL_CASES_SUFFIX || 'tests/ai-eval/cases.functional.json')
      .trim()
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
    if (arg === '--max-avg-duration-ms' && next) {
      out.maxAvgDurationMs = asFiniteNumber(next, out.maxAvgDurationMs);
      i += 1;
      continue;
    }
    if (arg === '--max-p95-duration-ms' && next) {
      out.maxP95DurationMs = asFiniteNumber(next, out.maxP95DurationMs);
      i += 1;
      continue;
    }
    if (arg === '--max-max-duration-ms' && next) {
      out.maxMaxDurationMs = asFiniteNumber(next, out.maxMaxDurationMs);
      i += 1;
      continue;
    }
    if (arg === '--max-transport-error-rate' && next) {
      out.maxTransportErrorRate = asFiniteNumber(next, out.maxTransportErrorRate);
      i += 1;
      continue;
    }
    if (arg === '--max-contract-response-mismatch-rate' && next) {
      out.maxContractResponseMismatchRate = asFiniteNumber(next, out.maxContractResponseMismatchRate);
      i += 1;
      continue;
    }
    if (arg === '--require-no-transport-abort' && next) {
      out.requireNoTransportAbort = !['0', 'false', 'no', 'off'].includes(String(next).trim().toLowerCase());
      i += 1;
      continue;
    }
    if (arg === '--cases-suffix' && next) {
      out.casesSuffix = String(next).trim();
      i += 1;
    }
  }
  return out;
}

function readJson(filePath) {
  return JSON.parse(fs.readFileSync(filePath, 'utf8'));
}

function round(value) {
  return Number(Number(value || 0).toFixed(3));
}

function percentile(values, q) {
  if (!values.length) return 0;
  if (values.length === 1) return round(values[0]);
  const sorted = [...values].sort((a, b) => a - b);
  const pos = (sorted.length - 1) * Math.max(0, Math.min(1, q));
  const lower = Math.floor(pos);
  const upper = Math.ceil(pos);
  if (lower === upper) return round(sorted[lower]);
  const weight = pos - lower;
  return round(sorted[lower] + (sorted[upper] - sorted[lower]) * weight);
}

function fail(message) {
  console.error(`[ai-operational] FAIL: ${message}`);
}

function resolveLatestReportPath(resultsDir, casesSuffix) {
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
        const casesPath = String(report?.casesPath || '');
        if (casesSuffix && !casesPath.endsWith(casesSuffix)) return null;
        const createdAtMs = Date.parse(String(report?.createdAt || '')) || fs.statSync(fullPath).mtimeMs || 0;
        return {fullPath, createdAtMs};
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
  return rows[0].fullPath;
}

function summarizeOperationalMetrics(report) {
  const cases = Array.isArray(report?.cases) ? report.cases : [];
  const durations = cases
    .map(row => Number(row?.durationMs || 0))
    .filter(value => Number.isFinite(value) && value >= 0);
  const transportErrorCount = cases.filter(row => String(row?.transportError || '').trim()).length;
  return {
    totalCases: cases.length,
    avgDurationMs: round(durations.reduce((acc, value) => acc + value, 0) / (durations.length || 1)),
    p95DurationMs: percentile(durations, 0.95),
    maxDurationMs: round(Math.max(...durations, 0)),
    transportErrorCount,
    transportErrorRate: round(transportErrorCount / (cases.length || 1)),
    transportAborted: Boolean(report?.transport?.aborted),
    contractResponseMismatchRate: round(Number(report?.summary?.contractResponseMismatchRate || 0)),
    contractResponseMismatchCases: Math.max(0, Math.trunc(Number(report?.summary?.contractResponseMismatchCases || 0))),
    totalContractResponseMismatches: Math.max(0, Math.trunc(Number(report?.summary?.totalContractResponseMismatches || 0)))
  };
}

function main() {
  const opts = parseArgs(process.argv.slice(2));
  const reportPath = opts.reportPath
    ? resolveQMapPath(opts.reportPath)
    : resolveLatestReportPath(opts.resultsDir, opts.casesSuffix);
  const report = readJson(reportPath);
  const metrics = summarizeOperationalMetrics(report);
  const violations = [];

  if (metrics.avgDurationMs > opts.maxAvgDurationMs + 1e-9) {
    violations.push(`avgDurationMs=${metrics.avgDurationMs} exceeds maxAvgDurationMs=${round(opts.maxAvgDurationMs)}`);
  }
  if (metrics.p95DurationMs > opts.maxP95DurationMs + 1e-9) {
    violations.push(`p95DurationMs=${metrics.p95DurationMs} exceeds maxP95DurationMs=${round(opts.maxP95DurationMs)}`);
  }
  if (metrics.maxDurationMs > opts.maxMaxDurationMs + 1e-9) {
    violations.push(`maxDurationMs=${metrics.maxDurationMs} exceeds maxMaxDurationMs=${round(opts.maxMaxDurationMs)}`);
  }
  if (metrics.transportErrorRate > opts.maxTransportErrorRate + 1e-9) {
    violations.push(
      `transportErrorRate=${metrics.transportErrorRate} exceeds maxTransportErrorRate=${round(opts.maxTransportErrorRate)}`
    );
  }
  if (metrics.contractResponseMismatchRate > opts.maxContractResponseMismatchRate + 1e-9) {
    violations.push(
      `contractResponseMismatchRate=${metrics.contractResponseMismatchRate} exceeds maxContractResponseMismatchRate=${round(opts.maxContractResponseMismatchRate)}`
    );
  }
  if (opts.requireNoTransportAbort && metrics.transportAborted) {
    violations.push('transport.aborted=true');
  }

  if (violations.length) {
    for (const violation of violations) fail(violation);
    fail(`report=${path.relative(QMAP_ROOT, reportPath)} totalCases=${metrics.totalCases}`);
    process.exit(1);
  }

  console.log(
    `[ai-operational] OK: report=${path.relative(QMAP_ROOT, reportPath)} totalCases=${metrics.totalCases} ` +
      `avgDurationMs=${metrics.avgDurationMs} p95DurationMs=${metrics.p95DurationMs} ` +
      `maxDurationMs=${metrics.maxDurationMs} transportErrorRate=${metrics.transportErrorRate} ` +
      `contractResponseMismatchRate=${metrics.contractResponseMismatchRate}`
  );
}

main();
