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

function parseBoolean(value, fallback = false) {
  if (value === undefined || value === null || value === '') return fallback;
  const normalized = String(value).trim().toLowerCase();
  if (['1', 'true', 'yes', 'y', 'on'].includes(normalized)) return true;
  if (['0', 'false', 'no', 'n', 'off'].includes(normalized)) return false;
  return fallback;
}

function parseArgs(argv) {
  const out = {
    resultsDir: 'tests/ai-eval/results',
    window: asFiniteNumber(process.env.QMAP_AI_EVAL_VARIANCE_WINDOW, 3),
    runType: String(process.env.QMAP_AI_EVAL_VARIANCE_RUN_TYPE || 'baseline').trim(),
    requireMin: parseBoolean(process.env.QMAP_AI_EVAL_VARIANCE_REQUIRE_MIN, false),
    maxSpanPassRate: asFiniteNumber(process.env.QMAP_AI_EVAL_VARIANCE_MAX_SPAN_PASS_RATE, 0.05),
    maxSpanAvgCaseScore: asFiniteNumber(process.env.QMAP_AI_EVAL_VARIANCE_MAX_SPAN_AVG_CASE_SCORE, 0.03),
    maxSpanMinCaseScore: asFiniteNumber(process.env.QMAP_AI_EVAL_VARIANCE_MAX_SPAN_MIN_CASE_SCORE, 0.1),
    maxSpanAvgToolPrecision: asFiniteNumber(
      process.env.QMAP_AI_EVAL_VARIANCE_MAX_SPAN_AVG_TOOL_PRECISION,
      0.03
    ),
    maxSpanAvgExtraToolCalls: asFiniteNumber(
      process.env.QMAP_AI_EVAL_VARIANCE_MAX_SPAN_AVG_EXTRA_TOOL_CALLS,
      0.05
    ),
    maxLatestAvgExtraToolCalls: asFiniteNumber(
      process.env.QMAP_AI_EVAL_VARIANCE_MAX_LATEST_AVG_EXTRA_TOOL_CALLS,
      0.1
    )
  };

  for (let i = 0; i < argv.length; i += 1) {
    const arg = String(argv[i] || '');
    const value = argv[i + 1];
    if (arg === '--results-dir' && value) out.resultsDir = String(value);
    if (arg === '--window' && value) out.window = Math.max(1, Math.trunc(asFiniteNumber(value, out.window)));
    if (arg === '--run-type' && value) out.runType = String(value).trim();
    if (arg === '--require-min' && value) out.requireMin = parseBoolean(value, out.requireMin);
    if (arg === '--max-span-pass-rate' && value) out.maxSpanPassRate = asFiniteNumber(value, out.maxSpanPassRate);
    if (arg === '--max-span-avg-case-score' && value) {
      out.maxSpanAvgCaseScore = asFiniteNumber(value, out.maxSpanAvgCaseScore);
    }
    if (arg === '--max-span-min-case-score' && value) {
      out.maxSpanMinCaseScore = asFiniteNumber(value, out.maxSpanMinCaseScore);
    }
    if (arg === '--max-span-avg-tool-precision' && value) {
      out.maxSpanAvgToolPrecision = asFiniteNumber(value, out.maxSpanAvgToolPrecision);
    }
    if (arg === '--max-span-avg-extra-tool-calls' && value) {
      out.maxSpanAvgExtraToolCalls = asFiniteNumber(value, out.maxSpanAvgExtraToolCalls);
    }
    if (arg === '--max-latest-avg-extra-tool-calls' && value) {
      out.maxLatestAvgExtraToolCalls = asFiniteNumber(value, out.maxLatestAvgExtraToolCalls);
    }
  }
  return out;
}

function normalizeRunType(value) {
  const token = String(value || '')
    .trim()
    .toLowerCase();
  if (!token) return '';
  if (['baseline', 'base'].includes(token)) return 'baseline';
  if (['stabilization', 'stabilisation', 'stabilize', 'stab'].includes(token)) {
    return 'stabilization';
  }
  if (['heldout', 'held-out', 'holdout'].includes(token)) return 'heldout';
  if (['adversarial', 'adversary', 'redteam', 'red-team'].includes(token)) return 'adversarial';
  if (['debug', 'dbg', 'diagnostic'].includes(token)) return 'debug';
  if (token === 'all') return 'all';
  return '';
}

function inferRunTypeFromRunId(runId) {
  const text = String(runId || '').toLowerCase();
  if (/(^|[-_])(heldout|held-out|holdout)([-_]|$)/.test(text)) {
    return 'heldout';
  }
  if (/(^|[-_])(adversarial|adversary|redteam|red-team)([-_]|$)/.test(text)) {
    return 'adversarial';
  }
  if (/(^|[-_])(debug|dbg|diagnostic|review|anti[-_]?bias|fix[0-9]*)([-_]|$)/.test(text)) {
    return 'debug';
  }
  if (
    /(^|[-_])stab[0-9]*([-_]|$)/.test(text) ||
    /(^|[-_])(stabilization|stabilisation|stabilize|improve|harden)([-_]|$)/.test(text)
  ) {
    return 'stabilization';
  }
  return 'baseline';
}

function readJson(filePath) {
  return JSON.parse(fs.readFileSync(filePath, 'utf8'));
}

function collectFunctionalReports(resultsDir) {
  const fullDir = resolveQMapPath(resultsDir);
  if (!fs.existsSync(fullDir)) return [];

  const names = fs.readdirSync(fullDir).filter(name => /^report-.*\.json$/i.test(name));
  const rows = [];
  for (const name of names) {
    const reportPath = path.join(fullDir, name);
    try {
      const report = readJson(reportPath);
      const casesPath = String(report?.casesPath || '');
      if (!casesPath.endsWith('tests/ai-eval/cases.functional.json')) continue;
      if (report?.invalidBaseline === true) continue;
      const summary = report?.summary || {};
      const createdAtMs = Date.parse(String(report?.createdAt || '')) || fs.statSync(reportPath).mtimeMs || 0;
      const runType = normalizeRunType(report?.runType) || inferRunTypeFromRunId(report?.runId);
      rows.push({
        path: reportPath,
        report,
        summary,
        runType,
        createdAtMs
      });
    } catch {
      // ignore malformed report files
    }
  }
  rows.sort((a, b) => a.createdAtMs - b.createdAtMs);
  return rows;
}

function round(value) {
  return Number(Number(value || 0).toFixed(3));
}

function summarizeSpan(series) {
  const min = Math.min(...series);
  const max = Math.max(...series);
  return {min, max, span: max - min};
}

function fail(message) {
  console.error(`[ai-eval-variance] FAIL: ${message}`);
}

function main() {
  const opts = parseArgs(process.argv.slice(2));
  const runType = normalizeRunType(opts.runType) || 'baseline';
  const reports = collectFunctionalReports(opts.resultsDir).filter(row =>
    runType === 'all' ? true : row.runType === runType
  );

  if (!reports.length) {
    const msg = `no functional reports found in ${opts.resultsDir} for runType=${runType}`;
    if (opts.requireMin) {
      fail(msg);
      process.exit(1);
    }
    console.log(`[ai-eval-variance] SKIP: ${msg}`);
    return;
  }

  if (reports.length < opts.window) {
    const msg = `need at least ${opts.window} functional reports for runType=${runType}, found ${reports.length}`;
    if (opts.requireMin) {
      fail(msg);
      process.exit(1);
    }
    console.log(`[ai-eval-variance] SKIP: ${msg}`);
    return;
  }

  const selected = reports.slice(-opts.window);
  const runIds = selected.map(row => String(row.report?.runId || path.basename(row.path, '.json')));

  const metrics = [
    {key: 'passRate', label: 'passRate', maxSpan: opts.maxSpanPassRate},
    {key: 'avgCaseScore', label: 'avgCaseScore', maxSpan: opts.maxSpanAvgCaseScore},
    {key: 'minCaseScore', label: 'minCaseScore', maxSpan: opts.maxSpanMinCaseScore},
    {key: 'avgToolPrecision', label: 'avgToolPrecision', maxSpan: opts.maxSpanAvgToolPrecision},
    {key: 'avgExtraToolCalls', label: 'avgExtraToolCalls', maxSpan: opts.maxSpanAvgExtraToolCalls}
  ];

  const violations = [];
  for (const metric of metrics) {
    const values = selected.map(row => asFiniteNumber(row.summary?.[metric.key], NaN));
    if (values.some(value => !Number.isFinite(value))) {
      fail(`metric "${metric.key}" is missing/invalid in selected reports (${runIds.join(', ')})`);
      process.exit(1);
    }
    const spanInfo = summarizeSpan(values);
    if (spanInfo.span > metric.maxSpan + 1e-9) {
      violations.push(
        `${metric.label} span=${round(spanInfo.span)} exceeds maxSpan=${round(metric.maxSpan)} ` +
          `(min=${round(spanInfo.min)}, max=${round(spanInfo.max)})`
      );
    }
  }

  const latest = selected[selected.length - 1];
  const latestAvgExtra = asFiniteNumber(latest.summary?.avgExtraToolCalls, 0);
  if (latestAvgExtra > opts.maxLatestAvgExtraToolCalls + 1e-9) {
    violations.push(
      `latest avgExtraToolCalls=${round(latestAvgExtra)} exceeds maxLatest=${round(opts.maxLatestAvgExtraToolCalls)}`
    );
  }

  if (violations.length) {
    for (const violation of violations) fail(violation);
    fail(`window=${opts.window} runIds=${runIds.join(', ')}`);
    process.exit(1);
  }

  console.log(
    `[ai-eval-variance] OK: runType=${runType} window=${opts.window} runIds=${runIds.join(', ')} ` +
      `limits={passRate:${opts.maxSpanPassRate},avgCase:${opts.maxSpanAvgCaseScore},` +
      `minCase:${opts.maxSpanMinCaseScore},avgPrecision:${opts.maxSpanAvgToolPrecision},` +
      `avgExtra:${opts.maxSpanAvgExtraToolCalls},latestAvgExtra:${opts.maxLatestAvgExtraToolCalls}}`
  );
}

main();
