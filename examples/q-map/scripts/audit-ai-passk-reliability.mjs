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
    window: Math.max(1, Math.trunc(asFiniteNumber(process.env.QMAP_AI_PASSK_WINDOW, 3))),
    runType: String(process.env.QMAP_AI_PASSK_RUN_TYPE || 'adversarial').trim(),
    casesSuffix: String(process.env.QMAP_AI_PASSK_CASES_SUFFIX || 'tests/ai-eval/cases.adversarial.json').trim(),
    requireMin: parseBoolean(process.env.QMAP_AI_PASSK_REQUIRE_MIN, false),
    minPassAtK: asFiniteNumber(process.env.QMAP_AI_PASSK_MIN_PASS_AT_K, 0.8),
    minCriticalPassAtK: asFiniteNumber(process.env.QMAP_AI_PASSK_MIN_CRITICAL_PASS_AT_K, 1)
  };

  for (let i = 0; i < argv.length; i += 1) {
    const arg = String(argv[i] || '');
    const next = argv[i + 1];
    if (arg === '--results-dir' && next) {
      out.resultsDir = String(next);
      i += 1;
      continue;
    }
    if (arg === '--window' && next) {
      out.window = Math.max(1, Math.trunc(asFiniteNumber(next, out.window)));
      i += 1;
      continue;
    }
    if (arg === '--run-type' && next) {
      out.runType = String(next).trim();
      i += 1;
      continue;
    }
    if (arg === '--cases-suffix' && next) {
      out.casesSuffix = String(next).trim();
      i += 1;
      continue;
    }
    if (arg === '--require-min' && next) {
      out.requireMin = parseBoolean(next, out.requireMin);
      i += 1;
      continue;
    }
    if (arg === '--min-pass-at-k' && next) {
      out.minPassAtK = asFiniteNumber(next, out.minPassAtK);
      i += 1;
      continue;
    }
    if (arg === '--min-critical-pass-at-k' && next) {
      out.minCriticalPassAtK = asFiniteNumber(next, out.minCriticalPassAtK);
      i += 1;
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
  if (['stabilization', 'stabilisation', 'stabilize', 'stab'].includes(token)) return 'stabilization';
  if (['heldout', 'held-out', 'holdout'].includes(token)) return 'heldout';
  if (['adversarial', 'adversary', 'redteam', 'red-team'].includes(token)) return 'adversarial';
  if (['debug', 'dbg', 'diagnostic'].includes(token)) return 'debug';
  if (token === 'all') return 'all';
  return '';
}

function inferRunTypeFromRunId(runId) {
  const text = String(runId || '').toLowerCase();
  if (/(^|[-_])(heldout|held-out|holdout)([-_]|$)/.test(text)) return 'heldout';
  if (/(^|[-_])(adversarial|adversary|redteam|red-team)([-_]|$)/.test(text)) return 'adversarial';
  if (/(^|[-_])(debug|dbg|diagnostic|review|anti[-_]?bias|fix[0-9]*)([-_]|$)/.test(text)) return 'debug';
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

function round(value) {
  return Number(Number(value || 0).toFixed(3));
}

function fail(message) {
  console.error(`[ai-passk] FAIL: ${message}`);
}

function collectReports(resultsDir, runType, casesSuffix) {
  const fullDir = resolveQMapPath(resultsDir);
  if (!fs.existsSync(fullDir)) return [];

  const names = fs.readdirSync(fullDir).filter(name => /^report-.*\.json$/i.test(name));
  const rows = [];
  for (const name of names) {
    const reportPath = path.join(fullDir, name);
    try {
      const report = readJson(reportPath);
      const reportCasesSuffix = String(report?.casesPath || '').trim();
      if (casesSuffix && !reportCasesSuffix.endsWith(casesSuffix)) continue;
      const resolvedRunType = normalizeRunType(report?.runType) || inferRunTypeFromRunId(report?.runId);
      if (runType !== 'all' && resolvedRunType !== runType) continue;
      const createdAtMs = Date.parse(String(report?.createdAt || '')) || fs.statSync(reportPath).mtimeMs || 0;
      rows.push({
        path: reportPath,
        report,
        runId: String(report?.runId || path.basename(reportPath, '.json')),
        runType: resolvedRunType,
        createdAtMs
      });
    } catch {
      // ignore malformed report files
    }
  }
  rows.sort((a, b) => a.createdAtMs - b.createdAtMs);
  return rows;
}

function caseMap(report) {
  const map = new Map();
  for (const row of Array.isArray(report?.cases) ? report.cases : []) {
    const caseId = String(row?.id || '').trim();
    if (!caseId || map.has(caseId)) continue;
    map.set(caseId, row);
  }
  return map;
}

function summarizeAttempts(selectedReports) {
  const byCaseId = new Map();
  const runIds = selectedReports.map(row => row.runId);

  for (const reportRow of selectedReports) {
    const map = caseMap(reportRow.report);
    for (const [caseId, row] of map.entries()) {
      if (!byCaseId.has(caseId)) byCaseId.set(caseId, []);
      byCaseId.get(caseId).push({
        runId: reportRow.runId,
        pass: Boolean(row?.pass),
        criticality: String(row?.criticality || 'standard').trim() || 'standard',
        caseScore: Number(row?.metrics?.caseScore || 0)
      });
    }
  }

  const results = [];
  for (const [caseId, attempts] of byCaseId.entries()) {
    if (attempts.length !== selectedReports.length) {
      fail(`case "${caseId}" missing from one or more selected reports (${runIds.join(', ')})`);
      process.exit(1);
    }
    const criticality = attempts[0].criticality;
    if (attempts.some(attempt => attempt.criticality !== criticality)) {
      fail(`case "${caseId}" has inconsistent criticality across selected reports`);
      process.exit(1);
    }
    const passCount = attempts.filter(attempt => attempt.pass).length;
    results.push({
      id: caseId,
      criticality,
      passCount,
      attemptCount: attempts.length,
      passAtK: passCount > 0 ? 1 : 0,
      attemptPassRate: round(passCount / attempts.length),
      attempts
    });
  }
  results.sort((a, b) => a.id.localeCompare(b.id));
  return results;
}

function main() {
  const opts = parseArgs(process.argv.slice(2));
  const runType = normalizeRunType(opts.runType) || 'adversarial';
  const reports = collectReports(opts.resultsDir, runType, opts.casesSuffix);

  if (!reports.length) {
    const msg = `no reports found in ${opts.resultsDir} for runType=${runType} casesSuffix=${opts.casesSuffix}`;
    if (opts.requireMin) {
      fail(msg);
      process.exit(1);
    }
    console.log(`[ai-passk] SKIP: ${msg}`);
    return;
  }

  if (reports.length < opts.window) {
    const msg = `need at least ${opts.window} reports for runType=${runType}, found ${reports.length}`;
    if (opts.requireMin) {
      fail(msg);
      process.exit(1);
    }
    console.log(`[ai-passk] SKIP: ${msg}`);
    return;
  }

  const selected = reports.slice(-opts.window);
  const selectedRunIds = selected.map(row => row.runId);
  const cases = summarizeAttempts(selected);
  const criticalCases = cases.filter(row => row.criticality === 'critical');
  const passAtK = round(cases.filter(row => row.passAtK === 1).length / (cases.length || 1));
  const criticalPassAtK = round(
    criticalCases.filter(row => row.passAtK === 1).length / (criticalCases.length || 1)
  );

  const violations = [];
  if (passAtK < opts.minPassAtK - 1e-9) {
    violations.push(`passAtK=${passAtK} < minPassAtK=${round(opts.minPassAtK)}`);
  }
  if (criticalPassAtK < opts.minCriticalPassAtK - 1e-9) {
    violations.push(
      `criticalPassAtK=${criticalPassAtK} < minCriticalPassAtK=${round(opts.minCriticalPassAtK)}`
    );
  }

  const failedCases = cases.filter(row => row.passAtK === 0);
  if (violations.length) {
    for (const violation of violations) fail(violation);
    for (const row of failedCases) {
      const attempts = row.attempts
        .map(attempt => `${attempt.runId}:${attempt.pass ? 'P' : 'F'}:${round(attempt.caseScore)}`)
        .join(', ');
      fail(`case=${row.id} criticality=${row.criticality} passAtK=0 attempts=[${attempts}]`);
    }
    fail(`window=${opts.window} runIds=${selectedRunIds.join(', ')}`);
    process.exit(1);
  }

  console.log(
    `[ai-passk] OK: runType=${runType} window=${opts.window} runIds=${selectedRunIds.join(', ')} ` +
      `cases=${cases.length} criticalCases=${criticalCases.length} passAtK=${passAtK} criticalPassAtK=${criticalPassAtK}`
  );
}

main();
