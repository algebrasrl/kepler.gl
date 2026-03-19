#!/usr/bin/env node

import fs from 'node:fs';
import path from 'node:path';

function readJson(filePath) {
  return JSON.parse(fs.readFileSync(filePath, 'utf8'));
}

function asFiniteNumber(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : undefined;
}

function normalizeCriticality(value) {
  return String(value || 'standard').trim().toLowerCase() === 'critical' ? 'critical' : 'standard';
}

function parseCaseIdAllowlist(rawValue) {
  const raw = String(rawValue || '').trim();
  if (!raw) return new Set();
  return new Set(
    raw
      .split(',')
      .map(token => token.trim())
      .filter(Boolean)
  );
}

function fail(message) {
  console.error(`[ai-eval-thresholds] FAIL: ${message}`);
}

function main() {
  const repoRoot = process.cwd();
  const matrixPath = path.join(repoRoot, 'tests/ai-eval/architecture-matrix.json');
  const functionalCasesPath = path.join(repoRoot, 'tests/ai-eval/cases.functional.json');
  const matrix = readJson(matrixPath);
  const functionalCases = readJson(functionalCasesPath);

  if (!Array.isArray(functionalCases) || !functionalCases.length) {
    fail('cases.functional.json must be a non-empty array');
    process.exit(1);
  }

  const defaults = matrix?.evaluationPolicy?.criticalityDefaults || {};
  const criticalDefault = asFiniteNumber(defaults?.critical?.min_case_score);
  const standardDefault = asFiniteNumber(defaults?.standard?.min_case_score);
  const criticalFloor =
    asFiniteNumber(process.env.QMAP_AI_EVAL_MIN_CASE_SCORE_FLOOR_CRITICAL) ??
    criticalDefault ??
    0.75;
  const standardFloor =
    asFiniteNumber(process.env.QMAP_AI_EVAL_MIN_CASE_SCORE_FLOOR_STANDARD) ??
    standardDefault ??
    0.6;
  const allowCaseIds = parseCaseIdAllowlist(process.env.QMAP_AI_EVAL_CASE_SCORE_FLOOR_EXCEPTIONS);
  const epsilon = 1e-9;

  const floorsByCriticality = {
    critical: criticalFloor,
    standard: standardFloor
  };

  const violations = [];
  for (let i = 0; i < functionalCases.length; i += 1) {
    const caseDef = functionalCases[i] || {};
    const caseId = String(caseDef.id || '').trim() || `<index:${i}>`;
    if (allowCaseIds.has(caseId)) continue;

    const criticality = normalizeCriticality(caseDef.criticality);
    const floor = floorsByCriticality[criticality];
    const explicitMin = asFiniteNumber(caseDef.min_case_score);
    const effectiveMin = explicitMin ?? floor;
    if (effectiveMin + epsilon < floor) {
      violations.push({
        caseId,
        criticality,
        floor,
        effectiveMin,
        explicit: explicitMin
      });
    }
  }

  if (violations.length) {
    for (const violation of violations) {
      const explicitNote =
        violation.explicit === undefined ? 'implicit default' : `explicit min_case_score=${violation.explicit}`;
      fail(
        `case "${violation.caseId}" (${violation.criticality}) is below floor ${violation.floor}: ` +
          `effectiveMin=${violation.effectiveMin} (${explicitNote})`
      );
    }
    fail(
      'Raise min_case_score to floor, or temporarily exempt case ids via QMAP_AI_EVAL_CASE_SCORE_FLOOR_EXCEPTIONS=id1,id2'
    );
    process.exit(1);
  }

  console.log(
    `[ai-eval-thresholds] OK: functionalCases=${functionalCases.length} ` +
      `floors={critical:${criticalFloor},standard:${standardFloor}} ` +
      `exceptions=${allowCaseIds.size}`
  );
}

main();
