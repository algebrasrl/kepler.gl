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
  return Number(Number(value || 0).toFixed(6));
}

function fail(message) {
  console.error(`[ai-cost] FAIL: ${message}`);
}

function parseArgs(argv) {
  const out = {
    resultsDir: resolveQMapPath('tests/ai-eval/results'),
    reportPath: String(process.env.QMAP_AI_COST_REPORT || '').trim(),
    casesSuffix: String(process.env.QMAP_AI_COST_CASES_SUFFIX || 'tests/ai-eval/cases.functional.json').trim(),
    maxAvgTotalTokens: asFiniteNumber(process.env.QMAP_AI_EVAL_MAX_AVG_TOTAL_TOKENS, 12000),
    maxP95TotalTokens: asFiniteNumber(process.env.QMAP_AI_EVAL_MAX_P95_TOTAL_TOKENS, 25000),
    maxMaxTotalTokens: asFiniteNumber(process.env.QMAP_AI_EVAL_MAX_MAX_TOTAL_TOKENS, 35000),
    maxAvgEstimatedPromptTokens: asFiniteNumber(process.env.QMAP_AI_EVAL_MAX_AVG_ESTIMATED_PROMPT_TOKENS, 22000),
    maxP95EstimatedPromptTokens: asFiniteNumber(process.env.QMAP_AI_EVAL_MAX_P95_ESTIMATED_PROMPT_TOKENS, 33000),
    maxMaxEstimatedPromptTokens: asFiniteNumber(process.env.QMAP_AI_EVAL_MAX_MAX_ESTIMATED_PROMPT_TOKENS, 36000),
    minUsageCoverageRate: asFiniteNumber(process.env.QMAP_AI_EVAL_MIN_USAGE_COVERAGE_RATE, 1),
    minEstimateCoverageRate: asFiniteNumber(process.env.QMAP_AI_EVAL_MIN_ESTIMATE_COVERAGE_RATE, 0),
    minTokenBudgetCoverageRate: asFiniteNumber(process.env.QMAP_AI_EVAL_MIN_TOKEN_BUDGET_COVERAGE_RATE, 1),
    maxAvgPromptBudgetUtilizationRatio: asFiniteNumber(
      process.env.QMAP_AI_EVAL_MAX_AVG_PROMPT_BUDGET_UTILIZATION_RATIO,
      0.35
    ),
    maxMaxPromptBudgetUtilizationRatio: asFiniteNumber(
      process.env.QMAP_AI_EVAL_MAX_MAX_PROMPT_BUDGET_UTILIZATION_RATIO,
      0.6
    ),
    maxTokenBudgetWarnCases: asFiniteNumber(process.env.QMAP_AI_EVAL_MAX_TOKEN_BUDGET_WARN_CASES, 0),
    maxTokenBudgetCompactCases: asFiniteNumber(process.env.QMAP_AI_EVAL_MAX_TOKEN_BUDGET_COMPACT_CASES, 0),
    maxTokenBudgetHardCases: asFiniteNumber(process.env.QMAP_AI_EVAL_MAX_TOKEN_BUDGET_HARD_CASES, 0),
    inputCostPerMTokenUsd: asFiniteNumber(process.env.QMAP_AI_EVAL_INPUT_COST_PER_MTOK_USD),
    outputCostPerMTokenUsd: asFiniteNumber(process.env.QMAP_AI_EVAL_OUTPUT_COST_PER_MTOK_USD),
    maxTotalEstimatedCostUsd: asFiniteNumber(process.env.QMAP_AI_EVAL_MAX_TOTAL_ESTIMATED_COST_USD),
    maxAvgEstimatedCostUsd: asFiniteNumber(process.env.QMAP_AI_EVAL_MAX_AVG_ESTIMATED_COST_USD)
  };

  for (let i = 0; i < argv.length; i += 1) {
    const arg = String(argv[i] || '');
    const next = argv[i + 1];
    if (!next && !arg.startsWith('--report=')) continue;
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
    const numericFlags = {
      '--max-avg-total-tokens': 'maxAvgTotalTokens',
      '--max-p95-total-tokens': 'maxP95TotalTokens',
      '--max-max-total-tokens': 'maxMaxTotalTokens',
      '--max-avg-estimated-prompt-tokens': 'maxAvgEstimatedPromptTokens',
      '--max-p95-estimated-prompt-tokens': 'maxP95EstimatedPromptTokens',
      '--max-max-estimated-prompt-tokens': 'maxMaxEstimatedPromptTokens',
      '--min-usage-coverage-rate': 'minUsageCoverageRate',
      '--min-estimate-coverage-rate': 'minEstimateCoverageRate',
      '--min-token-budget-coverage-rate': 'minTokenBudgetCoverageRate',
      '--max-avg-prompt-budget-utilization-ratio': 'maxAvgPromptBudgetUtilizationRatio',
      '--max-max-prompt-budget-utilization-ratio': 'maxMaxPromptBudgetUtilizationRatio',
      '--max-token-budget-warn-cases': 'maxTokenBudgetWarnCases',
      '--max-token-budget-compact-cases': 'maxTokenBudgetCompactCases',
      '--max-token-budget-hard-cases': 'maxTokenBudgetHardCases',
      '--input-cost-per-mtok-usd': 'inputCostPerMTokenUsd',
      '--output-cost-per-mtok-usd': 'outputCostPerMTokenUsd',
      '--max-total-estimated-cost-usd': 'maxTotalEstimatedCostUsd',
      '--max-avg-estimated-cost-usd': 'maxAvgEstimatedCostUsd'
    };
    if (numericFlags[arg] && next) {
      out[numericFlags[arg]] = asFiniteNumber(next, out[numericFlags[arg]]);
      i += 1;
    }
  }

  return out;
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

function summarizeCostMetrics(report, opts) {
  const summary = report?.summary && typeof report.summary === 'object' ? report.summary : {};
  const totalEstimatedCostUsd =
    Number(summary.totalPromptTokens || 0) * Number(opts.inputCostPerMTokenUsd || 0) / 1_000_000 +
    Number(summary.totalCompletionTokens || 0) * Number(opts.outputCostPerMTokenUsd || 0) / 1_000_000;
  const avgEstimatedCostUsd =
    Number(summary.totalCases || 0) > 0 ? totalEstimatedCostUsd / Number(summary.totalCases || 1) : 0;
  return {
    totalCases: Number(summary.totalCases || 0),
    totalTokens: Number(summary.totalTokens || 0),
    avgTotalTokens: asFiniteNumber(summary.avgTotalTokens, null),
    p95TotalTokens: asFiniteNumber(summary.p95TotalTokens, null),
    maxTotalTokens: asFiniteNumber(summary.maxTotalTokens, null),
    usageCoverageRate: asFiniteNumber(summary.usageCoverageRate, 0),
    totalEstimatedPromptTokens: Number(summary.totalEstimatedPromptTokens || 0),
    avgEstimatedPromptTokens: asFiniteNumber(summary.avgEstimatedPromptTokens, null),
    p95EstimatedPromptTokens: asFiniteNumber(summary.p95EstimatedPromptTokens, null),
    maxEstimatedPromptTokens: asFiniteNumber(summary.maxEstimatedPromptTokens, null),
    estimateCoverageRate: asFiniteNumber(summary.estimateCoverageRate, 0),
    tokenBudgetCoverageRate: asFiniteNumber(summary.tokenBudgetCoverageRate, 0),
    avgPromptBudgetUtilizationRatio: asFiniteNumber(summary.avgPromptBudgetUtilizationRatio, null),
    maxPromptBudgetUtilizationRatio: asFiniteNumber(summary.maxPromptBudgetUtilizationRatio, null),
    tokenBudgetWarnCases: Number(summary.tokenBudgetWarnCases || 0),
    tokenBudgetCompactCases: Number(summary.tokenBudgetCompactCases || 0),
    tokenBudgetHardCases: Number(summary.tokenBudgetHardCases || 0),
    totalEstimatedCostUsd: round(totalEstimatedCostUsd),
    avgEstimatedCostUsd: round(avgEstimatedCostUsd)
  };
}

function main() {
  const opts = parseArgs(process.argv.slice(2));
  const reportPath = opts.reportPath
    ? resolveQMapPath(opts.reportPath)
    : resolveLatestReportPath(opts.resultsDir, opts.casesSuffix);
  const report = readJson(reportPath);
  const metrics = summarizeCostMetrics(report, opts);
  const violations = [];

  const compareMax = (label, value, maxValue) => {
    if (value === null || value === undefined || maxValue === undefined) return;
    if (value > maxValue + 1e-9) {
      violations.push(`${label}=${value} exceeds max=${round(maxValue)}`);
    }
  };
  const compareMin = (label, value, minValue) => {
    if (value === null || value === undefined || minValue === undefined) return;
    if (value < minValue - 1e-9) {
      violations.push(`${label}=${value} below min=${round(minValue)}`);
    }
  };

  compareMax('avgTotalTokens', metrics.avgTotalTokens, opts.maxAvgTotalTokens);
  compareMax('p95TotalTokens', metrics.p95TotalTokens, opts.maxP95TotalTokens);
  compareMax('maxTotalTokens', metrics.maxTotalTokens, opts.maxMaxTotalTokens);
  compareMax('avgEstimatedPromptTokens', metrics.avgEstimatedPromptTokens, opts.maxAvgEstimatedPromptTokens);
  compareMax('p95EstimatedPromptTokens', metrics.p95EstimatedPromptTokens, opts.maxP95EstimatedPromptTokens);
  compareMax('maxEstimatedPromptTokens', metrics.maxEstimatedPromptTokens, opts.maxMaxEstimatedPromptTokens);
  compareMin('usageCoverageRate', metrics.usageCoverageRate, opts.minUsageCoverageRate);
  compareMin('estimateCoverageRate', metrics.estimateCoverageRate, opts.minEstimateCoverageRate);
  compareMin('tokenBudgetCoverageRate', metrics.tokenBudgetCoverageRate, opts.minTokenBudgetCoverageRate);
  compareMax(
    'avgPromptBudgetUtilizationRatio',
    metrics.avgPromptBudgetUtilizationRatio,
    opts.maxAvgPromptBudgetUtilizationRatio
  );
  compareMax(
    'maxPromptBudgetUtilizationRatio',
    metrics.maxPromptBudgetUtilizationRatio,
    opts.maxMaxPromptBudgetUtilizationRatio
  );
  compareMax('tokenBudgetWarnCases', metrics.tokenBudgetWarnCases, opts.maxTokenBudgetWarnCases);
  compareMax('tokenBudgetCompactCases', metrics.tokenBudgetCompactCases, opts.maxTokenBudgetCompactCases);
  compareMax('tokenBudgetHardCases', metrics.tokenBudgetHardCases, opts.maxTokenBudgetHardCases);

  if (opts.maxTotalEstimatedCostUsd !== undefined) {
    compareMax('totalEstimatedCostUsd', metrics.totalEstimatedCostUsd, opts.maxTotalEstimatedCostUsd);
  }
  if (opts.maxAvgEstimatedCostUsd !== undefined) {
    compareMax('avgEstimatedCostUsd', metrics.avgEstimatedCostUsd, opts.maxAvgEstimatedCostUsd);
  }

  if (violations.length) {
    for (const violation of violations) fail(violation);
    fail(`report=${path.relative(QMAP_ROOT, reportPath)} totalCases=${metrics.totalCases}`);
    process.exit(1);
  }

  const costSuffix =
    opts.inputCostPerMTokenUsd !== undefined || opts.outputCostPerMTokenUsd !== undefined
      ? ` totalEstimatedCostUsd=${metrics.totalEstimatedCostUsd} avgEstimatedCostUsd=${metrics.avgEstimatedCostUsd}`
      : '';
  console.log(
    `[ai-cost] OK: report=${path.relative(QMAP_ROOT, reportPath)} totalCases=${metrics.totalCases} ` +
      `avgTotalTokens=${metrics.avgTotalTokens ?? '-'} p95TotalTokens=${metrics.p95TotalTokens ?? '-'} ` +
      `maxTotalTokens=${metrics.maxTotalTokens ?? '-'} avgEstimatedPromptTokens=${metrics.avgEstimatedPromptTokens ?? '-'} ` +
      `maxPromptBudgetUtilizationRatio=${metrics.maxPromptBudgetUtilizationRatio ?? '-'} ` +
      `usageCoverageRate=${metrics.usageCoverageRate} tokenBudgetCoverageRate=${metrics.tokenBudgetCoverageRate}` +
      costSuffix
  );
}

main();
