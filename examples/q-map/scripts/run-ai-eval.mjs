#!/usr/bin/env node

import fs from 'node:fs';
import path from 'node:path';
import {execSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';
import {
  buildEvalToolCatalogFromManifest,
  loadQMapToolContracts,
  loadQMapToolManifest
} from './lib/tool-manifest-loader.mjs';
import {INVALID_BASELINE_REASONS} from './lib/eval-report-contract.mjs';

const QMAP_ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const EVAL_TOOL_CONTRACTS = loadQMapToolContracts(QMAP_ROOT);

const INVALID_BASELINE_REASON = Object.freeze({
  dryRun: INVALID_BASELINE_REASONS[0],
  transportAborted: INVALID_BASELINE_REASONS[1],
  emptyCases: INVALID_BASELINE_REASONS[2],
  allTransportSkipped: INVALID_BASELINE_REASONS[3],
  zeroMetricWindow: INVALID_BASELINE_REASONS[4]
});

function resolveQMapPath(target) {
  return path.isAbsolute(target) ? target : path.resolve(QMAP_ROOT, target);
}

function isPlainObject(value) {
  return Boolean(value) && typeof value === 'object' && !Array.isArray(value);
}

const EVAL_SYSTEM_PROMPT = [
  'You are a q-map assistant under tool-routing evaluation.',
  'Use tools for operational requests and avoid forbidden SQL-style tools.',
  'For provider+dataset administrative requests, use q-cumber query tools (queryQCumberTerritorialUnits or queryQCumberDataset), not generic loadData.',
  'When provider catalogs expose multiple datasets and datasetId is missing, call listQCumberDatasets and ask for explicit datasetId before any q-cumber query tool call.',
  'For H3 pipeline requests, continue after discovery with at least one operational H3 tool (tassellateDatasetLayer or aggregateDatasetToH3 or joinQMapDatasetsOnH3).',
  'For bridge load/save requests, you MUST call at least one bridge tool (loadData or saveDataToMap) before the final answer; dataset filtering alone is not sufficient.',
  'For styling requests, prioritize styling tools (setQMapLayerColorByField, setQMapLayerColorByStatsThresholds, setQMapTooltipFields, setQMapLayerOrder).',
  'If datasets are available from listQMapDatasets, continue the workflow instead of stopping at discovery.',
  'Use the shortest valid tool sequence: avoid optional discovery/redundant checks unless required by the case or needed to resolve missing identifiers.'
].join(' ');

function parseArgs(argv) {
  const out = {
    baseUrl:
      process.env.QMAP_AI_EVAL_BASE_URL ||
      process.env.EVAL_BASE_URL ||
      'http://localhost:8000/api/q-assistant',
    model: process.env.QMAP_AI_EVAL_MODEL || 'google/gemini-3-flash-preview',
    provider: process.env.QMAP_AI_EVAL_PROVIDER || 'openrouter',
    casesPath: 'tests/ai-eval/cases.sample.json',
    matrixPath: process.env.QMAP_AI_EVAL_MATRIX_PATH || 'tests/ai-eval/architecture-matrix.json',
    outDir: 'tests/ai-eval/results',
    runId: '',
    runType: String(process.env.QMAP_AI_EVAL_RUN_TYPE || '').trim(),
    temperature: 0,
    createBranch: false,
    branchPrefix: 'qmap-eval',
    dryRun: false,
    maxTurns: Number(process.env.QMAP_AI_EVAL_MAX_TURNS || 6),
    requestTimeoutMs: Number(process.env.QMAP_AI_EVAL_REQUEST_TIMEOUT_MS || 120000),
    requestRetries: Number(process.env.QMAP_AI_EVAL_REQUEST_RETRIES || 2),
    requestRetryDelayMs: Number(process.env.QMAP_AI_EVAL_REQUEST_RETRY_DELAY_MS || 350),
    preflightTimeoutMs: Number(process.env.QMAP_AI_EVAL_PREFLIGHT_TIMEOUT_MS || 5000),
    preflightRetries: Number(process.env.QMAP_AI_EVAL_PREFLIGHT_RETRIES || 6),
    preflightRetryDelayMs: Number(process.env.QMAP_AI_EVAL_PREFLIGHT_RETRY_DELAY_MS || 1500),
    bearerToken: String(
      process.env.QMAP_AI_EVAL_BEARER_TOKEN || process.env.EVAL_BEARER_TOKEN || ''
    ).trim(),
    disableDeterministicConstraints:
      ['1', 'true', 'yes', 'on'].includes(
        String(process.env.QMAP_AI_EVAL_DISABLE_DETERMINISTIC_CONSTRAINTS || '')
          .trim()
          .toLowerCase()
      ),
    transportFailureThreshold: Number(process.env.QMAP_AI_EVAL_TRANSPORT_FAILURE_THRESHOLD || 1),
    skipTransportPreflight:
      String(process.env.QMAP_AI_EVAL_SKIP_TRANSPORT_PREFLIGHT || '').trim() === '1',
    minPassRate: undefined,
    minAvgCaseScore: undefined,
    minP25CaseScore: undefined,
    minMinCaseScore: undefined,
    minAreaPassRate: undefined,
    minAreaAvgCaseScore: undefined,
    minAreaP25CaseScore: undefined,
    minAreaMinCaseScore: undefined
  };

  for (let i = 0; i < argv.length; i += 1) {
    const a = argv[i];
    const v = argv[i + 1];
    if (a === '--base-url' && v) out.baseUrl = v;
    if (a === '--model' && v) out.model = v;
    if (a === '--provider' && v) out.provider = v;
    if (a === '--cases' && v) out.casesPath = v;
    if (a === '--matrix' && v) out.matrixPath = v;
    if (a === '--out-dir' && v) out.outDir = v;
    if (a === '--run-id' && v) out.runId = v;
    if (a === '--run-type' && v) out.runType = v;
    if (a === '--temperature' && v) out.temperature = Number(v);
    if (a === '--branch-prefix' && v) out.branchPrefix = v;
    if (a === '--max-turns' && v) out.maxTurns = Math.max(1, Number(v) || 6);
    if (a === '--request-timeout-ms' && v) {
      out.requestTimeoutMs = Math.max(1000, optionalNonNegativeInt(v) || 120000);
    }
    if (a === '--request-retries' && v) {
      out.requestRetries = Math.max(1, optionalNonNegativeInt(v) || out.requestRetries);
    }
    if (a === '--request-retry-delay-ms' && v) {
      out.requestRetryDelayMs = Math.max(100, optionalNonNegativeInt(v) || out.requestRetryDelayMs);
    }
    if (a === '--preflight-timeout-ms' && v) {
      out.preflightTimeoutMs = Math.max(1000, optionalNonNegativeInt(v) || out.preflightTimeoutMs);
    }
    if (a === '--preflight-retries' && v) {
      out.preflightRetries = Math.max(1, optionalNonNegativeInt(v) || out.preflightRetries);
    }
    if (a === '--preflight-retry-delay-ms' && v) {
      out.preflightRetryDelayMs = Math.max(100, optionalNonNegativeInt(v) || out.preflightRetryDelayMs);
    }
    if (a === '--transport-failure-threshold' && v) {
      out.transportFailureThreshold = Math.max(1, optionalNonNegativeInt(v) || 1);
    }
    if (a === '--bearer-token' && v) out.bearerToken = String(v).trim();
    if (a === '--disable-deterministic-constraints') out.disableDeterministicConstraints = true;
    if (a === '--skip-transport-preflight') out.skipTransportPreflight = true;
    if (a === '--min-pass-rate' && v) out.minPassRate = Number(v);
    if (a === '--min-avg-case-score' && v) out.minAvgCaseScore = Number(v);
    if (a === '--min-p25-case-score' && v) out.minP25CaseScore = Number(v);
    if (a === '--min-min-case-score' && v) out.minMinCaseScore = Number(v);
    if (a === '--min-area-pass-rate' && v) out.minAreaPassRate = Number(v);
    if (a === '--min-area-avg-case-score' && v) out.minAreaAvgCaseScore = Number(v);
    if (a === '--min-area-p25-case-score' && v) out.minAreaP25CaseScore = Number(v);
    if (a === '--min-area-min-case-score' && v) out.minAreaMinCaseScore = Number(v);
    if (a === '--create-branch') out.createBranch = true;
    if (a === '--dry-run') out.dryRun = true;
  }

  const envGateValues = {
    minPassRate: process.env.QMAP_AI_EVAL_MIN_PASS_RATE,
    minAvgCaseScore: process.env.QMAP_AI_EVAL_MIN_AVG_CASE_SCORE,
    minP25CaseScore: process.env.QMAP_AI_EVAL_MIN_P25_CASE_SCORE,
    minMinCaseScore: process.env.QMAP_AI_EVAL_MIN_MIN_CASE_SCORE,
    minAreaPassRate: process.env.QMAP_AI_EVAL_MIN_AREA_PASS_RATE,
    minAreaAvgCaseScore: process.env.QMAP_AI_EVAL_MIN_AREA_AVG_CASE_SCORE,
    minAreaP25CaseScore: process.env.QMAP_AI_EVAL_MIN_AREA_P25_CASE_SCORE,
    minAreaMinCaseScore: process.env.QMAP_AI_EVAL_MIN_AREA_MIN_CASE_SCORE
  };
  for (const [key, rawValue] of Object.entries(envGateValues)) {
    if (out[key] !== undefined) continue;
    const parsed = Number(rawValue);
    if (Number.isFinite(parsed)) out[key] = parsed;
  }
  out.maxTurns = Math.max(1, optionalNonNegativeInt(out.maxTurns) || 6);
  out.requestTimeoutMs = Math.max(1000, optionalNonNegativeInt(out.requestTimeoutMs) || 120000);
  out.requestRetries = Math.max(1, optionalNonNegativeInt(out.requestRetries) || 2);
  out.requestRetryDelayMs = Math.max(100, optionalNonNegativeInt(out.requestRetryDelayMs) || 350);
  out.preflightTimeoutMs = Math.max(1000, optionalNonNegativeInt(out.preflightTimeoutMs) || 5000);
  out.preflightRetries = Math.max(1, optionalNonNegativeInt(out.preflightRetries) || 6);
  out.preflightRetryDelayMs = Math.max(100, optionalNonNegativeInt(out.preflightRetryDelayMs) || 1500);
  out.transportFailureThreshold = Math.max(1, optionalNonNegativeInt(out.transportFailureThreshold) || 1);
  return out;
}

function normalizeEvalRunType(value) {
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

function inferEvalRunTypeFromRunId(runId) {
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

function nowIso() {
  return new Date().toISOString();
}

function safeToken(value) {
  return String(value || '')
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9._-]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 60);
}

function ensureDir(dirPath) {
  fs.mkdirSync(dirPath, {recursive: true});
}

function readJson(filePath) {
  return JSON.parse(fs.readFileSync(filePath, 'utf-8'));
}

function readJsonIfExists(filePath) {
  if (!fs.existsSync(filePath)) return null;
  return readJson(filePath);
}

function optionalNumber(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : undefined;
}

function optionalNonNegativeInt(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return undefined;
  return Math.max(0, Math.trunc(n));
}

function round(value) {
  return Number(Number(value || 0).toFixed(3));
}

function asNonNegativeInt(value) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return null;
  return Math.max(0, Math.trunc(parsed));
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function resolveAuthHeaders(token) {
  const bearerToken = String(token || '').trim();
  if (!bearerToken) return {};
  return {authorization: `Bearer ${bearerToken}`};
}

function normalizeUpstreamUsage(value) {
  if (!isPlainObject(value)) return null;
  const promptTokens = asNonNegativeInt(
    value.promptTokens ?? value.prompt_tokens ?? value.input_tokens ?? value.promptTokenCount
  );
  const completionTokens = asNonNegativeInt(
    value.completionTokens ??
      value.completion_tokens ??
      value.output_tokens ??
      value.candidates_token_count
  );
  let totalTokens = asNonNegativeInt(
    value.totalTokens ?? value.total_tokens ?? value.totalTokenCount
  );
  if (totalTokens === null && promptTokens !== null && completionTokens !== null) {
    totalTokens = promptTokens + completionTokens;
  }
  if (promptTokens === null && completionTokens === null && totalTokens === null) return null;
  return {
    ...(promptTokens !== null ? {promptTokens} : {}),
    ...(completionTokens !== null ? {completionTokens} : {}),
    ...(totalTokens !== null ? {totalTokens} : {})
  };
}

function normalizeRequestPayloadTokenEstimate(value) {
  if (!isPlainObject(value)) return null;
  const estimatedPromptTokens = asNonNegativeInt(value.estimatedPromptTokens);
  const serializedChars = asNonNegativeInt(value.serializedChars);
  const messageCount = asNonNegativeInt(value.messageCount);
  const toolMessageCount = asNonNegativeInt(value.toolMessageCount);
  const toolCount = asNonNegativeInt(value.toolCount);
  const method = String(value.method || '').trim();
  if (
    estimatedPromptTokens === null &&
    serializedChars === null &&
    messageCount === null &&
    toolMessageCount === null &&
    toolCount === null &&
    !method
  ) {
    return null;
  }
  return {
    ...(estimatedPromptTokens !== null ? {estimatedPromptTokens} : {}),
    ...(serializedChars !== null ? {serializedChars} : {}),
    ...(messageCount !== null ? {messageCount} : {}),
    ...(toolMessageCount !== null ? {toolMessageCount} : {}),
    ...(toolCount !== null ? {toolCount} : {}),
    ...(method ? {method} : {})
  };
}

function normalizeTokenBudgetInfo(value) {
  if (!isPlainObject(value)) return null;
  const utilizationRatio =
    value.utilizationRatio === null || value.utilizationRatio === undefined
      ? undefined
      : optionalNumber(value.utilizationRatio);
  const contextLimitTokens = asNonNegativeInt(value.contextLimitTokens);
  const promptBudgetTokens = asNonNegativeInt(value.promptBudgetTokens);
  const reservedOutputTokens = asNonNegativeInt(value.reservedOutputTokens);
  const finalEstimatedPromptTokens = asNonNegativeInt(
    value.finalEstimatedPromptTokens ?? value.estimatedPromptTokens
  );
  const finalDecision = String(value.finalDecision ?? value.decision ?? '').trim();
  if (
    utilizationRatio === undefined &&
    contextLimitTokens === null &&
    promptBudgetTokens === null &&
    reservedOutputTokens === null &&
    finalEstimatedPromptTokens === null &&
    !finalDecision
  ) {
    return null;
  }
  return {
    ...(utilizationRatio !== undefined ? {utilizationRatio: round(utilizationRatio)} : {}),
    ...(contextLimitTokens !== null ? {contextLimitTokens} : {}),
    ...(promptBudgetTokens !== null ? {promptBudgetTokens} : {}),
    ...(reservedOutputTokens !== null ? {reservedOutputTokens} : {}),
    ...(finalEstimatedPromptTokens !== null ? {finalEstimatedPromptTokens} : {}),
    ...(finalDecision ? {finalDecision} : {})
  };
}

function normalizeQualityMetrics(value) {
  if (!isPlainObject(value)) return null;
  const knownKeys = new Set([
    'falseSuccessClaimCount',
    'contractSchemaMismatchCount',
    'contractResponseMismatchCount',
    'waitTimeoutCount',
    'workflowScore',
    'hasDatasetMutation',
    'postCreateWaitCountOk',
    'cloudFailureSeen',
    'cloudFailureExhausted',
    'cloudRecoveryValidated',
    'clarificationPending',
    'clarificationReason',
    'clarificationQuestionSeen',
    'clarificationOptionsCount',
    'responseModeHint'
  ]);
  const falseSuccessClaimCount = asNonNegativeInt(value.falseSuccessClaimCount);
  const contractSchemaMismatchCount = asNonNegativeInt(value.contractSchemaMismatchCount);
  const contractResponseMismatchCount = asNonNegativeInt(value.contractResponseMismatchCount);
  const waitTimeoutCount = asNonNegativeInt(value.waitTimeoutCount);
  const workflowScore =
    value.workflowScore === null || value.workflowScore === undefined
      ? undefined
      : optionalNumber(value.workflowScore);
  const hasDatasetMutation =
    typeof value.hasDatasetMutation === 'boolean' ? value.hasDatasetMutation : undefined;
  const postCreateWaitCountOk =
    typeof value.postCreateWaitCountOk === 'boolean' ? value.postCreateWaitCountOk : undefined;
  const cloudFailureSeen =
    typeof value.cloudFailureSeen === 'boolean' ? value.cloudFailureSeen : undefined;
  const cloudFailureExhausted =
    typeof value.cloudFailureExhausted === 'boolean' ? value.cloudFailureExhausted : undefined;
  const cloudRecoveryValidated =
    typeof value.cloudRecoveryValidated === 'boolean' ? value.cloudRecoveryValidated : undefined;
  const clarificationPending =
    typeof value.clarificationPending === 'boolean' ? value.clarificationPending : undefined;
  const clarificationReason = String(value.clarificationReason || '').trim() || undefined;
  const clarificationQuestionSeen =
    typeof value.clarificationQuestionSeen === 'boolean' ? value.clarificationQuestionSeen : undefined;
  const clarificationOptionsCount = asNonNegativeInt(value.clarificationOptionsCount);
  const responseModeHint = ['clarification', 'limitation'].includes(
    String(value.responseModeHint || '').trim().toLowerCase()
  )
    ? String(value.responseModeHint || '').trim().toLowerCase()
    : undefined;
  const extraMetrics = {};
  for (const [key, raw] of Object.entries(value)) {
    if (knownKeys.has(key)) continue;
    const normalized = normalizeExtraQualityMetricValue(raw);
    if (normalized !== undefined) {
      extraMetrics[key] = normalized;
    }
  }
  if (
    falseSuccessClaimCount === null &&
    contractSchemaMismatchCount === null &&
    contractResponseMismatchCount === null &&
    waitTimeoutCount === null &&
    workflowScore === undefined &&
    hasDatasetMutation === undefined &&
    postCreateWaitCountOk === undefined &&
    cloudFailureSeen === undefined &&
    cloudFailureExhausted === undefined &&
    cloudRecoveryValidated === undefined &&
    clarificationPending === undefined &&
    clarificationReason === undefined &&
    clarificationQuestionSeen === undefined &&
    clarificationOptionsCount === null &&
    responseModeHint === undefined &&
    Object.keys(extraMetrics).length === 0
  ) {
    return null;
  }
  return {
    ...(falseSuccessClaimCount !== null ? {falseSuccessClaimCount} : {}),
    ...(contractSchemaMismatchCount !== null ? {contractSchemaMismatchCount} : {}),
    ...(contractResponseMismatchCount !== null ? {contractResponseMismatchCount} : {}),
    ...(waitTimeoutCount !== null ? {waitTimeoutCount} : {}),
    ...(workflowScore !== undefined ? {workflowScore: round(workflowScore)} : {}),
    ...(hasDatasetMutation !== undefined ? {hasDatasetMutation} : {}),
    ...(postCreateWaitCountOk !== undefined ? {postCreateWaitCountOk} : {}),
    ...(cloudFailureSeen !== undefined ? {cloudFailureSeen} : {}),
    ...(cloudFailureExhausted !== undefined ? {cloudFailureExhausted} : {}),
    ...(cloudRecoveryValidated !== undefined ? {cloudRecoveryValidated} : {}),
    ...(clarificationPending !== undefined ? {clarificationPending} : {}),
    ...(clarificationReason !== undefined ? {clarificationReason} : {}),
    ...(clarificationQuestionSeen !== undefined ? {clarificationQuestionSeen} : {}),
    ...(clarificationOptionsCount !== null ? {clarificationOptionsCount} : {}),
    ...(responseModeHint !== undefined ? {responseModeHint} : {}),
    ...extraMetrics
  };
}

function normalizeExtraQualityMetricValue(value) {
  if (typeof value === 'boolean') return value;
  if (typeof value === 'number' && Number.isFinite(value)) return round(value);
  if (typeof value === 'string') {
    const trimmed = value.trim();
    return trimmed ? trimmed : undefined;
  }
  if (Array.isArray(value)) {
    const normalized = [];
    const seen = new Set();
    for (const item of value) {
      const next = normalizeExtraQualityMetricValue(item);
      if (next === undefined || isPlainObject(next) || Array.isArray(next)) continue;
      const signature = JSON.stringify(next);
      if (seen.has(signature)) continue;
      seen.add(signature);
      normalized.push(next);
    }
    return normalized.length ? normalized : undefined;
  }
  return undefined;
}

function extractResponseEvalDiagnostics(payload) {
  const root = isPlainObject(payload) ? payload : {};
  const qAssistant = isPlainObject(root.qAssistant) ? root.qAssistant : {};
  const choices = Array.isArray(root.choices) ? root.choices : [];
  let choiceUsage = null;
  for (const choice of choices) {
    choiceUsage = normalizeUpstreamUsage(choice?.usage);
    if (choiceUsage) break;
  }
  return {
    usedProvider: String(qAssistant.usedProvider || '').trim(),
    usedModel: String(qAssistant.usedModel || '').trim(),
    upstreamUsage:
      normalizeUpstreamUsage(qAssistant.upstreamUsage) ||
      normalizeUpstreamUsage(root.usage) ||
      choiceUsage,
    requestPayloadTokenEstimate: normalizeRequestPayloadTokenEstimate(
      qAssistant.requestPayloadTokenEstimate
    ),
    tokenBudget: normalizeTokenBudgetInfo(qAssistant.tokenBudget),
    qualityMetrics: normalizeQualityMetrics(qAssistant.qualityMetrics)
  };
}

function createEvalDiagnosticsAggregate() {
  return {
    requestCount: 0,
    usedProviders: new Set(),
    usedModels: new Set(),
    promptTokens: 0,
    completionTokens: 0,
    totalTokens: 0,
    upstreamUsageSamples: 0,
    estimatedPromptTokens: 0,
    estimateSamples: 0,
    utilizationRatios: [],
    tokenBudgetSamples: 0,
    warnCount: 0,
    compactCount: 0,
    hardCount: 0,
    maxContextLimitTokens: 0,
    minPromptBudgetTokens: null,
    qualityMetricSamples: 0,
    workflowScores: [],
    falseSuccessClaimCount: 0,
    falseSuccessCases: 0,
    contractSchemaMismatchCount: 0,
    contractResponseMismatchCount: 0,
    waitTimeoutCount: 0,
    hasDatasetMutation: null,
    postCreateWaitCountOk: null,
    cloudFailureSeen: null,
    cloudFailureExhausted: null,
    cloudRecoveryValidated: null,
    clarificationPending: null,
    clarificationReason: '',
    clarificationQuestionSeen: null,
    clarificationOptionsCount: 0,
    responseModeHint: '',
    qualityMetricsExtra: {}
  };
}

function mergeAggregateQualityMetricExtra(aggregate, key, value) {
  if (!aggregate || !key) return;
  const normalized = normalizeExtraQualityMetricValue(value);
  if (normalized === undefined) return;
  const extras = isPlainObject(aggregate.qualityMetricsExtra) ? aggregate.qualityMetricsExtra : {};
  const previous = extras[key];
  if (Array.isArray(normalized)) {
    const merged = [];
    const seen = new Set();
    for (const candidate of [...(Array.isArray(previous) ? previous : []), ...normalized]) {
      const signature = JSON.stringify(candidate);
      if (seen.has(signature)) continue;
      seen.add(signature);
      merged.push(candidate);
    }
    extras[key] = merged;
  } else {
    extras[key] = normalized;
  }
  aggregate.qualityMetricsExtra = extras;
}

function mergeEvalDiagnosticsAggregate(aggregate, diagnostics) {
  if (!aggregate || !diagnostics || typeof diagnostics !== 'object') return;
  aggregate.requestCount += 1;
  if (diagnostics.usedProvider) aggregate.usedProviders.add(diagnostics.usedProvider);
  if (diagnostics.usedModel) aggregate.usedModels.add(diagnostics.usedModel);

  const usage = normalizeUpstreamUsage(diagnostics.upstreamUsage);
  if (usage) {
    aggregate.upstreamUsageSamples += 1;
    aggregate.promptTokens += Number(usage.promptTokens || 0);
    aggregate.completionTokens += Number(usage.completionTokens || 0);
    aggregate.totalTokens += Number(
      usage.totalTokens ?? Number(usage.promptTokens || 0) + Number(usage.completionTokens || 0)
    );
  }

  const estimate = normalizeRequestPayloadTokenEstimate(diagnostics.requestPayloadTokenEstimate);
  if (estimate) {
    aggregate.estimateSamples += 1;
    aggregate.estimatedPromptTokens += Number(estimate.estimatedPromptTokens || 0);
  }

  const tokenBudget = normalizeTokenBudgetInfo(diagnostics.tokenBudget);
  if (tokenBudget) {
    aggregate.tokenBudgetSamples += 1;
    if (tokenBudget.utilizationRatio !== undefined) {
      aggregate.utilizationRatios.push(Number(tokenBudget.utilizationRatio || 0));
    }
    const decision = String(tokenBudget.finalDecision || '').trim().toLowerCase();
    if (decision === 'warn') aggregate.warnCount += 1;
    if (decision === 'compact') aggregate.compactCount += 1;
    if (decision === 'hard') aggregate.hardCount += 1;
    if (Number(tokenBudget.contextLimitTokens || 0) > aggregate.maxContextLimitTokens) {
      aggregate.maxContextLimitTokens = Number(tokenBudget.contextLimitTokens || 0);
    }
    const promptBudgetTokens = asNonNegativeInt(tokenBudget.promptBudgetTokens);
    if (promptBudgetTokens !== null) {
      aggregate.minPromptBudgetTokens =
        aggregate.minPromptBudgetTokens === null
          ? promptBudgetTokens
          : Math.min(aggregate.minPromptBudgetTokens, promptBudgetTokens);
    }
  }

  const qualityMetrics = normalizeQualityMetrics(diagnostics.qualityMetrics);
  if (qualityMetrics) {
    aggregate.qualityMetricSamples += 1;
    for (const [key, value] of Object.entries(qualityMetrics)) {
      if (
        ![
          'workflowScore',
          'falseSuccessClaimCount',
          'contractSchemaMismatchCount',
          'contractResponseMismatchCount',
          'waitTimeoutCount',
          'hasDatasetMutation',
          'postCreateWaitCountOk',
          'cloudFailureSeen',
          'cloudFailureExhausted',
          'cloudRecoveryValidated',
          'clarificationPending',
          'clarificationReason',
          'clarificationQuestionSeen',
          'clarificationOptionsCount',
          'responseModeHint'
        ].includes(key)
      ) {
        mergeAggregateQualityMetricExtra(aggregate, key, value);
      }
    }
    if (qualityMetrics.workflowScore !== undefined) {
      aggregate.workflowScores.push(Number(qualityMetrics.workflowScore || 0));
    }
    const falseSuccessClaimCount = Number(qualityMetrics.falseSuccessClaimCount || 0);
    aggregate.falseSuccessClaimCount += falseSuccessClaimCount;
    if (falseSuccessClaimCount > 0) aggregate.falseSuccessCases += 1;
    aggregate.contractSchemaMismatchCount += Number(qualityMetrics.contractSchemaMismatchCount || 0);
    aggregate.contractResponseMismatchCount += Number(qualityMetrics.contractResponseMismatchCount || 0);
    aggregate.waitTimeoutCount += Number(qualityMetrics.waitTimeoutCount || 0);
    if (typeof qualityMetrics.hasDatasetMutation === 'boolean') {
      aggregate.hasDatasetMutation =
        aggregate.hasDatasetMutation === true ? true : qualityMetrics.hasDatasetMutation;
    }
    if (typeof qualityMetrics.postCreateWaitCountOk === 'boolean') {
      aggregate.postCreateWaitCountOk =
        aggregate.postCreateWaitCountOk === true ? true : qualityMetrics.postCreateWaitCountOk;
    }
    if (typeof qualityMetrics.cloudFailureSeen === 'boolean') {
      aggregate.cloudFailureSeen =
        aggregate.cloudFailureSeen === true ? true : qualityMetrics.cloudFailureSeen;
    }
    if (typeof qualityMetrics.cloudFailureExhausted === 'boolean') {
      aggregate.cloudFailureExhausted =
        aggregate.cloudFailureExhausted === true ? true : qualityMetrics.cloudFailureExhausted;
    }
    if (typeof qualityMetrics.cloudRecoveryValidated === 'boolean') {
      aggregate.cloudRecoveryValidated =
        aggregate.cloudRecoveryValidated === true ? true : qualityMetrics.cloudRecoveryValidated;
    }
    if (typeof qualityMetrics.clarificationPending === 'boolean') {
      aggregate.clarificationPending =
        aggregate.clarificationPending === true ? true : qualityMetrics.clarificationPending;
    }
    if (qualityMetrics.clarificationReason) {
      aggregate.clarificationReason = qualityMetrics.clarificationReason;
    }
    if (typeof qualityMetrics.clarificationQuestionSeen === 'boolean') {
      aggregate.clarificationQuestionSeen =
        aggregate.clarificationQuestionSeen === true
          ? true
          : qualityMetrics.clarificationQuestionSeen;
    }
    aggregate.clarificationOptionsCount = Math.max(
      Number(aggregate.clarificationOptionsCount || 0),
      Number(qualityMetrics.clarificationOptionsCount || 0)
    );
    if (qualityMetrics.responseModeHint) {
      aggregate.responseModeHint = qualityMetrics.responseModeHint;
    }
  }
}

function finalizeEvalDiagnosticsAggregate(aggregate) {
  if (!aggregate || aggregate.requestCount <= 0) return null;
  const maxUtilizationRatio = aggregate.utilizationRatios.length
    ? round(Math.max(...aggregate.utilizationRatios))
    : null;
  const avgWorkflowScore = aggregate.workflowScores.length
    ? round(
        aggregate.workflowScores.reduce((acc, value) => acc + Number(value || 0), 0) /
          aggregate.workflowScores.length
      )
    : null;
  return {
    requestCount: aggregate.requestCount,
    usedProviders: Array.from(aggregate.usedProviders).sort(),
    usedModels: Array.from(aggregate.usedModels).sort(),
    upstreamUsage:
      aggregate.upstreamUsageSamples > 0
        ? {
            promptTokens: aggregate.promptTokens,
            completionTokens: aggregate.completionTokens,
            totalTokens: aggregate.totalTokens,
            sampleCount: aggregate.upstreamUsageSamples
          }
        : null,
    requestPayloadTokenEstimate:
      aggregate.estimateSamples > 0
        ? {
            estimatedPromptTokens: aggregate.estimatedPromptTokens,
            sampleCount: aggregate.estimateSamples
          }
        : null,
    tokenBudget:
      aggregate.tokenBudgetSamples > 0
        ? {
            sampleCount: aggregate.tokenBudgetSamples,
            warnCount: aggregate.warnCount,
            compactCount: aggregate.compactCount,
            hardCount: aggregate.hardCount,
            maxUtilizationRatio,
            maxContextLimitTokens: aggregate.maxContextLimitTokens || null,
            minPromptBudgetTokens: aggregate.minPromptBudgetTokens
          }
        : null,
    qualityMetrics:
      aggregate.qualityMetricSamples > 0
        ? {
            sampleCount: aggregate.qualityMetricSamples,
            avgWorkflowScore,
            falseSuccessClaimCount: aggregate.falseSuccessClaimCount,
            falseSuccessCases: aggregate.falseSuccessCases,
            contractSchemaMismatchCount: aggregate.contractSchemaMismatchCount,
            contractResponseMismatchCount: aggregate.contractResponseMismatchCount,
            waitTimeoutCount: aggregate.waitTimeoutCount,
            ...(aggregate.hasDatasetMutation !== null
              ? {hasDatasetMutation: aggregate.hasDatasetMutation}
              : {}),
            ...(aggregate.postCreateWaitCountOk !== null
              ? {postCreateWaitCountOk: aggregate.postCreateWaitCountOk}
              : {}),
            ...(aggregate.cloudFailureSeen !== null
              ? {cloudFailureSeen: aggregate.cloudFailureSeen}
              : {}),
            ...(aggregate.cloudFailureExhausted !== null
              ? {cloudFailureExhausted: aggregate.cloudFailureExhausted}
              : {}),
            ...(aggregate.cloudRecoveryValidated !== null
              ? {cloudRecoveryValidated: aggregate.cloudRecoveryValidated}
              : {}),
            ...(aggregate.clarificationPending !== null
              ? {clarificationPending: aggregate.clarificationPending}
              : {}),
            ...(aggregate.clarificationReason
              ? {clarificationReason: aggregate.clarificationReason}
              : {}),
            ...(aggregate.clarificationQuestionSeen !== null
              ? {clarificationQuestionSeen: aggregate.clarificationQuestionSeen}
              : {}),
            ...(Number(aggregate.clarificationOptionsCount || 0) > 0
              ? {clarificationOptionsCount: Number(aggregate.clarificationOptionsCount || 0)}
              : {}),
            ...(aggregate.responseModeHint
              ? {responseModeHint: aggregate.responseModeHint}
              : {}),
            ...(isPlainObject(aggregate.qualityMetricsExtra) ? aggregate.qualityMetricsExtra : {})
          }
        : null
  };
}

// Keep keyword matching semantically robust across language/style variants.
const KEYWORD_SYNONYM_GROUPS = [
  ['discovery', 'inventario', 'inventory', 'catalogo'],
  ['territorial', 'territoriale', 'amministrativa', 'amministrative', 'amministrativi', 'unita amministrative', 'comuni', 'province', 'giurisdizione', 'giurisdizioni'],
  ['spaziale', 'spatial', 'geospaziale'],
  ['bounding box', 'bbox', 'area delimitata', 'area di interesse delimitata'],
  ['ordering', 'ordinamento', 'ordinati', 'ordinate', 'ordine', 'sort', 'sorted'],
  ['load', 'loaded', 'caricamento', 'caricato', 'caricata', 'caricati', 'caricate'],
  ['routing', 'percorso', 'percorsi', 'itinerario', 'instradamento', 'route', 'routes'],
  ['isochrone', 'isocrona', 'isocrone'],
  ['distribuzioni', 'distribuzione', 'distribution', 'histogram', 'boxplot'],
  ['relazioni', 'relazione', 'correlazioni', 'correlation', 'scatter', 'bubble', 'pcp'],
  ['categorie', 'categoria', 'category bars', 'word cloud'],
  ['intersezione', 'intersection', 'overlay intersection'],
  ['differenza', 'difference', 'overlay difference', 'symmetric difference'],
  ['ripulizia', 'cleanup', 'pulizia', 'clean', 'semplificazione', 'simplify'],
  ['prossimita', 'nearest', 'buffer'],
  ['copertura', 'coverage'],
  ['normalizzazione', 'normalizzato', 'normalizzata', 'normalizzati', 'normalizzate', 'densita', 'density'],
  ['filtro', 'filtri', 'filtrato', 'filtrata', 'filtrati', 'filtrate', 'filtraggio'],
  ['materializza', 'materializzato', 'materializzata', 'materializzati', 'materializzate', 'creato', 'creata'],
  ['preview', 'anteprima', 'ispezione'],
  ['report', 'riepilogo', 'sintesi'],
  ['superamenti', 'superamento', 'non conformi', 'exceedance', 'exceedances'],
  ['metrica', 'metriche', 'indicatore', 'indicatori', 'valore', 'valori'],
  ['merge', 'unione', 'join'],
  ['tessellazione', 'tassellazione'],
  ['r6', 'h3 r6', 'res r6', 'risoluzione 6', 'resolution 6', 'h3 6'],
  ['r7', 'h3 r7', 'res r7', 'risoluzione 7', 'resolution 7', 'h3 7'],
  ['r8', 'h3 r8', 'res r8', 'risoluzione 8', 'resolution 8', 'h3 8'],
  ['proporzionale', 'area weighted', 'area-weighted'],
  ['discreto', 'discrete', 'discrete allocation'],
  ['correzione', 'correzioni', 'paint', 'modifica', 'editing'],
  ['anello', 'ring'],
  ['popolazione', 'popola', 'popolare']
];

function normalizeForMatch(text) {
  return String(text || '')
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function isPresentToolArgValue(value) {
  if (typeof value === 'string') return Boolean(value.trim());
  if (Array.isArray(value)) return value.length > 0;
  return value !== undefined && value !== null;
}

function matchesContractArgType(value, schema) {
  if (!schema || typeof schema !== 'object' || Array.isArray(schema)) return true;
  if (Array.isArray(schema.anyOf) && schema.anyOf.length) {
    return schema.anyOf.some(candidate => matchesContractArgType(value, candidate));
  }
  const expectedType = String(schema.type || '').trim();
  if (!expectedType) return true;
  if (expectedType === 'string') return typeof value === 'string';
  if (expectedType === 'boolean') return typeof value === 'boolean';
  if (expectedType === 'number') return typeof value === 'number' && Number.isFinite(value);
  if (expectedType === 'integer') return Number.isInteger(value);
  if (expectedType === 'array') {
    if (!Array.isArray(value)) return false;
    if (!schema.items || typeof schema.items !== 'object' || Array.isArray(schema.items)) return true;
    return value.every(item => matchesContractArgType(item, schema.items));
  }
  if (expectedType === 'object') return isPlainObject(value);
  return true;
}

function canonicalizeMockToolArgs(toolName, rawArgs) {
  const normalizedToolName = String(toolName || '').trim();
  const args = isPlainObject(rawArgs) ? {...rawArgs} : {};
  if (!normalizedToolName) return args;

  if (normalizedToolName === 'waitForQMapDataset' || normalizedToolName === 'countQMapRows') {
    const datasetName = String(args.datasetName || '').trim();
    const datasetRef = String(args.datasetRef || '').trim();
    const datasetId = String(args.datasetId || '').trim();
    if (!datasetName) {
      const canonicalDatasetName = datasetRef || datasetId;
      if (canonicalDatasetName) args.datasetName = canonicalDatasetName;
    }
    delete args.datasetRef;
    delete args.datasetId;
  }

  return args;
}

function validateMockToolArgs(toolName, rawArgs) {
  const args = canonicalizeMockToolArgs(toolName, rawArgs);
  const contract =
    EVAL_TOOL_CONTRACTS?.tools?.[String(toolName || '').trim()] ||
    null;
  const schema =
    contract?.argsSchema && typeof contract.argsSchema === 'object' && !Array.isArray(contract.argsSchema)
      ? contract.argsSchema
      : EVAL_TOOL_CONTRACTS?.defaults?.argsSchema;
  if (!schema || String(schema.type || '').trim() !== 'object') {
    return {ok: true, args};
  }

  const properties = isPlainObject(schema.properties) ? schema.properties : {};
  const required = Array.isArray(schema.required) ? schema.required.map(key => String(key || '').trim()).filter(Boolean) : [];
  const missingRequired = required.filter(key => !isPresentToolArgValue(args[key]));
  const unknownArgs =
    schema.additionalProperties === false
      ? Object.keys(args).filter(key => !Object.prototype.hasOwnProperty.call(properties, key))
      : [];
  const invalidTypes = Object.entries(args)
    .filter(([key, value]) =>
      Object.prototype.hasOwnProperty.call(properties, key) && !matchesContractArgType(value, properties[key])
    )
    .map(([key]) => key);

  if (!missingRequired.length && !unknownArgs.length && !invalidTypes.length) {
    return {ok: true, args};
  }

  const details = [];
  if (missingRequired.length) {
    details.push(`missing required argument(s): ${missingRequired.join(', ')}`);
  }
  if (unknownArgs.length) {
    details.push(`unknown argument(s): ${unknownArgs.join(', ')}`);
  }
  if (invalidTypes.length) {
    details.push(`invalid argument type(s): ${invalidTypes.join(', ')}`);
  }
  if (String(toolName || '').trim() === 'waitForQMapDataset' && missingRequired.includes('datasetName')) {
    details.push('Use datasetName with the canonical datasetRef or dataset label returned by the previous load/create tool.');
  }

  return {
    ok: false,
    args,
    errorResult: {
      success: false,
      contractViolation: true,
      missingArguments: missingRequired,
      unknownArguments: unknownArgs,
      invalidArgumentTypes: invalidTypes,
      details: `Mock eval contract rejected ${String(toolName || '').trim() || 'tool'} call: ${details.join(' ')}`
    }
  };
}

function buildKeywordSynonymIndex(groups) {
  const index = new Map();
  for (const rawGroup of groups) {
    const group = Array.from(
      new Set(
        (Array.isArray(rawGroup) ? rawGroup : [])
          .map(term => normalizeForMatch(term))
          .filter(Boolean)
      )
    );
    for (const term of group) {
      if (!index.has(term)) index.set(term, new Set());
      const related = index.get(term);
      for (const candidate of group) {
        if (candidate !== term) related.add(candidate);
      }
    }
  }
  return new Map([...index.entries()].map(([term, related]) => [term, [...related]]));
}

const KEYWORD_SYNONYMS = buildKeywordSynonymIndex(KEYWORD_SYNONYM_GROUPS);

function stemToken(token) {
  const raw = String(token || '');
  if (raw.length <= 4) return raw;
  const suffixes = [
    'izzazioni',
    'izzazione',
    'azioni',
    'azione',
    'zioni',
    'zione',
    'mente',
    'ing',
    'ed',
    'ly',
    'es',
    's'
  ];
  for (const suffix of suffixes) {
    if (raw.length - suffix.length < 4) continue;
    if (raw.endsWith(suffix)) return raw.slice(0, -suffix.length);
  }
  return raw;
}

function buildKeywordCandidates(keyword) {
  const normalized = normalizeForMatch(keyword);
  const candidates = new Set([normalized]);
  const mapped = KEYWORD_SYNONYMS.get(normalized) || [];
  for (const variant of mapped) candidates.add(normalizeForMatch(variant));
  return [...candidates].filter(Boolean);
}

function isEditDistanceAtMostOne(a, b) {
  if (a === b) return true;
  const left = String(a || '');
  const right = String(b || '');
  const leftLen = left.length;
  const rightLen = right.length;
  if (Math.abs(leftLen - rightLen) > 1) return false;

  let i = 0;
  let j = 0;
  let edits = 0;
  while (i < leftLen && j < rightLen) {
    if (left[i] === right[j]) {
      i += 1;
      j += 1;
      continue;
    }
    edits += 1;
    if (edits > 1) return false;
    if (leftLen > rightLen) {
      i += 1;
    } else if (rightLen > leftLen) {
      j += 1;
    } else {
      i += 1;
      j += 1;
    }
  }
  if (i < leftLen || j < rightLen) edits += 1;
  return edits <= 1;
}

function keywordMatchesContent(keyword, normalizedContent, contentTokens, contentStems) {
  const candidates = buildKeywordCandidates(keyword);
  for (const candidate of candidates) {
    if (!candidate) continue;
    const candidateTokens = candidate.split(' ').filter(Boolean);
    if (!candidateTokens.length) continue;
    if (candidateTokens.length > 1) {
      if (normalizedContent.includes(candidate)) return true;
      const phraseTokenMatch = candidateTokens.every(token => {
        const stem = stemToken(token);
        return contentTokens.has(token) || (stem && contentStems.has(stem));
      });
      if (phraseTokenMatch) return true;
      continue;
    }
    const token = candidateTokens[0];
    const stem = stemToken(token);
    if (contentTokens.has(token)) return true;
    if (stem && contentStems.has(stem)) return true;
    if (token.length >= 6) {
      const prefix = token.slice(0, 5);
      const hasPrefix = [...contentTokens].some(contentToken => contentToken.startsWith(prefix));
      if (hasPrefix) return true;
    }
    if (token.length >= 8) {
      const hasNearToken = [...contentTokens].some(contentToken =>
        isEditDistanceAtMostOne(token, contentToken)
      );
      if (hasNearToken) return true;
    }
  }
  return false;
}

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

function hasFailedToolEvidence(toolResults) {
  return (Array.isArray(toolResults) ? toolResults : []).some(row => {
    if (!row || typeof row !== 'object') return false;
    if (row.success === false || row.ok === false) return true;
    const status = Number(row.status);
    if (Number.isFinite(status)) return status >= 400;
    const outcome = String(row.outcome || '').trim().toLowerCase();
    return outcome === 'failed' || outcome === 'error';
  });
}

function evaluateExpectedResponseMode(caseDef, content, toolResults = [], evalDiagnostics = null) {
  const expectedMode = String(caseDef?.expected_response_mode || '').trim().toLowerCase();
  if (!expectedMode) {
    return {evaluated: false, pass: true, expectedMode: '', observedMode: ''};
  }
  const qualityMetrics = normalizeQualityMetrics(evalDiagnostics?.qualityMetrics);
  const runtimeResponseModeHint = String(qualityMetrics?.responseModeHint || '').trim().toLowerCase();
  const rawContent = String(content || '');
  const normalizedContent = normalizeForMatch(rawContent);
  const hasQuestionMark = rawContent.includes('?');
  const clarificationCue =
    hasQuestionMark || includesAnyPhrase(normalizedContent, CLARIFICATION_HINTS);
  const limitationCue =
    includesAnyPhrase(normalizedContent, LIMITATION_HINTS) || textAcknowledgesNonSuccessOutcome(rawContent);
  const failedToolEvidence = hasFailedToolEvidence(toolResults);
  const requiredMarkers = Array.isArray(caseDef?.response_mode_markers_any)
    ? caseDef.response_mode_markers_any.map(value => String(value || '').trim()).filter(Boolean)
    : [];
  const matchedMarkers = requiredMarkers.filter(marker =>
    includesAnyPhrase(normalizedContent, [marker])
  );
  const nonSuccessAcknowledged = textAcknowledgesNonSuccessOutcome(rawContent);
  const structuralLimitationPass =
    expectedMode === 'limitation' && (failedToolEvidence || nonSuccessAcknowledged);
  const structuralClarificationPass =
    expectedMode === 'clarification' && runtimeResponseModeHint === 'clarification';
  const markersPass =
    requiredMarkers.length === 0 ||
    matchedMarkers.length > 0 ||
    structuralLimitationPass ||
    structuralClarificationPass;
  let observedMode = '';
  if (runtimeResponseModeHint === 'clarification' || runtimeResponseModeHint === 'limitation') {
    observedMode = runtimeResponseModeHint;
  } else if ((limitationCue || failedToolEvidence) && !hasQuestionMark) observedMode = 'limitation';
  else if (clarificationCue) observedMode = 'clarification';
  else if (limitationCue) observedMode = 'limitation';
  return {
    evaluated: true,
    pass: observedMode === expectedMode && markersPass,
    expectedMode,
    observedMode,
    hasQuestionMark,
    runtimeResponseModeHint,
    failedToolEvidence,
    nonSuccessAcknowledged,
    structuralClarificationPass,
    structuralLimitationPass,
    requiredMarkers,
    matchedMarkers
  };
}

function evaluateGroundedFinalAnswer(caseDef, content, evalDiagnostics, toolCalls) {
  if (caseDef?.require_grounded_final_answer !== true) {
    return {evaluated: false, pass: true};
  }
  const qualityMetrics = normalizeQualityMetrics(evalDiagnostics?.qualityMetrics);
  const rawContent = String(content || '').trim();
  const hasText = Boolean(rawContent);
  const falseSuccessClaimCount = Number(qualityMetrics?.falseSuccessClaimCount || 0);
  const hasDatasetMutation = qualityMetrics?.hasDatasetMutation === true;
  const postCreateWaitCountOk = qualityMetrics?.postCreateWaitCountOk === true;
  const cloudFailureSeen = qualityMetrics?.cloudFailureSeen === true;
  const cloudRecoveryValidated = qualityMetrics?.cloudRecoveryValidated === true;
  const workflowScore = optionalNumber(qualityMetrics?.workflowScore);
  const observedTools = new Set(
    (Array.isArray(toolCalls) ? toolCalls : []).map(value => String(value || '').trim().toLowerCase()).filter(Boolean)
  );
  const requiredToolsAll = Array.isArray(caseDef?.grounded_required_tools_all)
    ? caseDef.grounded_required_tools_all
        .map(value => String(value || '').trim().toLowerCase())
        .filter(Boolean)
    : [];
  const matchedRequiredToolsAll = requiredToolsAll.filter(tool => observedTools.has(tool));
  const requiredToolsPass =
    requiredToolsAll.length === 0 || matchedRequiredToolsAll.length === requiredToolsAll.length;
  const requiresCloudValidatedRecovery =
    requiredToolsAll.includes('loadcloudmapandwait') && requiredToolsAll.includes('waitforqmapdataset');
  const pass =
    hasText &&
    falseSuccessClaimCount === 0 &&
    (!hasDatasetMutation || postCreateWaitCountOk) &&
    (!requiresCloudValidatedRecovery || !cloudFailureSeen || cloudRecoveryValidated) &&
    (workflowScore === undefined || workflowScore >= 80) &&
    requiredToolsPass;
  return {
    evaluated: true,
    pass,
    hasText,
    falseSuccessClaimCount,
    hasDatasetMutation,
    postCreateWaitCountOk,
    cloudFailureSeen,
    cloudRecoveryValidated,
    workflowScore: workflowScore === undefined ? null : round(workflowScore),
    requiredToolsAll,
    matchedRequiredToolsAll
  };
}

function percentile(values, q) {
  if (!values.length) return 0;
  if (values.length === 1) return round(values[0]);
  const sorted = [...values].sort((a, b) => a - b);
  const clampedQ = Math.max(0, Math.min(1, q));
  const pos = (sorted.length - 1) * clampedQ;
  const lower = Math.floor(pos);
  const upper = Math.ceil(pos);
  if (lower === upper) return round(sorted[lower]);
  const weight = pos - lower;
  return round(sorted[lower] + (sorted[upper] - sorted[lower]) * weight);
}

function normalizeCriticality(value) {
  const normalized = String(value || 'standard')
    .trim()
    .toLowerCase();
  return normalized === 'critical' ? 'critical' : 'standard';
}

function parseMetricGates(gatesBlock) {
  const source = gatesBlock && typeof gatesBlock === 'object' ? gatesBlock : {};
  return {
    minPassRate: optionalNumber(source.min_pass_rate),
    minAvgCaseScore: optionalNumber(source.min_avg_case_score),
    minP25CaseScore: optionalNumber(source.min_p25_case_score),
    minMinCaseScore: optionalNumber(source.min_min_case_score)
  };
}

function loadMatrixPolicy(matrix) {
  const rootPolicy = matrix?.evaluationPolicy && typeof matrix.evaluationPolicy === 'object'
    ? matrix.evaluationPolicy
    : {};
  const defaultsByCriticality =
    rootPolicy?.criticalityDefaults && typeof rootPolicy.criticalityDefaults === 'object'
      ? rootPolicy.criticalityDefaults
      : {};
  const areaGatesByAreaId = {};
  for (const area of Array.isArray(matrix?.areas) ? matrix.areas : []) {
    const areaId = String(area?.id || '').trim();
    if (!areaId) continue;
    areaGatesByAreaId[areaId] = parseMetricGates(area?.gates);
  }

  return {
    runGates: parseMetricGates(rootPolicy?.runGates),
    areaDefaults: parseMetricGates(rootPolicy?.areaDefaults),
    areaGatesByAreaId,
    defaultsByCriticality: {
      critical: {
        minCaseScore: optionalNumber(defaultsByCriticality?.critical?.min_case_score),
        minToolPrecision: optionalNumber(defaultsByCriticality?.critical?.min_tool_precision),
        minToolArgumentScore: optionalNumber(defaultsByCriticality?.critical?.min_tool_argument_score),
        maxExtraToolCalls: optionalNonNegativeInt(defaultsByCriticality?.critical?.max_extra_tool_calls)
      },
      standard: {
        minCaseScore: optionalNumber(defaultsByCriticality?.standard?.min_case_score),
        minToolPrecision: optionalNumber(defaultsByCriticality?.standard?.min_tool_precision),
        minToolArgumentScore: optionalNumber(defaultsByCriticality?.standard?.min_tool_argument_score),
        maxExtraToolCalls: optionalNonNegativeInt(defaultsByCriticality?.standard?.max_extra_tool_calls)
      }
    }
  };
}

function resolveCaseGates(caseDef, matrixPolicy) {
  const gatesBlock = caseDef?.gates && typeof caseDef.gates === 'object' ? caseDef.gates : {};
  const criticality = normalizeCriticality(caseDef?.criticality || gatesBlock?.criticality);
  const defaults = matrixPolicy?.defaultsByCriticality?.[criticality] || {};

  return {
    criticality,
    minCaseScore: optionalNumber(caseDef?.min_case_score ?? gatesBlock?.min_case_score ?? defaults.minCaseScore),
    minToolPrecision: optionalNumber(
      caseDef?.min_tool_precision ?? gatesBlock?.min_tool_precision ?? defaults.minToolPrecision
    ),
    minToolArgumentScore: optionalNumber(
      caseDef?.min_tool_argument_score ?? gatesBlock?.min_tool_argument_score ?? defaults.minToolArgumentScore
    ),
    maxExtraToolCalls: optionalNonNegativeInt(
      caseDef?.max_extra_tool_calls ?? gatesBlock?.max_extra_tool_calls ?? defaults.maxExtraToolCalls
    )
  };
}

function buildCaseConstraintMessage(caseDef) {
  const requiredAll = Array.isArray(caseDef?.required_tools_all) ? caseDef.required_tools_all : [];
  const requiredAny = Array.isArray(caseDef?.required_tools_any) ? caseDef.required_tools_any : [];
  const forbidden = Array.isArray(caseDef?.forbidden_tools) ? caseDef.forbidden_tools : [];
  const evalPrompt = String(caseDef?.eval_prompt || '').trim();
  const objectiveText = [
    String(caseDef?.user_prompt || ''),
    String(caseDef?.eval_prompt || '')
  ]
    .join(' ')
    .trim();
  const lines = [
    'Case-specific deterministic routing constraints.',
    'Missing required tools causes evaluation failure.'
  ];
  if (requiredAll.length) {
    lines.push(`Required ALL tools: ${requiredAll.join(', ')}`);
  }
  if (requiredAny.length) {
    lines.push(`Required ANY tools: ${requiredAny.join(', ')}`);
  }
  if (forbidden.length) {
    lines.push(`Forbidden tools: ${forbidden.join(', ')}`);
  }
  lines.push(
    'Call the minimum number of tools needed for correctness; avoid extra list/inspection steps after required routing is satisfied unless needed for disambiguation.'
  );
  if (evalPrompt) {
    lines.push(`Additional case instruction: ${evalPrompt}`);
  }
  const expectedToolArguments = Array.isArray(caseDef?.expected_tool_arguments)
    ? caseDef.expected_tool_arguments.filter(row => row && typeof row === 'object' && !Array.isArray(row))
    : [];
  for (const row of expectedToolArguments) {
    const toolName = String(row.tool || '').trim();
    const toolsAny = (Array.isArray(row.tools_any) ? row.tools_any : [])
      .map(value => String(value || '').trim())
      .filter(Boolean);
    const toolLabel = toolName || (toolsAny.length ? `any of {${toolsAny.join(', ')}}` : '');
    if (!toolLabel) continue;
    const requiredAll = (Array.isArray(row.required_keys_all) ? row.required_keys_all : [])
      .map(value => String(value || '').trim())
      .filter(Boolean);
    const requiredAny = (Array.isArray(row.required_keys_any) ? row.required_keys_any : [])
      .map(value => String(value || '').trim())
      .filter(Boolean);
    const forbiddenKeys = (Array.isArray(row.forbidden_keys) ? row.forbidden_keys : [])
      .map(value => String(value || '').trim())
      .filter(Boolean);
    if (requiredAll.length) {
      lines.push(`Argument rule for ${toolLabel}: include ALL of these keys: ${requiredAll.join(', ')}.`);
    }
    if (requiredAny.length) {
      lines.push(`Argument rule for ${toolLabel}: include at least one of these keys: ${requiredAny.join(', ')}.`);
    }
    if (forbiddenKeys.length) {
      lines.push(`Argument rule for ${toolLabel}: never send these keys: ${forbiddenKeys.join(', ')}.`);
    }
    if (toolName === 'waitForQMapDataset' && (requiredAll.length || requiredAny.length)) {
      lines.push(
        'For waitForQMapDataset never call the tool with empty `{}` arguments; pass a concrete datasetName, datasetRef, or datasetId.'
      );
    }
    if (toolName === 'loadCloudMapAndWait' && requiredAll.includes('mapId')) {
      lines.push(
        'For loadCloudMapAndWait never call the tool with empty `{}` arguments; pass a concrete mapId returned by listQMapCloudMaps.'
      );
    }
  }
  if (
    requiredAny.some(tool => ['loadData', 'saveDataToMap'].includes(String(tool))) ||
    /\b(load|save|bridge|caricamento|salvataggio)\b/i.test(objectiveText)
  ) {
    lines.push(
      'Bridge rule: call loadData or saveDataToMap explicitly; do not substitute with only filter/materialization tools.'
    );
    lines.push(
      'Bridge precision rule: after the explicit bridge step, limit follow-up to validation tools (listQMapDatasets, waitForQMapDataset, countQMapRows) and avoid ranking/chart/filter tools unless explicitly requested.'
    );
  }
  return lines.join(' ');
}

function shouldApplyDeterministicConstraints(caseDef, opts) {
  if (opts?.disableDeterministicConstraints) return false;
  if (caseDef?.deterministic_constraints === false) return false;
  return true;
}

function resolveCaseUserPrompt(caseDef) {
  return String(caseDef?.user_prompt || '').trim();
}

function buildTransportSkippedOutcome(caseDef, matrixPolicy, reason, deterministicConstraintsApplied) {
  const casePrompt = resolveCaseUserPrompt(caseDef);
  const caseGates = resolveCaseGates(caseDef, matrixPolicy);
  return {
    id: caseDef.id,
    area: caseDef.area,
    kpiId: caseDef.kpi_id,
    criticality: caseGates.criticality,
    deterministicConstraintsApplied: Boolean(deterministicConstraintsApplied),
    prompt: casePrompt,
    status: 0,
    ok: false,
    durationMs: 0,
    toolCalls: [],
    toolCallDetails: [],
    extraToolCalls: [],
    extraToolCallCount: 0,
    content: `[transport_error] ${reason}`,
    requiredToolsAll: (caseDef.required_tools_all || []).map(v => String(v).toLowerCase()),
    requiredToolsAny: (caseDef.required_tools_any || []).map(v => String(v).toLowerCase()),
    matchedRequired: [],
    matchedRequiredAll: [],
    matchedRequiredAny: [],
    matchedExpectedAny: [],
    matchedForbidden: [],
    matchedKeywords: [],
    metrics: {toolRecall: 0, toolPrecision: 0, toolArgumentScore: null, keywordScore: 0, caseScore: 0},
    gates: {
      configured: {
        minCaseScore: caseGates.minCaseScore,
        minToolPrecision: caseGates.minToolPrecision,
        minToolArgumentScore: caseGates.minToolArgumentScore,
        maxExtraToolCalls: caseGates.maxExtraToolCalls
      },
      failed: ['transport-error-abort']
    },
    toolArgumentChecks: {evaluated: false, matched: [], failed: []},
    transportError: String(reason || 'transport error').trim(),
    pass: false
  };
}

function toolCatalog(repoRoot) {
  const manifest = loadQMapToolManifest(repoRoot);
  const contracts = loadQMapToolContracts(repoRoot);
  return buildEvalToolCatalogFromManifest(manifest, contracts);
}

async function preflightBaseUrlTransport({baseUrl, preflightTimeoutMs, retries, retryDelayMs, authHeaders}) {
  const url = `${String(baseUrl).replace(/\/+$/, '')}/health`;
  const timeoutMs = Math.max(1000, optionalNonNegativeInt(preflightTimeoutMs) || 5000);
  const maxRetries = Math.max(1, optionalNonNegativeInt(retries) || 6);
  const waitMs = Math.max(100, optionalNonNegativeInt(retryDelayMs) || 1500);
  let lastError = 'fetch failed';
  let lastStatus = 0;
  let authFailure = false;
  for (let attempt = 1; attempt <= maxRetries; attempt += 1) {
    const controller = new AbortController();
    const timeoutHandle = setTimeout(() => {
      controller.abort(new Error(`request-timeout-${timeoutMs}ms`));
    }, timeoutMs);
    try {
      const res = await fetch(url, {
        method: 'GET',
        headers: {
          accept: 'application/json',
          ...authHeaders
        },
        signal: controller.signal
      });
      if (res.ok) return {ok: true, attempts: attempt};
      lastStatus = Number(res.status || 0);
      authFailure = res.status === 401 || res.status === 403;
      if (authFailure) {
        lastError = `status=${lastStatus} unauthorized`;
      } else {
        lastError = `status=${lastStatus}`;
      }
    } catch (error) {
      const message = String(error?.message || error || 'fetch failed');
      lastError = message;
      if (/operation not permitted|connect eperm|eperm/i.test(message)) {
        return {ok: false, attempts: attempt, fetchError: message};
      }
    } finally {
      clearTimeout(timeoutHandle);
    }
    if (attempt < maxRetries) await sleep(waitMs);
  }
  return {
    ok: false,
    attempts: maxRetries,
    fetchError: lastError,
    authFailure,
    ...(lastStatus > 0 ? {status: lastStatus} : {})
  };
}

async function callChatCompletions({
  baseUrl,
  model,
  messages,
  tools,
  temperature,
  requestTimeoutMs,
  authHeaders
}) {
  const url = `${String(baseUrl).replace(/\/+$/, '')}/chat/completions`;
  const body = {
    model,
    temperature,
    stream: false,
    messages,
    tools,
    tool_choice: 'auto'
  };

  const timeoutMs = Math.max(1000, optionalNonNegativeInt(requestTimeoutMs) || 120000);
  const controller = new AbortController();
  const timeoutHandle = setTimeout(() => {
    controller.abort(new Error(`request-timeout-${timeoutMs}ms`));
  }, timeoutMs);
  const start = Date.now();
  try {
    const res = await fetch(url, {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        ...authHeaders,
        'x-qmap-context': JSON.stringify({
          sessionId: `ai-eval-${Date.now()}`,
          source: 'qmap-ai-eval-suite'
        })
      },
      body: JSON.stringify(body),
      signal: controller.signal
    });
    const ms = Date.now() - start;
    const text = await res.text();
    let payload;
    try {
      payload = JSON.parse(text);
    } catch {
      payload = {raw: text};
    }
    return {
      ok: res.ok,
      status: res.status,
      durationMs: ms,
      requestId: String(res.headers.get('x-q-assistant-request-id') || '').trim(),
      payload,
      evalDiagnostics: extractResponseEvalDiagnostics(payload)
    };
  } catch (error) {
    const ms = Date.now() - start;
    const message = String(error?.message || error || 'fetch failed');
    return {
      ok: false,
      status: message.includes('request-timeout-') ? 408 : 0,
      durationMs: ms,
      requestId: '',
      payload: {error: message},
      fetchError: message,
      evalDiagnostics: null
    };
  } finally {
    clearTimeout(timeoutHandle);
  }
}

function shouldRetryChatCompletion(response) {
  if (String(response?.fetchError || '').trim()) return true;
  const status = Number(response?.status || 0);
  if (status === 400) return true;
  if (status === 408 || status === 409 || status === 425 || status === 429) return true;
  if (status >= 500) return true;
  return false;
}

async function callChatCompletionsWithRetry({
  baseUrl,
  model,
  messages,
  tools,
  temperature,
  requestTimeoutMs,
  authHeaders,
  requestRetries,
  requestRetryDelayMs
}) {
  const maxAttempts = Math.max(1, optionalNonNegativeInt(requestRetries) || 1);
  const delayMs = Math.max(100, optionalNonNegativeInt(requestRetryDelayMs) || 350);
  let lastResponse = null;
  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    const response = await callChatCompletions({
      baseUrl,
      model,
      messages,
      tools,
      temperature,
      requestTimeoutMs,
      authHeaders
    });
    lastResponse = response;
    if (response?.ok) return response;
    if (attempt >= maxAttempts) return response;
    if (!shouldRetryChatCompletion(response)) return response;
    await sleep(delayMs * attempt);
  }
  return (
    lastResponse || {
      ok: false,
      status: 0,
      durationMs: 0,
      requestId: '',
      payload: {},
      fetchError: 'no response'
    }
  );
}

function parseToolArguments(raw) {
  if (!raw || typeof raw !== 'string') return {};
  try {
    const parsed = JSON.parse(raw);
    return parsed && typeof parsed === 'object' ? parsed : {};
  } catch {
    return {};
  }
}

function truncateText(text, maxChars = 220) {
  const raw = String(text || '').trim();
  if (!raw) return '';
  return raw.length <= maxChars ? raw : `${raw.slice(0, maxChars)}...`;
}

function summarizeToolResultForFinalize(content) {
  const raw = String(content || '').trim();
  if (!raw) return 'no result payload';
  try {
    const parsed = JSON.parse(raw);
    if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) {
      const parts = [];
      if (typeof parsed.success === 'boolean') parts.push(`success=${parsed.success}`);
      if (parsed.loadedDatasetName) parts.push(`dataset=${parsed.loadedDatasetName}`);
      if (parsed.loadedDatasetRef) parts.push(`datasetRef=${parsed.loadedDatasetRef}`);
      if (Number.isFinite(Number(parsed.returned))) parts.push(`returned=${Number(parsed.returned)}`);
      if (Number.isFinite(Number(parsed.totalMatched))) {
        parts.push(`totalMatched=${Number(parsed.totalMatched)}`);
      }
      if (typeof parsed.details === 'string' && parsed.details.trim()) {
        parts.push(`details=${truncateText(parsed.details, 140)}`);
      }
      if (parts.length) return parts.join(', ');
    }
  } catch {
    // Fall back to a compact text preview when the payload is not JSON.
  }
  return truncateText(raw, 180);
}

function buildFinalizeMessages(initialMessages, convo) {
  const originalUserPrompt =
    [...(Array.isArray(initialMessages) ? initialMessages : [])]
      .reverse()
      .find(message => String(message?.role || '') === 'user')?.content || '';
  const compactTraceMessages = (Array.isArray(convo) ? convo : [])
    .filter(message => {
      const role = String(message?.role || '').trim().toLowerCase();
      return (
        (role === 'assistant' && Array.isArray(message?.tool_calls) && message.tool_calls.length > 0) ||
        role === 'tool'
      );
    })
    .slice(-12)
    .map(message => {
      const role = String(message?.role || '').trim().toLowerCase();
      if (role === 'assistant') {
        return {
          role: 'assistant',
          content: typeof message?.content === 'string' ? message.content : '',
          tool_calls: Array.isArray(message?.tool_calls) ? message.tool_calls : []
        };
      }
      return {
        role: 'tool',
        tool_call_id: String(message?.tool_call_id || '').trim(),
        content: typeof message?.content === 'string' ? truncateText(message.content, 260) : ''
      };
    });

  return [
    {
      role: 'system',
      content:
        'You are a q-map assistant under evaluation. Tool execution is complete. Do not call tools. ' +
        'Return one concise plain-text final answer only, using the validated tool outcomes below.'
    },
    {
      role: 'user',
      content: `Original user request:\n${String(originalUserPrompt || '').trim()}`
    },
    ...compactTraceMessages,
    {
      role: 'user',
      content:
        'Tool execution is complete. Provide one concise final answer in plain text only. ' +
        'If the workflow is complete, state the result concisely. If it is not complete, state the limitation clearly. ' +
        'Do not call tools and do not add extra analysis steps.'
    }
  ];
}

function normalizeToolCallDetails(toolCalls) {
  if (!Array.isArray(toolCalls)) return [];
  return toolCalls
    .map(call => {
      const toolName = String(call?.function?.name || '').trim();
      if (!toolName) return null;
      const rawArguments = String(call?.function?.arguments || '').trim();
      return {
        name: toolName,
        rawArguments,
        args: parseToolArguments(rawArguments)
      };
    })
    .filter(Boolean);
}

function matchesToolArgumentExpectation(expectation, callDetails) {
  const args = callDetails?.args && typeof callDetails.args === 'object' && !Array.isArray(callDetails.args)
    ? callDetails.args
    : {};
  const keys = new Set(Object.keys(args));
  const requiredAll = (Array.isArray(expectation?.required_keys_all) ? expectation.required_keys_all : [])
    .map(value => String(value || '').trim())
    .filter(Boolean);
  const requiredAny = (Array.isArray(expectation?.required_keys_any) ? expectation.required_keys_any : [])
    .map(value => String(value || '').trim())
    .filter(Boolean);
  const forbiddenKeys = (Array.isArray(expectation?.forbidden_keys) ? expectation.forbidden_keys : [])
    .map(value => String(value || '').trim())
    .filter(Boolean);
  const requiredKeyValues =
    expectation?.required_key_values && typeof expectation.required_key_values === 'object' && !Array.isArray(expectation.required_key_values)
      ? expectation.required_key_values
      : {};
  if (requiredAll.length && requiredAll.some(key => !keys.has(key))) return false;
  if (requiredAny.length && !requiredAny.some(key => keys.has(key))) return false;
  if (forbiddenKeys.some(key => keys.has(key))) return false;
  for (const [key, expectedValue] of Object.entries(requiredKeyValues)) {
    if (!keys.has(key)) return false;
    if (JSON.stringify(args[key]) !== JSON.stringify(expectedValue)) return false;
  }
  return true;
}

function evaluateToolArgumentExpectations(caseDef, toolCallDetails) {
  const expectations = Array.isArray(caseDef?.expected_tool_arguments)
    ? caseDef.expected_tool_arguments
        .filter(row => row && typeof row === 'object' && !Array.isArray(row))
        .map(row => ({
          tool: String(row.tool || '').trim(),
          tools_any: Array.isArray(row.tools_any) ? row.tools_any : [],
          required_keys_all: Array.isArray(row.required_keys_all) ? row.required_keys_all : [],
          required_keys_any: Array.isArray(row.required_keys_any) ? row.required_keys_any : [],
          forbidden_keys: Array.isArray(row.forbidden_keys) ? row.forbidden_keys : [],
          required_key_values:
            row.required_key_values && typeof row.required_key_values === 'object' && !Array.isArray(row.required_key_values)
              ? row.required_key_values
              : {}
        }))
        .map(row => ({
          ...row,
          tools_any: row.tools_any.map(value => String(value || '').trim()).filter(Boolean)
        }))
        .filter(row => row.tool || row.tools_any.length)
    : [];

  if (!expectations.length) {
    return {evaluated: false, score: null, matched: [], failed: []};
  }

  const matched = [];
  const failed = [];
  for (const expectation of expectations) {
    const toolNames = expectation.tool ? [expectation.tool] : expectation.tools_any;
    const normalizedNames = new Set(
      toolNames.map(name => String(name || '').trim().toLowerCase()).filter(Boolean)
    );
    const candidates = toolCallDetails.filter(
      call => normalizedNames.has(String(call?.name || '').trim().toLowerCase())
    );
    const ok = candidates.some(call => matchesToolArgumentExpectation(expectation, call));
    const summary = {
      tool: expectation.tool,
      toolsAny: expectation.tools_any,
      requiredKeysAll: expectation.required_keys_all,
      requiredKeysAny: expectation.required_keys_any,
      forbiddenKeys: expectation.forbidden_keys,
      requiredKeyValues: expectation.required_key_values
    };
    if (ok) matched.push(summary);
    else failed.push(summary);
  }

  return {
    evaluated: true,
    score: round(matched.length / expectations.length),
    matched,
    failed
  };
}

function resolveMockToolResultOverride(caseDef, toolName) {
  if (!caseDef || typeof caseDef !== 'object') return null;
  const overrides = caseDef.mock_tool_results;
  if (!overrides || typeof overrides !== 'object' || Array.isArray(overrides)) return null;
  const normalizedToolName = String(toolName || '').trim().toLowerCase();
  for (const [name, value] of Object.entries(overrides)) {
    if (String(name || '').trim().toLowerCase() !== normalizedToolName) continue;
    if (value && typeof value === 'object' && !Array.isArray(value)) {
      return value;
    }
  }
  return null;
}

function getMockToolResponseContract(toolName) {
  const normalizedToolName = String(toolName || '').trim();
  if (!normalizedToolName) return null;
  return (
    EVAL_TOOL_CONTRACTS?.tools?.[normalizedToolName]?.responseContract ||
    EVAL_TOOL_CONTRACTS?.defaults?.responseContract ||
    null
  );
}

function normalizeMockFieldToken(value, fallback = 'metric') {
  const normalized = String(value || '')
    .trim()
    .replace(/[^a-zA-Z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '')
    .toLowerCase();
  return normalized || fallback;
}

function buildMockMetricMetadata(toolName, args, result, datasetName) {
  const normalizedToolName = String(toolName || '').trim();
  const baseDatasetName =
    String(
      result?.dataset ||
        result?.datasetName ||
        result?.loadedDatasetName ||
        datasetName ||
        'EvalDataset'
    ).trim() || 'EvalDataset';

  switch (normalizedToolName) {
    case 'createDatasetWithGeometryArea': {
      const outputFieldName =
        String(args?.outputFieldName || args?.areaFieldName || result?.outputFieldName || 'area_m2').trim() ||
        'area_m2';
      return {
        dataset: result?.dataset || baseDatasetName,
        outputFieldName,
        fieldCatalog: ['name', 'population', 'flat_metric', '_geojson', outputFieldName],
        numericFields: [outputFieldName],
        styleableFields: [outputFieldName],
        defaultStyleField: outputFieldName
      };
    }
    case 'createDatasetWithNormalizedField': {
      const sourceField =
        args?.fieldToNormalize || args?.numeratorFieldName || args?.sourceFieldName || 'population';
      const outputFieldName =
        String(result?.outputFieldName || args?.outputFieldName || `${normalizeMockFieldToken(sourceField)}_per_100k`).trim() ||
        'population_per_100k';
      return {
        dataset: result?.dataset || baseDatasetName,
        outputFieldName,
        fieldCatalog: ['name', 'population', 'flat_metric', '_geojson', outputFieldName],
        numericFields: [outputFieldName],
        styleableFields: [outputFieldName],
        defaultStyleField: outputFieldName
      };
    }
    case 'nearestFeatureJoin': {
      const defaultStyleField =
        String(result?.defaultStyleField || args?.distanceFieldName || 'nearest_distance_km').trim() ||
        'nearest_distance_km';
      return {
        dataset: result?.dataset || baseDatasetName,
        fieldCatalog: ['name', 'population', '_geojson', 'nearest_count', defaultStyleField],
        numericFields: ['nearest_count', defaultStyleField],
        styleableFields: ['nearest_count', defaultStyleField],
        defaultStyleField
      };
    }
    case 'zonalStatsByAdmin': {
      const outputFieldName =
        String(result?.outputFieldName || args?.outputFieldName || 'zonal_value').trim() || 'zonal_value';
      return {
        dataset: result?.dataset || baseDatasetName,
        outputFieldName,
        fieldCatalog: ['name', 'population', '_geojson', outputFieldName],
        numericFields: [outputFieldName],
        styleableFields: [outputFieldName],
        defaultStyleField: outputFieldName,
        aggregationOutputs: {sum: outputFieldName}
      };
    }
    case 'bufferAndSummarize': {
      const outputFieldName =
        String(result?.outputFieldName || args?.outputFieldName || 'buffer_metric').trim() || 'buffer_metric';
      return {
        dataset: result?.dataset || baseDatasetName,
        outputFieldName,
        fieldCatalog: ['name', '_geojson', outputFieldName],
        numericFields: [outputFieldName],
        styleableFields: [outputFieldName],
        defaultStyleField: outputFieldName,
        aggregationOutputs: {sum: outputFieldName}
      };
    }
    case 'joinQMapDatasetsOnH3': {
      const outputFieldName =
        String(result?.outputFieldName || args?.targetValueFieldName || args?.targetValueFieldBase || 'population_2').trim() ||
        'population_2';
      return {
        dataset: result?.dataset || baseDatasetName,
        fieldCatalog: ['h3_id', 'count', outputFieldName],
        numericFields: ['count', outputFieldName],
        styleableFields: ['count', outputFieldName],
        defaultStyleField: outputFieldName
      };
    }
    case 'aggregateDatasetToH3': {
      return {
        dataset: result?.dataset || baseDatasetName,
        fieldCatalog: ['h3_id', 'h3_resolution', 'count', 'count_weighted', 'sum'],
        numericFields: ['count', 'count_weighted', 'sum'],
        styleableFields: ['count', 'count_weighted', 'sum'],
        defaultStyleField: 'count_weighted',
        aggregationOutputs: {
          count: 'count',
          count_weighted: 'count_weighted',
          sum: 'sum'
        }
      };
    }
    case 'populateTassellationFromAdminUnits':
    case 'populateTassellationFromAdminUnitsAreaWeighted':
    case 'populateTassellationFromAdminUnitsDiscrete': {
      const outputFieldName =
        String(result?.outputFieldName || args?.targetValueFieldName || args?.adminField || args?.sourceValueField || 'population').trim() ||
        'population';
      return {
        dataset: result?.dataset || baseDatasetName,
        outputFieldName,
        fieldCatalog: ['h3_id', 'h3_resolution', outputFieldName],
        numericFields: [outputFieldName],
        styleableFields: [outputFieldName],
        defaultStyleField: outputFieldName,
        aggregationOutputs: {sum: outputFieldName}
      };
    }
    case 'clipQMapDatasetByGeometry':
    case 'clipDatasetByBoundary': {
      return {
        dataset: result?.dataset || baseDatasetName,
        fieldCatalog: ['name', '_geojson', 'qmap_clip_match_count', 'qmap_clip_intersection_area_m2', 'qmap_clip_intersection_pct'],
        numericFields: ['qmap_clip_match_count', 'qmap_clip_intersection_area_m2', 'qmap_clip_intersection_pct'],
        styleableFields: ['qmap_clip_match_count', 'qmap_clip_intersection_area_m2', 'qmap_clip_intersection_pct'],
        defaultStyleField: 'qmap_clip_intersection_pct'
      };
    }
    case 'spatialJoinByPredicate': {
      return {
        dataset: result?.dataset || baseDatasetName,
        fieldCatalog: ['name', '_geojson', 'join_count', 'join_sum'],
        numericFields: ['join_count', 'join_sum'],
        styleableFields: ['join_count', 'join_sum'],
        defaultStyleField: 'join_sum',
        aggregationOutputs: {count: 'join_count', sum: 'join_sum'}
      };
    }
    default:
      return {
        dataset: result?.dataset || baseDatasetName
      };
  }
}

function alignMockToolResultToResponseContract(toolName, args, result, datasetName) {
  const responseContract = getMockToolResponseContract(toolName);
  if (!responseContract || !result || typeof result !== 'object' || Array.isArray(result)) {
    return result;
  }

  const required = new Set(
    Array.isArray(responseContract.required)
      ? responseContract.required.map(value => String(value || '').trim()).filter(Boolean)
      : []
  );
  if (!required.size) return result;

  const metadata = buildMockMetricMetadata(toolName, args, result, datasetName);
  const aligned = {...result};

  if (required.has('dataset') && !String(aligned.dataset || '').trim()) {
    aligned.dataset = metadata.dataset;
  }
  if (required.has('outputFieldName') && !String(aligned.outputFieldName || '').trim()) {
    aligned.outputFieldName = metadata.outputFieldName;
  }
  if (required.has('fieldCatalog') && !Array.isArray(aligned.fieldCatalog)) {
    aligned.fieldCatalog = metadata.fieldCatalog || [];
  }
  if (required.has('numericFields') && !Array.isArray(aligned.numericFields)) {
    aligned.numericFields = metadata.numericFields || [];
  }
  if (required.has('styleableFields') && !Array.isArray(aligned.styleableFields)) {
    aligned.styleableFields = metadata.styleableFields || [];
  }
  if (required.has('defaultStyleField') && !String(aligned.defaultStyleField || '').trim()) {
    aligned.defaultStyleField = metadata.defaultStyleField || metadata.outputFieldName || '';
  }
  if (required.has('aggregationOutputs')) {
    const hasAggregationOutputs =
      aligned.aggregationOutputs && typeof aligned.aggregationOutputs === 'object' && !Array.isArray(aligned.aggregationOutputs);
    if (!hasAggregationOutputs) {
      aligned.aggregationOutputs = metadata.aggregationOutputs || {};
    }
  }

  return aligned;
}

function mockToolResultForEval(toolName, args, caseDef) {
  const argsValidation = validateMockToolArgs(toolName, args);
  if (!argsValidation.ok) {
    return argsValidation.errorResult;
  }
  const datasetName = String(
    args?.newDatasetName ||
      args?.datasetName ||
      args?.sourceDatasetName ||
      args?.targetDatasetName ||
      args?.adminDatasetName ||
      args?.valueDatasetName ||
      'EvalDataset'
  );
  const base = {
    success: true,
    details: `Mock eval result for ${toolName}.`
  };

  let result = base;
  switch (String(toolName || '')) {
    case 'listQMapDatasets':
      result = {
        ...base,
        datasets: [
          {
            id: 'admin-boundaries',
            name: 'Admin Boundaries',
            datasetRef: 'id:admin-boundaries',
            fields: ['name', 'population', 'flat_metric', '_geojson']
          },
          {
            id: 'thematic-events',
            name: 'Thematic Events',
            datasetRef: 'id:thematic-events',
            fields: ['value', 'category', '_geojson']
          }
        ],
        layers: [
          {
            id: 'layer-admin-boundaries',
            name: 'Admin Boundaries',
            datasetId: 'admin-boundaries',
            datasetName: 'Admin Boundaries',
            datasetRef: 'id:admin-boundaries',
            type: 'geojson',
            activeFields: ['population'],
            tooltipFields: ['name', 'population'],
            availableFields: ['name', 'population', 'flat_metric', '_geojson']
          },
          {
            id: 'layer-thematic-events',
            name: 'Thematic Events',
            datasetId: 'thematic-events',
            datasetName: 'Thematic Events',
            datasetRef: 'id:thematic-events',
            type: 'point',
            activeFields: ['value', 'category'],
            tooltipFields: ['value', 'category'],
            availableFields: ['value', 'category', '_geojson']
          }
        ],
        details: 'Found 2 datasets in current map.'
      };
      break;
    case 'listQCumberProviders':
      result = {
        ...base,
        providers: [
          {id: 'local-assets-it', name: 'Local Assets IT'},
          {id: 'demo-global', name: 'Demo Global Assets'}
        ]
      };
      break;
    case 'listQCumberDatasets': {
      const providerId = String(args?.providerId || '').trim();
      if (providerId === 'local-assets-it') {
        result = {
          ...base,
          datasets: [
            {id: 'kontur-boundaries-italia', name: 'Kontur Boundaries Italia'},
            {id: 'clc-2018-italia', name: 'CLC 2018 Italia'}
          ]
        };
        break;
      }
      if (providerId === 'demo-global') {
        result = {
          ...base,
          datasets: [
            {id: 'admin-boundaries-global', name: 'Admin Boundaries Global'},
            {id: 'land-cover-global', name: 'Land Cover Global'}
          ]
        };
        break;
      }
      result = {
        ...base,
        datasets: [
          {id: 'admin-boundaries-global', name: 'Admin Boundaries Global'},
          {id: 'land-cover-global', name: 'Land Cover Global'},
          {id: 'kontur-boundaries-italia', name: 'Kontur Boundaries Italia'},
          {id: 'clc-2018-italia', name: 'CLC 2018 Italia'}
        ]
      };
      break;
    }
    case 'getQCumberDatasetHelp': {
      const datasetId = String(args?.datasetId || '');
      const isAdmin = /kontur|admin[-_]?boundar|territorial/i.test(datasetId);
      result = {
        ...base,
        datasetId,
        routing: {
          queryToolHint: {
            preferredTool: isAdmin ? 'queryQCumberTerritorialUnits' : 'queryQCumberDatasetSpatial'
          }
        }
      };
      break;
    }
    case 'queryQCumberTerritorialUnits':
    case 'queryQCumberDataset':
    case 'queryQCumberDatasetSpatial':
      result = {
        ...base,
        loadedDatasetName: `${datasetName} [eval]`,
        loadedDatasetRef: `id:${datasetName.toLowerCase().replace(/[^a-z0-9]+/g, '-')}-eval`,
        returned: 100,
        totalMatched: 100
      };
      break;
    case 'listQMapCloudMaps':
      result = {
        ...base,
        maps: [
          {id: 'cloud-timeout-map', name: 'Cloud Timeout Map'},
          {id: 'cloud-demo-map', name: 'Cloud Demo Map'}
        ],
        details: 'Found 2 cloud maps.'
      };
      break;
    case 'loadCloudMapAndWait':
    case 'loadQMapCloudMap':
      result = {
        ...base,
        loadedDatasetName: `${datasetName} [cloud-eval]`,
        loadedDatasetRef: `id:${datasetName.toLowerCase().replace(/[^a-z0-9]+/g, '-')}-cloud-eval`,
        datasetName: `${datasetName} [cloud-eval]`,
        datasetRef: `id:${datasetName.toLowerCase().replace(/[^a-z0-9]+/g, '-')}-cloud-eval`,
        returned: 100
      };
      break;
    case 'loadData':
      result = {
        ...base,
        loadedDatasetName: `${datasetName} [loaded-eval]`,
        loadedDatasetRef: `id:${datasetName.toLowerCase().replace(/[^a-z0-9]+/g, '-')}-loaded-eval`,
        returned: 100
      };
      break;
    case 'saveDataToMap':
      result = {
        ...base,
        savedDatasetNames: [datasetName],
        savedCount: 1
      };
      break;
    case 'waitForQMapDataset':
      result = {
        ...base,
        dataset: datasetName,
        rowCount: 100
      };
      break;
    case 'countQMapRows':
      result = {
        ...base,
        dataset: datasetName,
        rowCount: 100
      };
      break;
    case 'clipQMapDatasetByGeometry':
    case 'clipDatasetByBoundary':
      result = {
        ...base,
        dataset: `${datasetName}_clip_eval`,
        rowCount: 80
      };
      break;
    case 'aggregateDatasetToH3':
      result = {
        ...base,
        dataset: `${datasetName}_h3_eval`,
        rowCount: 500
      };
      break;
    case 'joinQMapDatasetsOnH3':
      result = {
        ...base,
        dataset: `${datasetName}_join_eval`,
        rowCount: 420,
        coverage: 0.87
      };
      break;
    case 'coverageQualityReport':
      result = {
        ...base,
        coverage: 0.87,
        matched: 420,
        unmatched: 63,
        nullRate: 0.03
      };
      break;
    case 'zonalStatsByAdmin':
      result = {
        ...base,
        dataset: `${datasetName}_zonal_eval`,
        rowCount: 110
      };
      break;
    case 'populateTassellationFromAdminUnits':
    case 'populateTassellationFromAdminUnitsAreaWeighted':
    case 'populateTassellationFromAdminUnitsDiscrete':
      result = {
        ...base,
        dataset: `${datasetName}_populated_eval`,
        rowCount: 415
      };
      break;
    case 'rankQMapDatasetRows':
      result = {
        ...base,
        dataset: datasetName,
        rows: [{rank: 1, name: 'mock-top', metric: 123}],
        returned: 1
      };
      break;
    case 'deriveQMapDatasetBbox':
      result = {
        ...base,
        spatialBbox: [6.5, 36.6, 18.8, 47.1]
      };
      break;
    default:
      break;
  }
  const override = resolveMockToolResultOverride(caseDef, toolName);
  const mergedResult = override ? {...result, ...override} : result;
  return alignMockToolResultToResponseContract(toolName, args, mergedResult, datasetName);
}

async function runCaseConversation({
  baseUrl,
  model,
  messages,
  tools,
  temperature,
  maxTurns,
  requestTimeoutMs,
  requestRetries,
  requestRetryDelayMs,
  authHeaders,
  caseDef
}) {
  const convo = [...messages];
  const collectedToolCalls = [];
  const collectedToolResults = [];
  let finalContent = '';
  let totalDurationMs = 0;
  let lastStatus = 0;
  let ok = true;
  let transportError = '';
  const requestIds = [];
  const requestStatuses = [];
  const diagnosticsAggregate = createEvalDiagnosticsAggregate();

  for (let turn = 0; turn < Math.max(1, Number(maxTurns || 1)); turn += 1) {
    const response = await callChatCompletionsWithRetry({
      baseUrl,
      model,
      messages: convo,
      tools,
      temperature,
      requestTimeoutMs,
      authHeaders,
      requestRetries,
      requestRetryDelayMs
    });
    totalDurationMs += Number(response.durationMs || 0);
    lastStatus = Number(response.status || 0);
    requestStatuses.push(lastStatus);
    ok = ok && Boolean(response.ok);
    if (response.requestId) requestIds.push(response.requestId);
    mergeEvalDiagnosticsAggregate(diagnosticsAggregate, response.evalDiagnostics);
    if (response.fetchError) {
      transportError = String(response.fetchError || '').trim();
      finalContent = `[transport_error] ${transportError}`;
      break;
    }

    const message = response?.payload?.choices?.[0]?.message || {};
    const content = typeof message?.content === 'string' ? message.content : '';
    const toolCalls = Array.isArray(message?.tool_calls) ? message.tool_calls : [];

    const assistantMessage = {
      role: 'assistant',
      content,
      ...(toolCalls.length ? {tool_calls: toolCalls} : {})
    };
    convo.push(assistantMessage);
    finalContent = content || finalContent;

    if (!toolCalls.length) {
      break;
    }

    for (let i = 0; i < toolCalls.length; i += 1) {
      const call = toolCalls[i] || {};
      const toolName = String(call?.function?.name || '').trim();
      if (!toolName) continue;
      collectedToolCalls.push({
        type: 'function',
        function: {
          name: toolName,
          arguments: String(call?.function?.arguments || '')
        }
      });
      const args = parseToolArguments(call?.function?.arguments);
      const toolResult = mockToolResultForEval(toolName, args, caseDef);
      collectedToolResults.push({
        toolName,
        ...(toolResult && typeof toolResult === 'object' ? toolResult : {success: false})
      });
      const toolContent = JSON.stringify(toolResult);
      convo.push({
        role: 'tool',
        tool_call_id: String(call?.id || `eval_tool_${turn}_${i}`),
        content: toolContent
      });
    }
  }

  // Some providers can end a bounded tool loop with only tool_calls and no
  // textual assistant conclusion. Request one final text-only summary so
  // keyword-based scoring can evaluate the actual narrative output.
  if (!finalContent && collectedToolCalls.length) {
    const finalizeMessages = buildFinalizeMessages(messages, convo);
    const finalizeResponse = await callChatCompletionsWithRetry({
      baseUrl,
      model,
      messages: finalizeMessages,
      tools: [],
      temperature,
      requestTimeoutMs,
      authHeaders,
      requestRetries,
      requestRetryDelayMs
    });
    totalDurationMs += Number(finalizeResponse.durationMs || 0);
    lastStatus = Number(finalizeResponse.status || 0);
    requestStatuses.push(lastStatus);
    ok = ok && Boolean(finalizeResponse.ok);
    if (finalizeResponse.requestId) requestIds.push(finalizeResponse.requestId);
    mergeEvalDiagnosticsAggregate(diagnosticsAggregate, finalizeResponse.evalDiagnostics);
    if (finalizeResponse.fetchError) {
      transportError = String(finalizeResponse.fetchError || '').trim();
      finalContent = `[transport_error] ${transportError}`;
    }
    const finalizeMessage = finalizeResponse?.payload?.choices?.[0]?.message || {};
    const finalizeContent =
      typeof finalizeMessage?.content === 'string' ? finalizeMessage.content : '';
    if (finalizeContent && !transportError) {
      finalContent = finalizeContent;
    }
  }

  return {
    ok,
    status: lastStatus,
    requestStatuses,
    durationMs: totalDurationMs,
    requestIds: Array.from(new Set(requestIds.filter(Boolean))),
    transportError,
    evalDiagnostics: finalizeEvalDiagnosticsAggregate(diagnosticsAggregate),
    payload: {
      choices: [
        {
          message: {
            content: finalContent,
            tool_calls: collectedToolCalls,
            tool_results: collectedToolResults
          }
        }
      ]
    }
  };
}

function extractCaseOutcome(caseDef, response, matrixPolicy, deterministicConstraintsApplied) {
  const choice = response?.payload?.choices?.[0] || {};
  const message = choice?.message || {};
  const content = String(message?.content || '');
  const requestStatuses = Array.isArray(response?.requestStatuses)
    ? response.requestStatuses.map(value => Number(value)).filter(Number.isFinite)
    : [];
  const transportErrorText = String(response?.transportError || '').trim();
  const finalStatus = Number(response?.status || 0);
  const finalStatus2xx = finalStatus >= 200 && finalStatus < 300;
  const allRequestStatuses2xx =
    requestStatuses.length > 0 &&
    requestStatuses.every(status => status >= 200 && status < 300);
  const responseSucceeded =
    !transportErrorText &&
    (Boolean(response?.ok) || allRequestStatuses2xx || finalStatus2xx);
  const caseGates = resolveCaseGates(caseDef, matrixPolicy);
  const toolCallDetails = normalizeToolCallDetails(message?.tool_calls);
  const toolCalls = toolCallDetails.map(call => call.name);
  const toolResults = Array.isArray(message?.tool_results) ? message.tool_results : [];

  const observedSet = new Set(toolCalls.map(v => v.toLowerCase()));
  const requiredAll = (caseDef.required_tools_all || []).map(v => String(v).toLowerCase());
  const requiredAny = (caseDef.required_tools_any || []).map(v => String(v).toLowerCase());
  const expectedAny = (caseDef.expected_tools_any || []).map(v => String(v).toLowerCase());
  const forbidden = (caseDef.forbidden_tools || []).map(v => String(v).toLowerCase());
  const keywordsAny = (caseDef.expected_keywords_any || []).map(v => String(v).toLowerCase());

  const matchedRequiredAll = requiredAll.filter(t => observedSet.has(t));
  const matchedRequiredAny = requiredAny.filter(t => observedSet.has(t));
  const matchedExpectedAny = expectedAny.filter(t => observedSet.has(t));
  const matchedForbidden = forbidden.filter(t => observedSet.has(t));
  const allowedTools = new Set([...requiredAll, ...requiredAny, ...expectedAny]);
  const extraToolCalls = [...observedSet].filter(t => !allowedTools.has(t));
  const extraToolCallCount = toolCalls.filter(
    toolName => !allowedTools.has(String(toolName || '').trim().toLowerCase())
  ).length;
  const maxToolCallsByName =
    caseDef?.max_tool_calls_by_name && typeof caseDef.max_tool_calls_by_name === 'object' && !Array.isArray(caseDef.max_tool_calls_by_name)
      ? Object.fromEntries(
          Object.entries(caseDef.max_tool_calls_by_name)
            .map(([toolName, limit]) => [String(toolName || '').trim().toLowerCase(), Number(limit)])
            .filter(([toolName, limit]) => toolName && Number.isInteger(limit) && limit >= 0)
        )
      : {};
  const toolCallCounts = toolCalls.reduce((acc, toolName) => {
    const normalizedToolName = String(toolName || '').trim().toLowerCase();
    if (!normalizedToolName) return acc;
    acc[normalizedToolName] = Number(acc[normalizedToolName] || 0) + 1;
    return acc;
  }, {});
  const toolCallLimitViolations = Object.entries(maxToolCallsByName)
    .filter(([toolName, limit]) => Number(toolCallCounts[toolName] || 0) > Number(limit))
    .map(([toolName, limit]) => ({
      toolName,
      observed: Number(toolCallCounts[toolName] || 0),
      maxAllowed: Number(limit)
    }));

  const normalizedContent = normalizeForMatch(content);
  const contentTokens = new Set(normalizedContent.split(' ').filter(Boolean));
  const contentStems = new Set([...contentTokens].map(stemToken));
  const matchedKeywords = keywordsAny.filter(keyword =>
    keywordMatchesContent(keyword, normalizedContent, contentTokens, contentStems)
  );

  const requiredAllRecall = requiredAll.length ? matchedRequiredAll.length / requiredAll.length : 1;
  const requiredAnyRecall = requiredAny.length ? (matchedRequiredAny.length > 0 ? 1 : 0) : 1;
  const toolRecall = Math.min(requiredAllRecall, requiredAnyRecall);
  const usefulToolCallCount = toolCalls.filter(toolName => {
    const normalized = String(toolName || '').trim().toLowerCase();
    return requiredAll.includes(normalized) || requiredAny.includes(normalized) || expectedAny.includes(normalized);
  }).length;
  const toolPrecision = toolCalls.length
    ? usefulToolCallCount / toolCalls.length
    : 0;
  const keywordDenominator = keywordsAny.length <= 1 ? keywordsAny.length : keywordsAny.length - 1;
  const keywordScore = keywordsAny.length
    ? Math.min(1, matchedKeywords.length / Math.max(1, keywordDenominator))
    : 1;
  const toolArgumentEval = evaluateToolArgumentExpectations(caseDef, toolCallDetails);
  const responseModeEval = evaluateExpectedResponseMode(
    caseDef,
    content,
    toolResults,
    response.evalDiagnostics
  );
  const groundedFinalAnswerEval = evaluateGroundedFinalAnswer(
    caseDef,
    content,
    response.evalDiagnostics,
    toolCalls
  );

  // Prioritize tool-evidence semantics over lexical variance in narrative text.
  const caseScoreBase = toolArgumentEval.evaluated
    ? 0.45 * toolRecall +
      0.30 * toolPrecision +
      0.15 * Number(toolArgumentEval.score || 0) +
      0.10 * keywordScore
    : 0.50 * toolRecall + 0.40 * toolPrecision + 0.10 * keywordScore;
  const caseScore = caseScoreBase - (matchedForbidden.length ? 0.5 : 0);
  const boundedCaseScore = Number(Math.max(0, caseScore).toFixed(3));
  const roundedToolPrecision = round(toolPrecision);
  const gateFailures = [];
  if (
    Number.isFinite(caseGates.minCaseScore) &&
    boundedCaseScore < Number(caseGates.minCaseScore)
  ) {
    gateFailures.push(
      `caseScore=${boundedCaseScore} < minCaseScore=${round(caseGates.minCaseScore)}`
    );
  }
  if (
    Number.isFinite(caseGates.minToolPrecision) &&
    roundedToolPrecision < Number(caseGates.minToolPrecision)
  ) {
    gateFailures.push(
      `toolPrecision=${roundedToolPrecision} < minToolPrecision=${round(caseGates.minToolPrecision)}`
    );
  }
  if (
    toolArgumentEval.evaluated &&
    Number.isFinite(caseGates.minToolArgumentScore) &&
    Number(toolArgumentEval.score || 0) < Number(caseGates.minToolArgumentScore)
  ) {
    gateFailures.push(
      `toolArgumentScore=${round(toolArgumentEval.score)} < minToolArgumentScore=${round(caseGates.minToolArgumentScore)}`
    );
  }
  if (
    Number.isFinite(caseGates.maxExtraToolCalls) &&
    extraToolCallCount > Number(caseGates.maxExtraToolCalls)
  ) {
    gateFailures.push(
      `extraToolCalls=${extraToolCallCount} > maxExtraToolCalls=${Number(caseGates.maxExtraToolCalls)}`
    );
  }
  for (const violation of toolCallLimitViolations) {
    gateFailures.push(
      `toolCalls.${violation.toolName}=${violation.observed} > maxToolCallsByName.${violation.toolName}=${violation.maxAllowed}`
    );
  }

  const basePass =
    responseSucceeded &&
    matchedForbidden.length === 0 &&
    requiredAllRecall >= 1 &&
    requiredAnyRecall >= 1;
  const pass = basePass && gateFailures.length === 0;

  return {
    id: caseDef.id,
    area: caseDef.area,
    kpiId: caseDef.kpi_id,
    criticality: caseGates.criticality,
    deterministicConstraintsApplied: Boolean(deterministicConstraintsApplied),
    prompt: resolveCaseUserPrompt(caseDef),
    status: response.status,
    ok: responseSucceeded,
    durationMs: response.durationMs,
    requestIds: Array.isArray(response.requestIds) ? response.requestIds : [],
    evalDiagnostics: isPlainObject(response.evalDiagnostics) ? response.evalDiagnostics : null,
    toolCalls,
    toolCallDetails,
    extraToolCalls,
    extraToolCallCount,
    toolCallCounts,
    content,
    requiredToolsAll: requiredAll,
    requiredToolsAny: requiredAny,
    matchedRequired: matchedRequiredAll,
    matchedRequiredAll,
    matchedRequiredAny,
    matchedExpectedAny,
    matchedForbidden,
    matchedKeywords,
    metrics: {
      toolRecall: round(toolRecall),
      toolPrecision: roundedToolPrecision,
      toolArgumentScore: toolArgumentEval.evaluated ? round(toolArgumentEval.score) : null,
      keywordScore: round(keywordScore),
      caseScore: boundedCaseScore
    },
    gates: {
      configured: {
        minCaseScore: caseGates.minCaseScore,
        minToolPrecision: caseGates.minToolPrecision,
        minToolArgumentScore: caseGates.minToolArgumentScore,
        maxExtraToolCalls: caseGates.maxExtraToolCalls,
        maxToolCallsByName
      },
      failed: gateFailures
    },
    toolArgumentChecks: {
      evaluated: toolArgumentEval.evaluated,
      matched: toolArgumentEval.matched,
      failed: toolArgumentEval.failed
    },
    responseModeChecks: responseModeEval,
    groundedFinalAnswerChecks: groundedFinalAnswerEval,
    transportError: String(response?.transportError || '').trim(),
    pass
  };
}

function summarize(results) {
  const count = results.length || 1;
  const passed = results.filter(r => r.pass).length;
  const durations = results
    .map(r => Number(r.durationMs || 0))
    .filter(value => Number.isFinite(value) && value >= 0);
  const transportErrorCount = results.filter(r => String(r.transportError || '').trim()).length;
  const tokenTotals = results
    .map(row => Number(row?.evalDiagnostics?.upstreamUsage?.totalTokens || 0))
    .filter(value => Number.isFinite(value) && value > 0);
  const estimatedPromptTotals = results
    .map(row => Number(row?.evalDiagnostics?.requestPayloadTokenEstimate?.estimatedPromptTokens || 0))
    .filter(value => Number.isFinite(value) && value > 0);
  const usageCases = results.filter(row => Number(row?.evalDiagnostics?.upstreamUsage?.totalTokens || 0) > 0);
  const estimateCases = results.filter(
    row => Number(row?.evalDiagnostics?.requestPayloadTokenEstimate?.estimatedPromptTokens || 0) > 0
  );
  const tokenBudgetCases = results.filter(row => isPlainObject(row?.evalDiagnostics?.tokenBudget));
  const utilizationRatios = tokenBudgetCases
    .map(row => optionalNumber(row?.evalDiagnostics?.tokenBudget?.maxUtilizationRatio))
    .filter(value => value !== undefined);
  const qualityMetricCases = results.filter(row => isPlainObject(row?.evalDiagnostics?.qualityMetrics));
  const workflowScores = qualityMetricCases
    .map(row => optionalNumber(row?.evalDiagnostics?.qualityMetrics?.workflowScore))
    .filter(value => value !== undefined);
  const falseSuccessCases = results.filter(
    row => Number(row?.evalDiagnostics?.qualityMetrics?.falseSuccessClaimCount || 0) > 0
  );
  const totalFalseSuccessClaims = results.reduce(
    (acc, row) => acc + Number(row?.evalDiagnostics?.qualityMetrics?.falseSuccessClaimCount || 0),
    0
  );
  const totalContractResponseMismatches = results.reduce(
    (acc, row) => acc + Number(row?.evalDiagnostics?.qualityMetrics?.contractResponseMismatchCount || 0),
    0
  );
  const contractResponseMismatchCases = results.filter(
    row => Number(row?.evalDiagnostics?.qualityMetrics?.contractResponseMismatchCount || 0) > 0
  );
  const escalationEvaluatedCases = results.filter(row => row?.responseModeChecks?.evaluated);
  const escalationPassingCases = escalationEvaluatedCases.filter(row => row?.responseModeChecks?.pass);
  const groundedAnswerEvaluatedCases = results.filter(row => row?.groundedFinalAnswerChecks?.evaluated);
  const groundedAnswerPassingCases = groundedAnswerEvaluatedCases.filter(
    row => row?.groundedFinalAnswerChecks?.pass
  );
  const avg = key =>
    Number((results.reduce((acc, r) => acc + Number(r.metrics?.[key] || 0), 0) / count).toFixed(3));
  const evaluatedArgumentCases = results.filter(r => r.toolArgumentChecks?.evaluated);
  const avgToolArgumentScore = evaluatedArgumentCases.length
    ? Number(
        (
          evaluatedArgumentCases.reduce(
            (acc, r) => acc + Number(r.metrics?.toolArgumentScore ?? 0),
            0
          ) / evaluatedArgumentCases.length
        ).toFixed(3)
      )
    : null;

  return {
    totalCases: results.length,
    passed,
    failed: results.length - passed,
    passRate: round(passed / count),
    avgToolRecall: avg('toolRecall'),
    avgToolPrecision: avg('toolPrecision'),
    avgToolArgumentScore,
    toolArgumentEvaluatedCases: evaluatedArgumentCases.length,
    avgKeywordScore: avg('keywordScore'),
    avgCaseScore: avg('caseScore'),
    minCaseScore: round(
      Math.min(...results.map(r => Number(r.metrics?.caseScore || 0)), Number.POSITIVE_INFINITY)
    ),
    p25CaseScore: percentile(results.map(r => Number(r.metrics?.caseScore || 0)), 0.25),
    avgDurationMs: round(durations.reduce((acc, value) => acc + value, 0) / (durations.length || 1)),
    p95DurationMs: percentile(durations, 0.95),
    maxDurationMs: round(Math.max(...durations, 0)),
    totalPromptTokens: results.reduce(
      (acc, row) => acc + Number(row?.evalDiagnostics?.upstreamUsage?.promptTokens || 0),
      0
    ),
    totalCompletionTokens: results.reduce(
      (acc, row) => acc + Number(row?.evalDiagnostics?.upstreamUsage?.completionTokens || 0),
      0
    ),
    totalTokens: results.reduce(
      (acc, row) => acc + Number(row?.evalDiagnostics?.upstreamUsage?.totalTokens || 0),
      0
    ),
    avgTotalTokens: usageCases.length
      ? round(tokenTotals.reduce((acc, value) => acc + value, 0) / usageCases.length)
      : null,
    p95TotalTokens: tokenTotals.length ? percentile(tokenTotals, 0.95) : null,
    maxTotalTokens: tokenTotals.length ? round(Math.max(...tokenTotals)) : null,
    usageCoverageRate: round(usageCases.length / count),
    totalEstimatedPromptTokens: results.reduce(
      (acc, row) => acc + Number(row?.evalDiagnostics?.requestPayloadTokenEstimate?.estimatedPromptTokens || 0),
      0
    ),
    avgEstimatedPromptTokens: estimateCases.length
      ? round(estimatedPromptTotals.reduce((acc, value) => acc + value, 0) / estimateCases.length)
      : null,
    p95EstimatedPromptTokens: estimatedPromptTotals.length ? percentile(estimatedPromptTotals, 0.95) : null,
    maxEstimatedPromptTokens: estimatedPromptTotals.length ? round(Math.max(...estimatedPromptTotals)) : null,
    estimateCoverageRate: round(estimateCases.length / count),
    tokenBudgetCoverageRate: round(tokenBudgetCases.length / count),
    avgPromptBudgetUtilizationRatio: utilizationRatios.length
      ? round(utilizationRatios.reduce((acc, value) => acc + Number(value || 0), 0) / utilizationRatios.length)
      : null,
    maxPromptBudgetUtilizationRatio: utilizationRatios.length
      ? round(Math.max(...utilizationRatios))
      : null,
    tokenBudgetWarnCases: tokenBudgetCases.filter(
      row => Number(row?.evalDiagnostics?.tokenBudget?.warnCount || 0) > 0
    ).length,
    tokenBudgetCompactCases: tokenBudgetCases.filter(
      row => Number(row?.evalDiagnostics?.tokenBudget?.compactCount || 0) > 0
    ).length,
    tokenBudgetHardCases: tokenBudgetCases.filter(
      row => Number(row?.evalDiagnostics?.tokenBudget?.hardCount || 0) > 0
    ).length,
    avgWorkflowScore: workflowScores.length
      ? round(workflowScores.reduce((acc, value) => acc + Number(value || 0), 0) / workflowScores.length)
      : null,
    workflowScoreCoverageRate: round(qualityMetricCases.length / count),
    totalFalseSuccessClaims,
    falseSuccessCases: falseSuccessCases.length,
    falseSuccessClaimRate: round(falseSuccessCases.length / count),
    totalContractResponseMismatches,
    contractResponseMismatchCases: contractResponseMismatchCases.length,
    contractResponseMismatchRate: round(contractResponseMismatchCases.length / count),
    escalationEvaluatedCases: escalationEvaluatedCases.length,
    escalationPassingCases: escalationPassingCases.length,
    escalationComplianceRate: escalationEvaluatedCases.length
      ? round(escalationPassingCases.length / escalationEvaluatedCases.length)
      : null,
    groundedAnswerEvaluatedCases: groundedAnswerEvaluatedCases.length,
    groundedAnswerPassingCases: groundedAnswerPassingCases.length,
    groundedFinalAnswerRate: groundedAnswerEvaluatedCases.length
      ? round(groundedAnswerPassingCases.length / groundedAnswerEvaluatedCases.length)
      : null,
    avgExtraToolCalls: round(
      results.reduce((acc, r) => acc + Number(r.extraToolCallCount || 0), 0) /
        count
    ),
    transportErrorCount,
    transportErrorRate: round(transportErrorCount / count),
    casesWithGateFailures: results.filter(r => (r.gates?.failed || []).length > 0).length
  };
}

function isTransportSkippedCase(result) {
  if (!result || typeof result !== 'object') return false;
  const gateFailures = Array.isArray(result.gates?.failed) ? result.gates.failed : [];
  const hasTransportAbortTag = gateFailures.some(value =>
    String(value || '')
      .toLowerCase()
      .includes('transport-error-abort')
  );
  const hasTransportError = String(result.transportError || '').trim().length > 0;
  const status = Number(result.status);
  return hasTransportAbortTag || hasTransportError || (Number.isFinite(status) && status <= 0);
}

function resolveInvalidBaseline(report) {
  const runType = String(report?.runType || '').trim().toLowerCase();
  if (runType !== 'baseline') {
    return {invalidBaseline: false, invalidBaselineReason: null};
  }
  if (Boolean(report?.dryRun)) {
    return {invalidBaseline: true, invalidBaselineReason: INVALID_BASELINE_REASON.dryRun};
  }
  if (Boolean(report?.transport?.aborted)) {
    return {invalidBaseline: true, invalidBaselineReason: INVALID_BASELINE_REASON.transportAborted};
  }

  const cases = Array.isArray(report?.cases) ? report.cases : [];
  if (!cases.length) {
    return {invalidBaseline: true, invalidBaselineReason: INVALID_BASELINE_REASON.emptyCases};
  }

  const allTransportSkipped = cases.every(isTransportSkippedCase);
  if (allTransportSkipped) {
    return {
      invalidBaseline: true,
      invalidBaselineReason: INVALID_BASELINE_REASON.allTransportSkipped
    };
  }

  const summary = report?.summary || {};
  const allZeroSummary =
    Number(summary.passed || 0) === 0 &&
    Number(summary.passRate || 0) === 0 &&
    Number(summary.avgCaseScore || 0) === 0 &&
    Number(summary.avgToolPrecision || 0) === 0;
  if (allZeroSummary && cases.every(row => Number(row?.status || 0) <= 0)) {
    return {invalidBaseline: true, invalidBaselineReason: INVALID_BASELINE_REASON.zeroMetricWindow};
  }

  return {invalidBaseline: false, invalidBaselineReason: null};
}

function evaluateRunGates(summary, opts, matrixPolicy) {
  const configured = {
    minPassRate:
      optionalNumber(opts.minPassRate) ?? optionalNumber(matrixPolicy?.runGates?.minPassRate),
    minAvgCaseScore:
      optionalNumber(opts.minAvgCaseScore) ?? optionalNumber(matrixPolicy?.runGates?.minAvgCaseScore),
    minP25CaseScore:
      optionalNumber(opts.minP25CaseScore) ?? optionalNumber(matrixPolicy?.runGates?.minP25CaseScore),
    minMinCaseScore:
      optionalNumber(opts.minMinCaseScore) ?? optionalNumber(matrixPolicy?.runGates?.minMinCaseScore)
  };
  const failed = [];
  if (Number.isFinite(configured.minPassRate) && summary.passRate < Number(configured.minPassRate)) {
    failed.push(`passRate=${summary.passRate} < minPassRate=${round(configured.minPassRate)}`);
  }
  if (
    Number.isFinite(configured.minAvgCaseScore) &&
    summary.avgCaseScore < Number(configured.minAvgCaseScore)
  ) {
    failed.push(
      `avgCaseScore=${summary.avgCaseScore} < minAvgCaseScore=${round(configured.minAvgCaseScore)}`
    );
  }
  if (
    Number.isFinite(configured.minP25CaseScore) &&
    summary.p25CaseScore < Number(configured.minP25CaseScore)
  ) {
    failed.push(
      `p25CaseScore=${summary.p25CaseScore} < minP25CaseScore=${round(configured.minP25CaseScore)}`
    );
  }
  if (
    Number.isFinite(configured.minMinCaseScore) &&
    summary.minCaseScore < Number(configured.minMinCaseScore)
  ) {
    failed.push(
      `minCaseScore=${summary.minCaseScore} < minMinCaseScore=${round(configured.minMinCaseScore)}`
    );
  }
  return {
    configured,
    failed,
    pass: failed.length === 0
  };
}

function summarizeByArea(results, matrix) {
  const byArea = new Map();
  for (const result of results) {
    const areaId = String(result?.area || '').trim() || 'unknown';
    if (!byArea.has(areaId)) byArea.set(areaId, []);
    byArea.get(areaId).push(result);
  }

  const labelByAreaId = new Map();
  const orderedAreaIds = [];
  for (const area of Array.isArray(matrix?.areas) ? matrix.areas : []) {
    const areaId = String(area?.id || '').trim();
    if (!areaId) continue;
    if (!labelByAreaId.has(areaId)) {
      labelByAreaId.set(areaId, String(area?.label || areaId));
      orderedAreaIds.push(areaId);
    }
  }
  for (const areaId of [...byArea.keys()].sort((a, b) => a.localeCompare(b))) {
    if (!labelByAreaId.has(areaId)) {
      labelByAreaId.set(areaId, areaId);
      orderedAreaIds.push(areaId);
    }
  }

  return orderedAreaIds
    .map(areaId => {
      const areaResults = byArea.get(areaId) || [];
      if (!areaResults.length) return null;
      return {
        areaId,
        areaLabel: labelByAreaId.get(areaId) || areaId,
        ...summarize(areaResults)
      };
    })
    .filter(Boolean);
}

function resolveAreaGateConfig(areaSummary, matrixPolicy, opts) {
  const areaId = String(areaSummary?.areaId || '').trim();
  const areaDefaults = matrixPolicy?.areaDefaults || {};
  const areaOverrides = matrixPolicy?.areaGatesByAreaId?.[areaId] || {};
  return {
    minPassRate:
      optionalNumber(opts?.minAreaPassRate) ??
      optionalNumber(areaOverrides?.minPassRate) ??
      optionalNumber(areaDefaults?.minPassRate),
    minAvgCaseScore:
      optionalNumber(opts?.minAreaAvgCaseScore) ??
      optionalNumber(areaOverrides?.minAvgCaseScore) ??
      optionalNumber(areaDefaults?.minAvgCaseScore),
    minP25CaseScore:
      optionalNumber(opts?.minAreaP25CaseScore) ??
      optionalNumber(areaOverrides?.minP25CaseScore) ??
      optionalNumber(areaDefaults?.minP25CaseScore),
    minMinCaseScore:
      optionalNumber(opts?.minAreaMinCaseScore) ??
      optionalNumber(areaOverrides?.minMinCaseScore) ??
      optionalNumber(areaDefaults?.minMinCaseScore)
  };
}

function evaluateAreaGates(areaSummaries, opts, matrixPolicy) {
  const areas = [];
  const failed = [];
  for (const areaSummary of areaSummaries) {
    const configured = resolveAreaGateConfig(areaSummary, matrixPolicy, opts);
    const areaFailed = [];
    if (
      Number.isFinite(configured.minPassRate) &&
      areaSummary.passRate < Number(configured.minPassRate)
    ) {
      areaFailed.push(
        `passRate=${areaSummary.passRate} < minPassRate=${round(configured.minPassRate)}`
      );
    }
    if (
      Number.isFinite(configured.minAvgCaseScore) &&
      areaSummary.avgCaseScore < Number(configured.minAvgCaseScore)
    ) {
      areaFailed.push(
        `avgCaseScore=${areaSummary.avgCaseScore} < minAvgCaseScore=${round(configured.minAvgCaseScore)}`
      );
    }
    if (
      Number.isFinite(configured.minP25CaseScore) &&
      areaSummary.p25CaseScore < Number(configured.minP25CaseScore)
    ) {
      areaFailed.push(
        `p25CaseScore=${areaSummary.p25CaseScore} < minP25CaseScore=${round(configured.minP25CaseScore)}`
      );
    }
    if (
      Number.isFinite(configured.minMinCaseScore) &&
      areaSummary.minCaseScore < Number(configured.minMinCaseScore)
    ) {
      areaFailed.push(
        `minCaseScore=${areaSummary.minCaseScore} < minMinCaseScore=${round(configured.minMinCaseScore)}`
      );
    }
    const areaOutcome = {
      areaId: areaSummary.areaId,
      areaLabel: areaSummary.areaLabel,
      configured,
      failed: areaFailed,
      pass: areaFailed.length === 0,
      summary: areaSummary
    };
    areas.push(areaOutcome);
    if (!areaOutcome.pass) {
      failed.push(`${areaSummary.areaId}: ${areaFailed.join(' && ')}`);
    }
  }
  return {
    areas,
    failed,
    pass: failed.length === 0
  };
}

function readComparableEvalReports(resultsDir, currentRunId) {
  if (!fs.existsSync(resultsDir)) return [];
  const names = fs.readdirSync(resultsDir).filter(name => /^report-.*\.json$/i.test(name));
  const rows = [];
  for (const name of names) {
    const fullPath = path.join(resultsDir, name);
    try {
      const report = readJson(fullPath);
      const runId = String(report?.runId || '').trim();
      if (!runId || runId === currentRunId) continue;
      if (report?.invalidBaseline === true) continue;
      const summary = isPlainObject(report?.summary) ? report.summary : null;
      if (!summary) continue;
      const runType = normalizeEvalRunType(report?.runType) || inferEvalRunTypeFromRunId(runId);
      const createdAtMs =
        Date.parse(String(report?.createdAt || '').trim()) || fs.statSync(fullPath).mtimeMs;
      rows.push({runId, runType, summary, createdAtMs});
    } catch (_error) {
      continue;
    }
  }
  rows.sort((a, b) => b.createdAtMs - a.createdAtMs);
  return rows;
}

function isGuidedRunType(runType) {
  return runType === 'baseline' || runType === 'stabilization';
}

function buildGuidedVsHeldoutGap({runType, runId, summary, resultsDir}) {
  const normalizedRunType = normalizeEvalRunType(runType) || inferEvalRunTypeFromRunId(runId);
  const comparableReports = readComparableEvalReports(resultsDir, runId);
  let guided = null;
  let heldout = null;

  if (isGuidedRunType(normalizedRunType)) {
    guided = {runId, runType: normalizedRunType, summary};
    heldout =
      comparableReports.find(row => row.runType === 'adversarial') ||
      comparableReports.find(row => row.runType === 'heldout') ||
      null;
  } else if (normalizedRunType === 'heldout' || normalizedRunType === 'adversarial') {
    heldout = {runId, runType: normalizedRunType, summary};
    guided =
      comparableReports.find(row => row.runType === 'baseline') ||
      comparableReports.find(row => row.runType === 'stabilization') ||
      null;
  } else {
    return null;
  }

  if (!guided || !heldout) return null;
  return {
    guidedRunId: guided.runId,
    guidedRunType: guided.runType,
    heldoutRunId: heldout.runId,
    heldoutRunType: heldout.runType,
    passRateGap: round(Number(guided.summary?.passRate || 0) - Number(heldout.summary?.passRate || 0)),
    avgCaseScoreGap: round(
      Number(guided.summary?.avgCaseScore || 0) - Number(heldout.summary?.avgCaseScore || 0)
    ),
    minCaseScoreGap: round(
      Number(guided.summary?.minCaseScore || 0) - Number(heldout.summary?.minCaseScore || 0)
    ),
    falseSuccessClaimRateDeltaHeldoutMinusGuided: round(
      Number(heldout.summary?.falseSuccessClaimRate || 0) -
        Number(guided.summary?.falseSuccessClaimRate || 0)
    )
  };
}

function toMarkdown(report) {
  const lines = [];
  lines.push(`# q-map AI eval report`);
  lines.push('');
  lines.push(`- runId: ${report.runId}`);
  lines.push(`- runType: ${report.runType || 'baseline'}`);
  lines.push(`- createdAt: ${report.createdAt}`);
  lines.push(`- baseUrl: ${report.baseUrl}`);
  lines.push(`- model: ${report.model}`);
  lines.push(`- provider: ${report.provider}`);
  lines.push(`- invalidBaseline: ${Boolean(report.invalidBaseline)}`);
  lines.push(`- invalidBaselineReason: ${report.invalidBaselineReason || '-'}`);
  if (report.branchName) lines.push(`- branch: ${report.branchName}`);
  lines.push('');
  lines.push('## Summary');
  lines.push('');
  lines.push(`- totalCases: ${report.summary.totalCases}`);
  lines.push(`- passed: ${report.summary.passed}`);
  lines.push(`- failed: ${report.summary.failed}`);
  lines.push(`- passRate: ${report.summary.passRate}`);
  lines.push(`- avgToolRecall: ${report.summary.avgToolRecall}`);
  lines.push(`- avgToolPrecision: ${report.summary.avgToolPrecision}`);
  lines.push(`- avgToolArgumentScore: ${report.summary.avgToolArgumentScore ?? '-'}`);
  lines.push(`- toolArgumentEvaluatedCases: ${report.summary.toolArgumentEvaluatedCases ?? 0}`);
  lines.push(`- avgKeywordScore: ${report.summary.avgKeywordScore}`);
  lines.push(`- avgCaseScore: ${report.summary.avgCaseScore}`);
  lines.push(`- minCaseScore: ${report.summary.minCaseScore}`);
  lines.push(`- p25CaseScore: ${report.summary.p25CaseScore}`);
  lines.push(`- avgDurationMs: ${report.summary.avgDurationMs}`);
  lines.push(`- p95DurationMs: ${report.summary.p95DurationMs}`);
  lines.push(`- maxDurationMs: ${report.summary.maxDurationMs}`);
  lines.push(`- totalTokens: ${report.summary.totalTokens ?? '-'}`);
  lines.push(`- avgTotalTokens: ${report.summary.avgTotalTokens ?? '-'}`);
  lines.push(`- p95TotalTokens: ${report.summary.p95TotalTokens ?? '-'}`);
  lines.push(`- maxTotalTokens: ${report.summary.maxTotalTokens ?? '-'}`);
  lines.push(`- usageCoverageRate: ${report.summary.usageCoverageRate ?? '-'}`);
  lines.push(`- totalEstimatedPromptTokens: ${report.summary.totalEstimatedPromptTokens ?? '-'}`);
  lines.push(`- avgEstimatedPromptTokens: ${report.summary.avgEstimatedPromptTokens ?? '-'}`);
  lines.push(`- p95EstimatedPromptTokens: ${report.summary.p95EstimatedPromptTokens ?? '-'}`);
  lines.push(`- maxEstimatedPromptTokens: ${report.summary.maxEstimatedPromptTokens ?? '-'}`);
  lines.push(`- estimateCoverageRate: ${report.summary.estimateCoverageRate ?? '-'}`);
  lines.push(`- tokenBudgetCoverageRate: ${report.summary.tokenBudgetCoverageRate ?? '-'}`);
  lines.push(`- avgPromptBudgetUtilizationRatio: ${report.summary.avgPromptBudgetUtilizationRatio ?? '-'}`);
  lines.push(`- maxPromptBudgetUtilizationRatio: ${report.summary.maxPromptBudgetUtilizationRatio ?? '-'}`);
  lines.push(`- tokenBudgetWarnCases: ${report.summary.tokenBudgetWarnCases ?? 0}`);
  lines.push(`- tokenBudgetCompactCases: ${report.summary.tokenBudgetCompactCases ?? 0}`);
  lines.push(`- tokenBudgetHardCases: ${report.summary.tokenBudgetHardCases ?? 0}`);
  lines.push(`- avgWorkflowScore: ${report.summary.avgWorkflowScore ?? '-'}`);
  lines.push(`- workflowScoreCoverageRate: ${report.summary.workflowScoreCoverageRate ?? '-'}`);
  lines.push(`- totalFalseSuccessClaims: ${report.summary.totalFalseSuccessClaims ?? 0}`);
  lines.push(`- falseSuccessCases: ${report.summary.falseSuccessCases ?? 0}`);
  lines.push(`- falseSuccessClaimRate: ${report.summary.falseSuccessClaimRate ?? 0}`);
  lines.push(`- escalationEvaluatedCases: ${report.summary.escalationEvaluatedCases ?? 0}`);
  lines.push(`- escalationPassingCases: ${report.summary.escalationPassingCases ?? 0}`);
  lines.push(`- escalationComplianceRate: ${report.summary.escalationComplianceRate ?? '-'}`);
  lines.push(`- groundedAnswerEvaluatedCases: ${report.summary.groundedAnswerEvaluatedCases ?? 0}`);
  lines.push(`- groundedAnswerPassingCases: ${report.summary.groundedAnswerPassingCases ?? 0}`);
  lines.push(`- groundedFinalAnswerRate: ${report.summary.groundedFinalAnswerRate ?? '-'}`);
  lines.push(`- avgExtraToolCalls: ${report.summary.avgExtraToolCalls}`);
  lines.push(`- transportErrorCount: ${report.summary.transportErrorCount}`);
  lines.push(`- transportErrorRate: ${report.summary.transportErrorRate}`);
  lines.push(`- casesWithGateFailures: ${report.summary.casesWithGateFailures}`);
  if (report.runGates) {
    lines.push(`- runGatesPass: ${report.runGates.pass}`);
    lines.push(`- runGatesConfigured: ${JSON.stringify(report.runGates.configured)}`);
    lines.push(`- runGatesFailed: ${report.runGates.failed.join(' | ') || '-'}`);
  }
  if (report.areaGates) {
    lines.push(`- areaGatesPass: ${report.areaGates.pass}`);
    lines.push(`- areaGatesFailed: ${(report.areaGates.failed || []).join(' | ') || '-'}`);
  }
  if (report.transport) {
    lines.push(`- transportFailureThreshold: ${report.transport.failureThreshold}`);
    lines.push(`- transportPreflightSkipped: ${Boolean(report.transport.preflightSkipped)}`);
    lines.push(`- transportAbort: ${report.transport.aborted}`);
    lines.push(`- transportAbortReason: ${report.transport.reason || '-'}`);
  }
  lines.push('');
  if (report.guidedVsHeldoutGap) {
    lines.push('## Guided Vs Heldout');
    lines.push('');
    lines.push(`- guidedRunId: ${report.guidedVsHeldoutGap.guidedRunId}`);
    lines.push(`- guidedRunType: ${report.guidedVsHeldoutGap.guidedRunType}`);
    lines.push(`- heldoutRunId: ${report.guidedVsHeldoutGap.heldoutRunId}`);
    lines.push(`- heldoutRunType: ${report.guidedVsHeldoutGap.heldoutRunType}`);
    lines.push(`- passRateGap: ${report.guidedVsHeldoutGap.passRateGap}`);
    lines.push(`- avgCaseScoreGap: ${report.guidedVsHeldoutGap.avgCaseScoreGap}`);
    lines.push(`- minCaseScoreGap: ${report.guidedVsHeldoutGap.minCaseScoreGap}`);
    lines.push(
      `- falseSuccessClaimRateDeltaHeldoutMinusGuided: ${report.guidedVsHeldoutGap.falseSuccessClaimRateDeltaHeldoutMinusGuided}`
    );
    lines.push('');
  }
  if (Array.isArray(report.summaryByArea) && report.summaryByArea.length) {
    lines.push('## Areas');
    lines.push('');
    for (const area of report.summaryByArea) {
      const areaGate = Array.isArray(report.areaGates?.areas)
        ? report.areaGates.areas.find(row => row.areaId === area.areaId)
        : null;
      lines.push(`### ${area.areaId}`);
      lines.push(`- label: ${area.areaLabel}`);
      lines.push(`- totalCases: ${area.totalCases}`);
      lines.push(`- passed: ${area.passed}`);
      lines.push(`- failed: ${area.failed}`);
      lines.push(`- passRate: ${area.passRate}`);
      lines.push(`- avgCaseScore: ${area.avgCaseScore}`);
      lines.push(`- p25CaseScore: ${area.p25CaseScore}`);
      lines.push(`- minCaseScore: ${area.minCaseScore}`);
      if (areaGate) {
        lines.push(`- gatePass: ${areaGate.pass}`);
        lines.push(`- gateConfig: ${JSON.stringify(areaGate.configured || {})}`);
        lines.push(`- gateFailures: ${(areaGate.failed || []).join(' | ') || '-'}`);
      }
      lines.push('');
    }
  }

  lines.push('');
  lines.push('## Cases');
  lines.push('');

  for (const c of report.cases) {
    lines.push(`### ${c.id}`);
    lines.push(`- pass: ${c.pass}`);
    lines.push(`- criticality: ${c.criticality || 'standard'}`);
    lines.push(`- deterministicConstraintsApplied: ${Boolean(c.deterministicConstraintsApplied)}`);
    lines.push(`- status: ${c.status}`);
    lines.push(`- requestIds: ${(c.requestIds || []).join(', ') || '-'}`);
    lines.push(`- toolCalls: ${c.toolCalls.join(', ') || '-'}`);
    lines.push(`- extraToolCalls: ${(c.extraToolCalls || []).join(', ') || '-'}`);
    lines.push(`- extraToolCallCount: ${Number(c.extraToolCallCount || 0)}`);
    lines.push(`- matchedRequiredAll: ${(c.matchedRequiredAll || c.matchedRequired || []).join(', ') || '-'}`);
    lines.push(`- matchedRequiredAny: ${(c.matchedRequiredAny || []).join(', ') || '-'}`);
    lines.push(`- forbiddenMatched: ${c.matchedForbidden.join(', ') || '-'}`);
    lines.push(`- gateConfig: ${JSON.stringify(c.gates?.configured || {})}`);
    lines.push(`- gateFailures: ${(c.gates?.failed || []).join(' | ') || '-'}`);
    lines.push(`- transportError: ${c.transportError || '-'}`);
    lines.push(`- usage: ${JSON.stringify(c.evalDiagnostics?.upstreamUsage || null)}`);
    lines.push(`- requestPayloadTokenEstimate: ${JSON.stringify(c.evalDiagnostics?.requestPayloadTokenEstimate || null)}`);
    lines.push(`- tokenBudget: ${JSON.stringify(c.evalDiagnostics?.tokenBudget || null)}`);
    lines.push(`- qualityMetrics: ${JSON.stringify(c.evalDiagnostics?.qualityMetrics || null)}`);
    lines.push(`- responseModeChecks: ${JSON.stringify(c.responseModeChecks || null)}`);
    lines.push(`- groundedFinalAnswerChecks: ${JSON.stringify(c.groundedFinalAnswerChecks || null)}`);
    lines.push(`- toolArgumentEvaluated: ${Boolean(c.toolArgumentChecks?.evaluated)}`);
    lines.push(`- metrics: recall=${c.metrics.toolRecall}, precision=${c.metrics.toolPrecision}, arg=${c.metrics.toolArgumentScore ?? '-'}, keyword=${c.metrics.keywordScore}, score=${c.metrics.caseScore}`);
    lines.push('');
  }

  return `${lines.join('\n')}\n`;
}

function maybeCreateBranch({enabled, branchPrefix, runId}) {
  if (!enabled) return null;
  try {
    execSync('git rev-parse --is-inside-work-tree', {stdio: 'pipe'});
    const shortTs = new Date().toISOString().replace(/[-:TZ.]/g, '').slice(0, 12);
    const suffix = safeToken(runId || 'run');
    const branchName = `${safeToken(branchPrefix || 'qmap-eval')}/${shortTs}-${suffix}`;
    execSync(`git checkout -b ${branchName}`, {stdio: 'inherit'});
    return branchName;
  } catch (error) {
    throw new Error(`Unable to create eval branch: ${String(error?.message || error)}`);
  }
}

async function main() {
  const opts = parseArgs(process.argv.slice(2));
  const createdAt = nowIso();
  const runId = safeToken(opts.runId || createdAt.replace(/[-:TZ.]/g, '').slice(0, 14));
  const runType = normalizeEvalRunType(opts.runType) || inferEvalRunTypeFromRunId(runId);
  const repoRoot = QMAP_ROOT;
  const casesPath = resolveQMapPath(opts.casesPath);
  const matrixPath = resolveQMapPath(opts.matrixPath);
  const outDir = resolveQMapPath(opts.outDir);
  ensureDir(outDir);
  const matrix = readJsonIfExists(matrixPath);
  const matrixPolicy = loadMatrixPolicy(matrix || {});

  const cases = readJson(casesPath);
  if (!Array.isArray(cases) || !cases.length) {
    throw new Error(`No cases found in ${casesPath}`);
  }

  const branchName = maybeCreateBranch({
    enabled: opts.createBranch && !opts.dryRun,
    branchPrefix: opts.branchPrefix,
    runId
  });

  const tools = toolCatalog(repoRoot);
  const authHeaders = resolveAuthHeaders(opts.bearerToken);
  const results = [];
  const transportFailureThreshold = Math.max(
    1,
    optionalNonNegativeInt(opts.transportFailureThreshold) || 1
  );
  const skipTransportPreflight = Boolean(opts.skipTransportPreflight);
  let transportAbortReason = '';

  if (!opts.dryRun && !skipTransportPreflight) {
    const preflight = await preflightBaseUrlTransport({
      baseUrl: opts.baseUrl,
      preflightTimeoutMs: opts.preflightTimeoutMs,
      retries: opts.preflightRetries,
      retryDelayMs: opts.preflightRetryDelayMs,
      authHeaders
    });
    process.stdout.write(
      `[ai-eval][preflight] attempts=${preflight.attempts || 1} timeoutMs=${opts.preflightTimeoutMs} retries=${opts.preflightRetries}\n`
    );
    if (!preflight.ok) {
      transportAbortReason = `preflight /health failed: ${preflight.fetchError}`;
      process.stdout.write(`[ai-eval][abort] ${transportAbortReason}\n`);
      if (preflight.authFailure && !opts.bearerToken) {
        process.stdout.write(
          '[ai-eval][hint] endpoint requires bearer auth; set QMAP_AI_EVAL_BEARER_TOKEN (or EVAL_BEARER_TOKEN).\n'
        );
      }
      for (const caseDef of cases) {
        results.push(
          buildTransportSkippedOutcome(
            caseDef,
            matrixPolicy,
            transportAbortReason,
            shouldApplyDeterministicConstraints(caseDef, opts)
          )
        );
      }
    }
  } else if (!opts.dryRun && skipTransportPreflight) {
    process.stdout.write('[ai-eval][preflight] skipped by option (--skip-transport-preflight)\n');
  }

  let consecutiveTransportFailures = 0;
  for (let caseIndex = 0; caseIndex < cases.length; caseIndex += 1) {
    const caseDef = cases[caseIndex];
    if (transportAbortReason && !opts.dryRun) break;
    const casePrompt = resolveCaseUserPrompt(caseDef);
    if (!casePrompt) {
      throw new Error(`Case ${String(caseDef?.id || '<missing-id>')} is missing user_prompt`);
    }
    const deterministicConstraintsApplied = shouldApplyDeterministicConstraints(caseDef, opts);
    const caseConstraintMessage = deterministicConstraintsApplied ? buildCaseConstraintMessage(caseDef) : '';
    const messages = [{role: 'system', content: EVAL_SYSTEM_PROMPT}];
    if (caseConstraintMessage) {
      messages.push({
        role: 'system',
        content: caseConstraintMessage
      });
    }
    messages.push({role: 'user', content: casePrompt});

    const caseGates = resolveCaseGates(caseDef, matrixPolicy);
    if (opts.dryRun) {
      results.push({
        id: caseDef.id,
        area: caseDef.area,
        kpiId: caseDef.kpi_id,
        criticality: caseGates.criticality,
        deterministicConstraintsApplied,
        prompt: casePrompt,
        status: 0,
        ok: true,
        durationMs: 0,
        requestIds: [],
        toolCalls: [],
        toolCallDetails: [],
        extraToolCalls: [],
        extraToolCallCount: 0,
        content: '',
        requiredToolsAll: [],
        requiredToolsAny: [],
        matchedRequired: [],
        matchedRequiredAll: [],
        matchedRequiredAny: [],
        matchedExpectedAny: [],
        matchedForbidden: [],
        matchedKeywords: [],
        metrics: {toolRecall: 0, toolPrecision: 0, toolArgumentScore: null, keywordScore: 0, caseScore: 0},
        gates: {
          configured: {
            minCaseScore: caseGates.minCaseScore,
            minToolPrecision: caseGates.minToolPrecision,
            minToolArgumentScore: caseGates.minToolArgumentScore,
            maxExtraToolCalls: caseGates.maxExtraToolCalls
          },
          failed: ['dry-run-no-execution']
        },
        toolArgumentChecks: {evaluated: false, matched: [], failed: []},
        pass: false
      });
      continue;
    }

    const response = await runCaseConversation({
      baseUrl: opts.baseUrl,
      model: opts.model,
      messages,
      tools,
      temperature: opts.temperature,
      maxTurns: opts.maxTurns,
      requestTimeoutMs: opts.requestTimeoutMs,
      requestRetries: opts.requestRetries,
      requestRetryDelayMs: opts.requestRetryDelayMs,
      authHeaders,
      caseDef
    });

    const outcome = extractCaseOutcome(caseDef, response, matrixPolicy, deterministicConstraintsApplied);
    results.push(outcome);
    process.stdout.write(
      `[ai-eval] ${outcome.id} status=${outcome.status} pass=${outcome.pass} score=${outcome.metrics.caseScore} precision=${outcome.metrics.toolPrecision} extra=${outcome.extraToolCalls.length}\n`
    );
    if (!outcome.pass) {
      process.stdout.write(
        `[ai-eval][fail] ${outcome.id} prompt="${casePrompt.replace(/\s+/g, ' ').trim()}"\n`
      );
      process.stdout.write(
        `[ai-eval][fail] toolCalls=${outcome.toolCalls.join(', ') || '-'} requiredAny=${outcome.requiredToolsAny.join(', ') || '-'} forbiddenMatched=${outcome.matchedForbidden.join(', ') || '-'}\n`
      );
      process.stdout.write(
        `[ai-eval][fail] assistant="${String(outcome.content || '').replace(/\s+/g, ' ').trim()}"\n`
      );
      if ((outcome.gates?.failed || []).length) {
        process.stdout.write(
          `[ai-eval][fail] gates=${outcome.gates.failed.join(' | ')} criticality=${outcome.criticality}\n`
        );
      }
    }
    if (outcome.transportError) {
      consecutiveTransportFailures += 1;
      if (consecutiveTransportFailures >= transportFailureThreshold) {
        transportAbortReason = `transport errors threshold reached (${consecutiveTransportFailures}/${transportFailureThreshold}): ${outcome.transportError}`;
        const remaining = Math.max(0, cases.length - caseIndex - 1);
        process.stdout.write(
          `[ai-eval][abort] ${transportAbortReason}; skipping remaining cases=${remaining}\n`
        );
        for (let skipIndex = caseIndex + 1; skipIndex < cases.length; skipIndex += 1) {
          results.push(
            buildTransportSkippedOutcome(
              cases[skipIndex],
              matrixPolicy,
              transportAbortReason,
              shouldApplyDeterministicConstraints(cases[skipIndex], opts)
            )
          );
        }
        break;
      }
    } else {
      consecutiveTransportFailures = 0;
    }
  }

  const summary = summarize(results);
  const summaryByArea = summarizeByArea(results, matrix);
  const runGates = evaluateRunGates(summary, opts, matrixPolicy);
  const areaGates = evaluateAreaGates(summaryByArea, opts, matrixPolicy);
  const guidedVsHeldoutGap = buildGuidedVsHeldoutGap({
    runType,
    runId,
    summary,
    resultsDir: outDir
  });
  const report = {
    runId,
    runType,
    createdAt,
    baseUrl: opts.baseUrl,
    model: opts.model,
    provider: opts.provider,
    branchName,
    dryRun: opts.dryRun,
    casesPath: path.relative(repoRoot, casesPath),
    matrixPath: matrix ? path.relative(repoRoot, matrixPath) : null,
    matrixPolicy,
    summary,
    summaryByArea,
    runGates,
    areaGates,
    guidedVsHeldoutGap,
    transport: {
      failureThreshold: transportFailureThreshold,
      preflightSkipped: skipTransportPreflight,
      aborted: Boolean(transportAbortReason),
      reason: transportAbortReason || null
    },
    cases: results
  };

  const baselineValidity = resolveInvalidBaseline(report);
  report.invalidBaseline = baselineValidity.invalidBaseline;
  report.invalidBaselineReason = baselineValidity.invalidBaselineReason;

  const jsonPath = path.join(outDir, `report-${runId}.json`);
  const mdPath = path.join(outDir, `report-${runId}.md`);
  fs.writeFileSync(jsonPath, JSON.stringify(report, null, 2));
  fs.writeFileSync(mdPath, toMarkdown(report));

  process.stdout.write(`[ai-eval] report json: ${path.relative(repoRoot, jsonPath)}\n`);
  process.stdout.write(`[ai-eval] report md: ${path.relative(repoRoot, mdPath)}\n`);
  process.stdout.write(`[ai-eval] summary: ${JSON.stringify(summary)}\n`);
  if (!runGates.pass) {
    process.stdout.write(`[ai-eval] run-gates: FAIL ${runGates.failed.join(' | ')}\n`);
  } else if (Object.values(runGates.configured).some(value => Number.isFinite(value))) {
    process.stdout.write(`[ai-eval] run-gates: PASS ${JSON.stringify(runGates.configured)}\n`);
  }
  if (!areaGates.pass) {
    process.stdout.write(`[ai-eval] area-gates: FAIL ${areaGates.failed.join(' | ')}\n`);
  } else if (areaGates.areas.some(area => Object.values(area.configured).some(value => Number.isFinite(value)))) {
    process.stdout.write(
      `[ai-eval] area-gates: PASS ${JSON.stringify(
        areaGates.areas.map(area => ({areaId: area.areaId, configured: area.configured}))
      )}\n`
    );
  }

  if (!opts.dryRun && (summary.failed > 0 || !runGates.pass || !areaGates.pass)) {
    process.exitCode = 1;
  }
}

main().catch(error => {
  process.stderr.write(`[ai-eval] fatal: ${String(error?.message || error)}\n`);
  process.exit(2);
});
