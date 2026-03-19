#!/usr/bin/env node

import fs from 'node:fs';
import path from 'node:path';

const cwd = process.cwd();
const artifactPath = path.resolve(
  cwd,
  process.env.QMAP_ASSISTANT_LIVE_ARTIFACT || 'test-results/assistant-live/treviso-smallest-map.json'
);
const requireServerAudit = String(process.env.QMAP_ASSISTANT_AUDIT_REQUIRE_SERVER || 'false').toLowerCase() === 'true';
const maxFailedPerRequest = Number(process.env.QMAP_ASSISTANT_MAX_FAILED_TOOLS_PER_REQUEST || 1);
const maxRepeatedFailsPerTool = Number(process.env.QMAP_ASSISTANT_MAX_REPEATED_FAILS_PER_TOOL || 1);
const minRequestIds = Number(process.env.QMAP_ASSISTANT_MIN_REQUEST_IDS || 2);
const configuredAuditDirs = String(process.env.QMAP_ASSISTANT_AUDIT_DIRS || '')
  .split(/[,;\n]+/)
  .map(value => value.trim())
  .filter(Boolean)
  .map(value => path.resolve(cwd, value));
const defaultAuditDirs = [
  path.resolve(cwd, 'test-results/assistant-live/chat-audit'),
  path.resolve(cwd, 'backends/logs/q-assistant/chat-audit'),
  path.resolve(cwd, 'backends/logs')
];
const auditDirs = Array.from(new Set((configuredAuditDirs.length ? configuredAuditDirs : defaultAuditDirs)));

function readJsonSafe(filePath) {
  try {
    return JSON.parse(fs.readFileSync(filePath, 'utf8'));
  } catch {
    return null;
  }
}

function parseJsonLines(raw) {
  const lines = String(raw || '')
    .split(/\r?\n/)
    .map(line => line.trim())
    .filter(Boolean);
  const parsed = [];
  for (const line of lines) {
    try {
      parsed.push(JSON.parse(line));
    } catch {
      // ignore malformed lines
    }
  }
  return parsed;
}

function collectSessionAuditFiles(rootPath) {
  let stats = null;
  try {
    stats = fs.statSync(rootPath);
  } catch {
    return [];
  }

  if (stats.isFile()) {
    const name = path.basename(rootPath);
    return /^session-.*\.jsonl$/i.test(name) ? [rootPath] : [];
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

function collectHostAuditEventsForRequestIds(requestIdSet, dirs) {
  const out = [];
  for (const dir of dirs) {
    for (const filePath of collectSessionAuditFiles(dir)) {
      let raw = '';
      try {
        raw = fs.readFileSync(filePath, 'utf8');
      } catch {
        continue;
      }
      const events = parseJsonLines(raw);
      for (const event of events) {
        const rid = String(event?.requestId || '').trim();
        if (rid && requestIdSet.has(rid)) out.push(event);
      }
    }
  }
  return out;
}

const artifact = readJsonSafe(artifactPath);
if (!artifact) {
  console.error(`[assistant-live-audit] FAIL: missing/invalid artifact: ${artifactPath}`);
  process.exit(2);
}

const requestIds = Array.from(
  new Set((Array.isArray(artifact.requestIds) ? artifact.requestIds : []).map(value => String(value || '').trim()).filter(Boolean))
);
if (requestIds.length < minRequestIds) {
  console.error(
    `[assistant-live-audit] FAIL: expected at least ${minRequestIds} requestIds in artifact, found ${requestIds.length}.`
  );
  process.exit(2);
}

const executionSummaries = Array.isArray(artifact.executionSummaries) ? artifact.executionSummaries : [];
if (executionSummaries.length >= minRequestIds) {
  const summaryFailures = [];
  for (const summary of executionSummaries.slice(-minRequestIds)) {
    const requestId = String(summary?.requestId || '<missing-request-id>');
    const status = String(summary?.status || '').toLowerCase();
    const failedSteps = Number(summary?.steps?.failed || 0);
    if (status !== 'success') {
      summaryFailures.push(`requestId ${requestId}: executionSummary.status=${status || '<empty>'}`);
    }
    if (failedSteps > 0) {
      summaryFailures.push(`requestId ${requestId}: executionSummary.steps.failed=${failedSteps}`);
    }
  }
  if (summaryFailures.length) {
    console.error('[assistant-live-audit] FAIL');
    summaryFailures.forEach(item => console.error(`[assistant-live-audit] ${item}`));
    process.exit(2);
  }
}

const requestIdSet = new Set(requestIds);
const events = collectHostAuditEventsForRequestIds(requestIdSet, auditDirs);
if (!events.length) {
  if (requireServerAudit) {
    console.error(
      '[assistant-live-audit] FAIL: no matching chat-audit lines found for provided requestIds. ' +
        `Checked dirs: ${auditDirs.join(', ')}. ` +
        'Run `make -C backends export-audit AUDIT_EXPORT_DIR=../test-results/assistant-live/chat-audit` before this audit.'
    );
    process.exit(2);
  }
  console.log('[assistant-live-audit] WARN: no matching server chat-audit lines found; artifact-only checks passed.');
  process.exit(0);
}
const byRequestId = new Map();
for (const event of events) {
  const rid = String(event?.requestId || '').trim();
  if (!rid) continue;
  byRequestId.set(rid, event);
}

const failures = [];
for (const requestId of requestIds) {
  const event = byRequestId.get(requestId);
  if (!event) {
    failures.push(`requestId ${requestId}: missing in chat-audit evidence from dirs [${auditDirs.join(', ')}]`);
    continue;
  }

  const summary = event?.requestToolResultsSummary || {};
  const toolResults = Array.isArray(event?.requestToolResults) ? event.requestToolResults : [];
  const failedToolResults = toolResults.filter(item => item?.success === false);
  const failedCount =
    Number.isFinite(Number(summary?.failed)) && Number(summary?.failed) >= 0
      ? Number(summary.failed)
      : failedToolResults.length;

  if (failedCount > maxFailedPerRequest) {
    failures.push(
      `requestId ${requestId}: failed tools ${failedCount} > max ${maxFailedPerRequest}`
    );
  }

  const failByTool = new Map();
  for (const item of failedToolResults) {
    const toolName = String(item?.toolName || '<unknown>');
    failByTool.set(toolName, (failByTool.get(toolName) || 0) + 1);
  }
  for (const [toolName, count] of failByTool.entries()) {
    if (count > maxRepeatedFailsPerTool) {
      failures.push(
        `requestId ${requestId}: tool "${toolName}" failed ${count} times (max ${maxRepeatedFailsPerTool})`
      );
    }
  }

  const falseSuccessClaims = Number(event?.qualityMetrics?.falseSuccessClaimCount || 0);
  if (falseSuccessClaims > 0) {
    failures.push(
      `requestId ${requestId}: falseSuccessClaimCount=${falseSuccessClaims}`
    );
  }
}

if (failures.length) {
  console.error('[assistant-live-audit] FAIL');
  failures.forEach(item => console.error(`[assistant-live-audit] ${item}`));
  process.exit(2);
}

console.log(
  `[assistant-live-audit] OK: requestIds=${requestIds.length} maxFailedPerRequest=${maxFailedPerRequest} maxRepeatedFailsPerTool=${maxRepeatedFailsPerTool} auditDirs=${auditDirs.length}`
);
