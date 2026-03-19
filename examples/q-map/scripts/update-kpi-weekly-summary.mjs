#!/usr/bin/env node
import fs from 'node:fs';
import path from 'node:path';

function parseArgs(argv) {
  const out = {
    resultsDir: 'tests/ai-eval/results',
    matrixPath: 'tests/ai-eval/architecture-matrix.json',
    outPath: 'docs/KPI_WEEKLY_SUMMARY.md',
    runId: '',
    runType: String(process.env.QMAP_KPI_SUMMARY_RUN_TYPE || 'baseline').trim()
  };
  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    const val = argv[i + 1];
    if (arg === '--results-dir' && val) out.resultsDir = String(val);
    if (arg === '--matrix' && val) out.matrixPath = String(val);
    if (arg === '--out' && val) out.outPath = String(val);
    if (arg === '--run-id' && val) out.runId = String(val).trim();
    if (arg === '--run-type' && val) out.runType = String(val).trim();
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

function toNumber(value, fallback = 0) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function round3(value) {
  return Number(toNumber(value, 0).toFixed(3));
}

function signed(value) {
  const v = round3(value);
  if (v > 0) return `+${v}`;
  return String(v);
}

function asMetricCell(value) {
  return String(round3(value));
}

function asDeltaCell(value, hasBaseline) {
  if (!hasBaseline) return '-';
  return signed(value);
}

function readJson(filePath) {
  return JSON.parse(fs.readFileSync(filePath, 'utf8'));
}

function readJsonIfExists(filePath) {
  if (!fs.existsSync(filePath)) return null;
  return readJson(filePath);
}

function collectFunctionalReports(resultsDir) {
  const fullDir = path.resolve(process.cwd(), resultsDir);
  if (!fs.existsSync(fullDir)) return [];
  const names = fs.readdirSync(fullDir).filter(name => /^report-.*\.json$/i.test(name));
  const reports = [];
  for (const name of names) {
    const reportPath = path.join(fullDir, name);
    try {
      const report = readJson(reportPath);
      const casesPath = String(report?.casesPath || '');
      if (!casesPath.endsWith('tests/ai-eval/cases.functional.json')) continue;
      const createdAtMs = Date.parse(String(report?.createdAt || '')) || fs.statSync(reportPath).mtimeMs || 0;
      const runType = normalizeRunType(report?.runType) || inferRunTypeFromRunId(report?.runId);
      reports.push({
        report,
        path: reportPath,
        runType,
        createdAtMs
      });
    } catch {
      // skip invalid report files
    }
  }
  reports.sort((a, b) => a.createdAtMs - b.createdAtMs);
  return reports;
}

const AREA_DRIFT_START = '<!-- KPI_AREA_DRIFT_START -->';
const AREA_DRIFT_END = '<!-- KPI_AREA_DRIFT_END -->';

function defaultMarkdown() {
  return [
    '# KPI Weekly Summary',
    '',
    'Compact weekly tracking of `ai-eval-functional` KPI deltas.',
    '',
    `${AREA_DRIFT_START}`,
    '## Latest Area Drift',
    '',
    '_No run yet._',
    `${AREA_DRIFT_END}`,
    '',
    '| Date | Run ID | Baseline Run | PassRate | AvgCaseScore | Delta AvgCaseScore | AvgToolPrecision | Delta AvgToolPrecision | AvgExtraToolCalls | Delta AvgExtraToolCalls | MinCaseScore | Delta MinCaseScore |',
    '| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |',
    ''
  ].join('\n');
}

function ensureOutFile(outPath) {
  const fullPath = path.resolve(process.cwd(), outPath);
  fs.mkdirSync(path.dirname(fullPath), {recursive: true});
  if (!fs.existsSync(fullPath)) {
    fs.writeFileSync(fullPath, defaultMarkdown());
  }
  return fullPath;
}

function pickLatestReports(reports, runId) {
  if (!reports.length) {
    throw new Error('No functional ai-eval reports found.');
  }
  let latestIndex = reports.length - 1;
  if (runId) {
    const idx = reports.findIndex(row => String(row.report?.runId || '') === runId);
    if (idx < 0) {
      throw new Error(`runId "${runId}" not found in functional reports.`);
    }
    latestIndex = idx;
  }
  const latest = reports[latestIndex];
  const previous = latestIndex > 0 ? reports[latestIndex - 1] : null;
  return {latest, previous};
}

function buildRow({latest, previous}) {
  const latestSummary = latest.report?.summary || {};
  const baselineSummary = previous?.report?.summary || {};
  const latestRunId = String(latest.report?.runId || path.basename(latest.path, '.json'));
  const hasBaseline = Boolean(previous);
  const baselineRunId = hasBaseline ? String(previous.report?.runId || path.basename(previous.path, '.json')) : '-';
  const day = new Date().toISOString().slice(0, 10);

  const deltaAvgCase = round3(toNumber(latestSummary.avgCaseScore) - toNumber(baselineSummary.avgCaseScore));
  const deltaPrecision = round3(toNumber(latestSummary.avgToolPrecision) - toNumber(baselineSummary.avgToolPrecision));
  const deltaExtra = round3(toNumber(latestSummary.avgExtraToolCalls) - toNumber(baselineSummary.avgExtraToolCalls));
  const deltaMinCase = round3(toNumber(latestSummary.minCaseScore) - toNumber(baselineSummary.minCaseScore));

  return (
    `| ${day} | \`${latestRunId}\` | ${hasBaseline ? `\`${baselineRunId}\`` : '-'} | ` +
    `${asMetricCell(latestSummary.passRate)} | ${asMetricCell(latestSummary.avgCaseScore)} | ${asDeltaCell(deltaAvgCase, hasBaseline)} | ` +
    `${asMetricCell(latestSummary.avgToolPrecision)} | ${asDeltaCell(deltaPrecision, hasBaseline)} | ` +
    `${asMetricCell(latestSummary.avgExtraToolCalls)} | ${asDeltaCell(deltaExtra, hasBaseline)} | ` +
    `${asMetricCell(latestSummary.minCaseScore)} | ${asDeltaCell(deltaMinCase, hasBaseline)} |`
  );
}

function appendRowIfMissing(outPath, runId, row) {
  const current = fs.readFileSync(outPath, 'utf8');
  if (current.includes(`\`${runId}\``)) {
    return false;
  }
  const next = current.trimEnd() + '\n' + row + '\n';
  fs.writeFileSync(outPath, next);
  return true;
}

function classifyAreaTrend(delta, hasBaseline) {
  if (!hasBaseline) return 'n/a';
  const regressing =
    delta.passRate <= -0.03 ||
    delta.avgCaseScore <= -0.02 ||
    delta.avgToolPrecision <= -0.02 ||
    delta.avgExtraToolCalls >= 0.04;
  if (regressing) return 'regressing';
  const improving =
    delta.passRate >= 0.02 ||
    delta.avgCaseScore >= 0.01 ||
    delta.avgToolPrecision >= 0.01 ||
    delta.avgExtraToolCalls <= -0.03;
  if (improving) return 'improving';
  return 'stable';
}

function orderedAreaIds(matrix, latestAreas, baselineAreas) {
  const ids = [];
  const seen = new Set();
  for (const area of Array.isArray(matrix?.areas) ? matrix.areas : []) {
    const areaId = String(area?.id || '').trim();
    if (!areaId || seen.has(areaId)) continue;
    seen.add(areaId);
    ids.push(areaId);
  }
  for (const area of [...latestAreas, ...baselineAreas]) {
    const areaId = String(area?.areaId || '').trim();
    if (!areaId || seen.has(areaId)) continue;
    seen.add(areaId);
    ids.push(areaId);
  }
  return ids;
}

function mapByAreaId(areas) {
  const map = new Map();
  for (const area of Array.isArray(areas) ? areas : []) {
    const areaId = String(area?.areaId || '').trim();
    if (!areaId || map.has(areaId)) continue;
    map.set(areaId, area);
  }
  return map;
}

function buildLatestAreaDriftSection({latest, previous, matrix}) {
  const latestRunId = String(latest.report?.runId || path.basename(latest.path, '.json'));
  const latestAreas = Array.isArray(latest.report?.summaryByArea) ? latest.report.summaryByArea : [];
  const baselineAreas = Array.isArray(previous?.report?.summaryByArea) ? previous.report.summaryByArea : [];
  const hasBaseline = Boolean(previous);
  const baselineRunId = hasBaseline
    ? String(previous.report?.runId || path.basename(previous.path, '.json'))
    : '';

  const latestMap = mapByAreaId(latestAreas);
  const baselineMap = mapByAreaId(baselineAreas);
  const areaIds = orderedAreaIds(matrix, latestAreas, baselineAreas);

  const lines = [];
  lines.push(AREA_DRIFT_START);
  lines.push('## Latest Area Drift');
  lines.push('');
  lines.push(
    hasBaseline
      ? `Run: \`${latestRunId}\` vs baseline \`${baselineRunId}\``
      : `Run: \`${latestRunId}\` (baseline unavailable for this runType window)`
  );
  lines.push('');
  lines.push(
    '| Area | PassRate | Delta PassRate | AvgCaseScore | Delta AvgCaseScore | AvgToolPrecision | Delta AvgToolPrecision | AvgExtraToolCalls | Delta AvgExtraToolCalls | Trend |'
  );
  lines.push('| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |');

  for (const areaId of areaIds) {
    const latestArea = latestMap.get(areaId);
    if (!latestArea) continue;
    const baselineArea = baselineMap.get(areaId);
    const label = String(latestArea?.areaLabel || areaId);

    const delta = {
      passRate: round3(toNumber(latestArea.passRate) - toNumber(baselineArea?.passRate)),
      avgCaseScore: round3(toNumber(latestArea.avgCaseScore) - toNumber(baselineArea?.avgCaseScore)),
      avgToolPrecision: round3(
        toNumber(latestArea.avgToolPrecision) - toNumber(baselineArea?.avgToolPrecision)
      ),
      avgExtraToolCalls: round3(
        toNumber(latestArea.avgExtraToolCalls) - toNumber(baselineArea?.avgExtraToolCalls)
      )
    };

    lines.push(
      `| ${label} | ${asMetricCell(latestArea.passRate)} | ${asDeltaCell(delta.passRate, hasBaseline)} | ` +
        `${asMetricCell(latestArea.avgCaseScore)} | ${asDeltaCell(delta.avgCaseScore, hasBaseline)} | ` +
        `${asMetricCell(latestArea.avgToolPrecision)} | ${asDeltaCell(delta.avgToolPrecision, hasBaseline)} | ` +
        `${asMetricCell(latestArea.avgExtraToolCalls)} | ${asDeltaCell(delta.avgExtraToolCalls, hasBaseline)} | ` +
        `${classifyAreaTrend(delta, hasBaseline)} |`
    );
  }

  lines.push(AREA_DRIFT_END);
  return `${lines.join('\n')}\n`;
}

function upsertLatestAreaDriftSection(outPath, section) {
  const current = fs.readFileSync(outPath, 'utf8');
  const startIndex = current.indexOf(AREA_DRIFT_START);
  const endIndex = current.indexOf(AREA_DRIFT_END);

  let next = current;
  if (startIndex >= 0 && endIndex > startIndex) {
    const endOffset = endIndex + AREA_DRIFT_END.length;
    next = `${current.slice(0, startIndex)}${section}${current.slice(endOffset)}`;
  } else {
    const tableAnchor = '| Date | Run ID | Baseline Run |';
    const anchorIndex = current.indexOf(tableAnchor);
    if (anchorIndex >= 0) {
      next = `${current.slice(0, anchorIndex).trimEnd()}\n\n${section}\n${current.slice(anchorIndex)}`;
    } else {
      next = `${current.trimEnd()}\n\n${section}`;
    }
  }

  if (next !== current) {
    fs.writeFileSync(outPath, next);
    return true;
  }
  return false;
}

function main() {
  const opts = parseArgs(process.argv.slice(2));
  const runType = normalizeRunType(opts.runType) || 'baseline';
  const reports = collectFunctionalReports(opts.resultsDir).filter(row =>
    runType === 'all' ? true : row.runType === runType
  );
  const {latest, previous} = pickLatestReports(reports, opts.runId);
  const outPath = ensureOutFile(opts.outPath);
  const matrix = readJsonIfExists(path.resolve(process.cwd(), opts.matrixPath));
  const runId = String(latest.report?.runId || '');
  const row = buildRow({latest, previous});
  const appended = appendRowIfMissing(outPath, runId, row);
  const areaSection = buildLatestAreaDriftSection({latest, previous, matrix});
  const areaUpdated = upsertLatestAreaDriftSection(outPath, areaSection);

  process.stdout.write(
    `[kpi-weekly-summary] ${appended ? 'UPDATED' : 'UNCHANGED'} runId=${runId} runType=${runType} ` +
      `areaDrift=${areaUpdated ? 'UPDATED' : 'UNCHANGED'} file=${path.relative(process.cwd(), outPath)}\n`
  );
}

main();
