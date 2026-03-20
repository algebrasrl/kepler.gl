#!/usr/bin/env node

import fs from 'node:fs';
import path from 'node:path';

function readJson(filePath) {
  return JSON.parse(fs.readFileSync(filePath, 'utf8'));
}

function uniqueSorted(values) {
  return Array.from(new Set(values)).sort((a, b) => a.localeCompare(b));
}

function parseArgs(argv) {
  return {
    json: argv.includes('--json'),
    requireCompleteCaseMapping: argv.includes('--require-complete-case-mapping')
  };
}

function collectCaseTools(caseDef) {
  const keys = ['required_tools_any', 'required_tools_all', 'expected_tools_any'];
  const tools = [];
  for (const key of keys) {
    const arr = Array.isArray(caseDef?.[key]) ? caseDef[key] : [];
    for (const item of arr) {
      const tool = String(item || '').trim();
      if (tool) tools.push(tool);
    }
  }
  return uniqueSorted(tools);
}

function extractBaseAllowlistTools(source) {
  const block = source.match(/const BASE_TOOL_ALLOWLIST = new Set\(\[(.*?)\]\);/s)?.[1] || '';
  return [...block.matchAll(/'([^']+)'/g)].map(match => match[1]);
}

function extractCloudTools(source) {
  const block =
    source.match(/export function getQMapCloudTools\([\s\S]*?return \{([\s\S]*?)\};\n}/)?.[1] || '';
  return [...block.matchAll(/\n\s*([A-Za-z0-9_]+):/g)].map(match => match[1]);
}

function extractExplicitRuntimeTools(source) {
  const block =
    source.match(
      /const toolsWithoutCategoryIntrospection = \{([\s\S]*?)\n\s*\};\n\s*const modeScopedToolsWithoutCategoryIntrospection/s
    )?.[1] || '';
  return [...block.matchAll(/\n\s*([A-Za-z0-9_]+),/g)]
    .map(match => match[1])
    .filter(name => name !== 'baseToolsWithChartPolicy' && name !== 'qMapCloudTools');
}

function loadRuntimeTools(repoRoot) {
  const assistantPath = path.join(repoRoot, 'src/features/qmap-ai/qmap-ai-assistant-component.tsx');
  const cloudPath = path.join(repoRoot, 'src/features/qmap-ai/cloud/components.tsx');
  const assistantSource = fs.readFileSync(assistantPath, 'utf8');
  const cloudSource = fs.readFileSync(cloudPath, 'utf8');
  return uniqueSorted([
    ...extractBaseAllowlistTools(assistantSource),
    ...extractCloudTools(cloudSource),
    ...extractExplicitRuntimeTools(assistantSource),
    'listQMapToolCategories',
    'listQMapToolsByCategory'
  ]);
}

function pct(part, total) {
  if (!total) return 0;
  return Number(((part / total) * 100).toFixed(1));
}

function main() {
  const args = parseArgs(process.argv.slice(2));
  const repoRoot = process.cwd();
  const matrixPath = path.join(repoRoot, 'tests/ai-eval/architecture-matrix.json');
  const samplePath = path.join(repoRoot, 'tests/ai-eval/cases.sample.json');
  const functionalPath = path.join(repoRoot, 'tests/ai-eval/cases.functional.json');
  const adversarialPath = path.join(repoRoot, 'tests/ai-eval/cases.adversarial.json');
  const runtimeTools = loadRuntimeTools(repoRoot);
  const matrix = readJson(matrixPath);
  const sampleCases = readJson(samplePath);
  const functionalCases = readJson(functionalPath);
  const adversarialCases = fs.existsSync(adversarialPath) ? readJson(adversarialPath) : [];
  const allCases = [
    ...(Array.isArray(sampleCases) ? sampleCases : []),
    ...(Array.isArray(functionalCases) ? functionalCases : []),
    ...(Array.isArray(adversarialCases) ? adversarialCases : [])
  ];
  const byId = new Map(allCases.map(c => [String(c?.id || '').trim(), c]));
  const areaRows = [];
  const matrixTools = new Set();
  let missingCaseRefs = [];

  for (const area of Array.isArray(matrix?.areas) ? matrix.areas : []) {
    const caseIds = Array.isArray(area?.caseIds) ? area.caseIds.map(id => String(id || '').trim()).filter(Boolean) : [];
    const resolvedCases = caseIds.map(id => byId.get(id)).filter(Boolean);
    const missing = caseIds.filter(id => !byId.has(id));
    missingCaseRefs = missingCaseRefs.concat(missing.map(id => `${String(area?.id || 'unknown')}:${id}`));
    const tools = uniqueSorted(resolvedCases.flatMap(collectCaseTools));
    tools.forEach(tool => matrixTools.add(tool));
    areaRows.push({
      id: String(area?.id || ''),
      label: String(area?.label || ''),
      kpiCount: Array.isArray(area?.kpis) ? area.kpis.length : 0,
      caseCount: caseIds.length,
      resolvedCaseCount: resolvedCases.length,
      missingCaseIds: missing,
      toolCount: tools.length
    });
  }

  const matrixToolList = uniqueSorted(Array.from(matrixTools));
  const missingRuntimeTools = runtimeTools.filter(tool => !matrixToolList.includes(tool));
  const summary = {
    areas: areaRows.length,
    totalRuntimeTools: runtimeTools.length,
    matrixToolsCovered: runtimeTools.length - missingRuntimeTools.length,
    matrixToolCoveragePct: pct(runtimeTools.length - missingRuntimeTools.length, runtimeTools.length),
    missingRuntimeTools,
    missingCaseRefs: uniqueSorted(missingCaseRefs),
    areaRows
  };

  if (args.json) {
    console.log(JSON.stringify(summary, null, 2));
  } else {
    console.log(
      `[ai-matrix] areas=${summary.areas} tools=${summary.matrixToolsCovered}/${summary.totalRuntimeTools} (${summary.matrixToolCoveragePct}%)`
    );
    for (const row of summary.areaRows) {
      const missing = row.missingCaseIds.length ? ` missingCases=${row.missingCaseIds.join(',')}` : '';
      console.log(
        `[ai-matrix] area=${row.id} kpis=${row.kpiCount} cases=${row.resolvedCaseCount}/${row.caseCount} tools=${row.toolCount}${missing}`
      );
    }
    if (summary.missingRuntimeTools.length) {
      console.log(`[ai-matrix] runtime tools not covered by matrix: ${summary.missingRuntimeTools.join(', ')}`);
    }
    if (summary.missingCaseRefs.length) {
      console.log(`[ai-matrix] missing case references: ${summary.missingCaseRefs.join(', ')}`);
    }
  }

  if (args.requireCompleteCaseMapping && (summary.missingRuntimeTools.length > 0 || summary.missingCaseRefs.length > 0)) {
    process.exitCode = 1;
  }
}

main();
