#!/usr/bin/env node

import fs from 'node:fs';
import path from 'node:path';

const rootDir = path.resolve(path.dirname(new URL(import.meta.url).pathname), '..');
const assistantPath = path.join(rootDir, 'src/features/qmap-ai/qmap-ai-assistant-component.tsx');
const cloudToolsPath = path.join(rootDir, 'src/features/qmap-ai/cloud-tools.tsx');
const toolsSpecPath = path.join(rootDir, 'tests/e2e/tools.spec.ts');
const aiCasePaths = [
  path.join(rootDir, 'tests/ai-eval/cases.sample.json'),
  path.join(rootDir, 'tests/ai-eval/cases.functional.json'),
  path.join(rootDir, 'tests/ai-eval/cases.adversarial.json')
].filter(filePath => fs.existsSync(filePath));

function readUtf8(filePath) {
  return fs.readFileSync(filePath, 'utf8');
}

function uniqueSorted(values) {
  return Array.from(new Set(values)).sort((a, b) => a.localeCompare(b));
}

function extractBaseAllowlistTools(source) {
  const block = source.match(/const BASE_TOOL_ALLOWLIST = new Set\(\[(.*?)\]\);/s)?.[1] || '';
  return [...block.matchAll(/'([^']+)'/g)].map(match => match[1]);
}

function extractCloudTools(source) {
  const block = source.match(/export function getQMapCloudTools\([\s\S]*?return \{([\s\S]*?)\};\n}/)?.[1] || '';
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

function extractToolsSpecCoverage(source) {
  return [...source.matchAll(/runTool(?:ExpectSuccess|ExpectHandled)?\(page,\s*'([^']+)'/g)].map(match => match[1]);
}

function extractAiEvalCoverage(filePath) {
  const parsed = JSON.parse(readUtf8(filePath));
  const cases = Array.isArray(parsed) ? parsed : Array.isArray(parsed?.cases) ? parsed.cases : [];
  const tools = [];
  for (const c of cases) {
    for (const key of ['required_tools_any', 'expected_tools_any']) {
      const arr = Array.isArray(c?.[key]) ? c[key] : [];
      for (const item of arr) {
        const toolName = String(item || '').trim();
        if (toolName) tools.push(toolName);
      }
    }
  }
  return tools;
}

function parseArgs(argv) {
  return {
    json: argv.includes('--json'),
    requireE2EFull: argv.includes('--require-e2e-full')
  };
}

function pct(covered, total) {
  if (!total) return 0;
  return Number(((covered / total) * 100).toFixed(1));
}

function main() {
  const args = parseArgs(process.argv.slice(2));
  const assistantSource = readUtf8(assistantPath);
  const cloudSource = readUtf8(cloudToolsPath);
  const toolsSpecSource = readUtf8(toolsSpecPath);

  const runtimeTools = uniqueSorted([
    ...extractBaseAllowlistTools(assistantSource),
    ...extractCloudTools(cloudSource),
    ...extractExplicitRuntimeTools(assistantSource),
    'listQMapToolCategories',
    'listQMapToolsByCategory'
  ]);
  const e2eTools = uniqueSorted(extractToolsSpecCoverage(toolsSpecSource));
  const aiTools = uniqueSorted(aiCasePaths.flatMap(extractAiEvalCoverage));

  const e2eMissing = runtimeTools.filter(tool => !e2eTools.includes(tool));
  const aiMissing = runtimeTools.filter(tool => !aiTools.includes(tool));
  const summary = {
    runtimeTools: runtimeTools.length,
    e2eCoveredTools: runtimeTools.length - e2eMissing.length,
    e2eCoveragePct: pct(runtimeTools.length - e2eMissing.length, runtimeTools.length),
    e2eMissingTools: e2eMissing,
    aiCoveredTools: runtimeTools.length - aiMissing.length,
    aiCoveragePct: pct(runtimeTools.length - aiMissing.length, runtimeTools.length),
    aiMissingTools: aiMissing
  };

  if (args.json) {
    console.log(JSON.stringify(summary, null, 2));
  } else {
    console.log(
      `[tool-coverage] runtime=${summary.runtimeTools} e2e=${summary.e2eCoveredTools} (${summary.e2eCoveragePct}%) ai=${summary.aiCoveredTools} (${summary.aiCoveragePct}%)`
    );
    if (summary.e2eMissingTools.length) {
      console.log(`[tool-coverage] e2e missing: ${summary.e2eMissingTools.join(', ')}`);
    }
    if (summary.aiMissingTools.length) {
      console.log(`[tool-coverage] ai-eval missing: ${summary.aiMissingTools.join(', ')}`);
    }
  }

  if (args.requireE2EFull && summary.e2eMissingTools.length > 0) {
    process.exitCode = 1;
  }
}

main();
