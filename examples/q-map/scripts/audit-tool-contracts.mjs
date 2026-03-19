#!/usr/bin/env node

import fs from 'node:fs';
import path from 'node:path';
import {fileURLToPath} from 'node:url';

const TOOL_MANIFEST_PATH = 'src/features/qmap-ai/tool-manifest.json';
const TOOL_CONTRACTS_PATH = 'artifacts/tool-contracts/qmap-tool-contracts.json';
const BACKEND_MIRROR_PATH = 'backends/q-assistant/src/q_assistant/qmap-tool-contracts.json';
const POST_VALIDATION_PATH = 'src/features/qmap-ai/post-validation.ts';
const QMAP_ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');

function readJson(filePath) {
  return JSON.parse(fs.readFileSync(filePath, 'utf8'));
}

function normalizeList(value) {
  if (!Array.isArray(value)) return [];
  return Array.from(new Set(value.map(item => String(item || '').trim()).filter(Boolean))).sort((a, b) =>
    a.localeCompare(b)
  );
}

function extractMutatingTools(postValidationSource) {
  const block =
    postValidationSource.match(/DATASET_VALIDATION_MUTATING_TOOLS\s*=\s*new Set\(\[(.*?)\]\);/s)?.[1] || '';
  return new Set(normalizeList([...block.matchAll(/'([^']+)'/g)].map(match => match[1])));
}

function parseArgs(argv) {
  return {json: argv.includes('--json')};
}

function validateResponseContract(contract, label, errors) {
  if (!contract || typeof contract !== 'object' || Array.isArray(contract)) {
    errors.push(`${label} must be an object`);
    return;
  }
  if (String(contract.schema || '').trim() !== 'qmap.tool_result.v1') {
    errors.push(`${label}.schema must be "qmap.tool_result.v1"`);
  }
  if (contract.properties !== undefined && (typeof contract.properties !== 'object' || Array.isArray(contract.properties))) {
    errors.push(`${label}.properties must be an object when present`);
  }
  if (contract.required !== undefined && !Array.isArray(contract.required)) {
    errors.push(`${label}.required must be an array when present`);
  }
  if (typeof contract.allowAdditionalProperties !== 'boolean') {
    errors.push(`${label}.allowAdditionalProperties must be boolean`);
  }
}

function main() {
  const args = parseArgs(process.argv.slice(2));
  const repoRoot = QMAP_ROOT;
  const manifest = readJson(path.join(repoRoot, TOOL_MANIFEST_PATH));
  const contracts = readJson(path.join(repoRoot, TOOL_CONTRACTS_PATH));
  const backendMirrorContracts = readJson(path.join(repoRoot, BACKEND_MIRROR_PATH));
  const postValidationSource = fs.readFileSync(path.join(repoRoot, POST_VALIDATION_PATH), 'utf8');
  const mutatingTools = extractMutatingTools(postValidationSource);
  const errors = [];

  const contractsCanonical = JSON.stringify(contracts);
  const backendMirrorCanonical = JSON.stringify(backendMirrorContracts);
  const mirrorMismatch = contractsCanonical !== backendMirrorCanonical;
  if (mirrorMismatch) {
    errors.push(
      `backend mirror is out of sync with shared contracts (${TOOL_CONTRACTS_PATH} != ${BACKEND_MIRROR_PATH})`
    );
  }

  if (String(contracts?.schema || '').trim() !== 'qmap.tool_contracts.v1') {
    errors.push(
      `contracts.schema must be "qmap.tool_contracts.v1" (got "${String(contracts?.schema || '').trim() || '<missing>'}")`
    );
  }

  const defaults = contracts?.defaults && typeof contracts.defaults === 'object' ? contracts.defaults : null;
  if (!defaults) {
    errors.push('contracts.defaults must be an object');
  } else {
    const argsSchema =
      defaults.argsSchema && typeof defaults.argsSchema === 'object' && !Array.isArray(defaults.argsSchema)
        ? defaults.argsSchema
        : null;
    const responseContract =
      defaults.responseContract &&
      typeof defaults.responseContract === 'object' &&
      !Array.isArray(defaults.responseContract)
        ? defaults.responseContract
        : null;
    if (!argsSchema || String(argsSchema.type || '').trim() !== 'object') {
      errors.push('contracts.defaults.argsSchema.type must be "object"');
    }
    if (argsSchema && typeof argsSchema.additionalProperties !== 'boolean') {
      errors.push('contracts.defaults.argsSchema.additionalProperties must be boolean');
    }
    validateResponseContract(responseContract, 'contracts.defaults.responseContract', errors);
  }

  const categories = Array.isArray(manifest?.categories) ? manifest.categories : [];
  const manifestToolToCategories = new Map();
  for (const category of categories) {
    const categoryKey = String(category?.key || '').trim();
    if (!categoryKey) continue;
    for (const tool of normalizeList(category?.tools)) {
      if (!manifestToolToCategories.has(tool)) manifestToolToCategories.set(tool, new Set());
      manifestToolToCategories.get(tool).add(categoryKey);
    }
  }

  const contractTools = contracts?.tools && typeof contracts.tools === 'object' ? contracts.tools : {};
  const manifestToolNames = [...manifestToolToCategories.keys()].sort((a, b) => a.localeCompare(b));
  const contractToolNames = normalizeList(Object.keys(contractTools));

  const missingInContracts = manifestToolNames.filter(name => !contractToolNames.includes(name));
  const extraInContracts = contractToolNames.filter(name => !manifestToolNames.includes(name));
  if (missingInContracts.length) {
    errors.push(`tools missing in contracts: ${missingInContracts.join(', ')}`);
  }
  if (extraInContracts.length) {
    errors.push(`contracts contain unknown tools: ${extraInContracts.join(', ')}`);
  }

  let mutatingMismatchCount = 0;
  for (const toolName of contractToolNames) {
    const row = contractTools[toolName];
    if (!row || typeof row !== 'object' || Array.isArray(row)) {
      errors.push(`contracts.tools.${toolName} must be an object`);
      continue;
    }
    const categoriesForTool = normalizeList(row.categories);
    const expectedCategories = [...(manifestToolToCategories.get(toolName) || [])].sort((a, b) =>
      a.localeCompare(b)
    );
    if (JSON.stringify(categoriesForTool) !== JSON.stringify(expectedCategories)) {
      errors.push(
        `contracts.tools.${toolName}.categories mismatch (expected ${expectedCategories.join('|') || '-'}, got ${categoriesForTool.join('|') || '-'})`
      );
    }
    const flags = row.flags && typeof row.flags === 'object' && !Array.isArray(row.flags) ? row.flags : {};
    const expectedMutating = mutatingTools.has(toolName);
    if (Boolean(flags.mutatesDataset) !== expectedMutating) {
      mutatingMismatchCount += 1;
      errors.push(
        `contracts.tools.${toolName}.flags.mutatesDataset mismatch (expected ${expectedMutating}, got ${Boolean(
          flags.mutatesDataset
        )})`
      );
    }
    if (typeof flags.discovery !== 'boolean') {
      errors.push(`contracts.tools.${toolName}.flags.discovery must be boolean`);
    }
    if (typeof flags.bridgeOperation !== 'boolean') {
      errors.push(`contracts.tools.${toolName}.flags.bridgeOperation must be boolean`);
    }
    if (row.responseContract !== undefined) {
      validateResponseContract(row.responseContract, `contracts.tools.${toolName}.responseContract`, errors);
    }
  }

  const summary = {
    schema: String(contracts?.schema || ''),
    manifestTools: manifestToolNames.length,
    contractTools: contractToolNames.length,
    missingInContracts: missingInContracts.length,
    extraInContracts: extraInContracts.length,
    mutatingMismatchCount,
    mirrorMismatch
  };

  if (args.json) {
    console.log(JSON.stringify({summary, errors}, null, 2));
  } else {
    console.log(
      `[tool-contract-audit] tools=${summary.contractTools}/${summary.manifestTools} missing=${summary.missingInContracts} extra=${summary.extraInContracts} mutatingMismatch=${summary.mutatingMismatchCount} mirrorMismatch=${summary.mirrorMismatch}`
    );
  }

  if (errors.length) {
    for (const error of errors) {
      console.error(`[tool-contract-audit] FAIL: ${error}`);
    }
    process.exit(1);
  }
}

main();
