import fs from 'node:fs';
import path from 'node:path';

const TOOL_MANIFEST_SCHEMA = 'qmap.tool_manifest.v1';
const TOOL_MANIFEST_RELATIVE_PATH = 'src/features/qmap-ai/tool-manifest.json';
const TOOL_CONTRACT_SCHEMA = 'qmap.tool_contracts.v1';
const TOOL_CONTRACT_RELATIVE_PATH = 'artifacts/tool-contracts/qmap-tool-contracts.json';

function normalizeStringList(value) {
  if (!Array.isArray(value)) return [];
  return Array.from(
    new Set(
      value
        .map(item => String(item || '').trim())
        .filter(Boolean)
    )
  );
}

function normalizeArgsSchema(raw) {
  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) {
    return {type: 'object', properties: {}, required: [], additionalProperties: true};
  }
  return {
    type: String(raw.type || 'object'),
    properties:
      raw.properties && typeof raw.properties === 'object' && !Array.isArray(raw.properties)
        ? raw.properties
        : {},
    required: normalizeStringList(raw.required),
    additionalProperties: typeof raw.additionalProperties === 'boolean' ? raw.additionalProperties : true
  };
}

function normalizeResponseContract(raw) {
  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) {
    return {
      schema: 'qmap.tool_result.v1',
      properties: {},
      required: ['success', 'details'],
      allowAdditionalProperties: true
    };
  }
  return {
    schema: String(raw.schema || 'qmap.tool_result.v1').trim() || 'qmap.tool_result.v1',
    properties:
      raw.properties && typeof raw.properties === 'object' && !Array.isArray(raw.properties)
        ? raw.properties
        : {},
    required: normalizeStringList(raw.required),
    allowAdditionalProperties: typeof raw.allowAdditionalProperties === 'boolean' ? raw.allowAdditionalProperties : true
  };
}

export function loadQMapToolManifest(repoRoot) {
  const manifestPath = path.resolve(repoRoot, TOOL_MANIFEST_RELATIVE_PATH);
  const raw = JSON.parse(fs.readFileSync(manifestPath, 'utf8'));
  if (String(raw?.schema || '').trim() !== TOOL_MANIFEST_SCHEMA) {
    throw new Error(
      `Unexpected tool manifest schema "${String(raw?.schema || '').trim() || '<missing>'}" in ${manifestPath}; expected ${TOOL_MANIFEST_SCHEMA}`
    );
  }

  const categories = Array.isArray(raw?.categories)
    ? raw.categories
        .map(category => {
          if (!category || typeof category !== 'object') return null;
          const key = String(category.key || '').trim();
          if (!key) return null;
          return {
            key,
            tools: normalizeStringList(category.tools)
          };
        })
        .filter(Boolean)
    : [];

  const allTools = Array.from(new Set(categories.flatMap(category => category.tools))).sort((a, b) => a.localeCompare(b));

  return {
    schema: TOOL_MANIFEST_SCHEMA,
    version: String(raw?.version || '').trim() || '0',
    categories,
    allTools,
    path: manifestPath
  };
}

export function loadQMapToolContracts(repoRoot) {
  const contractsPath = path.resolve(repoRoot, TOOL_CONTRACT_RELATIVE_PATH);
  const raw = JSON.parse(fs.readFileSync(contractsPath, 'utf8'));
  if (String(raw?.schema || '').trim() !== TOOL_CONTRACT_SCHEMA) {
    throw new Error(
      `Unexpected tool contracts schema "${String(raw?.schema || '').trim() || '<missing>'}" in ${contractsPath}; expected ${TOOL_CONTRACT_SCHEMA}`
    );
  }
  const defaults =
    raw?.defaults && typeof raw.defaults === 'object' && !Array.isArray(raw.defaults) ? raw.defaults : {};
  const defaultArgsSchema = normalizeArgsSchema(defaults?.argsSchema);
  const defaultResponseContract = normalizeResponseContract(defaults?.responseContract);
  const toolsRaw = raw?.tools && typeof raw.tools === 'object' && !Array.isArray(raw.tools) ? raw.tools : {};

  const tools = {};
  for (const toolName of normalizeStringList(Object.keys(toolsRaw))) {
    const row = toolsRaw[toolName];
    if (!row || typeof row !== 'object' || Array.isArray(row)) continue;
    const categories = normalizeStringList(row.categories);
    const flags = row.flags && typeof row.flags === 'object' && !Array.isArray(row.flags) ? row.flags : {};
    tools[toolName] = {
      categories,
      flags: {
        mutatesDataset: Boolean(flags.mutatesDataset),
        discovery: Boolean(flags.discovery),
        bridgeOperation: Boolean(flags.bridgeOperation)
      },
      argsSchema: normalizeArgsSchema(row.argsSchema || defaultArgsSchema),
      responseContract: normalizeResponseContract(row.responseContract || defaultResponseContract)
    };
  }

  return {
    schema: TOOL_CONTRACT_SCHEMA,
    version: String(raw?.version || '').trim() || '0',
    defaults: {
      argsSchema: defaultArgsSchema,
      responseContract: defaultResponseContract
    },
    tools,
    path: contractsPath
  };
}

export function buildEvalToolCatalogFromManifest(manifest, contracts = null) {
  const names = normalizeStringList(manifest?.allTools || []);
  return names.map(name => ({
    type: 'function',
    function: {
      name,
      description: `q-map tool ${name}`,
      parameters:
        contracts?.tools?.[name]?.argsSchema ||
        contracts?.defaults?.argsSchema || {
          type: 'object',
          properties: {},
          additionalProperties: true
        }
    }
  }));
}
