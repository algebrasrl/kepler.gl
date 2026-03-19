import contractsJson from '../../../artifacts/tool-contracts/qmap-tool-contracts.json';

export const QMAP_TOOL_CONTRACT_SCHEMA = 'qmap.tool_contracts.v1';

export type QMapToolContractFlags = {
  mutatesDataset: boolean;
  discovery: boolean;
  bridgeOperation: boolean;
};

export type QMapToolContractResponse = {
  schema: string;
  properties: Record<string, unknown>;
  required: string[];
  allowAdditionalProperties: boolean;
};

export type QMapToolContractEntry = {
  toolName: string;
  categories: string[];
  flags: QMapToolContractFlags;
  argsSchema: QMapToolArgsSchema;
  responseContract: QMapToolContractResponse;
};

export type QMapToolArgsSchema = {
  type: string;
  properties: Record<string, unknown>;
  required: string[];
  additionalProperties: boolean;
};

export type QMapToolContractsManifest = {
  schema: string;
  version: string;
  defaults: {
    argsSchema: QMapToolArgsSchema;
    responseContract: QMapToolContractResponse;
  };
  tools: Record<string, QMapToolContractEntry>;
};

function normalizeStringList(value: unknown): string[] {
  if (!Array.isArray(value)) return [];
  return Array.from(
    new Set(
      value
        .map(item => String(item || '').trim())
        .filter(Boolean)
    )
  ).sort((a, b) => a.localeCompare(b));
}

function normalizeArgsSchema(raw: unknown): QMapToolArgsSchema {
  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) {
    return {type: 'object', properties: {}, required: [], additionalProperties: true};
  }
  const next = raw as Record<string, unknown>;
  return {
    type: String(next.type || 'object'),
    properties:
      next.properties && typeof next.properties === 'object' && !Array.isArray(next.properties)
        ? (next.properties as Record<string, unknown>)
        : {},
    required: normalizeStringList(next.required),
    additionalProperties: typeof next.additionalProperties === 'boolean' ? next.additionalProperties : true
  };
}

function normalizeResponseContract(raw: unknown): QMapToolContractResponse {
  const row = raw && typeof raw === 'object' && !Array.isArray(raw) ? (raw as Record<string, unknown>) : {};
  return {
    schema: String(row.schema || 'qmap.tool_result.v1').trim() || 'qmap.tool_result.v1',
    properties:
      row.properties && typeof row.properties === 'object' && !Array.isArray(row.properties)
        ? (row.properties as Record<string, unknown>)
        : {},
    required: normalizeStringList(row.required),
    allowAdditionalProperties: typeof row.allowAdditionalProperties === 'boolean' ? row.allowAdditionalProperties : true
  };
}

function normalizeManifest(raw: unknown): QMapToolContractsManifest {
  const root = raw && typeof raw === 'object' && !Array.isArray(raw) ? (raw as Record<string, unknown>) : {};
  const defaults =
    root.defaults && typeof root.defaults === 'object' && !Array.isArray(root.defaults)
      ? (root.defaults as Record<string, unknown>)
      : {};
  const defaultArgsSchema = normalizeArgsSchema(defaults.argsSchema);
  const defaultResponseContract = normalizeResponseContract(defaults.responseContract);
  const toolsRaw = root.tools && typeof root.tools === 'object' && !Array.isArray(root.tools)
    ? (root.tools as Record<string, unknown>)
    : {};
  const tools: Record<string, QMapToolContractEntry> = {};
  for (const toolName of normalizeStringList(Object.keys(toolsRaw))) {
    const row = toolsRaw[toolName];
    if (!row || typeof row !== 'object' || Array.isArray(row)) continue;
    const record = row as Record<string, unknown>;
    const flagsRaw =
      record.flags && typeof record.flags === 'object' && !Array.isArray(record.flags)
        ? (record.flags as Record<string, unknown>)
        : {};
    const argsSchema =
      record.argsSchema && typeof record.argsSchema === 'object' && !Array.isArray(record.argsSchema)
        ? normalizeArgsSchema(record.argsSchema)
        : defaultArgsSchema;
    const responseContract =
      record.responseContract && typeof record.responseContract === 'object' && !Array.isArray(record.responseContract)
        ? normalizeResponseContract(record.responseContract)
        : defaultResponseContract;
    tools[toolName] = {
      toolName,
      categories: normalizeStringList(record.categories),
      flags: {
        mutatesDataset: Boolean(flagsRaw.mutatesDataset),
        discovery: Boolean(flagsRaw.discovery),
        bridgeOperation: Boolean(flagsRaw.bridgeOperation)
      },
      argsSchema,
      responseContract
    };
  }

  return {
    schema: String(root.schema || '').trim() || QMAP_TOOL_CONTRACT_SCHEMA,
    version: String(root.version || '').trim() || '0',
    defaults: {
      argsSchema: defaultArgsSchema,
      responseContract: defaultResponseContract
    },
    tools
  };
}

const TOOL_CONTRACTS = normalizeManifest(contractsJson as unknown);

export function getQMapToolContractsManifest(): QMapToolContractsManifest {
  if (TOOL_CONTRACTS.schema !== QMAP_TOOL_CONTRACT_SCHEMA) {
    console.warn(
      `Unexpected q-map tool contract schema "${TOOL_CONTRACTS.schema}" (expected "${QMAP_TOOL_CONTRACT_SCHEMA}").`
    );
  }
  return TOOL_CONTRACTS;
}

export function getQMapToolContract(toolName: string): QMapToolContractEntry | null {
  const normalizedName = String(toolName || '').trim();
  if (!normalizedName) return null;
  return getQMapToolContractsManifest().tools[normalizedName] || null;
}

export function getQMapContractToolNames(): string[] {
  return Object.keys(getQMapToolContractsManifest().tools).sort((a, b) => a.localeCompare(b));
}

export function getQMapToolContractUnknownArgKeys(
  toolName: string,
  args: Record<string, unknown>
): {unknownArgKeys: string[]; allowedArgKeys: string[]} {
  const contract = getQMapToolContract(toolName);
  const schema = contract?.argsSchema;
  if (!schema || schema.additionalProperties) {
    return {unknownArgKeys: [], allowedArgKeys: []};
  }
  const allowedArgKeys = normalizeStringList(Object.keys(schema.properties || {}));
  if (!allowedArgKeys.length) {
    return {unknownArgKeys: [], allowedArgKeys};
  }
  const allowedSet = new Set(allowedArgKeys);
  const unknownArgKeys = normalizeStringList(Object.keys(args || {}).filter(key => !allowedSet.has(String(key || ''))));
  return {unknownArgKeys, allowedArgKeys};
}
