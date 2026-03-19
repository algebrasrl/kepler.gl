type ToolArgs = Record<string, unknown>;

type DatasetRefResolver = (datasetCandidate: string) => string;

type NormalizeQMapToolExecuteArgsOptions = {
  resolveCanonicalDatasetRef?: DatasetRefResolver;
  resolveFallbackDatasetRef?: () => string;
};

const CANONICAL_DATASET_ARG_KEYS = new Set([
  'datasetName',
  'sourceDatasetName',
  'leftDatasetName',
  'rightDatasetName',
  'valueDatasetName',
  'adminDatasetName',
  'boundaryDatasetName',
  'inputDatasetName',
  'baseDatasetName',
  'joinDatasetName',
  'lookupDatasetName'
]);

const CANONICAL_DATASET_REF_TOOLS = new Set([
  'waitForQMapDataset',
  'countQMapRows',
  'createDatasetFromFilter',
  'createDatasetWithNormalizedField'
]);

// Tools that accept flat filter params (fieldName + operator + value)
const FLAT_FILTER_TOOLS = new Set(['createDatasetFromFilter', 'countQMapRows']);

// Operator aliases LLMs commonly hallucinate → canonical enum values
const OPERATOR_ALIASES: Record<string, string> = {
  '==': 'eq',
  '=': 'eq',
  equals: 'eq',
  equal: 'eq',
  '!=': 'neq',
  '<>': 'neq',
  not_eq: 'neq',
  not_equal: 'neq',
  '>': 'gt',
  greater_than: 'gt',
  '>=': 'gte',
  greater_than_or_equal: 'gte',
  '<': 'lt',
  less_than: 'lt',
  '<=': 'lte',
  less_than_or_equal: 'lte',
  like: 'contains',
  substring: 'contains',
  starts_with: 'startsWith',
  ends_with: 'endsWith'
};

/**
 * Normalize flat filter parameters for tools that accept fieldName + operator + value.
 * Handles LLM hallucinations like {filters: [{field, op, value}]} or {filter: {field, op, value}}.
 */
function normalizeFilterArgs(args: ToolArgs): void {
  // Already has flat fieldName → no recovery needed (just normalize operator alias)
  if (typeof args.fieldName === 'string' && args.fieldName) {
    if (typeof args.operator === 'string') {
      const alias = OPERATOR_ALIASES[args.operator.trim().toLowerCase()];
      if (alias) args.operator = alias;
    }
    return;
  }

  // Extract from {filters: [{field/fieldName, op/operator, value}]} or {filter: {...}}
  let filterObj: Record<string, unknown> | null = null;
  const filters = args.filters;
  const filter = args.filter;
  if (Array.isArray(filters) && filters.length > 0 && typeof filters[0] === 'object' && filters[0]) {
    filterObj = filters[0] as Record<string, unknown>;
    delete args.filters;
  } else if (filter && typeof filter === 'object' && !Array.isArray(filter)) {
    filterObj = filter as Record<string, unknown>;
    delete args.filter;
  }
  if (!filterObj) {
    // Also handle {field: "x"} without fieldName
    if (typeof args.field === 'string' && args.field) {
      args.fieldName = args.field;
      delete args.field;
    }
    if (typeof args.op === 'string' && !args.operator) {
      args.operator = args.op;
      delete args.op;
    }
  } else {
    // Extract from filter object: support field/fieldName, op/operator, value
    const fieldName = filterObj.fieldName || filterObj.field || filterObj.name;
    if (typeof fieldName === 'string' && fieldName) args.fieldName = fieldName;
    const operator = filterObj.operator || filterObj.op;
    if (typeof operator === 'string' && operator) args.operator = operator;
    if ('value' in filterObj && args.value === undefined) args.value = filterObj.value;
  }

  // Normalize operator alias
  if (typeof args.operator === 'string') {
    const alias = OPERATOR_ALIASES[args.operator.trim().toLowerCase()];
    if (alias) args.operator = alias;
  }
}

/**
 * Zod-preprocess callback for tools with flat filter params.
 * Usage: z.preprocess(preprocessFlatFilterToolArgs, z.object({...}))
 * Normalizes hallucinated shapes ({filters:[...]}, {filter:{...}}, {field,...})
 * BEFORE Zod validation so required fields (fieldName, value) are present.
 */
export function preprocessFlatFilterToolArgs(raw: unknown): unknown {
  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) return raw;
  const args = {...(raw as ToolArgs)};
  normalizeFilterArgs(args);
  return args;
}

export function normalizeQMapToolExecuteArgs(
  toolName: string,
  rawArgs: unknown,
  options: NormalizeQMapToolExecuteArgsOptions = {}
): ToolArgs {
  const normalizedToolName = String(toolName || '').trim();
  const safeArgs =
    rawArgs && typeof rawArgs === 'object' && !Array.isArray(rawArgs) ? ({...rawArgs} as ToolArgs) : {};
  const normalizedArgs: ToolArgs = {...safeArgs};

  // Normalize flat filter params for tools that accept fieldName + operator + value
  if (FLAT_FILTER_TOOLS.has(normalizedToolName)) {
    normalizeFilterArgs(normalizedArgs);
  }
  const resolver = options.resolveCanonicalDatasetRef;
  const fallbackDatasetRefResolver = options.resolveFallbackDatasetRef;
  const shouldCanonicalizeDatasetArgs = CANONICAL_DATASET_REF_TOOLS.has(normalizedToolName);
  if (
    typeof fallbackDatasetRefResolver === 'function' &&
    (normalizedToolName === 'waitForQMapDataset' || normalizedToolName === 'countQMapRows')
  ) {
    const currentDatasetName = String(normalizedArgs.datasetName || '').trim();
    if (!currentDatasetName) {
      const fallbackDatasetRef = String(fallbackDatasetRefResolver() || '').trim();
      if (fallbackDatasetRef) {
        normalizedArgs.datasetName = fallbackDatasetRef;
      }
    }
  }
  if (typeof resolver === 'function' && shouldCanonicalizeDatasetArgs) {
    for (const key of CANONICAL_DATASET_ARG_KEYS) {
      const current = normalizedArgs[key];
      if (typeof current !== 'string') continue;
      const rawValue = current.trim();
      if (!rawValue) continue;
      const canonicalRef = String(resolver(rawValue) || '').trim();
      if (!canonicalRef) continue;
      normalizedArgs[key] = canonicalRef;
    }
  }
  return normalizedArgs;
}
