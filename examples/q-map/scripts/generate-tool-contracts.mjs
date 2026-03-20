#!/usr/bin/env node

import fs from 'node:fs';
import path from 'node:path';
import {fileURLToPath} from 'node:url';

const TOOL_MANIFEST_PATH = 'src/features/qmap-ai/tool-manifest.json';
const POST_VALIDATION_PATH = 'src/features/qmap-ai/services/post-validation.ts';
const OUTPUT_PATH = 'artifacts/tool-contracts/qmap-tool-contracts.json';
const BACKEND_MIRROR_PATH = 'backends/q-assistant/src/q_assistant/qmap-tool-contracts.json';
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
  return normalizeList([...block.matchAll(/'([^']+)'/g)].map(match => match[1]));
}

function buildStrictArgsSchema(properties, required = []) {
  return {
    type: 'object',
    properties: properties && typeof properties === 'object' ? properties : {},
    required: normalizeList(required),
    additionalProperties: false
  };
}

function buildResponseContract(properties, required = [], allowAdditionalProperties = true) {
  return {
    schema: 'qmap.tool_result.v1',
    properties: properties && typeof properties === 'object' ? properties : {},
    required: normalizeList(required),
    allowAdditionalProperties: Boolean(allowAdditionalProperties)
  };
}

const STRING_SCHEMA = {type: 'string'};
const BOOLEAN_SCHEMA = {type: 'boolean'};
const STRING_LIST_SCHEMA = {type: 'array', items: {type: 'string'}};
const STRING_MAP_SCHEMA = {type: 'object', additionalProperties: {type: 'string'}};
const ARRAY_SCHEMA = {type: 'array'};

const DEFAULT_RESPONSE_CONTRACT = buildResponseContract({
  success: BOOLEAN_SCHEMA,
  details: STRING_SCHEMA,
  dataset: STRING_SCHEMA,
  datasetName: STRING_SCHEMA,
  datasetId: STRING_SCHEMA,
  datasetRef: STRING_SCHEMA,
  loadedDatasetName: STRING_SCHEMA,
  loadedDatasetRef: STRING_SCHEMA,
  outputFieldName: STRING_SCHEMA,
  fieldCatalog: STRING_LIST_SCHEMA,
  numericFields: STRING_LIST_SCHEMA,
  styleableFields: STRING_LIST_SCHEMA,
  defaultStyleField: STRING_SCHEMA,
  aggregationOutputs: STRING_MAP_SCHEMA,
  fieldAliases: STRING_MAP_SCHEMA,
  clarificationRequired: BOOLEAN_SCHEMA,
  clarificationQuestion: STRING_SCHEMA,
  clarificationOptions: STRING_LIST_SCHEMA
}, ['success', 'details']);

const STRICT_TOOL_ARGS_OVERRIDES = {
  countQMapRows: buildStrictArgsSchema(
    {
      datasetName: {type: 'string'},
      fieldName: {type: 'string'},
      operator: {type: 'string'},
      value: {
        anyOf: [
          {type: 'number'},
          {type: 'string'},
          {type: 'boolean'},
          {
            type: 'array',
            items: {
              anyOf: [{type: 'number'}, {type: 'string'}]
            }
          }
        ]
      }
    },
    ['datasetName']
  ),
  listQMapCloudMaps: buildStrictArgsSchema({
    provider: {type: 'string'}
  }),
  loadQMapCloudMap: buildStrictArgsSchema(
    {
      provider: {type: 'string'},
      mapId: {type: 'string'}
    },
    ['mapId']
  ),
  loadCloudMapAndWait: buildStrictArgsSchema({
    provider: {type: 'string'},
    mapId: {type: 'string'},
    timeoutMs: {type: 'number'}
  }, ['mapId']),
  listQCumberProviders: buildStrictArgsSchema({
    locale: {type: 'string'}
  }),
  listQCumberDatasets: buildStrictArgsSchema({
    providerId: {type: 'string'}
  }),
  getQCumberDatasetHelp: buildStrictArgsSchema(
    {
      providerId: {type: 'string'},
      datasetId: {type: 'string'}
    },
    ['providerId', 'datasetId']
  ),
  queryQCumberDataset: buildStrictArgsSchema(
    {
      providerId: {type: 'string'},
      datasetId: {type: 'string'},
      filters: {type: 'array', items: {type: 'object', properties: {field: {type: 'string'}, op: {type: 'string', enum: ['eq','ne','gt','gte','lt','lte','in','contains','startswith','endswith','is_null','not_null']}, value: {anyOf: [{type: 'string'},{type: 'number'},{type: 'boolean'},{type: 'null'}]}, values: {type: 'array', items: {anyOf: [{type: 'string'},{type: 'number'},{type: 'boolean'},{type: 'null'}]}}}, required: ['field']}},
      orderBy: {type: 'string'},
      orderDirection: {type: 'string'},
      limit: {type: 'integer'},
      offset: {type: 'integer'},
      loadToMap: {type: 'boolean'},
      showOnMap: {type: 'boolean'},
      spatialBbox: {type: 'array', items: {type: 'number'}, minItems: 4, maxItems: 4, description: '[minLon, minLat, maxLon, maxLat]'},
      expectedAdminType: {type: 'string'},
      inferPointsFromLatLon: {type: 'boolean'}
    },
    ['providerId', 'datasetId']
  ),
  queryQCumberTerritorialUnits: buildStrictArgsSchema(
    {
      providerId: {type: 'string'},
      datasetId: {type: 'string'},
      filters: {type: 'array', items: {type: 'object', properties: {field: {type: 'string'}, op: {type: 'string', enum: ['eq','ne','gt','gte','lt','lte','in','contains','startswith','endswith','is_null','not_null']}, value: {anyOf: [{type: 'string'},{type: 'number'},{type: 'boolean'},{type: 'null'}]}, values: {type: 'array', items: {anyOf: [{type: 'string'},{type: 'number'},{type: 'boolean'},{type: 'null'}]}}}, required: ['field']}},
      orderBy: {type: 'string'},
      orderDirection: {type: 'string'},
      limit: {type: 'integer'},
      offset: {type: 'integer'},
      loadToMap: {type: 'boolean'},
      showOnMap: {type: 'boolean'},
      expectedAdminType: {type: 'string'}
    },
    ['providerId', 'datasetId']
  ),
  queryQCumberDatasetSpatial: buildStrictArgsSchema(
    {
      providerId: {type: 'string'},
      datasetId: {type: 'string'},
      filters: {type: 'array', items: {type: 'object', properties: {field: {type: 'string'}, op: {type: 'string', enum: ['eq','ne','gt','gte','lt','lte','in','contains','startswith','endswith','is_null','not_null']}, value: {anyOf: [{type: 'string'},{type: 'number'},{type: 'boolean'},{type: 'null'}]}, values: {type: 'array', items: {anyOf: [{type: 'string'},{type: 'number'},{type: 'boolean'},{type: 'null'}]}}}, required: ['field']}},
      orderBy: {type: 'string'},
      orderDirection: {type: 'string'},
      limit: {type: 'integer'},
      offset: {type: 'integer'},
      loadToMap: {type: 'boolean'},
      showOnMap: {type: 'boolean'},
      spatialBbox: {type: 'array', items: {type: 'number'}, minItems: 4, maxItems: 4, description: '[minLon, minLat, maxLon, maxLat]'}
    },
    ['providerId', 'datasetId']
  ),
  waitForQMapDataset: buildStrictArgsSchema(
    {
      datasetName: {type: 'string'},
      timeoutMs: {type: 'number'}
    },
    ['datasetName']
  ),
  clipQMapDatasetByGeometry: buildStrictArgsSchema(
    {
      sourceDatasetName: {type: 'string'},
      clipDatasetName: {type: 'string'},
      sourceGeometryField: {type: 'string'},
      clipGeometryField: {type: 'string'},
      mode: {type: 'string', enum: ['intersects', 'centroid', 'within']},
      useActiveFilters: {type: 'boolean'},
      maxSourceFeatures: {type: 'number'},
      maxClipFeatures: {type: 'number'},
      includeIntersectionMetrics: {type: 'boolean'},
      includeDistinctPropertyCounts: {type: 'boolean'},
      includeDistinctPropertyValueCounts: {type: 'boolean'},
      showOnMap: {type: 'boolean'},
      newDatasetName: {type: 'string'}
    },
    ['sourceDatasetName', 'clipDatasetName']
  ),
  zonalStatsByAdmin: buildStrictArgsSchema(
    {
      adminDatasetName: {type: 'string'},
      valueDatasetName: {type: 'string'},
      adminGeometryField: {type: 'string'},
      valueGeometryField: {type: 'string'},
      valueField: {type: 'string'},
      aggregation: {type: 'string', enum: ['sum', 'avg', 'min', 'max', 'count']},
      weightMode: {type: 'string', enum: ['area_weighted', 'intersects', 'centroid']},
      useActiveFilters: {type: 'boolean'},
      maxAdminFeatures: {type: 'number'},
      maxValueFeatures: {type: 'number'},
      outputFieldName: {type: 'string'},
      outputAreaField: {type: 'string'},
      showOnMap: {type: 'boolean'},
      newDatasetName: {type: 'string'}
    },
    ['adminDatasetName', 'valueDatasetName']
  ),
  spatialJoinByPredicate: buildStrictArgsSchema(
    {
      leftDatasetName: {type: 'string'},
      rightDatasetName: {type: 'string'},
      leftGeometryField: {type: 'string'},
      rightGeometryField: {type: 'string'},
      predicate: {type: 'string', enum: ['intersects', 'within', 'contains', 'touches']},
      rightValueField: {type: 'string'},
      aggregations: {type: 'array', items: {type: 'string'}},
      includeRightFields: {type: 'array', items: {type: 'string'}},
      useActiveFilters: {type: 'boolean'},
      maxLeftFeatures: {type: 'number'},
      maxRightFeatures: {type: 'number'},
      showOnMap: {type: 'boolean'},
      newDatasetName: {type: 'string'}
    },
    ['leftDatasetName', 'rightDatasetName']
  ),
  overlayDifference: buildStrictArgsSchema(
    {
      datasetAName: {type: 'string'},
      datasetBName: {type: 'string'},
      geometryFieldA: {type: 'string'},
      geometryFieldB: {type: 'string'},
      includeIntersection: {type: 'boolean'},
      includeADifference: {type: 'boolean'},
      includeBDifference: {type: 'boolean'},
      useActiveFilters: {type: 'boolean'},
      maxFeaturesA: {type: 'number'},
      maxFeaturesB: {type: 'number'},
      showOnMap: {type: 'boolean'},
      newDatasetName: {type: 'string'}
    },
    ['datasetAName', 'datasetBName']
  ),
  bufferAndSummarize: buildStrictArgsSchema(
    {
      sourceDatasetName: {type: 'string'},
      targetDatasetName: {type: 'string'},
      sourceGeometryField: {type: 'string'},
      targetGeometryField: {type: 'string'},
      radiusKm: {type: 'number'},
      targetValueField: {type: 'string'},
      aggregation: {type: 'string', enum: ['count', 'sum', 'avg', 'min', 'max']},
      outputFieldName: {type: 'string'},
      useActiveFilters: {type: 'boolean'},
      maxSourceFeatures: {type: 'number'},
      maxTargetFeatures: {type: 'number'},
      showOnMap: {type: 'boolean'},
      newDatasetName: {type: 'string'}
    },
    ['sourceDatasetName', 'targetDatasetName', 'radiusKm']
  )
};

const RESPONSE_CONTRACT_OVERRIDES = {
  listQMapDatasets: buildResponseContract({
    success: BOOLEAN_SCHEMA,
    details: STRING_SCHEMA,
    datasets: ARRAY_SCHEMA,
    layers: ARRAY_SCHEMA
  }, ['success', 'details', 'datasets', 'layers']),
  listQMapCloudMaps: buildResponseContract({
    success: BOOLEAN_SCHEMA,
    details: STRING_SCHEMA,
    maps: ARRAY_SCHEMA
  }, ['success', 'details', 'maps']),
  loadCloudMapAndWait: buildResponseContract({
    ...DEFAULT_RESPONSE_CONTRACT.properties,
    loadedDatasetName: STRING_SCHEMA,
    loadedDatasetRef: STRING_SCHEMA
  }, ['success', 'details', 'loadedDatasetRef']),
  loadQMapCloudMap: buildResponseContract({
    ...DEFAULT_RESPONSE_CONTRACT.properties,
    loadedDatasetName: STRING_SCHEMA,
    loadedDatasetRef: STRING_SCHEMA
  }, ['success', 'details', 'loadedDatasetRef']),
  spatialJoinByPredicate: buildResponseContract({
    ...DEFAULT_RESPONSE_CONTRACT.properties,
    dataset: STRING_SCHEMA,
    datasetId: STRING_SCHEMA,
    fieldCatalog: STRING_LIST_SCHEMA,
    numericFields: STRING_LIST_SCHEMA,
    styleableFields: STRING_LIST_SCHEMA,
    defaultStyleField: STRING_SCHEMA,
    aggregationOutputs: STRING_MAP_SCHEMA
  }, ['success', 'details', 'dataset', 'fieldCatalog', 'numericFields', 'styleableFields', 'defaultStyleField']),
  createDatasetWithGeometryArea: buildResponseContract({
    ...DEFAULT_RESPONSE_CONTRACT.properties,
    dataset: STRING_SCHEMA,
    outputFieldName: STRING_SCHEMA,
    fieldCatalog: STRING_LIST_SCHEMA,
    numericFields: STRING_LIST_SCHEMA,
    styleableFields: STRING_LIST_SCHEMA,
    defaultStyleField: STRING_SCHEMA
  }, ['success', 'details', 'dataset', 'outputFieldName', 'fieldCatalog', 'numericFields', 'styleableFields', 'defaultStyleField']),
  createDatasetWithNormalizedField: buildResponseContract({
    ...DEFAULT_RESPONSE_CONTRACT.properties,
    dataset: STRING_SCHEMA,
    outputFieldName: STRING_SCHEMA,
    fieldCatalog: STRING_LIST_SCHEMA,
    numericFields: STRING_LIST_SCHEMA,
    styleableFields: STRING_LIST_SCHEMA,
    defaultStyleField: STRING_SCHEMA
  }, ['success', 'details', 'dataset', 'outputFieldName', 'fieldCatalog', 'numericFields', 'styleableFields', 'defaultStyleField']),
  nearestFeatureJoin: buildResponseContract({
    ...DEFAULT_RESPONSE_CONTRACT.properties,
    dataset: STRING_SCHEMA,
    fieldCatalog: STRING_LIST_SCHEMA,
    numericFields: STRING_LIST_SCHEMA,
    styleableFields: STRING_LIST_SCHEMA,
    defaultStyleField: STRING_SCHEMA
  }, ['success', 'details', 'dataset', 'fieldCatalog', 'numericFields', 'styleableFields', 'defaultStyleField']),
  zonalStatsByAdmin: buildResponseContract({
    ...DEFAULT_RESPONSE_CONTRACT.properties,
    dataset: STRING_SCHEMA,
    outputFieldName: STRING_SCHEMA,
    fieldCatalog: STRING_LIST_SCHEMA,
    numericFields: STRING_LIST_SCHEMA,
    styleableFields: STRING_LIST_SCHEMA,
    defaultStyleField: STRING_SCHEMA,
    aggregationOutputs: STRING_MAP_SCHEMA
  }, ['success', 'details', 'dataset', 'outputFieldName', 'fieldCatalog', 'numericFields', 'styleableFields', 'defaultStyleField', 'aggregationOutputs']),
  bufferAndSummarize: buildResponseContract({
    ...DEFAULT_RESPONSE_CONTRACT.properties,
    dataset: STRING_SCHEMA,
    outputFieldName: STRING_SCHEMA,
    fieldCatalog: STRING_LIST_SCHEMA,
    numericFields: STRING_LIST_SCHEMA,
    styleableFields: STRING_LIST_SCHEMA,
    defaultStyleField: STRING_SCHEMA,
    aggregationOutputs: STRING_MAP_SCHEMA
  }, ['success', 'details', 'dataset', 'outputFieldName', 'fieldCatalog', 'numericFields', 'styleableFields', 'defaultStyleField', 'aggregationOutputs']),
  joinQMapDatasetsOnH3: buildResponseContract({
    ...DEFAULT_RESPONSE_CONTRACT.properties,
    dataset: STRING_SCHEMA,
    fieldCatalog: STRING_LIST_SCHEMA,
    numericFields: STRING_LIST_SCHEMA,
    styleableFields: STRING_LIST_SCHEMA,
    defaultStyleField: STRING_SCHEMA
  }, ['success', 'details', 'dataset', 'fieldCatalog', 'numericFields', 'styleableFields', 'defaultStyleField']),
  aggregateDatasetToH3: buildResponseContract({
    ...DEFAULT_RESPONSE_CONTRACT.properties,
    dataset: STRING_SCHEMA,
    fieldCatalog: STRING_LIST_SCHEMA,
    numericFields: STRING_LIST_SCHEMA,
    styleableFields: STRING_LIST_SCHEMA,
    defaultStyleField: STRING_SCHEMA,
    aggregationOutputs: STRING_MAP_SCHEMA
  }, ['success', 'details', 'dataset', 'fieldCatalog', 'numericFields', 'styleableFields', 'defaultStyleField', 'aggregationOutputs']),
  populateTassellationFromAdminUnits: buildResponseContract({
    ...DEFAULT_RESPONSE_CONTRACT.properties,
    dataset: STRING_SCHEMA,
    outputFieldName: STRING_SCHEMA,
    fieldCatalog: STRING_LIST_SCHEMA,
    numericFields: STRING_LIST_SCHEMA,
    styleableFields: STRING_LIST_SCHEMA,
    defaultStyleField: STRING_SCHEMA,
    aggregationOutputs: STRING_MAP_SCHEMA
  }, ['success', 'details', 'dataset', 'outputFieldName', 'fieldCatalog', 'numericFields', 'styleableFields', 'defaultStyleField', 'aggregationOutputs']),
  populateTassellationFromAdminUnitsAreaWeighted: buildResponseContract({
    ...DEFAULT_RESPONSE_CONTRACT.properties,
    dataset: STRING_SCHEMA,
    outputFieldName: STRING_SCHEMA,
    fieldCatalog: STRING_LIST_SCHEMA,
    numericFields: STRING_LIST_SCHEMA,
    styleableFields: STRING_LIST_SCHEMA,
    defaultStyleField: STRING_SCHEMA,
    aggregationOutputs: STRING_MAP_SCHEMA
  }, ['success', 'details', 'dataset', 'outputFieldName', 'fieldCatalog', 'numericFields', 'styleableFields', 'defaultStyleField', 'aggregationOutputs']),
  populateTassellationFromAdminUnitsDiscrete: buildResponseContract({
    ...DEFAULT_RESPONSE_CONTRACT.properties,
    dataset: STRING_SCHEMA,
    outputFieldName: STRING_SCHEMA,
    fieldCatalog: STRING_LIST_SCHEMA,
    numericFields: STRING_LIST_SCHEMA,
    styleableFields: STRING_LIST_SCHEMA,
    defaultStyleField: STRING_SCHEMA,
    aggregationOutputs: STRING_MAP_SCHEMA
  }, ['success', 'details', 'dataset', 'outputFieldName', 'fieldCatalog', 'numericFields', 'styleableFields', 'defaultStyleField', 'aggregationOutputs']),
  clipQMapDatasetByGeometry: buildResponseContract({
    ...DEFAULT_RESPONSE_CONTRACT.properties,
    dataset: STRING_SCHEMA,
    fieldCatalog: STRING_LIST_SCHEMA,
    numericFields: STRING_LIST_SCHEMA,
    styleableFields: STRING_LIST_SCHEMA,
    defaultStyleField: STRING_SCHEMA
  }, ['success', 'details', 'dataset', 'fieldCatalog', 'numericFields', 'styleableFields', 'defaultStyleField']),
  clipDatasetByBoundary: buildResponseContract({
    ...DEFAULT_RESPONSE_CONTRACT.properties,
    dataset: STRING_SCHEMA,
    fieldCatalog: STRING_LIST_SCHEMA,
    numericFields: STRING_LIST_SCHEMA,
    styleableFields: STRING_LIST_SCHEMA,
    defaultStyleField: STRING_SCHEMA
  }, ['success', 'details', 'dataset', 'fieldCatalog', 'numericFields', 'styleableFields', 'defaultStyleField']),
  getQCumberDatasetHelp: buildResponseContract({
    success: BOOLEAN_SCHEMA,
    details: STRING_SCHEMA,
    providerId: STRING_SCHEMA,
    datasetId: STRING_SCHEMA,
    datasetName: STRING_SCHEMA,
    aiHints: {type: 'object', description: 'Field catalog, row count, geometry fields, suggested ops'},
    routing: {type: 'object', description: 'Query routing metadata: queryToolHint.preferredTool, levelFieldCandidates, parentIdFieldCandidates, nameFieldCandidates, metricProfile'},
    metricProfile: {type: 'object', description: 'Metric orchestration: metricSemantic, numeratorFieldCandidates, denominatorFieldCandidates, preferredRankingFieldCandidates, recommendedDerivedMetrics, analysisCaveats'}
  }, ['success', 'details', 'providerId', 'datasetId']),
  listQCumberProviders: buildResponseContract({
    success: BOOLEAN_SCHEMA,
    details: STRING_SCHEMA,
    providers: ARRAY_SCHEMA
  }, ['success', 'details', 'providers']),
  listQCumberDatasets: buildResponseContract({
    success: BOOLEAN_SCHEMA,
    details: STRING_SCHEMA,
    providerId: STRING_SCHEMA,
    datasets: ARRAY_SCHEMA
  }, ['success', 'details', 'providerId', 'datasets'])
};

/**
 * Estrae i nomi dei parametri Zod di primo livello da tutti i tool-builder TypeScript.
 * Strategia:
 *   - Per tool con STRICT_TOOL_ARGS_OVERRIDES: deriva da Object.keys(argsSchema.properties)
 *   - Per gli altri: regex su 'parameters: z.object({...})' nei file TS
 * Ritorna: Map<toolName, string[]>
 */
function extractInputKeysFromBuilders(toolNames, strictOverrides) {
  const result = new Map();

  // 1. Tool con override esplicito: inputKeys dalle properties già dichiarate
  for (const [toolName, schema] of Object.entries(strictOverrides)) {
    const props = schema?.properties ?? {};
    result.set(toolName, Object.keys(props));
  }

  // 2. Tool senza override: regex dai file TS
  const buildersDir = new URL('../src/features/qmap-ai/tool-builders', import.meta.url).pathname;
  const tsFiles = fs.readdirSync(buildersDir).filter(f => f.endsWith('.ts'));

  for (const file of tsFiles) {
    const src = fs.readFileSync(path.join(buildersDir, file), 'utf8');

    // Mappa factory name → tool name
    const toToolName = factory =>
      factory
        .replace(/^create/, '')
        .replace(/Tool$/, '')
        .replace(/^./, c => c.toLowerCase());

    // Raggruppa factory e parameters per blocco: split il file per factory function,
    // poi cerca il primo z.object
    const chunks = src.split(/(?=function create\w+Tool\s*[({])/);
    for (const chunk of chunks) {
      const factoryMatch = /function\s+(create\w+Tool)\s*[({]/.exec(chunk);
      if (!factoryMatch) continue;
      const factory = factoryMatch[1];
      const toolName = toToolName(factory);

      if (!toolNames.includes(toolName)) continue;
      if (result.has(toolName)) continue; // già coperto da override

      const paramsMatch = /parameters\s*:\s*z\.object\(\{([^}]*(?:\{[^}]*\}[^}]*)*)\}/s.exec(chunk);
      if (!paramsMatch) continue;

      const objectBody = paramsMatch[1];
      // Estrai chiavi di primo livello: linee con rientro 2-8 spazi + identificatore + ':'
      const keyRe = /^\s{2,8}([a-zA-Z_]\w*)\s*:/gm;
      const keys = [];
      let m;
      while ((m = keyRe.exec(objectBody)) !== null) {
        keys.push(m[1]);
      }
      result.set(toolName, keys);
    }
  }

  return result;
}

function main() {
  const repoRoot = QMAP_ROOT;
  const manifestPath = path.join(repoRoot, TOOL_MANIFEST_PATH);
  const postValidationPath = path.join(repoRoot, POST_VALIDATION_PATH);
  const outputPath = path.join(repoRoot, OUTPUT_PATH);
  const backendMirrorPath = path.join(repoRoot, BACKEND_MIRROR_PATH);

  const manifest = readJson(manifestPath);
  const postValidationSource = fs.readFileSync(postValidationPath, 'utf8');
  const mutatingTools = new Set(extractMutatingTools(postValidationSource));

  const categories = Array.isArray(manifest?.categories) ? manifest.categories : [];
  const toolToCategories = new Map();
  for (const category of categories) {
    const categoryKey = String(category?.key || '').trim();
    if (!categoryKey) continue;
    for (const tool of normalizeList(category?.tools)) {
      if (!toolToCategories.has(tool)) toolToCategories.set(tool, new Set());
      toolToCategories.get(tool).add(categoryKey);
    }
  }

  const tools = {};
  const allTools = [...toolToCategories.keys()].sort((a, b) => a.localeCompare(b));
  const inputKeysMap = extractInputKeysFromBuilders(allTools, STRICT_TOOL_ARGS_OVERRIDES);
  for (const toolName of allTools) {
    const categoriesForTool = [...(toolToCategories.get(toolName) || [])].sort((a, b) =>
      a.localeCompare(b)
    );
    const strictArgsSchema = STRICT_TOOL_ARGS_OVERRIDES[toolName] || null;
    tools[toolName] = {
      categories: categoriesForTool,
      inputKeys: inputKeysMap.get(toolName) ?? [],
      ...(strictArgsSchema ? {argsSchema: strictArgsSchema} : {}),
      ...(RESPONSE_CONTRACT_OVERRIDES[toolName] ? {responseContract: RESPONSE_CONTRACT_OVERRIDES[toolName]} : {}),
      flags: {
        mutatesDataset: mutatingTools.has(toolName),
        discovery: categoriesForTool.includes('discovery'),
        bridgeOperation: toolName === 'loadData' || toolName === 'saveDataToMap'
      }
    };
  }

  const contracts = {
    schema: 'qmap.tool_contracts.v1',
    version: String(manifest?.version || '').trim() || new Date().toISOString().slice(0, 10),
    source: {
      manifestSchema: String(manifest?.schema || '').trim(),
      manifestVersion: String(manifest?.version || '').trim()
    },
    defaults: {
      argsSchema: {
        type: 'object',
        properties: {},
        additionalProperties: true
      },
      responseContract: DEFAULT_RESPONSE_CONTRACT
    },
    tools
  };

  const serialized = `${JSON.stringify(contracts, null, 2)}\n`;
  fs.mkdirSync(path.dirname(outputPath), {recursive: true});
  fs.writeFileSync(outputPath, serialized);
  fs.writeFileSync(backendMirrorPath, serialized);
  console.log(
    `[tool-contracts] generated tools=${allTools.length} output=${path.relative(repoRoot, outputPath)} mirror=${path.relative(repoRoot, backendMirrorPath)}`
  );
}

main();
