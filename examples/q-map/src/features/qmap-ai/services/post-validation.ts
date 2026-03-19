/**
 * Post-validation: mutation tool set, dataset name resolution, validation timeout.
 */
import {H3_PAINT_DATASET_LABEL_PREFIX} from '../../h3-paint/utils';

const DEFAULT_TASSELLATION_DATASET = 'Tassellation';

export const DATASET_VALIDATION_MUTATING_TOOLS = new Set([
  'tassellateSelectedGeometry',
  'tassellateDatasetLayer',
  'aggregateDatasetToH3',
  'joinQMapDatasetsOnH3',
  'populateTassellationFromAdminUnits',
  'createDatasetFromFilter',
  'createDatasetFromCurrentFilters',
  'createDatasetWithGeometryArea',
  'createDatasetWithNormalizedField',
  'mergeQMapDatasets',
  'reprojectQMapDatasetCrs',
  'clipQMapDatasetByGeometry',
  'clipDatasetByBoundary',
  'overlayDifference',
  'overlayUnion',
  'overlayIntersection',
  'overlaySymmetricDifference',
  'spatialJoinByPredicate',
  'zonalStatsByAdmin',
  'bufferAndSummarize',
  'nearestFeatureJoin',
  'adjacencyGraphFromPolygons',
  'computeQMapSpatialAutocorrelation',
  'computeQMapBivariateCorrelation',
  'computeQMapEquityIndices',
  'computeQMapDatasetDelta',
  'addComputedField',
  'computeQMapHotspotAnalysis',
  'computeQMapCompositeIndex',
  'dissolveQMapDatasetByField',
  'simplifyQMapDatasetGeometry',
  'splitQMapPolygonByLine',
  'eraseQMapDatasetByGeometry',
  'drawQMapBoundingBox',
  'paintQMapH3Cell',
  'paintQMapH3Cells',
  'paintQMapH3Ring'
]);

function isDatasetValidationTool(toolName: string): boolean {
  return toolName === 'waitForQMapDataset' || toolName === 'countQMapRows';
}

export function shouldRunDatasetPostValidation(toolName: string): boolean {
  return DATASET_VALIDATION_MUTATING_TOOLS.has(toolName) && !isDatasetValidationTool(toolName);
}

export function firstNonEmptyString(...values: unknown[]): string {
  for (const value of values) {
    const text = String(value || '').trim();
    if (text) return text;
  }
  return '';
}

function resolveDatasetNameFromResultPayload(payload: Record<string, unknown> | null): string {
  if (!payload) return '';
  const directDatasetId = firstNonEmptyString(
    payload.datasetId,
    payload.outputDatasetId,
    payload.newDatasetId,
    payload.targetDatasetId,
    payload.joinedDatasetId,
    payload.aggregateDatasetId,
    payload.resultDatasetId,
    payload.materializedDatasetId,
    payload.intermediateDatasetId,
    payload.tessellationDatasetId,
    payload.tassellationDatasetId
  );
  if (directDatasetId) return `id:${directDatasetId}`;

  const direct = firstNonEmptyString(
    payload.dataset,
    payload.datasetName,
    payload.outputDatasetName,
    payload.newDatasetName,
    payload.targetDatasetName,
    payload.targetDataset,
    payload.joinedDatasetName,
    payload.aggregateDatasetName,
    payload.resultDataset,
    payload.materializedDataset,
    payload.intermediateDataset,
    payload.tessellationDatasetName,
    payload.tassellationDatasetName
  );
  if (direct) return direct;

  const nestedLlm =
    payload.llmResult && typeof payload.llmResult === 'object' && !Array.isArray(payload.llmResult)
      ? (payload.llmResult as Record<string, unknown>)
      : null;
  if (!nestedLlm) return '';

  const nestedDatasetId = firstNonEmptyString(
    nestedLlm.datasetId,
    nestedLlm.outputDatasetId,
    nestedLlm.newDatasetId,
    nestedLlm.targetDatasetId,
    nestedLlm.joinedDatasetId,
    nestedLlm.aggregateDatasetId,
    nestedLlm.resultDatasetId,
    nestedLlm.materializedDatasetId,
    nestedLlm.intermediateDatasetId,
    nestedLlm.tessellationDatasetId,
    nestedLlm.tassellationDatasetId
  );
  if (nestedDatasetId) return `id:${nestedDatasetId}`;

  return firstNonEmptyString(
    nestedLlm.dataset,
    nestedLlm.datasetName,
    nestedLlm.outputDatasetName,
    nestedLlm.newDatasetName,
    nestedLlm.targetDatasetName,
    nestedLlm.targetDataset,
    nestedLlm.joinedDatasetName,
    nestedLlm.aggregateDatasetName,
    nestedLlm.resultDataset,
    nestedLlm.materializedDataset,
    nestedLlm.intermediateDataset,
    nestedLlm.tessellationDatasetName,
    nestedLlm.tassellationDatasetName
  );
}

export function resolveDatasetNameForPostValidation(
  toolName: string,
  args: Record<string, unknown>,
  normalizedResult: Record<string, unknown>
): string {
  const fromResult = resolveDatasetNameFromResultPayload(normalizedResult);
  if (fromResult) return fromResult;

  const fromArgs = firstNonEmptyString(
    args.newDatasetName,
    args.targetDatasetName,
    args.datasetName,
    args.outputDatasetName,
    args.tessellationDatasetName,
    args.tassellationDatasetName
  );
  if (fromArgs) return fromArgs;

  if (toolName === 'tassellateSelectedGeometry' || toolName === 'tassellateDatasetLayer') {
    return String(args.targetDatasetName || DEFAULT_TASSELLATION_DATASET);
  }

  if (toolName === 'paintQMapH3Cell' || toolName === 'paintQMapH3Cells' || toolName === 'paintQMapH3Ring') {
    const resolution = Math.max(3, Math.min(11, Number(args.resolution || 7)));
    return `${H3_PAINT_DATASET_LABEL_PREFIX}${resolution}`;
  }

  return '';
}

export function resolveValidationTimeoutMs(toolName: string): number {
  const normalizedToolName = String(toolName || '').toLowerCase();
  if (
    normalizedToolName.includes('aggregate') ||
    normalizedToolName.includes('populate') ||
    normalizedToolName.includes('join') ||
    normalizedToolName.includes('tassell') ||
    normalizedToolName.includes('clip') ||
    normalizedToolName.includes('overlay') ||
    normalizedToolName.includes('spatialjoin') ||
    normalizedToolName.includes('bivariate') ||
    normalizedToolName.includes('delta') ||
    normalizedToolName.includes('hotspot')
  ) {
    return 180000;
  }
  return 90000;
}
