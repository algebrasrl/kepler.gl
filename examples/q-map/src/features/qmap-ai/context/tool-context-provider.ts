import React from 'react';
import {
  addDataToMap,
  createOrUpdateFilter,
  fitBounds,
  interactionConfigChange,
  layerConfigChange,
  loadCloudMap,
  reorderLayer,
  removeFilter,
  replaceDataInMap,
  setLoadingIndicator,
  setSelectedFeature,
  wrapTo
} from '@kepler.gl/actions';
import {ALL_FIELD_TYPES} from '@kepler.gl/constants';
import {
  latLngToCell,
  gridDistance,
  getResolution,
  isValidCell,
  cellToLatLng,
  cellToBoundary,
  gridDisk
} from 'h3-js-v4';
import proj4 from 'proj4';
import {
  area as turfArea,
  booleanContains as turfBooleanContains,
  booleanIntersects as turfBooleanIntersects,
  booleanPointInPolygon as turfBooleanPointInPolygon,
  booleanTouches as turfBooleanTouches,
  booleanWithin as turfBooleanWithin,
  buffer as turfBuffer,
  centroid as turfCentroid,
  distance as turfDistance
} from '@turf/turf';
import {
  QMAP_SORT_DIRECTION_SCHEMA,
  QMAP_COLOR_SCALE_MODE_SCHEMA,
  QMAP_HEIGHT_SCALE_SCHEMA,
  QMAP_THRESHOLD_STRATEGY_SCHEMA,
  QMAP_POSITION_SCHEMA,
  QMAP_GEOMETRY_MODE_SCHEMA,
  QMAP_CLIP_MODE_SCHEMA,
  QMAP_SPATIAL_PREDICATE_SCHEMA,
  QMAP_TOUCH_PREDICATE_SCHEMA,
  QMAP_AGGREGATION_SCHEMA,
  QMAP_AGGREGATION_BASIC_SCHEMA,
  QMAP_AGGREGATION_REQUIRED_SCHEMA,
  QMAP_AGGREGATION_WITH_DISTINCT_REQUIRED_SCHEMA,
  QMAP_WEIGHT_MODE_SCHEMA,
  QMAP_JOIN_TYPE_SCHEMA,
  QMAP_H3_JOIN_METRIC_SCHEMA,
  QMAP_AVG_SUM_SCHEMA,
  QMAP_VALUE_SEMANTICS_SCHEMA,
  QMAP_ALLOCATION_MODE_SCHEMA,
  qMapPaletteSchema,
  buildOptionalLenientEnumSchema
} from '../tool-schema-utils';
import {
  yieldToMainThread,
  resolveOptionalFeatureCap,
  filterIndexesChunked,
  mapIndexesChunked,
  resolveDatasetByName,
  toComparable,
  evaluateFilter,
  filterTargetsDataset,
  getDatasetIndexes,
  getFilteredDatasetIndexes,
  getDatasetFieldNames,
  findDatasetForLayer,
  extractLayerEffectiveFieldNames,
  getTooltipFieldNamesForDataset,
  resolveGeojsonFieldName,
  resolveDatasetFieldName,
  resolveH3FieldName,
  normalizeFieldToken,
  layerReferencesDataset,
  hideLayersForDatasetIds,
  resolveStyleTargetLayer,
  normalizeLayerLabelForGrouping,
  collectDatasetFeaturesForGeometryOps,
  isLikelyLandCoverDataset,
  resolveLandCoverGroupByFields,
  parseHexColor,
  parseHexColorRgba,
  ensureColorRange,
  toHex,
  buildLinearHexRange,
  getNamedPalette,
  normalizeThresholds,
  updateBoundsFromGeometry,
  hasValidBounds,
  boundsOverlap,
  toTurfPolygonFeature,
  toTurfFeature,
  isPolygonLikeFeature,
  turfIntersectSafe,
  turfDifferenceSafe,
  parseCoordinateValue,
  reprojectCoordinateArray,
  reprojectGeoJsonLike,
  normalizeH3Key,
  h3CellToPolygonFeature,
  collectLonLatPairs,
  geometryToBbox,
  geometryBboxOverlap,
  inferFieldTypeFromValue,
  isNumericFieldType,
  isCategoricalJoinField,
  getQMapProvider,
  extractPolygonsFromGeoJsonLike
} from '../dataset-utils';
import {
  isAreaLikeFieldName,
  resolveAreaLikeFieldName,
  summarizeNumericField,
  sampleNumericValues,
  computeThresholdsByStrategy,
  getNumericExtent,
  inferDatasetH3Resolution,
  buildNormalizedDenominatorPlan,
  computeNormalizedDenominatorValue,
  describeNormalizedDenominatorPlan
} from '../numeric-analysis';
import {
  normalizeFieldValueToken,
  normalizeMergeGeometryMode,
  normalizeCrsCode,
  buildMergeFieldDefinitions,
  getMergeDatasetGeometryReadiness,
  isGeojsonMergeFieldDefinition,
  normalizeGeojsonCellValue,
  convertPointToWgs84,
  ensureUniqueMergeFieldName,
  resolveDatasetPointFieldPair
} from '../merge-utils';
import {
  DEFAULT_TASSELLATION_DATASET,
  upsertIntermediateDataset,
  upsertDerivedDatasetRows,
  getDatasetInfoByLabel,
  normalizeDatasetIdSeed,
  toWorkerSafeAggregateRows,
  getTassellationDatasetInfo,
  upsertTassellationDataset,
  upsertH3AggregationDataset
} from '../dataset-upsert';
import {
  DEFAULT_PROVIDER,
  EXECUTED_FILTER_TOOL_SIGNATURES,
  EXECUTED_TOOL_COMPONENT_KEYS,
  shouldUseLoadingIndicator,
  makeExecutionKey,
  rememberExecutedToolComponentKey,
  rememberExecutedFilterToolSignature
} from '../tool-result-normalization';
import {
  shouldSkipToolComponentRun,
  shouldSkipToolComponentByExecutionKey,
  markToolComponentRunCompleted,
  rememberToolComponentExecutionKey
} from '../services/execution-tracking';
import {isLevelLikeField, isNameLikeField, isPopulationLikeField} from '../context-header';
import {callMcpToolParsed} from '../mcp-client';
import {normalizeCloudMapProvider} from '../cloud-tools';
import {getQMapStylePreset} from '../style-presets';
import {
  buildBboxFeature,
  dissolveFeaturesByProperty,
  eraseFeatureByMasks,
  featureAreaM2,
  intersectFeatureSets,
  simplifyAndCleanFeatures,
  splitPolygonFeatureByLine,
  symmetricDifferenceFeatureSets,
  unionFeatures
} from '../geometry-ops';
import {getIntersectingH3Ids, getPolygonsFromGeometry, parseGeoJsonLike} from '../../../geo';
import {runH3Job} from '../../../workers/h3-runner';
import {runReprojectJob} from '../../../workers/reproject-runner';
import {runClipRowsJob} from '../../../workers/clip-runner';
import {runZonalStatsJob} from '../../../workers/zonal-runner';
import {computeClipMetricsForFeature} from '../../../workers/clip-metrics';
import {aggregateGeometriesToH3Rows} from '../../../workers/h3-aggregate-core';
import {
  H3_PAINT_DATASET_ID_PREFIX,
  H3_PAINT_DATASET_LABEL_PREFIX,
  getH3PaintDataset,
  readH3PaintRows,
  upsertH3PaintHex
} from '../../h3-paint/utils';
import {selectQMapUiState, selectQMapVisState} from '../../../state/qmap-selectors';
import {resolveQMapAssistantBaseUrl} from '../../../utils/assistant-config';
import type {QMapToolContext} from './tool-context';

// ─── Module-level constants ──────────────────────────────────────────────────

const QMAP_CLIP_MAX_LOCAL_PAIR_EVAL = Math.max(
  50000,
  Number(import.meta.env.VITE_QMAP_AI_CLIP_MAX_LOCAL_PAIR_EVAL || 750000) || 750000
);
const QMAP_ZONAL_MAX_LOCAL_PAIR_EVAL = Math.max(
  50000,
  Number(import.meta.env.VITE_QMAP_AI_ZONAL_MAX_LOCAL_PAIR_EVAL || 600000) || 600000
);
const QMAP_DEFAULT_CHUNK_SIZE = 250;
const QMAP_AUTO_HIDE_SOURCE_LAYERS =
  String(import.meta.env.VITE_QMAP_AI_AUTO_HIDE_SOURCE_LAYERS || 'true').toLowerCase() !== 'false';
const WAIT_DATASET_RETRY_TRACKER = new Map<
  string,
  {failedAttempts: number; lastFailureAt: number}
>();
const WAIT_DATASET_RETRY_TTL_MS = 5 * 60 * 1000;

export type BuildToolContextInput = {
  dispatch: any;
  store: any;
  visState: any;
  aiAssistant: any;
  aiAssistantConfig: any;
  activeMode: string;
  lastRankContextRef: React.MutableRefObject<any>;
  scheduleMergedMapFit: (dispatchFn: any, bounds: any) => void;
  WordCloudToolComponent: React.ComponentType<any>;
  CategoryBarsToolComponent: React.ComponentType<any>;
};

export function buildQMapToolContext(input: BuildToolContextInput): QMapToolContext {
  const {
    dispatch,
    store,
    visState,
    aiAssistant,
    aiAssistantConfig,
    activeMode,
    lastRankContextRef,
    scheduleMergedMapFit,
    WordCloudToolComponent,
    CategoryBarsToolComponent
  } = input;

  const assistantBaseUrl = resolveQMapAssistantBaseUrl(aiAssistantConfig);
  const getCurrentVisState = () => selectQMapVisState(store.getState());
  const getCurrentUiState = () => selectQMapUiState(store.getState());

  return {
    // Runtime state
    dispatch,
    getCurrentVisState,
    getCurrentUiState,
    assistantBaseUrl,
    visState,
    aiAssistant,
    activeMode,

    // Refs
    lastRankContextRef,

    // Dataset resolution & utilities
    resolveDatasetByName,
    resolveDatasetFieldName,
    resolveGeojsonFieldName,
    resolveH3FieldName,
    getDatasetIndexes,
    getFilteredDatasetIndexes,
    getDatasetFieldNames,
    findDatasetForLayer,
    extractLayerEffectiveFieldNames,
    getTooltipFieldNamesForDataset,
    resolveDatasetPointFieldPair,
    normalizeFieldToken,
    normalizeFieldValueToken,
    filterTargetsDataset,
    toComparable,
    evaluateFilter,
    collectDatasetFeaturesForGeometryOps,
    isLikelyLandCoverDataset,
    resolveLandCoverGroupByFields,
    layerReferencesDataset,
    hideLayersForDatasetIds,
    resolveStyleTargetLayer,
    normalizeLayerLabelForGrouping,
    yieldToMainThread,
    resolveOptionalFeatureCap,
    filterIndexesChunked,
    mapIndexesChunked,

    // Numeric analysis
    isAreaLikeFieldName,
    resolveAreaLikeFieldName,
    summarizeNumericField,
    sampleNumericValues,
    computeThresholdsByStrategy,
    getNumericExtent,
    inferDatasetH3Resolution,
    buildNormalizedDenominatorPlan,
    computeNormalizedDenominatorValue,
    describeNormalizedDenominatorPlan,

    // Color & style utilities
    parseHexColor,
    parseHexColorRgba,
    ensureColorRange,
    toHex,
    buildLinearHexRange,
    getNamedPalette,
    normalizeThresholds,
    getQMapStylePreset,

    // Geometry utilities
    geometryToBbox,
    geometryBboxOverlap,
    updateBoundsFromGeometry,
    hasValidBounds,
    boundsOverlap,
    toTurfPolygonFeature,
    toTurfFeature,
    isPolygonLikeFeature,
    turfIntersectSafe,
    turfDifferenceSafe,
    parseCoordinateValue,
    reprojectCoordinateArray,
    reprojectGeoJsonLike,
    normalizeH3Key,
    h3CellToPolygonFeature,
    collectLonLatPairs,
    parseGeoJsonLike,
    extractPolygonsFromGeoJsonLike,
    getIntersectingH3Ids,
    getPolygonsFromGeometry,
    buildBboxFeature,
    dissolveFeaturesByProperty,
    eraseFeatureByMasks,
    featureAreaM2,
    intersectFeatureSets,
    simplifyAndCleanFeatures,
    splitPolygonFeatureByLine,
    symmetricDifferenceFeatureSets,
    unionFeatures,
    convertPointToWgs84,
    scheduleMergedMapFit,

    // Merge utilities
    normalizeMergeGeometryMode,
    normalizeCrsCode,
    buildMergeFieldDefinitions,
    getMergeDatasetGeometryReadiness,
    isGeojsonMergeFieldDefinition,
    normalizeGeojsonCellValue,
    ensureUniqueMergeFieldName,

    // Dataset upsert
    upsertDerivedDatasetRows,
    upsertIntermediateDataset,
    getDatasetInfoByLabel,
    normalizeDatasetIdSeed,
    toWorkerSafeAggregateRows,
    getTassellationDatasetInfo,
    upsertTassellationDataset,
    upsertH3AggregationDataset,

    // Tool runtime helpers
    makeExecutionKey,
    shouldUseLoadingIndicator,
    EXECUTED_TOOL_COMPONENT_KEYS,
    EXECUTED_FILTER_TOOL_SIGNATURES,
    executedToolComponentKeys: EXECUTED_TOOL_COMPONENT_KEYS,
    executedFilterToolSignatures: EXECUTED_FILTER_TOOL_SIGNATURES,
    rememberExecutedToolComponentKey,
    rememberExecutedFilterToolSignature,
    shouldSkipToolComponentRun,
    shouldSkipToolComponentByExecutionKey,
    markToolComponentRunCompleted,
    rememberToolComponentExecutionKey,

    // Field classification
    isLevelLikeField,
    isNameLikeField,
    isPopulationLikeField,
    isCategoricalJoinField,
    inferFieldTypeFromValue,
    isNumericFieldType,

    // MCP / Cloud
    callMcpToolParsed,
    normalizeCloudMapProvider,
    getQMapProvider,

    // Zod schemas (canonical)
    QMAP_SORT_DIRECTION_SCHEMA,
    QMAP_COLOR_SCALE_MODE_SCHEMA,
    QMAP_HEIGHT_SCALE_SCHEMA,
    QMAP_THRESHOLD_STRATEGY_SCHEMA,
    QMAP_POSITION_SCHEMA,
    QMAP_GEOMETRY_MODE_SCHEMA,
    QMAP_CLIP_MODE_SCHEMA,
    QMAP_SPATIAL_PREDICATE_SCHEMA,
    QMAP_TOUCH_PREDICATE_SCHEMA,
    QMAP_AGGREGATION_SCHEMA,
    QMAP_AGGREGATION_BASIC_SCHEMA,
    QMAP_AGGREGATION_REQUIRED_SCHEMA,
    QMAP_AGGREGATION_WITH_DISTINCT_REQUIRED_SCHEMA,
    QMAP_WEIGHT_MODE_SCHEMA,
    QMAP_JOIN_TYPE_SCHEMA,
    QMAP_H3_JOIN_METRIC_SCHEMA,
    QMAP_AVG_SUM_SCHEMA,
    QMAP_VALUE_SEMANTICS_SCHEMA,
    QMAP_ALLOCATION_MODE_SCHEMA,
    qMapPaletteSchema,
    buildOptionalLenientEnumSchema,
    // Schema aliases
    colorScaleModeSchema: QMAP_COLOR_SCALE_MODE_SCHEMA,
    heightScaleSchema: QMAP_HEIGHT_SCALE_SCHEMA,
    thresholdStrategySchema: QMAP_THRESHOLD_STRATEGY_SCHEMA,
    positionSchema: QMAP_POSITION_SCHEMA,
    geometryModeSchema: QMAP_GEOMETRY_MODE_SCHEMA,
    paletteSchema: qMapPaletteSchema,

    // Third-party: Turf
    turfArea,
    turfBooleanContains,
    turfBooleanIntersects,
    turfBooleanPointInPolygon,
    turfBooleanTouches,
    turfBooleanWithin,
    turfBuffer,
    turfCentroid,
    turfDistance,

    // Third-party: H3
    latLngToCell,
    gridDistance,
    getResolution,
    isValidCell,
    cellToLatLng,
    cellToBoundary,
    gridDisk,

    // Third-party: proj4
    proj4Transform: proj4,

    // Workers
    runH3Job,
    runReprojectJob,
    runClipRowsJob,
    runZonalStatsJob,
    computeClipMetricsForFeature,
    aggregateGeometriesToH3Rows,

    // H3 paint
    getH3PaintDataset,
    readH3PaintRows,
    upsertH3PaintHex,

    // Tool components
    WordCloudToolComponent,
    CategoryBarsToolComponent,

    // Kepler actions
    wrapTo,
    addDataToMap,
    replaceDataInMap,
    createOrUpdateFilter,
    removeFilter,
    interactionConfigChange,
    setLoadingIndicator,
    loadCloudMap,
    fitBounds,

    // Kepler constants
    ALL_FIELD_TYPES,

    // Config constants
    QMAP_DEFAULT_CHUNK_SIZE,
    QMAP_CLIP_MAX_LOCAL_PAIR_EVAL,
    QMAP_ZONAL_MAX_LOCAL_PAIR_EVAL,
    QMAP_AUTO_HIDE_SOURCE_LAYERS,
    DEFAULT_TASSELLATION_DATASET,
    DEFAULT_PROVIDER,
    H3_PAINT_DATASET_LABEL_PREFIX,
    H3_PAINT_DATASET_ID_PREFIX,
    WAIT_DATASET_RETRY_TRACKER,
    WAIT_DATASET_RETRY_TTL_MS,
    // Config aliases
    defaultChunkSize: QMAP_DEFAULT_CHUNK_SIZE,
    paintDatasetLabelPrefix: H3_PAINT_DATASET_LABEL_PREFIX,
    paintDatasetIdPrefix: H3_PAINT_DATASET_ID_PREFIX
  };
}
