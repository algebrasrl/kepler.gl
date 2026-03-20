import type {ComponentType} from 'react';

// ─── Dataset resolution & utilities ──────────────────────────────────────────
export interface QMapDatasetOps {
  resolveDatasetByName: any;
  resolveDatasetFieldName: any;
  resolveGeojsonFieldName: any;
  resolveH3FieldName: any;
  getDatasetIndexes: any;
  getFilteredDatasetIndexes: any;
  getDatasetFieldNames: any;
  findDatasetForLayer: any;
  extractLayerEffectiveFieldNames: any;
  getTooltipFieldNamesForDataset: any;
  resolveDatasetPointFieldPair: any;
  normalizeFieldToken: any;
  normalizeFieldValueToken: any;
  filterTargetsDataset: any;
  toComparable: any;
  evaluateFilter: any;
  collectDatasetFeaturesForGeometryOps: any;
  isLikelyLandCoverDataset: any;
  resolveLandCoverGroupByFields: any;
  layerReferencesDataset: any;
  hideLayersForDatasetIds: any;
  resolveStyleTargetLayer: any;
  normalizeLayerLabelForGrouping: any;
  yieldToMainThread: any;
  resolveOptionalFeatureCap: any;
  filterIndexesChunked: any;
  mapIndexesChunked: any;
}

// ─── Numeric analysis ────────────────────────────────────────────────────────
export interface QMapNumericOps {
  isAreaLikeFieldName: any;
  resolveAreaLikeFieldName: any;
  summarizeNumericField: any;
  sampleNumericValues: any;
  computeThresholdsByStrategy: any;
  getNumericExtent: any;
  inferDatasetH3Resolution: any;
  buildNormalizedDenominatorPlan: any;
  computeNormalizedDenominatorValue: any;
  describeNormalizedDenominatorPlan: any;
}

// ─── Color & style utilities ─────────────────────────────────────────────────
export interface QMapColorOps {
  parseHexColor: any;
  parseHexColorRgba: any;
  ensureColorRange: any;
  toHex: any;
  buildLinearHexRange: any;
  getNamedPalette: any;
  normalizeThresholds: any;
  getQMapStylePreset: any;
}

// ─── Geometry utilities ──────────────────────────────────────────────────────
export interface QMapGeometryOps {
  geometryToBbox: any;
  geometryBboxOverlap: any;
  updateBoundsFromGeometry: any;
  hasValidBounds: any;
  boundsOverlap: any;
  toTurfPolygonFeature: any;
  toTurfFeature: any;
  isPolygonLikeFeature: any;
  turfIntersectSafe: any;
  turfDifferenceSafe: any;
  parseCoordinateValue: any;
  reprojectCoordinateArray: any;
  reprojectGeoJsonLike: any;
  normalizeH3Key: any;
  h3CellToPolygonFeature: any;
  collectLonLatPairs: any;
  parseGeoJsonLike: any;
  extractPolygonsFromGeoJsonLike: any;
  getIntersectingH3Ids: any;
  getPolygonsFromGeometry: any;
  buildBboxFeature: any;
  dissolveFeaturesByProperty: any;
  eraseFeatureByMasks: any;
  featureAreaM2: any;
  intersectFeatureSets: any;
  simplifyAndCleanFeatures: any;
  splitPolygonFeatureByLine: any;
  symmetricDifferenceFeatureSets: any;
  unionFeatures: any;
  convertPointToWgs84: any;
  scheduleMergedMapFit: any;
}

// ─── Merge utilities ─────────────────────────────────────────────────────────
export interface QMapMergeOps {
  normalizeMergeGeometryMode: any;
  normalizeCrsCode: any;
  buildMergeFieldDefinitions: any;
  getMergeDatasetGeometryReadiness: any;
  isGeojsonMergeFieldDefinition: any;
  normalizeGeojsonCellValue: any;
  ensureUniqueMergeFieldName: any;
}

// ─── Dataset upsert ──────────────────────────────────────────────────────────
export interface QMapUpsertOps {
  upsertDerivedDatasetRows: any;
  upsertIntermediateDataset: any;
  getDatasetInfoByLabel: any;
  normalizeDatasetIdSeed: any;
  toWorkerSafeAggregateRows: any;
  getTassellationDatasetInfo: any;
  upsertTassellationDataset: any;
  upsertH3AggregationDataset: any;
}

// ─── Tool runtime helpers ────────────────────────────────────────────────────
export interface QMapToolRuntimeHelpers {
  makeExecutionKey: (prefix: string) => string;
  shouldUseLoadingIndicator: () => boolean;
  EXECUTED_TOOL_COMPONENT_KEYS: Set<string>;
  EXECUTED_FILTER_TOOL_SIGNATURES: Set<string>;
  executedToolComponentKeys: Set<string>;
  executedFilterToolSignatures: Set<string>;
  rememberExecutedToolComponentKey: any;
  rememberExecutedFilterToolSignature: any;
  shouldSkipToolComponentRun: any;
  shouldSkipToolComponentByExecutionKey: any;
  markToolComponentRunCompleted: any;
  rememberToolComponentExecutionKey: any;
}

// ─── Field classification ────────────────────────────────────────────────────
export interface QMapFieldClassification {
  isLevelLikeField: any;
  isNameLikeField: any;
  isPopulationLikeField: any;
  isCategoricalJoinField: any;
  inferFieldTypeFromValue: any;
  isNumericFieldType: any;
}

// ─── MCP / Cloud ─────────────────────────────────────────────────────────────
export interface QMapMcpCloud {
  callMcpToolParsed: any;
  normalizeCloudMapProvider: any;
  getQMapProvider: any;
}

// ─── Zod schemas (canonical names) ───────────────────────────────────────────
export interface QMapZodSchemas {
  QMAP_SORT_DIRECTION_SCHEMA: any;
  QMAP_COLOR_SCALE_MODE_SCHEMA: any;
  QMAP_HEIGHT_SCALE_SCHEMA: any;
  QMAP_THRESHOLD_STRATEGY_SCHEMA: any;
  QMAP_POSITION_SCHEMA: any;
  QMAP_GEOMETRY_MODE_SCHEMA: any;
  QMAP_CLIP_MODE_SCHEMA: any;
  QMAP_SPATIAL_PREDICATE_SCHEMA: any;
  QMAP_TOUCH_PREDICATE_SCHEMA: any;
  QMAP_AGGREGATION_SCHEMA: any;
  QMAP_AGGREGATION_BASIC_SCHEMA: any;
  QMAP_AGGREGATION_REQUIRED_SCHEMA: any;
  QMAP_AGGREGATION_WITH_DISTINCT_REQUIRED_SCHEMA: any;
  QMAP_WEIGHT_MODE_SCHEMA: any;
  QMAP_JOIN_TYPE_SCHEMA: any;
  QMAP_H3_JOIN_METRIC_SCHEMA: any;
  QMAP_AVG_SUM_SCHEMA: any;
  QMAP_VALUE_SEMANTICS_SCHEMA: any;
  QMAP_ALLOCATION_MODE_SCHEMA: any;
  qMapPaletteSchema: any;
  buildOptionalLenientEnumSchema: any;
  // Aliases used by specific tool builders
  colorScaleModeSchema: any;
  heightScaleSchema: any;
  thresholdStrategySchema: any;
  positionSchema: any;
  geometryModeSchema: any;
  paletteSchema: any;
}

// ─── Third-party: Turf ───────────────────────────────────────────────────────
export interface QMapThirdPartyTurf {
  turfArea: any;
  turfBooleanContains: any;
  turfBooleanIntersects: any;
  turfBooleanPointInPolygon: any;
  turfBooleanTouches: any;
  turfBooleanWithin: any;
  turfBuffer: any;
  turfCentroid: any;
  turfDistance: any;
}

// ─── Third-party: H3 ────────────────────────────────────────────────────────
export interface QMapThirdPartyH3 {
  latLngToCell: any;
  gridDistance: any;
  getResolution: any;
  isValidCell: any;
  cellToLatLng: any;
  cellToBoundary: any;
  gridDisk: any;
}

// ─── Third-party: proj4 ─────────────────────────────────────────────────────
export interface QMapThirdPartyProj4 {
  proj4Transform: any;
}

// ─── Workers ─────────────────────────────────────────────────────────────────
export interface QMapWorkerOps {
  runH3Job: any;
  runReprojectJob: any;
  runClipRowsJob: any;
  runZonalStatsJob: any;
  computeClipMetricsForFeature: any;
  aggregateGeometriesToH3Rows: any;
}

// ─── H3 paint ────────────────────────────────────────────────────────────────
export interface QMapH3PaintOps {
  getH3PaintDataset: any;
  readH3PaintRows: any;
  upsertH3PaintHex: any;
}

// ─── Tool components ─────────────────────────────────────────────────────────
export interface QMapToolComponents {
  WordCloudToolComponent: ComponentType<any>;
  CategoryBarsToolComponent: ComponentType<any>;
}

// ─── Kepler actions ──────────────────────────────────────────────────────────
export interface QMapKeplerActions {
  wrapTo: any;
  addDataToMap: any;
  replaceDataInMap: any;
  createOrUpdateFilter: any;
  removeFilter: any;
  interactionConfigChange: any;
  setLoadingIndicator: any;
  loadCloudMap: any;
  fitBounds: any;
}

// ─── Kepler constants ────────────────────────────────────────────────────────
export interface QMapKeplerConstants {
  ALL_FIELD_TYPES: any;
}

// ─── Config constants ────────────────────────────────────────────────────────
export interface QMapConfigConstants {
  QMAP_DEFAULT_CHUNK_SIZE: number;
  QMAP_CLIP_MAX_LOCAL_PAIR_EVAL: number;
  QMAP_ZONAL_MAX_LOCAL_PAIR_EVAL: number;
  QMAP_AUTO_HIDE_SOURCE_LAYERS: boolean;
  DEFAULT_TASSELLATION_DATASET: string;
  DEFAULT_PROVIDER: string;
  H3_PAINT_DATASET_LABEL_PREFIX: string;
  H3_PAINT_DATASET_ID_PREFIX: string;
  WAIT_DATASET_RETRY_TRACKER: Map<string, {failedAttempts: number; lastFailureAt: number}>;
  WAIT_DATASET_RETRY_TTL_MS: number;
  defaultChunkSize: number;
  paintDatasetLabelPrefix: string;
  paintDatasetIdPrefix: string;
}
