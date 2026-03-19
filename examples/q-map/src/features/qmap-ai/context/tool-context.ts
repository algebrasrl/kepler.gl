import type {MutableRefObject, ComponentType} from 'react';

/**
 * Consolidated dependency context for all q-map AI tool builder factories.
 *
 * Replaces the 15–25 ad-hoc parameters previously passed to each tool factory.
 * Each tool builder receives a single `QMapToolContext` and destructures what it needs.
 *
 * All function properties are typed as `any` to match the existing loose typing across
 * tool builders. This can be tightened incrementally in later phases.
 */
export interface QMapToolContext {
  // ─── Runtime state ──────────────────────────────────────────────────────────
  dispatch: any;
  getCurrentVisState: () => any;
  getCurrentUiState: () => any;
  assistantBaseUrl: string;
  visState: any;
  aiAssistant: any;
  activeMode: any;

  // ─── Refs ────────────────────────────────────────────────────────────────────
  lastRankContextRef: MutableRefObject<any>;

  // ─── Dataset resolution & utilities ──────────────────────────────────────────
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

  // ─── Numeric analysis ────────────────────────────────────────────────────────
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

  // ─── Color & style utilities ─────────────────────────────────────────────────
  parseHexColor: any;
  parseHexColorRgba: any;
  ensureColorRange: any;
  toHex: any;
  buildLinearHexRange: any;
  getNamedPalette: any;
  normalizeThresholds: any;
  getQMapStylePreset: any;

  // ─── Geometry utilities ──────────────────────────────────────────────────────
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

  // ─── Merge utilities ─────────────────────────────────────────────────────────
  normalizeMergeGeometryMode: any;
  normalizeCrsCode: any;
  buildMergeFieldDefinitions: any;
  getMergeDatasetGeometryReadiness: any;
  isGeojsonMergeFieldDefinition: any;
  normalizeGeojsonCellValue: any;
  ensureUniqueMergeFieldName: any;

  // ─── Dataset upsert ──────────────────────────────────────────────────────────
  upsertDerivedDatasetRows: any;
  upsertIntermediateDataset: any;
  getDatasetInfoByLabel: any;
  normalizeDatasetIdSeed: any;
  toWorkerSafeAggregateRows: any;
  getTassellationDatasetInfo: any;
  upsertTassellationDataset: any;
  upsertH3AggregationDataset: any;

  // ─── Tool runtime helpers ────────────────────────────────────────────────────
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

  // ─── Field classification ────────────────────────────────────────────────────
  isLevelLikeField: any;
  isNameLikeField: any;
  isPopulationLikeField: any;
  isCategoricalJoinField: any;
  inferFieldTypeFromValue: any;
  isNumericFieldType: any;

  // ─── MCP / Cloud ─────────────────────────────────────────────────────────────
  callMcpToolParsed: any;
  normalizeCloudMapProvider: any;
  getQMapProvider: any;

  // ─── Zod schemas (canonical names) ───────────────────────────────────────────
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

  // ─── Third-party: Turf ───────────────────────────────────────────────────────
  turfArea: any;
  turfBooleanContains: any;
  turfBooleanIntersects: any;
  turfBooleanPointInPolygon: any;
  turfBooleanTouches: any;
  turfBooleanWithin: any;
  turfBuffer: any;
  turfCentroid: any;
  turfDistance: any;

  // ─── Third-party: H3 ────────────────────────────────────────────────────────
  latLngToCell: any;
  gridDistance: any;
  getResolution: any;
  isValidCell: any;
  cellToLatLng: any;
  cellToBoundary: any;
  gridDisk: any;

  // ─── Third-party: proj4 ─────────────────────────────────────────────────────
  proj4Transform: any;

  // ─── Workers ─────────────────────────────────────────────────────────────────
  runH3Job: any;
  runReprojectJob: any;
  runClipRowsJob: any;
  runZonalStatsJob: any;
  computeClipMetricsForFeature: any;
  aggregateGeometriesToH3Rows: any;

  // ─── H3 paint ────────────────────────────────────────────────────────────────
  getH3PaintDataset: any;
  readH3PaintRows: any;
  upsertH3PaintHex: any;

  // ─── Tool components ─────────────────────────────────────────────────────────
  WordCloudToolComponent: ComponentType<any>;
  CategoryBarsToolComponent: ComponentType<any>;

  // ─── Kepler actions ──────────────────────────────────────────────────────────
  wrapTo: any;
  addDataToMap: any;
  replaceDataInMap: any;
  createOrUpdateFilter: any;
  removeFilter: any;
  interactionConfigChange: any;
  setLoadingIndicator: any;
  loadCloudMap: any;
  fitBounds: any;

  // ─── Kepler constants ────────────────────────────────────────────────────────
  ALL_FIELD_TYPES: any;

  // ─── Config constants ────────────────────────────────────────────────────────
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

  // ─── Inter-tool dependencies (populated during registry build) ───────────────
  clipQMapDatasetByGeometry?: any;
  setQMapLayerColorByThresholds?: any;

  // Allow additional properties for forward compatibility
  [key: string]: any;
}
