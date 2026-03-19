/**
 * Barrel re-export for dataset utilities.
 *
 * Actual implementations live in:
 *   - utils/dataset-resolve.ts — dataset resolution, lineage, field resolution
 *   - utils/geometry-ops.ts — async chunking, geometry, bounds, coordinate ops
 *   - utils/dataset-metadata.ts — color, layer queries, field classification
 *
 * All existing import sites continue to use `from './dataset-utils'` unchanged.
 */
export {
  normalizeDatasetLookupToken,
  normalizeCanonicalDatasetRef,
  registerDatasetLineageAlias,
  syncDatasetLineageFromCurrentDatasets,
  resolveCanonicalDatasetRefWithLineage,
  extractProducedDatasetRefsFromNormalizedResult,
  updateDatasetLineageFromToolResult,
  findDatasetCandidatesByName,
  normalizeToolDetails,
  normalizeMessageList,
  toCanonicalDatasetRef,
  dedupeNonEmpty,
  extractProducedDatasetRefs,
  getQMapProvider,
  resolveDatasetByName,
  toComparable,
  evaluateFilter,
  filterTargetsDataset,
  resolveGeojsonFieldName,
  getFilteredDatasetIndexes,
  resolveDatasetFieldName,
  resolveH3FieldName,
  normalizeH3Key,
  h3CellToPolygonFeature,
  getDatasetIndexes,
} from './utils/dataset-resolve';

export {
  resolveOptionalFeatureCap,
  yieldToMainThread,
  filterIndexesChunked,
  mapIndexesChunked,
  extractPolygonsFromGeoJsonLike,
  normalizeThresholds,
  updateBoundsFromGeometry,
  hasValidBounds,
  boundsOverlap,
  type LngLat,
  toTurfPolygonFeature,
  toTurfFeature,
  isPolygonLikeFeature,
  collectDatasetFeaturesForGeometryOps,
  turfIntersectSafe,
  turfDifferenceSafe,
  type ClipFeatureDiagnosticsInput,
  parseCoordinateValue,
  reprojectCoordinateArray,
  reprojectGeoJsonLike,
  collectLonLatPairs,
  geometryToBbox,
  geometryBboxOverlap
} from './utils/geometry-ops';

export {
  SAFE_COLOR_RANGE,
  parseHexColor,
  parseHexColorRgba,
  ensureColorRange,
  toHex,
  buildLinearHexRange,
  getNamedPalette,
  inferFieldTypeFromValue,
  isNumericFieldType,
  isCategoricalJoinField,
  getDatasetFieldNames,
  normalizeFieldToken,
  isLikelyLandCoverDataset,
  resolveLandCoverGroupByFields,
  findDatasetForLayer,
  getTooltipFieldNamesForDataset,
  extractLayerEffectiveFieldNames,
  layerReferencesDataset,
  hideLayersForDatasetIds,
  resolveStyleTargetLayer,
  normalizeLayerLabelForGrouping,
  type QMapRuntimeStep,
  type QMapRankContext
} from './utils/dataset-metadata';
