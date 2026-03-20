/**
 * Barrel re-export for utility modules.
 *
 * Actual implementations:
 *   - utils/dataset-resolve.ts — dataset resolution, lineage, field resolution
 *   - utils/geometry-ops.ts — async chunking, geometry, bounds, coordinate ops
 *   - utils/dataset-metadata.ts — color, layer queries, field classification
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
  extractPolygonsFromGeoJsonLike
} from './dataset-resolve';

export {
  resolveOptionalFeatureCap,
  yieldToMainThread,
  filterIndexesChunked,
  mapIndexesChunked,
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
} from './geometry-ops';

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
} from './dataset-metadata';
