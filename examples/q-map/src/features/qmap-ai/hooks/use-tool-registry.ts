import React from 'react';
import {setupLLMTools} from '@kepler.gl/ai-assistant/tools/tools';
import {applyQMapChartToolPolicy} from '../chart-tools';
import {getQMapCloudTools} from '../cloud-tools';
import {applyQMapAiModeToolPolicy} from '../mode-policy';
import {createQMapToolCategoryIntrospectionTools} from '../tool-registry';
import {buildQMapToolsWithoutCategoryIntrospection} from '../tool-groups';
import {buildQMapRuntimeCustomToolGroups, mergeQMapChartStatesWithCustom} from '../runtime-tool-groups';
import {getQMapBaseToolAllowlistSet} from '../tool-manifest';

// ─── Tool builder imports ────────────────────────────────────────────────────
import {createListQMapChartTools} from '../tool-builders/discovery';
import {
  createApplyQMapStylePresetTool,
  createSetQMapLayerColorByStatsThresholdsTool,
  createSetQMapLayerColorByThresholdsTool
} from '../tool-builders/advanced-styling';
import {createSummarizeQMapTimeSeriesTool, createAggregateQMapTimeSeriesTool} from '../tool-builders/timeseries-tools';
import {createWordCloudTool, createCategoryBarsTool, createGrammarAnalyzeTool} from '../tool-builders/chart-tool-builders';
import {
  createDescribeQMapFieldTool,
  createCompositeIndexTool,
  createDataQualityReportTool
} from '../tool-builders/statistical-tool-builders';
import {createCountQMapRowsTool, createDebugQMapActiveFiltersTool} from '../tool-builders/dataset';
import {
  createDistinctQMapFieldValuesTool,
  createPreviewQMapDatasetRowsTool,
  createRankQMapDatasetRowsTool,
  createSearchQMapFieldValuesTool
} from '../tool-builders/dataset-exploration';
import {createSetQMapFieldEqualsFilterTool, createSetQMapTooltipFieldsTool} from '../tool-builders/dataset-ui';
import {createListQMapDatasetsTool, createLoadCloudMapAndWaitTool} from '../tool-builders/runtime-discovery';
import {createClipDatasetByBoundaryTool, createDrawQMapBoundingBoxTool} from '../tool-builders/map-materialization';
import {createTassellateSelectedGeometryTool, createTassellateDatasetLayerTool} from '../tool-builders/tessellation-tools';
import {createAggregateDatasetToH3Tool} from '../tool-builders/h3-aggregation';
import {createPopulateTassellationFromAdminUnitsTool} from '../tool-builders/population-tools';
import {
  createDatasetFromCurrentFiltersTool,
  createDatasetFromFilterTool,
  createMergeQMapDatasetsTool
} from '../tool-builders/dataset-mutations';
import {createDatasetWithGeometryAreaTool} from '../tool-builders/geometry-area';
import {createDatasetWithNormalizedFieldTool} from '../tool-builders/normalized-field';
import {createReprojectQMapDatasetCrsTool} from '../tool-builders/crs-reproject';
import {createComputeQMapDatasetDeltaTool} from '../tool-builders/dataset-delta';
import {createAddComputedFieldTool} from '../tool-builders/computed-field';
import {createPaintQMapH3CellTool, createPaintQMapH3CellsTool, createPaintQMapH3RingTool} from '../tool-builders/h3-paint';
import {
  createSetQMapLayerColorByFieldTool,
  createSetQMapLayerHeightByFieldTool,
  createSetQMapLayerSolidColorTool
} from '../tool-builders/layer-styling';
import {
  createSetQMapLayerOrderTool,
  createSetQMapLayerVisibilityTool,
  createShowOnlyQMapLayerTool
} from '../tool-builders/layer-visibility';
import {createDeriveQMapDatasetBboxTool} from '../tool-builders/spatial';
import {createClipQMapDatasetByGeometryTool} from '../tool-builders/clip-by-geometry';
import {createZonalStatsByAdminTool} from '../tool-builders/zonal-stats';
import {createOverlayDifferenceTool, createSpatialJoinByPredicateTool} from '../tool-builders/spatial-overlays';
import {
  createDissolveQMapDatasetByFieldTool,
  createOverlayIntersectionTool,
  createOverlaySymmetricDifferenceTool,
  createOverlayUnionTool
} from '../tool-builders/constructive-geometry';
import {
  createBufferAndSummarizeTool,
  createEraseQMapDatasetByGeometryTool,
  createSimplifyQMapDatasetGeometryTool,
  createSplitQMapPolygonByLineTool
} from '../tool-builders/geometry-editing';
import {
  createAdjacencyGraphFromPolygonsTool,
  createCoverageQualityReportTool,
  createNearestFeatureJoinTool
} from '../tool-builders/spatial-analysis';
import {createComputeQMapSpatialAutocorrelationTool} from '../tool-builders/autocorrelation';
import {createComputeQMapBivariateCorrelationTool} from '../tool-builders/bivariate-correlation';
import {createComputeQMapHotspotAnalysisTool} from '../tool-builders/hotspot-analysis';
import {createComputeQMapEquityIndicesTool} from '../tool-builders/equity-analysis';
import {
  createRegressQMapFieldsTool,
  createClassifyQMapFieldBreaksTool,
  createCorrelateQMapFieldsTool
} from '../tool-builders/statistical-analysis';
import {
  createCheckRegulatoryComplianceTool,
  createListRegulatoryThresholdsTool
} from '../tool-builders/regulatory-compliance';
import {createAssessPopulationExposureTool} from '../tool-builders/exposure-assessment';
import {createInterpolateIDWTool} from '../tool-builders/spatial-interpolation';
import {
  createFitQMapToDatasetTool,
  createJoinQMapDatasetsOnH3Tool,
  createWaitForQMapDatasetTool
} from '../tool-builders/orchestration';
import {createOpenQMapPanelTool} from '../tool-builders/styling-ui';

import type {QMapToolContext} from '../context/tool-context';

/**
 * Hook that creates the complete q-map tool registry from a QMapToolContext.
 *
 * Replaces ~1,200 LOC of inline tool instantiation that previously lived in
 * qmap-ai-assistant-component.tsx (lines 755–1957).
 */
export function useToolRegistry(ctx: QMapToolContext): Record<string, any> {
  const {visState, aiAssistant, dispatch, activeMode, assistantBaseUrl} = ctx;

  // ─── 1. Instantiate all tool builders from context ──────────────────────────

  const listQMapDatasets = createListQMapDatasetsTool(ctx);
  const deriveQMapDatasetBbox = createDeriveQMapDatasetBboxTool(ctx);
  const previewQMapDatasetRows = createPreviewQMapDatasetRowsTool(ctx);
  const rankQMapDatasetRows = createRankQMapDatasetRowsTool(ctx);
  const distinctQMapFieldValues = createDistinctQMapFieldValuesTool(ctx);
  const searchQMapFieldValues = createSearchQMapFieldValuesTool(ctx);
  const summarizeQMapTimeSeries = createSummarizeQMapTimeSeriesTool(ctx);
  const aggregateQMapTimeSeries = createAggregateQMapTimeSeriesTool(ctx);
  const describeQMapField = createDescribeQMapFieldTool(ctx);
  const computeQMapCompositeIndex = createCompositeIndexTool(ctx);
  const computeQMapDataQualityReport = createDataQualityReportTool(ctx);
  const wordCloudTool = createWordCloudTool(ctx);
  const categoryBarsTool = createCategoryBarsTool(ctx);
  const grammarAnalyzeTool = createGrammarAnalyzeTool(ctx);
  const countQMapRows = createCountQMapRowsTool(ctx);
  const debugQMapActiveFilters = createDebugQMapActiveFiltersTool(ctx);
  const setQMapFieldEqualsFilter = createSetQMapFieldEqualsFilterTool(ctx);
  const setQMapTooltipFields = createSetQMapTooltipFieldsTool(ctx);
  const setQMapLayerColorByField = createSetQMapLayerColorByFieldTool(ctx);
  const setQMapLayerSolidColor = createSetQMapLayerSolidColorTool(ctx);
  const applyQMapStylePreset = createApplyQMapStylePresetTool(ctx);
  const setQMapLayerColorByThresholds = createSetQMapLayerColorByThresholdsTool(ctx);
  const setQMapLayerHeightByField = createSetQMapLayerHeightByFieldTool(ctx);

  // Inter-tool dependency: setQMapLayerColorByStatsThresholds needs setQMapLayerColorByThresholds
  const setQMapLayerColorByStatsThresholds = createSetQMapLayerColorByStatsThresholdsTool({
    ...ctx,
    setQMapLayerColorByThresholds
  });

  const setQMapLayerVisibility = createSetQMapLayerVisibilityTool(ctx);
  const showOnlyQMapLayer = createShowOnlyQMapLayerTool(ctx);
  const setQMapLayerOrder = createSetQMapLayerOrderTool(ctx);
  const createDatasetFromFilter = createDatasetFromFilterTool(ctx);
  const createDatasetWithGeometryArea = createDatasetWithGeometryAreaTool(ctx);
  const createDatasetWithNormalizedField = createDatasetWithNormalizedFieldTool(ctx);
  const createDatasetFromCurrentFilters = createDatasetFromCurrentFiltersTool(ctx);
  const mergeQMapDatasets = createMergeQMapDatasetsTool(ctx);
  const reprojectQMapDatasetCrs = createReprojectQMapDatasetCrsTool(ctx);
  const computeQMapDatasetDelta = createComputeQMapDatasetDeltaTool(ctx);
  const addComputedField = createAddComputedFieldTool(ctx);
  const clipQMapDatasetByGeometry = createClipQMapDatasetByGeometryTool(ctx);

  // Inter-tool dependency: clipDatasetByBoundary needs clipQMapDatasetByGeometry
  const clipDatasetByBoundary = createClipDatasetByBoundaryTool({
    ...ctx,
    clipQMapDatasetByGeometry
  });

  const spatialJoinByPredicate = createSpatialJoinByPredicateTool(ctx);
  const zonalStatsByAdmin = createZonalStatsByAdminTool(ctx);
  const overlayDifference = createOverlayDifferenceTool(ctx);
  const drawQMapBoundingBox = createDrawQMapBoundingBoxTool(ctx);
  const overlayUnion = createOverlayUnionTool(ctx);
  const overlayIntersection = createOverlayIntersectionTool(ctx);
  const overlaySymmetricDifference = createOverlaySymmetricDifferenceTool(ctx);
  const dissolveQMapDatasetByField = createDissolveQMapDatasetByFieldTool(ctx);
  const simplifyQMapDatasetGeometry = createSimplifyQMapDatasetGeometryTool(ctx);
  const splitQMapPolygonByLine = createSplitQMapPolygonByLineTool(ctx);
  const eraseQMapDatasetByGeometry = createEraseQMapDatasetByGeometryTool(ctx);
  const bufferAndSummarize = createBufferAndSummarizeTool(ctx);
  const nearestFeatureJoin = createNearestFeatureJoinTool(ctx);
  const adjacencyGraphFromPolygons = createAdjacencyGraphFromPolygonsTool(ctx);
  const coverageQualityReport = createCoverageQualityReportTool(ctx);
  const computeQMapSpatialAutocorrelation = createComputeQMapSpatialAutocorrelationTool(ctx);
  const computeQMapBivariateCorrelation = createComputeQMapBivariateCorrelationTool(ctx);
  const computeQMapHotspotAnalysis = createComputeQMapHotspotAnalysisTool(ctx);
  const computeQMapEquityIndices = createComputeQMapEquityIndicesTool(ctx);
  const joinQMapDatasetsOnH3 = createJoinQMapDatasetsOnH3Tool(ctx);
  const fitQMapToDataset = createFitQMapToDatasetTool(ctx);
  const tassellateSelectedGeometry = createTassellateSelectedGeometryTool(ctx);
  const tassellateDatasetLayer = createTassellateDatasetLayerTool(ctx);
  const aggregateDatasetToH3 = createAggregateDatasetToH3Tool(ctx);
  const paintQMapH3Cell = createPaintQMapH3CellTool(ctx);
  const paintQMapH3Cells = createPaintQMapH3CellsTool(ctx);
  const paintQMapH3Ring = createPaintQMapH3RingTool(ctx);
  const populateTassellationFromAdminUnits = createPopulateTassellationFromAdminUnitsTool(ctx);
  const loadCloudMapAndWait = createLoadCloudMapAndWaitTool(ctx);
  const waitForQMapDataset = createWaitForQMapDatasetTool(ctx);
  const openQMapPanel = createOpenQMapPanelTool(ctx);
  const regressQMapFields = createRegressQMapFieldsTool(ctx);
  const classifyQMapFieldBreaks = createClassifyQMapFieldBreaksTool(ctx);
  const correlateQMapFields = createCorrelateQMapFieldsTool(ctx);
  const checkRegulatoryCompliance = createCheckRegulatoryComplianceTool(ctx);
  const listRegulatoryThresholds = createListRegulatoryThresholdsTool();
  const assessPopulationExposure = createAssessPopulationExposureTool(ctx);
  const interpolateIDW = createInterpolateIDWTool(ctx);

  // ─── 2. Base tools from kepler.gl AI assistant ──────────────────────────────

  const baseToolsRaw = setupLLMTools({visState, aiAssistant, dispatch}) as Record<string, any>;
  const baseToolAllowlist = React.useMemo(() => getQMapBaseToolAllowlistSet(), []);
  const baseTools = Object.fromEntries(
    Object.entries(baseToolsRaw).filter(([toolName]) => baseToolAllowlist.has(toolName))
  ) as Record<string, any>;

  // ─── 3. Chart policy ────────────────────────────────────────────────────────

  const chartPolicy = applyQMapChartToolPolicy(baseTools, visState?.datasets || {}, (tool: any) => tool);
  const baseToolsWithChartPolicy = chartPolicy.tools;

  // ─── 4. Cloud tools ─────────────────────────────────────────────────────────

  const qMapCloudTools = getQMapCloudTools(assistantBaseUrl);

  // ─── 5. Chart introspection tool ────────────────────────────────────────────

  const listQMapChartTools = createListQMapChartTools({
    mode: chartPolicy.mode,
    timeSeriesEligibility: chartPolicy.timeSeriesEligibility,
    getMergedStates: () => mergeQMapChartStatesWithCustom(chartPolicy.states)
  });

  // ─── 6. Build runtime tool groups ───────────────────────────────────────────

  const customToolGroups = buildQMapRuntimeCustomToolGroups({
    listQMapChartTools,
    listQMapDatasets,
    deriveQMapDatasetBbox,
    previewQMapDatasetRows,
    rankQMapDatasetRows,
    distinctQMapFieldValues,
    searchQMapFieldValues,
    summarizeQMapTimeSeries,
    aggregateQMapTimeSeries,
    describeQMapField,
    wordCloudTool,
    categoryBarsTool,
    grammarAnalyzeTool,
    countQMapRows,
    debugQMapActiveFilters,
    createDatasetFromFilter,
    createDatasetFromCurrentFilters,
    mergeQMapDatasets,
    createDatasetWithGeometryArea,
    createDatasetWithNormalizedField,
    drawQMapBoundingBox,
    reprojectQMapDatasetCrs,
    computeQMapDatasetDelta,
    addComputedField,
    setQMapFieldEqualsFilter,
    setQMapTooltipFields,
    setQMapLayerSolidColor,
    setQMapLayerColorByField,
    setQMapLayerHeightByField,
    applyQMapStylePreset,
    setQMapLayerColorByThresholds,
    setQMapLayerColorByStatsThresholds,
    setQMapLayerVisibility,
    openQMapPanel,
    setQMapLayerOrder,
    showOnlyQMapLayer,
    fitQMapToDataset,
    clipQMapDatasetByGeometry,
    clipDatasetByBoundary,
    eraseQMapDatasetByGeometry,
    spatialJoinByPredicate,
    zonalStatsByAdmin,
    overlayDifference,
    overlayUnion,
    overlayIntersection,
    overlaySymmetricDifference,
    dissolveQMapDatasetByField,
    simplifyQMapDatasetGeometry,
    splitQMapPolygonByLine,
    bufferAndSummarize,
    nearestFeatureJoin,
    adjacencyGraphFromPolygons,
    coverageQualityReport,
    computeQMapSpatialAutocorrelation,
    computeQMapBivariateCorrelation,
    computeQMapHotspotAnalysis,
    computeQMapEquityIndices,
    computeQMapCompositeIndex,
    joinQMapDatasetsOnH3,
    waitForQMapDataset,
    loadCloudMapAndWait,
    tassellateSelectedGeometry,
    tassellateDatasetLayer,
    aggregateDatasetToH3,
    paintQMapH3Cell,
    paintQMapH3Cells,
    paintQMapH3Ring,
    populateTassellationFromAdminUnits,
    computeQMapDataQualityReport,
    regressQMapFields,
    classifyQMapFieldBreaks,
    correlateQMapFields,
    checkRegulatoryCompliance,
    listRegulatoryThresholds,
    assessPopulationExposure,
    interpolateIDW
  });

  // ─── 7. Merge and apply policies ────────────────────────────────────────────

  const toolsWithoutCategoryIntrospection = buildQMapToolsWithoutCategoryIntrospection({
    baseToolsWithChartPolicy,
    qMapCloudTools,
    customToolGroups,
    strict: Boolean(import.meta.env.DEV)
  });

  const modeScopedToolsWithoutCategoryIntrospection = applyQMapAiModeToolPolicy(
    activeMode,
    toolsWithoutCategoryIntrospection
  );

  // ─── 8. Category introspection tools ────────────────────────────────────────

  const {listQMapToolCategories, listQMapToolsByCategory} = createQMapToolCategoryIntrospectionTools({
    toolRegistry: modeScopedToolsWithoutCategoryIntrospection,
    chartStates: chartPolicy.states,
    baseToolsRaw
  });

  const modeScopedToolsWithCategoryIntrospection = applyQMapAiModeToolPolicy(activeMode, {
    ...modeScopedToolsWithoutCategoryIntrospection,
    listQMapToolCategories,
    listQMapToolsByCategory
  });

  return modeScopedToolsWithCategoryIntrospection;
}
