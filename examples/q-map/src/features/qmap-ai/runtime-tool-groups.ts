import type {QMapChartToolState} from './chart-tools';
import type {QMapToolGroups} from './tool-groups';

export const QMAP_CUSTOM_CHART_STATES: QMapChartToolState[] = [
  {
    key: 'wordCloudTool',
    label: 'Word Cloud',
    available: true,
    enabled: true,
    reason: 'q-map custom chart tool'
  },
  {
    key: 'categoryBarsTool',
    label: 'Category Bars',
    available: true,
    enabled: true,
    reason: 'q-map custom chart tool'
  }
];

export function mergeQMapChartStatesWithCustom(chartStates: QMapChartToolState[]): QMapChartToolState[] {
  const mergedStates: QMapChartToolState[] = [...(chartStates || [])];
  QMAP_CUSTOM_CHART_STATES.forEach(state => {
    if (!mergedStates.some(item => item.key === state.key)) {
      mergedStates.push(state);
    }
  });
  return mergedStates;
}

type QMapRuntimeToolInput = {
  listQMapChartTools: any;
  listQMapDatasets: any;
  deriveQMapDatasetBbox: any;
  previewQMapDatasetRows: any;
  rankQMapDatasetRows: any;
  distinctQMapFieldValues: any;
  searchQMapFieldValues: any;
  summarizeQMapTimeSeries: any;
  wordCloudTool: any;
  categoryBarsTool: any;
  grammarAnalyzeTool: any;
  countQMapRows: any;
  debugQMapActiveFilters: any;
  createDatasetFromFilter: any;
  createDatasetFromCurrentFilters: any;
  mergeQMapDatasets: any;
  createDatasetWithGeometryArea: any;
  createDatasetWithNormalizedField: any;
  drawQMapBoundingBox: any;
  reprojectQMapDatasetCrs: any;
  setQMapFieldEqualsFilter: any;
  setQMapTooltipFields: any;
  setQMapLayerSolidColor: any;
  setQMapLayerColorByField: any;
  setQMapLayerHeightByField: any;
  applyQMapStylePreset: any;
  setQMapLayerColorByThresholds: any;
  setQMapLayerColorByStatsThresholds: any;
  setQMapLayerVisibility: any;
  openQMapPanel: any;
  setQMapLayerOrder: any;
  showOnlyQMapLayer: any;
  fitQMapToDataset: any;
  clipQMapDatasetByGeometry: any;
  clipDatasetByBoundary: any;
  eraseQMapDatasetByGeometry: any;
  spatialJoinByPredicate: any;
  zonalStatsByAdmin: any;
  overlayDifference: any;
  overlayUnion: any;
  overlayIntersection: any;
  overlaySymmetricDifference: any;
  dissolveQMapDatasetByField: any;
  simplifyQMapDatasetGeometry: any;
  splitQMapPolygonByLine: any;
  bufferAndSummarize: any;
  nearestFeatureJoin: any;
  adjacencyGraphFromPolygons: any;
  coverageQualityReport: any;
  computeQMapSpatialAutocorrelation: any;
  computeQMapEquityIndices: any;
  joinQMapDatasetsOnH3: any;
  waitForQMapDataset: any;
  loadCloudMapAndWait: any;
  tassellateSelectedGeometry: any;
  tassellateDatasetLayer: any;
  aggregateDatasetToH3: any;
  paintQMapH3Cell: any;
  paintQMapH3Cells: any;
  paintQMapH3Ring: any;
  populateTassellationFromAdminUnits: any;
  aggregateQMapTimeSeries: any;
  describeQMapField: any;
  computeQMapBivariateCorrelation: any;
  computeQMapDatasetDelta: any;
  addComputedField: any;
  computeQMapHotspotAnalysis: any;
  computeQMapCompositeIndex: any;
  computeQMapDataQualityReport: any;
  regressQMapFields: any;
  classifyQMapFieldBreaks: any;
  correlateQMapFields: any;
  checkRegulatoryCompliance: any;
  listRegulatoryThresholds: any;
  assessPopulationExposure: any;
  interpolateIDW: any;
};

export function buildQMapRuntimeCustomToolGroups(tools: QMapRuntimeToolInput): QMapToolGroups {
  return {
    discovery: {
      listQMapChartTools: tools.listQMapChartTools,
      listQMapDatasets: tools.listQMapDatasets,
      deriveQMapDatasetBbox: tools.deriveQMapDatasetBbox,
      previewQMapDatasetRows: tools.previewQMapDatasetRows,
      rankQMapDatasetRows: tools.rankQMapDatasetRows,
      distinctQMapFieldValues: tools.distinctQMapFieldValues,
      searchQMapFieldValues: tools.searchQMapFieldValues,
      summarizeQMapTimeSeries: tools.summarizeQMapTimeSeries,
      grammarAnalyzeTool: tools.grammarAnalyzeTool,
      aggregateQMapTimeSeries: tools.aggregateQMapTimeSeries,
      describeQMapField: tools.describeQMapField,
      computeQMapDataQualityReport: tools.computeQMapDataQualityReport,
      correlateQMapFields: tools.correlateQMapFields,
      listRegulatoryThresholds: tools.listRegulatoryThresholds
    },
    datasetOps: {
      wordCloudTool: tools.wordCloudTool,
      categoryBarsTool: tools.categoryBarsTool,
      countQMapRows: tools.countQMapRows,
      debugQMapActiveFilters: tools.debugQMapActiveFilters,
      createDatasetFromFilter: tools.createDatasetFromFilter,
      createDatasetFromCurrentFilters: tools.createDatasetFromCurrentFilters,
      mergeQMapDatasets: tools.mergeQMapDatasets,
      createDatasetWithGeometryArea: tools.createDatasetWithGeometryArea,
      createDatasetWithNormalizedField: tools.createDatasetWithNormalizedField,
      drawQMapBoundingBox: tools.drawQMapBoundingBox,
      reprojectQMapDatasetCrs: tools.reprojectQMapDatasetCrs,
      computeQMapDatasetDelta: tools.computeQMapDatasetDelta,
      addComputedField: tools.addComputedField,
      regressQMapFields: tools.regressQMapFields,
      classifyQMapFieldBreaks: tools.classifyQMapFieldBreaks,
      checkRegulatoryCompliance: tools.checkRegulatoryCompliance,
      assessPopulationExposure: tools.assessPopulationExposure
    },
    stylingUi: {
      setQMapFieldEqualsFilter: tools.setQMapFieldEqualsFilter,
      setQMapTooltipFields: tools.setQMapTooltipFields,
      setQMapLayerSolidColor: tools.setQMapLayerSolidColor,
      setQMapLayerColorByField: tools.setQMapLayerColorByField,
      setQMapLayerHeightByField: tools.setQMapLayerHeightByField,
      applyQMapStylePreset: tools.applyQMapStylePreset,
      setQMapLayerColorByThresholds: tools.setQMapLayerColorByThresholds,
      setQMapLayerColorByStatsThresholds: tools.setQMapLayerColorByStatsThresholds,
      setQMapLayerVisibility: tools.setQMapLayerVisibility,
      openQMapPanel: tools.openQMapPanel,
      setQMapLayerOrder: tools.setQMapLayerOrder,
      showOnlyQMapLayer: tools.showOnlyQMapLayer,
      fitQMapToDataset: tools.fitQMapToDataset
    },
    spatialAnalysis: {
      clipQMapDatasetByGeometry: tools.clipQMapDatasetByGeometry,
      clipDatasetByBoundary: tools.clipDatasetByBoundary,
      eraseQMapDatasetByGeometry: tools.eraseQMapDatasetByGeometry,
      spatialJoinByPredicate: tools.spatialJoinByPredicate,
      zonalStatsByAdmin: tools.zonalStatsByAdmin,
      overlayDifference: tools.overlayDifference,
      overlayUnion: tools.overlayUnion,
      overlayIntersection: tools.overlayIntersection,
      overlaySymmetricDifference: tools.overlaySymmetricDifference,
      dissolveQMapDatasetByField: tools.dissolveQMapDatasetByField,
      simplifyQMapDatasetGeometry: tools.simplifyQMapDatasetGeometry,
      splitQMapPolygonByLine: tools.splitQMapPolygonByLine,
      bufferAndSummarize: tools.bufferAndSummarize,
      nearestFeatureJoin: tools.nearestFeatureJoin,
      adjacencyGraphFromPolygons: tools.adjacencyGraphFromPolygons,
      coverageQualityReport: tools.coverageQualityReport,
      computeQMapSpatialAutocorrelation: tools.computeQMapSpatialAutocorrelation,
      computeQMapEquityIndices: tools.computeQMapEquityIndices,
      computeQMapBivariateCorrelation: tools.computeQMapBivariateCorrelation,
      computeQMapHotspotAnalysis: tools.computeQMapHotspotAnalysis,
      computeQMapCompositeIndex: tools.computeQMapCompositeIndex,
      interpolateIDW: tools.interpolateIDW
    },
    h3Processing: {
      joinQMapDatasetsOnH3: tools.joinQMapDatasetsOnH3,
      waitForQMapDataset: tools.waitForQMapDataset,
      loadCloudMapAndWait: tools.loadCloudMapAndWait,
      tassellateSelectedGeometry: tools.tassellateSelectedGeometry,
      tassellateDatasetLayer: tools.tassellateDatasetLayer,
      aggregateDatasetToH3: tools.aggregateDatasetToH3,
      paintQMapH3Cell: tools.paintQMapH3Cell,
      paintQMapH3Cells: tools.paintQMapH3Cells,
      paintQMapH3Ring: tools.paintQMapH3Ring,
      populateTassellationFromAdminUnits: tools.populateTassellationFromAdminUnits
    }
  };
}
