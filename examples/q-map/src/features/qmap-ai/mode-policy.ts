import type {QMapMode} from '../../mode/qmap-mode';

type QMapModePromptByLocale = Record<'en' | 'it', Record<QMapMode, string[]>>;

type BuildModePromptOptions = {
  mode: QMapMode;
  locale?: string;
  availableToolNames?: string[];
};

const QMAP_AI_MODE_MINIMUM_TOOL_ALLOWLIST = new Set([
  'listQMapToolCategories',
  'listQMapToolsByCategory',
  'listQMapDatasets',
  'listQCumberProviders',
  'listQCumberDatasets',
  'getQCumberDatasetHelp',
  'queryQCumberDataset',
  'queryQCumberTerritorialUnits',
  'queryQCumberDatasetSpatial',
  'previewQMapDatasetRows',
  'rankQMapDatasetRows',
  'countQMapRows',
  'waitForQMapDataset',
  'debugQMapActiveFilters',
  'fitQMapToDataset',
  'showOnlyQMapLayer',
  'openQMapPanel'
]);

const QMAP_AI_MODE_TOOL_ALLOWLIST: Record<QMapMode, ReadonlySet<string> | null> = {
  kepler: null,
  'draw-stressor': new Set([
    'listQMapToolCategories',
    'listQMapToolsByCategory',
    'listQMapChartTools',
    'listQMapDatasets',
    'listQCumberProviders',
    'listQCumberDatasets',
    'getQCumberDatasetHelp',
    'queryQCumberDataset',
    'queryQCumberTerritorialUnits',
    'queryQCumberDatasetSpatial',
    'previewQMapDatasetRows',
    'rankQMapDatasetRows',
    'distinctQMapFieldValues',
    'searchQMapFieldValues',
    'grammarAnalyzeTool',
    'summarizeQMapTimeSeries',
    'countQMapRows',
    'waitForQMapDataset',
    'debugQMapActiveFilters',
    'setQMapFieldEqualsFilter',
    'fitQMapToDataset',
    'setQMapTooltipFields',
    'setQMapLayerSolidColor',
    'setQMapLayerColorByField',
    'setQMapLayerColorByThresholds',
    'setQMapLayerColorByStatsThresholds',
    'setQMapLayerHeightByField',
    'setQMapLayerVisibility',
    'showOnlyQMapLayer',
    'setQMapLayerOrder',
    'openQMapPanel',
    'drawQMapBoundingBox',
    'createDatasetFromFilter',
    'createDatasetFromCurrentFilters',
    'mergeQMapDatasets',
    'createDatasetWithGeometryArea',
    'createDatasetWithNormalizedField',
    'clipQMapDatasetByGeometry',
    'clipDatasetByBoundary',
    'eraseQMapDatasetByGeometry',
    'spatialJoinByPredicate',
    'zonalStatsByAdmin',
    'overlayDifference',
    'overlayUnion',
    'overlayIntersection',
    'overlaySymmetricDifference',
    'dissolveQMapDatasetByField',
    'simplifyQMapDatasetGeometry',
    'splitQMapPolygonByLine',
    'bufferAndSummarize',
    'nearestFeatureJoin',
    'adjacencyGraphFromPolygons',
    'coverageQualityReport',
    'computeQMapSpatialAutocorrelation',
    'computeQMapEquityIndices',
    'reprojectQMapDatasetCrs',
    'tassellateSelectedGeometry',
    'tassellateDatasetLayer',
    'aggregateDatasetToH3',
    'joinQMapDatasetsOnH3',
    'populateTassellationFromAdminUnits',
    'paintQMapH3Cell',
    'paintQMapH3Cells',
    'paintQMapH3Ring',
    'geocoding',
    'routing',
    'isochrone',
    'roads',
    'computeQMapCompositeIndex',
    'computeQMapDataQualityReport'
  ]),
  'draw-on-map': new Set([
    'listQMapToolCategories',
    'listQMapToolsByCategory',
    'listQMapDatasets',
    'countQMapRows',
    'waitForQMapDataset',
    'fitQMapToDataset',
    'setQMapLayerSolidColor',
    'setQMapLayerColorByField',
    'setQMapLayerColorByThresholds',
    'setQMapLayerColorByStatsThresholds',
    'setQMapLayerHeightByField',
    'setQMapLayerVisibility',
    'showOnlyQMapLayer',
    'setQMapLayerOrder',
    'openQMapPanel',
    'drawQMapBoundingBox',
    'clipQMapDatasetByGeometry',
    'clipDatasetByBoundary',
    'eraseQMapDatasetByGeometry',
    'overlayDifference',
    'overlayUnion',
    'overlayIntersection',
    'overlaySymmetricDifference',
    'dissolveQMapDatasetByField',
    'simplifyQMapDatasetGeometry',
    'splitQMapPolygonByLine',
    'tassellateSelectedGeometry',
    'tassellateDatasetLayer',
    'aggregateDatasetToH3',
    'joinQMapDatasetsOnH3',
    'paintQMapH3Cell',
    'paintQMapH3Cells',
    'paintQMapH3Ring'
  ]),
  geotoken: new Set([
    'listQMapToolCategories',
    'listQMapToolsByCategory',
    'listQMapDatasets',
    'countQMapRows',
    'waitForQMapDataset',
    'fitQMapToDataset',
    'setQMapLayerSolidColor',
    'setQMapLayerColorByField',
    'setQMapLayerColorByThresholds',
    'setQMapLayerColorByStatsThresholds',
    'setQMapLayerHeightByField',
    'setQMapLayerVisibility',
    'showOnlyQMapLayer',
    'setQMapLayerOrder',
    'openQMapPanel',
    'drawQMapBoundingBox',
    'clipQMapDatasetByGeometry',
    'clipDatasetByBoundary',
    'eraseQMapDatasetByGeometry',
    'overlayDifference',
    'overlayUnion',
    'overlayIntersection',
    'overlaySymmetricDifference',
    'dissolveQMapDatasetByField',
    'simplifyQMapDatasetGeometry',
    'splitQMapPolygonByLine',
    'tassellateSelectedGeometry',
    'tassellateDatasetLayer',
    'aggregateDatasetToH3',
    'joinQMapDatasetsOnH3',
    'paintQMapH3Cell',
    'paintQMapH3Cells',
    'paintQMapH3Ring'
  ])
};

const MODE_PROMPT_LINES: QMapModePromptByLocale = {
  en: {
    kepler: [
      '[MODE] Current mode is kepler (full capability). Use complete q-map workflows, including discovery, analytics, styling, and persistence when requested.',
      '[MODE] Keep execution deterministic: pick the minimal valid tool chain and stop after objective completion.'
    ],
    'draw-stressor': [
      '[MODE] Current mode is draw-stressor. Prioritize geometry drafting and stressor-focused analysis; when required, use q-cumber discovery/query tools to load administrative/thematic context.',
      '[MODE] Keep responses concise and execution deterministic; use only the minimum tool chain needed for the requested outcome.'
    ],
    'draw-on-map': [
      '[MODE] Current mode is draw-on-map. Focus on local draw/tessellation/H3 paint and lightweight inspection/styling of loaded data.',
      '[MODE] Keep responses concise and execution deterministic; use only the minimum tool chain needed for the requested outcome.'
    ],
    geotoken: [
      '[MODE] Current mode is geotoken. Focus on perimeter drawing and H3 tessellation needed to reserve geotokens on the selected area.',
      '[MODE] Keep responses concise and execution deterministic; use only the minimum tool chain needed for the requested outcome.'
    ]
  },
  it: {
    kepler: [
      '[MODE] Modalita corrente: kepler (capabilita completa). Usa workflow q-map completi, inclusi discovery, analisi, styling e persistenza quando richiesto.',
      '[MODE] Mantieni esecuzione deterministica: scegli la catena tool minima valida e fermati appena l obiettivo e completato.'
    ],
    'draw-stressor': [
      '[MODE] Modalita corrente: draw-stressor. Dai priorita a disegno geometrie e analisi stressor; quando serve usa i tool di discovery/query q-cumber per caricare contesto amministrativo/tematico.',
      '[MODE] Mantieni risposte concise ed esecuzione deterministica; usa solo la catena minima di tool necessaria al risultato richiesto.'
    ],
    'draw-on-map': [
      '[MODE] Modalita corrente: draw-on-map. Concentrati su disegno locale, tassellazione/H3 paint e ispezione/styling leggero dei dati caricati.',
      '[MODE] Mantieni risposte concise ed esecuzione deterministica; usa solo la catena minima di tool necessaria al risultato richiesto.'
    ],
    geotoken: [
      '[MODE] Modalita corrente: geotoken. Concentrati su disegno del perimetro e tassellazione H3 necessari a riservare geotoken sull area selezionata.',
      '[MODE] Mantieni risposte concise ed esecuzione deterministica; usa solo la catena minima di tool necessaria al risultato richiesto.'
    ]
  }
};

function normalizeLocale(locale: string | undefined): 'en' | 'it' {
  return String(locale || '')
    .toLowerCase()
    .startsWith('it')
    ? 'it'
    : 'en';
}

function dedupeLines(lines: string[]): string[] {
  const seen = new Set<string>();
  return lines.filter(rawLine => {
    const line = String(rawLine || '').trim();
    if (!line || seen.has(line)) return false;
    seen.add(line);
    return true;
  });
}

export function applyQMapAiModeToolPolicy(
  mode: QMapMode,
  toolRegistry: Record<string, unknown>
): Record<string, unknown> {
  const allowlist = QMAP_AI_MODE_TOOL_ALLOWLIST[mode];
  if (!allowlist) return toolRegistry;
  const effectiveAllowlist = new Set<string>(allowlist);
  QMAP_AI_MODE_MINIMUM_TOOL_ALLOWLIST.forEach(toolName => effectiveAllowlist.add(toolName));
  return Object.fromEntries(Object.entries(toolRegistry || {}).filter(([name]) => effectiveAllowlist.has(name)));
}

export function buildQMapAiModePromptOverlay({
  mode,
  locale = 'en',
  availableToolNames = []
}: BuildModePromptOptions): string {
  const lang = normalizeLocale(locale);
  const baseLines = MODE_PROMPT_LINES[lang][mode] || MODE_PROMPT_LINES[lang].kepler;
  const lines = [...baseLines];

  if (mode !== 'kepler' && QMAP_AI_MODE_TOOL_ALLOWLIST[mode] && availableToolNames.length) {
    const sortedNames = [...new Set(availableToolNames.map(name => String(name || '').trim()).filter(Boolean))].sort();
    if (sortedNames.length) {
      lines.push(
        lang === 'it'
          ? `[MODE][TOOLS] Tool consentiti in questa modalita: ${sortedNames.join(', ')}.`
          : `[MODE][TOOLS] Allowed tools in this mode: ${sortedNames.join(', ')}.`
      );
    }
  }

  return dedupeLines(lines).join(' ');
}
