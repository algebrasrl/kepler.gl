import type {
  QMapAiAssistantConfig,
  QMapAiAssistantState,
  QMapAiPanelState,
  QMapDrawState,
  QMapH3PaintState,
  QMapKeplerMapState,
  QMapRootState,
  QMapUiState
} from './qmap-state-types';

export function selectQMapKeplerMapState(state: QMapRootState): QMapKeplerMapState | null {
  return state?.demo?.keplerGl?.map || null;
}

export function selectQMapVisState(state: QMapRootState): any {
  return selectQMapKeplerMapState(state)?.visState || null;
}

export function selectQMapUiState(state: QMapRootState): QMapUiState | null {
  return selectQMapKeplerMapState(state)?.uiState || null;
}

export function selectQMapDatasets(state: QMapRootState): Record<string, any> {
  return selectQMapVisState(state)?.datasets || {};
}

export function selectQMapLayers(state: QMapRootState): any[] {
  const layers = selectQMapVisState(state)?.layers;
  return Array.isArray(layers) ? layers : [];
}

export function selectQMapEditorFeatures(state: QMapRootState): any[] {
  const features = selectQMapVisState(state)?.editor?.features;
  return Array.isArray(features) ? features : [];
}

/** Stable sub-selectors — return the same reference when the sub-tree hasn't changed. */
export function selectQMapDatasetsObj(state: QMapRootState): Record<string, any> {
  return selectQMapVisState(state)?.datasets || _EMPTY_DATASETS;
}
export function selectQMapLayersArray(state: QMapRootState): any[] {
  const layers = selectQMapVisState(state)?.layers;
  return Array.isArray(layers) ? layers : _EMPTY_LAYERS;
}
export function selectQMapFiltersArray(state: QMapRootState): any[] {
  const filters = selectQMapVisState(state)?.filters;
  return Array.isArray(filters) ? filters : _EMPTY_FILTERS;
}
export function selectQMapMapState(state: QMapRootState): any {
  return selectQMapKeplerMapState(state)?.mapState || null;
}
const _EMPTY_DATASETS: Record<string, any> = {};
const _EMPTY_LAYERS: any[] = [];
const _EMPTY_FILTERS: any[] = [];

export function selectQMapFilters(state: QMapRootState): any[] {
  const filters = selectQMapVisState(state)?.filters;
  return Array.isArray(filters) ? filters : [];
}

export function selectQMapAiAssistantState(state: QMapRootState): QMapAiAssistantState | null {
  return state?.demo?.aiAssistant || null;
}

export function selectQMapAiAssistantConfig(state: QMapRootState): QMapAiAssistantConfig | null {
  return selectQMapAiAssistantState(state)?.config || null;
}

export function selectQMapAiPanelState(state: QMapRootState): QMapAiPanelState | null {
  return state?.demo?.qmapAi || null;
}

export function selectQMapDrawState(state: QMapRootState): QMapDrawState | null {
  return state?.demo?.qmapDraw || null;
}

export function selectQMapH3PaintState(state: QMapRootState): QMapH3PaintState | null {
  return state?.demo?.h3Paint || null;
}
