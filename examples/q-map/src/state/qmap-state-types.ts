import type {MessageModel} from '@openassistant/core';

export type QMapFeatureState = {
  id?: string | number;
  geometry?: unknown;
  properties?: Record<string, unknown>;
} & Record<string, unknown>;

export type QMapFieldState = {
  name?: string;
  fieldIdx?: number;
  type?: string;
} & Record<string, unknown>;

export type QMapDatasetState = {
  id?: string;
  label?: string;
  fields?: QMapFieldState[];
  allIndexes?: number[];
  length?: number;
  dataContainer?: {
    numRows?: () => number;
    rowAsArray?: (index: number) => unknown;
  };
  getValue?: (fieldName: string, rowIdx: number) => unknown;
} & Record<string, unknown>;

export type QMapLayerState = {
  id?: string;
  type?: string;
  config?: {
    label?: string;
    dataId?: string | string[];
    isVisible?: boolean;
    visConfig?: Record<string, unknown>;
  } & Record<string, unknown>;
} & Record<string, unknown>;

export type QMapVisState = {
  datasets?: Record<string, QMapDatasetState>;
  layers?: QMapLayerState[];
  filters?: unknown[];
  editor?: {features?: QMapFeatureState[]} & Record<string, unknown>;
  interactionConfig?: Record<string, unknown>;
} & Record<string, unknown>;

export type QMapUiState = {
  locale?: string;
  qmapMode?: string;
  activeSidePanel?: string | null;
  mapControls?: Record<string, unknown>;
} & Record<string, unknown>;

export type QMapKeplerMapState = {
  visState?: QMapVisState;
  uiState?: QMapUiState;
  mapStyle?: {
    styleType?: string;
    mapStyles?: Record<string, unknown>;
  } & Record<string, unknown>;
} & Record<string, unknown>;

export type QMapAiAssistantConfig = {
  baseUrl?: string;
  temperature?: number;
  topP?: number;
  isReady?: boolean;
  apiKey?: string;
  mapboxToken?: string;
} & Record<string, unknown>;

export type QMapAiAssistantState = {
  config?: QMapAiAssistantConfig;
  messages?: MessageModel[];
} & Record<string, unknown>;

export type QMapAiPanelState = {
  isOpen?: boolean;
} & Record<string, unknown>;

export type QMapDrawState = {
  activeTarget?: string;
  activeTool?: string;
} & Record<string, unknown>;

export type QMapH3PaintState = {
  active?: boolean;
  resolution?: number;
} & Record<string, unknown>;

export type QMapDemoState = {
  keplerGl?: {map?: QMapKeplerMapState} & Record<string, unknown>;
  aiAssistant?: QMapAiAssistantState;
  qmapAi?: QMapAiPanelState;
  qmapDraw?: QMapDrawState;
  h3Paint?: QMapH3PaintState;
} & Record<string, unknown>;

export type QMapRootState = {
  demo?: QMapDemoState;
} & Record<string, unknown>;
