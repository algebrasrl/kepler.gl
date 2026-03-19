import {MAP_CONTROLS} from '@kepler.gl/constants';

export type QMapMode = 'kepler' | 'draw-stressor' | 'draw-on-map' | 'geotoken';
type QMapCustomControl = 'h3Paint' | 'ai' | 'drawTools';
type QMapControlKey = keyof typeof MAP_CONTROLS;
const QMAP_UI_MODE_KEY = 'qmapMode';
const QMAP_UI_USER_TYPE_KEY = 'qmapUserType';
const QMAP_UI_GROUP_SLUG_KEY = 'qmapUserGroupSlug';
const QMAP_DEFAULT_USER_TYPE = 'user';

export type QMapModeConfig = {
  id: QMapMode;
  sidePanels: string[];
  defaultSidePanel: string | null;
  mapControlVisibility: Partial<Record<QMapControlKey, boolean>>;
  customControls: Record<QMapCustomControl, boolean>;
};
export type QMapModeOption = {id: QMapMode; label: string};
export type QMapUserModeContext = {
  userType?: string | null;
  groupSlug?: string | null;
};

const FALLBACK_MODE: QMapMode = 'kepler';

function buildStrictMapControlVisibility(
  visibleControlIds: QMapControlKey[]
): Partial<Record<QMapControlKey, boolean>> {
  const visibility: Partial<Record<QMapControlKey, boolean>> = {};
  (Object.keys(MAP_CONTROLS) as QMapControlKey[]).forEach(controlId => {
    visibility[controlId] = false;
  });
  visibleControlIds.forEach(controlId => {
    visibility[controlId] = true;
  });
  return visibility;
}

const QMAP_MODE_CONFIGS: Record<QMapMode, QMapModeConfig> = {
  kepler: {
    id: 'kepler',
    sidePanels: ['profile', 'layer', 'filter', 'interaction', 'map', 'operations'],
    defaultSidePanel: 'profile',
    mapControlVisibility: {},
    customControls: {
      h3Paint: true,
      ai: true,
      drawTools: false
    }
  },
  'draw-stressor': {
    id: 'draw-stressor',
    sidePanels: ['layer', 'map'],
    defaultSidePanel: 'layer',
    mapControlVisibility: {
      visibleLayers: false,
      mapLegend: false,
      toggle3d: true,
      splitMap: true,
      mapDraw: true,
      mapLocale: true,
      effect: false,
      aiAssistant: false
    },
    customControls: {
      h3Paint: false,
      ai: true,
      drawTools: true
    }
  },
  'draw-on-map': {
    id: 'draw-on-map',
    sidePanels: ['layer'],
    defaultSidePanel: null,
    mapControlVisibility: buildStrictMapControlVisibility(['mapDraw', 'splitMap', 'toggle3d', 'mapLocale']),
    customControls: {
      h3Paint: true,
      ai: true,
      drawTools: false
    }
  },
  geotoken: {
    id: 'geotoken',
    sidePanels: ['layer'],
    defaultSidePanel: null,
    mapControlVisibility: buildStrictMapControlVisibility(['mapDraw', 'toggle3d']),
    customControls: {
      h3Paint: true,
      ai: false,
      drawTools: false
    }
  }
};

export const QMAP_MODE_OPTIONS: QMapModeOption[] = [
  {id: 'kepler', label: 'qmapMode.kepler'},
  {id: 'draw-stressor', label: 'qmapMode.drawStressor'},
  {id: 'draw-on-map', label: 'qmapMode.drawOnMap'},
  {id: 'geotoken', label: 'qmapMode.geotoken'}
];

function normalizeModeValue(raw: unknown): string {
  return String(raw || '')
    .trim()
    .toLowerCase()
    .replace(/_/g, '-');
}

export function resolveQMapMode(raw: unknown): QMapMode {
  const normalized = normalizeModeValue(raw);
  if (normalized in QMAP_MODE_CONFIGS) {
    return normalized as QMapMode;
  }
  return FALLBACK_MODE;
}

export const ACTIVE_QMAP_MODE = resolveQMapMode(import.meta.env.VITE_QMAP_MODE);

export function getQMapModeConfig(mode: QMapMode = ACTIVE_QMAP_MODE): QMapModeConfig {
  return QMAP_MODE_CONFIGS[mode] || QMAP_MODE_CONFIGS[FALLBACK_MODE];
}

function normalizeUserType(raw: unknown): string {
  const next = String(raw || '')
    .trim()
    .toLowerCase();
  return next || QMAP_DEFAULT_USER_TYPE;
}

export function getQMapUserModeContextFromUiState(uiState: any): QMapUserModeContext {
  return {
    userType: normalizeUserType(uiState?.[QMAP_UI_USER_TYPE_KEY]),
    groupSlug: String(uiState?.[QMAP_UI_GROUP_SLUG_KEY] || '').trim() || null
  };
}

export function getQMapModeOptionsForUser(userContext?: QMapUserModeContext): QMapModeOption[] {
  const _normalizedUserType = normalizeUserType(userContext?.userType);
  const _groupSlug = String(userContext?.groupSlug || '').trim().toLowerCase();
  // Current policy: every user sees every mode.
  // Future specialization can branch on _normalizedUserType / _groupSlug.
  return [...QMAP_MODE_OPTIONS];
}

export function resolveQMapModeForUser(
  requestedMode: unknown,
  userContext?: QMapUserModeContext
): QMapMode {
  const requested = resolveQMapMode(requestedMode);
  const allowed = getQMapModeOptionsForUser(userContext).map(item => item.id);
  if (allowed.includes(requested)) {
    return requested;
  }
  if (allowed.length > 0) {
    return allowed[0];
  }
  return FALLBACK_MODE;
}

export function resolveQMapModeFromUiState(uiState: any): QMapMode {
  const userContext = getQMapUserModeContextFromUiState(uiState);
  return resolveQMapModeForUser(uiState?.[QMAP_UI_MODE_KEY], userContext);
}

export function isQMapSidePanelEnabled(
  panelId: string | null | undefined,
  modeConfig: QMapModeConfig = getQMapModeConfig()
): boolean {
  if (!panelId) {
    return true;
  }
  return modeConfig.sidePanels.includes(String(panelId));
}

export function resolveQMapSidePanelId(
  panelId: string | null | undefined,
  modeConfig: QMapModeConfig = getQMapModeConfig()
): string | null {
  const requested = String(panelId || '').trim();
  if (requested && isQMapSidePanelEnabled(requested, modeConfig)) {
    return requested;
  }

  if (modeConfig.defaultSidePanel === null) {
    return null;
  }

  const configuredDefault = String(modeConfig.defaultSidePanel || '').trim();
  if (configuredDefault && isQMapSidePanelEnabled(configuredDefault, modeConfig)) {
    return configuredDefault;
  }

  const firstPanel = String(modeConfig.sidePanels[0] || '').trim();
  return firstPanel || null;
}

export function filterQMapSidePanels<T extends {id: string}>(
  panels: T[],
  modeConfig: QMapModeConfig = getQMapModeConfig()
): T[] {
  return (Array.isArray(panels) ? panels : []).filter(panel =>
    isQMapSidePanelEnabled(panel?.id, modeConfig)
  );
}

const DEFAULT_MAP_CONTROL = {
  show: true,
  active: false,
  disableClose: false,
  activeMapIndex: 0
};

const DEFAULT_MAP_LEGEND_CONTROL = {
  ...DEFAULT_MAP_CONTROL,
  disableEdit: false
};

export function buildQMapModeMapControls(
  modeConfig: QMapModeConfig = getQMapModeConfig()
): Record<string, any> {
  const controls: Record<string, any> = {};
  (Object.keys(MAP_CONTROLS) as QMapControlKey[]).forEach(controlId => {
    controls[controlId] =
      controlId === MAP_CONTROLS.mapLegend
        ? {...DEFAULT_MAP_LEGEND_CONTROL}
        : {...DEFAULT_MAP_CONTROL};
    const configuredVisibility = modeConfig.mapControlVisibility[controlId];
    if (typeof configuredVisibility === 'boolean') {
      controls[controlId].show = configuredVisibility;
    }
  });
  return controls;
}

export function applyQMapModeToMapControls(
  currentMapControls: Record<string, any> | null | undefined,
  modeConfig: QMapModeConfig = getQMapModeConfig()
): Record<string, any> {
  const baseControls = buildQMapModeMapControls(modeConfig);
  const current = currentMapControls || {};
  const nextControls: Record<string, any> = {};
  Object.keys(baseControls).forEach(controlId => {
    const base = baseControls[controlId] || {};
    const existing = current[controlId] || {};
    const next = {
      ...base,
      ...existing,
      show: Boolean(base.show)
    };
    if (!next.show) {
      next.active = false;
    }
    nextControls[controlId] = next;
  });
  return {
    ...current,
    ...nextControls
  };
}

export function isQMapCustomControlEnabled(
  control: QMapCustomControl,
  modeConfig: QMapModeConfig = getQMapModeConfig()
): boolean {
  return Boolean(modeConfig.customControls?.[control]);
}
