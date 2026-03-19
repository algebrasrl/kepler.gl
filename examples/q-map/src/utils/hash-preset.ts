const QMAP_HASH_PRESET_PARAM = 'preset';
const QMAP_HASH_MODE_PARAM = 'mode';
const QMAP_HASH_LAT_PARAM = 'lat';
const QMAP_HASH_LON_PARAM = 'lon';
const QMAP_HASH_LNG_PARAM = 'lng';
const QMAP_HASH_ZOOM_PARAM = 'zoom';
const QMAP_HASH_ZOOM_SHORT_PARAM = 'z';
const QMAP_HASH_BEARING_PARAM = 'bearing';
const QMAP_HASH_BEARING_SHORT_PARAM = 'brg';
const QMAP_HASH_PITCH_PARAM = 'pitch';
const QMAP_HASH_BASEMAP_PARAM = 'basemap';
const QMAP_HASH_STYLE_PARAM = 'style';
const QMAP_HASH_STYLE_TYPE_PARAM = 'styleType';

export type QMapUiMapControlPreset = {
  show?: boolean;
  active?: boolean;
};

export type QMapUiPreset = {
  qmapMode?: string;
  activeSidePanel?: string | null;
  readOnly?: boolean;
  locale?: string;
  mapControls?: Record<string, QMapUiMapControlPreset>;
};

export type QMapMapViewportPreset = {
  latitude?: number;
  longitude?: number;
  zoom?: number;
  bearing?: number;
  pitch?: number;
};

export type QMapMapPreset = {
  viewport?: QMapMapViewportPreset;
  basemap?: string;
};

export type QMapHashPresetPayload = {
  source: 'preset' | 'params' | 'mixed';
  encoded: string;
  mode: string | null;
  decodedText: string | null;
  decodedJson: unknown | null;
  uiPreset: QMapUiPreset | null;
  mapPreset: QMapMapPreset | null;
  error: string | null;
};

function getCurrentHash(hash?: string): string {
  if (typeof hash === 'string') {
    return hash;
  }
  if (typeof window !== 'undefined') {
    return window.location.hash || '';
  }
  return '';
}

function decodeBase64UrlToText(encoded: string): string {
  if (typeof atob !== 'function') {
    throw new Error('Base64 decoder is not available in this runtime');
  }

  const normalized = encoded.replace(/-/g, '+').replace(/_/g, '/');
  const remainder = normalized.length % 4;
  const padded = remainder === 0 ? normalized : `${normalized}${'='.repeat(4 - remainder)}`;
  const binary = atob(padded);
  const bytes = Uint8Array.from(binary, ch => ch.charCodeAt(0));
  return new TextDecoder().decode(bytes);
}

function normalizeEncodedPreset(raw: string): string {
  const compact = String(raw || '')
    .trim()
    .replace(/\s+/g, '');
  if (!compact) {
    return '';
  }
  try {
    return decodeURIComponent(compact);
  } catch {
    return compact;
  }
}

function normalizeHashParamValue(raw: string): string {
  const compact = String(raw || '').trim();
  if (!compact) {
    return '';
  }
  try {
    return decodeURIComponent(compact).trim();
  } catch {
    return compact;
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === 'object' && !Array.isArray(value);
}

function hasOwnKey<T extends object>(value: T | null | undefined, key: string): boolean {
  return Boolean(value) && Object.prototype.hasOwnProperty.call(value, key);
}

function toFiniteInRange(raw: unknown, min?: number, max?: number): number | undefined {
  if (raw === '' || raw === null || raw === undefined) {
    return undefined;
  }
  const value = Number(raw);
  if (!Number.isFinite(value)) {
    return undefined;
  }
  if (typeof min === 'number' && value < min) {
    return undefined;
  }
  if (typeof max === 'number' && value > max) {
    return undefined;
  }
  return value;
}

function extractUiPreset(decodedJson: unknown): QMapUiPreset | null {
  if (!isRecord(decodedJson)) {
    return null;
  }
  const state = isRecord(decodedJson.state) ? decodedJson.state : null;
  const uiState = state && isRecord(state.uiState) ? state.uiState : null;
  if (!uiState) {
    return null;
  }

  const preset: QMapUiPreset = {};
  if (typeof uiState.qmapMode === 'string' && uiState.qmapMode.trim()) {
    preset.qmapMode = uiState.qmapMode.trim();
  }
  if (typeof uiState.activeSidePanel === 'string') {
    const next = uiState.activeSidePanel.trim();
    preset.activeSidePanel = next || null;
  } else if (uiState.activeSidePanel === null) {
    preset.activeSidePanel = null;
  }
  if (typeof uiState.readOnly === 'boolean') {
    preset.readOnly = uiState.readOnly;
  }
  if (typeof uiState.locale === 'string' && uiState.locale.trim()) {
    preset.locale = uiState.locale.trim();
  }

  if (isRecord(uiState.mapControls)) {
    const mapControls: Record<string, QMapUiMapControlPreset> = {};
    Object.entries(uiState.mapControls).forEach(([controlId, rawValue]) => {
      if (!isRecord(rawValue)) {
        return;
      }
      const controlPreset: QMapUiMapControlPreset = {};
      if (typeof rawValue.show === 'boolean') {
        controlPreset.show = rawValue.show;
      }
      if (typeof rawValue.active === 'boolean') {
        controlPreset.active = rawValue.active;
      }
      if (Object.keys(controlPreset).length > 0) {
        mapControls[controlId] = controlPreset;
      }
    });
    if (Object.keys(mapControls).length > 0) {
      preset.mapControls = mapControls;
    }
  }

  return Object.keys(preset).length > 0 ? preset : null;
}

function extractMapPreset(decodedJson: unknown): QMapMapPreset | null {
  if (!isRecord(decodedJson)) {
    return null;
  }
  const state = isRecord(decodedJson.state) ? decodedJson.state : null;
  if (!state) {
    return null;
  }

  const mapState = isRecord(state.mapState) ? state.mapState : null;
  const mapStyle = isRecord(state.mapStyle) ? state.mapStyle : null;

  const viewport: QMapMapViewportPreset = {};
  if (mapState) {
    const latitude = toFiniteInRange(mapState.latitude, -90, 90);
    if (Number.isFinite(latitude as number)) {
      viewport.latitude = latitude;
    }
    const longitude = toFiniteInRange(mapState.longitude, -180, 180);
    if (Number.isFinite(longitude as number)) {
      viewport.longitude = longitude;
    }
    const zoom = toFiniteInRange(mapState.zoom, -20, 30);
    if (Number.isFinite(zoom as number)) {
      viewport.zoom = zoom;
    }
    const bearing = toFiniteInRange(mapState.bearing, -360, 360);
    if (Number.isFinite(bearing as number)) {
      viewport.bearing = bearing;
    }
    const pitch = toFiniteInRange(mapState.pitch, 0, 85);
    if (Number.isFinite(pitch as number)) {
      viewport.pitch = pitch;
    }
  }

  const preset: QMapMapPreset = {};
  if (Object.keys(viewport).length > 0) {
    preset.viewport = viewport;
  }
  if (mapStyle && typeof mapStyle.styleType === 'string' && mapStyle.styleType.trim()) {
    preset.basemap = mapStyle.styleType.trim();
  }

  return Object.keys(preset).length > 0 ? preset : null;
}

function readHashParamRaw(hashValue: string, key: string): string {
  const candidate = String(hashValue || '').trim();
  if (!candidate) {
    return '';
  }

  const directParams = new URLSearchParams(candidate);
  const directValue = directParams.get(key);
  if (typeof directValue === 'string' && directValue.trim()) {
    return directValue;
  }

  const queryStart = candidate.indexOf('?');
  if (queryStart >= 0 && queryStart < candidate.length - 1) {
    const queryParams = new URLSearchParams(candidate.slice(queryStart + 1));
    const queryValue = queryParams.get(key);
    if (typeof queryValue === 'string' && queryValue.trim()) {
      return queryValue;
    }
  }

  return '';
}

function extractPresetFromHash(hashValue: string): string {
  return normalizeEncodedPreset(readHashParamRaw(hashValue, QMAP_HASH_PRESET_PARAM));
}

function extractModeFromHash(hashValue: string): string {
  return normalizeHashParamValue(readHashParamRaw(hashValue, QMAP_HASH_MODE_PARAM));
}

function extractMapPresetFromHashParams(hashValue: string): QMapMapPreset | null {
  const latRaw = readHashParamRaw(hashValue, QMAP_HASH_LAT_PARAM) || readHashParamRaw(hashValue, 'latitude');
  const lonRaw =
    readHashParamRaw(hashValue, QMAP_HASH_LON_PARAM) ||
    readHashParamRaw(hashValue, QMAP_HASH_LNG_PARAM) ||
    readHashParamRaw(hashValue, 'longitude');
  const zoomRaw =
    readHashParamRaw(hashValue, QMAP_HASH_ZOOM_PARAM) || readHashParamRaw(hashValue, QMAP_HASH_ZOOM_SHORT_PARAM);
  const bearingRaw =
    readHashParamRaw(hashValue, QMAP_HASH_BEARING_PARAM) ||
    readHashParamRaw(hashValue, QMAP_HASH_BEARING_SHORT_PARAM);
  const pitchRaw = readHashParamRaw(hashValue, QMAP_HASH_PITCH_PARAM);
  const basemapRaw =
    readHashParamRaw(hashValue, QMAP_HASH_BASEMAP_PARAM) ||
    readHashParamRaw(hashValue, QMAP_HASH_STYLE_PARAM) ||
    readHashParamRaw(hashValue, QMAP_HASH_STYLE_TYPE_PARAM);

  const viewport: QMapMapViewportPreset = {};
  const latitude = toFiniteInRange(latRaw, -90, 90);
  if (Number.isFinite(latitude as number)) {
    viewport.latitude = latitude;
  }
  const longitude = toFiniteInRange(lonRaw, -180, 180);
  if (Number.isFinite(longitude as number)) {
    viewport.longitude = longitude;
  }
  const zoom = toFiniteInRange(zoomRaw, -20, 30);
  if (Number.isFinite(zoom as number)) {
    viewport.zoom = zoom;
  }
  const bearing = toFiniteInRange(bearingRaw, -360, 360);
  if (Number.isFinite(bearing as number)) {
    viewport.bearing = bearing;
  }
  const pitch = toFiniteInRange(pitchRaw, 0, 85);
  if (Number.isFinite(pitch as number)) {
    viewport.pitch = pitch;
  }

  const basemap = normalizeHashParamValue(basemapRaw);
  const preset: QMapMapPreset = {};
  if (Object.keys(viewport).length > 0) {
    preset.viewport = viewport;
  }
  if (basemap) {
    preset.basemap = basemap;
  }
  return Object.keys(preset).length > 0 ? preset : null;
}

function mergeUiPresets(basePreset: QMapUiPreset | null, overrides: QMapUiPreset | null): QMapUiPreset | null {
  if (!basePreset && !overrides) {
    return null;
  }
  const next = {...(basePreset || {})};
  if (overrides) {
    if (hasOwnKey(overrides, 'qmapMode') && typeof overrides.qmapMode === 'string' && overrides.qmapMode.trim()) {
      next.qmapMode = overrides.qmapMode.trim();
    }
    if (hasOwnKey(overrides, 'activeSidePanel')) {
      next.activeSidePanel = overrides.activeSidePanel ?? null;
    }
    if (typeof overrides.readOnly === 'boolean') {
      next.readOnly = overrides.readOnly;
    }
    if (typeof overrides.locale === 'string' && overrides.locale.trim()) {
      next.locale = overrides.locale.trim();
    }
    if (overrides.mapControls && typeof overrides.mapControls === 'object') {
      next.mapControls = {...(next.mapControls || {}), ...overrides.mapControls};
    }
  }
  return Object.keys(next).length > 0 ? next : null;
}

function mergeMapPresets(basePreset: QMapMapPreset | null, overrides: QMapMapPreset | null): QMapMapPreset | null {
  if (!basePreset && !overrides) {
    return null;
  }
  const next: QMapMapPreset = {...(basePreset || {})};
  const mergedViewport: QMapMapViewportPreset = {...(basePreset?.viewport || {})};

  if (overrides?.viewport) {
    if (Number.isFinite(overrides.viewport.latitude as number)) {
      mergedViewport.latitude = overrides.viewport.latitude;
    }
    if (Number.isFinite(overrides.viewport.longitude as number)) {
      mergedViewport.longitude = overrides.viewport.longitude;
    }
    if (Number.isFinite(overrides.viewport.zoom as number)) {
      mergedViewport.zoom = overrides.viewport.zoom;
    }
    if (Number.isFinite(overrides.viewport.bearing as number)) {
      mergedViewport.bearing = overrides.viewport.bearing;
    }
    if (Number.isFinite(overrides.viewport.pitch as number)) {
      mergedViewport.pitch = overrides.viewport.pitch;
    }
  }
  if (Object.keys(mergedViewport).length > 0) {
    next.viewport = mergedViewport;
  }

  if (typeof overrides?.basemap === 'string' && overrides.basemap.trim()) {
    next.basemap = overrides.basemap.trim();
  }

  return Object.keys(next).length > 0 ? next : null;
}

export function parseQMapPresetFromHash(hash?: string): QMapHashPresetPayload | null {
  const rawHash = getCurrentHash(hash);
  const normalizedHash = rawHash.startsWith('#') ? rawHash.slice(1) : rawHash;
  const encoded = extractPresetFromHash(normalizedHash);
  const mode = extractModeFromHash(normalizedHash) || null;
  const mapPresetFromParams = extractMapPresetFromHashParams(normalizedHash);

  const uiPresetFromParams: QMapUiPreset | null = mode ? {qmapMode: mode} : null;
  let decodedText: string | null = null;
  let decodedJson: unknown | null = null;
  let uiPresetFromPayload: QMapUiPreset | null = null;
  let mapPresetFromPayload: QMapMapPreset | null = null;
  let error: string | null = null;

  if (encoded) {
    try {
      decodedText = decodeBase64UrlToText(encoded);
      try {
        decodedJson = JSON.parse(decodedText);
        uiPresetFromPayload = extractUiPreset(decodedJson);
        mapPresetFromPayload = extractMapPreset(decodedJson);
      } catch {
        error = 'Decoded value is not valid JSON';
      }
    } catch (decodeError) {
      error = decodeError instanceof Error ? decodeError.message : String(decodeError);
    }
  }

  const uiPreset = mergeUiPresets(uiPresetFromPayload, uiPresetFromParams);
  const mapPreset = mergeMapPresets(mapPresetFromPayload, mapPresetFromParams);

  if (!encoded && !uiPreset && !mapPreset) {
    return null;
  }

  const source: QMapHashPresetPayload['source'] = encoded
    ? uiPresetFromParams || mapPresetFromParams
      ? 'mixed'
      : 'preset'
    : 'params';

  return {
    source,
    encoded,
    mode,
    decodedText,
    decodedJson,
    uiPreset,
    mapPreset,
    error
  };
}

export function getQMapUiPresetFromHash(hash?: string): QMapUiPreset | null {
  const payload = parseQMapPresetFromHash(hash);
  if (!payload || (payload.error && !payload.uiPreset)) {
    return null;
  }
  return payload.uiPreset || null;
}

export function getQMapMapPresetFromHash(hash?: string): QMapMapPreset | null {
  const payload = parseQMapPresetFromHash(hash);
  if (!payload || (payload.error && !payload.mapPreset)) {
    return null;
  }
  return payload.mapPreset || null;
}

export function logQMapPresetFromHash(hash?: string): QMapHashPresetPayload | null {
  const payload = parseQMapPresetFromHash(hash);
  if (!payload) {
    return null;
  }

  if (payload.source === 'params') {
    console.log('[q-map] hash params received', {mode: payload.mode, mapPreset: payload.mapPreset});
  } else if (payload.source === 'mixed') {
    console.log('[q-map] hash preset + params received', {
      encoded: payload.encoded,
      mode: payload.mode,
      mapPreset: payload.mapPreset
    });
  } else {
    console.log('[q-map] hash preset received', {encoded: payload.encoded});
  }
  if (payload.error) {
    console.warn('[q-map] hash preset decode error', {
      encoded: payload.encoded,
      error: payload.error,
      decodedText: payload.decodedText
    });
    return payload;
  }

  console.log('[q-map] hash preset decoded', {
    source: payload.source,
    mode: payload.mode,
    encoded: payload.encoded,
    decodedText: payload.decodedText,
    decodedJson: payload.decodedJson,
    uiPreset: payload.uiPreset,
    mapPreset: payload.mapPreset
  });
  return payload;
}
