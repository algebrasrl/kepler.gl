export const QMAP_IFRAME_EXPORT_MESSAGE_TYPE = 'QMAP_IFRAME_EXPORT';
export const QMAP_IFRAME_CLOUD_EXPORT_MESSAGE_TYPE = 'QMAP_IFRAME_CLOUD_EXPORT';
export const QMAP_IFRAME_CANCEL_MESSAGE_TYPE = 'QMAP_IFRAME_CANCEL';
export const QMAP_IFRAME_MESSAGE_SOURCE = 'q-map';
export const QMAP_IFRAME_MESSAGE_VERSION = 1 as const;
const QMAP_IFRAME_EXPORT_ACTION_UUID_PARAM = 'action_uuid';
const QMAP_IFRAME_EXPORT_CLOUD_MAP_ID_PARAM = 'cloud_map_id';
const QMAP_IFRAME_EXPORT_CLOUD_PROVIDER_PARAM = 'cloud_provider';
const QMAP_IFRAME_EXPORT_PAYLOAD_PARAM = 'export_payload';

const QMAP_IFRAME_EXPORT_HASH_MARKER = String(import.meta.env.VITE_QMAP_IFRAME_EXPORT_HASH_MARKER || 'double-setup')
  .trim()
  .toLowerCase();
const QMAP_IFRAME_EXPORT_TARGET_ORIGIN = String(import.meta.env.VITE_QMAP_IFRAME_EXPORT_TARGET_ORIGIN || '').trim();

function resolveOriginFromUrl(rawValue: string | null | undefined): string {
  const candidate = String(rawValue || '').trim();
  if (!candidate) {
    return '';
  }
  try {
    return new URL(candidate).origin;
  } catch {
    return '';
  }
}

function resolveQMapIframeTargetOrigin(): string {
  if (QMAP_IFRAME_EXPORT_TARGET_ORIGIN && QMAP_IFRAME_EXPORT_TARGET_ORIGIN !== '*') {
    return QMAP_IFRAME_EXPORT_TARGET_ORIGIN;
  }

  if (typeof document !== 'undefined') {
    const referrerOrigin = resolveOriginFromUrl(document.referrer);
    if (referrerOrigin) {
      return referrerOrigin;
    }
  }

  if (typeof window !== 'undefined' && window.location?.origin) {
    return window.location.origin;
  }

  return '';
}

export function isQMapInIframe(): boolean {
  if (typeof window === 'undefined') {
    return false;
  }
  try {
    return window.self !== window.top;
  } catch {
    return true;
  }
}

function hasQMapHashMarker(hashValue: string | null | undefined, marker: string): boolean {
  const normalizedMarker = String(marker || '').trim().toLowerCase();
  if (!normalizedMarker) {
    return false;
  }

  const rawHash = String(hashValue || '').trim();
  if (!rawHash) {
    return false;
  }
  const normalizedHash = rawHash.startsWith('#') ? rawHash.slice(1) : rawHash;
  if (!normalizedHash) {
    return false;
  }

  const tokens = normalizedHash
    .toLowerCase()
    .split(/[^a-z0-9_-]+/g)
    .filter(Boolean);
  if (tokens.includes(normalizedMarker)) {
    return true;
  }

  const directParams = new URLSearchParams(normalizedHash);
  if (directParams.has(normalizedMarker)) {
    return true;
  }

  const queryStart = normalizedHash.indexOf('?');
  if (queryStart >= 0 && queryStart < normalizedHash.length - 1) {
    const queryParams = new URLSearchParams(normalizedHash.slice(queryStart + 1));
    if (queryParams.has(normalizedMarker)) {
      return true;
    }
  }

  return false;
}

function resolveQMapHashParam(hashValue: string | null | undefined, paramName: string): string | null {
  const normalizedParamName = String(paramName || '').trim();
  if (!normalizedParamName) {
    return null;
  }

  const rawHash = String(hashValue || '').trim();
  if (!rawHash) {
    return null;
  }
  const normalizedHash = rawHash.startsWith('#') ? rawHash.slice(1) : rawHash;
  if (!normalizedHash) {
    return null;
  }

  const directParams = new URLSearchParams(normalizedHash);
  const directValue = String(directParams.get(normalizedParamName) || '').trim();
  if (directValue) {
    return directValue;
  }

  const queryStart = normalizedHash.indexOf('?');
  if (queryStart >= 0 && queryStart < normalizedHash.length - 1) {
    const queryParams = new URLSearchParams(normalizedHash.slice(queryStart + 1));
    const queryValue = String(queryParams.get(normalizedParamName) || '').trim();
    if (queryValue) {
      return queryValue;
    }
  }

  return null;
}

export function isQMapIframeExportEnabled(hashValue?: string | null | undefined): boolean {
  if (typeof window === 'undefined') {
    return false;
  }
  const nextHash = hashValue === undefined ? window.location.hash : hashValue;
  return isQMapInIframe() && hasQMapHashMarker(nextHash, QMAP_IFRAME_EXPORT_HASH_MARKER);
}

export function getQMapIframeActionUuid(hashValue?: string | null | undefined): string {
  const nextHash = hashValue === undefined ? window.location.hash : hashValue;
  return resolveQMapHashParam(nextHash, QMAP_IFRAME_EXPORT_ACTION_UUID_PARAM) || '';
}

export type QMapIframeExportPayloadMode = 'full' | 'subset' | 'perimeter';

export function getQMapIframeExportPayloadMode(
  hashValue?: string | null | undefined
): QMapIframeExportPayloadMode {
  const nextHash = hashValue === undefined ? window.location.hash : hashValue;
  const rawMode = String(resolveQMapHashParam(nextHash, QMAP_IFRAME_EXPORT_PAYLOAD_PARAM) || '')
    .trim()
    .toLowerCase();
  if (rawMode === 'full' || rawMode === 'subset' || rawMode === 'perimeter') {
    return rawMode;
  }
  return 'subset';
}

export type QMapIframeCloudMapReference = {
  id: string;
  provider: string;
};

export type QMapIframeMessageType =
  | typeof QMAP_IFRAME_EXPORT_MESSAGE_TYPE
  | typeof QMAP_IFRAME_CLOUD_EXPORT_MESSAGE_TYPE
  | typeof QMAP_IFRAME_CANCEL_MESSAGE_TYPE;

export type QMapIframePostFailureReason =
  | 'missing_map'
  | 'missing_perimeter'
  | 'not_in_iframe'
  | 'missing_parent_window'
  | 'missing_target_origin';

type QMapExportedMap = {
  info?: Record<string, unknown>;
} & Record<string, unknown>;

type QMapGeoJsonFeature = {
  type: 'Feature';
  geometry: {
    type: string;
    coordinates: unknown;
  };
  properties: Record<string, unknown>;
};

export type QMapPerimeterFeatureCollection = {
  type: 'FeatureCollection';
  features: QMapGeoJsonFeature[];
};

export type QMapIframeBasePayload = {
  instanceId: string;
  hash: string;
  sentAt: string;
};

export type QMapIframeExportPayload = QMapIframeBasePayload & {
  map?: QMapExportedMap;
  perimeterFeatureCollection?: QMapPerimeterFeatureCollection;
};

export type QMapIframeCloudExportPayload = QMapIframeExportPayload & {
  format: 'keplergl';
  actionUuid: string;
  cloudMap: QMapIframeCloudMapReference;
  mapInfo: Record<string, unknown>;
};

export type QMapIframeCancelPayload = {
  sentAt: string;
  hash: string;
};

export type QMapIframeMessageEnvelope<TType extends QMapIframeMessageType, TPayload> = {
  type: TType;
  source: typeof QMAP_IFRAME_MESSAGE_SOURCE;
  version: typeof QMAP_IFRAME_MESSAGE_VERSION;
  payload: TPayload;
};

export type QMapIframeExportMessage = QMapIframeMessageEnvelope<
  typeof QMAP_IFRAME_EXPORT_MESSAGE_TYPE,
  QMapIframeExportPayload
>;

export type QMapIframeCloudExportMessage = QMapIframeMessageEnvelope<
  typeof QMAP_IFRAME_CLOUD_EXPORT_MESSAGE_TYPE,
  QMapIframeCloudExportPayload
>;

export type QMapIframeCancelMessage = QMapIframeMessageEnvelope<
  typeof QMAP_IFRAME_CANCEL_MESSAGE_TYPE,
  QMapIframeCancelPayload
>;

export type QMapIframeAnyMessage =
  | QMapIframeExportMessage
  | QMapIframeCloudExportMessage
  | QMapIframeCancelMessage;

export type QMapIframePostResult<TType extends QMapIframeMessageType> =
  | {
      ok: true;
      messageType: TType;
      targetOrigin: string;
    }
  | {
      ok: false;
      reason: QMapIframePostFailureReason;
    };

export function getQMapIframeCloudMapReference(
  hashValue?: string | null | undefined
): QMapIframeCloudMapReference | null {
  const nextHash = hashValue === undefined ? window.location.hash : hashValue;
  const cloudMapId = resolveQMapHashParam(nextHash, QMAP_IFRAME_EXPORT_CLOUD_MAP_ID_PARAM) || '';
  if (!cloudMapId) {
    return null;
  }
  const cloudProvider =
    resolveQMapHashParam(nextHash, QMAP_IFRAME_EXPORT_CLOUD_PROVIDER_PARAM) || 'q-storage-backend';
  return {
    id: cloudMapId,
    provider: cloudProvider
  };
}

type QMapIframeExportMeta = {
  cloudMapId?: string;
  cloudProvider?: string;
};

function isObjectRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === 'object' && !Array.isArray(value);
}

function isArrayOfItems<T = unknown>(value: unknown): value is T[] {
  return Array.isArray(value);
}

function isGeoJsonFeature(value: unknown): value is QMapGeoJsonFeature {
  return (
    isObjectRecord(value)
    && value.type === 'Feature'
    && isObjectRecord(value.geometry)
    && typeof value.geometry.type === 'string'
    && value.geometry.type.trim().length > 0
  );
}

function isPolygonGeometryType(value: unknown): boolean {
  return value === 'Polygon' || value === 'MultiPolygon';
}

function sanitizePerimeterFeatureCollection(value: unknown): QMapPerimeterFeatureCollection | null {
  if (!isObjectRecord(value) || value.type !== 'FeatureCollection' || !isArrayOfItems(value.features)) {
    return null;
  }
  const features = value.features
    .filter(item => isGeoJsonFeature(item))
    .filter(item => isPolygonGeometryType(item.geometry.type))
    .map(item => ({
      type: 'Feature' as const,
      geometry: item.geometry,
      properties: isObjectRecord(item.properties) ? item.properties : {}
    }));
  if (!features.length) {
    return null;
  }
  return {
    type: 'FeatureCollection',
    features
  };
}

function extractQMapEditorPerimeterFeatureCollection(map: QMapExportedMap): QMapPerimeterFeatureCollection | null {
  const editorFeatures = map?.config
    && isObjectRecord(map.config)
    && map.config.config
    && isObjectRecord(map.config.config)
    && map.config.config.visState
    && isObjectRecord(map.config.config.visState)
    && map.config.config.visState.editor
    && isObjectRecord(map.config.config.visState.editor)
    ? map.config.config.visState.editor.features
    : null;
  return sanitizePerimeterFeatureCollection({
    type: 'FeatureCollection',
    features: isArrayOfItems(editorFeatures) ? editorFeatures : []
  });
}

function resolveQMapIframePostTarget(mapRequired: boolean, map?: QMapExportedMap): QMapIframePostResult<QMapIframeMessageType> {
  if (mapRequired && !map) {
    return {ok: false, reason: 'missing_map'};
  }
  if (typeof window === 'undefined' || !isQMapInIframe()) {
    return {ok: false, reason: 'not_in_iframe'};
  }
  if (!window.parent) {
    return {ok: false, reason: 'missing_parent_window'};
  }
  const targetOrigin = resolveQMapIframeTargetOrigin();
  if (!targetOrigin) {
    return {ok: false, reason: 'missing_target_origin'};
  }
  return {ok: true, messageType: QMAP_IFRAME_EXPORT_MESSAGE_TYPE, targetOrigin};
}

function buildQMapIframeMessage<TType extends QMapIframeMessageType, TPayload>(
  type: TType,
  payload: TPayload
): QMapIframeMessageEnvelope<TType, TPayload> {
  return {
    type,
    source: QMAP_IFRAME_MESSAGE_SOURCE,
    version: QMAP_IFRAME_MESSAGE_VERSION,
    payload
  };
}

export function describeQMapIframePostFailure(reason: QMapIframePostFailureReason): string {
  switch (reason) {
    case 'missing_map':
      return 'Map snapshot is empty.';
    case 'missing_perimeter':
      return 'No polygon perimeter found in the drawn map.';
    case 'not_in_iframe':
      return 'q-map is not running inside an iframe export session.';
    case 'missing_parent_window':
      return 'Parent window is not available for iframe export.';
    case 'missing_target_origin':
      return 'Unable to resolve the parent origin for iframe export.';
    default:
      return 'Unable to post iframe message.';
  }
}

export function postQMapIframeExportDetailed(
  map: QMapExportedMap,
  instanceId: string,
  meta?: QMapIframeExportMeta
): QMapIframePostResult<typeof QMAP_IFRAME_EXPORT_MESSAGE_TYPE | typeof QMAP_IFRAME_CLOUD_EXPORT_MESSAGE_TYPE> {
  const postTarget = resolveQMapIframePostTarget(true, map);
  if (!postTarget.ok) {
    return postTarget;
  }

  const resolvedInstanceId =
    getQMapIframeActionUuid(window.location.hash) ||
    String(instanceId || '').trim() ||
    'map';
  const actionUuid = getQMapIframeActionUuid(window.location.hash);
  const cloudMapId = String(meta?.cloudMapId || '').trim();
  const cloudProvider = String(meta?.cloudProvider || '').trim();
  const payloadMode = getQMapIframeExportPayloadMode(window.location.hash);
  const mapInfo = map.info && typeof map.info === 'object' ? map.info : {};
  const perimeterFeatureCollection = extractQMapEditorPerimeterFeatureCollection(map);
  if (payloadMode === 'perimeter' && !perimeterFeatureCollection) {
    return {ok: false, reason: 'missing_perimeter'};
  }
  const hasCloudRef = Boolean(cloudMapId && cloudProvider);
  const messageType = hasCloudRef ? QMAP_IFRAME_CLOUD_EXPORT_MESSAGE_TYPE : QMAP_IFRAME_EXPORT_MESSAGE_TYPE;
  const basePayload: QMapIframeExportPayload = {
    instanceId: resolvedInstanceId,
    hash: window.location.hash || '',
    sentAt: new Date().toISOString(),
    ...(payloadMode === 'full' || !hasCloudRef ? {map} : {}),
    ...(perimeterFeatureCollection ? {perimeterFeatureCollection} : {})
  };
  const payload: QMapIframeExportPayload | QMapIframeCloudExportPayload = hasCloudRef
    ? {
        ...basePayload,
        format: 'keplergl',
        actionUuid: actionUuid || resolvedInstanceId,
        cloudMap: {
          id: cloudMapId,
          provider: cloudProvider
        },
        mapInfo
      }
    : basePayload;

  window.parent.postMessage(buildQMapIframeMessage(messageType, payload), postTarget.targetOrigin);
  return {ok: true, messageType, targetOrigin: postTarget.targetOrigin};
}

export function postQMapIframeExport(map: QMapExportedMap, instanceId: string, meta?: QMapIframeExportMeta): boolean {
  return postQMapIframeExportDetailed(map, instanceId, meta).ok;
}

export function postQMapIframeCancelDetailed(): QMapIframePostResult<typeof QMAP_IFRAME_CANCEL_MESSAGE_TYPE> {
  const postTarget = resolveQMapIframePostTarget(false);
  if (!postTarget.ok) {
    return postTarget;
  }

  const payload: QMapIframeCancelPayload = {
    sentAt: new Date().toISOString(),
    hash: window.location.hash || ''
  };

  window.parent.postMessage(buildQMapIframeMessage(QMAP_IFRAME_CANCEL_MESSAGE_TYPE, payload), postTarget.targetOrigin);
  return {ok: true, messageType: QMAP_IFRAME_CANCEL_MESSAGE_TYPE, targetOrigin: postTarget.targetOrigin};
}

export function postQMapIframeCancel(): boolean {
  return postQMapIframeCancelDetailed().ok;
}
