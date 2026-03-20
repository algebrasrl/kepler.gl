/**
 * Module-level constants for cloud/q-cumber integration.
 */
import {DEFAULT_QMAP_ASSISTANT_BASE_URL} from '../../../utils/assistant-config';

export const DEFAULT_QCUMBER_BACKEND_PROVIDER = 'q-cumber-backend';
export const DEFAULT_QCUMBER_TOKEN_KEY = 'qmap_qcumber_backend_token';
export const STATIC_QCUMBER_TOKEN = import.meta.env.VITE_QCUMBER_CLOUD_TOKEN || '';
export const DEFAULT_QSTORAGE_BACKEND_PROVIDER = 'q-storage-backend';
export const DEFAULT_QSTORAGE_TOKEN_KEY = 'qmap_qstorage_backend_token';
export const STATIC_QSTORAGE_TOKEN = import.meta.env.VITE_QSTORAGE_CLOUD_TOKEN || '';
export const DEFAULT_CLOUD_MAP_PROVIDER = DEFAULT_QSTORAGE_BACKEND_PROVIDER;
export const DEFAULT_ASSISTANT_BASE = (
  import.meta.env.VITE_QMAP_AI_PROXY_BASE ||
  import.meta.env.VITE_QMAP_AI_API_BASE ||
  DEFAULT_QMAP_ASSISTANT_BASE_URL
).replace(/\/+$/, '');
// QCUMBER_HTTP_TIMEOUT_MS removed — timeout handled by qcumber-api.ts proxy client
export const QCUMBER_MAX_AUTO_LAYER_GEOMETRY_ROWS = Math.max(
  1000,
  Number(import.meta.env.VITE_QMAP_AI_QUERY_MAX_AUTO_LAYER_GEOMETRY_ROWS || 15000) || 15000
);
export const QCUMBER_INCLUDE_LATLON_FALLBACK_FIELDS =
  String(import.meta.env.VITE_QMAP_AI_QUERY_INCLUDE_LATLON_FALLBACK || 'false').toLowerCase() === 'true';
export const ITALY_DEFAULT_SPATIAL_BBOX: [number, number, number, number] = [6.6272658, 35.2889616, 18.7844746, 47.0921462];
export const QCUMBER_PROVIDER_ROUTING_HINTS: Record<string, string> = {
  'local-assets-it': 'Use for Italian administrative boundaries (regions/provinces/municipalities, Kontur).',
  'geoapi-q-cumber':
    'Use for GeoAPI collections: events-data for events, feature-data for heterogeneous geospatial features.',
  'q-cumber': 'Use for platform APIs (devices/sensors/readings/emission factors), not administrative boundaries.'
};
