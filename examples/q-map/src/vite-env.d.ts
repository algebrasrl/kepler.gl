/// <reference types="vite/client" />

interface ImportMetaEnv {
  readonly VITE_MAPBOX_TOKEN?: string;
  // provider/model selection is orchestrated server-side by q-assistant
  readonly VITE_QMAP_AI_API_BASE?: string;
  readonly VITE_QMAP_AI_PROXY_BASE?: string;
  readonly VITE_QMAP_AUTH_TOKEN_STORAGE_KEYS?: string;
  readonly VITE_QMAP_AUTH_ALLOW_OPAQUE_BEARER?: string;
  readonly VITE_QMAP_AI_TEMPERATURE?: string;
  readonly VITE_QMAP_AI_TOP_P?: string;
  readonly VITE_QMAP_MODE?: 'kepler' | 'draw-stressor' | 'draw-on-map' | 'geotoken';
  // legacy compatibility
  readonly VITE_AI_PROVIDER?: string;
  readonly VITE_AI_MODEL?: string;
  readonly VITE_AI_BASE_URL?: string;
  readonly VITE_H3_LOOKUP_API_BASE_URL?: string;
  readonly VITE_QCUMBER_CLOUD_API_BASE?: string;
  readonly VITE_QCUMBER_CLOUD_DISPLAY_NAME?: string;
  readonly VITE_QCUMBER_CLOUD_MANAGEMENT_URL?: string;
  readonly VITE_QCUMBER_CLOUD_TOKEN?: string;
  readonly VITE_QCUMBER_TILESET_BASE?: string;
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}
