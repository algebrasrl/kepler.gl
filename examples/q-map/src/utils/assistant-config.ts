import type {QMapAiAssistantConfig} from '../state/qmap-state-types';

export const DEFAULT_QMAP_ASSISTANT_BASE_URL = '/api/q-assistant';

export function resolveQMapAssistantBaseUrl(config?: QMapAiAssistantConfig | null): string {
  return String(config?.baseUrl || import.meta.env.VITE_QMAP_AI_PROXY_BASE || DEFAULT_QMAP_ASSISTANT_BASE_URL).replace(
    /\/+$/,
    ''
  );
}
