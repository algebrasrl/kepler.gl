/**
 * Thin TypeScript client for q-cumber operations via q-assistant proxy.
 *
 * All q-cumber requests go through the q-assistant backend, which forwards
 * them server-side. This avoids exposing q-cumber tokens to the browser
 * and reduces network round-trips.
 */

import {resolveQMapAuthorizationHeader} from '../../../utils/auth-token';

const QCUMBER_PROXY_TIMEOUT_MS = 30_000;

function resolveProxyBaseUrl(): string {
  // q-assistant base URL — the q-cumber proxy is mounted at /qcumber on the same host
  const raw = String(
    import.meta.env.VITE_QMAP_AI_PROXY_BASE ||
    import.meta.env.VITE_Q_ASSISTANT_BASE_URL ||
    ''
  ).replace(/\/+$/, '');
  return raw || 'http://localhost:3004';
}

async function proxyFetch(path: string, init: RequestInit = {}): Promise<any> {
  const baseUrl = resolveProxyBaseUrl();
  const headers: Record<string, string> = {
    Accept: 'application/json',
    ...(init.body ? {'Content-Type': 'application/json'} : {})
  };
  const auth = resolveQMapAuthorizationHeader();
  if (auth) {
    headers.Authorization = auth;
  }

  const controller = new AbortController();
  const timeout = globalThis.setTimeout(() => controller.abort(), QCUMBER_PROXY_TIMEOUT_MS);

  try {
    const response = await fetch(`${baseUrl}/qcumber${path}`, {
      ...init,
      signal: controller.signal,
      headers: {...headers, ...(init.headers as Record<string, string> || {})}
    });
    if (!response.ok) {
      const text = await response.text().catch(() => '');
      throw new Error(`q-cumber proxy ${response.status}: ${text}`);
    }
    return response.json();
  } catch (error: any) {
    if (error?.name === 'AbortError') {
      throw new Error(`q-cumber proxy timeout after ${QCUMBER_PROXY_TIMEOUT_MS}ms for ${path}`);
    }
    throw error;
  } finally {
    globalThis.clearTimeout(timeout);
  }
}

export async function qcumberListProviders(flat = false): Promise<any> {
  return proxyFetch(`/providers${flat ? '?flat=true' : ''}`);
}

export async function qcumberListDatasets(providerId: string): Promise<any> {
  return proxyFetch(`/providers/${encodeURIComponent(providerId)}/datasets`);
}

export async function qcumberGetDatasetHelp(providerId: string, datasetId: string): Promise<any> {
  return proxyFetch(`/providers/${encodeURIComponent(providerId)}/datasets/${encodeURIComponent(datasetId)}`);
}

export async function qcumberQuery(body: Record<string, unknown>): Promise<any> {
  return proxyFetch('/query', {
    method: 'POST',
    body: JSON.stringify(body)
  });
}
