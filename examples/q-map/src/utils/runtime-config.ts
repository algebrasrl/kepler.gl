type BrowserWindowWithRuntimeConfig = Window & {
  __QMAP_AUTH_TOKEN__?: unknown;
  __QMAP_MAPBOX_TOKEN__?: unknown;
};

function normalizeString(value: unknown): string {
  return String(value || '').trim();
}

export function resolveQMapRuntimeAuthToken(): string {
  if (typeof window === 'undefined') {
    return '';
  }
  return normalizeString((window as BrowserWindowWithRuntimeConfig).__QMAP_AUTH_TOKEN__);
}

export function resolveQMapRuntimeMapboxToken(): string {
  if (typeof window === 'undefined') {
    return '';
  }
  return normalizeString((window as BrowserWindowWithRuntimeConfig).__QMAP_MAPBOX_TOKEN__);
}
