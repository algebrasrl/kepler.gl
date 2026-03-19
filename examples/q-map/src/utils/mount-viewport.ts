export type QMapViewport = {
  latitude: number;
  longitude: number;
  zoom: number;
  bearing: number;
  pitch: number;
};

function readHashParamRaw(hashValue: string | null | undefined, key: string): string {
  const candidate = String(hashValue || '').trim();
  if (!candidate) {
    return '';
  }

  const normalizedHash = candidate.startsWith('#') ? candidate.slice(1) : candidate;
  if (!normalizedHash) {
    return '';
  }

  const directParams = new URLSearchParams(normalizedHash);
  const directValue = directParams.get(key);
  if (typeof directValue === 'string' && directValue.trim()) {
    return directValue.trim();
  }

  const queryStart = normalizedHash.indexOf('?');
  if (queryStart >= 0 && queryStart < normalizedHash.length - 1) {
    const queryParams = new URLSearchParams(normalizedHash.slice(queryStart + 1));
    const queryValue = queryParams.get(key);
    if (typeof queryValue === 'string' && queryValue.trim()) {
      return queryValue.trim();
    }
  }

  return '';
}

export function hasQMapIframeCloudMapId(hashValue?: string | null): boolean {
  return Boolean(readHashParamRaw(hashValue, 'cloud_map_id'));
}

export function resolveQMapMountViewport(
  hashValue: string | null | undefined,
  initialMapViewport: QMapViewport
): QMapViewport | null {
  // Iframe sessions pass mode/preset/action params even when no explicit viewport is present.
  // Reapply the intended startup viewport unless a saved cloud map is about to load.
  if (hasQMapIframeCloudMapId(hashValue)) {
    return null;
  }
  return initialMapViewport;
}
