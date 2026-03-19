import {polygonToCells, polygonToCellsExperimental, POLYGON_TO_CELLS_FLAGS} from 'h3-js-v4';
import type {PolygonCoords} from './geometry';

function resolveOverlappingFlag(flags: Record<string, unknown> | undefined): string | null {
  if (!flags) return null;

  const candidates: string[] = [
    'containmentOverlapping',
    'containment_overlapping',
    'CONTAINMENT_OVERLAPPING',
    'containmentOverlappingBbox',
    'containment_overlapping_bbox',
    'CONTAINMENT_OVERLAPPING_BBOX'
  ];

  for (const key of candidates) {
    const value = flags[key];
    if (typeof value === 'string' && value.trim()) {
      return value;
    }
  }
  return null;
}

function polygonToCellsIntersectingV4(polygonCoords: PolygonCoords, resolution: number): string[] | null {
  if (typeof polygonToCellsExperimental !== 'function') return null;

  const overlappingFlag = resolveOverlappingFlag(POLYGON_TO_CELLS_FLAGS as Record<string, unknown>);
  if (overlappingFlag === null) return null;

  const out = polygonToCellsExperimental(polygonCoords, resolution, overlappingFlag, true);
  return Array.isArray(out) ? out : null;
}

export function getIntersectingH3Ids(polygons: PolygonCoords[], resolution: number): string[] {
  const ids = new Set<string>();

  polygons.forEach(polygonCoords => {
    let polyIds: string[] | null = null;
    try {
      polyIds = polygonToCellsIntersectingV4(polygonCoords, resolution);
    } catch {
      polyIds = null;
    }

    let fallbackIds: string[] = [];
    try {
      fallbackIds = polygonToCells(polygonCoords, resolution, true) || [];
    } catch {
      fallbackIds = [];
    }
    const selectedIds = polyIds && polyIds.length > 0 ? polyIds : fallbackIds;
    selectedIds.forEach(id => ids.add(String(id)));
  });

  return Array.from(ids);
}
