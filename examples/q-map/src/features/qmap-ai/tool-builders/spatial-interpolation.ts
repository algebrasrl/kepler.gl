import React, {useEffect} from 'react';
import {useDispatch, useSelector} from 'react-redux';
import {z} from 'zod';

import {selectQMapDatasets, selectQMapVisState} from '../../../state/qmap-selectors';
import {useToolExecution} from './use-tool-execution';

import type {QMapToolContext} from '../context/tool-context';

// ─── H3 grid step sizes (degrees) by resolution ────────────────────────────
const H3_STEP_BY_RES: Record<number, number> = {
  3: 0.5,
  4: 0.2,
  5: 0.08,
  6: 0.03,
  7: 0.012,
  8: 0.005
};

const MAX_OUTPUT_CELLS = 50_000;

// ─── Haversine distance in km ───────────────────────────────────────────────
const haversineKm = (lat1: number, lon1: number, lat2: number, lon2: number): number => {
  const R = 6371;
  const dLat = ((lat2 - lat1) * Math.PI) / 180;
  const dLon = ((lon2 - lon1) * Math.PI) / 180;
  const a =
    Math.sin(dLat / 2) ** 2 +
    Math.cos((lat1 * Math.PI) / 180) * Math.cos((lat2 * Math.PI) / 180) * Math.sin(dLon / 2) ** 2;
  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
};

// ─── IDW interpolation for a single grid point ─────────────────────────────
function idw(
  gridLat: number,
  gridLon: number,
  sources: Array<{lat: number; lon: number; value: number}>,
  power: number,
  maxNeighbors: number,
  maxDistKm: number
): {value: number; neighborCount: number} | null {
  const dists = sources
    .map(s => ({
      dist: haversineKm(gridLat, gridLon, s.lat, s.lon),
      value: s.value
    }))
    .filter(d => d.dist <= maxDistKm && d.dist > 0.001);

  dists.sort((a, b) => a.dist - b.dist);
  const neighbors = dists.slice(0, maxNeighbors);

  if (!neighbors.length) return null;

  // If a source point is essentially at this grid location, return its value
  if (neighbors[0].dist < 0.01) {
    return {value: neighbors[0].value, neighborCount: neighbors.length};
  }

  let weightedSum = 0;
  let weightSum = 0;
  for (const n of neighbors) {
    const w = 1 / Math.pow(n.dist, power);
    weightedSum += n.value * w;
    weightSum += w;
  }
  return {value: weightedSum / weightSum, neighborCount: neighbors.length};
}

// ─── Tool factory ───────────────────────────────────────────────────────────

export function createInterpolateIDWTool(ctx: QMapToolContext) {
  const {
    getCurrentVisState,
    resolveDatasetByName,
    resolveDatasetFieldName,
    resolveGeojsonFieldName,
    getDatasetInfoByLabel,
    makeExecutionKey,
    EXECUTED_TOOL_COMPONENT_KEYS,
    rememberExecutedToolComponentKey,
    parseGeoJsonLike,
    turfCentroid,
    toTurfFeature,
    latLngToCell,
    upsertDerivedDatasetRows
  } = ctx;

  return {
    description:
      '[PREFERRED for spatial interpolation] Inverse Distance Weighting (IDW) interpolation from point measurements ' +
      'to an H3 hexagonal grid. Creates a continuous surface estimate from sparse point data ' +
      '(e.g. air quality stations -> regional coverage).',
    parameters: z.object({
      sourceDatasetName: z.string().describe('Dataset with point measurements'),
      valueField: z.string().describe('Numeric field to interpolate'),
      resolution: z
        .number()
        .min(3)
        .max(8)
        .optional()
        .describe('H3 resolution for output grid. Default 5.'),
      power: z
        .number()
        .min(1)
        .max(5)
        .optional()
        .describe('IDW power parameter (higher = more local). Default 2.'),
      maxNeighbors: z
        .number()
        .min(3)
        .max(50)
        .optional()
        .describe('Max nearby source points to use per grid cell. Default 12.'),
      maxDistanceKm: z
        .number()
        .min(1)
        .max(500)
        .optional()
        .describe('Max search radius in km. Default 100.'),
      newDatasetName: z.string().optional().describe('Name for output dataset'),
      showOnMap: z.boolean().optional().describe('Create layer for output dataset. Default true.')
    }),
    execute: async ({
      sourceDatasetName,
      valueField,
      resolution,
      power,
      maxNeighbors,
      maxDistanceKm,
      newDatasetName,
      showOnMap
    }: any) => {
      const vis = getCurrentVisState();
      const dataset = resolveDatasetByName(vis?.datasets || {}, sourceDatasetName);
      if (!dataset?.id) {
        return {llmResult: {success: false, details: `Dataset "${sourceDatasetName}" not found.`}};
      }

      const resolvedValueField = resolveDatasetFieldName(dataset, valueField);
      if (!resolvedValueField) {
        return {
          llmResult: {
            success: false,
            details: `Field "${valueField}" not found in dataset "${sourceDatasetName}".`
          }
        };
      }

      const geomField =
        resolveGeojsonFieldName(dataset, null) ||
        resolveDatasetFieldName(dataset, '_geojson') ||
        null;

      if (!geomField) {
        return {
          llmResult: {
            success: false,
            details: 'Dataset must have a geometry field (_geojson) with point geometries.'
          }
        };
      }

      // Extract source points
      const idx = Array.isArray(dataset.allIndexes)
        ? dataset.allIndexes
        : Array.from({length: Number(dataset.length || 0)}, (_, i) => i);

      const sourcePoints: Array<{lat: number; lon: number; value: number}> = [];

      for (const rowIdx of idx) {
        const rawValue = dataset.getValue(resolvedValueField, rowIdx);
        const numValue = Number(rawValue);
        if (!Number.isFinite(numValue)) continue;

        const rawGeom = dataset.getValue(geomField, rowIdx);
        const parsed = parseGeoJsonLike(rawGeom);
        if (!parsed) continue;

        // Extract centroid lon/lat from the geometry
        let lon: number | null = null;
        let lat: number | null = null;

        const geom = parsed?.geometry || parsed;
        if (geom?.type === 'Point' && Array.isArray(geom.coordinates)) {
          lon = Number(geom.coordinates[0]);
          lat = Number(geom.coordinates[1]);
        } else {
          // For non-point geometries, use turf centroid
          try {
            const feature = toTurfFeature(parsed);
            if (feature) {
              const centroid = turfCentroid(feature);
              if (centroid?.geometry?.coordinates) {
                lon = Number(centroid.geometry.coordinates[0]);
                lat = Number(centroid.geometry.coordinates[1]);
              }
            }
          } catch {
            continue;
          }
        }

        if (lon !== null && lat !== null && Number.isFinite(lon) && Number.isFinite(lat)) {
          sourcePoints.push({lat, lon, value: numValue});
        }
      }

      if (sourcePoints.length < 3) {
        return {
          llmResult: {
            success: false,
            details: `Need at least 3 source points with valid geometry and numeric value, found ${sourcePoints.length}.`
          }
        };
      }

      // Parameters with defaults
      const effectiveResolution = Math.max(3, Math.min(8, Number(resolution || 5)));
      const effectivePower = Math.max(1, Math.min(5, Number(power || 2)));
      const effectiveMaxNeighbors = Math.max(3, Math.min(50, Number(maxNeighbors || 12)));
      const effectiveMaxDistKm = Math.max(1, Math.min(500, Number(maxDistanceKm || 100)));

      // Compute bounding box with buffer
      const bufferDeg = effectiveMaxDistKm / 111; // approximate km-to-degrees
      let minLat = Infinity;
      let maxLat = -Infinity;
      let minLon = Infinity;
      let maxLon = -Infinity;
      for (const p of sourcePoints) {
        if (p.lat < minLat) minLat = p.lat;
        if (p.lat > maxLat) maxLat = p.lat;
        if (p.lon < minLon) minLon = p.lon;
        if (p.lon > maxLon) maxLon = p.lon;
      }
      minLat = Math.max(-85, minLat - bufferDeg);
      maxLat = Math.min(85, maxLat + bufferDeg);
      minLon = Math.max(-180, minLon - bufferDeg);
      maxLon = Math.min(180, maxLon + bufferDeg);

      // Generate grid and compute IDW values
      let step = H3_STEP_BY_RES[effectiveResolution] || 0.08;

      // Estimate grid size and cap to avoid browser freeze
      const estRows = Math.ceil((maxLat - minLat) / step);
      const estCols = Math.ceil((maxLon - minLon) / step);
      let estCells = estRows * estCols;

      // If estimated cells exceed max, increase step size
      if (estCells > MAX_OUTPUT_CELLS) {
        const scaleFactor = Math.sqrt(estCells / MAX_OUTPUT_CELLS);
        step = step * scaleFactor;
        estCells = Math.ceil((maxLat - minLat) / step) * Math.ceil((maxLon - minLon) / step);
      }

      // Compute IDW for each grid point and map to H3 cells
      const cellMap = new Map<string, {totalValue: number; count: number; neighborCount: number}>();

      for (let lat = minLat; lat <= maxLat; lat += step) {
        for (let lon = minLon; lon <= maxLon; lon += step) {
          const result = idw(lat, lon, sourcePoints, effectivePower, effectiveMaxNeighbors, effectiveMaxDistKm);
          if (result === null) continue;

          let h3Id: string;
          try {
            h3Id = latLngToCell(lat, lon, effectiveResolution);
          } catch {
            continue;
          }
          if (!h3Id) continue;

          const existing = cellMap.get(h3Id);
          if (existing) {
            existing.totalValue += result.value;
            existing.count += 1;
            existing.neighborCount = Math.max(existing.neighborCount, result.neighborCount);
          } else {
            cellMap.set(h3Id, {totalValue: result.value, count: 1, neighborCount: result.neighborCount});
          }
        }
      }

      if (cellMap.size === 0) {
        return {
          llmResult: {
            success: false,
            details: 'IDW interpolation produced no output cells. All grid points may be outside the search radius.'
          }
        };
      }

      // Build output rows (average if multiple grid points mapped to the same H3 cell)
      const outputRows: Array<{
        h3_id: string;
        h3_resolution: number;
        idw_value: number;
        neighbor_count: number;
      }> = [];

      let valueMin = Infinity;
      let valueMax = -Infinity;

      for (const [h3Id, entry] of cellMap) {
        const avgValue = entry.totalValue / entry.count;
        const rounded = Math.round(avgValue * 1_000_000) / 1_000_000;
        if (rounded < valueMin) valueMin = rounded;
        if (rounded > valueMax) valueMax = rounded;
        outputRows.push({
          h3_id: h3Id,
          h3_resolution: effectiveResolution,
          idw_value: rounded,
          neighbor_count: entry.neighborCount
        });
      }

      const wantMap = showOnMap !== false;
      const targetName =
        String(newDatasetName || '').trim() ||
        `${dataset.label || dataset.id}_idw_h3r${effectiveResolution}`;
      const {label: resolvedTargetLabel, datasetId: resolvedTargetDatasetId} = getDatasetInfoByLabel(
        vis?.datasets || {},
        targetName,
        'qmap_idw'
      );

      return {
        llmResult: {
          success: true,
          sourcePoints: sourcePoints.length,
          outputCells: outputRows.length,
          resolution: effectiveResolution,
          valueRange: {min: valueMin, max: valueMax},
          outputDataset: resolvedTargetLabel,
          outputDatasetId: resolvedTargetDatasetId,
          details:
            `IDW interpolation from ${sourcePoints.length} source points produced ${outputRows.length} H3 cells ` +
            `at resolution ${effectiveResolution} (power=${effectivePower}, maxNeighbors=${effectiveMaxNeighbors}, ` +
            `maxDistKm=${effectiveMaxDistKm}). Value range: [${valueMin.toFixed(4)}, ${valueMax.toFixed(4)}].`
        },
        additionalData: {
          executionKey: makeExecutionKey('interpolate-idw'),
          outputRows,
          showOnMap: wantMap,
          newDatasetName: resolvedTargetLabel,
          newDatasetId: resolvedTargetDatasetId
        }
      };
    },
    component: function InterpolateIDWComponent({
      executionKey,
      outputRows,
      showOnMap,
      newDatasetName
    }: {
      executionKey?: string;
      outputRows: Array<{
        h3_id: string;
        h3_resolution: number;
        idw_value: number;
        neighbor_count: number;
      }>;
      showOnMap: boolean;
      newDatasetName: string;
    }) {
      const localDispatch = useDispatch<any>();
      const localVisState = useSelector(selectQMapVisState);
      const localDatasets = useSelector(selectQMapDatasets) as Record<string, any>;
      const {shouldSkip, complete} = useToolExecution({
        executionKey,
        executedToolComponentKeys: EXECUTED_TOOL_COMPONENT_KEYS,
        rememberExecutedToolComponentKey
      });

      useEffect(() => {
        if (shouldSkip()) return;
        if (!showOnMap || !outputRows?.length || !newDatasetName) return;
        const datasets = localVisState?.datasets || {};
        complete();

        const outRows: Array<Record<string, unknown>> = outputRows.map(row => ({
          h3_id: row.h3_id,
          h3_resolution: row.h3_resolution,
          idw_value: row.idw_value,
          neighbor_count: row.neighbor_count
        }));

        if (outRows.length) {
          upsertDerivedDatasetRows(localDispatch, datasets, newDatasetName, outRows, 'qmap_idw', true);
        }
      }, [
        localDispatch,
        localVisState,
        localDatasets,
        executionKey,
        outputRows,
        showOnMap,
        newDatasetName,
        shouldSkip,
        complete
      ]);

      return null;
    }
  };
}
