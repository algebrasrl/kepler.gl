import {z} from 'zod';
import type {Feature, Polygon, LineString, Point, Position} from 'geojson';

import type {QMapToolContext} from '../context/tool-context';
import {upsertDerivedDatasetRows} from '../dataset-upsert';

/**
 * Compute the geographic centroid of a dataset's geometries.
 * Returns {lon, lat} that can be piped into circleBufferFromPoint or used
 * as spatialBbox center for thematic queries.
 */
export function createCentroidOfDatasetTool(ctx: QMapToolContext) {
  const {
    getCurrentVisState,
    resolveDatasetByName,
    resolveGeojsonFieldName,
    resolveDatasetFieldName,
    getDatasetIndexes,
    parseGeoJsonLike,
    geometryToBbox,
    turfCentroid,
    yieldToMainThread,
    defaultChunkSize
  } = ctx;

  return {
    description:
      'Compute the geographic centroid (center point) of a loaded dataset. ' +
      'Use this to find the center of visible layers before creating a circular buffer for spatial queries.',
    parameters: z.object({
      datasetName: z
        .string()
        .describe('Exact dataset name or datasetRef (id:...) from listQMapDatasets.'),
      geometryField: z
        .string()
        .optional()
        .describe('Optional geometry field name; default auto-detect (_geojson).')
    }),
    execute: async ({datasetName, geometryField}: {datasetName: string; geometryField?: string}) => {
      const currentVisState = getCurrentVisState();
      const dataset = resolveDatasetByName(currentVisState?.datasets || {}, datasetName);
      if (!dataset?.id) {
        return {llmResult: {success: false, details: `Dataset "${datasetName}" not found.`}};
      }
      const geoField =
        resolveGeojsonFieldName(dataset, String(geometryField || '')) ||
        resolveDatasetFieldName(dataset, '_geojson') ||
        null;
      if (!geoField) {
        return {
          llmResult: {
            success: false,
            details: `No geometry field found in dataset "${dataset.label || dataset.id}".`
          }
        };
      }

      const idx = getDatasetIndexes(dataset).slice(0, 100000);
      let minLon = Infinity;
      let minLat = Infinity;
      let maxLon = -Infinity;
      let maxLat = -Infinity;
      let usedRows = 0;
      const loopYield = Math.max(100, defaultChunkSize);

      for (let i = 0; i < idx.length; i++) {
        const parsed = parseGeoJsonLike(dataset.getValue(geoField, idx[i]));
        const geom = parsed?.type === 'Feature' ? parsed.geometry : parsed;
        const bbox = geometryToBbox(geom);
        if (bbox) {
          minLon = Math.min(minLon, bbox[0]);
          minLat = Math.min(minLat, bbox[1]);
          maxLon = Math.max(maxLon, bbox[2]);
          maxLat = Math.max(maxLat, bbox[3]);
          usedRows++;
        }
        if (i > 0 && i % loopYield === 0) {
          await yieldToMainThread();
        }
      }

      if (!Number.isFinite(minLon)) {
        return {
          llmResult: {
            success: false,
            details: `Could not compute centroid: no valid geometries in "${dataset.label || dataset.id}".`
          }
        };
      }

      // Build bbox polygon and compute centroid via turf.
      const bboxPolygon = {
        type: 'Feature' as const,
        properties: {},
        geometry: {
          type: 'Polygon' as const,
          coordinates: [
            [
              [minLon, minLat],
              [maxLon, minLat],
              [maxLon, maxLat],
              [minLon, maxLat],
              [minLon, minLat]
            ]
          ]
        }
      };
      const center = turfCentroid(bboxPolygon);
      const [lon, lat] = center.geometry.coordinates;

      return {
        llmResult: {
          success: true,
          dataset: dataset.label || dataset.id,
          usedRows,
          longitude: Number(lon.toFixed(8)),
          latitude: Number(lat.toFixed(8)),
          spatialBbox: [
            Number(minLon.toFixed(8)),
            Number(minLat.toFixed(8)),
            Number(maxLon.toFixed(8)),
            Number(maxLat.toFixed(8))
          ],
          details: `Centroid of "${dataset.label || dataset.id}": [${lon.toFixed(6)}, ${lat.toFixed(6)}] (from ${usedRows} geometries).`
        }
      };
    }
  };
}

/**
 * Create a circular buffer polygon dataset from a point + radius.
 * The resulting dataset can be used as a clip mask or spatial filter
 * for thematic queries (Natura 2000, CLC, ISPRA, etc.).
 */
export function createCircleBufferFromPointTool(ctx: QMapToolContext) {
  const {dispatch, getCurrentVisState, turfBuffer} = ctx;

  return {
    description:
      'Create a circular buffer polygon dataset from a geographic point and radius. ' +
      'Use this after centroidOfDataset to draw a search area, then clip or query thematic datasets within it.',
    parameters: z.object({
      longitude: z.number().describe('Center longitude (WGS84).'),
      latitude: z.number().describe('Center latitude (WGS84).'),
      radiusKm: z
        .number()
        .min(0.01)
        .max(500)
        .optional()
        .default(10)
        .describe('Buffer radius in kilometers (default 10).'),
      radiusM: z
        .number()
        .min(1)
        .max(500000)
        .optional()
        .describe('Buffer radius in meters. If provided, overrides radiusKm.'),
      showOnMap: z
        .boolean()
        .optional()
        .default(true)
        .describe('Whether to auto-create a layer for the buffer polygon (default true).'),
      newDatasetName: z
        .string()
        .optional()
        .describe('Optional name for the new dataset. Default: "Buffer_{radius}km".')
    }),
    execute: async ({
      longitude,
      latitude,
      radiusKm,
      radiusM,
      showOnMap,
      newDatasetName
    }: {
      longitude: number;
      latitude: number;
      radiusKm?: number;
      radiusM?: number;
      showOnMap?: boolean;
      newDatasetName?: string;
    }) => {
      const effectiveRadiusKm = radiusM ? radiusM / 1000 : radiusKm || 10;
      const units = 'kilometers';

      const point = {
        type: 'Feature' as const,
        properties: {},
        geometry: {type: 'Point' as const, coordinates: [longitude, latitude]}
      };

      let buffered: any;
      try {
        buffered = turfBuffer(point as any, effectiveRadiusKm, {units});
      } catch (err: any) {
        return {
          llmResult: {
            success: false,
            details: `Failed to create buffer: ${err?.message || String(err)}`
          }
        };
      }
      if (!buffered?.geometry) {
        return {llmResult: {success: false, details: 'turfBuffer returned null geometry.'}};
      }

      const radiusLabel = effectiveRadiusKm >= 1 ? `${effectiveRadiusKm}km` : `${Math.round(effectiveRadiusKm * 1000)}m`;
      const dsName = (newDatasetName || `Buffer_${radiusLabel}`).trim();

      const datasets = getCurrentVisState()?.datasets || {};
      upsertDerivedDatasetRows(
        dispatch,
        datasets,
        dsName,
        [
          {
            _geojson: buffered.geometry,
            center_lon: longitude,
            center_lat: latitude,
            radius_km: effectiveRadiusKm,
            label: dsName
          }
        ],
        'qmap_circle_buffer',
        showOnMap !== false
      );

      return {
        llmResult: {
          success: true,
          datasetName: dsName,
          center: [longitude, latitude],
          radiusKm: effectiveRadiusKm,
          showOnMap: showOnMap !== false,
          details: `Created circular buffer "${dsName}" (${radiusLabel} radius around [${longitude.toFixed(4)}, ${latitude.toFixed(4)}]). Use this dataset as clip geometry for spatial queries.`
        }
      };
    }
  };
}

// ─── Helpers for metric computation ──────────────────────────────────────────

function haversineDistance(a: Position, b: Position): number {
  const toRad = (d: number) => (d * Math.PI) / 180;
  const R = 6371000; // Earth radius in metres
  const dLat = toRad(b[1] - a[1]);
  const dLon = toRad(b[0] - a[0]);
  const sinLat = Math.sin(dLat / 2);
  const sinLon = Math.sin(dLon / 2);
  const h = sinLat * sinLat + Math.cos(toRad(a[1])) * Math.cos(toRad(b[1])) * sinLon * sinLon;
  return 2 * R * Math.asin(Math.sqrt(h));
}

function lineLength(coords: Position[]): number {
  let total = 0;
  for (let i = 1; i < coords.length; i++) {
    total += haversineDistance(coords[i - 1], coords[i]);
  }
  return total;
}

function ringArea(ring: Position[]): number {
  // Shoelace on projected coords — rough but adequate for metric annotation.
  // For accurate area we delegate to turfArea when available.
  let sum = 0;
  for (let i = 0; i < ring.length - 1; i++) {
    sum += ring[i][0] * ring[i + 1][1] - ring[i + 1][0] * ring[i][1];
  }
  return Math.abs(sum / 2);
}

/**
 * Programmatically draw a geometry (point, line, polygon, or circle) and
 * materialise it as a new dataset with useful metric properties.
 */
export function createDrawGeometryToDatasetTool(ctx: QMapToolContext) {
  const {dispatch, getCurrentVisState, turfBuffer, turfArea} = ctx;

  return {
    description:
      'Programmatically draw a geometry (point, line, polygon or circle) and save it as a new dataset. ' +
      'Includes computed metrics: area_m2/area_km2 for polygons/circles, length_m for lines, radius_km for circles. ' +
      'Use the resulting dataset for clip, spatial join, or thematic queries.',
    parameters: z.object({
      type: z
        .enum(['point', 'line', 'polygon', 'circle'])
        .describe('Geometry type to create.'),
      coordinates: z
        .array(z.array(z.number()))
        .describe(
          'Array of [lon, lat] pairs. ' +
          'Point: [[lon,lat]]. Line: [[lon,lat],...]. ' +
          'Polygon: [[lon,lat],...] (auto-closed). ' +
          'Circle: [[centerLon, centerLat]].'
        ),
      radiusKm: z
        .number()
        .min(0.01)
        .max(500)
        .optional()
        .describe('Radius in km (only for type=circle).'),
      radiusM: z
        .number()
        .min(1)
        .max(500000)
        .optional()
        .describe('Radius in metres (only for type=circle, overrides radiusKm).'),
      label: z.string().optional().describe('Optional label property for the feature.'),
      showOnMap: z.boolean().optional().default(true),
      newDatasetName: z.string().optional().describe('Dataset name. Default auto-generated.')
    }),
    execute: async ({
      type,
      coordinates,
      radiusKm,
      radiusM,
      label,
      showOnMap,
      newDatasetName
    }: {
      type: 'point' | 'line' | 'polygon' | 'circle';
      coordinates: number[][];
      radiusKm?: number;
      radiusM?: number;
      label?: string;
      showOnMap?: boolean;
      newDatasetName?: string;
    }) => {
      if (!coordinates?.length) {
        return {llmResult: {success: false, details: 'coordinates array is empty.'}};
      }

      let geometry: Point | LineString | Polygon;
      const metrics: Record<string, number | string> = {};

      switch (type) {
        case 'point': {
          const [lon, lat] = coordinates[0];
          geometry = {type: 'Point', coordinates: [lon, lat]};
          metrics.lon = lon;
          metrics.lat = lat;
          break;
        }
        case 'line': {
          if (coordinates.length < 2) {
            return {llmResult: {success: false, details: 'Line requires at least 2 coordinate pairs.'}};
          }
          geometry = {type: 'LineString', coordinates: coordinates.map(c => [c[0], c[1]])};
          metrics.length_m = Math.round(lineLength(geometry.coordinates));
          metrics.length_km = Number((metrics.length_m as number / 1000).toFixed(3));
          break;
        }
        case 'polygon': {
          if (coordinates.length < 3) {
            return {llmResult: {success: false, details: 'Polygon requires at least 3 coordinate pairs.'}};
          }
          const ring = coordinates.map(c => [c[0], c[1]] as Position);
          // Auto-close if needed.
          if (ring[0][0] !== ring[ring.length - 1][0] || ring[0][1] !== ring[ring.length - 1][1]) {
            ring.push([...ring[0]]);
          }
          geometry = {type: 'Polygon', coordinates: [ring]};
          const feat: Feature<Polygon> = {type: 'Feature', properties: {}, geometry};
          const areaM2 = typeof turfArea === 'function' ? turfArea(feat) : ringArea(ring) * 1e10;
          metrics.area_m2 = Math.round(areaM2);
          metrics.area_km2 = Number((areaM2 / 1e6).toFixed(4));
          metrics.perimeter_m = Math.round(lineLength(ring));
          break;
        }
        case 'circle': {
          const [cLon, cLat] = coordinates[0];
          const effectiveKm = radiusM ? radiusM / 1000 : radiusKm || 10;
          const pt = {type: 'Feature' as const, properties: {}, geometry: {type: 'Point' as const, coordinates: [cLon, cLat]}};
          let buffered: any;
          try {
            buffered = turfBuffer(pt as any, effectiveKm, {units: 'kilometers'});
          } catch (err: any) {
            return {llmResult: {success: false, details: `turfBuffer failed: ${err?.message || err}`}};
          }
          if (!buffered?.geometry) {
            return {llmResult: {success: false, details: 'turfBuffer returned null.'}};
          }
          geometry = buffered.geometry;
          metrics.center_lon = cLon;
          metrics.center_lat = cLat;
          metrics.radius_km = effectiveKm;
          metrics.radius_m = Math.round(effectiveKm * 1000);
          const circFeat: Feature<Polygon> = {type: 'Feature', properties: {}, geometry: geometry as Polygon};
          const circArea = typeof turfArea === 'function' ? turfArea(circFeat) : Math.PI * (effectiveKm * 1000) ** 2;
          metrics.area_m2 = Math.round(circArea);
          metrics.area_km2 = Number((circArea / 1e6).toFixed(4));
          break;
        }
        default:
          return {llmResult: {success: false, details: `Unknown geometry type: ${type}`}};
      }

      const dsName = (newDatasetName || `Drawn_${type}`).trim();
      const row: Record<string, unknown> = {
        _geojson: geometry,
        geometry_type: type,
        ...metrics
      };
      if (label) row.label = label;

      const datasets = getCurrentVisState()?.datasets || {};
      upsertDerivedDatasetRows(dispatch, datasets, dsName, [row], 'qmap_draw_geometry', showOnMap !== false);

      const metricsStr = Object.entries(metrics)
        .map(([k, v]) => `${k}=${v}`)
        .join(', ');

      return {
        llmResult: {
          success: true,
          datasetName: dsName,
          geometryType: type,
          metrics,
          showOnMap: showOnMap !== false,
          details: `Created ${type} dataset "${dsName}" (${metricsStr}). Use this dataset as geometry input for spatial queries.`
        }
      };
    }
  };
}
