import {extendedTool} from '../tool-shim';
import {z} from 'zod';

import type {QMapToolContext} from '../context/tool-context';

export function createDeriveQMapDatasetBboxTool(ctx: QMapToolContext) {
  const {
    getCurrentVisState,
    resolveDatasetByName,
    resolveGeojsonFieldName,
    resolveDatasetFieldName,
    getDatasetIndexes,
    parseGeoJsonLike,
    geometryToBbox,
    yieldToMainThread,
    defaultChunkSize
  } = ctx;
  return extendedTool({
    description:
      'Derive bounding box [minLon,minLat,maxLon,maxLat] from a loaded geometry dataset, for backend spatial prefilter queries.',
    parameters: z.object({
      datasetName: z.string().describe('Exact dataset name from listQMapDatasets'),
      geometryField: z.string().optional().describe('Optional geometry field, default auto (_geojson/geojson).'),
      sampleLimit: z.number().min(1).max(200000).optional().describe('Rows sampled to compute bbox, default 100000.'),
      paddingPercent: z
        .number()
        .min(0)
        .max(20)
        .optional()
        .describe('Optional bbox padding in percent, default 0.')
    }),
    execute: async ({datasetName, geometryField, sampleLimit, paddingPercent}) => {
      const currentVisState = getCurrentVisState();
      const dataset = resolveDatasetByName(currentVisState?.datasets || {}, datasetName);
      if (!dataset?.id) {
        return {llmResult: {success: false, details: `Dataset "${datasetName}" not found.`}};
      }
      const resolvedGeometryField =
        resolveGeojsonFieldName(dataset, String(geometryField || '')) ||
        resolveDatasetFieldName(dataset, '_geojson') ||
        null;
      if (!resolvedGeometryField) {
        return {
          llmResult: {
            success: false,
            details: `No geometry field found in dataset "${dataset.label || dataset.id}".`
          }
        };
      }

      const idx = getDatasetIndexes(dataset).slice(0, Math.max(1, Number(sampleLimit || 100000)));
      let minLon = Number.POSITIVE_INFINITY;
      let minLat = Number.POSITIVE_INFINITY;
      let maxLon = Number.NEGATIVE_INFINITY;
      let maxLat = Number.NEGATIVE_INFINITY;
      let usedRows = 0;

      const loopYieldEvery = Math.max(100, defaultChunkSize);
      for (let i = 0; i < idx.length; i += 1) {
        const rowIdx = idx[i];
        const parsed = parseGeoJsonLike(dataset.getValue(resolvedGeometryField, rowIdx));
        const geometry = parsed?.type === 'Feature' ? parsed.geometry : parsed;
        const bbox = geometryToBbox(geometry);
        if (bbox) {
          minLon = Math.min(minLon, bbox[0]);
          minLat = Math.min(minLat, bbox[1]);
          maxLon = Math.max(maxLon, bbox[2]);
          maxLat = Math.max(maxLat, bbox[3]);
          usedRows += 1;
        }
        if (i > 0 && i % loopYieldEvery === 0) {
          await yieldToMainThread();
        }
      }

      if (!Number.isFinite(minLon) || !Number.isFinite(minLat) || !Number.isFinite(maxLon) || !Number.isFinite(maxLat)) {
        return {
          llmResult: {
            success: false,
            details: `Could not compute bbox from dataset "${dataset.label || dataset.id}" geometry rows.`
          }
        };
      }

      const padPct = Math.max(0, Number(paddingPercent || 0));
      const lonSpan = maxLon - minLon;
      const latSpan = maxLat - minLat;
      const lonPad = (lonSpan * padPct) / 100;
      const latPad = (latSpan * padPct) / 100;
      const bbox: [number, number, number, number] = [
        Number((minLon - lonPad).toFixed(8)),
        Number((minLat - latPad).toFixed(8)),
        Number((maxLon + lonPad).toFixed(8)),
        Number((maxLat + latPad).toFixed(8))
      ];

      return {
        llmResult: {
          success: true,
          dataset: dataset.label || dataset.id,
          geometryField: resolvedGeometryField,
          sampledRows: idx.length,
          usedRows,
          spatialBbox: bbox,
          details: `Computed spatialBbox for "${dataset.label || dataset.id}" from ${usedRows} geometry rows.`
        }
      };
    }
  });
}
