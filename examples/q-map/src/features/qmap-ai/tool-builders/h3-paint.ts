import {z} from 'zod';

import type {QMapToolContext} from '../context/tool-context';

function clampH3Resolution(value: number) {
  return Math.max(3, Math.min(11, Number(value || 7)));
}

export function createPaintQMapH3CellTool(ctx: QMapToolContext) {
  const {
    dispatch,
    getCurrentVisState,
    getH3PaintDataset,
    readH3PaintRows,
    upsertH3PaintHex,
    latLngToCell,
    paintDatasetLabelPrefix,
    paintDatasetIdPrefix
  } = ctx;
  return {
    description:
      'Paint a single H3 cell at a geographic coordinate (lat/lng) into Hex_Paint_r{resolution}.',
    parameters: z.object({
      lat: z.number().min(-90).max(90).describe('Latitude in WGS84'),
      lng: z.number().min(-180).max(180).describe('Longitude in WGS84'),
      resolution: z.number().min(3).max(11).optional().describe('H3 resolution, default 7')
    }),
    execute: async ({lat, lng, resolution}: any) => {
      const vis = getCurrentVisState();
      const datasets = vis?.datasets || {};
      const targetResolution = clampH3Resolution(Number(resolution || 7));
      const targetDataset = getH3PaintDataset(datasets, targetResolution);
      const beforeRows = readH3PaintRows(targetDataset, targetResolution);
      const beforeCount = beforeRows.length;
      const beforeIds = new Set(beforeRows.map((r: any) => r[0]));
      const targetHex = latLngToCell(Number(lat), Number(lng), targetResolution);
      const paintLabel = `${paintDatasetLabelPrefix}${targetResolution}`;
      const paintDatasetId = `${paintDatasetIdPrefix}${targetResolution}`;

      upsertH3PaintHex({
        dispatch,
        datasets,
        resolution: targetResolution,
        lng: Number(lng),
        lat: Number(lat)
      });

      const afterDatasets = getCurrentVisState()?.datasets || {};
      const afterDataset = getH3PaintDataset(afterDatasets, targetResolution);
      const afterRows = readH3PaintRows(afterDataset, targetResolution);
      const added = afterRows.length > beforeCount || !beforeIds.has(targetHex);

      return {
        llmResult: {
          success: true,
          dataset: paintLabel,
          datasetId: paintDatasetId,
          h3Id: targetHex,
          resolution: targetResolution,
          added,
          rowCount: afterRows.length,
          details: added
            ? `Painted H3 cell ${targetHex} into ${paintLabel}.`
            : `H3 cell ${targetHex} already exists in ${paintLabel}.`
        }
      };
    }
  };
}

export function createPaintQMapH3CellsTool(ctx: QMapToolContext) {
  const {
    dispatch,
    getCurrentVisState,
    getH3PaintDataset,
    readH3PaintRows,
    upsertH3PaintHex,
    latLngToCell,
    paintDatasetLabelPrefix,
    paintDatasetIdPrefix
  } = ctx;
  return {
    description:
      'Paint multiple H3 cells from a list of coordinates into Hex_Paint_r{resolution}.',
    parameters: z.object({
      resolution: z.number().min(3).max(11).describe('H3 resolution'),
      points: z
        .array(
          z.object({
            lat: z.number().min(-90).max(90),
            lng: z.number().min(-180).max(180)
          })
        )
        .min(1)
        .max(200)
        .describe('List of coordinates to convert/paint as H3 cells')
    }),
    execute: async ({resolution, points}: any) => {
      const targetResolution = clampH3Resolution(Number(resolution || 7));
      const beforeDatasets = getCurrentVisState()?.datasets || {};
      const beforeDataset = getH3PaintDataset(beforeDatasets, targetResolution);
      const beforeRows = readH3PaintRows(beforeDataset, targetResolution);
      const beforeIds = new Set(beforeRows.map((r: any) => r[0]));
      const paintLabel = `${paintDatasetLabelPrefix}${targetResolution}`;
      const paintDatasetId = `${paintDatasetIdPrefix}${targetResolution}`;

      const touchedHexes = new Set<string>();
      (points || []).forEach((point: {lat: number; lng: number}) => {
        const lat = Number(point?.lat);
        const lng = Number(point?.lng);
        if (!Number.isFinite(lat) || !Number.isFinite(lng)) return;
        touchedHexes.add(latLngToCell(lat, lng, targetResolution));
        upsertH3PaintHex({
          dispatch,
          datasets: getCurrentVisState()?.datasets || {},
          resolution: targetResolution,
          lng,
          lat
        });
      });

      const afterDatasets = getCurrentVisState()?.datasets || {};
      const afterDataset = getH3PaintDataset(afterDatasets, targetResolution);
      const afterRows = readH3PaintRows(afterDataset, targetResolution);
      let addedCount = 0;
      touchedHexes.forEach(h => {
        if (!beforeIds.has(h)) addedCount += 1;
      });

      return {
        llmResult: {
          success: true,
          dataset: paintLabel,
          datasetId: paintDatasetId,
          resolution: targetResolution,
          requestedPoints: Array.isArray(points) ? points.length : 0,
          distinctHexes: touchedHexes.size,
          added: addedCount,
          rowCount: afterRows.length,
          details: `Processed ${Array.isArray(points) ? points.length : 0} points at H3 r${targetResolution}; ${addedCount} new cells added.`
        }
      };
    }
  };
}

export function createPaintQMapH3RingTool(ctx: QMapToolContext) {
  const {
    dispatch,
    getCurrentVisState,
    getH3PaintDataset,
    readH3PaintRows,
    upsertH3PaintHex,
    isValidCell,
    getResolution,
    gridDisk,
    gridDistance,
    cellToLatLng,
    paintDatasetLabelPrefix,
    paintDatasetIdPrefix
  } = ctx;
  return {
    description:
      'Paint an H3 ring around a center H3 cell into Hex_Paint_r{resolution}. Radius k=1 means the 6 adjacent neighbors.',
    parameters: z.object({
      centerH3: z.string().describe('Center H3 cell id'),
      k: z.number().min(1).max(10).optional().describe('Ring radius, default 1'),
      resolution: z.number().min(3).max(11).optional().describe('Optional target resolution override'),
      includeCenter: z.boolean().optional().describe('Include center cell in output, default false')
    }),
    execute: async ({centerH3, k, resolution, includeCenter}: any) => {
      const center = String(centerH3 || '').trim();
      if (!center || !isValidCell(center)) {
        return {llmResult: {success: false, details: `Invalid center H3 id: "${centerH3}".`}};
      }

      const sourceResolution = Number(getResolution(center));
      const targetResolution = Number.isFinite(Number(resolution))
        ? clampH3Resolution(Number(resolution))
        : sourceResolution;
      if (targetResolution !== sourceResolution) {
        return {
          llmResult: {
            success: false,
            details: `Resolution mismatch: center cell is r${sourceResolution} but requested r${targetResolution}.`
          }
        };
      }

      const radius = Math.max(1, Math.min(10, Number(k || 1)));
      const disk: string[] = (gridDisk(center, radius) || []).map((id: string) => String(id));
      const ringOnly: string[] = disk.filter((id: string) => {
        if (includeCenter !== true && id === center) return false;
        try {
          return Number(gridDistance(center, id)) === radius;
        } catch {
          return false;
        }
      });

      if (!ringOnly.length) {
        return {llmResult: {success: false, details: `No H3 cells generated for ring k=${radius} around ${center}.`}};
      }

      const datasets = getCurrentVisState()?.datasets || {};
      const targetDataset = getH3PaintDataset(datasets, targetResolution);
      const beforeRows = readH3PaintRows(targetDataset, targetResolution);
      const beforeIds = new Set(beforeRows.map((r: any) => r[0]));
      const paintLabel = `${paintDatasetLabelPrefix}${targetResolution}`;
      const paintDatasetId = `${paintDatasetIdPrefix}${targetResolution}`;

      ringOnly.forEach((h3Id: string) => {
        const centerLatLng = cellToLatLng(h3Id);
        const lat = Number(centerLatLng?.[0]);
        const lng = Number(centerLatLng?.[1]);
        if (!Number.isFinite(lat) || !Number.isFinite(lng)) return;
        upsertH3PaintHex({
          dispatch,
          datasets: getCurrentVisState()?.datasets || {},
          resolution: targetResolution,
          lng,
          lat
        });
      });

      const afterDataset = getH3PaintDataset(getCurrentVisState()?.datasets || {}, targetResolution);
      const afterRows = readH3PaintRows(afterDataset, targetResolution);
      let added = 0;
      ringOnly.forEach((h3Id: string) => {
        if (!beforeIds.has(h3Id)) added += 1;
      });

      return {
        llmResult: {
          success: true,
          dataset: paintLabel,
          datasetId: paintDatasetId,
          centerH3: center,
          k: radius,
          includeCenter: includeCenter === true,
          ringSize: ringOnly.length,
          added,
          rowCount: afterRows.length,
          details: `Painted H3 ring k=${radius} around ${center}: ${added} new cells added (${ringOnly.length} ring cells).`
        }
      };
    }
  };
}
