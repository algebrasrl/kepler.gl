import {ALL_FIELD_TYPES} from '@kepler.gl/constants';
import {layerConfigChange, wrapTo} from '@kepler.gl/actions';
import {normalizeQMapPaletteName} from '../tool-schema-utils';
import {resolveDatasetFieldName} from './dataset-resolve';

export const SAFE_COLOR_RANGE = {
  name: 'qmap.safeColorRange',
  type: 'custom',
  category: 'Custom',
  colors: ['#f8fafc', '#cbd5e1', '#94a3b8', '#64748b', '#475569', '#334155']
};
export function parseHexColor(hex: string): [number, number, number] | null {
  const normalized = String(hex || '').trim().replace(/^#/, '');
  if (!/^[0-9a-fA-F]{6}$/.test(normalized)) {
    return null;
  }
  const r = parseInt(normalized.slice(0, 2), 16);
  const g = parseInt(normalized.slice(2, 4), 16);
  const b = parseInt(normalized.slice(4, 6), 16);
  return [r, g, b];
}

export function parseHexColorRgba(hex: string, alpha = 255): [number, number, number, number] | null {
  const rgb = parseHexColor(hex);
  if (!rgb) return null;
  const a = Math.max(0, Math.min(255, Math.round(alpha)));
  return [rgb[0], rgb[1], rgb[2], a];
}

export function ensureColorRange(range: any) {
  if (range && Array.isArray(range.colors) && range.colors.length > 0) {
    return range;
  }
  return SAFE_COLOR_RANGE;
}

export function toHex([r, g, b]: [number, number, number]): string {
  return (
    '#' +
    [r, g, b]
      .map(v => Math.max(0, Math.min(255, Math.round(v))).toString(16).padStart(2, '0'))
      .join('')
  );
}

export function buildLinearHexRange(lowHex: string, highHex: string, steps = 6): string[] {
  const low = parseHexColor(lowHex);
  const high = parseHexColor(highHex);
  if (!low || !high || steps < 2) {
    return [lowHex, highHex];
  }
  const colors: string[] = [];
  for (let i = 0; i < steps; i += 1) {
    const t = i / (steps - 1);
    colors.push(
      toHex([
        low[0] + (high[0] - low[0]) * t,
        low[1] + (high[1] - low[1]) * t,
        low[2] + (high[2] - low[2]) * t
      ])
    );
  }
  return colors;
}

export function getNamedPalette(name: string): string[] {
  const key = String(normalizeQMapPaletteName(name) || 'redGreen').toLowerCase();
  const palettes: Record<string, string[]> = {
    redgreen: ['#d73027', '#f46d43', '#fdae61', '#fee08b', '#66bd63', '#1a9850'],
    greenred: ['#1a9850', '#66bd63', '#fee08b', '#fdae61', '#f46d43', '#d73027'],
    bluered: ['#4575b4', '#74add1', '#abd9e9', '#fdae61', '#f46d43', '#d73027'],
    viridis: ['#440154', '#46327e', '#365c8d', '#277f8e', '#1fa187', '#4ac16d', '#a0da39', '#fde725'],
    magma: ['#000004', '#1b0c41', '#4f0a6d', '#781c6d', '#a52c60', '#cf4446', '#ed6925', '#fb9b06', '#f7d13d'],
    yellowred: ['#fff7bc', '#fee391', '#fec44f', '#fe9929', '#ec7014', '#cc4c02', '#993404'],
    yellowblue: ['#ffffd9', '#edf8b1', '#c7e9b4', '#7fcdbb', '#41b6c4', '#1d91c0', '#225ea8', '#253494']
  };
  return palettes[key] || palettes.redgreen;
}

export function inferFieldTypeFromValue(value: unknown): string {
  if (value === null || value === undefined) return ALL_FIELD_TYPES.string;
  if (typeof value === 'boolean') return ALL_FIELD_TYPES.boolean;
  if (typeof value === 'number') {
    return Number.isInteger(value) ? ALL_FIELD_TYPES.integer : ALL_FIELD_TYPES.real;
  }
  if (typeof value === 'object') {
    const asAny = value as any;
    if (asAny?.type === 'Feature' || asAny?.type === 'Polygon' || asAny?.type === 'MultiPolygon') {
      return ALL_FIELD_TYPES.geojson;
    }
  }
  return ALL_FIELD_TYPES.string;
}

export function isNumericFieldType(fieldType: unknown): boolean {
  const normalized = String(fieldType || '').toLowerCase();
  return (
    normalized === String(ALL_FIELD_TYPES.integer).toLowerCase() ||
    normalized === String(ALL_FIELD_TYPES.real).toLowerCase() ||
    normalized === 'number' ||
    normalized === 'float' ||
    normalized === 'double' ||
    normalized === 'decimal'
  );
}

export function isCategoricalJoinField(fieldName: string, fieldType: unknown): boolean {
  const normalizedName = String(fieldName || '').toLowerCase();
  if (!isNumericFieldType(fieldType)) return true;
  return (
    /(^|[_-])(code|class|category|cat|level|lv|id)($|[_-])/.test(normalizedName) ||
    normalizedName.includes('clc')
  );
}

export function normalizeFieldToken(value: unknown): string {
  return String(value || '')
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '');
}

export function isLikelyLandCoverDataset(dataset: any): boolean {
  const labelTokens = `${String(dataset?.label || '')} ${String(dataset?.id || '')}`.toLowerCase();
  if (/(^|[^a-z])(clc|corine|land cover|landcover|copertura suolo|cover)([^a-z]|$)/.test(labelTokens)) {
    return true;
  }
  const fields = Array.isArray(dataset?.fields) ? dataset.fields : [];
  const normalizedFieldNames = new Set(
    fields
      .map((field: any) => normalizeFieldToken(field?.name))
      .filter(Boolean)
  );
  if (normalizedFieldNames.has('code18')) {
    return true;
  }
  const hasClassCode =
    normalizedFieldNames.has('clccode') ||
    normalizedFieldNames.has('landcover') ||
    normalizedFieldNames.has('landcoverclass');
  const hasClassLabel =
    normalizedFieldNames.has('clcnameit') ||
    normalizedFieldNames.has('clcnameen') ||
    normalizedFieldNames.has('class') ||
    normalizedFieldNames.has('classname');
  return hasClassCode && hasClassLabel;
}

export function resolveLandCoverGroupByFields(dataset: any): string[] {
  const preferred = ['code_18', 'clc_name_it', 'clc_name_en', 'class_name', 'class'];
  return preferred
    .map(fieldName => resolveDatasetFieldName(dataset, fieldName))
    .filter((value, index, arr): value is string => Boolean(value) && arr.indexOf(value) === index);
}

export function findDatasetForLayer(datasets: any, layer: any): any | null {
  const dataId = layer?.config?.dataId;
  if (Array.isArray(dataId)) {
    const found = dataId
      .map(id => Object.values(datasets || {}).find((d: any) => String(d?.id || '') === String(id)))
      .find(Boolean);
    return (found as any) || null;
  }
  return (
    (Object.values(datasets || {}).find(
      (d: any) => String(d?.id || '') === String(dataId || '')
    ) as any) || null
  );
}

export function getDatasetFieldNames(dataset: any, limit = 256): string[] {
  return (dataset?.fields || [])
    .map((f: any) => String(f?.name || '').trim())
    .filter(Boolean)
    .slice(0, Math.max(1, Number(limit || 256)));
}

export function getTooltipFieldNamesForDataset(visState: any, datasetId: string): string[] {
  const normalizedDatasetId = String(datasetId || '').trim();
  if (!normalizedDatasetId) return [];
  const tooltipFields = visState?.interactionConfig?.tooltip?.config?.fieldsToShow?.[normalizedDatasetId];
  if (!Array.isArray(tooltipFields)) return [];
  return tooltipFields
    .map((entry: any) => String(entry?.name || '').trim())
    .filter(Boolean);
}

export function extractLayerEffectiveFieldNames(layer: any, dataset: any): string[] {
  const datasetFieldNames = getDatasetFieldNames(dataset, 1000);
  if (!datasetFieldNames.length) return [];
  const datasetFieldSet = new Set(datasetFieldNames);
  const found = new Set<string>();

  const visit = (node: any, depth = 0) => {
    if (depth > 5 || node === null || node === undefined) return;
    if (Array.isArray(node)) {
      node.slice(0, 40).forEach(item => visit(item, depth + 1));
      return;
    }
    if (typeof node !== 'object') return;

    const directName = typeof node?.name === 'string' ? String(node.name).trim() : '';
    if (directName && datasetFieldSet.has(directName)) {
      found.add(directName);
    }
    const directField = typeof node?.field === 'string' ? String(node.field).trim() : '';
    if (directField && datasetFieldSet.has(directField)) {
      found.add(directField);
    }
    const directFieldName = typeof node?.fieldName === 'string' ? String(node.fieldName).trim() : '';
    if (directFieldName && datasetFieldSet.has(directFieldName)) {
      found.add(directFieldName);
    }

    Object.entries(node).forEach(([key, value]) => {
      if (
        key === 'allData' ||
        key === 'dataContainer' ||
        key === 'gpuFilter' ||
        key === 'animationConfig' ||
        key === 'layerMeta'
      ) {
        return;
      }
      visit(value, depth + 1);
    });
  };

  visit(layer?.visualChannels);
  visit(layer?.columns);
  visit(layer?.config);

  return datasetFieldNames.filter((fieldName: string) => found.has(fieldName));
}

export function layerReferencesDataset(layer: any, datasetId: string): boolean {
  const dataId = layer?.config?.dataId;
  const normalizedDatasetId = String(datasetId || '');
  if (!normalizedDatasetId) return false;
  if (Array.isArray(dataId)) {
    return dataId.some(id => String(id || '') === normalizedDatasetId);
  }
  return String(dataId || '') === normalizedDatasetId;
}

export function hideLayersForDatasetIds(dispatchFn: any, layers: any[], datasetIds: string[]) {
  const normalized = Array.from(new Set((datasetIds || []).map(id => String(id || '')).filter(Boolean)));
  if (!normalized.length) return;
  (Array.isArray(layers) ? layers : []).forEach((layer: any) => {
    if (!layer || layer?.config?.isVisible === false) return;
    const shouldHide = normalized.some(datasetId => layerReferencesDataset(layer, datasetId));
    if (!shouldHide) return;
    dispatchFn(wrapTo('map', layerConfigChange(layer, {isVisible: false})));
  });
}

export function resolveStyleTargetLayer(
  layers: any[],
  dataset: any,
  layerName?: string
): {layer: any | null; details?: string} {
  const needleLayer = String(layerName || '').trim().toLowerCase();
  if (needleLayer) {
    const explicit =
      layers.find((l: any) => {
        const label = String(l?.config?.label || '').toLowerCase();
        const id = String(l?.id || '').toLowerCase();
        return label === needleLayer || id === needleLayer;
      }) || null;
    return {layer: explicit};
  }

  const byDataset = layers.filter((l: any) => String(l?.config?.dataId || '') === String(dataset?.id || ''));
  if (byDataset.length === 1) {
    return {layer: byDataset[0]};
  }

  const visible = byDataset.filter((l: any) => l?.config?.isVisible !== false);
  if (visible.length === 1) {
    return {layer: visible[0]};
  }

  if (byDataset.length > 1) {
    const candidates = byDataset
      .map((l: any) => `"${String(l?.config?.label || l?.id || '')}"`)
      .filter(Boolean)
      .slice(0, 8);
    return {
      layer: null,
      details:
        `Ambiguous target layer for dataset "${dataset?.label || dataset?.id}". ` +
        `Provide layerName/id explicitly. Candidates: ${candidates.join(', ')}.`
    };
  }

  return {layer: null};
}

export function normalizeLayerLabelForGrouping(value: unknown): string {
  return String(value || '')
    .toLowerCase()
    .replace(/\s*\(query\)\s*\[[^\]]+\]\s*$/, '')
    .replace(/\s+/g, ' ')
    .trim();
}

export type QMapRuntimeStep = {
  toolCallId: string;
  toolName: string;
  status: 'running' | 'success' | 'failed' | 'blocked';
  details: string;
};

export type QMapRankContext = {
  datasetKeys: string[];
  rows: Array<Record<string, unknown>>;
  metricFieldName: string;
  updatedAtMs: number;
};
