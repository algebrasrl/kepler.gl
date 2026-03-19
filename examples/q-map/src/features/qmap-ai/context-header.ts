import {findDatasetForLayer, extractLayerEffectiveFieldNames, getTooltipFieldNamesForDataset} from './dataset-utils';

export const QMAP_CONTEXT_HEADER = 'x-qmap-context';
export const Q_ASSISTANT_SESSION_HEADER = 'x-q-assistant-session-id';
export const Q_ASSISTANT_CHAT_HEADER = 'x-q-assistant-chat-id';
export const Q_ASSISTANT_RUNTIME_POLICY_HEADER = 'x-q-assistant-runtime-policy-summary';

export const QMAP_CONTEXT_HEADER_ENABLED =
  String(import.meta.env.VITE_QMAP_AI_SEND_CONTEXT_HEADER || 'true').toLowerCase() !== 'false';

export const QMAP_CONTEXT_MAX_HEADER_BYTES = Math.max(
  1024,
  Number(import.meta.env.VITE_QMAP_AI_CONTEXT_HEADER_MAX_BYTES || 6000) || 6000
);

export async function parseAssistantRequestBody(
  input: RequestInfo | URL,
  init?: RequestInit
): Promise<Record<string, unknown> | null> {
  const parseStringBody = (raw: string): Record<string, unknown> | null => {
    const text = String(raw || '').trim();
    if (!text) return null;
    try {
      const parsed = JSON.parse(text);
      if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) {
        return parsed as Record<string, unknown>;
      }
      return null;
    } catch {
      return null;
    }
  };

  if (typeof init?.body === 'string') {
    return parseStringBody(init.body);
  }

  if (typeof Request !== 'undefined' && input instanceof Request) {
    try {
      const clone = input.clone();
      const bodyText = await clone.text();
      return parseStringBody(bodyText);
    } catch {
      return null;
    }
  }
  return null;
}

export function isLevelLikeField(fieldName: string): boolean {
  const n = String(fieldName || '').toLowerCase();
  return n === 'lv' || n.endsWith('__lv') || n.endsWith('_lv') || /(^|[_-])level($|[_-])/.test(n);
}

export function isNameLikeField(fieldName: string): boolean {
  const n = String(fieldName || '').toLowerCase();
  return n === 'name' || n === 'name_en' || /(^|[_-])name($|[_-])/.test(n);
}

export function isPopulationLikeField(fieldName: string): boolean {
  const n = String(fieldName || '').toLowerCase();
  return n.includes('population') || n.includes('popolaz') || /(^|[_-])pop($|[_-])/.test(n);
}

export function toSafeString(value: unknown, maxLength = 180): string | null {
  if (value === null || value === undefined) return null;
  const text = String(value).trim();
  if (!text) return null;
  return text.length > maxLength ? `${text.slice(0, maxLength - 3)}...` : text;
}

export function buildQMapContextHeaderValue(visState: any, sessionId?: string): string | null {
  const datasets = (Object.values(visState?.datasets || {}) as any[]).slice(0, 8);
  const layers = (Array.isArray(visState?.layers) ? visState.layers : []).slice(0, 12);
  const filters = (Array.isArray(visState?.filters) ? visState.filters : []).slice(0, 20);
  const mapState = visState?.mapState || {};
  const datasetsMap = visState?.datasets || {};

  const context = {
    sessionId: toSafeString(sessionId, 120),
    map: {
      latitude: Number(mapState?.latitude),
      longitude: Number(mapState?.longitude),
      zoom: Number(mapState?.zoom)
    },
    datasets: datasets.map((dataset: any) => ({
      id: toSafeString(dataset?.id, 80),
      name: toSafeString(dataset?.label || dataset?.id, 120),
      rowCount: Number(dataset?.length || 0),
      fields: (dataset?.fields || [])
        .slice(0, 24)
        .map((f: any) => ({
          name: toSafeString(f?.name, 64),
          type: toSafeString(f?.type, 32)
        }))
        .filter((f: any) => Boolean(f.name))
    })),
    layers: layers.map((layer: any) => {
      const layerDataset = findDatasetForLayer(datasetsMap, layer);
      const activeFields = extractLayerEffectiveFieldNames(layer, layerDataset)
        .slice(0, 12)
        .map((name: string) => toSafeString(name, 64))
        .filter(Boolean);
      const tooltipFields = getTooltipFieldNamesForDataset(visState, layerDataset?.id || '')
        .slice(0, 12)
        .map((name: string) => toSafeString(name, 64))
        .filter(Boolean);
      return {
        id: toSafeString(layer?.id, 80),
        name: toSafeString(layer?.config?.label || layer?.id, 120),
        dataId: toSafeString(layer?.config?.dataId, 80),
        datasetName: toSafeString(layerDataset?.label || layerDataset?.id, 120),
        type: toSafeString(layer?.type, 40),
        visible: layer?.config?.isVisible !== false,
        activeFields,
        tooltipFields
      };
    }),
    filters: filters.map((filter: any) => ({
      id: toSafeString(filter?.id, 80),
      name: toSafeString(filter?.name, 80),
      type: toSafeString(filter?.type, 40),
      dataId: Array.isArray(filter?.dataId)
        ? filter.dataId.map((id: unknown) => toSafeString(id, 80)).filter(Boolean)
        : toSafeString(filter?.dataId, 80),
      valueCount: Array.isArray(filter?.value) ? filter.value.length : undefined
    })),
    counts: {
      datasets: Number(Object.keys(visState?.datasets || {}).length),
      layers: Number((visState?.layers || []).length),
      filters: Number((visState?.filters || []).length)
    }
  };

  let encoded = JSON.stringify(context);
  const bytes = new TextEncoder().encode(encoded).length;
  if (bytes <= QMAP_CONTEXT_MAX_HEADER_BYTES) {
    return encoded;
  }

  encoded = JSON.stringify({
    truncated: true,
    sessionId: context.sessionId,
    map: context.map,
    datasets: context.datasets.map((d: any) => ({
      id: d.id,
      name: d.name,
      rowCount: d.rowCount,
      fieldNames: (d.fields || []).map((f: any) => f?.name).filter(Boolean).slice(0, 16)
    })),
    layers: context.layers.map((l: any) => ({
      id: l.id,
      name: l.name,
      dataId: l.dataId,
      datasetName: l.datasetName,
      type: l.type,
      visible: l.visible,
      activeFields: (l.activeFields || []).slice(0, 6),
      tooltipFields: (l.tooltipFields || []).slice(0, 6)
    })),
    filters: context.filters.map((f: any) => ({
      id: f.id,
      name: f.name,
      type: f.type,
      dataId: f.dataId,
      valueCount: f.valueCount
    })),
    counts: context.counts
  });

  if (new TextEncoder().encode(encoded).length <= QMAP_CONTEXT_MAX_HEADER_BYTES) {
    return encoded;
  }

  return JSON.stringify({
    truncated: true,
    map: context.map,
    counts: context.counts
  });
}

export function buildRuntimeDatasetHints(visState: any): string {
  const datasets = Object.values(visState?.datasets || {}) as any[];
  const layers = Array.isArray(visState?.layers) ? visState.layers : [];
  const datasetsMap = visState?.datasets || {};
  if (!datasets.length) {
    return 'Runtime context: no datasets loaded in current map.';
  }

  const maxDatasets = 8;
  const datasetLines = datasets.slice(0, maxDatasets).map((dataset: any) => {
    const name = String(dataset?.label || dataset?.id || 'unknown');
    const fieldNames = (dataset?.fields || [])
      .map((f: any) => String(f?.name || '').trim())
      .filter(Boolean);
    const levelFields = fieldNames.filter((f: string) => isLevelLikeField(f));
    const nameFields = fieldNames.filter((f: string) => isNameLikeField(f));
    const timeFields = fieldNames.filter((f: string) => /(^|[_-])(time|timestamp|date|data|datetime)($|[_-])/.test(f.toLowerCase()));
    const h3Fields = fieldNames.filter((f: string) => /(^|[_-])h3(_id|__id)?($|[_-])/.test(f.toLowerCase()));
    const latFields = fieldNames.filter((f: string) => /(^|[_-])(lat|latitude)($|[_-])/.test(f.toLowerCase()));
    const lngFields = fieldNames.filter((f: string) => /(^|[_-])(lng|lon|longitude)($|[_-])/.test(f.toLowerCase()));
    const fieldPreview = fieldNames.slice(0, 14).join(', ');
    const roles = [
      nameFields.length ? `name=[${nameFields.join(', ')}]` : '',
      levelFields.length ? `level=[${levelFields.join(', ')}]` : '',
      timeFields.length ? `time=[${timeFields.join(', ')}]` : '',
      h3Fields.length ? `h3=[${h3Fields.join(', ')}]` : '',
      latFields.length && lngFields.length ? `point=[${latFields[0]}, ${lngFields[0]}]` : ''
    ]
      .filter(Boolean)
      .join(' ');
    return `- ${name} | fields: ${fieldPreview}${roles ? ` | roles: ${roles}` : ''}`;
  });

  const layerLines = layers.slice(0, 12).map((layer: any) => {
    const lname = String(layer?.config?.label || layer?.id || 'unknown-layer');
    const dtype = String(layer?.type || '');
    const layerDataset = findDatasetForLayer(datasetsMap, layer);
    const dataId = String(layerDataset?.id || layer?.config?.dataId || '');
    const activeFields = extractLayerEffectiveFieldNames(layer, layerDataset).slice(0, 8);
    const tooltipFields = getTooltipFieldNamesForDataset(visState, layerDataset?.id || '').slice(0, 8);
    const fieldInfo = activeFields.length ? ` | activeFields=[${activeFields.join(', ')}]` : '';
    const tooltipInfo = tooltipFields.length ? ` | tooltip=[${tooltipFields.join(', ')}]` : '';
    return `- ${lname} | type=${dtype} | dataId=${dataId}${fieldInfo}${tooltipInfo}`;
  });

  const moreDatasets = datasets.length > maxDatasets ? ` (${datasets.length - maxDatasets} more datasets hidden)` : '';
  const moreLayers = layers.length > 12 ? ` (${layers.length - 12} more layers hidden)` : '';
  return [
    'Runtime dataset context (authoritative; use exact datasetName/fieldName from here):',
    ...datasetLines,
    `Runtime layers:${moreLayers}`,
    ...layerLines,
    `Dataset count: ${datasets.length}${moreDatasets}`
  ].join('\n');
}
