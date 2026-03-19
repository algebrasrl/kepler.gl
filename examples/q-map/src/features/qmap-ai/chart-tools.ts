type ChartToolMeta = {
  key: string;
  label: string;
  safeDefaultEnabled: boolean;
  envToggle: string;
};

export type QMapChartToolState = {
  key: string;
  label: string;
  available: boolean;
  enabled: boolean;
  reason: string;
};

export type QMapChartMode = 'safe' | 'full' | 'timeseries-safe';

type TimeSeriesEligibility = {
  hasValidTimeField: boolean;
  datasetsChecked: number;
  matchingDatasets: Array<{
    dataset: string;
    field: string;
    validSamples: number;
    totalSamples: number;
  }>;
  reason: string;
};

const KNOWN_CHART_TOOLS: Record<
  string,
  {label: string; safeDefaultEnabled: boolean}
> = {
  bubbleChartTool: {label: 'Bubble Chart', safeDefaultEnabled: true},
  histogramTool: {label: 'Histogram', safeDefaultEnabled: true},
  boxplotTool: {label: 'Box Plot', safeDefaultEnabled: true},
  pcpTool: {label: 'Parallel Coordinates', safeDefaultEnabled: true},
  scatterplotTool: {label: 'Scatterplot', safeDefaultEnabled: false},
  lineChartTool: {label: 'Line Chart', safeDefaultEnabled: false},
  wordCloudTool: {label: 'Word Cloud', safeDefaultEnabled: true},
  categoryBarsTool: {label: 'Category Bars', safeDefaultEnabled: true}
};

function parseOptionalBool(raw: string | undefined): boolean | null {
  if (raw === undefined) return null;
  const normalized = String(raw).trim().toLowerCase();
  if (['1', 'true', 'yes', 'on'].includes(normalized)) return true;
  if (['0', 'false', 'no', 'off'].includes(normalized)) return false;
  return null;
}

function toEnvToggleName(toolKey: string): string {
  const upperSnake = String(toolKey || '')
    .replace(/([a-z0-9])([A-Z])/g, '$1_$2')
    .replace(/[^a-zA-Z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '')
    .toUpperCase();
  return `VITE_QMAP_AI_ENABLE_${upperSnake}`;
}

function toChartLabel(toolKey: string): string {
  const known = KNOWN_CHART_TOOLS[toolKey];
  if (known?.label) return known.label;
  const base = String(toolKey || '')
    .replace(/Tool$/i, '')
    .replace(/([a-z0-9])([A-Z])/g, '$1 $2')
    .replace(/[_-]+/g, ' ')
    .trim();
  return base
    .split(/\s+/)
    .filter(Boolean)
    .map(token => token.charAt(0).toUpperCase() + token.slice(1))
    .join(' ');
}

function isChartToolKey(toolKey: string): boolean {
  const key = String(toolKey || '').toLowerCase();
  if (!key.endsWith('tool')) return false;
  if (KNOWN_CHART_TOOLS[toolKey]) return true;
  return key.includes('chart') || key.includes('plot') || key.includes('histogram') || key.includes('boxplot');
}

function shouldEnableByDefaultInSafeMode(toolKey: string): boolean {
  const known = KNOWN_CHART_TOOLS[toolKey];
  if (known) return known.safeDefaultEnabled;
  const key = String(toolKey || '').toLowerCase();
  if (key.includes('line') || key.includes('scatter') || key.includes('timeseries')) {
    return false;
  }
  return true;
}

function buildChartToolMeta(baseTools: Record<string, any>): ChartToolMeta[] {
  const toolKeys = Object.keys(baseTools || {});
  return toolKeys
    .filter(isChartToolKey)
    .map(key => ({
      key,
      label: toChartLabel(key),
      safeDefaultEnabled: shouldEnableByDefaultInSafeMode(key),
      envToggle: toEnvToggleName(key)
    }));
}

function resolveChartMode(): QMapChartMode {
  const mode = String(import.meta.env.VITE_QMAP_AI_CHARTS_MODE || 'safe').toLowerCase();
  if (mode === 'timeseries-safe') return 'timeseries-safe';
  return mode === 'full' ? 'full' : 'safe';
}

function isLikelyTimeField(field: any): boolean {
  const fieldName = String(field?.name || '').toLowerCase();
  const fieldType = String(field?.type || '').toLowerCase();
  if (/time|timestamp|date|datetime/.test(fieldType)) return true;
  return /(^|[_-])(time|timestamp|date|data|datetime)($|[_-])/.test(fieldName);
}

function isValidTimeValue(value: unknown): boolean {
  if (value === null || value === undefined || String(value).trim() === '') return false;
  if (typeof value === 'number' && Number.isFinite(value)) {
    if (value > 1e12 && value < 1e14) return true; // ms epoch
    if (value > 1e9 && value < 1e11) return true; // s epoch
    return false;
  }
  if (value instanceof Date && Number.isFinite(value.getTime())) return true;
  const text = String(value).trim();
  if (!text) return false;
  if (!/[-/:T]/.test(text) && !/^\d{8,}$/.test(text)) return false;
  const parsed = Date.parse(text);
  return Number.isFinite(parsed);
}

function resolveTimeSeriesEligibility(datasetsById: Record<string, any> | undefined): TimeSeriesEligibility {
  const datasets = Object.values(datasetsById || {}) as any[];
  const matching: TimeSeriesEligibility['matchingDatasets'] = [];
  datasets.forEach(dataset => {
    const fields = (dataset?.fields || []).filter(isLikelyTimeField);
    if (!fields.length) return;
    const allIndexes = Array.isArray(dataset?.allIndexes)
      ? dataset.allIndexes
      : Array.from({length: Number(dataset?.length || 0)}, (_, i) => i);
    const sampledIndexes = allIndexes.slice(0, 400);
    if (!sampledIndexes.length) return;
    fields.slice(0, 8).forEach((field: any) => {
      const fieldName = String(field?.name || '');
      if (!fieldName) return;
      let total = 0;
      let valid = 0;
      sampledIndexes.forEach((rowIdx: number) => {
        const raw = dataset.getValue(fieldName, rowIdx);
        if (raw === null || raw === undefined || String(raw).trim() === '') return;
        total += 1;
        if (isValidTimeValue(raw)) {
          valid += 1;
        }
      });
      if (total >= 8 && valid >= 6 && valid / total >= 0.6) {
        matching.push({
          dataset: String(dataset?.label || dataset?.id || 'unknown'),
          field: fieldName,
          validSamples: valid,
          totalSamples: total
        });
      }
    });
  });
  if (!matching.length) {
    return {
      hasValidTimeField: false,
      datasetsChecked: datasets.length,
      matchingDatasets: [],
      reason: 'No valid time field detected in loaded datasets.'
    };
  }
  return {
    hasValidTimeField: true,
    datasetsChecked: datasets.length,
    matchingDatasets: matching,
    reason: `Detected valid time fields in ${matching.length} dataset/field candidates.`
  };
}

function resolveEnabled(
  meta: ChartToolMeta,
  mode: QMapChartMode,
  timeSeriesEligibility: TimeSeriesEligibility
): {enabled: boolean; reason: string} {
  const envValue = parseOptionalBool((import.meta.env as any)[meta.envToggle]);
  if (envValue !== null) {
    return {
      enabled: envValue,
      reason: `${meta.envToggle}=${String(envValue)}`
    };
  }
  if (mode === 'full') {
    return {enabled: true, reason: 'charts mode=full'};
  }
  if (mode === 'timeseries-safe' && meta.key === 'lineChartTool') {
    if (timeSeriesEligibility.hasValidTimeField) {
      return {
        enabled: true,
        reason: 'charts mode=timeseries-safe and valid time field detected'
      };
    }
    return {
      enabled: false,
      reason: 'charts mode=timeseries-safe but no valid time field detected'
    };
  }
  return {
    enabled: meta.safeDefaultEnabled,
    reason: meta.safeDefaultEnabled ? 'charts mode=safe default enabled' : 'charts mode=safe default disabled'
  };
}

export function applyQMapChartToolPolicy(
  baseTools: Record<string, any>,
  datasetsById: Record<string, any> | undefined,
  chartToolNormalizer?: (tool: any) => any
): {
  tools: Record<string, any>;
  states: QMapChartToolState[];
  mode: QMapChartMode;
  timeSeriesEligibility: TimeSeriesEligibility;
} {
  const mode = resolveChartMode();
  const chartToolMeta = buildChartToolMeta(baseTools);
  const timeSeriesEligibility = resolveTimeSeriesEligibility(datasetsById);
  const nextTools: Record<string, any> = {...(baseTools || {})};
  const states: QMapChartToolState[] = chartToolMeta.map(meta => {
    const available = meta.key in nextTools;
    const policy = resolveEnabled(meta, mode, timeSeriesEligibility);
    if (!available) {
      return {
        key: meta.key,
        label: meta.label,
        available: false,
        enabled: false,
        reason: 'not provided by base runtime'
      };
    }
    if (!policy.enabled) {
      delete nextTools[meta.key];
      return {
        key: meta.key,
        label: meta.label,
        available: true,
        enabled: false,
        reason: policy.reason
      };
    }
    if (chartToolNormalizer) {
      nextTools[meta.key] = chartToolNormalizer(nextTools[meta.key]);
    }
    return {
      key: meta.key,
      label: meta.label,
      available: true,
      enabled: true,
      reason: policy.reason
    };
  });
  return {tools: nextTools, states, mode, timeSeriesEligibility};
}
