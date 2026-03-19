import {cellArea} from 'h3-js-v4';
import {extent as d3Extent, mean as d3Mean, median as d3Median, quantileSorted as d3QuantileSorted} from 'd3-array';
import {featureAreaM2} from './geometry-ops';
import {
  getDatasetIndexes,
  resolveDatasetFieldName,
  resolveH3FieldName,
  resolveGeojsonFieldName,
  toTurfPolygonFeature,
  parseCoordinateValue,
  normalizeH3Key,
  h3CellToPolygonFeature,
  normalizeThresholds
} from './dataset-utils';
import {parseGeoJsonLike} from '../../geo';

export const AREA_FIELD_ALIASES = [
  'area',
  'area_m2',
  'area_mq',
  'area_km2',
  'surface',
  'superficie',
  'polygon_area'
];

export function isAreaLikeFieldName(name?: string | null): boolean {
  const normalized = String(name || '').trim().toLowerCase();
  if (!normalized) return false;
  if (AREA_FIELD_ALIASES.includes(normalized)) return true;
  return /(area|surface|superficie)/.test(normalized);
}

export function resolveAreaLikeFieldName(dataset: any): string | null {
  const fields = Array.isArray(dataset?.fields) ? dataset.fields : [];
  const exactAlias = fields.find((f: any) => AREA_FIELD_ALIASES.includes(String(f?.name || '').toLowerCase()));
  if (exactAlias?.name) return String(exactAlias.name);
  const fuzzy = fields.find((f: any) => isAreaLikeFieldName(String(f?.name || '')));
  return fuzzy?.name ? String(fuzzy.name) : null;
}

export type AreaUnit = 'm2' | 'ha' | 'km2';
export type NormalizedDenominatorMode = 'field' | 'derived_geojson_area' | 'derived_h3_area';

export type NormalizedDenominatorPlan = {
  mode: NormalizedDenominatorMode;
  requestedFieldName: string;
  fieldName: string | null;
  geometryField: string | null;
  h3Field: string | null;
  sourceAreaUnit: AreaUnit | null;
  targetAreaUnit: AreaUnit | null;
  usedFallback: boolean;
};

export function inferAreaUnitFromFieldName(fieldName?: string | null): AreaUnit {
  const raw = String(fieldName || '').trim().toLowerCase();
  if (!raw) return 'm2';
  if (/(^|[_\W])(km2|kmq|sqkm|square_?km|chilometri?_?quadrati?)($|[_\W])/.test(raw)) return 'km2';
  if (/(^|[_\W])(ha|ettari?|hectares?)($|[_\W])/.test(raw)) return 'ha';
  return 'm2';
}

export function convertAreaValue(value: number, fromUnit: AreaUnit, toUnit: AreaUnit): number {
  if (!Number.isFinite(value)) return Number.NaN;
  const m2 =
    fromUnit === 'm2'
      ? value
      : fromUnit === 'ha'
        ? value * 10000
        : value * 1_000_000;
  if (!Number.isFinite(m2)) return Number.NaN;
  if (toUnit === 'm2') return m2;
  if (toUnit === 'ha') return m2 / 10000;
  return m2 / 1_000_000;
}

export function buildNormalizedDenominatorPlan(
  dataset: any,
  denominatorFieldNameRaw: string
): {plan: NormalizedDenominatorPlan; detailHint: string} | {error: string} {
  const requestedFieldName = String(denominatorFieldNameRaw || '').trim();
  if (!requestedFieldName) {
    return {error: 'denominatorFieldName is required.'};
  }

  const requestedIsAreaLike = isAreaLikeFieldName(requestedFieldName);
  const requestedAreaUnit = requestedIsAreaLike ? inferAreaUnitFromFieldName(requestedFieldName) : null;
  const directField = resolveDatasetFieldName(dataset, requestedFieldName);
  if (directField) {
    const sourceAreaUnit = requestedIsAreaLike ? inferAreaUnitFromFieldName(directField) : null;
    const conversionNote =
      sourceAreaUnit && requestedAreaUnit && sourceAreaUnit !== requestedAreaUnit
        ? ` (converted ${sourceAreaUnit}->${requestedAreaUnit})`
        : '';
    return {
      plan: {
        mode: 'field',
        requestedFieldName,
        fieldName: directField,
        geometryField: null,
        h3Field: null,
        sourceAreaUnit,
        targetAreaUnit: requestedAreaUnit || sourceAreaUnit,
        usedFallback: false
      },
      detailHint: `Denominator uses field "${directField}"${conversionNote}.`
    };
  }

  if (!requestedIsAreaLike) {
    return {
      error: `Denominator field "${requestedFieldName}" not found in dataset "${dataset?.label || dataset?.id}".`
    };
  }

  const fallbackAreaField = resolveAreaLikeFieldName(dataset);
  if (fallbackAreaField) {
    const sourceAreaUnit = inferAreaUnitFromFieldName(fallbackAreaField);
    const targetAreaUnit = requestedAreaUnit || sourceAreaUnit;
    const conversionNote = sourceAreaUnit !== targetAreaUnit ? ` (converted ${sourceAreaUnit}->${targetAreaUnit})` : '';
    return {
      plan: {
        mode: 'field',
        requestedFieldName,
        fieldName: fallbackAreaField,
        geometryField: null,
        h3Field: null,
        sourceAreaUnit,
        targetAreaUnit,
        usedFallback: true
      },
      detailHint:
        `Denominator field "${requestedFieldName}" not found; fallback to area field "${fallbackAreaField}"` +
        `${conversionNote}.`
    };
  }

  const geometryField = resolveGeojsonFieldName(dataset, null);
  if (geometryField) {
    const targetAreaUnit = requestedAreaUnit || 'm2';
    return {
      plan: {
        mode: 'derived_geojson_area',
        requestedFieldName,
        fieldName: null,
        geometryField,
        h3Field: null,
        sourceAreaUnit: 'm2',
        targetAreaUnit,
        usedFallback: true
      },
      detailHint:
        `Denominator field "${requestedFieldName}" not found; derived from geometry field "${geometryField}" as ${targetAreaUnit}.`
    };
  }

  const h3Field = resolveH3FieldName(dataset, 'h3_id') || resolveH3FieldName(dataset, null);
  if (h3Field) {
    const targetAreaUnit = requestedAreaUnit || 'm2';
    return {
      plan: {
        mode: 'derived_h3_area',
        requestedFieldName,
        fieldName: null,
        geometryField: null,
        h3Field,
        sourceAreaUnit: 'm2',
        targetAreaUnit,
        usedFallback: true
      },
      detailHint: `Denominator field "${requestedFieldName}" not found; derived from H3 cell area using "${h3Field}" as ${targetAreaUnit}.`
    };
  }

  return {
    error:
      `Denominator field "${requestedFieldName}" not found in dataset "${dataset?.label || dataset?.id}", ` +
      'and no area/geometric fallback is available (_geojson or h3_id missing).'
  };
}

export function computeNormalizedDenominatorValue(
  dataset: any,
  rowIdx: number,
  plan: NormalizedDenominatorPlan
): number | null {
  if (!plan || typeof plan !== 'object') return null;
  if (plan.mode === 'field') {
    const fieldName = String(plan.fieldName || '').trim();
    if (!fieldName) return null;
    const parsed = parseCoordinateValue(dataset.getValue(fieldName, rowIdx));
    if (parsed === null || !Number.isFinite(parsed)) return null;
    const sourceUnit = plan.sourceAreaUnit;
    const targetUnit = plan.targetAreaUnit;
    if (sourceUnit && targetUnit && sourceUnit !== targetUnit) {
      const converted = convertAreaValue(parsed, sourceUnit, targetUnit);
      return Number.isFinite(converted) ? converted : null;
    }
    return parsed;
  }

  if (plan.mode === 'derived_geojson_area') {
    const geometryField = String(plan.geometryField || '').trim();
    if (!geometryField) return null;
    const parsed = parseGeoJsonLike(dataset.getValue(geometryField, rowIdx));
    const feature = toTurfPolygonFeature(parsed);
    if (!feature) return null;
    const areaM2 = featureAreaM2(feature as any);
    if (!Number.isFinite(areaM2)) return null;
    const targetUnit = plan.targetAreaUnit || 'm2';
    const converted = convertAreaValue(areaM2, 'm2', targetUnit);
    return Number.isFinite(converted) ? converted : null;
  }

  if (plan.mode === 'derived_h3_area') {
    const h3Field = String(plan.h3Field || '').trim();
    if (!h3Field) return null;
    const h3Id = normalizeH3Key(dataset.getValue(h3Field, rowIdx));
    if (!h3Id) return null;
    let areaM2 = Number.NaN;
    try {
      areaM2 = Number(cellArea(h3Id, 'm2'));
    } catch {
      areaM2 = Number.NaN;
    }
    if (!Number.isFinite(areaM2)) {
      const feature = h3CellToPolygonFeature(h3Id);
      if (feature) {
        areaM2 = featureAreaM2(feature as any);
      }
    }
    if (!Number.isFinite(areaM2)) return null;
    const targetUnit = plan.targetAreaUnit || 'm2';
    const converted = convertAreaValue(areaM2, 'm2', targetUnit);
    return Number.isFinite(converted) ? converted : null;
  }

  return null;
}

export function describeNormalizedDenominatorPlan(plan: NormalizedDenominatorPlan): string {
  const toUnit = plan.targetAreaUnit;
  if (plan.mode === 'field') {
    const fieldName = String(plan.fieldName || '').trim() || plan.requestedFieldName;
    const conversion =
      plan.sourceAreaUnit && toUnit && plan.sourceAreaUnit !== toUnit
        ? ` (${plan.sourceAreaUnit}->${toUnit})`
        : '';
    return `"${fieldName}"${conversion}`;
  }
  if (plan.mode === 'derived_geojson_area') {
    const geometryField = String(plan.geometryField || '').trim() || '_geojson';
    return `area(${geometryField})${toUnit && toUnit !== 'm2' ? ` in ${toUnit}` : ''}`;
  }
  const h3Field = String(plan.h3Field || '').trim() || 'h3_id';
  return `cellArea(${h3Field})${toUnit && toUnit !== 'm2' ? ` in ${toUnit}` : ''}`;
}

export function summarizeNumericField(dataset: any, fieldName: string, sampleLimit = 50000) {
  const idx = getDatasetIndexes(dataset).slice(0, Math.max(1, Number(sampleLimit || 50000)));
  let nonNullCount = 0;
  let numericCount = 0;
  const distinctNumeric = new Set<number>();
  idx.forEach((rowIdx: number) => {
    const raw = dataset.getValue(fieldName, rowIdx);
    if (raw === null || raw === undefined || raw === '') return;
    nonNullCount += 1;
    const num = Number(raw);
    if (Number.isFinite(num)) {
      numericCount += 1;
      if (distinctNumeric.size < 256) {
        distinctNumeric.add(num);
      }
    }
  });
  return {
    sampledRows: idx.length,
    nonNullCount,
    numericCount,
    distinctNumericCount: distinctNumeric.size
  };
}

export function sampleNumericValues(dataset: any, fieldName: string, sampleLimit = 50000): number[] {
  const idx = getDatasetIndexes(dataset).slice(0, Math.max(1, Number(sampleLimit || 50000)));
  const out: number[] = [];
  idx.forEach((rowIdx: number) => {
    const raw = dataset.getValue(fieldName, rowIdx);
    const num = Number(raw);
    if (Number.isFinite(num)) out.push(num);
  });
  return out;
}

export function quantileSorted(sortedValues: number[], q: number): number {
  if (!sortedValues.length) return NaN;
  return Number(d3QuantileSorted(sortedValues, Math.max(0, Math.min(1, q))));
}

export function computeThresholdsByStrategy(
  values: number[],
  strategy: 'mean' | 'median' | 'mode' | 'quantiles',
  classes = 5,
  quantiles?: number[]
): {thresholds: number[]; details: string; warning?: string} {
  const sorted = [...values].sort((a, b) => a - b);
  if (!sorted.length) {
    return {thresholds: [], details: 'No numeric values available.'};
  }

  if (strategy === 'mean') {
    const mean = Number(d3Mean(sorted));
    return {thresholds: [Number(mean.toFixed(6))], details: `mean=${Number(mean.toFixed(6))}`};
  }

  if (strategy === 'median') {
    const median = Number(d3Median(sorted));
    return {thresholds: [Number(median.toFixed(6))], details: `median=${Number(median.toFixed(6))}`};
  }

  if (strategy === 'mode') {
    const counts = new Map<number, number>();
    sorted.forEach(v => counts.set(v, (counts.get(v) || 0) + 1));
    let bestValue = sorted[0];
    let bestCount = 0;
    counts.forEach((count, value) => {
      if (count > bestCount) {
        bestCount = count;
        bestValue = value;
      }
    });
    const warning = bestCount <= 1 ? 'Mode is weak (all/most values unique); using computed mode anyway.' : undefined;
    return {thresholds: [Number(bestValue.toFixed(6))], details: `mode=${Number(bestValue.toFixed(6))} (freq=${bestCount})`, warning};
  }

  const targetClasses = Math.max(3, Math.min(12, Number(classes || 5)));
  const qValues = Array.isArray(quantiles) && quantiles.length
    ? quantiles
        .map(v => Number(v))
        .filter(v => Number.isFinite(v) && v > 0 && v < 1)
        .sort((a, b) => a - b)
    : Array.from({length: targetClasses - 1}, (_, i) => (i + 1) / targetClasses);

  const thresholds = normalizeThresholds(qValues.map(q => Number(quantileSorted(sorted, q).toFixed(6)))) || [];
  return {
    thresholds,
    details: `quantiles=[${qValues.map(v => Number(v.toFixed(4))).join(', ')}]`
  };
}

export function getNumericExtent(dataset: any, fieldName: string, sampleLimit = 50000): [number, number] | null {
  const values = sampleNumericValues(dataset, fieldName, sampleLimit);
  const [rawMin, rawMax] = d3Extent(values);
  if (!Number.isFinite(rawMin) || !Number.isFinite(rawMax)) return null;
  const min = Number(rawMin);
  const max = Number(rawMax);
  if (min === max) {
    return [min, min + 1];
  }
  return [min, max];
}

export function inferDatasetH3Resolution(dataset: any, fallbackH3Field?: string | null): number | null {
  const resolvedResField = resolveDatasetFieldName(dataset, 'h3_resolution');
  const idx = getDatasetIndexes(dataset).slice(0, 10000);
  const values = new Set<number>();
  if (resolvedResField) {
    idx.forEach((rowIdx: number) => {
      const raw = dataset.getValue(resolvedResField, rowIdx);
      const num = Number(raw);
      if (Number.isFinite(num) && num >= 0 && num <= 15) {
        values.add(Math.trunc(num));
      }
    });
    if (values.size === 1) {
      return Array.from(values)[0];
    }
  }

  // If no explicit resolution field exists, do not guess from H3 string length.
  if (!fallbackH3Field) {
    return null;
  }
  return null;
}
