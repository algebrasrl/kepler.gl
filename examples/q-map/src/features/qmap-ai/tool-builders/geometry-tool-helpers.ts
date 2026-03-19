/**
 * Shared helpers for geometry tool builders.
 *
 * Extracts the repeated "resolve dataset → resolve geometry field →
 * collect features → loading indicator" preamble into reusable functions.
 */
import {setLoadingIndicator, wrapTo} from '@kepler.gl/actions';
import type {QMapToolContext} from '../context/tool-context';

export type ResolvedGeometryDataset = {
  dataset: any;
  geomField: string | null;
  h3Field: string | null;
};

/**
 * Resolve a dataset and its geometry field (geojson or H3).
 * Returns null with a failure result if resolution fails.
 */
export function resolveGeometryDataset(
  ctx: QMapToolContext,
  datasets: any,
  datasetName: string,
  geometryFieldHint?: string | null,
  label = 'Dataset'
): {resolved: ResolvedGeometryDataset} | {resolved: null; failResult: any} {
  const dataset = ctx.resolveDatasetByName(datasets, datasetName);
  if (!dataset?.id) {
    return {
      resolved: null,
      failResult: {llmResult: {success: false, details: `${label} "${datasetName}" not found.`}}
    };
  }
  const geomField = ctx.resolveGeojsonFieldName(dataset, geometryFieldHint);
  const h3Field = !geomField ? ctx.resolveH3FieldName(dataset, geometryFieldHint || null) : null;
  if (!geomField && !h3Field) {
    return {
      resolved: null,
      failResult: {llmResult: {success: false, details: `${label} must expose a geojson or H3 field.`}}
    };
  }
  return {resolved: {dataset, geomField, h3Field}};
}

/**
 * Collect features from a resolved geometry dataset with loading indicator management.
 */
export async function collectFeaturesWithLoading(
  ctx: QMapToolContext,
  resolved: ResolvedGeometryDataset,
  visState: any,
  opts: {useActiveFilters?: boolean; maxFeatures?: number; includeRowProperties?: boolean}
): Promise<any[]> {
  return ctx.collectDatasetFeaturesForGeometryOps(resolved.dataset, visState, {
    geometryField: resolved.geomField,
    h3Field: resolved.h3Field,
    useActiveFilters: opts.useActiveFilters !== false,
    maxFeatures: opts.maxFeatures,
    includeRowProperties: opts.includeRowProperties
  });
}

/**
 * Wrap an async geometry operation with loading indicator on/off.
 */
export async function withLoadingIndicator<T>(
  ctx: QMapToolContext,
  fn: () => Promise<T>
): Promise<T> {
  const useLoading = ctx.shouldUseLoadingIndicator();
  if (useLoading) ctx.dispatch(wrapTo('map', setLoadingIndicator({change: 1}) as any));
  try {
    return await fn();
  } finally {
    if (useLoading) ctx.dispatch(wrapTo('map', setLoadingIndicator({change: -1}) as any));
  }
}

/**
 * Upsert result rows and optionally hide source layers.
 */
export function upsertAndHideSources(
  ctx: QMapToolContext,
  visState: any,
  opts: {
    targetName: string;
    rows: Array<Record<string, unknown>>;
    idPrefix: string;
    showOnMap: boolean;
    hideSourceIds?: string[];
  }
): void {
  ctx.upsertDerivedDatasetRows(
    ctx.dispatch,
    visState?.datasets || {},
    opts.targetName,
    opts.rows,
    opts.idPrefix,
    opts.showOnMap
  );
  if (opts.showOnMap && ctx.QMAP_AUTO_HIDE_SOURCE_LAYERS && opts.hideSourceIds?.length) {
    ctx.hideLayersForDatasetIds(ctx.dispatch, visState?.layers || [], opts.hideSourceIds);
  }
}
