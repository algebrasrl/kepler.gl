import {z} from 'zod';

import type {QMapToolContext} from '../context/tool-context';
import {createDualDatasetOverlayTool} from './dual-dataset-overlay-factory';
import {
  resolveGeometryDataset,
  collectFeaturesWithLoading,
  withLoadingIndicator,
  upsertAndHideSources
} from './geometry-tool-helpers';

export function createOverlayUnionTool(ctx: QMapToolContext) {
  return createDualDatasetOverlayTool(ctx, {
    toolDescription: 'Compute union overlay geometry for two polygon/H3 datasets and materialize as a derived dataset.',
    overlayType: 'union',
    idPrefix: 'qmap_overlay_union',
    defaultNameSuffix: 'union',
    noResultMessage: 'Union could not be computed (no valid geometry overlap).',
    successVerb: 'Union',
    geometryOp: (a, b) => ctx.unionFeatures([...a, ...b])
  });
}

export function createOverlayIntersectionTool(ctx: QMapToolContext) {
  return createDualDatasetOverlayTool(ctx, {
    toolDescription:
      'Compute intersection overlay geometry for two polygon/H3 datasets and materialize as a derived dataset.',
    overlayType: 'intersection',
    idPrefix: 'qmap_overlay_intersection',
    defaultNameSuffix: 'intersection',
    noResultMessage: 'Intersection produced no geometry (datasets do not overlap).',
    successVerb: 'Intersection',
    geometryOp: (a, b) => ctx.intersectFeatureSets(a, b)
  });
}

export function createOverlaySymmetricDifferenceTool(ctx: QMapToolContext) {
  return createDualDatasetOverlayTool(ctx, {
    toolDescription:
      'Compute symmetric-difference overlay geometry for two polygon/H3 datasets and materialize as a derived dataset.',
    overlayType: 'symmetric_difference',
    idPrefix: 'qmap_overlay_symdiff',
    defaultNameSuffix: 'symdiff',
    noResultMessage: 'Symmetric difference produced no geometry (datasets fully overlap or are empty).',
    successVerb: 'Symmetric-difference',
    geometryOp: (a, b) => ctx.symmetricDifferenceFeatureSets(a, b)
  });
}

export function createDissolveQMapDatasetByFieldTool(ctx: QMapToolContext) {
  return {
    description: 'Dissolve polygon/H3 features of a dataset into merged geometry, optionally grouped by a field.',
    parameters: z.object({
      datasetName: z.string(),
      geometryField: z.string().optional(),
      groupByField: z.string().optional(),
      useActiveFilters: z.boolean().optional().describe('Default true'),
      maxFeatures: z.number().optional(),
      showOnMap: z.boolean().optional().describe('Default false'),
      newDatasetName: z.string().optional()
    }),
    execute: async ({datasetName, geometryField, groupByField, useActiveFilters, maxFeatures, showOnMap, newDatasetName}: any) => {
      const vis = ctx.getCurrentVisState();
      const res = resolveGeometryDataset(ctx, vis?.datasets || {}, datasetName, geometryField);
      if (!res.resolved) return (res as any).failResult;
      const {dataset: source} = res.resolved;
      const groupFieldResolved = groupByField ? ctx.resolveDatasetFieldName(source, groupByField) : null;
      if (groupByField && !groupFieldResolved) {
        return {llmResult: {success: false, details: `Group field "${groupByField}" not found in dataset "${source.label || source.id}".`}};
      }
      return withLoadingIndicator(ctx, async () => {
        const features = await collectFeaturesWithLoading(ctx, res.resolved!, vis, {
          useActiveFilters, maxFeatures, includeRowProperties: true
        });
        if (!features.length) {
          return {llmResult: {success: false, details: `No valid geometries found in dataset "${source.label || source.id}" for dissolve.`}};
        }
        const dissolvedRows = ctx.dissolveFeaturesByProperty(
          features.map((item: any) => ({feature: item.feature, properties: item.rowProperties || {}})),
          groupFieldResolved || undefined
        );
        if (!dissolvedRows.length) return {llmResult: {success: false, details: 'Dissolve produced no output geometries.'}};
        const targetName = String(newDatasetName || '').trim() || `${source.label || source.id}_dissolved`;
        const rows = dissolvedRows.map((item: any) => ({
          _geojson: item.feature, dissolve_group: item.groupValue,
          dissolve_feature_count: item.featureCount,
          area_m2: Number(ctx.featureAreaM2(item.feature).toFixed(2))
        }));
        upsertAndHideSources(ctx, vis, {
          targetName, rows, idPrefix: 'qmap_dissolve',
          showOnMap: showOnMap === true, hideSourceIds: [source.id]
        });
        return {
          llmResult: {success: true, dataset: targetName, groups: rows.length,
            details: `Dissolve created ${rows.length} geometry group(s)` + (groupFieldResolved ? ` using field "${groupFieldResolved}".` : ' without grouping field.')}
        };
      });
    }
  };
}
