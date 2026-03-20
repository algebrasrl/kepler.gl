/**
 * Higher-order factory for dual-dataset overlay tools.
 *
 * Eliminates ~350 LOC of near-identical boilerplate across union,
 * intersection, and symmetric-difference overlay tools.
 */
import {setLoadingIndicator, wrapTo} from '@kepler.gl/actions';
import {z} from 'zod';

import type {QMapToolContext} from '../context/tool-context';

export type OverlayConfig = {
  toolDescription: string;
  overlayType: string;
  idPrefix: string;
  defaultNameSuffix: string;
  noResultMessage: string;
  successVerb: string;
  geometryOp: (featuresA: any[], featuresB: any[]) => any | null;
};

export function createDualDatasetOverlayTool(ctx: QMapToolContext, config: OverlayConfig) {
  const {
    dispatch,
    QMAP_AUTO_HIDE_SOURCE_LAYERS,
    getCurrentVisState,
    resolveDatasetByName,
    resolveGeojsonFieldName,
    resolveH3FieldName,
    shouldUseLoadingIndicator,
    collectDatasetFeaturesForGeometryOps,
    featureAreaM2,
    upsertDerivedDatasetRows,
    hideLayersForDatasetIds
  } = ctx;

  return {
    description: config.toolDescription,
    parameters: z.object({
      datasetAName: z.string(),
      datasetBName: z.string(),
      geometryFieldA: z.string().optional(),
      geometryFieldB: z.string().optional(),
      useActiveFilters: z.boolean().optional().describe('Default true'),
      maxFeaturesA: z.number().optional(),
      maxFeaturesB: z.number().optional(),
      showOnMap: z.boolean().optional().describe('Default false'),
      newDatasetName: z.string().optional()
    }),
    execute: async ({
      datasetAName,
      datasetBName,
      geometryFieldA,
      geometryFieldB,
      useActiveFilters,
      maxFeaturesA,
      maxFeaturesB,
      showOnMap,
      newDatasetName
    }: any) => {
      const vis = getCurrentVisState();
      const datasets = vis?.datasets || {};
      const a = resolveDatasetByName(datasets, datasetAName);
      const b = resolveDatasetByName(datasets, datasetBName);
      if (!a?.id) return {llmResult: {success: false, details: `Dataset A "${datasetAName}" not found.`}};
      if (!b?.id) return {llmResult: {success: false, details: `Dataset B "${datasetBName}" not found.`}};
      const aGeom = resolveGeojsonFieldName(a, geometryFieldA);
      const bGeom = resolveGeojsonFieldName(b, geometryFieldB);
      const aH3 = !aGeom ? resolveH3FieldName(a, geometryFieldA || null) : null;
      const bH3 = !bGeom ? resolveH3FieldName(b, geometryFieldB || null) : null;
      if ((!aGeom && !aH3) || (!bGeom && !bH3)) {
        return {llmResult: {success: false, details: 'Both datasets must expose a geojson or H3 field.'}};
      }

      const useLoadingIndicator = shouldUseLoadingIndicator();
      if (useLoadingIndicator) dispatch(wrapTo('map', setLoadingIndicator({change: 1}) as any));
      try {
        const aFeatures = await collectDatasetFeaturesForGeometryOps(a, vis, {
          geometryField: aGeom,
          h3Field: aH3,
          useActiveFilters: useActiveFilters !== false,
          maxFeatures: maxFeaturesA
        });
        const bFeatures = await collectDatasetFeaturesForGeometryOps(b, vis, {
          geometryField: bGeom,
          h3Field: bH3,
          useActiveFilters: useActiveFilters !== false,
          maxFeatures: maxFeaturesB
        });
        const result = config.geometryOp(
          aFeatures.map((item: any) => item.feature),
          bFeatures.map((item: any) => item.feature)
        );
        if (!result) {
          return {llmResult: {success: false, details: config.noResultMessage}};
        }
        const targetName =
          String(newDatasetName || '').trim() ||
          `${a.label || a.id}_${config.defaultNameSuffix}_${b.label || b.id}`;
        upsertDerivedDatasetRows(
          dispatch,
          datasets,
          targetName,
          [
            {
              _geojson: result,
              overlay_type: config.overlayType,
              source_a: a.label || a.id,
              source_b: b.label || b.id,
              area_m2: Number(featureAreaM2(result).toFixed(2))
            }
          ],
          `qmap_overlay_${config.overlayType}`,
          showOnMap === true
        );
        if (showOnMap === true && QMAP_AUTO_HIDE_SOURCE_LAYERS) {
          hideLayersForDatasetIds(dispatch, vis?.layers || [], [a.id, b.id]);
        }
        return {
          llmResult: {
            success: true,
            dataset: targetName,
            details: `${config.successVerb} overlay created for "${a.label || a.id}" and "${b.label || b.id}".`
          }
        };
      } finally {
        if (useLoadingIndicator) dispatch(wrapTo('map', setLoadingIndicator({change: -1}) as any));
      }
    }
  };
}
