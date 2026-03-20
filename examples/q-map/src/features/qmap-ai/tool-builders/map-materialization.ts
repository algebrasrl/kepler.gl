import {z} from 'zod';

import type {QMapToolContext} from '../context/tool-context';
import {preprocessBoundaryClipArgs} from '../tool-args-normalization';

export function createClipDatasetByBoundaryTool(ctx: QMapToolContext & {clipQMapDatasetByGeometry: any}) {
  const {
    buildOptionalLenientEnumSchema,
    getCurrentVisState,
    resolveDatasetByName,
    getDatasetInfoByLabel,
    makeExecutionKey,
    resolveGeojsonFieldName,
    resolveOptionalFeatureCap,
    clipQMapDatasetByGeometry
  } = ctx;

  return {
    description:
      'Clip/mask a source geometry dataset by a boundary dataset (administrative boundary or custom shape).',
    parameters: z.preprocess(preprocessBoundaryClipArgs, z.object({
      sourceDatasetName: z.string(),
      boundaryDatasetName: z.string(),
      mode: buildOptionalLenientEnumSchema(
        ['intersects', 'within'],
        {intersect: 'intersects', intersection: 'intersects', inside: 'within'},
        'mode'
      ).describe('Default intersects'),
      sourceGeometryField: z.string().optional(),
      boundaryGeometryField: z.string().optional(),
      useActiveFilters: z.boolean().optional().describe('Default true'),
      maxSourceFeatures: z
        .number()
        .optional()
        .describe('Optional explicit cap on source features. Unset = full matched coverage (no truncation).'),
      maxBoundaryFeatures: z
        .number()
        .optional()
        .describe('Optional explicit cap on boundary features. Unset = full matched coverage (no truncation).'),
      includeIntersectionMetrics: z
        .boolean()
        .optional()
        .describe(
          'Default true: append per-row metrics qmap_clip_match_count, qmap_clip_intersection_area_m2, qmap_clip_intersection_pct'
        ),
      includeDistinctPropertyCounts: z
        .boolean()
        .optional()
        .describe('Default true: append <clip_field>__count fields with number of distinct clip-side values'),
      includeDistinctPropertyValueCounts: z
        .boolean()
        .optional()
        .describe(
          'Default false: append <clip_field>__<value>__count fields with matched clip-side element counts by value'
        ),
      showOnMap: z
        .boolean()
        .optional()
        .describe('Default true. Set false for intermediate technical datasets kept off-map.'),
      newDatasetName: z.string().optional()
    })),
    execute: async (args: any) => {
      const {sourceDatasetName, boundaryDatasetName} = args as any;
      const mode = (args as any).mode || 'intersects';
      const currentVisState = getCurrentVisState();
      const source = resolveDatasetByName(currentVisState?.datasets || {}, sourceDatasetName);
      const boundary = resolveDatasetByName(currentVisState?.datasets || {}, boundaryDatasetName);
      if (!source?.id) return {llmResult: {success: false, details: `Source dataset "${sourceDatasetName}" not found.`}};
      if (!boundary?.id) {
        return {llmResult: {success: false, details: `Boundary dataset "${boundaryDatasetName}" not found.`}};
      }
      const target =
        String((args as any).newDatasetName || '').trim() ||
        `${source.label || source.id}_clipped_${boundary.label || boundary.id}`;
      const {label: resolvedTargetLabel, datasetId: resolvedTargetDatasetId} = getDatasetInfoByLabel(
        currentVisState?.datasets || {},
        target,
        'qmap_clip'
      );
      const sourceFieldNames = (source.fields || [])
        .map((field: any) => String(field?.name || '').trim())
        .filter(Boolean);
      const clipFieldNames =
        (args as any).includeDistinctPropertyCounts !== false ||
        (args as any).includeDistinctPropertyValueCounts === true
          ? (boundary.fields || [])
              .filter((field: any) => {
                const fieldName = String(field?.name || '').trim();
                if (!fieldName) return false;
                if (fieldName === (resolveGeojsonFieldName(boundary, (args as any).boundaryGeometryField) || '_geojson')) {
                  return false;
                }
                return String(field?.type || '').toLowerCase() !== 'geojson';
              })
              .map((field: any) => String(field?.name || '').trim())
              .filter(Boolean)
          : [];
      const reservedFieldNames = new Set(sourceFieldNames.map((name: string) => name.toLowerCase()));
      const ensureUniqueFieldName = (baseName: string): string => {
        let candidate = String(baseName || '').trim();
        let suffix = 1;
        while (!candidate || reservedFieldNames.has(candidate.toLowerCase())) {
          candidate = `${baseName}_${suffix}`;
          suffix += 1;
        }
        reservedFieldNames.add(candidate.toLowerCase());
        return candidate;
      };
      const styleableFields: string[] = [];
      const fieldCatalog = [...sourceFieldNames];
      const fieldAliases: Record<string, string> = {};
      if ((args as any).includeIntersectionMetrics !== false) {
        const metricFields = [
          ensureUniqueFieldName('qmap_clip_match_count'),
          ensureUniqueFieldName('qmap_clip_intersection_area_m2'),
          ensureUniqueFieldName('qmap_clip_intersection_pct')
        ];
        const [matchCountField, intersectionAreaField, intersectionPctField] = metricFields;
        fieldCatalog.push(...metricFields);
        styleableFields.push(...metricFields);
        fieldAliases.clip_count = matchCountField;
        fieldAliases.match_count = matchCountField;
        fieldAliases.intersection_area_m2 = intersectionAreaField;
        fieldAliases.intersection_pct = intersectionPctField;
      }
      if ((args as any).includeDistinctPropertyCounts !== false) {
        const distinctCountFields = clipFieldNames.map((fieldName: string) => ensureUniqueFieldName(`${fieldName}__count`));
        fieldCatalog.push(...distinctCountFields);
        styleableFields.push(...distinctCountFields);
        clipFieldNames.forEach((fieldName: string, idx: number) => {
          const resolvedCountField = distinctCountFields[idx];
          if (!resolvedCountField) return;
          fieldAliases[`${fieldName}_count`] = resolvedCountField;
          fieldAliases[`distinct_count_${fieldName}`] = resolvedCountField;
        });
      }
      const defaultStyleField =
        styleableFields.find((fieldName: string) => fieldName.toLowerCase().includes('intersection_pct')) ||
        styleableFields.find((fieldName: string) => fieldName.toLowerCase().includes('intersection_area')) ||
        styleableFields[0] ||
        '';
      return {
        llmResult: {
          success: true,
          dataset: resolvedTargetLabel,
          datasetId: resolvedTargetDatasetId,
          fieldCatalog,
          numericFields: styleableFields,
          styleableFields,
          defaultStyleField,
          fieldAliases,
          details: `Clipping "${source.label || source.id}" by boundary "${boundary.label || boundary.id}" (${mode}).`
        },
        additionalData: {
          executionKey: makeExecutionKey('clip-by-boundary'),
          sourceDatasetId: source.id,
          clipDatasetId: boundary.id,
          sourceGeometryField:
            resolveGeojsonFieldName(source, (args as any).sourceGeometryField) || '_geojson',
          clipGeometryField:
            resolveGeojsonFieldName(boundary, (args as any).boundaryGeometryField) || '_geojson',
          mode,
          useActiveFilters: (args as any).useActiveFilters !== false,
          maxSourceFeatures: resolveOptionalFeatureCap((args as any).maxSourceFeatures),
          maxClipFeatures: resolveOptionalFeatureCap((args as any).maxBoundaryFeatures),
          includeIntersectionMetrics: (args as any).includeIntersectionMetrics !== false,
          includeDistinctPropertyCounts: (args as any).includeDistinctPropertyCounts !== false,
          includeDistinctPropertyValueCounts: (args as any).includeDistinctPropertyValueCounts === true,
          showOnMap: (args as any).showOnMap !== false,
          newDatasetName: resolvedTargetLabel,
          newDatasetId: resolvedTargetDatasetId,
          fieldAliases
        }
      };
    },
    component: clipQMapDatasetByGeometry.component as any
  };

}

export function createDrawQMapBoundingBoxTool(ctx: QMapToolContext) {
  const {
    getCurrentVisState,
    resolveDatasetByName,
    resolveGeojsonFieldName,
    resolveH3FieldName,
    collectDatasetFeaturesForGeometryOps,
    hasValidBounds,
    buildBboxFeature,
    upsertDerivedDatasetRows,
    dispatch
  } = ctx;

  return {
    description:
      'Create/draw a bounding-box polygon dataset from explicit bounds, center+radius, or an existing dataset extent.',
    parameters: z.object({
      datasetName: z.string().optional().describe('Optional source dataset name to derive bbox from loaded geometry/H3'),
      geometryField: z.string().optional(),
      minLon: z.number().optional(),
      minLat: z.number().optional(),
      maxLon: z.number().optional(),
      maxLat: z.number().optional(),
      centerLon: z.number().optional(),
      centerLat: z.number().optional(),
      halfSizeKm: z.number().positive().optional().describe('Half-size radius in km for center-based bbox'),
      useActiveFilters: z.boolean().optional().describe('Default true when deriving from dataset'),
      maxFeatures: z
        .number()
        .optional()
        .describe('Optional explicit cap when deriving bbox from dataset features. Unset = full matched coverage.'),
      showOnMap: z.boolean().optional().describe('Default true'),
      newDatasetName: z.string().optional()
    }),
    execute: async ({
      datasetName,
      geometryField,
      minLon,
      minLat,
      maxLon,
      maxLat,
      centerLon,
      centerLat,
      halfSizeKm,
      useActiveFilters,
      maxFeatures,
      showOnMap,
      newDatasetName
    }: any) => {
      const vis = getCurrentVisState();
      const datasets = vis?.datasets || {};
      let resolvedMinLon: number | null = null;
      let resolvedMinLat: number | null = null;
      let resolvedMaxLon: number | null = null;
      let resolvedMaxLat: number | null = null;
      let sourceLabel = 'manual';

      const hasExplicitBounds = [minLon, minLat, maxLon, maxLat].every(value => Number.isFinite(Number(value)));
      if (hasExplicitBounds) {
        resolvedMinLon = Number(minLon);
        resolvedMinLat = Number(minLat);
        resolvedMaxLon = Number(maxLon);
        resolvedMaxLat = Number(maxLat);
      } else if (
        Number.isFinite(Number(centerLon)) &&
        Number.isFinite(Number(centerLat)) &&
        Number.isFinite(Number(halfSizeKm)) &&
        Number(halfSizeKm) > 0
      ) {
        const cLon = Number(centerLon);
        const cLat = Number(centerLat);
        const halfKm = Number(halfSizeKm);
        const latDelta = halfKm / 111;
        const lonDelta = halfKm / Math.max(1e-6, 111 * Math.cos((Math.PI * cLat) / 180));
        resolvedMinLon = cLon - lonDelta;
        resolvedMaxLon = cLon + lonDelta;
        resolvedMinLat = cLat - latDelta;
        resolvedMaxLat = cLat + latDelta;
      } else if (String(datasetName || '').trim()) {
        const sourceDataset = resolveDatasetByName(datasets, String(datasetName || ''));
        if (!sourceDataset?.id) {
          return {llmResult: {success: false, details: `Dataset "${String(datasetName)}" not found.`}};
        }
        const sourceGeom = resolveGeojsonFieldName(sourceDataset, geometryField);
        const sourceH3 = !sourceGeom ? resolveH3FieldName(sourceDataset, geometryField || null) : null;
        if (!sourceGeom && !sourceH3) {
          return {
            llmResult: {
              success: false,
              details: `Dataset "${sourceDataset.label || sourceDataset.id}" has no geojson/H3 geometry field.`
            }
          };
        }
        const features = await collectDatasetFeaturesForGeometryOps(sourceDataset, vis, {
          geometryField: sourceGeom,
          h3Field: sourceH3,
          useActiveFilters: useActiveFilters !== false,
          maxFeatures
        });
        if (!features.length) {
          return {
            llmResult: {
              success: false,
              details: `No valid geometries found in dataset "${sourceDataset.label || sourceDataset.id}".`
            }
          };
        }
        const bounds = {
          minLng: Number.POSITIVE_INFINITY,
          minLat: Number.POSITIVE_INFINITY,
          maxLng: Number.NEGATIVE_INFINITY,
          maxLat: Number.NEGATIVE_INFINITY
        };
        features.forEach((item: any) => {
          if (!item?.bbox) return;
          bounds.minLng = Math.min(bounds.minLng, Number(item.bbox[0]));
          bounds.minLat = Math.min(bounds.minLat, Number(item.bbox[1]));
          bounds.maxLng = Math.max(bounds.maxLng, Number(item.bbox[2]));
          bounds.maxLat = Math.max(bounds.maxLat, Number(item.bbox[3]));
        });
        if (!hasValidBounds(bounds)) {
          return {
            llmResult: {
              success: false,
              details: `Unable to derive bbox for dataset "${sourceDataset.label || sourceDataset.id}".`
            }
          };
        }
        resolvedMinLon = bounds.minLng;
        resolvedMinLat = bounds.minLat;
        resolvedMaxLon = bounds.maxLng;
        resolvedMaxLat = bounds.maxLat;
        sourceLabel = String(sourceDataset.label || sourceDataset.id);
      } else {
        return {
          llmResult: {
            success: false,
            details:
              'Provide explicit bounds (minLon/minLat/maxLon/maxLat), center+halfSizeKm, or datasetName for bbox derivation.'
          }
        };
      }

      if (
        !Number.isFinite(resolvedMinLon) ||
        !Number.isFinite(resolvedMinLat) ||
        !Number.isFinite(resolvedMaxLon) ||
        !Number.isFinite(resolvedMaxLat)
      ) {
        return {llmResult: {success: false, details: 'Invalid bbox coordinates.'}};
      }

      const minLonSafe = Number(resolvedMinLon);
      const minLatSafe = Number(resolvedMinLat);
      const maxLonSafe = Number(resolvedMaxLon);
      const maxLatSafe = Number(resolvedMaxLat);
      if (maxLonSafe <= minLonSafe || maxLatSafe <= minLatSafe) {
        return {llmResult: {success: false, details: 'Invalid bbox extents (max must be greater than min).'}};
      }

      const bboxFeature = buildBboxFeature(minLonSafe, minLatSafe, maxLonSafe, maxLatSafe, {
        source: sourceLabel
      });
      const targetName = String(newDatasetName || '').trim() || `${sourceLabel}_bbox`;
      upsertDerivedDatasetRows(
        dispatch,
        datasets,
        targetName,
        [
          {
            _geojson: bboxFeature,
            bbox_min_lon: minLonSafe,
            bbox_min_lat: minLatSafe,
            bbox_max_lon: maxLonSafe,
            bbox_max_lat: maxLatSafe,
            bbox_source: sourceLabel
          }
        ],
        'qmap_bbox',
        showOnMap !== false
      );

      return {
        llmResult: {
          success: true,
          dataset: targetName,
          bbox: [minLonSafe, minLatSafe, maxLonSafe, maxLatSafe],
          details: `Bounding box created for "${sourceLabel}".`
        }
      };
    }
  };

}
