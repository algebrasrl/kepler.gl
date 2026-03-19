import React, {useEffect} from 'react';
import {addDataToMap, replaceDataInMap, setLoadingIndicator, wrapTo} from '@kepler.gl/actions';
import {ALL_FIELD_TYPES} from '@kepler.gl/constants';
import {extendedTool} from '../tool-shim';
import {useDispatch, useSelector} from 'react-redux';
import {z} from 'zod';

import {selectQMapVisState} from '../../../state/qmap-selectors';
import {useToolExecution} from './use-tool-execution';

import type {QMapToolContext} from '../context/tool-context';

type ClipFeatureDiagnosticsInput = any;

export function createClipQMapDatasetByGeometryTool(ctx: QMapToolContext) {
  const {
    QMAP_CLIP_MODE_SCHEMA,
    QMAP_DEFAULT_CHUNK_SIZE,
    QMAP_CLIP_MAX_LOCAL_PAIR_EVAL,
    QMAP_AUTO_HIDE_SOURCE_LAYERS,
    getCurrentVisState,
    resolveDatasetByName,
    resolveDatasetFieldName,
    resolveH3FieldName,
    getDatasetInfoByLabel,
    makeExecutionKey,
    resolveOptionalFeatureCap,
    EXECUTED_TOOL_COMPONENT_KEYS,
    rememberExecutedToolComponentKey,
    filterTargetsDataset,
    toComparable,
    shouldUseLoadingIndicator,
    mapIndexesChunked,
    parseGeoJsonLike,
    toTurfPolygonFeature,
    computeClipMetricsForFeature,
    runClipRowsJob,
    inferDatasetH3Resolution,
    normalizeH3Key,
    getResolution,
    extractPolygonsFromGeoJsonLike,
    getIntersectingH3Ids,
    h3CellToPolygonFeature,
    geometryToBbox,
    boundsOverlap,
    normalizeFieldValueToken,
    getPolygonsFromGeometry,
    runH3Job,
    yieldToMainThread,
    upsertDerivedDatasetRows,
    hideLayersForDatasetIds
  } = ctx;

  return extendedTool({
    description:
      'Clip/mask a geometry dataset using another geometry dataset and materialize a derived dataset. Keeps source schema and applies active filters by default.',
    parameters: z.object({
      sourceDatasetName: z.string().describe('Dataset to clip/mask'),
      clipDatasetName: z.string().describe('Dataset providing clipping geometry'),
      sourceGeometryField: z.string().optional().describe('Optional explicit geojson field in source dataset'),
      clipGeometryField: z.string().optional().describe('Optional explicit geojson field in clipping dataset'),
      mode: QMAP_CLIP_MODE_SCHEMA.describe('Default intersects'),
      useActiveFilters: z
        .boolean()
        .optional()
        .describe('Default true: apply current UI filters to both source and clipping datasets'),
      maxSourceFeatures: z
        .number()
        .optional()
        .describe('Optional explicit cap on source features. Unset = full matched coverage (no truncation).'),
      maxClipFeatures: z
        .number()
        .optional()
        .describe('Optional explicit cap on clip features. Unset = full matched coverage (no truncation).'),
      includeIntersectionMetrics: z
        .boolean()
        .optional()
        .describe(
          'Default true: append per-row metrics qmap_clip_match_count, qmap_clip_intersection_area_m2, qmap_clip_intersection_pct'
        ),
      includeDistinctPropertyCounts: z
        .boolean()
        .optional()
        .describe(
          'Default true: append <clip_field>__count fields with number of distinct clip-side values matched per output row'
        ),
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
      newDatasetName: z.string().optional().describe('Default <source>_clipped_<clip>')
    }),
    execute: async ({
      sourceDatasetName,
      clipDatasetName,
      sourceGeometryField,
      clipGeometryField,
      mode,
      useActiveFilters,
      maxSourceFeatures,
      maxClipFeatures,
      includeIntersectionMetrics,
      includeDistinctPropertyCounts,
      includeDistinctPropertyValueCounts,
      showOnMap,
      newDatasetName
    }) => {
      const currentVisState = getCurrentVisState();
      const sourceDataset = resolveDatasetByName(currentVisState?.datasets || {}, sourceDatasetName);
      const clipDataset = resolveDatasetByName(currentVisState?.datasets || {}, clipDatasetName);

      if (!sourceDataset?.id) {
        return {llmResult: {success: false, details: `Source dataset "${sourceDatasetName}" not found.`}};
      }
      if (!clipDataset?.id) {
        return {llmResult: {success: false, details: `Clip dataset "${clipDatasetName}" not found.`}};
      }

      const sourceGeometryExplicit =
        typeof sourceGeometryField === 'string' && sourceGeometryField.trim().length > 0;
      const selectedMode = (mode || 'intersects') as 'intersects' | 'centroid' | 'within';
      const resolvedSourceGeom =
        resolveDatasetFieldName(sourceDataset, String(sourceGeometryField || '_geojson')) ||
        (sourceDataset.fields || []).find((f: any) => f?.type === 'geojson')?.name ||
        null;
      const resolvedSourceH3 = resolveH3FieldName(sourceDataset, sourceGeometryField || null);
      const effectiveSourceGeom =
        !sourceGeometryExplicit && selectedMode === 'intersects' && resolvedSourceH3
          ? null
          : resolvedSourceGeom;
      const resolvedClipGeom =
        resolveDatasetFieldName(clipDataset, String(clipGeometryField || '_geojson')) ||
        (clipDataset.fields || []).find((f: any) => f?.type === 'geojson')?.name ||
        null;
      if (!resolvedClipGeom) {
        return {
          llmResult: {
            success: false,
            details: 'Clip dataset must expose a geojson field.'
          }
        };
      }
      if (!effectiveSourceGeom && !resolvedSourceH3) {
        return {
          llmResult: {
            success: false,
            details: 'Source dataset must expose either a geojson field or an H3 field (h3_id/h3__id).'
          }
        };
      }

      const sourceRowEstimate = Number(sourceDataset.length || 0);
      const clipRowEstimate = Number(clipDataset.length || 0);
      const estimatedPairEval = sourceRowEstimate * Math.max(1, clipRowEstimate);
      const largeClipWorkload = estimatedPairEval >= 50000 || sourceRowEstimate >= 3000;
      const outputName =
        String(newDatasetName || '').trim() ||
        `${sourceDataset.label || sourceDataset.id}_clipped_${clipDataset.label || clipDataset.id}`;
      const isIntermediateOutput = showOnMap === false;
      const metricsExplicit = typeof includeIntersectionMetrics === 'boolean';
      const distinctExplicit = typeof includeDistinctPropertyCounts === 'boolean';
      const metricsEnabled =
        metricsExplicit ? includeIntersectionMetrics !== false : !isIntermediateOutput && !largeClipWorkload;
      const distinctCountsEnabled =
        distinctExplicit ? includeDistinctPropertyCounts !== false : !isIntermediateOutput && !largeClipWorkload;
      const distinctValueCountFieldsEnabled = includeDistinctPropertyValueCounts === true;
      const autoDiagnosticsDisabled = largeClipWorkload && !metricsExplicit && !distinctExplicit;
      const {label: resolvedTargetLabel, datasetId: resolvedTargetDatasetId} = getDatasetInfoByLabel(
        currentVisState?.datasets || {},
        outputName,
        'qmap_clip'
      );
      const sourceFieldNames = (sourceDataset.fields || [])
        .map((field: any) => String(field?.name || '').trim())
        .filter(Boolean);
      const clipPropertyFields =
        distinctCountsEnabled || distinctValueCountFieldsEnabled
          ? (clipDataset.fields || [])
              .filter((field: any) => {
                const fieldName = String(field?.name || '').trim();
                if (!fieldName || fieldName === resolvedClipGeom) return false;
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
      if (metricsEnabled) {
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
      if (distinctCountsEnabled) {
        const distinctCountFields = clipPropertyFields.map((fieldName: string) =>
          ensureUniqueFieldName(`${fieldName}__count`)
        );
        fieldCatalog.push(...distinctCountFields);
        styleableFields.push(...distinctCountFields);
        clipPropertyFields.forEach((fieldName: string, idx: number) => {
          const resolvedCountField = distinctCountFields[idx];
          if (!resolvedCountField) return;
          fieldAliases[`${fieldName}_count`] = resolvedCountField;
          fieldAliases[`distinct_count_${fieldName}`] = resolvedCountField;
        });
      }
      const defaultStyleField =
        styleableFields.find(fieldName => fieldName.toLowerCase().includes('intersection_pct')) ||
        styleableFields.find(fieldName => fieldName.toLowerCase().includes('intersection_area')) ||
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
          details:
            `Clipping "${sourceDataset.label || sourceDataset.id}" with "${clipDataset.label || clipDataset.id}" ` +
            `using mode ${selectedMode}${useActiveFilters !== false ? ' (active filters on)' : ''}` +
            `${metricsEnabled ? ' with intersection diagnostics fields' : ''}` +
            `${distinctCountsEnabled ? `${metricsEnabled ? ' and' : ' with'} distinct-count fields (<prop>__count)` : ''}` +
            `${distinctValueCountFieldsEnabled ? `${metricsEnabled || distinctCountsEnabled ? ' and' : ' with'} per-value count fields (<prop>__<value>__count)` : ''}.` +
            `${autoDiagnosticsDisabled ? ' Diagnostics auto-disabled for large workload to reduce wait/freeze risk (override by setting includeIntersectionMetrics/includeDistinctPropertyCounts=true).' : ''}` +
            `${showOnMap === false ? ' Output dataset will be created without auto layer.' : ''}`
        },
        additionalData: {
          executionKey: makeExecutionKey('clip-qmap-dataset-by-geometry'),
          sourceDatasetId: sourceDataset.id,
          clipDatasetId: clipDataset.id,
          sourceGeometryField: effectiveSourceGeom,
          sourceH3Field: resolvedSourceH3,
          clipGeometryField: resolvedClipGeom,
          mode: selectedMode,
          useActiveFilters: useActiveFilters !== false,
          maxSourceFeatures: resolveOptionalFeatureCap(maxSourceFeatures),
          maxClipFeatures: resolveOptionalFeatureCap(maxClipFeatures),
          includeIntersectionMetrics: metricsEnabled,
          includeDistinctPropertyCounts: distinctCountsEnabled,
          includeDistinctPropertyValueCounts: distinctValueCountFieldsEnabled,
          estimatedPairEval,
          autoDiagnosticsDisabled,
          showOnMap: showOnMap !== false,
          newDatasetName: resolvedTargetLabel,
          newDatasetId: resolvedTargetDatasetId,
          fieldAliases
        }
      };
    },
    component: function ClipQMapDatasetByGeometryComponent({
      executionKey,
      sourceDatasetId,
      clipDatasetId,
      sourceGeometryField,
      sourceH3Field,
      clipGeometryField,
      mode,
      useActiveFilters,
      maxSourceFeatures,
      maxClipFeatures,
      includeIntersectionMetrics,
      includeDistinctPropertyCounts,
      includeDistinctPropertyValueCounts,
      showOnMap,
      newDatasetName,
      newDatasetId
    }: {
      executionKey?: string;
      sourceDatasetId: string;
      clipDatasetId: string;
      sourceGeometryField: string | null;
      sourceH3Field: string | null;
      clipGeometryField: string;
      mode: 'intersects' | 'centroid' | 'within';
      useActiveFilters: boolean;
      maxSourceFeatures: number;
      maxClipFeatures: number;
      includeIntersectionMetrics: boolean;
      includeDistinctPropertyCounts: boolean;
      includeDistinctPropertyValueCounts: boolean;
      showOnMap: boolean;
      newDatasetName: string;
      newDatasetId: string;
    }) {
      const localDispatch = useDispatch<any>();
      const localVisState = useSelector(selectQMapVisState);
      const cancelledRef = React.useRef(false);
      const activeAbortControllersRef = React.useRef<Set<AbortController>>(new Set());
      const {shouldSkip, complete} = useToolExecution({
        executionKey,
        executedToolComponentKeys: EXECUTED_TOOL_COMPONENT_KEYS,
        rememberExecutedToolComponentKey
      });
      useEffect(() => {
        return () => {
          cancelledRef.current = true;
          activeAbortControllersRef.current.forEach(controller => {
            try {
              controller.abort();
            } catch {
              // ignore
            }
          });
          activeAbortControllersRef.current.clear();
        };
      }, []);

      useEffect(() => {
        if (shouldSkip()) return;

        const datasets = localVisState?.datasets || {};
        const sourceDataset = datasets[sourceDatasetId];
        const clipDataset = datasets[clipDatasetId];
        if (!sourceDataset || !clipDataset) return;
        complete();

        const resolveFilteredIndexes = (dataset: any): number[] => {
          const baseIdx = Array.isArray(dataset?.allIndexes)
            ? dataset.allIndexes
            : Array.from({length: Number(dataset?.length || 0)}, (_, i) => i);
          if (!useActiveFilters) return baseIdx;
          const filters = (localVisState?.filters || []).filter((f: any) => filterTargetsDataset(f, dataset.id));
          if (!filters.length) return baseIdx;
          return baseIdx.filter((rowIdx: number) => {
            return filters.every((filter: any) => {
              const rawFieldName = Array.isArray(filter?.name) ? filter.name[0] : filter?.name;
              if (!rawFieldName) return true;
              const resolvedFilterField = resolveDatasetFieldName(dataset, String(rawFieldName));
              if (!resolvedFilterField) return true;
              const rowValue = dataset.getValue(resolvedFilterField, rowIdx);
              const filterValue = filter?.value;
              if (Array.isArray(filterValue) && filterValue.length === 2 && filter?.type !== 'multiSelect') {
                const minV = filterValue[0];
                const maxV = filterValue[1];
                return Number(rowValue) >= Number(minV) && Number(rowValue) <= Number(maxV);
              }
              if (Array.isArray(filterValue)) {
                return filterValue.map(toComparable).includes(toComparable(rowValue));
              }
              return toComparable(rowValue) === toComparable(filterValue);
            });
          });
        };

        const useLoadingIndicator = shouldUseLoadingIndicator();
        if (useLoadingIndicator) {
          localDispatch(wrapTo('map', setLoadingIndicator({change: 1}) as any));
        }

        (async () => {
          try {
            const clipIdx = resolveFilteredIndexes(clipDataset).slice(
              0,
              resolveOptionalFeatureCap(maxClipFeatures)
            );
            const sourceIdx = resolveFilteredIndexes(sourceDataset).slice(
              0,
              resolveOptionalFeatureCap(maxSourceFeatures)
            );

            type ClipWorkerBoundaryRow = {
              geometry: unknown;
              properties: Record<string, unknown>;
            };
            type ClipWorkerSourceRow = {
              rowIdx: number;
              geometry?: unknown;
              h3Id?: unknown;
            };
            type ClipMetricsRow = {
              rowIdx: number;
              matchCount: number;
              intersectionAreaM2: number;
              intersectionPct: number;
              distinctValueCounts: Record<string, number>;
              propertyValueMatchCounts: Record<string, Record<string, number>>;
            };

            let matchedRows: number[] = [];
            const includeMetrics = includeIntersectionMetrics !== false;
            const includeDistinctCounts = includeDistinctPropertyCounts !== false;
            const includeValueCountFields = includeDistinctPropertyValueCounts === true;
            const includeDiagnostics = includeMetrics || includeDistinctCounts || includeValueCountFields;
            const clipReadChunkSize = Math.max(25, Math.min(100, QMAP_DEFAULT_CHUNK_SIZE));
            const sourceReadChunkSize = Math.max(25, Math.min(100, QMAP_DEFAULT_CHUNK_SIZE));
            const runWithAbortSignal = async <T,>(runner: (signal: AbortSignal) => Promise<T>): Promise<T> => {
              const controller = new AbortController();
              activeAbortControllersRef.current.add(controller);
              try {
                return await runner(controller.signal);
              } finally {
                activeAbortControllersRef.current.delete(controller);
              }
            };
            const metricsByRow = new Map<
              number,
              {
                matchCount: number;
                intersectionAreaM2: number;
                intersectionPct: number;
                distinctValueCounts: Record<string, number>;
                propertyValueMatchCounts: Record<string, Record<string, number>>;
              }
            >();

            const clipPropertyFields = includeDistinctCounts || includeValueCountFields
              ? (clipDataset.fields || [])
                  .filter((field: any) => {
                    const fieldName = String(field?.name || '');
                    if (!fieldName || fieldName === clipGeometryField) return false;
                    const fieldType = String(field?.type || '').toLowerCase();
                    return fieldType !== 'geojson';
                  })
                  .map((field: any) => String(field?.name || ''))
              : [];

            let clipRowsPayloadCache: ClipWorkerBoundaryRow[] | null = null;
            const getClipRowsPayload = async (): Promise<ClipWorkerBoundaryRow[]> => {
              if (clipRowsPayloadCache) return clipRowsPayloadCache;
              const rows = await mapIndexesChunked(
                clipIdx,
                (rowIdx: number): ClipWorkerBoundaryRow | null => {
                  const geometry = clipDataset.getValue(clipGeometryField, rowIdx);
                  if (geometry === null || geometry === undefined) return null;
                  const properties: Record<string, unknown> = {};
                  if (includeDistinctCounts || includeValueCountFields) {
                    clipPropertyFields.forEach((fieldName: string) => {
                      properties[fieldName] = clipDataset.getValue(fieldName, rowIdx);
                    });
                  }
                  return {geometry, properties};
                },
                clipReadChunkSize
              );
              clipRowsPayloadCache = rows.filter(Boolean) as ClipWorkerBoundaryRow[];
              return clipRowsPayloadCache;
            };

            const toClipFeatureDiagnosticsRows = (
              rows: ClipWorkerBoundaryRow[]
            ): ClipFeatureDiagnosticsInput[] => {
              return rows
                .map((row: ClipWorkerBoundaryRow): ClipFeatureDiagnosticsInput | null => {
                  const parsed = parseGeoJsonLike(row.geometry);
                  const feature = toTurfPolygonFeature(parsed);
                  if (!feature) return null;
                  return {feature, properties: row.properties || {}};
                })
                .filter(Boolean) as ClipFeatureDiagnosticsInput[];
            };

            const buildSourceRowsPayload = async (rowIndexes: number[]): Promise<ClipWorkerSourceRow[]> => {
              return mapIndexesChunked(
                rowIndexes,
                (rowIdx: number): ClipWorkerSourceRow => {
                  if (sourceGeometryField) {
                    return {rowIdx, geometry: sourceDataset.getValue(sourceGeometryField, rowIdx)};
                  }
                  return {rowIdx, h3Id: sourceDataset.getValue(String(sourceH3Field || ''), rowIdx)};
                },
                sourceReadChunkSize
              );
            };

            const applyMetricsRows = (rows: ClipMetricsRow[]) => {
              rows.forEach((metricsRow: ClipMetricsRow) => {
                const rowIdx = Number(metricsRow?.rowIdx);
                if (!Number.isFinite(rowIdx)) return;
                metricsByRow.set(rowIdx, {
                  matchCount: Number(metricsRow?.matchCount || 0),
                  intersectionAreaM2: Number(metricsRow?.intersectionAreaM2 || 0),
                  intersectionPct: Number(metricsRow?.intersectionPct || 0),
                  distinctValueCounts:
                    metricsRow?.distinctValueCounts && typeof metricsRow.distinctValueCounts === 'object'
                      ? metricsRow.distinctValueCounts
                      : {},
                  propertyValueMatchCounts:
                    metricsRow?.propertyValueMatchCounts && typeof metricsRow.propertyValueMatchCounts === 'object'
                      ? metricsRow.propertyValueMatchCounts
                      : {}
                });
              });
            };

            // Fast path for H3 sources: tessellate clip geometries at source H3 resolution, then set-lookup by h3_id.
            if (!sourceGeometryField && sourceH3Field && mode === 'intersects') {
              const inferredRes =
                inferDatasetH3Resolution(sourceDataset, sourceH3Field) ||
                (() => {
                  const first = sourceIdx.find((rowIdx: number) =>
                    normalizeH3Key(sourceDataset.getValue(sourceH3Field, rowIdx))
                  );
                  if (first === undefined) return null;
                  try {
                    return Number(getResolution(normalizeH3Key(sourceDataset.getValue(sourceH3Field, first))));
                  } catch {
                    return null;
                  }
                })();

              if (Number.isFinite(Number(inferredRes))) {
                const clipRowsPayload = await getClipRowsPayload();
                const clipGeometries = clipRowsPayload.map(row => row.geometry);
                const clipFeaturesForMetrics = includeDiagnostics
                  ? toClipFeatureDiagnosticsRows(clipRowsPayload)
                  : [];

                let clipIds: string[] = [];
                if (clipGeometries.length) {
                  try {
                    const result = (await runWithAbortSignal(signal =>
                      runH3Job({
                        name: 'tessellateGeometries',
                        payload: {resolution: Number(inferredRes), geometries: clipGeometries},
                        timeoutMs: Math.min(300000, Math.max(120000, 30000 + clipGeometries.length * 15)),
                        signal
                      })
                    )) as any;
                    clipIds = Array.isArray(result?.ids)
                      ? result.ids.map((id: string) => normalizeH3Key(id))
                      : [];
                  } catch (error) {
                    if ((error as Error)?.name === 'AbortError' && cancelledRef.current) return;
                    console.error('ClipQMapDatasetByGeometry H3 worker failed; fallback to local path:', error);
                    const idSet = new Set<string>();
                    clipGeometries.forEach((rawGeometry: unknown) => {
                      const polygons = extractPolygonsFromGeoJsonLike(rawGeometry);
                      if (!polygons.length) return;
                      const ids = getIntersectingH3Ids(polygons, Number(inferredRes));
                      ids.forEach((id: string) => idSet.add(normalizeH3Key(id)));
                    });
                    clipIds = Array.from(idSet);
                  }
                }

                if (cancelledRef.current) return;
                const clipSet = new Set(clipIds.filter(Boolean));
                if (clipSet.size) {
                  matchedRows = sourceIdx.filter((rowIdx: number) =>
                    clipSet.has(normalizeH3Key(sourceDataset.getValue(sourceH3Field, rowIdx)))
                  );

                  if (includeDiagnostics && clipFeaturesForMetrics.length && matchedRows.length) {
                    const sourceRowsForMetrics = await buildSourceRowsPayload(matchedRows);
                    const pairEstimate = sourceRowsForMetrics.length * Math.max(1, clipRowsPayload.length);
                    const useWorkerForMetrics = typeof Worker !== 'undefined';
                    let metricsFromWorkerApplied = false;
                    if (useWorkerForMetrics) {
                      try {
                        const adaptiveTimeout = Math.min(900000, Math.max(180000, 60000 + pairEstimate * 0.03));
                        const metricsWorkerResult = (await runWithAbortSignal(signal =>
                          runClipRowsJob({
                            payload: {
                              mode,
                              includeMetrics,
                              includeDistinctCounts,
                              includeValueCountFields,
                              sourceRows: sourceRowsForMetrics,
                              clipRows: clipRowsPayload
                            },
                            timeoutMs: adaptiveTimeout,
                            signal
                          })
                        )) as any;
                        applyMetricsRows(metricsWorkerResult?.metricsByRow || []);
                        metricsFromWorkerApplied = true;
                      } catch (error) {
                        if ((error as Error)?.name === 'AbortError' && cancelledRef.current) return;
                        console.error(
                          'ClipQMapDatasetByGeometry diagnostics worker failed; fallback to local path:',
                          error
                        );
                      }
                    }

                    if (!metricsFromWorkerApplied) {
                      if (pairEstimate > QMAP_CLIP_MAX_LOCAL_PAIR_EVAL) {
                        console.error(
                          `[qmap-ai] clip diagnostics local fallback skipped to avoid UI freeze (pairEstimate=${pairEstimate}, threshold=${QMAP_CLIP_MAX_LOCAL_PAIR_EVAL}).`
                        );
                      } else {
                        for (let i = 0; i < matchedRows.length; i += 1) {
                          const rowIdx = matchedRows[i];
                          const sourceFeature = h3CellToPolygonFeature(sourceDataset.getValue(sourceH3Field, rowIdx));
                          if (!sourceFeature) continue;
                          const metrics = computeClipMetricsForFeature(
                            sourceFeature,
                            clipFeaturesForMetrics,
                            {
                              mode,
                              includeAreaMetrics: true,
                              includeDistinctCounts,
                              includeValueCountFields
                            }
                          );
                          if (metrics.matchCount > 0) {
                            metricsByRow.set(rowIdx, metrics);
                          }
                          if (i > 0 && i % QMAP_DEFAULT_CHUNK_SIZE === 0) {
                            await yieldToMainThread();
                          }
                        }
                      }
                    }
                  }
                }
              }
            }

            // Geometric path for geojson sources (or when H3 fast path cannot infer resolution), worker-first.
            if (!matchedRows.length) {
              const clipRowsPayload = await getClipRowsPayload();
              if (!clipRowsPayload.length) {
                return;
              }

              const sourceRowsPayload = await buildSourceRowsPayload(sourceIdx);
              const pairEstimate = sourceRowsPayload.length * Math.max(1, clipRowsPayload.length);
              const useWorkerFirst = typeof Worker !== 'undefined';

              let workerApplied = false;
              if (useWorkerFirst) {
                try {
                  const adaptiveTimeout = Math.min(900000, Math.max(180000, 60000 + pairEstimate * 0.03));
                  const workerResult = (await runWithAbortSignal(signal =>
                    runClipRowsJob({
                      payload: {
                        mode,
                        includeMetrics,
                        includeDistinctCounts,
                        includeValueCountFields,
                        sourceRows: sourceRowsPayload,
                        clipRows: clipRowsPayload
                      },
                      timeoutMs: adaptiveTimeout,
                      signal
                    })
                  )) as any;
                  matchedRows = Array.from(
                    new Set(
                      (workerResult?.matchedRows || [])
                        .map((rowIdx: unknown) => Number(rowIdx))
                        .filter((rowIdx: number) => Number.isFinite(rowIdx))
                    )
                  );
                  applyMetricsRows(workerResult?.metricsByRow || []);
                  workerApplied = true;
                } catch (error) {
                  if ((error as Error)?.name === 'AbortError' && cancelledRef.current) return;
                  console.error('ClipQMapDatasetByGeometry worker failed; fallback to local path:', error);
                }
              }

              if (!workerApplied) {
                if (pairEstimate > QMAP_CLIP_MAX_LOCAL_PAIR_EVAL) {
                  console.error(
                    `[qmap-ai] clip local fallback aborted to avoid UI freeze (pairEstimate=${pairEstimate}, threshold=${QMAP_CLIP_MAX_LOCAL_PAIR_EVAL}).`
                  );
                  return;
                }
                const clipFeatureRows = toClipFeatureDiagnosticsRows(clipRowsPayload);
                if (!clipFeatureRows.length) {
                  return;
                }
                const clipFeatureRowsWithBbox = clipFeatureRows.map((row: ClipFeatureDiagnosticsInput) => ({
                  ...row,
                  bbox: geometryToBbox((row?.feature as any)?.geometry)
                }));

                for (let i = 0; i < sourceRowsPayload.length; i += 1) {
                  const sourceRow = sourceRowsPayload[i];
                  const rowIdx = sourceRow.rowIdx;
                  const sourceFeature = sourceGeometryField
                    ? toTurfPolygonFeature(parseGeoJsonLike(sourceRow.geometry))
                    : h3CellToPolygonFeature(sourceRow.h3Id);

                  if (!sourceFeature) {
                    if (i > 0 && i % QMAP_DEFAULT_CHUNK_SIZE === 0) {
                      await yieldToMainThread();
                    }
                    continue;
                  }
                  const sourceBbox = geometryToBbox((sourceFeature as any)?.geometry);
                  const candidateClipRows = sourceBbox
                    ? clipFeatureRowsWithBbox.filter(
                        (row: ClipFeatureDiagnosticsInput & {bbox: [number, number, number, number] | null}) =>
                          !row.bbox || boundsOverlap(sourceBbox as any, row.bbox as any)
                      )
                    : clipFeatureRowsWithBbox;
                  if (!candidateClipRows.length) {
                    if (i > 0 && i % QMAP_DEFAULT_CHUNK_SIZE === 0) {
                      await yieldToMainThread();
                    }
                    continue;
                  }

                  if (includeDiagnostics) {
                    const metrics = computeClipMetricsForFeature(
                      sourceFeature,
                      candidateClipRows as ClipFeatureDiagnosticsInput[],
                      {
                        mode,
                        includeAreaMetrics: true,
                        includeDistinctCounts,
                        includeValueCountFields
                      }
                    );
                    if (metrics.matchCount > 0) {
                      matchedRows.push(rowIdx);
                      metricsByRow.set(rowIdx, metrics);
                    }
                    if (i > 0 && i % QMAP_DEFAULT_CHUNK_SIZE === 0) {
                      await yieldToMainThread();
                    }
                    continue;
                  }

                  const matchProbe = computeClipMetricsForFeature(
                    sourceFeature,
                    candidateClipRows as ClipFeatureDiagnosticsInput[],
                    {
                      mode,
                      includeAreaMetrics: false,
                      includeDistinctCounts: false,
                      includeValueCountFields: false
                    }
                  );
                  if (matchProbe.matchCount > 0) {
                    matchedRows.push(rowIdx);
                  }
                  if (i > 0 && i % QMAP_DEFAULT_CHUNK_SIZE === 0) {
                    await yieldToMainThread();
                  }
                }
              }
            }

            if (cancelledRef.current) return;
            if (!matchedRows.length) {
              return;
            }

            const datasetFields = (sourceDataset.fields || []).map((f: any) => ({name: f.name, type: f.type}));
            const existingFieldNames = new Set(datasetFields.map((f: any) => String(f?.name || '')));
            const metricFieldNames = {
              matchCount: 'qmap_clip_match_count',
              intersectionAreaM2: 'qmap_clip_intersection_area_m2',
              intersectionPct: 'qmap_clip_intersection_pct'
            };
            const ensureUniqueMetricField = (baseName: string): string => {
              let candidate = baseName;
              let suffix = 1;
              while (existingFieldNames.has(candidate)) {
                candidate = `${baseName}_${suffix}`;
                suffix += 1;
              }
              existingFieldNames.add(candidate);
              return candidate;
            };
            let resolvedMetricFields:
              | {
                  matchCount: string;
                  intersectionAreaM2: string;
                  intersectionPct: string;
                }
              | null = null;
            const resolvedDistinctCountFields: Record<string, string> = {};
            const resolvedDistinctValueCountFields: Array<{
              sourceFieldName: string;
              sourceFieldValue: string;
              fieldName: string;
            }> = [];
            if (includeMetrics) {
              resolvedMetricFields = {
                matchCount: ensureUniqueMetricField(metricFieldNames.matchCount),
                intersectionAreaM2: ensureUniqueMetricField(metricFieldNames.intersectionAreaM2),
                intersectionPct: ensureUniqueMetricField(metricFieldNames.intersectionPct)
              };
              datasetFields.push(
                {name: resolvedMetricFields.matchCount, type: ALL_FIELD_TYPES.integer},
                {name: resolvedMetricFields.intersectionAreaM2, type: ALL_FIELD_TYPES.real},
                {name: resolvedMetricFields.intersectionPct, type: ALL_FIELD_TYPES.real}
              );
            }
            if (includeDistinctCounts) {
              clipPropertyFields.forEach((sourceFieldName: string) => {
                const countFieldName = ensureUniqueMetricField(`${sourceFieldName}__count`);
                resolvedDistinctCountFields[sourceFieldName] = countFieldName;
                datasetFields.push({name: countFieldName, type: ALL_FIELD_TYPES.integer});
              });
            }
            if (includeValueCountFields) {
              const valueKeysByField = new Map<string, Set<string>>();
              metricsByRow.forEach(metrics => {
                const byField = metrics?.propertyValueMatchCounts || {};
                Object.entries(byField).forEach(([sourceFieldName, counts]) => {
                  if (!counts || typeof counts !== 'object') return;
                  const set = valueKeysByField.get(sourceFieldName) || new Set<string>();
                  Object.entries(counts).forEach(([rawValue, rawCount]) => {
                    const numericCount = Number(rawCount);
                    if (!Number.isFinite(numericCount) || numericCount <= 0) return;
                    set.add(String(rawValue));
                  });
                  if (set.size) {
                    valueKeysByField.set(sourceFieldName, set);
                  }
                });
              });
              Array.from(valueKeysByField.entries())
                .sort(([left], [right]) => left.localeCompare(right))
                .forEach(([sourceFieldName, valueSet]) => {
                  Array.from(valueSet)
                    .sort((left, right) => left.localeCompare(right))
                    .forEach(sourceFieldValue => {
                      const valueToken = normalizeFieldValueToken(sourceFieldValue);
                      const valueCountFieldName = ensureUniqueMetricField(
                        `${sourceFieldName}__${valueToken}__count`
                      );
                      resolvedDistinctValueCountFields.push({
                        sourceFieldName,
                        sourceFieldValue,
                        fieldName: valueCountFieldName
                      });
                      datasetFields.push({name: valueCountFieldName, type: ALL_FIELD_TYPES.integer});
                    });
                });
            }

            const rows = await mapIndexesChunked(
              matchedRows,
              (rowIdx: number) => {
                const baseValues = (sourceDataset.fields || []).map((f: any) =>
                  sourceDataset.getValue(f.name, rowIdx)
                );
                const metrics = metricsByRow.get(rowIdx) || {
                  matchCount: 0,
                  intersectionAreaM2: 0,
                  intersectionPct: 0,
                  distinctValueCounts: {},
                  propertyValueMatchCounts: {}
                };
                const metricValues = resolvedMetricFields
                  ? [metrics.matchCount, metrics.intersectionAreaM2, metrics.intersectionPct]
                  : [];
                const distinctCountValues = includeDistinctCounts
                  ? Object.keys(resolvedDistinctCountFields).map((sourceFieldName: string) => {
                      const rawValue = metrics.distinctValueCounts?.[sourceFieldName];
                      const parsedValue = Number(rawValue);
                      return Number.isFinite(parsedValue) ? parsedValue : 0;
                    })
                  : [];
                const distinctValueCountValues = includeValueCountFields
                  ? resolvedDistinctValueCountFields.map(({sourceFieldName, sourceFieldValue}) => {
                      const rawValue = metrics.propertyValueMatchCounts?.[sourceFieldName]?.[sourceFieldValue];
                      const parsedValue = Number(rawValue);
                      return Number.isFinite(parsedValue) ? parsedValue : 0;
                    })
                  : [];
                return [...baseValues, ...metricValues, ...distinctCountValues, ...distinctValueCountValues];
              },
              QMAP_DEFAULT_CHUNK_SIZE
            );

            const existing = Object.values(datasets || {}).find(
              (d: any) => String(d?.label || '').toLowerCase() === String(newDatasetName).toLowerCase()
            ) as any;
            const datasetToUse = {
              info: {
                id: existing?.id || newDatasetId,
                label: newDatasetName
              },
              data: {
                fields: datasetFields,
                rows
              }
            };

            if (existing?.id) {
              localDispatch(
                wrapTo(
                  'map',
                  replaceDataInMap({
                    datasetToReplaceId: existing.id,
                    datasetToUse,
                    options: {
                      keepExistingConfig: true,
                      centerMap: false,
                      autoCreateLayers: false
                    }
                  }) as any
                )
              );
            } else {
              localDispatch(
                wrapTo(
                  'map',
                  addDataToMap({
                    datasets: datasetToUse as any,
                    options: {autoCreateLayers: showOnMap !== false, centerMap: false}
                  }) as any
                )
              );
            }

            if (showOnMap && QMAP_AUTO_HIDE_SOURCE_LAYERS) {
              hideLayersForDatasetIds(localDispatch, localVisState?.layers || [], [sourceDataset.id, clipDataset.id]);
            }
          } finally {
            if (useLoadingIndicator) {
              localDispatch(wrapTo('map', setLoadingIndicator({change: -1}) as any));
            }
          }
        })();
      }, [
        localDispatch,
        localVisState,
        executionKey,
        sourceDatasetId,
        clipDatasetId,
        sourceGeometryField,
        sourceH3Field,
        clipGeometryField,
        mode,
        useActiveFilters,
        maxSourceFeatures,
        maxClipFeatures,
        includeIntersectionMetrics,
        includeDistinctPropertyCounts,
        includeDistinctPropertyValueCounts,
        showOnMap,
        newDatasetName,
        newDatasetId,
        shouldSkip,
        complete
      ]);
      return null;
    }
  });


}

export function createZonalStatsByAdminTool(ctx: QMapToolContext) {
  const {
    QMAP_AGGREGATION_SCHEMA,
    QMAP_WEIGHT_MODE_SCHEMA,
    QMAP_DEFAULT_CHUNK_SIZE,
    QMAP_ZONAL_MAX_LOCAL_PAIR_EVAL,
    QMAP_AUTO_HIDE_SOURCE_LAYERS,
    getCurrentVisState,
    resolveDatasetByName,
    resolveGeojsonFieldName,
    resolveH3FieldName,
    inferDatasetH3Resolution,
    resolveDatasetFieldName,
    getFilteredDatasetIndexes,
    resolveOptionalFeatureCap,
    getDatasetInfoByLabel,
    makeExecutionKey,
    EXECUTED_TOOL_COMPONENT_KEYS,
    rememberExecutedToolComponentKey,
    shouldUseLoadingIndicator,
    mapIndexesChunked,
    normalizeH3Key,
    yieldToMainThread,
    parseGeoJsonLike,
    h3CellToPolygonFeature,
    toTurfFeature,
    turfCentroid,
    geometryToBbox,
    geometryBboxOverlap,
    toTurfPolygonFeature,
    turfBooleanPointInPolygon,
    turfBooleanIntersects,
    isPolygonLikeFeature,
    turfIntersectSafe,
    turfArea,
    runZonalStatsJob,
    upsertDerivedDatasetRows,
    hideLayersForDatasetIds
  } = ctx;

  return extendedTool({
    description:
      'Compute zonal statistics on administrative geometries/H3 from a value dataset with optional area-weighted aggregation.',
    parameters: z.object({
      adminDatasetName: z.string(),
      valueDatasetName: z.string(),
      adminGeometryField: z.string().optional(),
      valueGeometryField: z.string().optional(),
      valueField: z.string().optional().describe('Numeric field to aggregate; omit for count-only'),
      aggregation: QMAP_AGGREGATION_SCHEMA.describe('Default sum when valueField exists, else count'),
      weightMode: QMAP_WEIGHT_MODE_SCHEMA.describe('Default area_weighted'),
      useActiveFilters: z.boolean().optional().describe('Default true'),
      maxAdminFeatures: z
        .number()
        .optional()
        .describe('Optional explicit cap on admin features. Unset = full matched coverage (no truncation).'),
      maxValueFeatures: z
        .number()
        .optional()
        .describe('Optional explicit cap on value features. Unset = full matched coverage (no truncation).'),
      outputFieldName: z.string().optional().describe('Default zonal_value'),
      showOnMap: z
        .boolean()
        .optional()
        .describe('Default false. Set true to auto-create a map layer for the output dataset.'),
      newDatasetName: z.string().optional()
    }),
    execute: async ({
      adminDatasetName,
      valueDatasetName,
      adminGeometryField,
      valueGeometryField,
      valueField,
      aggregation,
      weightMode,
      useActiveFilters,
      maxAdminFeatures,
      maxValueFeatures,
      outputFieldName,
      showOnMap,
      newDatasetName
    }) => {
      const currentVisState = getCurrentVisState();
      const admin = resolveDatasetByName(currentVisState?.datasets || {}, adminDatasetName);
      const values = resolveDatasetByName(currentVisState?.datasets || {}, valueDatasetName);
      if (!admin?.id) return {llmResult: {success: false, details: `Admin dataset "${adminDatasetName}" not found.`}};
      if (!values?.id) return {llmResult: {success: false, details: `Value dataset "${valueDatasetName}" not found.`}};
      const adminGeom = resolveGeojsonFieldName(admin, adminGeometryField);
      const valueGeom = resolveGeojsonFieldName(values, valueGeometryField);
      const adminH3 = resolveH3FieldName(admin, adminGeometryField || null);
      const valueH3 = resolveH3FieldName(values, valueGeometryField || null);
      const canUseH3FastPath = Boolean(adminH3 && valueH3);
      const adminH3Resolution = adminH3 ? inferDatasetH3Resolution(admin, adminH3) : null;
      const valueH3Resolution = valueH3 ? inferDatasetH3Resolution(values, valueH3) : null;
      const h3ResolutionMismatch =
        canUseH3FastPath &&
        adminH3Resolution !== null &&
        valueH3Resolution !== null &&
        adminH3Resolution !== valueH3Resolution;
      if (h3ResolutionMismatch && !adminGeom && !valueGeom) {
        return {
          llmResult: {
            success: false,
            details:
              `H3 resolution mismatch for zonal stats: admin=${adminH3Resolution}, values=${valueH3Resolution}. ` +
              'Align resolutions first (e.g. aggregateDatasetToH3), then retry.'
          }
        };
      }
      const executionMode: 'h3_fast_path' | 'geometry' = canUseH3FastPath && !h3ResolutionMismatch ? 'h3_fast_path' : 'geometry';
      if ((!adminGeom && !adminH3) || (!valueGeom && !valueH3)) {
        return {
          llmResult: {
            success: false,
            details: 'Both datasets must include either a geojson field or an H3 field (h3_id/h3__id).'
          }
        };
      }
      const valueFields = (values.fields || []).map((f: any) => String(f?.name || '')).filter(Boolean);
      const numericValueFields = valueFields.filter((fieldName: string) => {
        const field = (values.fields || []).find((f: any) => String(f?.name || '') === fieldName);
        const fieldType = String(field?.type || '').toLowerCase();
        return (
          fieldType === String(ALL_FIELD_TYPES.integer).toLowerCase() ||
          fieldType === String(ALL_FIELD_TYPES.real).toLowerCase() ||
          fieldType === 'integer' ||
          fieldType === 'real'
        );
      });
      const agg = (aggregation ||
        (valueField ? 'sum' : 'count')) as 'count' | 'sum' | 'avg' | 'min' | 'max';
      let resolvedValueField = valueField ? resolveDatasetFieldName(values, valueField) : null;
      if (!resolvedValueField && agg !== 'count') {
        const fallbackCandidates = Array.from(
          new Set(
            [
              valueField ? String(valueField).trim() : '',
              valueField ? `${String(valueField).trim()}__${agg}` : '',
              agg,
              agg === 'sum' ? 'count_weighted' : ''
            ].filter(Boolean)
          )
        );
        for (const candidate of fallbackCandidates) {
          const resolvedCandidate = resolveDatasetFieldName(values, candidate);
          if (resolvedCandidate) {
            resolvedValueField = resolvedCandidate;
            break;
          }
        }
        if (!resolvedValueField && numericValueFields.length === 1) {
          resolvedValueField = numericValueFields[0];
        }
      }
      if (agg !== 'count' && !resolvedValueField) {
        return {
          llmResult: {
            success: false,
            details:
              `Aggregation "${agg}" requires a numeric valueField on dataset "${values.label || values.id}", ` +
              `but "${valueField || 'undefined'}" was not resolved. Available numeric fields: ` +
              `${numericValueFields.slice(0, 12).join(', ') || '[none]'}.`
          }
        };
      }
      const effectiveUseActiveFilters = useActiveFilters !== false;
      const resolvedMaxAdminFeatures = resolveOptionalFeatureCap(maxAdminFeatures);
      const resolvedMaxValueFeatures = resolveOptionalFeatureCap(maxValueFeatures);
      const adminCount = getFilteredDatasetIndexes(admin, currentVisState, effectiveUseActiveFilters).length;
      const valueCount = getFilteredDatasetIndexes(values, currentVisState, effectiveUseActiveFilters).length;
      const cappedAdminCount = Math.min(adminCount, resolvedMaxAdminFeatures);
      const cappedValueCount = Math.min(valueCount, resolvedMaxValueFeatures);
      const estimatedPairEvaluations = cappedAdminCount * cappedValueCount;
      const pairEvalBudget =
        (weightMode || 'area_weighted') === 'area_weighted'
          ? Math.max(10000, Math.floor(QMAP_ZONAL_MAX_LOCAL_PAIR_EVAL / 2))
          : QMAP_ZONAL_MAX_LOCAL_PAIR_EVAL;
      if (executionMode === 'geometry' && estimatedPairEvaluations > pairEvalBudget) {
        const inferH3ResolutionFromLabel = (label: string): number | null => {
          const text = String(label || '').toLowerCase();
          const match = text.match(/(?:^|[_\s-])r(\d{1,2})(?:$|[_\s-])/);
          if (!match) return null;
          const parsed = Number(match[1]);
          return Number.isFinite(parsed) ? parsed : null;
        };
        const suggestStartingResolution = () => {
          if (valueH3Resolution !== null && Number.isFinite(Number(valueH3Resolution))) {
            return Number(valueH3Resolution);
          }
          const inferredFromLabel = inferH3ResolutionFromLabel(values.label || values.id);
          if (inferredFromLabel !== null) return inferredFromLabel;
          if (valueCount >= 12000) return 6;
          if (valueCount >= 4000) return 5;
          return 4;
        };
        const currentSuggestedResolution = suggestStartingResolution();
        const nextResolution = Math.max(4, currentSuggestedResolution - 1);
        const replaceOrAppendResolutionSuffix = (name: string, nextRes: number) => {
          const raw = String(name || '').trim();
          if (!raw) return `h3_agg_r${nextRes}`;
          if (/(?:^|[_\s-])r\d{1,2}(?:$|[_\s-])/i.test(raw)) {
            return raw.replace(/(?:^|[_\s-])r\d{1,2}(?:$|[_\s-])/i, match =>
              match.replace(/r\d{1,2}/i, `r${nextRes}`)
            );
          }
          return `${raw}_r${nextRes}`;
        };
        const aggregateOp = agg === 'count' ? 'count' : agg;
        const reroutedDatasetName = replaceOrAppendResolutionSuffix(
          String(values.label || values.id || 'value_dataset_h3'),
          nextResolution
        );
        const reroutedValueField =
          agg === 'count'
            ? undefined
            : aggregateOp === 'sum'
            ? 'sum'
            : aggregateOp === 'avg'
            ? 'avg'
            : aggregateOp === 'min'
            ? 'min'
            : aggregateOp === 'max'
            ? 'max'
            : undefined;
        if (currentSuggestedResolution <= 4) {
          return {
            llmResult: {
              success: false,
              details:
                `zonalStatsByAdmin aborted to prevent UI freeze: estimated pair evaluations ` +
                `${estimatedPairEvaluations.toLocaleString()} exceed budget ${pairEvalBudget.toLocaleString()} ` +
                `(admin=${cappedAdminCount.toLocaleString()}, values=${cappedValueCount.toLocaleString()}, mode=${
                  weightMode || 'area_weighted'
                }). Deterministic H3 fallback reached minimum resolution r4; cannot proceed without truncation.`
            }
          };
        }
        return {
          llmResult: {
            success: false,
            retryWithTool: 'aggregateDatasetToH3',
            retryWithArgs: {
              datasetName: values.label || values.id,
              resolution: nextResolution,
              valueField: agg === 'count' ? undefined : resolvedValueField || valueField || undefined,
              operations: [aggregateOp],
              targetDatasetName: reroutedDatasetName,
              weightMode: valueH3 ? 'intersects' : 'area_weighted',
              useActiveFilters: effectiveUseActiveFilters,
              showOnMap: false
            },
            retryReason: 'zonal-ui-freeze-budget',
            suggestedNextZonalArgs: {
              adminDatasetName: admin.label || admin.id,
              valueDatasetName: reroutedDatasetName,
              valueField: reroutedValueField,
              aggregation: agg,
              outputFieldName: String(outputFieldName || '').trim() || 'zonal_value',
              showOnMap: showOnMap === true,
              newDatasetName: String(newDatasetName || '').trim() || `${admin.label || admin.id}_zonal_${reroutedDatasetName}`
            },
            details:
              `zonalStatsByAdmin aborted to prevent UI freeze: estimated pair evaluations ` +
              `${estimatedPairEvaluations.toLocaleString()} exceed budget ${pairEvalBudget.toLocaleString()} ` +
              `(admin=${cappedAdminCount.toLocaleString()}, values=${cappedValueCount.toLocaleString()}, mode=${
                weightMode || 'area_weighted'
              }). Deterministic fallback selected: aggregate value dataset to H3 r${nextResolution} ` +
              `("${reroutedDatasetName}") and rerun zonal stats. If it still exceeds budget, repeat with r${
                Math.max(4, nextResolution - 1)
              } until r4; then stop with explicit failure.`
          }
        };
      }
      const targetField = String(outputFieldName || '').trim() || 'zonal_value';
      const target =
        String(newDatasetName || '').trim() || `${admin.label || admin.id}_zonal_${values.label || values.id}`;
      const {label: resolvedTargetLabel, datasetId: resolvedTargetDatasetId} = getDatasetInfoByLabel(
        currentVisState?.datasets || {},
        target,
        'qmap_zonal_stats'
      );
      const fieldCatalog = Array.from(
        new Set([
          ...((admin.fields || []).map((field: any) => String(field?.name || '').trim()).filter(Boolean) as string[]),
          targetField
        ])
      );
      const aggregationOutputs: Record<string, string> = {
        [String(agg || '').trim().toLowerCase() || 'count']: targetField
      };
      const fieldAliases: Record<string, string> = {};
      if (resolvedValueField) {
        const aliasBase = String(resolvedValueField).trim();
        const aggKey = String(agg || '').trim().toLowerCase() || 'count';
        fieldAliases[`${aggKey}_${aliasBase}`] = targetField;
        fieldAliases[`${aliasBase}_${aggKey}`] = targetField;
      }
      return {
        llmResult: {
          success: true,
          dataset: resolvedTargetLabel,
          datasetId: resolvedTargetDatasetId,
          outputFieldName: targetField,
          fieldCatalog,
          numericFields: [targetField],
          styleableFields: [targetField],
          defaultStyleField: targetField,
          aggregationOutputs,
          fieldAliases,
          details:
            `Computing zonal stats (${agg}, ${weightMode || 'area_weighted'}, mode=${executionMode}).` +
            `${showOnMap === true ? '' : ' Output dataset will be created without auto layer.'}`
        },
        additionalData: {
          executionKey: makeExecutionKey('zonal-stats-by-admin'),
          adminDatasetId: admin.id,
          valueDatasetId: values.id,
          adminGeometryField: adminGeom || null,
          valueGeometryField: valueGeom || null,
          adminH3Field: adminH3 || null,
          valueH3Field: valueH3 || null,
          valueField: resolvedValueField,
          aggregation: agg,
          weightMode: (weightMode || 'area_weighted') as 'intersects' | 'centroid' | 'area_weighted',
          executionMode,
          useActiveFilters: effectiveUseActiveFilters,
          maxAdminFeatures: resolvedMaxAdminFeatures,
          maxValueFeatures: resolvedMaxValueFeatures,
          estimatedPairEvaluations,
          pairEvalBudget,
          adminH3Resolution,
          valueH3Resolution,
          outputFieldName: targetField,
          fieldCatalog,
          numericFields: [targetField],
          styleableFields: [targetField],
          defaultStyleField: targetField,
          aggregationOutputs,
          fieldAliases,
          showOnMap: showOnMap === true,
          newDatasetName: resolvedTargetLabel,
          newDatasetId: resolvedTargetDatasetId
        }
      };
    },
    component: function ZonalStatsByAdminComponent({
      executionKey,
      adminDatasetId,
      valueDatasetId,
      adminGeometryField,
      valueGeometryField,
      adminH3Field,
      valueH3Field,
      valueField,
      aggregation,
      weightMode,
      executionMode,
      useActiveFilters,
      maxAdminFeatures,
      maxValueFeatures,
      outputFieldName,
      showOnMap,
      newDatasetName
    }: {
      executionKey?: string;
      adminDatasetId: string;
      valueDatasetId: string;
      adminGeometryField: string | null;
      valueGeometryField: string | null;
      adminH3Field: string | null;
      valueH3Field: string | null;
      valueField: string | null;
      aggregation: 'count' | 'sum' | 'avg' | 'min' | 'max';
      weightMode: 'intersects' | 'centroid' | 'area_weighted';
      executionMode: 'h3_fast_path' | 'geometry';
      useActiveFilters: boolean;
      maxAdminFeatures: number;
      maxValueFeatures: number;
      outputFieldName: string;
      showOnMap: boolean;
      newDatasetName: string;
    }) {
      const localDispatch = useDispatch<any>();
      const localVisState = useSelector(selectQMapVisState);
      const cancelledRef = React.useRef(false);
      const activeAbortControllersRef = React.useRef<Set<AbortController>>(new Set());
      const {shouldSkip, complete} = useToolExecution({
        executionKey,
        executedToolComponentKeys: EXECUTED_TOOL_COMPONENT_KEYS,
        rememberExecutedToolComponentKey
      });
      useEffect(() => {
        return () => {
          cancelledRef.current = true;
          activeAbortControllersRef.current.forEach(controller => {
            try {
              controller.abort();
            } catch {
              // ignore
            }
          });
          activeAbortControllersRef.current.clear();
        };
      }, []);
      useEffect(() => {
        if (shouldSkip()) return;
        const datasets = localVisState?.datasets || {};
        const admin = datasets[adminDatasetId];
        const values = datasets[valueDatasetId];
        if (!admin || !values) return;
        complete();

        const useLoadingIndicator = shouldUseLoadingIndicator();
        if (useLoadingIndicator) {
          localDispatch(wrapTo('map', setLoadingIndicator({change: 1}) as any));
        }

        (async () => {
          try {
            const adminIdx = getFilteredDatasetIndexes(admin, localVisState, useActiveFilters).slice(0, maxAdminFeatures);
            const valueIdx = getFilteredDatasetIndexes(values, localVisState, useActiveFilters).slice(0, maxValueFeatures);
            const rowsOut: Array<Record<string, unknown>> = [];
            const loopYieldEvery = Math.max(20, Math.floor(QMAP_DEFAULT_CHUNK_SIZE / 3));
            const runWithAbortSignal = async <T,>(runner: (signal: AbortSignal) => Promise<T>): Promise<T> => {
              const controller = new AbortController();
              activeAbortControllersRef.current.add(controller);
              try {
                return await runner(controller.signal);
              } finally {
                activeAbortControllersRef.current.delete(controller);
              }
            };

            if (executionMode === 'h3_fast_path' && adminH3Field && valueH3Field) {
              type H3Bucket = {count: number; sum: number; denom: number; min: number | null; max: number | null};
              const valueLookup = new Map<string, H3Bucket>();
              for (let i = 0; i < valueIdx.length; i += 1) {
                if (cancelledRef.current) return;
                const rowIdx = valueIdx[i];
                const h3 = normalizeH3Key(values.getValue(valueH3Field, rowIdx));
                if (!h3) continue;
                const bucket = valueLookup.get(h3) || {count: 0, sum: 0, denom: 0, min: null, max: null};
                bucket.count += 1;
                if (valueField) {
                  const raw = Number(values.getValue(valueField, rowIdx));
                  if (Number.isFinite(raw)) {
                    bucket.sum += raw;
                    bucket.denom += 1;
                    bucket.min = bucket.min === null ? raw : Math.min(bucket.min, raw);
                    bucket.max = bucket.max === null ? raw : Math.max(bucket.max, raw);
                  }
                }
                valueLookup.set(h3, bucket);
                if (i > 0 && i % loopYieldEvery === 0) {
                  await yieldToMainThread();
                  if (cancelledRef.current) return;
                }
              }

              if (!valueLookup.size) return;
              for (let i = 0; i < adminIdx.length; i += 1) {
                if (cancelledRef.current) return;
                const rowIdx = adminIdx[i];
                const adminH3 = normalizeH3Key(admin.getValue(adminH3Field, rowIdx));
                if (!adminH3) {
                  if (i > 0 && i % loopYieldEvery === 0) {
                    await yieldToMainThread();
                  }
                  continue;
                }
                const bucket = valueLookup.get(adminH3) || null;
                const base: Record<string, unknown> = {};
                (admin.fields || []).forEach((f: any) => {
                  base[f.name] = admin.getValue(f.name, rowIdx);
                });
                if (aggregation === 'count' || !valueField) {
                  base[outputFieldName] = bucket ? bucket.count : 0;
                } else if (aggregation === 'sum') {
                  base[outputFieldName] = bucket ? bucket.sum : 0;
                } else if (aggregation === 'avg') {
                  base[outputFieldName] = bucket && bucket.denom > 0 ? bucket.sum / bucket.denom : null;
                } else if (aggregation === 'min') {
                  base[outputFieldName] = bucket ? bucket.min : null;
                } else if (aggregation === 'max') {
                  base[outputFieldName] = bucket ? bucket.max : null;
                }
                rowsOut.push(base);
                if (i > 0 && i % loopYieldEvery === 0) {
                  await yieldToMainThread();
                }
              }
            } else {
              const pairEstimate = adminIdx.length * Math.max(1, valueIdx.length);
              const localFallbackBudget =
                weightMode === 'area_weighted'
                  ? Math.max(10000, Math.floor(QMAP_ZONAL_MAX_LOCAL_PAIR_EVAL / 2))
                  : QMAP_ZONAL_MAX_LOCAL_PAIR_EVAL;

              const runLocalGeometryFallback = async () => {
                const valueFeaturesRaw = await mapIndexesChunked(
                  valueIdx,
                  (rowIdx: number) => {
                    const valueGeometryRaw = valueGeometryField
                      ? parseGeoJsonLike(values.getValue(valueGeometryField, rowIdx))
                      : h3CellToPolygonFeature(values.getValue(String(valueH3Field || ''), rowIdx));
                    const feature = toTurfFeature(valueGeometryRaw);
                    if (!feature) return null;
                    const centroid = turfCentroid(feature as any);
                    return {
                      feature,
                      value: valueField ? Number(values.getValue(valueField, rowIdx)) : NaN,
                      bbox: geometryToBbox((feature as any)?.geometry),
                      centroid
                    };
                  },
                  Math.max(50, Math.min(200, QMAP_DEFAULT_CHUNK_SIZE))
                );
                if (cancelledRef.current) return;
                const valueFeatures = valueFeaturesRaw.filter(Boolean) as Array<{
                  feature: any;
                  value: number;
                  bbox: [number, number, number, number] | null;
                  centroid: any;
                }>;
                if (!valueFeatures.length) return;

                for (let i = 0; i < adminIdx.length; i += 1) {
                  if (cancelledRef.current) return;
                  const rowIdx = adminIdx[i];
                  const adminGeometryRaw = adminGeometryField
                    ? parseGeoJsonLike(admin.getValue(adminGeometryField, rowIdx))
                    : h3CellToPolygonFeature(admin.getValue(String(adminH3Field || ''), rowIdx));
                  const adminFeature = toTurfPolygonFeature(adminGeometryRaw);
                  if (!adminFeature) {
                    if (i > 0 && i % loopYieldEvery === 0) {
                      await yieldToMainThread();
                    }
                    continue;
                  }
                  const adminBbox = geometryToBbox((adminFeature as any)?.geometry);
                  const candidates = adminBbox
                    ? valueFeatures.filter(item => !item.bbox || geometryBboxOverlap(adminBbox, item.bbox))
                    : valueFeatures;
                  let sum = 0;
                  let denom = 0;
                  let min: number | null = null;
                  let max: number | null = null;
                  let count = 0;

                  for (let j = 0; j < candidates.length; j += 1) {
                    const item = candidates[j];
                    let matched = false;
                    let weight = 1;
                    try {
                      if (weightMode === 'centroid') {
                        matched = turfBooleanPointInPolygon(item.centroid, adminFeature);
                      } else if (weightMode === 'intersects') {
                        matched = turfBooleanIntersects(item.feature, adminFeature);
                      } else if (isPolygonLikeFeature(item.feature)) {
                        const inter = turfIntersectSafe(adminFeature, item.feature);
                        if (inter) {
                          const interArea = turfArea(inter as any);
                          const total = Math.max(1e-12, turfArea(item.feature as any));
                          weight = Math.max(0, interArea / total);
                          matched = weight > 0;
                        }
                      } else {
                        matched = turfBooleanPointInPolygon(item.centroid, adminFeature);
                      }
                    } catch {
                      matched = false;
                    }
                    if (!matched) continue;
                    count += 1;
                    if (valueField && Number.isFinite(item.value)) {
                      const v = Number(item.value);
                      const weighted = v * Math.max(0, weight);
                      sum += weighted;
                      denom += Math.max(0, weight);
                      min = min === null ? v : Math.min(min, v);
                      max = max === null ? v : Math.max(max, v);
                    }
                    if (j > 0 && j % loopYieldEvery === 0) {
                      await yieldToMainThread();
                      if (cancelledRef.current) return;
                    }
                  }

                  const base: Record<string, unknown> = {};
                  (admin.fields || []).forEach((f: any) => {
                    base[f.name] = admin.getValue(f.name, rowIdx);
                  });
                  if (aggregation === 'count' || !valueField) {
                    base[outputFieldName] = count;
                  } else if (aggregation === 'sum') {
                    base[outputFieldName] = sum;
                  } else if (aggregation === 'avg') {
                    base[outputFieldName] = denom > 0 ? sum / denom : null;
                  } else if (aggregation === 'min') {
                    base[outputFieldName] = min;
                  } else if (aggregation === 'max') {
                    base[outputFieldName] = max;
                  }
                  rowsOut.push(base);

                  if (i > 0 && i % loopYieldEvery === 0) {
                    await yieldToMainThread();
                  }
                }
              };

              let workerApplied = false;
              if (typeof Worker !== 'undefined') {
                try {
                  const adminRowsPayload = await mapIndexesChunked(
                    adminIdx,
                    (rowIdx: number) =>
                      adminGeometryField
                        ? {rowIdx, geometry: admin.getValue(adminGeometryField, rowIdx)}
                        : {rowIdx, h3Id: admin.getValue(String(adminH3Field || ''), rowIdx)},
                    Math.max(50, Math.min(200, QMAP_DEFAULT_CHUNK_SIZE))
                  );
                  if (cancelledRef.current) return;
                  const valueRowsPayload = await mapIndexesChunked(
                    valueIdx,
                    (rowIdx: number) => ({
                      rowIdx,
                      ...(valueGeometryField
                        ? {geometry: values.getValue(valueGeometryField, rowIdx)}
                        : {h3Id: values.getValue(String(valueH3Field || ''), rowIdx)}),
                      value: valueField ? Number(values.getValue(valueField, rowIdx)) : null
                    }),
                    Math.max(50, Math.min(200, QMAP_DEFAULT_CHUNK_SIZE))
                  );
                  if (cancelledRef.current) return;

                  const adaptiveTimeout = Math.min(900000, Math.max(180000, 60000 + pairEstimate * 0.03));
                  const workerResult = (await runWithAbortSignal(signal =>
                    runZonalStatsJob({
                      payload: {
                        weightMode,
                        includeValue: Boolean(valueField),
                        adminRows: adminRowsPayload,
                        valueRows: valueRowsPayload
                      },
                      timeoutMs: adaptiveTimeout,
                      signal
                    })
                  )) as any;
                  if (cancelledRef.current) return;

                  const statsByRow = new Map<
                    number,
                    {count: number; sum: number; denom: number; min: number | null; max: number | null}
                  >();
                  (workerResult?.statsByRow || []).forEach((stat: any) => {
                    const rowIdx = Number(stat?.rowIdx);
                    if (!Number.isFinite(rowIdx)) return;
                    statsByRow.set(rowIdx, {
                      count: Number(stat?.count || 0),
                      sum: Number(stat?.sum || 0),
                      denom: Number(stat?.denom || 0),
                      min: stat?.min === null || stat?.min === undefined ? null : Number(stat.min),
                      max: stat?.max === null || stat?.max === undefined ? null : Number(stat.max)
                    });
                  });

                  for (let i = 0; i < adminIdx.length; i += 1) {
                    if (cancelledRef.current) return;
                    const rowIdx = adminIdx[i];
                    const stat = statsByRow.get(rowIdx) || null;
                    const base: Record<string, unknown> = {};
                    (admin.fields || []).forEach((f: any) => {
                      base[f.name] = admin.getValue(f.name, rowIdx);
                    });
                    if (aggregation === 'count' || !valueField) {
                      base[outputFieldName] = stat ? stat.count : 0;
                    } else if (aggregation === 'sum') {
                      base[outputFieldName] = stat ? stat.sum : 0;
                    } else if (aggregation === 'avg') {
                      base[outputFieldName] = stat && stat.denom > 0 ? stat.sum / stat.denom : null;
                    } else if (aggregation === 'min') {
                      base[outputFieldName] = stat ? stat.min : null;
                    } else if (aggregation === 'max') {
                      base[outputFieldName] = stat ? stat.max : null;
                    }
                    rowsOut.push(base);
                    if (i > 0 && i % loopYieldEvery === 0) {
                      await yieldToMainThread();
                    }
                  }
                  workerApplied = true;
                } catch (error) {
                  if ((error as Error)?.name === 'AbortError' && cancelledRef.current) return;
                  console.error('zonalStatsByAdmin worker failed; fallback to local path:', error);
                }
              }

              if (!workerApplied) {
                if (pairEstimate > localFallbackBudget) {
                  console.error(
                    `[qmap-ai] zonal local fallback aborted to avoid UI freeze (pairEstimate=${pairEstimate}, threshold=${localFallbackBudget}).`
                  );
                  return;
                }
                await runLocalGeometryFallback();
              }
            }
            if (cancelledRef.current) return;
            if (!rowsOut.length) return;
            upsertDerivedDatasetRows(localDispatch, datasets, newDatasetName, rowsOut, 'qmap_zonal_stats', showOnMap);
            if (showOnMap && QMAP_AUTO_HIDE_SOURCE_LAYERS) {
              hideLayersForDatasetIds(localDispatch, localVisState?.layers || [], [admin.id, values.id]);
            }
          } finally {
            if (useLoadingIndicator) {
              localDispatch(wrapTo('map', setLoadingIndicator({change: -1}) as any));
            }
          }
        })();
      }, [
        localDispatch,
        localVisState,
        executionKey,
        adminDatasetId,
        valueDatasetId,
        adminGeometryField,
        valueGeometryField,
        adminH3Field,
        valueH3Field,
        valueField,
        aggregation,
        weightMode,
        executionMode,
        useActiveFilters,
        maxAdminFeatures,
        maxValueFeatures,
        outputFieldName,
        showOnMap,
        newDatasetName,
        shouldSkip,
        complete
      ]);
      return null;
    }
  });

}
