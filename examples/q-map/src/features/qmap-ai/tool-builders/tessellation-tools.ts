import React, {useEffect} from 'react';
import {useDispatch, useSelector} from 'react-redux';
import {z} from 'zod';

import {selectQMapVisState} from '../../../state/qmap-selectors';
import type {H3AggregateRow} from '../../../workers/h3-aggregate-core';
import {useToolExecution} from './use-tool-execution';

import type {QMapToolContext} from '../context/tool-context';

export function createTassellateSelectedGeometryTool(ctx: QMapToolContext) {
  const {
    DEFAULT_TASSELLATION_DATASET,
    getCurrentVisState,
    getPolygonsFromGeometry,
    getTassellationDatasetInfo,
    makeExecutionKey,
    EXECUTED_TOOL_COMPONENT_KEYS,
    rememberExecutedToolComponentKey,
    getIntersectingH3Ids,
    upsertTassellationDataset
  } = ctx;

  return {
    description:
      'Tessellate currently selected geometry into H3 cells (intersection-based) and upsert dataset Tassellation.',
    parameters: z.object({
      resolution: z.number().min(4).max(11),
      targetDatasetName: z.string().optional(),
      appendToExisting: z.boolean().optional().describe('Default false: replace target dataset content')
    }),
    execute: async ({resolution, targetDatasetName, appendToExisting}: any) => {
      const selectedGeometry = getCurrentVisState()?.editor?.selectedFeature?.geometry;
      const polygons = getPolygonsFromGeometry(selectedGeometry);
      if (!polygons.length) {
        return {
          llmResult: {
            success: false,
            details: 'No selected Polygon/MultiPolygon geometry found.'
          }
        };
      }
      const {label: resolvedTargetLabel, datasetId: resolvedTargetDatasetId} = getTassellationDatasetInfo(
        String(targetDatasetName || DEFAULT_TASSELLATION_DATASET),
        getCurrentVisState()?.datasets || {}
      );
      return {
        llmResult: {
          success: true,
          dataset: resolvedTargetLabel,
          datasetId: resolvedTargetDatasetId,
          details: `Tessellating selected geometry at H3 resolution ${resolution}.`
        },
        additionalData: {
          executionKey: makeExecutionKey('tassellate-selected-geometry'),
          resolution,
          targetDatasetName: resolvedTargetLabel,
          targetDatasetId: resolvedTargetDatasetId,
          appendToExisting: appendToExisting === true
        }
      };
    },
    component: function TassellateSelectedGeometryComponent({
      executionKey,
      resolution,
      targetDatasetName,
      targetDatasetId,
      appendToExisting
    }: {
      executionKey?: string;
      resolution: number;
      targetDatasetName: string;
      targetDatasetId: string;
      appendToExisting: boolean;
    }) {
      const localDispatch = useDispatch<any>();
      const localVisState = useSelector(selectQMapVisState);
      const {shouldSkip, complete} = useToolExecution({executionKey, executedToolComponentKeys: EXECUTED_TOOL_COMPONENT_KEYS, rememberExecutedToolComponentKey});
      useEffect(() => {
        if (shouldSkip()) return;
        const geometry = localVisState?.editor?.selectedFeature?.geometry;
        const polygons = getPolygonsFromGeometry(geometry);
        if (!polygons.length) return;
        const ids = getIntersectingH3Ids(polygons, resolution);
        if (!ids.length) return;
        complete();
        upsertTassellationDataset(
          localDispatch,
          localVisState?.datasets,
          ids,
          resolution,
          targetDatasetName,
          appendToExisting
        );
      }, [localDispatch, localVisState, executionKey, resolution, targetDatasetName, targetDatasetId, appendToExisting, shouldSkip, complete]);
      return null;
    }
  };

}

export function createTassellateDatasetLayerTool(ctx: QMapToolContext) {
  const {
    DEFAULT_TASSELLATION_DATASET,
    getCurrentVisState,
    resolveDatasetByName,
    getDatasetIndexes,
    isLikelyLandCoverDataset,
    resolveLandCoverGroupByFields,
    resolveOptionalFeatureCap,
    filterTargetsDataset,
    getTassellationDatasetInfo,
    getDatasetInfoByLabel,
    makeExecutionKey,
    EXECUTED_TOOL_COMPONENT_KEYS,
    rememberExecutedToolComponentKey,
    shouldUseLoadingIndicator,
    wrapTo,
    setLoadingIndicator,
    filterIndexesChunked,
    toComparable,
    resolveDatasetFieldName,
    upsertIntermediateDataset,
    mapIndexesChunked,
    runH3Job,
    extractPolygonsFromGeoJsonLike,
    getIntersectingH3Ids,
    upsertTassellationDataset
  } = ctx;

  return {
    description:
      'Tessellate geometries from a dataset/layer into H3 cells (intersection-based) and upsert dataset Tassellation.',
    parameters: z.object({
      datasetName: z.string().describe('Exact dataset name from listQMapDatasets'),
      resolution: z.number().min(4).max(11),
      targetDatasetName: z.string().optional(),
      maxFeatures: z
        .number()
        .optional()
        .describe('Optional explicit cap on features to tessellate. Unset = full matched coverage (no truncation).'),
      useActiveFilters: z.boolean().optional().describe('Default true: apply current dataset UI filters first'),
      appendToExisting: z.boolean().optional().describe('Default false: replace target dataset content'),
      showOnMap: z
        .boolean()
        .optional()
        .describe('Default true: auto-create layer for tessellation dataset'),
      materializeFilteredDataset: z
        .boolean()
        .optional()
        .describe('Default true with active filters: create intermediate filtered dataset before tessellation')
    }),
    execute: async ({
      datasetName,
      resolution,
      targetDatasetName,
      maxFeatures,
      useActiveFilters,
      appendToExisting,
      showOnMap,
      materializeFilteredDataset
    }: any) => {
      const sourceDataset = resolveDatasetByName(getCurrentVisState()?.datasets || {}, datasetName);
      if (!sourceDataset?.id) {
        return {
          llmResult: {
            success: false,
            details: `Dataset "${datasetName}" not found.`
          }
        };
      }
      const sourceRowCount = getDatasetIndexes(sourceDataset).length;
      const likelyLandCover = isLikelyLandCoverDataset(sourceDataset);
      const isLargeThematicCoverageDataset = likelyLandCover && sourceRowCount > 4000;
      if (isLargeThematicCoverageDataset) {
        const suggestedWeightMode: 'intersects' | 'area_weighted' =
          sourceRowCount > 12000 ? 'intersects' : 'area_weighted';
        const suggestedGroupByFields = resolveLandCoverGroupByFields(sourceDataset);
        const suggestedMaxFeatures =
          Number.isFinite(Number(maxFeatures)) && Number(maxFeatures) > 0
            ? Math.max(1, Math.floor(Number(maxFeatures)))
            : undefined;
        return {
          llmResult: {
            success: false,
            retryWithTool: 'aggregateDatasetToH3',
            retryWithArgs: {
              datasetName: sourceDataset.label || sourceDataset.id,
              resolution,
              operations: ['count'],
              groupByFields: suggestedGroupByFields.length ? suggestedGroupByFields : undefined,
              weightMode: suggestedWeightMode,
              targetDatasetName: targetDatasetName || undefined,
              maxFeatures: suggestedMaxFeatures,
              useActiveFilters: useActiveFilters !== false,
              showOnMap: showOnMap !== false
            },
            retryReason: 'thematic-coverage-large-dataset',
            details:
              `Dataset "${sourceDataset.label || sourceDataset.id}" looks like thematic land-cover with ${sourceRowCount} rows. ` +
              'Direct tessellation is heavy and often times out. Auto-routing to aggregateDatasetToH3 with class grouping is recommended.'
          }
        };
      }
      const geometryField =
        (sourceDataset.fields || []).find((f: any) => f?.type === 'geojson')?.name || '_geojson';
      const cap = resolveOptionalFeatureCap(maxFeatures);
      const explicitCap =
        Number.isFinite(Number(maxFeatures)) && Number(maxFeatures) > 0 ? Math.max(1, Math.floor(Number(maxFeatures))) : null;
      const applyFilters = useActiveFilters !== false;
      const activeFilters = applyFilters
        ? (getCurrentVisState()?.filters || []).filter((f: any) => filterTargetsDataset(f, sourceDataset.id))
        : [];
      const createIntermediate = materializeFilteredDataset ?? applyFilters;
      const intermediateName = `${sourceDataset.label || sourceDataset.id}_filtered_for_tassellation`;
      const shouldShowOnMap = showOnMap !== false;
      const {label: resolvedTargetLabel, datasetId: resolvedTargetDatasetId} = getTassellationDatasetInfo(
        String(targetDatasetName || DEFAULT_TASSELLATION_DATASET),
        getCurrentVisState()?.datasets || {}
      );
      const {label: resolvedIntermediateLabel, datasetId: resolvedIntermediateDatasetId} = getDatasetInfoByLabel(
        getCurrentVisState()?.datasets || {},
        intermediateName,
        'qmap_intermediate'
      );

      return {
        llmResult: {
          success: true,
          details: `Tessellating dataset "${sourceDataset.label || sourceDataset.id}" using field "${geometryField}" at H3 resolution ${resolution} (${explicitCap ? `cap=${explicitCap}` : 'full matched coverage'}${applyFilters ? `, filters enabled (${activeFilters.length})` : ''}${createIntermediate ? `, intermediate dataset "${intermediateName}" added (no layer)` : ''}). Output dataset: "${resolvedTargetLabel}" (id: ${resolvedTargetDatasetId})${shouldShowOnMap ? '' : ' (no auto layer)'}.`,
          dataset: resolvedTargetLabel,
          datasetId: resolvedTargetDatasetId,
          intermediateDataset: createIntermediate ? resolvedIntermediateLabel : null,
          intermediateDatasetId: createIntermediate ? resolvedIntermediateDatasetId : null
        },
        additionalData: {
          executionKey: makeExecutionKey('tassellate-dataset-layer'),
          datasetId: sourceDataset.id,
          geometryField,
          resolution,
          targetDatasetName: resolvedTargetLabel,
          targetDatasetId: resolvedTargetDatasetId,
          maxFeatures: cap,
          useActiveFilters: applyFilters,
          appendToExisting: appendToExisting === true,
          showOnMap: shouldShowOnMap,
          materializeFilteredDataset: createIntermediate,
          intermediateDatasetName: resolvedIntermediateLabel,
          intermediateDatasetId: resolvedIntermediateDatasetId
        }
      };
    },
    component: function TassellateDatasetLayerComponent({
      executionKey,
      datasetId,
      geometryField,
      resolution,
      targetDatasetName,
      maxFeatures,
      useActiveFilters,
      appendToExisting,
      showOnMap,
      materializeFilteredDataset,
      intermediateDatasetName
    }: {
      executionKey?: string;
      datasetId: string;
      geometryField: string;
      resolution: number;
      targetDatasetName: string;
      maxFeatures: number;
      useActiveFilters: boolean;
      appendToExisting: boolean;
      showOnMap: boolean;
      materializeFilteredDataset: boolean;
      intermediateDatasetName: string;
    }) {
      const localDispatch = useDispatch<any>();
      const localVisState = useSelector(selectQMapVisState);
      const cancelledRef = React.useRef(false);
      const {shouldSkip, complete} = useToolExecution({executionKey, executedToolComponentKeys: EXECUTED_TOOL_COMPONENT_KEYS, rememberExecutedToolComponentKey});
      useEffect(() => {
        return () => {
          cancelledRef.current = true;
        };
      }, []);
      useEffect(() => {
        if (shouldSkip()) return;
        const sourceDataset = localVisState?.datasets?.[datasetId];
        if (!sourceDataset) return;
        // Mark as started before any dispatch/state mutation to avoid effect re-entry loops.
        complete();
        const useLoadingIndicator = shouldUseLoadingIndicator();
        if (useLoadingIndicator) {
          localDispatch(wrapTo('map', setLoadingIndicator({change: 1}) as any));
        }
        (async () => {
          let cappedIdxForFallback: number[] = [];
          try {
            const baseIdx = Array.isArray(sourceDataset.allIndexes)
              ? sourceDataset.allIndexes
              : Array.from({length: Number(sourceDataset.length || 0)}, (_, i) => i);

            const filters = useActiveFilters
              ? (localVisState?.filters || []).filter((f: any) => filterTargetsDataset(f, sourceDataset.id))
              : [];

            let matchedIdx = await filterIndexesChunked(baseIdx, (rowIdx: number) => {
              return filters.every((filter: any) => {
                const rawFieldName = Array.isArray(filter?.name) ? filter.name[0] : filter?.name;
                if (!rawFieldName) return true;
                const resolvedFilterField = resolveDatasetFieldName(sourceDataset, String(rawFieldName));
                if (!resolvedFilterField) return true;
                const rowValue = sourceDataset.getValue(resolvedFilterField, rowIdx);
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
            if (cancelledRef.current) return;
            if (useActiveFilters && matchedIdx.length === 0 && baseIdx.length > 0) {
              // Avoid empty tessellation due to stale/overly-restrictive filters.
              matchedIdx = baseIdx.slice();
            }

            const cappedIdx = matchedIdx.slice(0, resolveOptionalFeatureCap(maxFeatures));
            cappedIdxForFallback = cappedIdx;

            if (materializeFilteredDataset && cappedIdx.length > 0) {
              await upsertIntermediateDataset(
                localDispatch,
                localVisState?.datasets,
                sourceDataset,
                cappedIdx,
                intermediateDatasetName
              );
            }

            const rawGeometries = await mapIndexesChunked(
              cappedIdx,
              (rowIdx: number) => sourceDataset.getValue(geometryField, rowIdx)
            );
            if (cancelledRef.current) return;

            const h3TimeoutMs =
              typeof window !== 'undefined' && (window as any).__QMAP_E2E_TOOLS__ ? 15000 : 120000;
            const result = await runH3Job({
              name: 'tessellateGeometries',
              payload: {
                resolution,
                geometries: rawGeometries
              },
              timeoutMs: h3TimeoutMs
            });
            if (cancelledRef.current) return;
            let ids = Array.isArray(result?.ids) ? result.ids : [];
            if (!ids.length) {
              const fallbackSet = new Set<string>();
              rawGeometries.forEach((rawGeometry: unknown) => {
                const polygons = extractPolygonsFromGeoJsonLike(rawGeometry);
                if (!polygons.length) return;
                const rowIds = getIntersectingH3Ids(polygons, resolution);
                rowIds.forEach((id: string) => fallbackSet.add(id));
              });
              ids = Array.from(fallbackSet);
            }
            const existingTarget = resolveDatasetByName(localVisState?.datasets || {}, targetDatasetName);
            if (!ids.length) {
              if (!existingTarget?.id) {
                upsertTassellationDataset(
                  localDispatch,
                  localVisState?.datasets,
                  [],
                  resolution,
                  targetDatasetName,
                  appendToExisting,
                  showOnMap
                );
              }
              return;
            }
            upsertTassellationDataset(
              localDispatch,
              localVisState?.datasets,
              ids,
              resolution,
              targetDatasetName,
              appendToExisting,
              showOnMap
            );
          } catch (error) {
            if (cancelledRef.current) return;
            if ((error as Error)?.name === 'AbortError') return;
            // Worker fallback: run tessellation locally to avoid failed tool execution.
            console.error('TassellateDatasetLayer worker failed; fallback to local path:', error);
            const ids = new Set<string>();
            const rawGeometries = await mapIndexesChunked(
              cappedIdxForFallback,
              (rowIdx: number) => sourceDataset.getValue(geometryField, rowIdx)
            );
            rawGeometries.forEach((rawGeometry: unknown) => {
              const polygons = extractPolygonsFromGeoJsonLike(rawGeometry);
              if (!polygons.length) return;
              const rowIds = getIntersectingH3Ids(polygons, resolution);
              rowIds.forEach((id: string) => ids.add(id));
            });
            if (cancelledRef.current) return;
            if (!ids.size) {
              const existingTarget = resolveDatasetByName(localVisState?.datasets || {}, targetDatasetName);
              if (!existingTarget?.id) {
                upsertTassellationDataset(
                  localDispatch,
                  localVisState?.datasets,
                  [],
                  resolution,
                  targetDatasetName,
                  appendToExisting,
                  showOnMap
                );
              }
              return;
            }
            upsertTassellationDataset(
              localDispatch,
              localVisState?.datasets,
              Array.from(ids),
              resolution,
              targetDatasetName,
              appendToExisting,
              showOnMap
            );
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
        datasetId,
        geometryField,
        resolution,
        targetDatasetName,
        maxFeatures,
        useActiveFilters,
        appendToExisting,
        showOnMap,
        materializeFilteredDataset,
        intermediateDatasetName,
        shouldSkip,
        complete
      ]);
      return null;
    }
  };

}
