import {useEffect} from 'react';
import {layerConfigChange, wrapTo} from '@kepler.gl/actions';
import {useDispatch, useSelector} from 'react-redux';
import {z} from 'zod';

import {selectQMapDatasets, selectQMapLayers} from '../../../state/qmap-selectors';
import type {QMapToolContext} from '../context/tool-context';
import {useToolExecution} from './use-tool-execution';

export function createApplyQMapStylePresetTool(ctx: QMapToolContext) {
  const {
    getCurrentVisState,
    getQMapStylePreset,
    resolveStyleTargetLayer,
    isLevelLikeField,
    isPopulationLikeField,
    getNamedPalette,
    getNumericExtent,
    makeExecutionKey,
    findDatasetForLayer,
    resolveDatasetFieldName,
    resolveDatasetByName,
    ensureColorRange,
    executedToolComponentKeys,
    rememberExecutedToolComponentKey
  } = ctx;
  return {
    description:
      'Apply a predefined style preset to a target layer/dataset. Useful for consistent choropleth styling.',
    parameters: z.object({
      presetName: z.enum(['comuni_population']).describe('Preset id'),
      datasetName: z.string().optional().describe('Exact dataset name from listQMapDatasets'),
      layerName: z.string().optional().describe('Exact layer name or id')
    }),
    execute: async ({presetName, datasetName, layerName}: any) => {
      const currentVisState = getCurrentVisState();
      const datasets = Object.values(currentVisState?.datasets || {}) as any[];
      const layers = (currentVisState?.layers || []) as any[];
      const preset = getQMapStylePreset(presetName);
      if (!preset) {
        return {
          llmResult: {
            success: false,
            details: `Preset "${presetName}" not found.`
          }
        };
      }

      const findLayerByNameOrId = (nameOrId?: string) => {
        const needle = String(nameOrId || '').toLowerCase();
        if (!needle) return null;
        return (
          layers.find((l: any) => {
            const label = String(l?.config?.label || '').toLowerCase();
            const id = String(l?.id || '').toLowerCase();
            return label === needle || id === needle;
          }) || null
        );
      };

      let targetLayer = findLayerByNameOrId(layerName) as any;
      let targetDataset = null as any;

      if (!targetLayer && datasetName) {
        targetDataset = resolveDatasetByName(currentVisState?.datasets || {}, datasetName);
        if (targetDataset?.id) {
          const resolved = resolveStyleTargetLayer(layers, targetDataset, undefined);
          targetLayer = resolved.layer;
          if (!targetLayer && resolved.details) {
            return {
              llmResult: {
                success: false,
                details: resolved.details
              }
            };
          }
        }
      }

      if (!targetLayer && layers.length === 1) {
        targetLayer = layers[0];
      }

      if (!targetLayer?.id) {
        return {
          llmResult: {
            success: false,
            details:
              'Target layer not found. Provide layerName or datasetName from listQMapDatasets, or keep a single layer visible.'
          }
        };
      }

      targetDataset =
        targetDataset ||
        datasets.find((d: any) => String(d?.id || '') === String(targetLayer?.config?.dataId || '')) ||
        null;

      const getLevelStats = (dataset: any): {fieldName: string | null; lv9Count: number; sampled: number} => {
        const fields = (dataset?.fields || []).map((f: any) => String(f?.name || '')).filter(Boolean);
        const levelField = fields.find((name: string) => isLevelLikeField(name)) || null;
        if (!levelField) return {fieldName: null, lv9Count: 0, sampled: 0};
        const rowIndexes = Array.isArray(dataset?.allIndexes)
          ? dataset.allIndexes
          : Array.from({length: Number(dataset?.length || 0)}, (_, i) => i);
        const capped = rowIndexes.slice(0, 50000);
        let lv9Count = 0;
        capped.forEach((rowIdx: number) => {
          const raw = dataset.getValue(levelField, rowIdx);
          if (Number(raw) === 9 || String(raw).trim() === '9') {
            lv9Count += 1;
          }
        });
        return {fieldName: levelField, lv9Count, sampled: capped.length};
      };

      if (presetName === 'comuni_population') {
        const currentStats = getLevelStats(targetDataset);
        if (currentStats.fieldName && currentStats.lv9Count <= 0) {
          const candidates = datasets
            .map((d: any) => {
              const stats = getLevelStats(d);
              const popField = (d?.fields || []).find((f: any) =>
                isPopulationLikeField(String(f?.name || ''))
              );
              return {dataset: d, stats, hasPopulation: Boolean(popField)};
            })
            .filter((c: any) => c.hasPopulation && c.stats.lv9Count > 0)
            .sort((a: any, b: any) => b.stats.lv9Count - a.stats.lv9Count);
          const picked = candidates[0];
          if (picked?.dataset?.id) {
            targetDataset = picked.dataset;
            const resolved = resolveStyleTargetLayer(layers, targetDataset, undefined);
            targetLayer = resolved.layer;
            if (!targetLayer?.id) {
              return {
                llmResult: {
                  success: false,
                  details:
                    `Preset "${presetName}" requires municipalities (lv=9), but target layer for dataset ` +
                    `"${targetDataset?.label || targetDataset?.id}" is ambiguous. Provide layerName explicitly.`
                }
              };
            }
          } else {
            return {
              llmResult: {
                success: false,
                details:
                  `Preset "${presetName}" requires a municipalities dataset (lv=9). ` +
                  `Current target "${targetDataset?.label || targetDataset?.id}" has 0 lv=9 rows.`
              }
            };
          }
        }
      }

      const field = (targetDataset?.fields || []).find(
        (f: any) => String(f?.name || '').toLowerCase() === String(preset.colorField).toLowerCase()
      );
      if (!field?.name) {
        return {
          llmResult: {
            success: false,
            details: `Field "${preset.colorField}" not found in target dataset "${targetDataset?.label || targetDataset?.id || ''}".`
          }
        };
      }

      const paletteColors = getNamedPalette(preset.palette).slice(0, Math.max(3, Math.min(12, preset.classes)));
      const colorRange = {
        name: `qmap.preset.${presetName}`,
        type: 'custom',
        category: 'Custom',
        colors: paletteColors
      };
      const colorDomain = getNumericExtent(targetDataset, String(field.name), 50000);

      return {
        llmResult: {
          success: true,
          details: `Applying preset "${presetName}" on layer "${targetLayer.config?.label || targetLayer.id}".`
        },
        additionalData: {
          executionKey: makeExecutionKey('style-preset'),
          layerId: targetLayer.id,
          fieldName: String(field.name),
          colorRange,
          colorDomain,
          scaleType: preset.mode,
          applyToStroke: preset.applyToStroke,
          fillOpacity: preset.fillOpacity,
          strokeOpacity: preset.strokeOpacity,
          isolateLayer: preset.isolateLayer
        }
      };
    },
    component: function ApplyQMapStylePresetComponent({
      executionKey,
      layerId,
      fieldName,
      colorRange,
      colorDomain,
      scaleType,
      applyToStroke,
      fillOpacity,
      strokeOpacity,
      isolateLayer
    }: {
      executionKey?: string;
      layerId: string;
      fieldName: string;
      colorRange: any;
      colorDomain?: [number, number] | null;
      scaleType: 'linear' | 'quantize' | 'quantile';
      applyToStroke: boolean;
      fillOpacity: number;
      strokeOpacity: number;
      isolateLayer: boolean;
    }) {
      const localDispatch = useDispatch<any>();
      const localLayers = useSelector(selectQMapLayers) as any[];
      const localDatasets = useSelector(selectQMapDatasets) as Record<string, any>;
      const {shouldSkip, complete} = useToolExecution({executionKey, executedToolComponentKeys, rememberExecutedToolComponentKey});
      useEffect(() => {
        if (shouldSkip()) return;
        const layer = (localLayers || []).find((l: any) => String(l?.id || '') === String(layerId));
        if (!layer) return;
        const layerDataset = findDatasetForLayer(localDatasets, layer);
        const resolvedFieldName = resolveDatasetFieldName(layerDataset, fieldName);
        const fieldObj =
          resolvedFieldName && Array.isArray(layerDataset?.fields)
            ? layerDataset.fields.find((f: any) => String(f?.name || '') === String(resolvedFieldName))
            : null;
        if (!fieldObj) return;

        complete();

        if (isolateLayer) {
          (localLayers || []).forEach((currentLayer: any) => {
            const isVisible = String(currentLayer?.id || '') === String(layerId);
            localDispatch(wrapTo('map', layerConfigChange(currentLayer, {isVisible})));
          });
        }

        const safeRange = ensureColorRange(colorRange);
        const nextVisConfig: any = {
          ...(layer.config?.visConfig || {}),
          colorRange: safeRange,
          ...(applyToStroke ? {strokeColorRange: safeRange} : {}),
          opacity: Math.max(0, Math.min(1, Number(fillOpacity))),
          strokeOpacity: Math.max(0, Math.min(1, Number(strokeOpacity)))
        };
        const nextConfig: any = {
          colorField: fieldObj,
          colorScale: scaleType,
          ...(Array.isArray(colorDomain) &&
          colorDomain.length === 2 &&
          Number.isFinite(Number(colorDomain[0])) &&
          Number.isFinite(Number(colorDomain[1]))
            ? {colorDomain: [Number(colorDomain[0]), Number(colorDomain[1])]}
            : {}),
          visConfig: nextVisConfig
        };
        if (applyToStroke) {
          nextConfig.strokeColorField = fieldObj;
          nextConfig.strokeColorScale = scaleType;
          if (Array.isArray(colorDomain) && colorDomain.length === 2) {
            nextConfig.strokeColorDomain = [Number(colorDomain[0]), Number(colorDomain[1])];
          }
        } else {
          nextConfig.strokeColorField = null;
        }
        localDispatch(wrapTo('map', layerConfigChange(layer, nextConfig)));
      }, [
        localDispatch,
        localLayers,
        localDatasets,
        executionKey,
        layerId,
        fieldName,
        colorRange,
        colorDomain,
        scaleType,
        applyToStroke,
        fillOpacity,
        strokeOpacity,
        isolateLayer,
        shouldSkip,
        complete
      ]);
      return null;
    }
  };
}

export function createSetQMapLayerColorByThresholdsTool(ctx: QMapToolContext) {
  const {
    getCurrentVisState,
    resolveDatasetByName,
    resolveDatasetFieldName,
    isAreaLikeFieldName,
    resolveAreaLikeFieldName,
    normalizeThresholds,
    resolveStyleTargetLayer,
    getNamedPalette,
    makeExecutionKey,
    ensureColorRange,
    findDatasetForLayer,
    paletteSchema,
    executedToolComponentKeys,
    rememberExecutedToolComponentKey
  } = ctx;
  return {
    description:
      'Color an existing layer by numeric field using explicit manual thresholds (q-hive custom threshold scale).',
    parameters: z.object({
      datasetName: z.string().describe('Exact dataset name from listQMapDatasets'),
      fieldName: z.string().describe('Numeric field to color by'),
      thresholds: z.array(z.number()).min(1).describe('Sorted threshold values, e.g. [10, 20, 50]'),
      layerName: z.string().optional().describe('Optional layer name to target'),
      palette: paletteSchema.describe(
        'Named palette (redGreen, greenRed, blueRed, viridis, magma, yellowRed, yellowBlue).'
      ),
      reverse: z.boolean().optional(),
      applyToStroke: z.boolean().optional()
    }),
    execute: async ({datasetName, fieldName, thresholds, layerName, palette, reverse, applyToStroke}: any) => {
      const currentVisState = getCurrentVisState();
      const layers = (currentVisState?.layers || []) as any[];
      const dataset = resolveDatasetByName(currentVisState?.datasets || {}, datasetName);

      if (!dataset?.id) {
        return {
          llmResult: {
            success: false,
            details: `Dataset "${datasetName}" not found. Call listQMapDatasets first.`
          }
        };
      }

      const requestedFieldName = String(fieldName || '');
      const resolvedFieldName =
        resolveDatasetFieldName(dataset, requestedFieldName) ||
        (isAreaLikeFieldName(requestedFieldName) ? resolveAreaLikeFieldName(dataset) : null);
      const field = (dataset.fields || []).find(
        (f: any) => String(f?.name || '') === String(resolvedFieldName || '')
      );
      if (!field?.name) {
        if (isAreaLikeFieldName(requestedFieldName)) {
          return {
            llmResult: {
              success: false,
              details:
                `Field "${fieldName}" not found in dataset "${datasetName}". ` +
                'Per soglie statistiche sull area, crea prima un dataset derivato con createDatasetWithGeometryArea, poi usa setQMapLayerColorByStatsThresholds sul nuovo campo area.'
            }
          };
        }
        return {
          llmResult: {
            success: false,
            details: `Field "${fieldName}" not found in dataset "${datasetName}".`
          }
        };
      }

      const normalized = normalizeThresholds(thresholds);
      if (!normalized || !normalized.length) {
        return {
          llmResult: {
            success: false,
            details: 'Invalid thresholds. Provide numeric ascending values (at least one).'
          }
        };
      }

      const target = resolveStyleTargetLayer(layers, dataset, layerName);
      const layer = target.layer;

      if (!layer?.id) {
        return {
          llmResult: {
            success: false,
            details: target.details || `No layer found for dataset "${datasetName}".`
          }
        };
      }

      const classCount = normalized.length + 1;
      const paletteColors = getNamedPalette(String(palette || 'redGreen'));
      const colors =
        reverse
          ? [...paletteColors.slice(0, classCount)].reverse()
          : paletteColors.slice(0, classCount);
      const colorRange = {
        name: 'qmap.thresholdPalette',
        type: 'custom',
        category: 'Custom',
        colors
      };

      return {
        llmResult: {
          success: true,
          details: `Applying custom-threshold color scale on layer "${layer.config?.label || layer.id}" using field "${field.name}".`
        },
        additionalData: {
          executionKey: makeExecutionKey('color-thresholds'),
          layerId: layer.id,
          fieldName: String(field.name),
          colorRange,
          scaleType: 'custom',
          manualDomain: normalized,
          applyToStroke: applyToStroke !== false
        }
      };
    },
    component: function SetQMapLayerColorByThresholdsComponent({
      executionKey,
      layerId,
      fieldName,
      colorRange,
      manualDomain,
      applyToStroke
    }: {
      executionKey?: string;
      layerId: string;
      fieldName: string;
      colorRange: any;
      manualDomain: number[] | null;
      applyToStroke: boolean;
    }) {
      const localDispatch = useDispatch<any>();
      const localLayers = useSelector(selectQMapLayers) as any[];
      const localDatasets = useSelector(selectQMapDatasets) as Record<string, any>;
      const {shouldSkip, abort, complete} = useToolExecution({executionKey, executedToolComponentKeys, rememberExecutedToolComponentKey});
      useEffect(() => {
        if (shouldSkip()) return;
        const layer = (localLayers || []).find((l: any) => String(l?.id || '') === String(layerId));
        if (!layer) {
          abort();
          return;
        }
        const layerDataset = findDatasetForLayer(localDatasets, layer);
        const resolvedFieldName = resolveDatasetFieldName(layerDataset, fieldName);
        const fieldObj =
          resolvedFieldName && Array.isArray(layerDataset?.fields)
            ? layerDataset.fields.find((f: any) => String(f?.name || '') === String(resolvedFieldName))
            : null;
        if (!fieldObj) {
          abort();
          return;
        }
        const safeRange = ensureColorRange(colorRange);
        complete();
        const nextConfig: any = {
          colorField: fieldObj,
          colorScale: 'custom',
          visConfig: {
            ...(layer.config?.visConfig || {}),
            colorRange: safeRange,
            ...(applyToStroke ? {strokeColorRange: safeRange} : {})
          },
          colorDomain: manualDomain || undefined
        };
        if (applyToStroke) {
          nextConfig.strokeColorField = fieldObj;
          nextConfig.strokeColorScale = 'custom';
          nextConfig.strokeColorDomain = manualDomain || undefined;
        }
        try {
          localDispatch(wrapTo('map', layerConfigChange(layer, nextConfig)));
        } catch {
          // swallow to avoid breaking UI when layer/dataset schema changed mid-conversation
        }
      }, [localDispatch, localLayers, localDatasets, executionKey, layerId, fieldName, colorRange, manualDomain, applyToStroke, shouldSkip, abort, complete]);
      return null;
    }
  };
}

export function createSetQMapLayerColorByStatsThresholdsTool(ctx: QMapToolContext & {setQMapLayerColorByThresholds: any}) {
  const {
    getCurrentVisState,
    resolveDatasetByName,
    sampleNumericValues,
    computeThresholdsByStrategy,
    setQMapLayerColorByThresholds,
    thresholdStrategySchema,
    paletteSchema
  } = ctx;
  return {
    description:
      'Color an existing layer by numeric field using thresholds derived from statistics (mean/median/mode/quantiles).',
    parameters: z.object({
      datasetName: z.string().describe('Exact dataset name from listQMapDatasets'),
      fieldName: z.string().describe('Numeric field to color by'),
      strategy: thresholdStrategySchema.describe('Threshold strategy'),
      layerName: z.string().optional().describe('Optional layer name to target'),
      classes: z.number().min(3).max(12).optional().describe('Used for strategy=quantiles when quantiles list is not provided'),
      quantiles: z
        .array(z.number().gt(0).lt(1))
        .min(1)
        .max(10)
        .optional()
        .describe('Explicit quantile cut points in (0,1), e.g. [0.25,0.5,0.75]'),
      palette: paletteSchema.describe(
        'Named palette (redGreen, greenRed, blueRed, viridis, magma, yellowRed, yellowBlue).'
      ),
      reverse: z.boolean().optional(),
      applyToStroke: z.boolean().optional()
    }),
    execute: async ({datasetName, fieldName, strategy, layerName, classes, quantiles, palette, reverse, applyToStroke}: any) => {
      const currentVisState = getCurrentVisState();
      const dataset = resolveDatasetByName(currentVisState?.datasets || {}, datasetName);
      if (!dataset?.id) {
        return {
          llmResult: {
            success: false,
            details: `Dataset "${datasetName}" not found. Call listQMapDatasets first.`
          }
        };
      }
      const field = (dataset.fields || []).find(
        (f: any) => String(f?.name || '').toLowerCase() === String(fieldName || '').toLowerCase()
      );
      if (!field?.name) {
        return {
          llmResult: {
            success: false,
            details: `Field "${fieldName}" not found in dataset "${datasetName}".`
          }
        };
      }

      const values = sampleNumericValues(dataset, String(field.name), 50000);
      if (!values.length) {
        return {
          llmResult: {
            success: false,
            details: `Field "${field.name}" has no numeric values in sampled rows.`
          }
        };
      }

      const normalizedStrategy = (strategy || 'quantiles') as 'mean' | 'median' | 'mode' | 'quantiles';
      const computed = computeThresholdsByStrategy(values, normalizedStrategy, classes, quantiles);
      if (!computed.thresholds.length) {
        return {
          llmResult: {
            success: false,
            details: `Could not compute thresholds for strategy "${normalizedStrategy}".`
          }
        };
      }

      const forwarded = await (setQMapLayerColorByThresholds as any).execute({
        datasetName,
        fieldName,
        thresholds: computed.thresholds,
        layerName,
        palette,
        reverse,
        applyToStroke
      });
      if (!forwarded?.llmResult) return forwarded;
      const baseDetails = String(forwarded.llmResult.details || '').trim();
      forwarded.llmResult.details =
        `${baseDetails}${baseDetails ? ' ' : ''}` +
        `Derived thresholds (${strategy}): [${computed.thresholds.join(', ')}]; ${computed.details}` +
        (computed.warning ? ` Warning: ${computed.warning}` : '');
      return forwarded;
    },
    component: (setQMapLayerColorByThresholds as any).component
  };
}
