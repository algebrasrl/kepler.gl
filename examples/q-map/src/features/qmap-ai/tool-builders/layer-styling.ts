import {useEffect} from 'react';
import {layerConfigChange, wrapTo} from '@kepler.gl/actions';
import {useDispatch, useSelector} from 'react-redux';
import {z} from 'zod';

import {selectQMapDatasets, selectQMapLayers} from '../../../state/qmap-selectors';
import type {QMapToolContext} from '../context/tool-context';
import {useToolExecution} from './use-tool-execution';

export function createSetQMapLayerColorByFieldTool(ctx: QMapToolContext) {
  const {
    getCurrentVisState,
    resolveDatasetByName,
    resolveDatasetFieldName,
    isAreaLikeFieldName,
    resolveAreaLikeFieldName,
    summarizeNumericField,
    resolveStyleTargetLayer,
    buildLinearHexRange,
    getNamedPalette,
    normalizeLayerLabelForGrouping,
    findDatasetForLayer,
    ensureColorRange,
    getNumericExtent,
    makeExecutionKey,
    colorScaleModeSchema,
    paletteSchema,
    executedToolComponentKeys,
    rememberExecutedToolComponentKey
  } = ctx;
  return {
    description:
      'Color an existing layer by a field with q-hive native scales: linear/quantize/quantile (numeric) or ordinal/categorical (string/category).',
    parameters: z.object({
      datasetName: z.string().describe('Exact dataset name from listQMapDatasets'),
      fieldName: z.string().describe('Field to color by (numeric for linear/quantize/quantile, string for ordinal/categorical)'),
      layerName: z.string().optional().describe('Optional layer name to target'),
      mode: colorScaleModeSchema.describe('Default quantile'),
      classes: z.number().min(3).max(12).optional().describe('Color classes count, default 6'),
      palette: paletteSchema.describe(
        'Named palette (redGreen, greenRed, blueRed, viridis, magma, yellowRed, yellowBlue).'
      ),
      reverse: z.boolean().optional().describe('Reverse palette order'),
      lowColor: z.string().optional().describe('Low-value hex color, default #ff3b30'),
      highColor: z.string().optional().describe('High-value hex color, default #2fbf71'),
      minValue: z.number().optional().describe('Manual min for linear/quantize'),
      maxValue: z.number().optional().describe('Manual max for linear/quantize'),
      applyToStroke: z.boolean().optional().describe('Also apply to stroke color when supported')
    }),
    execute: async ({
      datasetName,
      fieldName,
      layerName,
      mode,
      classes,
      palette,
      reverse,
      lowColor,
      highColor,
      minValue,
      maxValue,
      applyToStroke
    }: any) => {
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
                'Per colorazione per area geometrica, crea prima un dataset derivato con createDatasetWithGeometryArea, poi applica setQMapLayerColorByField sul nuovo campo area.'
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
      const isOrdinal = String(mode || '').toLowerCase() === 'ordinal';

      const numericStats = summarizeNumericField(dataset, String(field.name), 50000);
      if (!isOrdinal && numericStats.numericCount <= 0) {
        return {
          llmResult: {
            success: false,
            details:
              `Field "${field.name}" has no numeric values in sampled rows, so quantitative coloring cannot be applied. ` +
              `Stats: sampled=${numericStats.sampledRows}, nonNull=${numericStats.nonNullCount}, numeric=${numericStats.numericCount}. ` +
              'For categorical/string fields, use mode="ordinal" or mode="categorical".'
          }
        };
      }
      if (!isOrdinal && numericStats.distinctNumericCount <= 1) {
        return {
          llmResult: {
            success: false,
            details:
              `Field "${field.name}" has <=1 distinct numeric value in sampled rows, so color scale would appear uniform. ` +
              `Stats: sampled=${numericStats.sampledRows}, numeric=${numericStats.numericCount}, distinct=${numericStats.distinctNumericCount}.`
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

      const cls = Math.max(3, Math.min(12, Number(classes || 6)));
      const scaleType: 'linear' | 'quantize' | 'quantile' | 'ordinal' = isOrdinal
        ? 'ordinal'
        : (String(mode || 'quantile') as 'linear' | 'quantize' | 'quantile');
      const defaultPalette = isOrdinal ? 'viridis' : 'redGreen';
      const paletteColors =
        lowColor || highColor
          ? buildLinearHexRange(String(lowColor || '#ff3b30'), String(highColor || '#2fbf71'), cls)
          : getNamedPalette(String(palette || defaultPalette)).slice(0, cls);
      const colors = reverse ? [...paletteColors].reverse() : paletteColors;

      const requestedManualDomain =
        !isOrdinal && Number.isFinite(minValue) && Number.isFinite(maxValue) && scaleType !== 'quantile'
          ? [Number(minValue), Number(maxValue)].sort((a, b) => a - b)
          : null;
      // Guardrail: ignore degenerate manual domains (e.g. min=max=0) to avoid uniform single-color styling.
      const manualDomain =
        requestedManualDomain && requestedManualDomain[1] > requestedManualDomain[0]
          ? requestedManualDomain
          : null;
      const colorRange = {
        name: isOrdinal ? 'qmap.ordinalCategorical' : 'qmap.redGreenContinuous',
        type: 'custom',
        category: 'Custom',
        colors
      };
      const targetLabelBase = normalizeLayerLabelForGrouping(layer?.config?.label || layer?.id || '');
      const siblingSingleRowLayerIds = (layers || [])
        .filter((l: any) => String(l?.id || '') !== String(layer?.id || ''))
        .filter((l: any) => l?.config?.isVisible !== false)
        .filter((l: any) => {
          const base = normalizeLayerLabelForGrouping(l?.config?.label || l?.id || '');
          return Boolean(base) && base === targetLabelBase;
        })
        .map((l: any) => {
          const ds = findDatasetForLayer(currentVisState?.datasets || {}, l);
          return {
            id: String(l?.id || ''),
            rowCount: Number(ds?.length || 0)
          };
        })
        .filter((x: {id: string; rowCount: number}) => x.rowCount > 0 && x.rowCount <= 2)
        .map((x: {id: string; rowCount: number}) => x.id);

      return {
        llmResult: {
          success: true,
          details: isOrdinal
            ? `Applying ordinal (categorical) color scale on layer "${layer.config?.label || layer.id}" using field "${field.name}".`
            : `Applying ${scaleType} color scale on layer "${layer.config?.label || layer.id}" using field "${field.name}". ` +
              `Field stats: sampled=${numericStats.sampledRows}, numeric=${numericStats.numericCount}, distinct=${numericStats.distinctNumericCount}.`
        },
        additionalData: {
          executionKey: makeExecutionKey('color-by-field'),
          layerId: layer.id,
          fieldName: String(field.name),
          colorRange,
          scaleType,
          manualDomain,
          applyToStroke: applyToStroke !== false,
          siblingSingleRowLayerIds
        }
      };
    },
    component: function SetQMapLayerColorByFieldComponent({
      executionKey,
      layerId,
      fieldName,
      colorRange,
      scaleType,
      manualDomain,
      applyToStroke,
      siblingSingleRowLayerIds
    }: {
      executionKey?: string;
      layerId: string;
      fieldName: string;
      colorRange: any;
      scaleType: 'linear' | 'quantize' | 'quantile' | 'ordinal' | 'custom';
      manualDomain: number[] | null;
      applyToStroke: boolean;
      siblingSingleRowLayerIds?: string[];
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
        const autoDomain =
          manualDomain && manualDomain.length
            ? manualDomain
            : (resolvedFieldName ? getNumericExtent(layerDataset, resolvedFieldName, 50000) : null);
        complete();
        const nextConfig: any = {
          colorField: fieldObj,
          colorScale: scaleType,
          visConfig: {
            ...(layer.config?.visConfig || {}),
            colorRange: safeRange,
            ...(applyToStroke ? {strokeColorRange: safeRange} : {})
          },
          ...(autoDomain ? {colorDomain: autoDomain} : {})
        };
        if (applyToStroke) {
          nextConfig.strokeColorField = fieldObj;
          nextConfig.strokeColorScale = scaleType;
          if (autoDomain) {
            nextConfig.strokeColorDomain = autoDomain;
          }
        }
        try {
          localDispatch(wrapTo('map', layerConfigChange(layer, nextConfig)));
          (Array.isArray(siblingSingleRowLayerIds) ? siblingSingleRowLayerIds : []).forEach((otherId: string) => {
            const otherLayer = (localLayers || []).find((l: any) => String(l?.id || '') === String(otherId));
            if (!otherLayer || otherLayer?.config?.isVisible === false) return;
            localDispatch(wrapTo('map', layerConfigChange(otherLayer, {isVisible: false})));
          });
        } catch {
          // swallow to avoid breaking UI when layer/dataset schema changed mid-conversation
        }
      }, [
        localDispatch,
        localLayers,
        localDatasets,
        executionKey,
        layerId,
        fieldName,
        colorRange,
        scaleType,
        manualDomain,
        applyToStroke,
        siblingSingleRowLayerIds,
        shouldSkip,
        abort,
        complete
      ]);
      return null;
    }
  };
}

export function createSetQMapLayerSolidColorTool(ctx: QMapToolContext) {
  const {
    getCurrentVisState,
    resolveDatasetByName,
    resolveStyleTargetLayer,
    parseHexColorRgba,
    makeExecutionKey,
    executedToolComponentKeys,
    rememberExecutedToolComponentKey
  } = ctx;
  return {
    description:
      'Apply a solid fill/stroke color style to a layer (no statistical field mapping).',
    parameters: z.object({
      datasetName: z.string().optional().describe('Exact dataset name'),
      layerName: z.string().optional().describe('Exact layer name/id'),
      fillColor: z.string().describe('Hex color for fill, e.g. #f7c600'),
      strokeColor: z.string().optional().describe('Hex color for border, default same as fill'),
      fillOpacity: z.number().min(0).max(1).optional().describe('Default 0.8'),
      strokeOpacity: z.number().min(0).max(1).optional().describe('Default 1'),
      hideFill: z.boolean().optional().describe('If true, transparent fill')
    }),
    execute: async (input: any) => {
      const normalizedInput =
        input && typeof input === 'object' && !Array.isArray(input)
          ? (input as Record<string, unknown>)
          : ({} as Record<string, unknown>);
      const datasetName = String(normalizedInput.datasetName || '').trim() || undefined;
      const layerName = String(normalizedInput.layerName || '').trim() || undefined;
      const fillColor = String(normalizedInput.fillColor || '').trim() || undefined;
      const strokeColor = String(normalizedInput.strokeColor || '').trim() || undefined;
      const fillOpacity =
        normalizedInput.fillOpacity === undefined ? undefined : Number(normalizedInput.fillOpacity);
      const strokeOpacity =
        normalizedInput.strokeOpacity === undefined ? undefined : Number(normalizedInput.strokeOpacity);
      const hideFill = Boolean(normalizedInput.hideFill === true);
      const currentVisState = getCurrentVisState();
      const datasets = Object.values(currentVisState?.datasets || {}) as any[];
      const layers = (currentVisState?.layers || []) as any[];

      let targetLayer: any = null;
      if (layerName) {
        const needle = String(layerName).toLowerCase();
        targetLayer =
          layers.find((l: any) => String(l?.config?.label || '').toLowerCase() === needle) ||
          layers.find((l: any) => String(l?.id || '').toLowerCase() === needle);
      }
      if (!targetLayer && datasetName) {
        const dataset = resolveDatasetByName(
          Object.fromEntries(datasets.map((d: any) => [d?.id, d]).filter(([k]: any) => k)),
          String(datasetName)
        );
        if (dataset?.id && !targetLayer) {
          const resolved = resolveStyleTargetLayer(layers, dataset, undefined);
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
            details: 'Target layer not found. Provide datasetName/layerName or load a dataset first.'
          }
        };
      }

      if (!fillColor) {
        return {
          llmResult: {
            success: false,
            details: 'Missing fillColor. Use fillColor with format #RRGGBB.'
          }
        };
      }
      const fillRgba = parseHexColorRgba(String(fillColor), hideFill ? 0 : (fillOpacity ?? 0.8) * 255);
      const strokeHex = String(strokeColor || fillColor);
      const strokeRgba = parseHexColorRgba(strokeHex, (strokeOpacity ?? 1) * 255);
      if (!fillRgba || !strokeRgba) {
        return {
          llmResult: {
            success: false,
            details: 'Invalid hex color. Use format #RRGGBB.'
          }
        };
      }

      return {
        llmResult: {
          success: true,
          details: `Applying solid style on layer "${targetLayer.config?.label || targetLayer.id}".`
        },
        additionalData: {
          executionKey: makeExecutionKey('solid-color'),
          layerId: targetLayer.id,
          fillRgba,
          strokeRgba
        }
      };
    },
    component: function SetQMapLayerSolidColorComponent({
      executionKey,
      layerId,
      fillRgba,
      strokeRgba
    }: {
      executionKey?: string;
      layerId: string;
      fillRgba: [number, number, number, number];
      strokeRgba: [number, number, number, number];
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
        const layerDataId = layer?.config?.dataId;
        const datasetExists = Array.isArray(layerDataId)
          ? layerDataId.some((id: any) =>
              Object.values(localDatasets || {}).some((d: any) => String(d?.id || '') === String(id))
            )
          : !!String(layerDataId || '') &&
            Object.values(localDatasets || {}).some(
              (d: any) => String(d?.id || '') === String(layerDataId || '')
            );
        if (!datasetExists) {
          abort();
          return;
        }
        complete();
        const fillOpacity = Math.max(0, Math.min(1, Number(fillRgba?.[3] ?? 255) / 255));
        const strokeOpacity = Math.max(0, Math.min(1, Number(strokeRgba?.[3] ?? 255) / 255));
        const fillRgb: [number, number, number] = [fillRgba[0], fillRgba[1], fillRgba[2]];
        const strokeRgb: [number, number, number] = [strokeRgba[0], strokeRgba[1], strokeRgba[2]];
        const nextConfig: any = {
          color: fillRgb,
          colorField: null,
          strokeColorField: null,
          visConfig: {
            ...(layer.config?.visConfig || {}),
            filled: true,
            outline: true,
            opacity: fillOpacity,
            strokeColor: strokeRgb,
            strokeOpacity
          }
        };
        try {
          localDispatch(wrapTo('map', layerConfigChange(layer, nextConfig)));
        } catch {
          // swallow to avoid breaking UI when layer/dataset schema changed mid-conversation
        }
      }, [localDispatch, localLayers, localDatasets, executionKey, layerId, fillRgba, strokeRgba, shouldSkip, abort, complete]);
      return null;
    }
  };
}

export function createSetQMapLayerHeightByFieldTool(ctx: QMapToolContext) {
  const {
    getCurrentVisState,
    resolveDatasetByName,
    resolveDatasetFieldName,
    summarizeNumericField,
    getNumericExtent,
    resolveStyleTargetLayer,
    findDatasetForLayer,
    makeExecutionKey,
    heightScaleSchema,
    executedToolComponentKeys,
    rememberExecutedToolComponentKey
  } = ctx;
  return {
    description:
      'Set layer 3D height/extrusion by numeric field (enables 3D and maps field to elevation).',
    parameters: z.object({
      datasetName: z.string().describe('Exact dataset name from listQMapDatasets'),
      fieldName: z.string().describe('Numeric field for height/extrusion, e.g. population'),
      layerName: z.string().optional().describe('Optional layer name to target'),
      scale: heightScaleSchema.describe('Height scale, default linear'),
      elevationScale: z.number().min(0.1).max(1000).optional().describe('Extrusion multiplier, default 1'),
      maxHeight: z.number().min(10).max(1000).optional().describe('Maximum extruded height (range upper bound), default 120')
    }),
    execute: async ({datasetName, fieldName, layerName, scale, elevationScale, maxHeight}: any) => {
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
      const numericStats = summarizeNumericField(dataset, String(field.name), 50000);
      if (numericStats.numericCount <= 0) {
        return {
          llmResult: {
            success: false,
            details:
              `Field "${field.name}" has no numeric values in sampled rows, so 3D height cannot be applied. ` +
              `Stats: sampled=${numericStats.sampledRows}, nonNull=${numericStats.nonNullCount}, numeric=${numericStats.numericCount}.`
          }
        };
      }
      const extent = getNumericExtent(dataset, String(field.name), 50000);
      if (!extent) {
        return {
          llmResult: {
            success: false,
            details: `Cannot compute numeric extent for field "${field.name}".`
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
      const supports3d = Boolean(layer?.visualChannels?.size || layer?.visConfigSettings?.enable3d);
      if (!supports3d) {
        return {
          llmResult: {
            success: false,
            details: `Layer "${layer.config?.label || layer.id}" does not support 3D height mapping.`
          }
        };
      }

      return {
        llmResult: {
          success: true,
          details:
            `Applying 3D extrusion on layer "${layer.config?.label || layer.id}" using field "${field.name}". ` +
            `Field stats: sampled=${numericStats.sampledRows}, numeric=${numericStats.numericCount}, distinct=${numericStats.distinctNumericCount}.`
        },
        additionalData: {
          executionKey: makeExecutionKey('height-by-field'),
          layerId: layer.id,
          fieldName: String(field.name),
          scale: (scale || 'linear') as 'linear' | 'log' | 'sqrt',
          domain: extent,
          elevationScale: Number.isFinite(Number(elevationScale)) ? Number(elevationScale) : 1,
          maxHeight: Number.isFinite(Number(maxHeight)) ? Number(maxHeight) : 120
        }
      };
    },
    component: function SetQMapLayerHeightByFieldComponent({
      executionKey,
      layerId,
      fieldName,
      scale,
      elevationScale,
      domain,
      maxHeight
    }: {
      executionKey?: string;
      layerId: string;
      fieldName: string;
      scale: 'linear' | 'log' | 'sqrt';
      elevationScale: number;
      domain: [number, number];
      maxHeight: number;
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

        complete();
        const layerType = String(layer?.type || '').toLowerCase();
        const isGeojsonLike =
          layerType.includes('geojson') || layerType.includes('polygon') || layerType.includes('vector');
        const safeElevationScale = Math.max(0.1, Number.isFinite(Number(elevationScale)) ? Number(elevationScale) : 1);
        const safeDomain: [number, number] = Array.isArray(domain) && domain.length === 2
          ? [Number(domain[0]), Number(domain[1])]
          : [0, 1];
        const safeMaxHeight = Math.max(10, Number.isFinite(Number(maxHeight)) ? Number(maxHeight) : 120);
        const nextConfig: any = {
          ...(isGeojsonLike ? {heightField: fieldObj, heightScale: scale} : {sizeField: fieldObj, sizeScale: scale}),
          // Keep both channels aligned to avoid fallback-to-default elevation when layer type changes.
          sizeField: fieldObj,
          sizeScale: scale,
          heightField: fieldObj,
          heightScale: scale,
          sizeDomain: safeDomain,
          heightDomain: safeDomain,
          visConfig: {
            ...(layer.config?.visConfig || {}),
            enable3d: true,
            fixedHeight: true,
            elevationScale: safeElevationScale,
            elevationRange: [0, safeMaxHeight]
          }
        };
        try {
          localDispatch(wrapTo('map', layerConfigChange(layer, nextConfig)));
        } catch {
          // swallow to avoid breaking UI when layer/dataset schema changed mid-conversation
        }
      }, [
        localDispatch,
        localLayers,
        localDatasets,
        executionKey,
        layerId,
        fieldName,
        scale,
        elevationScale,
        domain,
        maxHeight,
        shouldSkip,
        abort,
        complete
      ]);
      return null;
    }
  };
}
