import {useEffect} from 'react';
import {layerConfigChange, reorderLayer, wrapTo} from '@kepler.gl/actions';
import {useDispatch, useSelector} from 'react-redux';
import {z} from 'zod';

import {selectQMapLayers} from '../../../state/qmap-selectors';
import type {QMapToolContext} from '../context/tool-context';
import {useToolExecution} from './use-tool-execution';

export function createSetQMapLayerVisibilityTool(ctx: QMapToolContext) {
  const {getCurrentVisState, resolveDatasetByName, makeExecutionKey, executedToolComponentKeys, rememberExecutedToolComponentKey} = ctx;
  return {
    description: 'Set a layer visibility on/off by layer name or id.',
    parameters: z.object({
      layerNameOrId: z.string().describe('Exact layer label or id'),
      visible: z.boolean().describe('true to show, false to hide')
    }),
    execute: async ({layerNameOrId, visible}: any) => {
      const currentVisState = getCurrentVisState();
      const datasets = currentVisState?.datasets || {};
      const layers = (currentVisState?.layers || []) as any[];
      const rawNeedle = String(layerNameOrId || '').trim();
      const needle = rawNeedle.toLowerCase();
      const matchesDatasetId = (l: any, datasetId: string) => {
        const did = String(datasetId || '').toLowerCase();
        if (!did) return false;
        const dataId = l?.config?.dataId;
        if (Array.isArray(dataId)) return dataId.some((v: any) => String(v || '').toLowerCase() === did);
        return String(dataId || '').toLowerCase() === did;
      };
      let layer = layers.find((l: any) => {
        const label = String(l?.config?.label || '').toLowerCase();
        const id = String(l?.id || '').toLowerCase();
        return label === needle || id === needle;
      });
      if (!layer?.id) {
        const stripped = needle.replace(/^id:\s*/i, '').trim();
        if (stripped) layer = layers.find((l: any) => matchesDatasetId(l, stripped));
      }
      if (!layer?.id) {
        const dataset = resolveDatasetByName(datasets, rawNeedle);
        const rid = String(dataset?.id || '').trim();
        if (rid) layer = layers.find((l: any) => matchesDatasetId(l, rid));
      }

      if (!layer?.id) {
        return {
          llmResult: {
            success: false,
            details: `Layer "${layerNameOrId}" not found. Call listQMapDatasets first and use the exact layer name/id.`
          }
        };
      }

      return {
        llmResult: {
          success: true,
          details: `Setting layer "${layer.config?.label || layer.id}" visibility to ${visible ? 'ON' : 'OFF'}.`
        },
        additionalData: {
          executionKey: makeExecutionKey('layer-visibility'),
          layerId: layer.id,
          visible
        }
      };
    },
    component: function SetQMapLayerVisibilityComponent({
      executionKey,
      layerId,
      visible
    }: {
      executionKey?: string;
      layerId: string;
      visible: boolean;
    }) {
      const localDispatch = useDispatch<any>();
      const localLayers = useSelector(selectQMapLayers) as any[];
      const {shouldSkip, complete} = useToolExecution({executionKey, executedToolComponentKeys, rememberExecutedToolComponentKey});
      useEffect(() => {
        if (shouldSkip()) return;
        const layer = (localLayers || []).find((l: any) => String(l?.id || '') === String(layerId));
        if (!layer) return;
        complete();
        localDispatch(wrapTo('map', layerConfigChange(layer, {isVisible: Boolean(visible)})));
      }, [localDispatch, localLayers, executionKey, layerId, visible, shouldSkip, complete]);
      return null;
    }
  };
}

export function createShowOnlyQMapLayerTool(ctx: QMapToolContext) {
  const {getCurrentVisState, resolveDatasetByName, makeExecutionKey, executedToolComponentKeys, rememberExecutedToolComponentKey} = ctx;
  return {
    description: 'Show only one layer and hide all others.',
    parameters: z.object({
      layerNameOrId: z.string().describe('Layer to keep visible')
    }),
    execute: async ({layerNameOrId}: any) => {
      const currentVisState = getCurrentVisState();
      const datasets = currentVisState?.datasets || {};
      const layers = (currentVisState?.layers || []) as any[];
      const rawNeedle = String(layerNameOrId || '').trim();
      const needle = rawNeedle.toLowerCase();
      const matchesDatasetId = (layer: any, datasetId: string) => {
        const normalizedDatasetId = String(datasetId || '').toLowerCase();
        if (!normalizedDatasetId) return false;
        const dataId = layer?.config?.dataId;
        if (Array.isArray(dataId)) {
          return dataId.some(value => String(value || '').toLowerCase() === normalizedDatasetId);
        }
        return String(dataId || '').toLowerCase() === normalizedDatasetId;
      };
      let target = layers.find((l: any) => {
        const label = String(l?.config?.label || '').toLowerCase();
        const id = String(l?.id || '').toLowerCase();
        return label === needle || id === needle;
      });
      if (!target?.id) {
        const datasetIdNeedle = needle.replace(/^id:\s*/i, '').trim();
        if (datasetIdNeedle) {
          target = layers.find((layer: any) => matchesDatasetId(layer, datasetIdNeedle));
        }
      }
      if (!target?.id) {
        const dataset = resolveDatasetByName(datasets, rawNeedle);
        const resolvedDatasetId = String(dataset?.id || '').trim();
        if (resolvedDatasetId) {
          target = layers.find((layer: any) => matchesDatasetId(layer, resolvedDatasetId));
        }
      }

      if (!target?.id) {
        return {
          llmResult: {
            success: false,
            details: `Layer "${layerNameOrId}" not found.`
          }
        };
      }

      return {
        llmResult: {
          success: true,
          details: `Showing only "${target.config?.label || target.id}" and hiding other layers.`
        },
        additionalData: {
          executionKey: makeExecutionKey('show-only-layer'),
          keepLayerId: target.id
        }
      };
    },
    component: function ShowOnlyQMapLayerComponent({
      executionKey,
      keepLayerId
    }: {
      executionKey?: string;
      keepLayerId: string;
    }) {
      const localDispatch = useDispatch<any>();
      const localLayers = useSelector(selectQMapLayers) as any[];
      const {shouldSkip, complete} = useToolExecution({executionKey, executedToolComponentKeys, rememberExecutedToolComponentKey});
      useEffect(() => {
        if (shouldSkip()) return;
        if (!Array.isArray(localLayers) || !localLayers.length) return;
        complete();
        localLayers.forEach((layer: any) => {
          const isVisible = String(layer?.id || '') === String(keepLayerId);
          localDispatch(wrapTo('map', layerConfigChange(layer, {isVisible})));
        });
      }, [localDispatch, localLayers, executionKey, keepLayerId, shouldSkip, complete]);
      return null;
    }
  };
}

export function createSetQMapLayerOrderTool(ctx: QMapToolContext) {
  const {getCurrentVisState, makeExecutionKey, positionSchema, executedToolComponentKeys, rememberExecutedToolComponentKey} = ctx;
  return {
    description: 'Reorder layers in z-order. Index 0 = bottom, last = top.',
    parameters: z.object({
      layerNameOrId: z.string().describe('Layer to move'),
      position: positionSchema.describe('Target position'),
      referenceLayerNameOrId: z.string().optional().describe('Required for above/below')
    }),
    execute: async ({layerNameOrId, position, referenceLayerNameOrId}: any) => {
      const currentVisState = getCurrentVisState();
      const layers = (currentVisState?.layers || []) as any[];
      const findLayer = (nameOrId?: string) => {
        const needle = String(nameOrId || '').toLowerCase();
        return layers.find((l: any) => {
          const label = String(l?.config?.label || '').toLowerCase();
          const id = String(l?.id || '').toLowerCase();
          return label === needle || id === needle;
        });
      };

      const target = findLayer(layerNameOrId);
      if (!target?.id) {
        return {llmResult: {success: false, details: `Layer "${layerNameOrId}" not found.`}};
      }

      let reference: any = null;
      if (position === 'above' || position === 'below') {
        reference = findLayer(referenceLayerNameOrId);
        if (!reference?.id) {
          return {
            llmResult: {
              success: false,
              details: `Reference layer "${referenceLayerNameOrId}" not found for position "${position}".`
            }
          };
        }
        if (String(reference.id) === String(target.id)) {
          return {
            llmResult: {
              success: false,
              details: 'Target layer and reference layer are the same.'
            }
          };
        }
      }

      return {
        llmResult: {
          success: true,
          details:
            position === 'top' || position === 'bottom'
              ? `Moving "${target.config?.label || target.id}" to ${position}.`
              : `Moving "${target.config?.label || target.id}" ${position} "${reference?.config?.label || reference?.id}".`
        },
        additionalData: {
          executionKey: makeExecutionKey('layer-order'),
          targetLayerId: target.id,
          referenceLayerId: reference?.id || null,
          position
        }
      };
    },
    component: function SetQMapLayerOrderComponent({
      executionKey,
      targetLayerId,
      referenceLayerId,
      position
    }: {
      executionKey?: string;
      targetLayerId: string;
      referenceLayerId: string | null;
      position: 'top' | 'bottom' | 'above' | 'below';
    }) {
      const localDispatch = useDispatch<any>();
      const localLayers = useSelector(selectQMapLayers) as any[];
      const {shouldSkip, complete} = useToolExecution({executionKey, executedToolComponentKeys, rememberExecutedToolComponentKey});
      useEffect(() => {
        if (shouldSkip()) return;
        if (!Array.isArray(localLayers) || !localLayers.length) return;
        const ids = localLayers.map((l: any) => String(l?.id || '')).filter(Boolean);
        const targetIdx = ids.indexOf(String(targetLayerId));
        if (targetIdx < 0) return;
        const nextOrder = ids.filter(id => id !== String(targetLayerId));
        let insertAt = nextOrder.length;

        if (position === 'top') {
          insertAt = nextOrder.length;
        } else if (position === 'bottom') {
          insertAt = 0;
        } else {
          const refIdx = nextOrder.indexOf(String(referenceLayerId || ''));
          if (refIdx < 0) return;
          insertAt = position === 'above' ? refIdx + 1 : refIdx;
        }

        nextOrder.splice(Math.max(0, Math.min(nextOrder.length, insertAt)), 0, String(targetLayerId));
        complete();
        localDispatch(wrapTo('map', reorderLayer(nextOrder)));
      }, [localDispatch, localLayers, executionKey, targetLayerId, referenceLayerId, position, shouldSkip, complete]);
      return null;
    }
  };
}
