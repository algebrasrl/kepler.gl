import {extendedTool} from '../tool-shim';
import {z} from 'zod';

import type {QMapToolContext} from '../context/tool-context';

export function createListQMapDatasetsTool(ctx: QMapToolContext) {
  const {
    getCurrentVisState,
    findDatasetForLayer,
    getDatasetFieldNames,
    extractLayerEffectiveFieldNames,
    getTooltipFieldNamesForDataset
  } = ctx;

  return extendedTool({
    description:
      'List currently loaded datasets in q-map with exact names, ids and datasetRef (`id:<datasetId>`) for reliable chaining across tools.',
    parameters: z.object({}),
    execute: async () => {
      const currentVisState = getCurrentVisState();
      const datasetsMap = currentVisState?.datasets || {};
      const datasetList = Object.values(currentVisState?.datasets || {}).map((dataset: any) => ({
        id: dataset?.id || '',
        name: dataset?.label || dataset?.id || '',
        datasetRef: dataset?.id ? `id:${dataset.id}` : '',
        fields: (dataset?.fields || []).map((f: any) => f?.name).filter(Boolean)
      }));
      const layerList = (currentVisState?.layers || []).map((layer: any) => {
        const layerDataset = findDatasetForLayer(datasetsMap, layer);
        const availableFields = getDatasetFieldNames(layerDataset, 64);
        return {
          id: layer?.id || '',
          name: layer?.config?.label || layer?.id || '',
          datasetId: layerDataset?.id || layer?.config?.dataId || '',
          datasetName: layerDataset?.label || layerDataset?.id || '',
          datasetRef: layerDataset?.id ? `id:${layerDataset.id}` : '',
          type: layer?.type || '',
          activeFields: extractLayerEffectiveFieldNames(layer, layerDataset),
          tooltipFields: getTooltipFieldNamesForDataset(currentVisState, layerDataset?.id || ''),
          availableFields
        };
      });

      return {
        llmResult: {
          success: true,
          datasets: datasetList,
          layers: layerList,
          details: datasetList.length
            ? `Found ${datasetList.length} datasets in current map.`
            : 'No datasets loaded in current map.'
        }
      };
    }
  });

}

export function createLoadCloudMapAndWaitTool(ctx: QMapToolContext) {
  const {
    DEFAULT_PROVIDER,
    assistantBaseUrl,
    normalizeCloudMapProvider,
    callMcpToolParsed,
    getQMapProvider,
    getCurrentVisState,
    dispatch,
    wrapTo,
    loadCloudMap
  } = ctx;

  return extendedTool({
    description:
      'Load a q-map cloud map and wait until visState datasets are available. Uses MCP for map resolution and q-hive loadCloudMap action.',
    parameters: z.object({
      provider: z.string().optional(),
      mapId: z.string().describe('Cloud map id returned by listQMapCloudMaps'),
      timeoutMs: z.number().optional()
    }),
    execute: async ({provider, mapId, timeoutMs}) => {
      const resolvedProvider = normalizeCloudMapProvider(provider || DEFAULT_PROVIDER);
      const requestedProvider = String(resolvedProvider).trim().toLowerCase();
      const resolvedMapId = String(mapId || '').trim();
      const mapsPayload = await callMcpToolParsed(assistantBaseUrl, ['list_qmap_cloud_maps'], {
        provider: resolvedProvider
      });
      const maps = Array.isArray(mapsPayload?.maps) ? mapsPayload.maps : [];
      if (!resolvedMapId) {
        if (requestedProvider === 'q-cumber-backend') {
          return {
            llmResult: {
              success: false,
              details:
                'Missing mapId. For administrative boundaries from q-cumber, use listQCumberDatasets + queryQCumberDataset (loadToMap=true), then filter/tessellate.'
            }
          };
        }
        return {
          llmResult: {
            success: false,
            details: 'Missing mapId. Call listQMapCloudMaps first and pass a valid mapId.'
          }
        };
      }
      const hasExactMap = maps.some((m: any) => String(m?.id || '').toLowerCase() === resolvedMapId.toLowerCase());
      if (!hasExactMap) {
        return {
          llmResult: {
            success: false,
            details:
              `Map "${resolvedMapId}" not found in cloud maps. ` +
              'For administrative boundaries use listQCumberDatasets + queryQCumberDataset, not loadCloudMapAndWait.'
          }
        };
      }

      const actionPayload = await callMcpToolParsed(assistantBaseUrl, ['build_load_cloud_map_action'], {
        provider: resolvedProvider,
        mapId: resolvedMapId
      });
      const args = actionPayload?.action?.args || {};
      const providerName = normalizeCloudMapProvider(String(args.provider || resolvedProvider));
      const actionMapId = String(args.mapId || resolvedMapId);

      const providerInstance = getQMapProvider(providerName);
      const beforeIds = new Set(Object.keys(getCurrentVisState()?.datasets || {}));
      dispatch(
        wrapTo(
          'map',
          loadCloudMap({
            provider: providerInstance as any,
            loadParams: {
              id: actionMapId,
              path: `/maps/${actionMapId}`
            }
          }) as any
        )
      );

      const timeout = Math.max(1000, Number(timeoutMs || 12000));
      const pollEvery = 200;
      const startedAt = Date.now();
      while (Date.now() - startedAt < timeout) {
        await new Promise(resolve => setTimeout(resolve, pollEvery));
        const currentDatasets = getCurrentVisState()?.datasets || {};
        const currentIds = Object.keys(currentDatasets);
        const hasNew = currentIds.some(id => !beforeIds.has(id));
        if (hasNew || currentIds.length > 0) {
          const loadedDatasetId = currentIds.find(id => !beforeIds.has(id)) || currentIds[0] || '';
          const loadedDataset = loadedDatasetId ? currentDatasets?.[loadedDatasetId] : null;
          const loadedDatasetName = String((loadedDataset as any)?.label || loadedDatasetId || '').trim();
          const loadedDatasetRef = loadedDatasetId ? `id:${loadedDatasetId}` : '';
          return {
            llmResult: {
              success: true,
              loadedDatasetId: loadedDatasetId || undefined,
              loadedDatasetName: loadedDatasetName || undefined,
              loadedDatasetRef: loadedDatasetRef || undefined,
              datasetId: loadedDatasetId || undefined,
              datasetName: loadedDatasetName || undefined,
              datasetRef: loadedDatasetRef || undefined,
              details: loadedDatasetRef
                ? `Loaded cloud map ${actionMapId}. Final dataset "${loadedDatasetName}" (${loadedDatasetRef}) is available.`
                : `Loaded cloud map ${actionMapId}. Datasets are available.`
            }
          };
        }
      }

      return {
        llmResult: {
          success: false,
          details: `Cloud map ${actionMapId} load dispatched but timed out waiting for datasets.`
        }
      };
    }
  });

}
