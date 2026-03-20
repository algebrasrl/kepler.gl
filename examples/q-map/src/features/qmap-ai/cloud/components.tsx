/**
 * React components for cloud tool side effects and the getQMapCloudTools barrel.
 */
import {useEffect} from 'react';
import {useDispatch} from 'react-redux';
import {addDataToMap, loadCloudMap, wrapTo} from '@kepler.gl/actions';
import {createCloudStorageProvider} from './cloud-providers';
import {withUniqueMapDatasetIdentity} from './qcumber-dataset-identity';
import {hasExecutedQueryKey, rememberExecutedQueryKey} from './qcumber-cache';
import {DEFAULT_ASSISTANT_BASE} from './constants';
import {
  createListQMapCloudMapsTool,
  createLoadQMapCloudMapTool,
  createListQCumberProvidersTool,
  createListQCumberDatasetsTool,
  createGetQCumberDatasetHelpTool,
  createQueryQCumberDatasetTool
} from './tool-factories';

export function LoadQMapCloudMapComponent({provider, loadParams}: {provider: string; loadParams: any}) {
  const dispatch = useDispatch();

  useEffect(() => {
    const providerInstance = createCloudStorageProvider(provider);
    dispatch(
      wrapTo(
        'map',
        loadCloudMap({
          provider: providerInstance as any,
          loadParams
        }) as any
      )
    );
  }, [dispatch, provider, loadParams]);

  return null;
}

export function QueryQCumberDatasetComponent({
  dataset,
  executionKey,
  autoCreateLayers
}: {
  dataset: any;
  executionKey?: string;
  autoCreateLayers?: boolean;
}) {
  const dispatch = useDispatch();

  useEffect(() => {
    if (!dataset) {
      return;
    }
    if (executionKey && hasExecutedQueryKey(executionKey)) {
      return;
    }
    if (executionKey) {
      rememberExecutedQueryKey(executionKey);
    }
    const mapDataset = withUniqueMapDatasetIdentity(dataset, executionKey);

    dispatch(
      wrapTo(
        'map',
        addDataToMap({
          datasets: mapDataset,
          options: {
            // Query results can change schema between calls; avoid reusing stale layer config.
            keepExistingConfig: false,
            centerMap: false,
            autoCreateLayers: autoCreateLayers !== false
          }
        }) as any
      )
    );
  }, [dispatch, dataset, executionKey, autoCreateLayers]);

  return null;
}

export function getQMapCloudTools(apiBaseUrl?: string) {
  const resolvedBase = (apiBaseUrl || DEFAULT_ASSISTANT_BASE).replace(/\/+$/, '');
  return {
    listQMapCloudMaps: createListQMapCloudMapsTool(resolvedBase),
    loadQMapCloudMap: createLoadQMapCloudMapTool(resolvedBase),
    listQCumberProviders: createListQCumberProvidersTool(),
    listQCumberDatasets: createListQCumberDatasetsTool(),
    getQCumberDatasetHelp: createGetQCumberDatasetHelpTool(),
    queryQCumberDataset: createQueryQCumberDatasetTool('auto'),
    queryQCumberTerritorialUnits: createQueryQCumberDatasetTool('territorial'),
    queryQCumberDatasetSpatial: createQueryQCumberDatasetTool('thematic_spatial')
  };
}
