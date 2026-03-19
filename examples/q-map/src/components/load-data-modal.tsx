import React from 'react';
import {useSelector} from 'react-redux';
import {LoadDataModalFactory} from '@kepler.gl/components';
import QMapLoadRemoteMap from './load-remote-map';
import QMapLoadStorageMap from './load-storage-map';
import {resolveQMapModeFromUiState} from '../mode/qmap-mode';
import {selectQMapUiState} from '../state/qmap-selectors';

const LazyQMapFileUpload = React.lazy(async () => {
  const module = await import('./file-upload-with-url');
  return {default: module.default()};
});

const LazyQMapFileUploadWrapper: React.FC<any> = props => (
  <React.Suspense fallback={null}>
    <LazyQMapFileUpload {...props} />
  </React.Suspense>
);

function QMapLoadDataModalFactory(...deps: any[]) {
  const LoadDataModal = (LoadDataModalFactory as any)(...deps);
  const defaultLoadingMethods = LoadDataModal.defaultLoadingMethods || [];

  const uploadMethod = defaultLoadingMethods.find((lm: any) => lm.id === 'upload');
  const uploadMethodWithQMapExtensions = uploadMethod
    ? {
        ...uploadMethod,
        elementType: LazyQMapFileUploadWrapper
      }
    : null;

  const remoteMethod = {
    id: 'remote',
    label: 'modal.loadData.remote',
    elementType: QMapLoadRemoteMap
  };

  const storageMethod = {
    id: 'storage',
    label: 'modal.loadData.storage',
    elementType: QMapLoadStorageMap
  };

  const defaultOrderedMethods = [
    uploadMethodWithQMapExtensions,
    defaultLoadingMethods.find((lm: any) => lm.id === 'tileset'),
    remoteMethod,
    storageMethod
  ].filter(Boolean);

  const WrappedLoadDataModal: React.FC<any> = props => {
    const uiState = useSelector((state: any) => selectQMapUiState(state));
    const mode = resolveQMapModeFromUiState(uiState);

    const loadingMethods =
      mode === 'draw-stressor'
        ? defaultOrderedMethods.filter((method: any) => method?.id === 'upload')
        : defaultOrderedMethods;

    return <LoadDataModal {...props} loadingMethods={loadingMethods} />;
  };

  return WrappedLoadDataModal;
}

QMapLoadDataModalFactory.deps = (LoadDataModalFactory as any).deps;

export default QMapLoadDataModalFactory;
