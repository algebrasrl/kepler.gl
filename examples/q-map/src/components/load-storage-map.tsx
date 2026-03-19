import React, {useCallback, useEffect, useMemo, useState} from 'react';
import styled from 'styled-components';
import moment from 'moment';
import {LoadingDialog, useCloudListProvider} from '@kepler.gl/components';
import {FormattedMessage} from '@kepler.gl/localization';
import {MapListItem, Provider} from '@kepler.gl/cloud-providers';

const Container = styled.div`
  display: flex;
  flex-direction: column;
  gap: 12px;
`;

const HeaderRow = styled.div`
  display: flex;
  align-items: center;
  justify-content: space-between;
  font-size: 12px;
`;

const BackLink = styled.button`
  border: 0;
  background: transparent;
  color: #6a7484;
  cursor: pointer;
  padding: 0;
  font-size: 12px;
`;

const ProviderLink = styled.a`
  text-decoration: underline;
  font-size: 12px;
`;

const ProviderTitle = styled.div`
  font-size: 14px;
  font-weight: 600;
  margin-bottom: 6px;
`;

const ProviderList = styled.div`
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
`;

const ProviderButton = styled.button`
  border: 1px solid #ced4da;
  background: #ffffff;
  border-radius: 4px;
  padding: 8px 10px;
  font-size: 12px;
  cursor: pointer;
  &:disabled {
    cursor: not-allowed;
    opacity: 0.6;
  }
`;

const ErrorText = styled.div`
  color: #b91c1c;
  font-size: 12px;
`;

const Grid = styled.div`
  display: flex;
  flex-wrap: wrap;
  gap: 12px;
`;

const Card = styled.div`
  width: 208px;
  border: 1px solid #d1d5db;
  border-radius: 6px;
  padding: 8px;
  background: #ffffff;
`;

const Thumb = styled.div<{imageUrl?: string}>`
  height: 108px;
  border-radius: 4px;
  background-color: #6a7484;
  background-image: ${props => (props.imageUrl ? `url(${props.imageUrl})` : 'none')};
  background-size: cover;
  background-position: center;
  margin-bottom: 8px;
  cursor: pointer;
`;

const Title = styled.div`
  font-size: 12px;
  font-weight: 600;
  margin-bottom: 4px;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
`;

const Description = styled.div`
  font-size: 11px;
  color: #6a7484;
  min-height: 32px;
  margin-bottom: 6px;
`;

const Meta = styled.div`
  font-size: 11px;
  color: #6a7484;
  margin-bottom: 8px;
`;

const CardActions = styled.div`
  display: flex;
  justify-content: flex-end;
`;

const DeleteButton = styled.button`
  border: 1px solid #ef4444;
  color: #ef4444;
  background: #fff;
  border-radius: 4px;
  padding: 2px 8px;
  font-size: 11px;
  cursor: pointer;
`;

function pickMapId(item: any): string {
  const direct = String(item?.id || '').trim();
  if (direct) return direct;
  const fromLoadParams = String(item?.loadParams?.id || '').trim();
  if (fromLoadParams) return fromLoadParams;
  const fromPath = String(item?.loadParams?.path || '').trim();
  return fromPath ? fromPath.split('/').filter(Boolean).pop() || '' : '';
}

type LoadStorageMapProps = {
  onLoadCloudMap: (payload: {loadParams: any; provider: Provider}) => void;
};

const QMapLoadStorageMap: React.FC<LoadStorageMapProps> = ({onLoadCloudMap}) => {
  const {provider: currentProvider, setProvider, cloudProviders} = useCloudListProvider();
  const [isLoading, setIsLoading] = useState(false);
  const [maps, setMaps] = useState<MapListItem[]>([]);
  const [error, setError] = useState<string>('');
  const [deletingId, setDeletingId] = useState<string>('');
  const [selectingProvider, setSelectingProvider] = useState<string>('');

  const canDelete = useMemo(
    () => Boolean(currentProvider && typeof (currentProvider as any).deleteMap === 'function'),
    [currentProvider]
  );

  const refreshMaps = useCallback(async (provider: Provider) => {
    setIsLoading(true);
    setError('');
    try {
      const nextMaps = await provider.listMaps();
      setMaps(Array.isArray(nextMaps) ? nextMaps : []);
    } catch (err: any) {
      setMaps([]);
      setError(String(err?.message || err || 'Failed to list maps'));
    } finally {
      setIsLoading(false);
    }
  }, []);

  useEffect(() => {
    if (!currentProvider) {
      setMaps([]);
      setError('');
      setIsLoading(false);
      return;
    }
    refreshMaps(currentProvider);
  }, [currentProvider, refreshMaps]);

  const onSelectMap = useCallback(
    (mapItem: MapListItem) => {
      if (!currentProvider) return;
      onLoadCloudMap({
        loadParams: mapItem.loadParams,
        provider: currentProvider
      });
    },
    [currentProvider, onLoadCloudMap]
  );

  const onDeleteMap = useCallback(
    async (event: React.MouseEvent, mapItem: MapListItem) => {
      event.preventDefault();
      event.stopPropagation();
      if (!currentProvider) return;
      const mapId = pickMapId(mapItem);
      if (!mapId) {
        setError('Map id non disponibile per la cancellazione.');
        return;
      }
      if (typeof window !== 'undefined') {
        const mapTitle = mapItem.title || mapItem.id || 'mappa';
        const accepted = window.confirm(`Eliminare la mappa "${mapTitle}"?`);
        if (!accepted) return;
      }
      setDeletingId(mapId);
      setError('');
      try {
        await (currentProvider as any).deleteMap(mapId);
        await refreshMaps(currentProvider);
      } catch (err: any) {
        setError(String(err?.message || err || 'Delete failed'));
      } finally {
        setDeletingId('');
      }
    },
    [currentProvider, refreshMaps]
  );

  const onChooseProvider = useCallback(
    async (provider: Provider) => {
      setSelectingProvider(provider.name);
      setError('');
      try {
        if (typeof (provider as any).login === 'function') {
          await (provider as any).login();
        }
        setProvider(provider);
      } catch (err: any) {
        setProvider(null);
        setError(String(err?.message || err || 'Provider login failed'));
      } finally {
        setSelectingProvider('');
      }
    },
    [setProvider]
  );

  if (!currentProvider) {
    return (
      <Container>
        <ProviderTitle>
          <FormattedMessage id="modal.loadData.storage" defaultMessage="Cloud storage" />
        </ProviderTitle>
        {!cloudProviders?.length ? (
          <div>Nessun provider cloud disponibile</div>
        ) : (
          <ProviderList>
            {cloudProviders.map((provider: Provider) => (
              <ProviderButton
                key={provider.name}
                onClick={() => onChooseProvider(provider)}
                disabled={Boolean(selectingProvider)}
              >
                {selectingProvider === provider.name
                  ? `Connessione a ${provider.displayName || provider.name}...`
                  : provider.displayName || provider.name}
              </ProviderButton>
            ))}
          </ProviderList>
        )}
      </Container>
    );
  }

  return (
    <Container>
      <HeaderRow>
        <BackLink onClick={() => setProvider(null)}>
          <FormattedMessage id="modal.loadStorageMap.back" />
        </BackLink>
        {currentProvider.getManagementUrl() ? (
          <ProviderLink href={currentProvider.getManagementUrl()} target="_blank" rel="noopener noreferrer">
            {currentProvider.displayName}
          </ProviderLink>
        ) : null}
      </HeaderRow>
      <ProviderTitle>
        {currentProvider.displayName} <FormattedMessage id="modal.loadStorageMap.storageMaps" />
      </ProviderTitle>
      {error ? <ErrorText>{error}</ErrorText> : null}
      {isLoading ? (
        <LoadingDialog size={64} />
      ) : maps.length ? (
        <Grid>
          {maps.map(mapItem => {
            const mapId = pickMapId(mapItem);
            return (
              <Card key={mapId || mapItem.title}>
                <Thumb
                  imageUrl={mapItem.imageUrl}
                  onClick={() => onSelectMap(mapItem)}
                  title={mapItem.title}
                />
                <Title title={mapItem.title}>{mapItem.title}</Title>
                <Description>{mapItem.description || ''}</Description>
                <Meta>
                  {mapItem.updatedAt ? `Last modified ${moment.utc(mapItem.updatedAt).fromNow()}` : ''}
                </Meta>
                {canDelete ? (
                  <CardActions>
                    <DeleteButton
                      onClick={event => onDeleteMap(event, mapItem)}
                      disabled={Boolean(deletingId) && deletingId === mapId}
                    >
                      {deletingId && deletingId === mapId ? 'Eliminazione...' : 'Elimina'}
                    </DeleteButton>
                  </CardActions>
                ) : null}
              </Card>
            );
          })}
        </Grid>
      ) : (
        <div>
          <FormattedMessage id="modal.loadStorageMap.noSavedMaps" />
        </div>
      )}
    </Container>
  );
};

export default QMapLoadStorageMap;
