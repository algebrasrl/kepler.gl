// SPDX-License-Identifier: MIT
// Copyright contributors to the kepler.gl project

import {KEPLER_FORMAT, MapListItem, Provider} from '@kepler.gl/cloud-providers';
import {resolveQMapAuthBearerToken} from '../utils/auth-token';

type CustomCloudProviderConfig = {
  name?: string;
  apiBaseUrl: string;
  displayName?: string;
  managementUrl?: string;
  tokenStorageKey?: string;
  staticToken?: string;
  privateStorage?: boolean;
};

type MaybeMap = {
  datasets?: unknown[];
  config?: Record<string, unknown>;
  info?: Record<string, unknown>;
};

type QMapCloudUserResponse = {
  name?: string;
  username?: string;
  email?: string;
};

type QMapCloudMapInfo = {
  id?: string;
  title?: string;
  description?: string;
  loadParams?: {id?: string; path?: string} & Record<string, unknown>;
} & Record<string, unknown>;

type QMapCloudMapMetadata = {
  locked?: boolean;
  lockType?: string;
  actionUuid?: string;
  lockSource?: string;
} & Record<string, unknown>;

type QMapCloudMapRecord = {
  id?: string;
  mapId?: string;
  title?: string;
  name?: string;
  description?: string;
  imageUrl?: string;
  thumbnail?: string;
  thumbnailUrl?: string;
  updatedAt?: string | number;
  updated_at?: string | number;
  lastModified?: string | number;
  privateMap?: boolean;
  loadParams?: {id?: string; path?: string} & Record<string, unknown>;
  info?: QMapCloudMapInfo;
  metadata?: QMapCloudMapMetadata;
  map?: MaybeMap;
  latestState?: {data?: MaybeMap} & Record<string, unknown>;
  format?: string;
  deleted?: boolean;
} & Record<string, unknown>;

type QMapCloudMapIdentifier =
  | string
  | (QMapCloudMapRecord & {loadParams?: {id?: string; path?: string} & Record<string, unknown>});

type QMapUploadMapRequest = {
  mapData?: {
    map?: MaybeMap;
    thumbnail?: Blob | null;
  };
  options?: {
    overwrite?: boolean;
    isPublic?: boolean;
    mapIdToOverwrite?: string;
    mapMetadata?: QMapCloudMapMetadata;
  } & Record<string, unknown>;
};

const NAME = 'custom-cloud';
const DEFAULT_DISPLAY_NAME = 'Custom Cloud';
const DEFAULT_TOKEN_STORAGE_KEY = 'qmap_custom_cloud_token';

function toMillis(value: unknown): number | undefined {
  if (typeof value === 'number') {
    return value;
  }
  if (typeof value === 'string') {
    const dateValue = Date.parse(value);
    return Number.isNaN(dateValue) ? undefined : dateValue;
  }
  return undefined;
}

function isObjectRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === 'object' && !Array.isArray(value);
}

function isActionLockedMetadata(metadata: unknown): boolean {
  if (!isObjectRecord(metadata)) {
    return false;
  }
  if (String(metadata.lockType || '').trim() !== 'action') {
    return false;
  }
  if (!('locked' in metadata)) {
    return true;
  }
  return Boolean(metadata.locked);
}

export default class CustomCloudProvider extends (Provider as any) {
  private apiBaseUrl: string;
  private managementUrl: string;
  private tokenStorageKey: string;
  private staticToken?: string;
  private privateStorage: boolean;

  constructor(config: CustomCloudProviderConfig) {
    super({
      name: config.name || NAME,
      displayName: config.displayName || DEFAULT_DISPLAY_NAME
    });

    this.apiBaseUrl = config.apiBaseUrl.replace(/\/+$/, '');
    this.managementUrl = config.managementUrl || this.apiBaseUrl;
    this.tokenStorageKey = config.tokenStorageKey || DEFAULT_TOKEN_STORAGE_KEY;
    this.staticToken = config.staticToken;
    this.privateStorage = config.privateStorage ?? true;
  }

  hasPrivateStorage(): boolean {
    return this.privateStorage;
  }

  hasSharingUrl(): boolean {
    return false;
  }

  async login() {
    let token = await this.getAccessToken();

    // In dev setups, backend may allow unauthenticated access. Try /me first.
    if (!token) {
      const userWithoutToken = await this.getUser();
      if (userWithoutToken) {
        return userWithoutToken;
      }
    }

    if (!token && typeof window !== 'undefined') {
      token = window.prompt('Enter your custom cloud API token')?.trim() || '';
      if (token) {
        this.saveToken(token);
      }
    }

    const user = await this.getUser();
    if (!user) {
      throw new Error(
        'Authentication failed. Provide a valid token or enable unauthenticated /me access on backend.'
      );
    }

    return user;
  }

  async logout(): Promise<void> {
    if (typeof window !== 'undefined' && window.localStorage) {
      window.localStorage.removeItem(this.tokenStorageKey);
    }
  }

  async getUser() {
    const response = await this.request('/me', {method: 'GET'});
    if (response.status === 401) {
      return null;
    }
    await this.throwIfNotOk(response, 'Failed to get current user');

    const user = (await response.json()) as QMapCloudUserResponse;
    return {
      name: user?.name || user?.username || user?.email || 'Authenticated user',
      email: user?.email || ''
    };
  }

  async getAccessToken(): Promise<string | null> {
    const sharedToken = resolveQMapAuthBearerToken();
    if (sharedToken) {
      return sharedToken;
    }

    if (typeof window !== 'undefined' && window.localStorage) {
      const localToken = window.localStorage.getItem(this.tokenStorageKey);
      if (localToken) {
        return localToken;
      }
    }

    if (this.staticToken) {
      return this.staticToken;
    }

    return null;
  }

  async uploadMap({mapData, options = {}}: QMapUploadMapRequest) {
    const map = mapData?.map as MaybeMap;
    const mapInfo = (map?.info || {}) as QMapCloudMapInfo;

    const title =
      typeof mapInfo.title === 'string' && mapInfo.title.trim() ? mapInfo.title : 'Untitled map';
    const description = typeof mapInfo.description === 'string' ? mapInfo.description : '';
    const overwriteId =
      options.mapIdToOverwrite ||
      (typeof mapInfo.id === 'string' ? mapInfo.id : undefined) ||
      (typeof mapInfo.loadParams?.id === 'string' ? mapInfo.loadParams.id : undefined);

    const method = options.overwrite && overwriteId ? 'PUT' : 'POST';
    const path = method === 'PUT' ? `/maps/${encodeURIComponent(overwriteId as string)}` : '/maps';

    const payload: Record<string, unknown> = {
      title,
      description,
      isPublic: Boolean(options.isPublic),
      map,
      format: KEPLER_FORMAT
    };
    if (isObjectRecord(options.mapMetadata)) {
      payload.metadata = options.mapMetadata;
    }

    if (mapData?.thumbnail) {
      payload.thumbnail = await this.blobToDataUrl(mapData.thumbnail);
    }

    const response = await this.request(path, {
      method,
      body: JSON.stringify(payload)
    });

    if (response.status === 409 && !options.overwrite) {
      throw this.getFileConflictError();
    }
    await this.throwIfNotOk(response, 'Failed to save map');

    const data = await this.readJson<QMapCloudMapRecord>(response);
    const savedId = this.extractId(data) || overwriteId;

    return {
      ...data,
      id: savedId,
      info: {
        ...(data?.info || {}),
        id: savedId
      } as QMapCloudMapInfo,
      loadParams: data?.loadParams || (savedId ? {id: savedId} : undefined)
    };
  }

  async listMaps(): Promise<MapListItem[]> {
    const response = await this.request('/maps', {method: 'GET'});
    await this.throwIfNotOk(response, 'Failed to list maps');

    const data = await this.readJson<QMapCloudMapRecord[] | {items?: QMapCloudMapRecord[]}>(response);
    const items = Array.isArray(data) ? data : data?.items;
    if (!Array.isArray(items)) {
      return [];
    }

    return items.map((item: QMapCloudMapRecord) => {
      const id = this.extractId(item) || '';
      const title =
        (typeof item?.title === 'string' && item.title) ||
        (typeof item?.name === 'string' && item.name) ||
        'Untitled map';
      const updatedAt = toMillis(item?.updatedAt ?? item?.updated_at ?? item?.lastModified);
      const metadata = isObjectRecord(item?.metadata) ? item.metadata : {};
      const actionLocked = isActionLockedMetadata(metadata);

      return {
        id,
        title,
        description: item?.description || '',
        imageUrl: item?.imageUrl || item?.thumbnail || item?.thumbnailUrl,
        updatedAt,
        privateMap: item?.privateMap,
        readOnly: actionLocked,
        metadata,
        loadParams: item?.loadParams || {id, path: item?.path || `/maps/${id}`}
      };
    });
  }

  async downloadMap(loadParams: {id?: string; path?: string} & Record<string, unknown>): Promise<{map: MaybeMap; format: string}> {
    const id =
      loadParams?.id ||
      (typeof loadParams?.path === 'string' ? loadParams.path.split('/').filter(Boolean).pop() : '');

    if (!id) {
      throw new Error('Missing map id in loadParams. Expected loadParams.id or loadParams.path.');
    }

    const response = await this.request(`/maps/${encodeURIComponent(id)}`, {method: 'GET'});
    await this.throwIfNotOk(response, 'Failed to load map');

    const data = await this.readJson<QMapCloudMapRecord>(response);
    const map = data?.map || data?.latestState?.data || data;
    const format = data?.format || KEPLER_FORMAT;
    const metadata = isObjectRecord(data?.metadata) ? data.metadata : null;

    if (!map?.datasets || !map?.config) {
      throw new Error('Invalid map payload from custom cloud API. Expected object with datasets and config.');
    }

    if (isObjectRecord(map)) {
      const existingInfo = isObjectRecord(map.info) ? map.info : {};
      if (metadata) {
        map.info = {
          ...existingInfo,
          qMapStorageMetadata: metadata
        };
      }
      if (metadata && isActionLockedMetadata(metadata) && typeof window !== 'undefined' && window.self === window.top) {
        const outerConfig = isObjectRecord(map.config) ? map.config : {};
        const innerConfig = isObjectRecord(outerConfig.config) ? outerConfig.config : {};
        const existingUiState = isObjectRecord(innerConfig.uiState) ? innerConfig.uiState : {};
        map.config = {
          ...outerConfig,
          config: {
            ...innerConfig,
            uiState: {
              ...existingUiState,
              readOnly: true
            }
          }
        };
      }
    }

    return {
      map,
      format
    };
  }

  async deleteMap(mapOrId: QMapCloudMapIdentifier): Promise<{id: string; deleted: boolean}> {
    const record = typeof mapOrId === 'string' ? null : mapOrId;
    const id =
      (typeof mapOrId === 'string' ? mapOrId : '') ||
      this.extractId(record) ||
      record?.loadParams?.id ||
      (typeof record?.loadParams?.path === 'string'
        ? record.loadParams.path.split('/').filter(Boolean).pop()
        : '');
    if (!id) {
      throw new Error('Missing map id for delete operation.');
    }

    const response = await this.request(`/maps/${encodeURIComponent(id)}`, {method: 'DELETE'});
    await this.throwIfNotOk(response, 'Failed to delete map');

    const data = await this.readJson<QMapCloudMapRecord>(response);
    return {
      id: String(data?.id || id),
      deleted: data?.deleted !== false
    };
  }

  getManagementUrl(): string {
    return this.managementUrl;
  }

  private async request(path: string, init: RequestInit): Promise<Response> {
    const token = await this.getAccessToken();
    const headers: Record<string, string> = {
      Accept: 'application/json'
    };

    if (init.body) {
      headers['Content-Type'] = 'application/json';
    }
    if (token) {
      headers.Authorization = `Bearer ${token}`;
    }

    return fetch(`${this.apiBaseUrl}${path}`, {
      ...init,
      headers: {
        ...headers,
        ...(init.headers || {})
      }
    });
  }

  private async throwIfNotOk(response: Response, fallbackMessage: string) {
    if (response.ok) {
      return;
    }

    const errorBody = await this.readJson<Record<string, unknown>>(response);
    const message =
      this.readErrorMessage(errorBody) ||
      response.statusText ||
      fallbackMessage;
    throw new Error(message);
  }

  private async readJson<T>(response: Response): Promise<T | null> {
    const text = await response.text();
    if (!text) {
      return null;
    }

    try {
      return JSON.parse(text) as T;
    } catch {
      return null;
    }
  }

  private saveToken(token: string) {
    if (typeof window !== 'undefined' && window.localStorage) {
      window.localStorage.setItem(this.tokenStorageKey, token);
    }
  }

  private extractId(data: QMapCloudMapRecord | null | undefined): string {
    if (!data) {
      return '';
    }

    return data.id || data.mapId || data.info?.id || '';
  }

  private blobToDataUrl(blob: Blob): Promise<string> {
    return new Promise((resolve, reject) => {
      const reader = new FileReader();
      reader.onload = () => resolve(String(reader.result || ''));
      reader.onerror = () => reject(new Error('Unable to serialize thumbnail image'));
      reader.readAsDataURL(blob);
    });
  }

  private readErrorMessage(errorBody: Record<string, unknown> | null): string {
    if (!errorBody) {
      return '';
    }

    if (typeof errorBody.message === 'string' && errorBody.message.trim()) {
      return errorBody.message;
    }

    const nestedError = errorBody.error;
    if (typeof nestedError === 'string' && nestedError.trim()) {
      return nestedError;
    }

    if (nestedError && typeof nestedError === 'object') {
      const nestedMessage = (nestedError as {message?: unknown}).message;
      if (typeof nestedMessage === 'string' && nestedMessage.trim()) {
        return nestedMessage;
      }
    }

    return '';
  }
}
