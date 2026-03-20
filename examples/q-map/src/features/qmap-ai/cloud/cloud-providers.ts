/**
 * Cloud storage provider construction and normalization.
 */
import CustomCloudProvider from '../../../cloud-providers/custom-cloud-provider';
import {
  DEFAULT_QCUMBER_BACKEND_PROVIDER,
  DEFAULT_QCUMBER_TOKEN_KEY,
  STATIC_QCUMBER_TOKEN,
  DEFAULT_QSTORAGE_BACKEND_PROVIDER,
  DEFAULT_QSTORAGE_TOKEN_KEY,
  STATIC_QSTORAGE_TOKEN,
  DEFAULT_CLOUD_MAP_PROVIDER
} from './constants';

export function normalizeCloudMapProvider(rawProvider?: string): string {
  const normalized = String(rawProvider || '').trim().toLowerCase();
  if (!normalized) return DEFAULT_CLOUD_MAP_PROVIDER;
  return normalized;
}

export function createCloudStorageProvider(rawProvider?: string) {
  const providerName = normalizeCloudMapProvider(rawProvider);
  if (providerName === DEFAULT_QCUMBER_BACKEND_PROVIDER) {
    const apiBase = String(import.meta.env.VITE_QCUMBER_CLOUD_API_BASE || '').replace(/\/+$/, '');
    if (!apiBase) {
      throw new Error('Q-cumber cloud API base URL is not configured');
    }
    return new CustomCloudProvider({
      name: DEFAULT_QCUMBER_BACKEND_PROVIDER,
      apiBaseUrl: apiBase,
      displayName: import.meta.env.VITE_QCUMBER_CLOUD_DISPLAY_NAME || 'Q-cumber',
      managementUrl: import.meta.env.VITE_QCUMBER_CLOUD_MANAGEMENT_URL,
      staticToken: STATIC_QCUMBER_TOKEN,
      tokenStorageKey: DEFAULT_QCUMBER_TOKEN_KEY
    });
  }

  if (providerName === DEFAULT_QSTORAGE_BACKEND_PROVIDER) {
    const apiBase = String(import.meta.env.VITE_QSTORAGE_CLOUD_API_BASE || '').replace(/\/+$/, '');
    if (!apiBase) {
      throw new Error('Q-storage cloud API base URL is not configured');
    }
    return new CustomCloudProvider({
      name: DEFAULT_QSTORAGE_BACKEND_PROVIDER,
      apiBaseUrl: apiBase,
      displayName: import.meta.env.VITE_QSTORAGE_CLOUD_DISPLAY_NAME || 'My Maps',
      managementUrl: import.meta.env.VITE_QSTORAGE_CLOUD_MANAGEMENT_URL,
      staticToken: STATIC_QSTORAGE_TOKEN,
      tokenStorageKey: DEFAULT_QSTORAGE_TOKEN_KEY,
      privateStorage: true
    });
  }

  throw new Error(`Unsupported cloud provider "${providerName}"`);
}
