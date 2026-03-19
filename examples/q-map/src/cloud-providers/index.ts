// SPDX-License-Identifier: MIT
// Copyright contributors to the kepler.gl project

import {Provider} from '@kepler.gl/cloud-providers';
import CustomCloudProvider from './custom-cloud-provider';

function getLocalizedQStorageDisplayName(): string {
  const lang =
    (typeof document !== 'undefined' && document.documentElement?.lang) ||
    (typeof navigator !== 'undefined' && navigator.language) ||
    'en';
  return String(lang).toLowerCase().startsWith('it') ? 'Le mie mappe' : 'My Maps';
}

export function getQMapCloudProviders(): Provider[] {
  const providers: Provider[] = [];

  const addProvider = ({
    name,
    apiBaseUrl,
    displayName,
    managementUrl,
    staticToken,
    privateStorage,
    tokenStorageKey
  }: {
    name: string;
    apiBaseUrl?: string;
    displayName: string;
    managementUrl?: string;
    staticToken?: string;
    privateStorage?: boolean;
    tokenStorageKey: string;
  }) => {
    if (!apiBaseUrl?.trim()) {
      return;
    }

    providers.push(
      new CustomCloudProvider({
        name,
        apiBaseUrl: apiBaseUrl.trim(),
        displayName,
        managementUrl,
        staticToken,
        privateStorage,
        tokenStorageKey
      }) as unknown as Provider
    );
  };

  addProvider({
    name: 'q-cumber-backend',
    apiBaseUrl: import.meta.env.VITE_QCUMBER_CLOUD_API_BASE,
    displayName: import.meta.env.VITE_QCUMBER_CLOUD_DISPLAY_NAME || 'Q-cumber',
    managementUrl: import.meta.env.VITE_QCUMBER_CLOUD_MANAGEMENT_URL,
    staticToken: import.meta.env.VITE_QCUMBER_CLOUD_TOKEN,
    privateStorage: false,
    tokenStorageKey: 'qmap_qcumber_backend_token'
  });

  addProvider({
    name: 'q-storage-backend',
    apiBaseUrl: import.meta.env.VITE_QSTORAGE_CLOUD_API_BASE,
    displayName: import.meta.env.VITE_QSTORAGE_CLOUD_DISPLAY_NAME || getLocalizedQStorageDisplayName(),
    managementUrl: import.meta.env.VITE_QSTORAGE_CLOUD_MANAGEMENT_URL,
    staticToken: import.meta.env.VITE_QSTORAGE_CLOUD_TOKEN,
    privateStorage: true,
    tokenStorageKey: 'qmap_qstorage_backend_token'
  });

  return providers;
}
