/**
 * Q-cumber provider catalog, dataset catalog, help, and routing resolution.
 */
import {
  QCUMBER_PROVIDER_CATALOG_CACHE_TTL_MS,
  getProviderCatalogCache,
  setProviderCatalogCache,
  qcumberDatasetHelpCacheKey,
  hasDatasetHelpInCache,
  getDatasetHelpFromCache,
  setDatasetHelpInCache
} from './qcumber-cache';
import {resolveQCumberProviderId, isInvalidProviderIdLiteral, normalizeDatasetToken} from './qcumber-dataset-identity';
import {normalizeQcumberEnumToken, QCUMBER_EXPECTED_ADMIN_TYPE_ALIASES} from './qcumber-schemas';
import {requestQcumberJson, requestQcumberDatasetHelp} from './qcumber-query-core';

// --- Provider catalog ---

export function pickPreferredProviderIdFromCatalog(
  providerItems: any[],
  preferredProviderId?: string
): {providerId: string; autoSelected: boolean} {
  const ids = (Array.isArray(providerItems) ? providerItems : [])
    .map((item: any) => String(item?.id || '').trim())
    .filter(Boolean);
  const uniqueIds = Array.from(new Set(ids));
  const preferred = resolveQCumberProviderId(preferredProviderId);
  if (preferred && uniqueIds.includes(preferred)) {
    return {providerId: preferred, autoSelected: false};
  }
  // Keep provider selection strict when the caller passed an explicit id:
  // returning a silent fallback can hide provider mismatches and lead to tool loops.
  if (preferred) {
    return {providerId: preferred, autoSelected: false};
  }
  if (uniqueIds.length === 1) {
    return {providerId: uniqueIds[0], autoSelected: true};
  }
  return {providerId: '', autoSelected: false};
}

export async function listQcumberProvidersCatalog(locale?: string, forceRefresh = false): Promise<any[]> {
  const suffix = locale ? `?locale=${encodeURIComponent(String(locale))}` : '';
  const cacheKey = String(locale || '').trim().toLowerCase();
  const now = Date.now();
  const cachedCatalog = getProviderCatalogCache();
  if (!forceRefresh && cachedCatalog && cachedCatalog.key === cacheKey) {
    if (cachedCatalog.expiresAt > now) {
      return cachedCatalog.items;
    }
  }
  const payload = await requestQcumberJson(`/providers${suffix}`);
  const items = Array.isArray(payload?.items) ? payload.items : [];
  setProviderCatalogCache({
    key: cacheKey,
    expiresAt: now + QCUMBER_PROVIDER_CATALOG_CACHE_TTL_MS,
    items
  });
  return items;
}

export async function resolveExistingQCumberProviderId(
  rawProviderId?: string,
  options?: {forceRefresh?: boolean}
): Promise<{providerId: string; autoSelected: boolean; availableProviderIds: string[]}> {
  const invalidLiteral = isInvalidProviderIdLiteral(rawProviderId);
  if (invalidLiteral) {
    try {
      const items = await listQcumberProvidersCatalog(undefined, Boolean(options?.forceRefresh));
      const availableProviderIds = items.map((item: any) => String(item?.id || '').trim()).filter(Boolean);
      return {
        providerId: '',
        autoSelected: false,
        availableProviderIds
      };
    } catch {
      return {
        providerId: '',
        autoSelected: false,
        availableProviderIds: []
      };
    }
  }
  const preferred = resolveQCumberProviderId(rawProviderId);
  try {
    const items = await listQcumberProvidersCatalog(undefined, Boolean(options?.forceRefresh));
    const availableProviderIds = items.map((item: any) => String(item?.id || '').trim()).filter(Boolean);
    const picked = pickPreferredProviderIdFromCatalog(items, preferred);
    return {
      providerId: picked.providerId || preferred,
      autoSelected: picked.autoSelected,
      availableProviderIds
    };
  } catch {
    return {
      providerId: preferred,
      autoSelected: false,
      availableProviderIds: []
    };
  }
}

export async function resolvePreferredTerritorialProviderId(
  rawProviderId: string | undefined,
  _rawDatasetId: unknown,
  fallbackProviderId: string
): Promise<string> {
  const raw = String(rawProviderId || '').trim();
  if (raw) {
    return resolveQCumberProviderId(rawProviderId);
  }
  return String(fallbackProviderId || '').trim();
}

// --- Dataset catalog ---

export async function getQcumberDatasetCatalogItem(providerId: string, datasetId: string): Promise<any | null> {
  const items = await listQcumberDatasetCatalogItems(providerId);
  const match = items.find(
    (item: any) => String(item?.id || '').trim().toLowerCase() === String(datasetId || '').trim().toLowerCase()
  );
  return match || null;
}

export async function listQcumberDatasetCatalogItems(providerId: string): Promise<any[]> {
  const payload = await requestQcumberJson(`/providers/${encodeURIComponent(providerId)}/datasets`);
  return Array.isArray(payload?.items) ? payload.items : [];
}

export function collectCatalogDatasetIds(items: any[]): string[] {
  return (Array.isArray(items) ? items : [])
    .map((item: any) => String(item?.id || '').trim())
    .filter(Boolean);
}

export function formatAvailableDatasetIds(ids: string[], maxItems = 12): string {
  const list = (Array.isArray(ids) ? ids : []).map(id => String(id || '').trim()).filter(Boolean);
  if (!list.length) return '';
  const shown = list.slice(0, maxItems);
  const suffix = list.length > shown.length ? ', ...' : '';
  return `${shown.join(', ')}${suffix}`;
}

export type QcumberDatasetIdResolution = {
  requestedDatasetId: string;
  resolvedDatasetId: string;
  availableDatasetIds: string[];
  exactMatch: boolean;
};

export async function resolveQCumberDatasetId(providerId: string, rawDatasetId: unknown): Promise<QcumberDatasetIdResolution> {
  const raw = String(rawDatasetId || '').trim();
  const items = await listQcumberDatasetCatalogItems(providerId);
  const availableDatasetIds = collectCatalogDatasetIds(items);
  if (!raw) {
    return {
      requestedDatasetId: '',
      resolvedDatasetId: '',
      availableDatasetIds,
      exactMatch: false
    };
  }

  const byId = items.find((item: any) => String(item?.id || '').trim().toLowerCase() === raw.toLowerCase()) || null;
  if (!byId?.id) {
    // Recovery: detect loaded dataset references
    // (pattern: <providerId>-<datasetId>-query-qcumber-query-<ts>-<hash>)
    // or hallucinated refs that contain the loaded-ref suffix.
    const loadedRefMatch = raw.match(/^(.+)-query-qcumber-query-\d+-\w+$/i);
    if (loadedRefMatch) {
      const prefix = loadedRefMatch[1]; // e.g. "natura2000-it-natura2000-siti"
      // Try matching each catalog ID as a suffix of the prefix (after providerId dash).
      const recoveredItem = items.find((item: any) => {
        const catalogId = String(item?.id || '').trim().toLowerCase();
        return catalogId && prefix.toLowerCase().endsWith('-' + catalogId);
      }) || null;
      if (recoveredItem?.id) {
        return {
          requestedDatasetId: raw,
          resolvedDatasetId: String(recoveredItem.id).trim(),
          availableDatasetIds,
          exactMatch: false
        };
      }
      // Fallback: if there is exactly one catalog dataset, auto-select it.
      // The model clearly intended to query this provider's data but mangled
      // the ID from a loaded-ref or provider name.
      if (availableDatasetIds.length === 1) {
        return {
          requestedDatasetId: raw,
          resolvedDatasetId: availableDatasetIds[0],
          availableDatasetIds,
          exactMatch: false
        };
      }
    }
    return {
      requestedDatasetId: raw,
      resolvedDatasetId: '',
      availableDatasetIds,
      exactMatch: false
    };
  }
  return {
    requestedDatasetId: raw,
    resolvedDatasetId: String(byId.id).trim(),
    availableDatasetIds,
    exactMatch: true
  };
}

// --- Dataset routing ---

export type QcumberCatalogDataset = {
  id: string;
  name: string;
  description: string;
  tags: string[];
  routing: any;
};

export const QCUMBER_ROUTING_QUERY_TOOLS = new Set([
  'queryQCumberTerritorialUnits',
  'queryQCumberDatasetSpatial',
  'queryQCumberDataset'
]);

export function getRoutingPreferredQueryTool(routing: any): string {
  const queryToolHint = String(routing?.queryToolHint?.preferredTool || '').trim();
  if (QCUMBER_ROUTING_QUERY_TOOLS.has(queryToolHint)) return queryToolHint;
  return '';
}

export function normalizeMetricProfile(metricProfile: any, sourceFallback = 'backend_routing'): any | null {
  if (!metricProfile || typeof metricProfile !== 'object') return null;
  const asStringList = (value: unknown): string[] =>
    Array.isArray(value) ? value.map(item => String(item || '').trim()).filter(Boolean) : [];
  const asMetricList = (value: unknown): any[] => (Array.isArray(value) ? value.filter(item => item && typeof item === 'object') : []);
  const out: Record<string, unknown> = {
    source: String(metricProfile?.source || '').trim() || sourceFallback,
    confidence: String(metricProfile?.confidence || '').trim() || 'medium',
    metricSemantic: String(metricProfile?.metricSemantic || '').trim() || 'generic',
    biasRisk: String(metricProfile?.biasRisk || '').trim() || 'unknown',
    numeratorFieldCandidates: asStringList(metricProfile?.numeratorFieldCandidates),
    denominatorFieldCandidates: asStringList(metricProfile?.denominatorFieldCandidates),
    preferredRankingFieldCandidates: asStringList(metricProfile?.preferredRankingFieldCandidates),
    recommendedDerivedMetrics: asMetricList(metricProfile?.recommendedDerivedMetrics),
    analysisCaveats: asStringList(metricProfile?.analysisCaveats)
  };
  return out;
}

function inferMetricProfileFromContext(params: {
  aiProfile: any;
  datasetClass: string;
  text: string;
  normalizedFields: string[];
}): any | null {
  const declared = normalizeMetricProfile(params?.aiProfile?.analysisMetrics, 'descriptor_profile');
  if (declared) return declared;
  const text = String(params?.text || '').toLowerCase();
  const fields = Array.isArray(params?.normalizedFields) ? params.normalizedFields : [];
  const isClcLike =
    params?.datasetClass === 'land_cover' ||
    text.includes('clc') ||
    text.includes('land cover') ||
    text.includes('land-cover') ||
    fields.includes('code_18');
  if (!isClcLike) return null;
  return normalizeMetricProfile(
    {
      source: 'inferred_land_cover_proxy',
      confidence: 'medium',
      metricSemantic: 'proxy_environmental_pressure',
      biasRisk: 'absolute_only_bias',
      numeratorFieldCandidates: ['area_ha', 'zonal_value', 'sum', 'sum_area_m2'],
      denominatorFieldCandidates: ['area_region_m2', 'population'],
      preferredRankingFieldCandidates: ['pressure_pct_area', 'pressure_ha_per_100k', 'zonal_value', 'area_ha'],
      recommendedDerivedMetrics: [
        {
          name: 'pressure_pct_area',
          formulaHint: 'numerator_m2 / area_region_m2',
          unit: 'ratio'
        },
        {
          name: 'pressure_ha_per_100k',
          formulaHint: '(numerator_ha / population) * 100000',
          unit: 'ha_per_100k'
        }
      ],
      analysisCaveats: [
        'Proxy based on land-cover classes and not a direct clinical/sanitary risk measure.',
        'Avoid absolute-only ranking when denominator fields are available.'
      ]
    },
    'inferred_land_cover_proxy'
  );
}

export function buildMetricProfileHint(metricProfile: any): string {
  const profile = normalizeMetricProfile(metricProfile);
  if (!profile) return '';
  const semantic = String(profile.metricSemantic || '').trim();
  const bias = String(profile.biasRisk || '').trim();
  const derived = Array.isArray(profile.recommendedDerivedMetrics)
    ? profile.recommendedDerivedMetrics
        .map((item: any) => String(item?.name || '').trim())
        .filter(Boolean)
        .slice(0, 2)
    : [];
  const caveat = Array.isArray(profile.analysisCaveats) ? String(profile.analysisCaveats[0] || '').trim() : '';
  const segments = [
    semantic ? `metricSemantic=${semantic}` : '',
    bias ? `biasRisk=${bias}` : '',
    derived.length ? `derived=${derived.join(',')}` : '',
    caveat ? `caveat=${caveat}` : ''
  ].filter(Boolean);
  return segments.length ? ` Metric profile: ${segments.join('; ')}.` : '';
}

export type QcumberQueryPolicy = 'auto' | 'territorial' | 'thematic_spatial';

export async function listQcumberCatalogDatasetsWithRouting(providerId: string): Promise<QcumberCatalogDataset[]> {
  try {
    const payload = await requestQcumberJson(`/providers/${encodeURIComponent(providerId)}/datasets`);
    const items = Array.isArray(payload?.items) ? payload.items : [];
    return items
      .map((item: any) => ({
        id: String(item?.id || '').trim(),
        name: String(item?.name || '').trim(),
        description: String(item?.description || '').trim(),
        tags: Array.isArray(item?.tags)
          ? item.tags.map((tag: any) => String(tag || '').trim()).filter(Boolean)
          : [],
        routing: pickRoutingFromBackendOrInfer(item)
      }))
      .filter((item: QcumberCatalogDataset) => !!item.id);
  } catch {
    return [];
  }
}

export async function resolveQCumberDatasetIdForQuery(params: {
  providerId: string;
  rawDatasetId: unknown;
  policyMode: QcumberQueryPolicy;
  normalizedExpectedAdminType: string | null;
  spatialBbox: number[] | undefined;
}): Promise<{
  datasetId: string;
  requestedDatasetId: string;
  requestedDatasetIdInvalid: boolean;
  availableDatasetIds: string[];
  autoSelected: boolean;
}> {
  const {providerId, rawDatasetId, policyMode, normalizedExpectedAdminType, spatialBbox} = params;
  const explicitResolution = await resolveQCumberDatasetId(providerId, rawDatasetId);
  if (explicitResolution.requestedDatasetId) {
    if (explicitResolution.resolvedDatasetId) {
      return {
        datasetId: explicitResolution.resolvedDatasetId,
        requestedDatasetId: explicitResolution.requestedDatasetId,
        requestedDatasetIdInvalid: false,
        availableDatasetIds: explicitResolution.availableDatasetIds,
        autoSelected: false
      };
    }
    return {
      datasetId: '',
      requestedDatasetId: explicitResolution.requestedDatasetId,
      requestedDatasetIdInvalid: true,
      availableDatasetIds: explicitResolution.availableDatasetIds,
      autoSelected: false
    };
  }

  const catalogItems = await listQcumberCatalogDatasetsWithRouting(providerId);
  const availableDatasetIds = catalogItems.map(item => String(item?.id || '').trim()).filter(Boolean);
  if (!catalogItems.length) {
    return {
      datasetId: '',
      requestedDatasetId: '',
      requestedDatasetIdInvalid: false,
      availableDatasetIds: [],
      autoSelected: false
    };
  }
  if (catalogItems.length === 1) {
    return {
      datasetId: catalogItems[0].id,
      requestedDatasetId: '',
      requestedDatasetIdInvalid: false,
      availableDatasetIds,
      autoSelected: true
    };
  }

  const wantsAdministrative = policyMode === 'territorial' || !!normalizedExpectedAdminType;
  if (wantsAdministrative) {
    const adminCandidates = catalogItems.filter(item => Boolean(item?.routing?.isAdministrative));
    if (adminCandidates.length === 1) {
      return {
        datasetId: adminCandidates[0].id,
        requestedDatasetId: '',
        requestedDatasetIdInvalid: false,
        availableDatasetIds,
        autoSelected: true
      };
    }
    const territorialPreferred = adminCandidates.filter(
      item => getRoutingPreferredQueryTool(item?.routing) === 'queryQCumberTerritorialUnits'
    );
    if (territorialPreferred.length === 1) {
      return {
        datasetId: territorialPreferred[0].id,
        requestedDatasetId: '',
        requestedDatasetIdInvalid: false,
        availableDatasetIds,
        autoSelected: true
      };
    }
  }

  const hasSpatialBbox = Array.isArray(spatialBbox) && spatialBbox.length === 4;
  const wantsThematicSpatial =
    policyMode === 'thematic_spatial' || (policyMode === 'auto' && hasSpatialBbox && !wantsAdministrative);
  if (wantsThematicSpatial) {
    const thematicCandidates = catalogItems.filter(item => {
      const preferredTool = getRoutingPreferredQueryTool(item?.routing);
      return !item?.routing?.isAdministrative || preferredTool === 'queryQCumberDatasetSpatial';
    });
    if (thematicCandidates.length === 1) {
      return {
        datasetId: thematicCandidates[0].id,
        requestedDatasetId: '',
        requestedDatasetIdInvalid: false,
        availableDatasetIds,
        autoSelected: true
      };
    }
    const thematicSpatialPreferred = thematicCandidates.filter(
      item => getRoutingPreferredQueryTool(item?.routing) === 'queryQCumberDatasetSpatial'
    );
    if (thematicSpatialPreferred.length === 1) {
      return {
        datasetId: thematicSpatialPreferred[0].id,
        requestedDatasetId: '',
        requestedDatasetIdInvalid: false,
        availableDatasetIds,
        autoSelected: true
      };
    }
  }

  return {
    datasetId: '',
    requestedDatasetId: '',
    requestedDatasetIdInvalid: false,
    availableDatasetIds,
    autoSelected: false
  };
}

// --- Dataset help (cached) ---

export async function getQcumberDatasetHelpCached(providerId: string, datasetId: string): Promise<any | null> {
  const key = qcumberDatasetHelpCacheKey(providerId, datasetId);
  if (hasDatasetHelpInCache(key)) {
    return getDatasetHelpFromCache(key) || null;
  }
  try {
    const payload = await requestQcumberDatasetHelp(providerId, datasetId);
    setDatasetHelpInCache(key, payload || null);
    return payload || null;
  } catch {
    setDatasetHelpInCache(key, null);
    return null;
  }
}

// --- Dataset classification and routing inference ---

export function inferQcumberDatasetClass(dataset: any): string {
  return inferQcumberDatasetRouting(dataset).datasetClass;
}

export function hasBackendRoutingMetadata(dataset: any, datasetHelp?: any): boolean {
  const helpRouting = datasetHelp && typeof datasetHelp?.routing === 'object' ? datasetHelp.routing : null;
  const datasetRouting = dataset && typeof dataset?.routing === 'object' ? dataset.routing : null;
  return Boolean(helpRouting || datasetRouting);
}

export function pickRoutingFromBackendOrInfer(dataset: any, datasetHelp?: any) {
  const backendRouting =
    datasetHelp && typeof datasetHelp?.routing === 'object'
      ? datasetHelp.routing
      : dataset && typeof dataset?.routing === 'object'
      ? dataset.routing
      : null;
  if (backendRouting && typeof backendRouting === 'object') {
    const normalizedMetric = normalizeMetricProfile((backendRouting as any)?.metricProfile, 'backend_routing');
    if (normalizedMetric) {
      return {
        ...backendRouting,
        metricProfile: normalizedMetric
      };
    }
    return backendRouting;
  }
  return inferQcumberDatasetRouting(dataset, datasetHelp);
}

function getFieldNamesFromAiHints(aiHints: any): string[] {
  const hints = aiHints && typeof aiHints === 'object' ? aiHints : null;
  const rawCatalog = Array.isArray(hints?.fieldCatalog) ? hints.fieldCatalog : [];
  const out: string[] = [];
  rawCatalog.forEach((item: any) => {
    const name = String(item?.name || '').trim();
    if (name && !out.includes(name)) {
      out.push(name);
    }
  });
  return out;
}

export type CanonicalAdminType = 'country' | 'region' | 'province' | 'municipality';

export const CANONICAL_ADMIN_TYPES = new Set<CanonicalAdminType>(['country', 'region', 'province', 'municipality']);

export function canonicalAdminType(raw: string | null): CanonicalAdminType | null {
  if (!raw) return null;
  if (raw === 'country' || raw === 'stato') return 'country';
  if (raw === 'region' || raw === 'regione') return 'region';
  if (raw === 'province' || raw === 'provincia') return 'province';
  if (raw === 'municipality' || raw === 'comune') return 'municipality';
  return null;
}

export function normalizeExpectedAdminType(raw: unknown): string | null {
  const token = normalizeQcumberEnumToken(raw);
  if (!token) return null;
  const normalized = QCUMBER_EXPECTED_ADMIN_TYPE_ALIASES[token] || token;
  const canonical = canonicalAdminType(normalized);
  return canonical && CANONICAL_ADMIN_TYPES.has(canonical) ? canonical : null;
}

export function parseAdminTypeToLevelMap(raw: unknown): Partial<Record<CanonicalAdminType, number>> {
  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) return {};
  const out: Partial<Record<CanonicalAdminType, number>> = {};
  Object.entries(raw as Record<string, unknown>).forEach(([rawKey, rawValue]) => {
    const canonical = canonicalAdminType(normalizeQcumberEnumToken(rawKey));
    if (!canonical) return;
    const numeric = Number(rawValue);
    if (!Number.isFinite(numeric)) return;
    out[canonical] = Math.trunc(numeric);
  });
  return out;
}

export function resolveAdminTypeToLevelMap(datasetCatalogItem: any, datasetHelp: any, datasetRouting: any) {
  return {
    ...parseAdminTypeToLevelMap(datasetRouting?.adminTypeToLevel),
    ...parseAdminTypeToLevelMap(datasetCatalogItem?.ai?.profile?.adminWorkflows?.adminTypeToLevel),
    ...parseAdminTypeToLevelMap(datasetHelp?.aiHints?.aiProfile?.adminWorkflows?.adminTypeToLevel)
  } as Partial<Record<CanonicalAdminType, number>>;
}

export function resolveExpectedAdminLevel(
  raw: unknown,
  adminTypeToLevel: Partial<Record<CanonicalAdminType, number>>
): number | null {
  const normalized = normalizeExpectedAdminType(raw);
  if (!normalized) return null;
  const canonical = canonicalAdminType(normalized);
  if (!canonical) return null;
  const mapped = adminTypeToLevel[canonical];
  return Number.isFinite(Number(mapped)) ? Math.trunc(Number(mapped)) : null;
}

export function inferExpectedAdminLevelFromAvailableLevels(
  normalizedExpectedAdminType: string | null,
  adminTypeToLevel: Partial<Record<CanonicalAdminType, number>>,
  levelCounts: Record<string, number> | undefined,
  fallbackLevel: number | null
): number | null {
  const canonical = canonicalAdminType(normalizedExpectedAdminType);
  if (!canonical) return fallbackLevel;
  const mapped = adminTypeToLevel[canonical];
  if (Number.isFinite(Number(mapped))) {
    return Math.trunc(Number(mapped));
  }
  void levelCounts;
  return fallbackLevel;
}

export function inferQcumberDatasetRouting(dataset: any, datasetHelp?: any) {
  const id = String(dataset?.id || datasetHelp?.datasetId || '').toLowerCase();
  const name = String(dataset?.name || datasetHelp?.datasetName || '').toLowerCase();
  const description = String(dataset?.description || '').toLowerCase();
  const tags = (Array.isArray(dataset?.tags) ? dataset.tags : []).map((tag: any) => String(tag || '').toLowerCase());
  const aiHints = datasetHelp?.aiHints || dataset?.aiHints || null;
  const aiProfile = aiHints && typeof aiHints?.aiProfile === 'object' ? aiHints.aiProfile : null;
  const fields = getFieldNamesFromAiHints(aiHints);
  const normalizedFields = fields.map(field => field.toLowerCase());
  const geometryFields = Array.isArray(aiHints?.geometryFields)
    ? aiHints.geometryFields.map((field: any) => String(field || '')).filter(Boolean)
    : [];
  const numericFields = Array.isArray(aiHints?.numericFields)
    ? aiHints.numericFields.map((field: any) => String(field || '')).filter(Boolean)
    : [];
  const text = `${id} ${name} ${description} ${tags.join(' ')} ${normalizedFields.join(' ')}`;

  const levelFieldCandidates = fields.filter(field => {
    const normalized = field.toLowerCase();
    return normalized === 'lv' || normalized.endsWith('__lv') || normalized.endsWith('_lv');
  });
  const parentIdFieldCandidates = fields.filter(field => {
    const normalized = field.toLowerCase();
    return normalized.includes('__lv') && normalized.endsWith('_id');
  });
  if (aiProfile && typeof aiProfile?.adminWorkflows === 'object') {
    const parentIdFields = (aiProfile.adminWorkflows as any)?.parentIdFields;
    if (Array.isArray(parentIdFields)) {
      parentIdFields.forEach((fieldName: any) => {
        const candidate = String(fieldName || '').trim();
        if (candidate && !parentIdFieldCandidates.includes(candidate)) {
          parentIdFieldCandidates.push(candidate);
        }
      });
    }
  }
  const nameFieldCandidates = fields.filter(field => {
    const normalized = field.toLowerCase();
    return normalized === 'name' || normalized === 'name_en' || normalized.endsWith('_name');
  });

  const hasAdministrativeSignals =
    levelFieldCandidates.length > 0 ||
    parentIdFieldCandidates.length > 0 ||
    text.includes('administrative') ||
    text.includes('confini') ||
    text.includes('province') ||
    text.includes('provincia') ||
    text.includes('municipalit') ||
    text.includes('comune') ||
    text.includes('region') ||
    text.includes('regione') ||
    text.includes('kontur') ||
    tags.includes('boundaries');

  let datasetClass = 'other';
  if (hasAdministrativeSignals) {
    datasetClass = 'administrative';
  } else if (text.includes('event')) {
    datasetClass = 'events';
  } else if (text.includes('feature')) {
    datasetClass = 'features';
  } else if (text.includes('land-cover') || text.includes('land cover') || text.includes('clc')) {
    datasetClass = 'land_cover';
  }
  const profileDatasetClass = String(aiProfile?.datasetClass || '').trim().toLowerCase();
  if (profileDatasetClass) {
    datasetClass = profileDatasetClass;
  }

  const queryRouting = aiProfile && typeof aiProfile?.queryRouting === 'object' ? aiProfile.queryRouting : null;
  const adminWorkflows = aiProfile && typeof aiProfile?.adminWorkflows === 'object' ? aiProfile.adminWorkflows : null;
  const adminTypeToLevel = parseAdminTypeToLevelMap(adminWorkflows?.adminTypeToLevel);
  const profilePreferredToolRaw = String(aiProfile?.queryRouting?.preferredTool || '').trim();
  const profilePreferredTool = QCUMBER_ROUTING_QUERY_TOOLS.has(profilePreferredToolRaw) ? profilePreferredToolRaw : '';
  const inferredPreferredQueryTool =
    hasAdministrativeSignals
      ? 'queryQCumberTerritorialUnits'
      : geometryFields.length
      ? 'queryQCumberDatasetSpatial'
      : 'queryQCumberDataset';
  const preferredQueryTool =
    profilePreferredTool ||
    inferredPreferredQueryTool;
  const parseLooseBool = (raw: unknown, fallback: boolean): boolean => {
    if (typeof raw === 'boolean') return raw;
    if (typeof raw === 'number') return raw !== 0;
    if (typeof raw === 'string') {
      const normalized = raw.trim().toLowerCase();
      if (['1', 'true', 'yes', 'y', 'on'].includes(normalized)) return true;
      if (['0', 'false', 'no', 'n', 'off'].includes(normalized)) return false;
    }
    return fallback;
  };
  const requiresSpatialBbox = parseLooseBool(
    queryRouting?.requiresSpatialBbox,
    preferredQueryTool === 'queryQCumberDatasetSpatial' && !hasAdministrativeSignals
  );
  const expectedAdminTypeSupported = parseLooseBool(queryRouting?.expectedAdminTypeSupported, hasAdministrativeSignals);
  const forbiddenAdminConstraints = Array.isArray(queryRouting?.forbiddenAdminConstraints)
    ? queryRouting.forbiddenAdminConstraints.map((item: any) => String(item || '').trim()).filter(Boolean)
    : [];
  const queryToolHintSource = profilePreferredTool
    ? 'descriptor_profile'
    : hasAdministrativeSignals
    ? 'inferred_admin_signals'
    : geometryFields.length
    ? 'inferred_geometry'
    : 'inferred_fallback';
  const queryToolHintConfidence = profilePreferredTool
    ? 'high'
    : hasAdministrativeSignals || geometryFields.length
    ? 'medium'
    : 'low';
  const queryToolHintReason = profilePreferredTool
    ? `Preferred tool provided by dataset descriptor profile (${profilePreferredTool}).`
    : hasAdministrativeSignals
    ? 'Administrative signals detected from fields/tags/text.'
    : geometryFields.length
    ? 'Geometry fields detected in dataset metadata.'
    : 'No administrative/spatial signal; fallback to generic query tool.';
  const suggestedOps = Array.isArray(aiHints?.suggestedOps) ? aiHints.suggestedOps : [];
  const metricProfile = inferMetricProfileFromContext({
    aiProfile,
    datasetClass,
    text,
    normalizedFields
  });

  return {
    datasetClass,
    isAdministrative: hasAdministrativeSignals,
    queryToolHint: {
      preferredTool: preferredQueryTool,
      confidence: queryToolHintConfidence,
      source: queryToolHintSource,
      reason: queryToolHintReason,
      requiresSpatialBbox,
      expectedAdminTypeSupported,
      forbiddenAdminConstraints
    },
    adminTypeToLevel,
    levelFieldCandidates,
    parentIdFieldCandidates,
    nameFieldCandidates,
    geometryFields,
    numericFields,
    suggestedOps,
    metricProfile
  };
}
