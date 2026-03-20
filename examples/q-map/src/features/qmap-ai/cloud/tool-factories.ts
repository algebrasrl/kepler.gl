/**
 * Cloud tool factory functions that build the tool definitions for the AI tool registry.
 */
import {z} from 'zod';
import {callMcpToolParsed} from '../mcp-client';
import {normalizeCloudMapProvider} from './cloud-providers';
import {QCUMBER_PROVIDER_ROUTING_HINTS, ITALY_DEFAULT_SPATIAL_BBOX, QCUMBER_MAX_AUTO_LAYER_GEOMETRY_ROWS, QCUMBER_INCLUDE_LATLON_FALLBACK_FIELDS} from './constants';
import {
  NON_EMPTY_STRING_SCHEMA,
  OPTIONAL_NON_EMPTY_STRING_SCHEMA,
  QCUMBER_ORDER_DIRECTION_SCHEMA,
  QCUMBER_EXPECTED_ADMIN_TYPE_SCHEMA,
  QCUMBER_FILTER_SCHEMA,
  QCUMBER_FILTER_OPS,
  normalizeQcumberFilterOp
} from './qcumber-schemas';
import {
  resolveQCumberProviderId,
  isInvalidProviderIdLiteral,
  rebuildQcumberMapDatasetRows,
  deriveDatasetIdentity,
  withUniqueMapDatasetIdentity
} from './qcumber-dataset-identity';
import {
  getCachedSuccessfulQcumberQuery,
  setCachedSuccessfulQcumberQuery,
  stableSerializeForCache,
  qcumberDatasetHelpCacheKey,
  setDatasetHelpInCache
} from './qcumber-cache';
import {
  listQcumberProvidersCatalog,
  resolveExistingQCumberProviderId,
  resolvePreferredTerritorialProviderId,
  getQcumberDatasetCatalogItem,
  formatAvailableDatasetIds,
  resolveQCumberDatasetId,
  resolveQCumberDatasetIdForQuery,
  getQcumberDatasetHelpCached,
  inferQcumberDatasetClass,
  hasBackendRoutingMetadata,
  pickRoutingFromBackendOrInfer,
  getRoutingPreferredQueryTool,
  normalizeMetricProfile,
  buildMetricProfileHint,
  inferQcumberDatasetRouting,
  normalizeExpectedAdminType,
  resolveAdminTypeToLevelMap,
  resolveExpectedAdminLevel,
  inferExpectedAdminLevelFromAvailableLevels
} from './qcumber-catalog';
import type {QcumberQueryPolicy} from './qcumber-catalog';
import {
  requestQcumberJson,
  requestQcumberDatasetHelp,
  isFilterValueLikelyFieldName,
  compactQcumberPreviewRows,
  buildRankingPreviewDetails,
  validateRankingMetric,
  isGeometryLikeRankingField,
  collectMetadataRankingFieldCandidates,
  pickBestMetadataRankingCandidate,
  resolveFallbackRankingOrderBy,
  inferQcumberH3Metadata,
  inferQcumberAdminMetadata,
  normalizeSpatialBboxInput,
  ensureGeojsonPointsFromLatLon,
  datasetHasRenderableGeometry,
  verifyRowsAgainstFilters,
  isLikelyMunicipalFilter,
  hasGenericParentIdFilter,
  isMissingFilterFieldError,
  rewriteGenericParentIdFilters,
  collectParentIdRetryFieldCandidates,
  isLikelyAdminLevelFieldName,
  valuesEqualAdminLevel,
  extractExactNameFilterValues
} from './qcumber-query-core';
import {LoadQMapCloudMapComponent, QueryQCumberDatasetComponent} from './components';

// --- Cloud map tools ---

async function listMapsFromCloud(apiBaseUrl: string, provider?: string) {
  const resolvedProvider = normalizeCloudMapProvider(provider);
  const payload = await callMcpToolParsed(apiBaseUrl, ['list_qmap_cloud_maps'], {provider: resolvedProvider});

  const items = payload?.maps;
  if (!Array.isArray(items)) {
    return [];
  }

  return items.map((item: any) => ({
    id: String(item?.id || item?.mapId || ''),
    title: String(item?.title || item?.name || 'Untitled map'),
    description: String(item?.description || '')
  }));
}

export function createListQMapCloudMapsTool(apiBaseUrl: string) {
  return {
    description: 'List available maps from q-map cloud storage provider.',
    parameters: z
      .object({
        provider: OPTIONAL_NON_EMPTY_STRING_SCHEMA
      })
      .strict(),
    execute: async ({provider}: any) => {
      const resolvedProvider = normalizeCloudMapProvider(provider);
      const maps = await listMapsFromCloud(apiBaseUrl, resolvedProvider);
      return {
        llmResult: {
          success: true,
          provider: resolvedProvider,
          maps,
          details: maps.length
            ? `Found ${maps.length} cloud maps in provider ${resolvedProvider}.`
            : `No cloud maps were found for current account in provider ${resolvedProvider}.`
        }
      };
    }
  };
}

export function createLoadQMapCloudMapTool(apiBaseUrl: string) {
  return {
    description:
      'Load a map from q-map cloud storage into q-hive. Provide mapId from listQMapCloudMaps.',
    parameters: z
      .object({
        provider: OPTIONAL_NON_EMPTY_STRING_SCHEMA,
        mapId: NON_EMPTY_STRING_SCHEMA
      })
      .strict(),
    execute: async ({provider, mapId}: any) => {
      const resolvedProvider = normalizeCloudMapProvider(provider);
      const resolvedId = (mapId || '').trim();

      if (!resolvedId) {
        return {
          llmResult: {
            success: false,
            details: 'Missing mapId. Call listQMapCloudMaps first and pass a valid mapId.'
          }
        };
      }

      const actionPayload = await callMcpToolParsed(
        apiBaseUrl,
        ['build_load_cloud_map_action'],
        {
          provider: resolvedProvider,
          mapId: resolvedId
        }
      );

      const args = actionPayload?.action?.args || {};
      const providerName = normalizeCloudMapProvider(String(args.provider || resolvedProvider));
      const actionMapId = String(args.mapId || resolvedId);

      return {
        llmResult: {
          success: true,
          details: `Loading cloud map ${actionMapId}...`,
          mapId: actionMapId
        },
        additionalData: {
          provider: providerName,
          loadParams: {
            id: actionMapId,
            path: `/maps/${actionMapId}`
          }
        }
      };
    },
    component: LoadQMapCloudMapComponent
  };
}

// --- Q-Cumber provider/dataset tools ---

export function createListQCumberProvidersTool() {
  return {
    description: 'List available dataset providers from q-cumber backend (e.g. q-cumber, ckan, esri).',
    parameters: z
      .object({
        locale: OPTIONAL_NON_EMPTY_STRING_SCHEMA.describe('Optional locale (e.g. it, en)')
      })
      .strict(),
    execute: async ({locale}: any) => {
      try {
        const items = await listQcumberProvidersCatalog(locale, true);
        const providers = items.map((item: any) => ({
          id: String(item?.id || ''),
          name: String(item?.name || item?.id || ''),
          locale: String(item?.locale || ''),
          category: String(item?.category || ''),
          apiType: String(item?.apiType || ''),
          apiBaseUrl: String(item?.apiBaseUrl || ''),
          capabilities: Array.isArray(item?.capabilities) ? item.capabilities : [],
          formats: Array.isArray(item?.formats) ? item.formats : [],
          tags: Array.isArray(item?.tags) ? item.tags : [],
          routingHint: QCUMBER_PROVIDER_ROUTING_HINTS[String(item?.id || '')] || '',
          helperTools: ['listQCumberDatasets', 'getQCumberDatasetHelp']
        }));
        return {
          llmResult: {
            success: true,
            providers,
            details: providers.length
              ? `Found ${providers.length} data providers.`
              : 'No data providers found.'
          }
        };
      } catch (error: any) {
        return {
          llmResult: {
            success: false,
            providers: [],
            details: `Failed to list providers: ${String(error?.message || error || 'Unknown error')}`
          }
        };
      }
    }
  };
}

export function createListQCumberDatasetsTool() {
  return {
    description: 'List datasets available from q-cumber dynamic provider catalog.',
    parameters: z
      .object({
        providerId: OPTIONAL_NON_EMPTY_STRING_SCHEMA
      })
      .strict(),
    execute: async ({providerId}: any) => {
      if (isInvalidProviderIdLiteral(providerId)) {
        const providerSelection = await resolveExistingQCumberProviderId(undefined, {forceRefresh: true});
        return {
          llmResult: {
            success: false,
            providerId: String(providerId || ''),
            datasets: [],
            details:
              `Invalid providerId "${String(providerId || '')}". ` +
              `Use an exact provider id from listQCumberProviders (e.g. ${providerSelection.availableProviderIds.join(', ') || 'local-assets-it'}).`
          }
        };
      }
      const providerSelection = await resolveExistingQCumberProviderId(providerId);
      const resolvedProviderId = providerSelection.providerId;
      if (!resolvedProviderId) {
        const availableHint = providerSelection.availableProviderIds.length
          ? ` Available providers: ${providerSelection.availableProviderIds.join(', ')}.`
          : '';
        return {
          llmResult: {
            success: false,
            providerId: '',
            datasets: [],
            details:
              `Missing providerId. Call listQCumberProviders and pass a valid providerId.${availableHint}`
          }
        };
      }
      try {
        const payload = await requestQcumberJson(`/providers/${encodeURIComponent(resolvedProviderId)}/datasets`);
        const items = Array.isArray(payload?.items) ? payload.items : [];
        const datasets = items.map((item: any) => {
          const routing = pickRoutingFromBackendOrInfer(item);
          const preferredQueryTool = getRoutingPreferredQueryTool(routing);
          const metricProfile = normalizeMetricProfile(routing?.metricProfile);
          return {
            id: String(item?.id || ''),
            name: String(item?.name || item?.id || ''),
            description: String(item?.description || ''),
            format: String(item?.format || ''),
            tags: Array.isArray(item?.tags) ? item.tags : [],
            aiHints: item?.aiHints || null,
            routingClass: String(routing?.datasetClass || inferQcumberDatasetClass(item) || 'other'),
            routing,
            metricProfile,
            helpAvailable: true
          };
        });

        return {
          llmResult: {
            success: true,
            providerId: resolvedProviderId,
            datasets,
            routingHint: QCUMBER_PROVIDER_ROUTING_HINTS[resolvedProviderId] || '',
            details: datasets.length
              ? `Found ${datasets.length} datasets in provider ${resolvedProviderId}.${providerSelection.autoSelected ? ' (provider auto-selected)' : ''}`
              : `No datasets found for provider ${resolvedProviderId}.`
          }
        };
      } catch (error: any) {
        return {
          llmResult: {
            success: false,
            providerId: resolvedProviderId,
            datasets: [],
            details: `Failed to list datasets for provider ${resolvedProviderId}: ${String(
              error?.message || error || 'Unknown error'
            )}.${providerSelection.availableProviderIds.length ? ` Available providers: ${providerSelection.availableProviderIds.join(', ')}.` : ''}`
          }
        };
      }
    }
  };
}

export function createGetQCumberDatasetHelpTool() {
  return {
    description:
      'Get backend AI help/metadata for a specific q-cumber dataset. Use this to choose the right query tool and field-level routing before querying.',
    parameters: z
      .object({
        providerId: NON_EMPTY_STRING_SCHEMA,
        datasetId: NON_EMPTY_STRING_SCHEMA
      })
      .strict(),
    execute: async ({providerId, datasetId}: any) => {
      if (isInvalidProviderIdLiteral(providerId)) {
        const providerSelection = await resolveExistingQCumberProviderId(undefined, {forceRefresh: true});
        return {
          llmResult: {
            success: false,
            providerId: String(providerId || ''),
            datasetId: String(datasetId || ''),
            details:
              `Invalid providerId "${String(providerId || '')}". ` +
              `Use an exact provider id from listQCumberProviders (e.g. ${providerSelection.availableProviderIds.join(', ') || 'local-assets-it'}).`
          }
        };
      }
      const providerSelection = await resolveExistingQCumberProviderId(providerId);
      const resolvedProviderId = providerSelection.providerId || resolveQCumberProviderId(providerId);
      const datasetResolution = await resolveQCumberDatasetId(resolvedProviderId, datasetId);
      const resolvedDatasetId = datasetResolution.resolvedDatasetId;
      if (!resolvedDatasetId) {
        const availableDatasets = formatAvailableDatasetIds(datasetResolution.availableDatasetIds);
        return {
          llmResult: {
            success: false,
            providerId: resolvedProviderId,
            datasetId: String(datasetResolution.requestedDatasetId || datasetId || ''),
            details:
              `Invalid datasetId "${String(datasetResolution.requestedDatasetId || datasetId || '').trim()}". ` +
              'Use an exact datasetId from listQCumberDatasets(providerId).' +
              (availableDatasets ? ` Available datasetIds: ${availableDatasets}.` : '')
          }
        };
      }
      let catalogItem: any | null = null;
      try {
        catalogItem = await getQcumberDatasetCatalogItem(resolvedProviderId, resolvedDatasetId);
      } catch {
        catalogItem = null;
      }
      try {
        const help = await requestQcumberDatasetHelp(resolvedProviderId, resolvedDatasetId);
        const cacheKey = qcumberDatasetHelpCacheKey(resolvedProviderId, resolvedDatasetId);
        setDatasetHelpInCache(cacheKey, help || null);
        const routing = pickRoutingFromBackendOrInfer(catalogItem || {id: resolvedDatasetId}, help || null);
        const metricProfile = normalizeMetricProfile(routing?.metricProfile);
        return {
          llmResult: {
            success: true,
            providerId: resolvedProviderId,
            datasetId: resolvedDatasetId,
            datasetName: String(help?.datasetName || catalogItem?.name || resolvedDatasetId),
            aiHints: help?.aiHints || null,
            routing,
            metricProfile,
            details:
              `Loaded dataset help for ${resolvedDatasetId}. ` +
              `Preferred query tool: ${getRoutingPreferredQueryTool(routing) || 'queryQCumberDataset'}.` +
              `${buildMetricProfileHint(metricProfile)}${providerSelection.autoSelected ? ' (provider auto-selected)' : ''}`
          }
        };
      } catch (error: any) {
        const routing = pickRoutingFromBackendOrInfer(catalogItem || {id: resolvedDatasetId}, null);
        const metricProfile = normalizeMetricProfile(routing?.metricProfile);
        const hasUsableRouting = Boolean(
          routing &&
            (routing.isAdministrative !== undefined ||
              routing.queryToolHint?.preferredTool ||
              routing.datasetClass)
        );
        const errorNote = `Help endpoint unavailable (${String(
          error?.message || error || 'Unknown error'
        ).slice(0, 80)}); routing inferred from catalog.`;
        return {
          llmResult: {
            success: hasUsableRouting,
            providerId: resolvedProviderId,
            datasetId: resolvedDatasetId,
            routing,
            metricProfile,
            details: hasUsableRouting
              ? `${errorNote} Preferred query tool: ${getRoutingPreferredQueryTool(routing) || 'queryQCumberDataset'}.${buildMetricProfileHint(metricProfile)}`
              : `Failed to load dataset help for ${resolvedDatasetId}: ${String(
                  error?.message || error || 'Unknown error'
                )}.${providerSelection.availableProviderIds.length ? ` Available providers: ${providerSelection.availableProviderIds.join(', ')}.` : ''}`
          }
        };
      }
    }
  };
}

// --- Query tool ---

export function createQueryQCumberDatasetTool(policyMode: QcumberQueryPolicy = 'auto') {
  const isTerritorialOnly = policyMode === 'territorial';
  const isSpatialOnly = policyMode === 'thematic_spatial';
  const description = isTerritorialOnly
    ? 'Query territorial/admin-unit datasets from q-cumber backend (boundaries/levels).'
    : isSpatialOnly
    ? 'Query thematic datasets from q-cumber backend. Prefer spatialBbox to constrain extent.'
    : 'Run a read-only query on q-cumber dataset and optionally load result into map.';
  const commonParameterShape = {
    providerId: OPTIONAL_NON_EMPTY_STRING_SCHEMA,
    datasetId: z
      .string()
      .trim()
      .min(1)
      .optional()
      .describe(
        'Exact datasetId from listQCumberDatasets(providerId). If omitted, the tool attempts deterministic auto-selection from provider catalog metadata.'
      ),
    filters: z.array(QCUMBER_FILTER_SCHEMA).max(32).optional(),
    orderBy: OPTIONAL_NON_EMPTY_STRING_SCHEMA,
    orderDirection: QCUMBER_ORDER_DIRECTION_SCHEMA,
    limit: z.number().int().min(1).max(100000).optional(),
    offset: z.number().int().min(0).optional(),
    loadToMap: z.boolean().optional(),
    showOnMap: z
      .boolean()
      .optional()
      .describe('When loadToMap=true, controls layer visibility. Default false (dataset loaded, layer hidden).')
  };
  const expectedAdminTypeSchema = QCUMBER_EXPECTED_ADMIN_TYPE_SCHEMA.describe(
    'Optional strict administrative guardrail. Enforces expected level (e.g. comune/municipality -> lv=9).'
  );
  const spatialBboxSchema = z
    .tuple([z.number(), z.number(), z.number(), z.number()])
    .optional()
    .describe('Optional bbox [minLon,minLat,maxLon,maxLat] in EPSG:4326 for backend prefilter.');
  const parameters = isTerritorialOnly
    ? z
        .object({
          ...commonParameterShape,
          expectedAdminType: expectedAdminTypeSchema
        })
        .strict()
    : isSpatialOnly
    ? z
        .object({
          ...commonParameterShape,
          spatialBbox: spatialBboxSchema
        })
        .strict()
    : z
        .object({
          ...commonParameterShape,
          spatialBbox: spatialBboxSchema,
          expectedAdminType: expectedAdminTypeSchema,
          inferPointsFromLatLon: z
            .boolean()
            .optional()
            .describe('Default false. If true and geometry is missing, build point geometry from lat/lon columns.')
        })
        .strict();

  return {
    description,
    parameters,
    execute: async (args: any) => {
      const {
        providerId,
        datasetId,
        filters,
        orderBy,
        orderDirection,
        limit,
        offset,
        spatialBbox,
        loadToMap,
        showOnMap,
        expectedAdminType,
        inferPointsFromLatLon
      } = args || {};
      if (isInvalidProviderIdLiteral(providerId)) {
        const providerSelection = await resolveExistingQCumberProviderId(undefined, {forceRefresh: true});
        return {
          llmResult: {
            success: false,
            providerId: String(providerId || ''),
            requestedDatasetId: String(datasetId || '').trim() || undefined,
            details:
              `Invalid providerId "${String(providerId || '')}". ` +
              `Use an exact provider id from listQCumberProviders (e.g. ${providerSelection.availableProviderIds.join(', ') || 'local-assets-it'}).`
          }
        };
      }
      const requestedDatasetId = String(datasetId || '').trim();
      const requestedProviderId = String(providerId || '').trim();
      const normalizedExpectedAdminTypeInput = normalizeExpectedAdminType(expectedAdminType);
      const normalizedExpectedAdminType = isSpatialOnly ? null : normalizedExpectedAdminTypeInput;
      let queryPolicyAdjustmentsNote =
        isSpatialOnly && normalizedExpectedAdminTypeInput
          ? ` expectedAdminType "${normalizedExpectedAdminTypeInput}" ignored for thematic spatial query.`
          : '';
      const normalizedSpatialBbox = normalizeSpatialBboxInput(spatialBbox);
      const providerSelection = await resolveExistingQCumberProviderId(providerId);
      let resolvedProviderId = await resolvePreferredTerritorialProviderId(
        providerId,
        requestedDatasetId,
        providerSelection.providerId || resolveQCumberProviderId(providerId)
      );
      if (
        providerSelection.availableProviderIds.length &&
        resolvedProviderId &&
        !providerSelection.availableProviderIds.includes(resolvedProviderId)
      ) {
        resolvedProviderId = providerSelection.providerId || resolvedProviderId;
      }
      const resolveDatasetIdForProvider = async (providerIdCandidate: string) =>
        resolveQCumberDatasetIdForQuery({
          providerId: providerIdCandidate,
          rawDatasetId: datasetId,
          policyMode,
          normalizedExpectedAdminType,
          spatialBbox: normalizedSpatialBbox
        });
      let resolvedDatasetId = '';
      let datasetResolution: {
        datasetId: string;
        requestedDatasetId: string;
        requestedDatasetIdInvalid: boolean;
        availableDatasetIds: string[];
        autoSelected: boolean;
      } = {
        datasetId: '',
        requestedDatasetId: '',
        requestedDatasetIdInvalid: false,
        availableDatasetIds: [],
        autoSelected: false
      };
      let datasetResolutionError: unknown = null;
      try {
        datasetResolution = await resolveDatasetIdForProvider(resolvedProviderId);
        resolvedDatasetId = datasetResolution.datasetId;
      } catch (error) {
        datasetResolutionError = error;
      }
      const providerResolutionChanged =
        !!requestedProviderId &&
        !!resolvedProviderId &&
        requestedProviderId.toLowerCase() !== resolvedProviderId.toLowerCase();
      const providerAutoSelected = !requestedProviderId && !!resolvedProviderId;
      const providerResolutionNote = providerResolutionChanged
        ? ` Provider resolved from "${requestedProviderId}" to "${resolvedProviderId}".`
        : providerAutoSelected && providerSelection.autoSelected
        ? ` Provider auto-selected as "${resolvedProviderId}".`
        : '';
      if (datasetResolutionError) {
        return {
          llmResult: {
            success: false,
            providerId: resolvedProviderId || resolveQCumberProviderId(providerId),
            requestedDatasetId: requestedDatasetId || undefined,
            details:
              `Failed to resolve provider/dataset before query: ${String(
                (datasetResolutionError as any)?.message || datasetResolutionError || 'Unknown error'
              )}.${providerResolutionNote}` +
              (providerSelection.availableProviderIds.length
                ? ` Available providers: ${providerSelection.availableProviderIds.join(', ')}.`
                : '')
          }
        };
      }
      if (datasetResolution.requestedDatasetIdInvalid) {
        const availableDatasets = formatAvailableDatasetIds(datasetResolution.availableDatasetIds);
        return {
          llmResult: {
            success: false,
            providerId: resolvedProviderId,
            requestedDatasetId: requestedDatasetId || undefined,
            details:
              `Invalid datasetId "${datasetResolution.requestedDatasetId}". ` +
              'Use an exact datasetId from listQCumberDatasets(providerId).' +
              (availableDatasets ? ` Available datasetIds: ${availableDatasets}.` : '') +
              providerResolutionNote
          }
        };
      }
      if (!resolvedDatasetId) {
        const availableDatasets = formatAvailableDatasetIds(datasetResolution.availableDatasetIds);
        return {
          llmResult: {
            success: false,
            providerId: resolvedProviderId,
            requestedDatasetId: requestedDatasetId || undefined,
            details:
              `Missing datasetId and unable to auto-select a unique dataset for provider "${resolvedProviderId}". ` +
              `Call listQCumberDatasets(providerId), then pass datasetId explicitly (optionally after getQCumberDatasetHelp).` +
              (availableDatasets ? ` Available datasetIds: ${availableDatasets}.` : '') +
              providerResolutionNote
          }
        };
      }
      let datasetCatalogItem: any | null = null;
      let datasetHelp: any | null = null;
      let datasetRouting = inferQcumberDatasetRouting({id: resolvedDatasetId});
      let datasetIsTerritorial = false;
      try {
        datasetCatalogItem = await getQcumberDatasetCatalogItem(resolvedProviderId, resolvedDatasetId);
      } catch {
        datasetCatalogItem = null;
      }
      datasetHelp = await getQcumberDatasetHelpCached(resolvedProviderId, resolvedDatasetId);
      datasetRouting = pickRoutingFromBackendOrInfer(datasetCatalogItem || {id: resolvedDatasetId}, datasetHelp);
      const hasBackendRouting = hasBackendRoutingMetadata(datasetCatalogItem, datasetHelp);
      const hasAdministrativeRoutingSignal = hasBackendRouting && typeof datasetRouting?.isAdministrative === 'boolean';
      if ((isTerritorialOnly || !!normalizedExpectedAdminType) && !hasAdministrativeRoutingSignal) {
        return {
          llmResult: {
            success: false,
            providerId: resolvedProviderId,
            datasetId: resolvedDatasetId,
            routing: datasetRouting,
            details:
              `Cannot enforce territorial/admin routing for dataset "${resolvedDatasetId}" because backend routing metadata is missing or incomplete. ` +
              'Call getQCumberDatasetHelp(providerId,datasetId) and retry with a dataset that exposes routing.isAdministrative.'
          }
        };
      }
      datasetIsTerritorial = hasAdministrativeRoutingSignal ? Boolean(datasetRouting?.isAdministrative) : false;
      if (isTerritorialOnly && !datasetIsTerritorial) {
        const preferredRetryTool = getRoutingPreferredQueryTool(datasetRouting);
        const retryTool =
          preferredRetryTool && preferredRetryTool !== 'queryQCumberTerritorialUnits'
            ? preferredRetryTool
            : 'queryQCumberDataset';
        const retryFilters = (Array.isArray(filters) ? filters : []).filter(
          filterItem => !isLikelyAdminLevelFieldName((filterItem as any)?.field)
        );
        return {
          llmResult: {
            success: false,
            providerId: resolvedProviderId,
            datasetId: resolvedDatasetId,
            routing: datasetRouting,
            retryWithTool: retryTool,
            retryWithArgs: {
              providerId: resolvedProviderId,
              datasetId: resolvedDatasetId,
              filters: retryFilters.length ? retryFilters : undefined,
              orderBy,
              orderDirection,
              limit,
              offset,
              spatialBbox: normalizedSpatialBbox,
              loadToMap,
              showOnMap
            },
            retryReason: 'non-administrative-dataset',
            details:
              'This tool is reserved for territorial/admin-unit datasets. ' +
              `Metadata suggests "${resolvedDatasetId}" should use ${String(
                getRoutingPreferredQueryTool(datasetRouting) || 'queryQCumberDataset'
              )}.`
          }
        };
      }
      const spatialRequired = isSpatialOnly || (policyMode === 'auto' && !datasetIsTerritorial);
      let effectiveSpatialBbox = normalizedSpatialBbox;
      if (spatialRequired && !effectiveSpatialBbox) {
        if (resolvedProviderId === 'local-assets-it') {
          effectiveSpatialBbox = [...ITALY_DEFAULT_SPATIAL_BBOX];
          queryPolicyAdjustmentsNote +=
            ' spatialBbox auto-set to Italy extent for local-assets-it thematic query.';
        } else {
          queryPolicyAdjustmentsNote += ' spatialBbox not provided; running thematic query without bbox prefilter.';
        }
      }
      const datasetResolutionChanged =
        !!requestedDatasetId &&
        !!resolvedDatasetId &&
        requestedDatasetId.toLowerCase() !== resolvedDatasetId.toLowerCase();
      const datasetAutoSelected = !requestedDatasetId && datasetResolution.autoSelected && !!resolvedDatasetId;
      const datasetResolutionNote = datasetResolutionChanged
        ? ` Dataset resolved from "${requestedDatasetId}" to "${resolvedDatasetId}".`
        : datasetAutoSelected
        ? ` Dataset auto-selected as "${resolvedDatasetId}" for provider "${resolvedProviderId}".`
        : '';
      const adminTypeToLevel = resolveAdminTypeToLevelMap(datasetCatalogItem, datasetHelp, datasetRouting);
      const explicitExpectedLv = resolveExpectedAdminLevel(normalizedExpectedAdminType, adminTypeToLevel);
      if (normalizedExpectedAdminType && explicitExpectedLv === null) {
        return {
          llmResult: {
            success: false,
            providerId: resolvedProviderId,
            datasetId: resolvedDatasetId,
            routing: datasetRouting,
            details:
              `Cannot resolve expectedAdminType "${normalizedExpectedAdminType}" for dataset "${resolvedDatasetId}" ` +
              'because adminTypeToLevel metadata is missing/incomplete. Add ai.profile.adminWorkflows.adminTypeToLevel in provider descriptor or pass explicit lv filter.'
          }
        };
      }
      let expectedLv = explicitExpectedLv;
      let normalizedFilters = (filters || []).map((filterItem: any) => {
        const normalizedOp = normalizeQcumberFilterOp(filterItem?.op);
        return {
          ...filterItem,
          op: normalizedOp
        };
      });
      const firstInvalidFilter = normalizedFilters.find((filterItem: any) => {
        const op = String(filterItem?.op || 'eq').toLowerCase();
        return !QCUMBER_FILTER_OPS.has(op);
      });
      if (firstInvalidFilter) {
        return {
          llmResult: {
            success: false,
            providerId: resolvedProviderId,
            datasetId: resolvedDatasetId,
            details: `Unsupported filter operator "${String(
              firstInvalidFilter?.op || ''
            )}". Use attribute operators only; spatial constraints must be passed via spatialBbox.`
          }
        };
      }
      const suspiciousFieldValueFilter = normalizedFilters.find((filterItem: any) => {
        const op = String(filterItem?.op || 'eq').toLowerCase();
        if (op === 'eq') {
          return isFilterValueLikelyFieldName(filterItem?.field, filterItem?.value);
        }
        if (op === 'in') {
          const values = Array.isArray(filterItem?.values)
            ? filterItem.values
            : Array.isArray(filterItem?.value)
            ? filterItem.value
            : [filterItem?.value];
          return values.some((candidate: unknown) => isFilterValueLikelyFieldName(filterItem?.field, candidate));
        }
        return false;
      });
      if (suspiciousFieldValueFilter) {
        const badValue = Array.isArray(suspiciousFieldValueFilter?.values)
          ? suspiciousFieldValueFilter.values.find((candidate: unknown) =>
              isFilterValueLikelyFieldName(suspiciousFieldValueFilter?.field, candidate)
            )
          : suspiciousFieldValueFilter?.value;
        return {
          llmResult: {
            success: false,
            providerId: resolvedProviderId,
            datasetId: resolvedDatasetId,
            routing: datasetRouting,
            details:
              `Suspicious filter value "${String(badValue || '')}" for field "${String(
                suspiciousFieldValueFilter?.field || ''
              )}". ` +
              'The value looks like a field name. Use the actual parent-id value from a returned row (e.g. "037126"), not the field label.'
          }
        };
      }
      const requestedLimit = Number.isFinite(Number(limit))
        ? Math.max(1, Math.min(100000, Number(limit)))
        : loadToMap === false
        ? 1000
        : 50000;
      const requestedOffset = Number.isFinite(Number(offset)) ? Number(offset) : 0;
      const hasExplicitLoadToMap = typeof loadToMap === 'boolean';
      const hasExplicitShowOnMap = typeof showOnMap === 'boolean';
      const hasRequestedOrderBy = String(orderBy || '').trim().length > 0;
      const isRankingLikeQuery = hasRequestedOrderBy && requestedLimit <= 200 && normalizedFilters.length <= 1;
      const preserveRankingLimit = isRankingLikeQuery && Number.isFinite(Number(limit));
      // Keep off-map ranking scans by default only for non-territorial/thematic lookups.
      // Territorial ranking flows are often followed by map transforms (area/styling), so prefer loadToMap.
      const preferOffMapRanking = isRankingLikeQuery && !isTerritorialOnly && !normalizedExpectedAdminType;
      const shouldLoadToMap = hasExplicitLoadToMap ? loadToMap !== false : !preferOffMapRanking;
      const shouldShowOnMap = shouldLoadToMap ? (hasExplicitShowOnMap ? showOnMap === true : false) : false;
      if (hasExplicitShowOnMap && !shouldLoadToMap && showOnMap === true) {
        queryPolicyAdjustmentsNote += ' showOnMap=true ignored because loadToMap=false.';
      }
      const municipalWorkflow = isLikelyMunicipalFilter(normalizedFilters);
      const resolvedLimit =
        shouldLoadToMap && municipalWorkflow && !preserveRankingLimit ? Math.max(requestedLimit, 50000) : requestedLimit;
      // Keep full source schema for q-cumber queries.
      // Do not pass `select` to backend: kepler/frontend decides field filtering later.
      const resolvedSelect: string[] = [];
      const rankingFieldCandidates = collectMetadataRankingFieldCandidates(
        datasetRouting,
        datasetHelp,
        datasetCatalogItem
      );
      let effectiveOrderBy = String(orderBy || '').trim();
      let rankingFieldPreFixNote = '';
      if (preserveRankingLimit && isGeometryLikeRankingField(effectiveOrderBy)) {
        const metadataFallback = pickBestMetadataRankingCandidate(rankingFieldCandidates);
        if (metadataFallback) {
          const requestedOrderBy = String(orderBy || '').trim();
          effectiveOrderBy = metadataFallback;
          rankingFieldPreFixNote =
            ` Requested orderBy "${requestedOrderBy || '_auto_'}" was replaced with "${metadataFallback}"` +
            ' because the original field was not comparable for ranking.';
        }
      }

      const baseBody = {
        providerId: resolvedProviderId,
        datasetId: String(resolvedDatasetId || '').trim(),
        select: resolvedSelect.length ? resolvedSelect : undefined,
        orderBy: effectiveOrderBy || undefined,
        orderDirection: orderDirection || 'asc'
      };

      if (!baseBody.datasetId) {
        return {
          llmResult: {
            success: false,
            details: 'Missing datasetId.'
          }
        };
      }

      const shouldInferPoints = inferPointsFromLatLon === true || QCUMBER_INCLUDE_LATLON_FALLBACK_FIELDS;
      const queryCacheKey = stableSerializeForCache({
        cacheVersion: 1,
        policyMode,
        providerId: resolvedProviderId,
        datasetId: baseBody.datasetId,
        filters: normalizedFilters,
        orderBy: baseBody.orderBy || null,
        orderDirection: baseBody.orderDirection || 'asc',
        limit: resolvedLimit,
        offset: requestedOffset,
        spatialBbox: effectiveSpatialBbox || null,
        expectedAdminType: normalizedExpectedAdminType || null,
        loadToMap: shouldLoadToMap,
        showOnMap: shouldShowOnMap,
        inferPointsFromLatLon: shouldInferPoints,
        rankingMode: preserveRankingLimit
      });
      const cachedSuccessfulQuery = getCachedSuccessfulQcumberQuery(queryCacheKey);
      if (cachedSuccessfulQuery?.llmResult?.success) {
        return {
          llmResult: {
            ...cachedSuccessfulQuery.llmResult,
            details: `${String(cachedSuccessfulQuery.llmResult?.details || '').trim()} Reused cached successful query result for identical arguments.`
          }
        };
      }

      try {
        let filterAutoRewriteNote = '';
        let spatialRetryNote = '';
        let querySpatialBbox = effectiveSpatialBbox;
        const parentIdRetryCandidates = collectParentIdRetryFieldCandidates(
          datasetRouting,
          datasetHelp,
          datasetCatalogItem,
          normalizedFilters
        );
        const queryWithLimit = async (limitValue: number, orderByOverride?: string, offsetValue?: number) => {
          const body = {
            ...baseBody,
            spatialBbox: querySpatialBbox,
            filters: normalizedFilters.length ? normalizedFilters : undefined,
            orderBy: String(orderByOverride || '').trim() || baseBody.orderBy,
            limit: Math.max(1, Math.min(100000, Number(limitValue))),
            offset: Number.isFinite(Number(offsetValue)) ? Number(offsetValue) : requestedOffset
          };
          try {
            return await requestQcumberJson('/datasets/query', {
              method: 'POST',
              body: JSON.stringify(body)
            });
          } catch (queryError) {
            const canRetryGenericParent =
              hasGenericParentIdFilter(normalizedFilters) &&
              isMissingFilterFieldError(queryError, 'parent_id') &&
              parentIdRetryCandidates.length > 0;
            if (!canRetryGenericParent) {
              throw queryError;
            }
            for (const retryFieldName of parentIdRetryCandidates) {
              const retryFilters = rewriteGenericParentIdFilters(normalizedFilters, retryFieldName);
              if (!retryFilters.length) continue;
              const retryBody = {
                ...body,
                filters: retryFilters
              };
              try {
                const retryPayload = await requestQcumberJson('/datasets/query', {
                  method: 'POST',
                  body: JSON.stringify(retryBody)
                });
                normalizedFilters = retryFilters;
                filterAutoRewriteNote =
                  ` Parent filter auto-resolved: "parent_id" -> "${retryFieldName}" using backend routing metadata.`;
                return retryPayload;
              } catch (retryError) {
                if (isMissingFilterFieldError(retryError, retryFieldName)) {
                  continue;
                }
                throw retryError;
              }
            }
            throw queryError;
          }
        };
        const runPagedQueryWindow = async () => {
          let payload = await queryWithLimit(resolvedLimit);
          let rows = Array.isArray(payload?.rows) ? payload.rows : [];
          let fields = Array.isArray(payload?.fields) ? payload.fields : [];
          let returnedCount = Number(payload?.returned || rows.length || 0);
          let totalMatched = Number(payload?.totalMatched || rows.length || 0);
          let paginationNote = '';
          if (shouldLoadToMap && !preserveRankingLimit && totalMatched > returnedCount && resolvedLimit < 100000) {
            const retryLimit = Math.max(resolvedLimit, Math.min(100000, totalMatched));
            payload = await queryWithLimit(retryLimit);
            rows = Array.isArray(payload?.rows) ? payload.rows : [];
            fields = Array.isArray(payload?.fields) ? payload.fields : [];
            returnedCount = Number(payload?.returned || rows.length || 0);
            totalMatched = Number(payload?.totalMatched || rows.length || 0);
          }
          const canAutoPageFullWindow =
            shouldLoadToMap && !preserveRankingLimit && requestedOffset <= 0 && totalMatched > returnedCount;
          if (canAutoPageFullWindow) {
            const mergedRows = Array.isArray(rows) ? rows.slice() : [];
            let nextOffset = Number(returnedCount || mergedRows.length || 0);
            const pageLimit = 100000;
            while (nextOffset < totalMatched) {
              const remaining = Math.max(0, totalMatched - nextOffset);
              if (remaining <= 0) break;
              const pagePayload = await queryWithLimit(Math.min(pageLimit, remaining), undefined, nextOffset);
              const pageRows = Array.isArray(pagePayload?.rows) ? pagePayload.rows : [];
              if (!pageRows.length) break;
              mergedRows.push(...pageRows);
              nextOffset += pageRows.length;
              if (pageRows.length < Math.min(pageLimit, remaining)) {
                break;
              }
            }
            rows = mergedRows;
            returnedCount = rows.length;
            if (payload && typeof payload === 'object') {
              payload = {
                ...payload,
                rows,
                returned: returnedCount
              };
            }
            paginationNote =
              returnedCount >= totalMatched
                ? ` Loaded all ${totalMatched} matched rows across paged windows.`
                : ` Loaded ${returnedCount}/${totalMatched} rows across paged windows.`;
          }
          return {payload, rows, fields, returnedCount, totalMatched, paginationNote};
        };

        let {payload, rows, fields, returnedCount, totalMatched, paginationNote} = await runPagedQueryWindow();
        const isAutoSpatialBbox = !normalizedSpatialBbox && Array.isArray(effectiveSpatialBbox);
        const canRetryWithoutSpatialBbox =
          isAutoSpatialBbox &&
          shouldLoadToMap &&
          !preserveRankingLimit &&
          spatialRequired &&
          !datasetIsTerritorial &&
          !normalizedFilters.length &&
          requestedOffset <= 0 &&
          totalMatched <= 0;
        if (canRetryWithoutSpatialBbox) {
          querySpatialBbox = undefined;
          try {
            const noBboxResult = await runPagedQueryWindow();
            if (noBboxResult.totalMatched > 0) {
              payload = noBboxResult.payload;
              rows = noBboxResult.rows;
              fields = noBboxResult.fields;
              returnedCount = noBboxResult.returnedCount;
              totalMatched = noBboxResult.totalMatched;
              paginationNote = noBboxResult.paginationNote;
              spatialRetryNote =
                ` Spatial prefilter returned zero rows; retried without spatialBbox and recovered ${totalMatched} rows.`;
            } else {
              querySpatialBbox = effectiveSpatialBbox;
            }
          } catch {
            querySpatialBbox = effectiveSpatialBbox;
          }
        }

        const normalizeLevelString = (value: unknown) => String(value ?? '').trim();
        const rowsMatchExpectedLevel = (inputRows: any[], levelFieldName: string, expectedLevel: number) => {
          const list = Array.isArray(inputRows) ? inputRows : [];
          return list.every((row: any) => {
            if (!row || typeof row !== 'object' || Array.isArray(row)) return true;
            return valuesEqualAdminLevel((row as any)[levelFieldName], expectedLevel);
          });
        };
        const filtersIncludeExpectedLevel = (inputFilters: any[], levelFieldName: string, expectedLevel: number) => {
          const list = Array.isArray(inputFilters) ? inputFilters : [];
          return list.some((filter: any) => {
            const field = String(filter?.field || '').trim();
            const op = String(filter?.op || 'eq').toLowerCase();
            if (field !== levelFieldName) return false;
            if (op === 'eq') {
              return valuesEqualAdminLevel(filter?.value, expectedLevel);
            }
            if (op === 'in') {
              const values = Array.isArray(filter?.values)
                ? filter.values
                : Array.isArray(filter?.value)
                ? filter.value
                : [filter?.value];
              return values.some((value: unknown) => valuesEqualAdminLevel(value, expectedLevel));
            }
            return false;
          });
        };

        let adminMetadata = inferQcumberAdminMetadata(fields, rows);
        const exactNameFilterValues = extractExactNameFilterValues(normalizedFilters);
        // Preserve explicit administrative intent (e.g. province -> lv=7).
        // Inference from sampled levels can be misleading on small/partial result sets.
        if (explicitExpectedLv === null) {
          expectedLv = inferExpectedAdminLevelFromAvailableLevels(
            normalizedExpectedAdminType,
            adminTypeToLevel,
            adminMetadata?.levelCounts,
            expectedLv
          );
        }

        const sampledLevels = Object.keys(adminMetadata?.levelCounts || {})
          .map(value => String(value || '').trim())
          .filter(Boolean);
        if (!normalizedExpectedAdminType && exactNameFilterValues.length > 0 && sampledLevels.length > 1) {
          return {
            llmResult: {
              success: false,
              providerId: resolvedProviderId,
              datasetId: baseBody.datasetId,
              clarificationRequired: true,
              clarificationQuestion:
                `Il toponimo ${exactNameFilterValues.join(', ')} esiste su più livelli amministrativi (${sampledLevels.join(', ')}). ` +
                'Vuoi provincia, comune, regione o stato?',
              clarificationOptions: ['province', 'municipality', 'region', 'country'],
              details:
                `Ambiguous administrative match for name filter (${exactNameFilterValues.join(', ')}). ` +
                `Matched multiple levels (${sampledLevels.join(', ')}). ` +
                'Retry with expectedAdminType (province/municipality/region/country) or add explicit lv filter.'
            }
          };
        }
        if (expectedLv !== null) {
          let levelFieldName = String(adminMetadata?.levelField || '').trim();
          if (!levelFieldName) {
            const strictLvBody = {
              ...baseBody,
              // Remove select for this internal guardrail retry to expose all fields.
              select: undefined,
              filters: [...normalizedFilters, {field: 'lv', op: 'eq', value: expectedLv}],
              limit: Math.max(1, Math.min(100000, Number(resolvedLimit))),
              offset: Number.isFinite(Number(offset)) ? Number(offset) : 0
            };
            payload = await requestQcumberJson('/datasets/query', {
              method: 'POST',
              body: JSON.stringify(strictLvBody)
            });
            rows = Array.isArray(payload?.rows) ? payload.rows : [];
            fields = Array.isArray(payload?.fields) ? payload.fields : [];
            returnedCount = Number(payload?.returned || rows.length || 0);
            totalMatched = Number(payload?.totalMatched || rows.length || 0);
            adminMetadata = inferQcumberAdminMetadata(fields, rows);
            levelFieldName = String(adminMetadata?.levelField || '').trim();
          }
          if (!levelFieldName) {
            return {
              llmResult: {
                success: false,
                providerId: resolvedProviderId,
                datasetId: baseBody.datasetId,
                details:
                  `Cannot enforce expected administrative type "${normalizedExpectedAdminType}" because no level field was found in dataset rows.`
              }
            };
          }

          const expectedLevelLabel = String(expectedLv);
          const availableLevels = Object.keys(adminMetadata?.levelCounts || {}).map(normalizeLevelString);
          const allRowsMatch = rowsMatchExpectedLevel(rows, levelFieldName, expectedLv);
          const alreadyConstrained = filtersIncludeExpectedLevel(normalizedFilters, levelFieldName, expectedLv);

          if (!allRowsMatch && !alreadyConstrained) {
            const strictFilters = [...normalizedFilters, {field: levelFieldName, op: 'eq', value: expectedLv}];
            const strictBody = {
              ...baseBody,
              filters: strictFilters,
              limit: Math.max(1, Math.min(100000, Number(resolvedLimit))),
              offset: Number.isFinite(Number(offset)) ? Number(offset) : 0
            };
            payload = await requestQcumberJson('/datasets/query', {
              method: 'POST',
              body: JSON.stringify(strictBody)
            });
            rows = Array.isArray(payload?.rows) ? payload.rows : [];
            fields = Array.isArray(payload?.fields) ? payload.fields : [];
            returnedCount = Number(payload?.returned || rows.length || 0);
            totalMatched = Number(payload?.totalMatched || rows.length || 0);
            adminMetadata = inferQcumberAdminMetadata(fields, rows);
          } else if (!allRowsMatch && alreadyConstrained) {
            return {
              llmResult: {
                success: false,
                providerId: resolvedProviderId,
                datasetId: baseBody.datasetId,
                details:
                  `Administrative level mismatch: expected ${normalizedExpectedAdminType} (lv=${expectedLevelLabel}), ` +
                  `but result rows include other levels in field "${levelFieldName}".`
              }
            };
          }

          const postRowsMatch = rowsMatchExpectedLevel(rows, levelFieldName, expectedLv);
          if (!postRowsMatch) {
            return {
              llmResult: {
                success: false,
                providerId: resolvedProviderId,
                datasetId: baseBody.datasetId,
                details:
                  `Administrative level mismatch after strict filtering: expected ${normalizedExpectedAdminType} (lv=${expectedLevelLabel}) on field "${levelFieldName}".`
              }
            };
          }

          if (rows.length === 0 && availableLevels.length && !availableLevels.includes(expectedLevelLabel)) {
            return {
              llmResult: {
                success: false,
                providerId: resolvedProviderId,
                datasetId: baseBody.datasetId,
                details:
                  `Expected administrative type "${normalizedExpectedAdminType}" (lv=${expectedLevelLabel}) not found. ` +
                  `Available sampled levels: ${availableLevels.join(', ')}.`
              }
            };
          }
        }

        const executionKey = `qcumber-query-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
        const hasUserFilters = normalizedFilters.length > 0;
        const consistency = verifyRowsAgainstFilters(rows, normalizedFilters);
        if (hasUserFilters && !consistency.ok) {
          return {
            llmResult: {
              success: false,
              providerId: resolvedProviderId,
              datasetId: baseBody.datasetId,
              details:
                `Query returned rows not consistent with requested filters (${consistency.failedFilter || 'filter mismatch'}). ` +
                'Result not loaded to map. Refine filters and retry.'
            }
          };
        }
        if (hasUserFilters && totalMatched <= 0) {
          return {
            llmResult: {
              success: true,
              providerId: resolvedProviderId,
              datasetId: baseBody.datasetId,
              returned: 0,
              totalMatched: 0,
              fields,
              h3Metadata: inferQcumberH3Metadata(fields, []),
              rows: [],
              details: `Query completed for dataset ${baseBody.datasetId} with zero matches. No dataset loaded to map.`
            }
          };
        }
        let rankingMetricField = String(effectiveOrderBy || '').trim();
        let rankingFieldAutoFixNote = '';
        if (preserveRankingLimit) {
          let rankingMetricValidation = validateRankingMetric(rows, fields, rankingMetricField);
          if (!rankingMetricValidation.ok) {
            const fallbackOrderBy = resolveFallbackRankingOrderBy(rows, fields, [
              ...rankingFieldCandidates,
              ...['population', 'area_m2', 'gid', 'id', 'name']
            ]);
            const requestedOrderBy = String(orderBy || '').trim();
            const canRetryWithFallback =
              !!fallbackOrderBy && fallbackOrderBy.toLowerCase() !== String(rankingMetricField || '').toLowerCase();
            if (canRetryWithFallback) {
              payload = await queryWithLimit(resolvedLimit, fallbackOrderBy);
              rows = Array.isArray(payload?.rows) ? payload.rows : [];
              fields = Array.isArray(payload?.fields) ? payload.fields : [];
              returnedCount = Number(payload?.returned || rows.length || 0);
              totalMatched = Number(payload?.totalMatched || rows.length || 0);
              rankingMetricValidation = validateRankingMetric(rows, fields, fallbackOrderBy);
              if (rankingMetricValidation.ok) {
                rankingMetricField = rankingMetricValidation.metricField;
                rankingFieldAutoFixNote =
                  ` Requested orderBy "${requestedOrderBy || '_auto_'}" was replaced with "${rankingMetricField}"` +
                  ' because the original field was not comparable for ranking.';
              }
            }
            if (!rankingMetricValidation.ok) {
              return {
                llmResult: {
                  success: false,
                  providerId: resolvedProviderId,
                  requestedDatasetId: requestedDatasetId || undefined,
                  datasetId: baseBody.datasetId,
                  routing: datasetRouting,
                  details:
                    `Ranking query guardrail: ${rankingMetricValidation.reason || 'invalid orderBy metric'}.` +
                    `${providerResolutionNote}${datasetResolutionNote}`
                }
              };
            }
          }
          rankingMetricField = rankingMetricValidation.metricField;
        }
        const h3Metadata = inferQcumberH3Metadata(fields, rows);

        const rawMapDataset = shouldLoadToMap ? rebuildQcumberMapDatasetRows(payload?.dataset, rows, fields) : null;
        const mapDataset =
          shouldLoadToMap && shouldInferPoints ? ensureGeojsonPointsFromLatLon(rawMapDataset) : rawMapDataset;
        const canRenderGeometryLayer = shouldLoadToMap ? datasetHasRenderableGeometry(mapDataset) : false;
        const forceTableOnlyForLargeThematicGeometry =
          canRenderGeometryLayer &&
          !datasetIsTerritorial &&
          returnedCount > QCUMBER_MAX_AUTO_LAYER_GEOMETRY_ROWS;
        const autoCreateLayers =
          shouldShowOnMap && canRenderGeometryLayer && !forceTableOnlyForLargeThematicGeometry;
        const autoLayerDisabledReason = !shouldShowOnMap
          ? 'showOnMap=false'
          : forceTableOnlyForLargeThematicGeometry
          ? 'large geometry policy'
          : 'no renderable geometry';
        const autoLayerPolicyNote = forceTableOnlyForLargeThematicGeometry
          ? ` Auto-layer disabled for large thematic geometry dataset (${returnedCount} rows > ${QCUMBER_MAX_AUTO_LAYER_GEOMETRY_ROWS}) to avoid UI freeze.`
          : !shouldShowOnMap
          ? ' Auto-layer disabled (showOnMap=false).'
          : '';
        const loadedIdentity = shouldLoadToMap
          ? deriveDatasetIdentity(mapDataset, executionKey)
          : {id: '', label: '', ref: ''};
        const previewRowsLimit = preserveRankingLimit ? Math.min(20, Math.max(1, requestedLimit)) : 8;
        const rankingSummary = preserveRankingLimit
          ? ` Ranking preserved with limit=${requestedLimit}, ordered by ${rankingMetricField || 'field'} ${orderDirection || 'asc'}.${rankingFieldPreFixNote}${rankingFieldAutoFixNote}`
          : '';
        const rankingPreview = preserveRankingLimit
          ? buildRankingPreviewDetails(rows, rankingMetricField, orderDirection, requestedLimit)
          : '';
        const offMapGuidance = shouldLoadToMap
          ? ''
          : ' Off-map query only: no dataset was created in current map state. Do not call waitForQMapDataset/countQMapRows; answer directly from returned rows. If you need map transforms (e.g. createDatasetWithGeometryArea), rerun the same query with loadToMap=true.';

        const queryResult = {
          llmResult: {
            success: true,
            providerId: resolvedProviderId,
            requestedDatasetId: requestedDatasetId || undefined,
            datasetId: baseBody.datasetId,
            loadedDatasetId: loadedIdentity.id || undefined,
            loadedDatasetName: loadedIdentity.label || undefined,
            loadedDatasetRef: loadedIdentity.ref || undefined,
            returned: returnedCount,
            totalMatched,
            fields,
            h3Metadata,
            adminMetadata,
            adminTypeToLevel,
            routing: datasetRouting,
            expectedAdminType: normalizedExpectedAdminType || undefined,
            rows: compactQcumberPreviewRows(rows, previewRowsLimit),
            loadedToMap: shouldLoadToMap,
            showOnMap: shouldLoadToMap ? shouldShowOnMap : false,
            details: shouldLoadToMap
              ? autoCreateLayers
                ? `Query completed and loaded dataset ${baseBody.datasetId} into map as "${loadedIdentity.label}" (${loadedIdentity.ref}).${providerResolutionNote}${datasetResolutionNote}${queryPolicyAdjustmentsNote}${filterAutoRewriteNote}${spatialRetryNote}${rankingSummary}${rankingPreview}${paginationNote}`
                : `Query completed and loaded dataset ${baseBody.datasetId} as table "${loadedIdentity.label}" (${loadedIdentity.ref}) without auto layer (${autoLayerDisabledReason}).${providerResolutionNote}${datasetResolutionNote}${queryPolicyAdjustmentsNote}${filterAutoRewriteNote}${spatialRetryNote}${rankingSummary}${rankingPreview}${paginationNote}${autoLayerPolicyNote}`
              : `Query completed for dataset ${baseBody.datasetId}.${providerResolutionNote}${datasetResolutionNote}${queryPolicyAdjustmentsNote}${filterAutoRewriteNote}${spatialRetryNote}${rankingSummary}${rankingPreview}${offMapGuidance}`
          },
          additionalData: shouldLoadToMap
            ? {
                dataset: mapDataset,
                autoCreateLayers,
                executionKey
              }
            : undefined
        };
        setCachedSuccessfulQcumberQuery(queryCacheKey, queryResult.llmResult);
        return queryResult;
      } catch (error: any) {
        return {
          llmResult: {
            success: false,
            providerId: resolvedProviderId,
            requestedDatasetId: requestedDatasetId || undefined,
            datasetId: baseBody.datasetId,
            routing: datasetRouting,
            details: `Query failed for dataset ${baseBody.datasetId}: ${String(
              error?.message || error || 'Unknown error'
            )}.${providerResolutionNote}${datasetResolutionNote}`
          }
        };
      }
    },
    component: QueryQCumberDatasetComponent
  };
}
