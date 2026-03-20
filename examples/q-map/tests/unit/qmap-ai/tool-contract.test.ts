import {describe, it, expect} from 'vitest';
import {
  getQMapToolContract,
  getQMapContractToolNames,
  getQMapToolContractsManifest,
  getQMapToolContractUnknownArgKeys,
  QMAP_TOOL_CONTRACT_SCHEMA
} from '../../../src/features/qmap-ai/tool-contract';

// ─── getQMapToolContractsManifest ───────────────────────────────────────────

describe('getQMapToolContractsManifest', () => {
  it('returns a manifest with the expected schema', () => {
    const manifest = getQMapToolContractsManifest();
    expect(manifest.schema).toBe(QMAP_TOOL_CONTRACT_SCHEMA);
  });

  it('has a non-empty version string', () => {
    const manifest = getQMapToolContractsManifest();
    expect(manifest.version.length).toBeGreaterThan(0);
  });

  it('has default argsSchema and responseContract', () => {
    const manifest = getQMapToolContractsManifest();
    expect(manifest.defaults.argsSchema).toBeDefined();
    expect(manifest.defaults.argsSchema.type).toBe('object');
    expect(manifest.defaults.responseContract).toBeDefined();
    expect(manifest.defaults.responseContract.schema).toBe('qmap.tool_result.v1');
  });

  it('has a non-empty tools record', () => {
    const manifest = getQMapToolContractsManifest();
    const toolNames = Object.keys(manifest.tools);
    expect(toolNames.length).toBeGreaterThan(0);
  });
});

// ─── getQMapToolContract ────────────────────────────────────────────────────

describe('getQMapToolContract', () => {
  it('returns contract for known tool', () => {
    const contract = getQMapToolContract('listQMapDatasets');
    expect(contract).not.toBeNull();
    expect(contract!.toolName).toBe('listQMapDatasets');
  });

  it('returns null for unknown tool', () => {
    expect(getQMapToolContract('nonExistentToolXyz')).toBeNull();
  });

  it('returns null for empty string', () => {
    expect(getQMapToolContract('')).toBeNull();
  });

  it('returns null for undefined/null coerced input', () => {
    expect(getQMapToolContract(undefined as any)).toBeNull();
    expect(getQMapToolContract(null as any)).toBeNull();
  });

  it('trims whitespace from tool name', () => {
    const direct = getQMapToolContract('listQMapDatasets');
    const padded = getQMapToolContract('  listQMapDatasets  ');
    // Both should match (or both null if padded doesn't match)
    // The implementation trims, so they should be equal
    expect(direct).not.toBeNull();
    expect(padded).toEqual(direct);
  });

  it('contract entry has required fields', () => {
    const contract = getQMapToolContract('listQMapDatasets');
    expect(contract).not.toBeNull();
    expect(contract!.categories).toBeInstanceOf(Array);
    expect(contract!.flags).toBeDefined();
    expect(typeof contract!.flags.mutatesDataset).toBe('boolean');
    expect(typeof contract!.flags.discovery).toBe('boolean');
    expect(typeof contract!.flags.bridgeOperation).toBe('boolean');
    expect(contract!.argsSchema).toBeDefined();
    expect(contract!.responseContract).toBeDefined();
  });

  it('discovery tools have discovery flag set', () => {
    const contract = getQMapToolContract('listQMapDatasets');
    expect(contract!.flags.discovery).toBe(true);
  });

  it('mutation tools have mutatesDataset flag set', () => {
    const contract = getQMapToolContract('clipQMapDatasetByGeometry');
    expect(contract).not.toBeNull();
    expect(contract!.flags.mutatesDataset).toBe(true);
  });

  it('response contract has expected shape', () => {
    const contract = getQMapToolContract('spatialJoinByPredicate');
    expect(contract).not.toBeNull();
    expect(contract!.responseContract.schema).toBe('qmap.tool_result.v1');
    expect(typeof contract!.responseContract.properties).toBe('object');
    expect(contract!.responseContract.required).toBeInstanceOf(Array);
    expect(typeof contract!.responseContract.allowAdditionalProperties).toBe('boolean');
  });
});

// ─── getQMapContractToolNames ───────────────────────────────────────────────

describe('getQMapContractToolNames', () => {
  it('returns a non-empty sorted array', () => {
    const names = getQMapContractToolNames();
    expect(names.length).toBeGreaterThan(0);
    // Check sorted
    const sorted = [...names].sort((a, b) => a.localeCompare(b));
    expect(names).toEqual(sorted);
  });

  it('includes known tool names', () => {
    const names = getQMapContractToolNames();
    expect(names).toContain('listQMapDatasets');
    expect(names).toContain('clipQMapDatasetByGeometry');
  });
});

// ─── getQMapToolContractUnknownArgKeys ──────────────────────────────────────

describe('getQMapToolContractUnknownArgKeys', () => {
  it('reports unknown keys for strict-schema tools', () => {
    const result = getQMapToolContractUnknownArgKeys('queryQCumberDataset', {
      providerId: 'local-assets-it',
      datasetId: 'kontur-boundaries-italia',
      legacyDatasetRef: 'kontur-boundaries-italia'
    });
    expect(result.unknownArgKeys).toContain('legacyDatasetRef');
    expect(result.allowedArgKeys).toContain('providerId');
    expect(result.allowedArgKeys).toContain('datasetId');
  });

  it('accepts all canonical keys without reporting unknown', () => {
    const result = getQMapToolContractUnknownArgKeys('queryQCumberTerritorialUnits', {
      providerId: 'local-assets-it',
      datasetId: 'kontur-boundaries-italia',
      expectedAdminType: 'province',
      limit: 1000
    });
    expect(result.unknownArgKeys).toEqual([]);
  });

  it('does not enforce unknown-key rejection when additionalProperties=true', () => {
    const result = getQMapToolContractUnknownArgKeys('setQMapLayerSolidColor', {
      layerName: 'Comuni',
      fillColor: '#0000FF',
      legacyColor: '#00FF00'
    });
    expect(result.unknownArgKeys).toEqual([]);
    expect(result.allowedArgKeys).toEqual([]);
  });

  it('returns empty arrays for unknown tool', () => {
    const result = getQMapToolContractUnknownArgKeys('nonExistentTool', {x: 1});
    expect(result.unknownArgKeys).toEqual([]);
    expect(result.allowedArgKeys).toEqual([]);
  });

  it('returns empty unknownArgKeys when args are empty', () => {
    const result = getQMapToolContractUnknownArgKeys('queryQCumberDataset', {});
    expect(result.unknownArgKeys).toEqual([]);
  });
});
