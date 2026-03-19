import assert from 'node:assert/strict';
import test from 'node:test';

import {getQMapToolContract, getQMapToolContractUnknownArgKeys} from '../../../src/features/qmap-ai/tool-contract';

test('reports unknown keys for strict contract tools', () => {
  const result = getQMapToolContractUnknownArgKeys('queryQCumberDataset', {
    providerId: 'local-assets-it',
    datasetId: 'kontur-boundaries-italia',
    legacyDatasetRef: 'kontur-boundaries-italia'
  });
  assert.deepEqual(result.unknownArgKeys, ['legacyDatasetRef']);
  assert.ok(result.allowedArgKeys.includes('providerId'));
  assert.ok(result.allowedArgKeys.includes('datasetId'));
});

test('accepts canonical keys for strict contract tools', () => {
  const result = getQMapToolContractUnknownArgKeys('queryQCumberTerritorialUnits', {
    providerId: 'local-assets-it',
    datasetId: 'kontur-boundaries-italia',
    expectedAdminType: 'province',
    limit: 1000
  });
  assert.deepEqual(result.unknownArgKeys, []);
});

test('does not enforce unknown-key rejection when contract allows additional properties', () => {
  const result = getQMapToolContractUnknownArgKeys('setQMapLayerSolidColor', {
    layerName: 'Comuni',
    fillColor: '#0000FF',
    legacyColor: '#00FF00'
  });
  assert.deepEqual(result.unknownArgKeys, []);
  assert.deepEqual(result.allowedArgKeys, []);
});

test('exposes declared response metadata fields for metric-producing tools', () => {
  const contract = getQMapToolContract('spatialJoinByPredicate');
  assert.ok(contract);
  assert.equal(contract?.responseContract.schema, 'qmap.tool_result.v1');
  assert.ok(contract?.responseContract.properties.fieldCatalog);
  assert.ok(contract?.responseContract.properties.aggregationOutputs);
  assert.ok(contract?.responseContract.properties.fieldAliases);
  assert.ok(contract?.responseContract.required.includes('fieldCatalog'));
  assert.ok(contract?.responseContract.required.includes('defaultStyleField'));
});

test('keeps per-tool response contract overrides for discovery outputs', () => {
  const contract = getQMapToolContract('listQMapDatasets');
  assert.ok(contract);
  assert.ok(contract?.responseContract.properties.datasets);
  assert.ok(contract?.responseContract.properties.layers);
  assert.ok(contract?.responseContract.required.includes('datasets'));
  assert.ok(contract?.responseContract.required.includes('layers'));
});
