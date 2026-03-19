import assert from 'node:assert/strict';
import test from 'node:test';
import {resolveDatasetNameForPostValidation, shouldRunDatasetPostValidation} from '../../../src/features/qmap-ai/services/execution-tracking';

test('resolveDatasetNameForPostValidation prefers canonical datasetRef from llmResult datasetId', () => {
  const out = resolveDatasetNameForPostValidation(
    'tassellateDatasetLayer',
    {targetDatasetName: 'Tassellation_Brescia_r8'},
    {
      llmResult: {
        success: true,
        dataset: 'Tassellation_Brescia_r8',
        datasetId: 'pzbzvu'
      }
    }
  );
  assert.equal(out, 'id:pzbzvu');
});

test('resolveDatasetNameForPostValidation falls back to datasetName when no datasetId is available', () => {
  const out = resolveDatasetNameForPostValidation(
    'tassellateDatasetLayer',
    {targetDatasetName: 'Tassellation_Brescia_r8'},
    {
      llmResult: {
        success: true,
        dataset: 'Tassellation_Brescia_r8'
      }
    }
  );
  assert.equal(out, 'Tassellation_Brescia_r8');
});

test('shouldRunDatasetPostValidation keeps mutate tools in hard-validation set', () => {
  assert.equal(shouldRunDatasetPostValidation('tassellateDatasetLayer'), true);
  assert.equal(shouldRunDatasetPostValidation('populateTassellationFromAdminUnits'), true);
  assert.equal(shouldRunDatasetPostValidation('waitForQMapDataset'), false);
});

