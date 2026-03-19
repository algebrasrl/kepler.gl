import assert from 'node:assert/strict';
import test from 'node:test';
import {hasQMapIframeCloudMapId, resolveQMapMountViewport} from '../../../src/utils/mount-viewport';

const ITALY_VIEWPORT = {
  latitude: 42.5,
  longitude: 12.5,
  zoom: 5,
  bearing: 0,
  pitch: 0
};

test('reapplies initial viewport for iframe hashes without cloud map reference', () => {
  const hashValue = '#mode=geotoken&double-setup=1&preset=eyJ2IjoxfQ&action_uuid=test-action';

  assert.equal(hasQMapIframeCloudMapId(hashValue), false);
  assert.deepEqual(resolveQMapMountViewport(hashValue, ITALY_VIEWPORT), ITALY_VIEWPORT);
});

test('does not reapply initial viewport when iframe cloud map will load', () => {
  const hashValue = '#mode=geotoken&double-setup=1&cloud_map_id=map-123&cloud_provider=q-storage-backend';

  assert.equal(hasQMapIframeCloudMapId(hashValue), true);
  assert.equal(resolveQMapMountViewport(hashValue, ITALY_VIEWPORT), null);
});

test('supports query-style hash payloads used by /map routes', () => {
  const hashValue = '#/map?mode=geotoken&double-setup=1&action_uuid=test-action';
  const explicitViewport = {
    latitude: 45.46,
    longitude: 9.19,
    zoom: 9,
    bearing: 0,
    pitch: 0
  };

  assert.equal(hasQMapIframeCloudMapId(hashValue), false);
  assert.deepEqual(resolveQMapMountViewport(hashValue, explicitViewport), explicitViewport);
});
