import test from 'node:test';
import assert from 'node:assert/strict';
import {buildQMapToolsWithoutCategoryIntrospection} from '../../../src/features/qmap-ai/tool-groups';

test('merges base/cloud/custom grouped tools', () => {
  const registry = buildQMapToolsWithoutCategoryIntrospection({
    baseToolsWithChartPolicy: {baseA: {execute: () => 'a'}},
    qMapCloudTools: {cloudA: {execute: () => 'b'}},
    customToolGroups: {
      discovery: {customA: {execute: () => 'c'}},
      styling: {customB: {execute: () => 'd'}}
    }
  });

  assert.ok(registry.baseA);
  assert.ok(registry.cloudA);
  assert.ok(registry.customA);
  assert.ok(registry.customB);
});

test('throws on duplicate tool registrations in strict mode', () => {
  assert.throws(() => {
    buildQMapToolsWithoutCategoryIntrospection({
      baseToolsWithChartPolicy: {},
      qMapCloudTools: {},
      customToolGroups: {
        discovery: {sameTool: {execute: () => 'x'}},
        styling: {sameTool: {execute: () => 'y'}}
      },
      strict: true
    });
  }, /Duplicate q-map tool registrations/);
});

test('allows duplicate tool registrations when strict is disabled', () => {
  const registry = buildQMapToolsWithoutCategoryIntrospection({
    baseToolsWithChartPolicy: {},
    qMapCloudTools: {},
    customToolGroups: {
      discovery: {sameTool: {execute: () => 'x'}},
      styling: {sameTool: {execute: () => 'y'}}
    },
    strict: false
  });

  assert.ok(registry.sameTool);
});
