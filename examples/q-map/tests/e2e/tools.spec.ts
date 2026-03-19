import {expect, test} from '@playwright/test';
import fs from 'node:fs';
import path from 'node:path';

const ADD_DATA_BUTTON = /Aggiungi dati|Add Data/i;
const ADD_DATA_MODAL_TITLE = /Add Data To Map|Aggiungi dati alla mappa/i;
const TAB_UPLOAD = /Carica file|Upload/i;
const REQUIRE_BACKEND_AUTH =
  String(process.env.QMAP_E2E_REQUIRE_BACKEND_AUTH || 'false').toLowerCase() === 'true';
const MAX_TOOL_TRACE_ENTRIES = 60;
const MAX_STRING_LEN = 240;

type ToolTraceEntry = {
  at: string;
  toolName: string;
  args: Record<string, unknown>;
  success: boolean | null;
  details: string;
  error: string;
  result: unknown;
};

let currentToolTrace: ToolTraceEntry[] = [];

function truncateText(value: unknown, maxLen = MAX_STRING_LEN): string {
  const text = String(value || '');
  if (text.length <= maxLen) return text;
  return `${text.slice(0, maxLen)}...`;
}

function compactTraceValue(value: unknown): unknown {
  if (value === null || value === undefined) return value;
  if (typeof value === 'string') return truncateText(value);
  if (typeof value === 'number' || typeof value === 'boolean') return value;
  if (Array.isArray(value)) return value.slice(0, 8).map(item => compactTraceValue(item));
  if (typeof value === 'object') {
    const entries = Object.entries(value as Record<string, unknown>).slice(0, 12);
    return Object.fromEntries(entries.map(([k, v]) => [k, compactTraceValue(v)]));
  }
  return truncateText(value);
}

function pushToolTrace(entry: ToolTraceEntry) {
  currentToolTrace.push(entry);
  if (currentToolTrace.length > MAX_TOOL_TRACE_ENTRIES) {
    currentToolTrace = currentToolTrace.slice(-MAX_TOOL_TRACE_ENTRIES);
  }
}

async function firstVisible(locator: any) {
  const count = await locator.count();
  for (let i = 0; i < count; i += 1) {
    const candidate = locator.nth(i);
    if (await candidate.isVisible().catch(() => false)) {
      return candidate;
    }
  }
  return locator.first();
}

async function ensureDatasetPanel(page: any) {
  const roleButton = page.getByRole('button', {name: ADD_DATA_BUTTON});
  const classButton = page.locator('.side-panel--container .add-data-button').first();
  if ((await roleButton.isVisible().catch(() => false)) || (await classButton.isVisible().catch(() => false))) return;

  const layerTab = page.locator('.side-panel__tab[data-for="layer-nav"]').first();
  if (await layerTab.count()) {
    await layerTab.click();
  } else {
    const sideTabs = page.locator('.side-panel__tab');
    if ((await sideTabs.count()) >= 2) {
      await sideTabs.nth(1).click();
    }
  }

  await expect(roleButton.or(classButton)).toBeVisible();
}

async function openAddDataModal(page: any) {
  await ensureDatasetPanel(page);
  const roleButton = page.getByRole('button', {name: ADD_DATA_BUTTON});
  const classButton = page.locator('.side-panel--container .add-data-button').first();
  if (await roleButton.isVisible().catch(() => false)) {
    await roleButton.click();
  } else {
    await classButton.click();
  }
  await expect(page.getByText(ADD_DATA_MODAL_TITLE)).toBeVisible();
}

async function openAssistant(page: any) {
  const aiToggle = page.getByText(/^AI$/).first();
  await expect(aiToggle).toBeVisible({timeout: 20_000});
  await aiToggle.click();
  await expect(page.getByText(/Assistente|Assistant/i).first()).toBeVisible({timeout: 20_000});
}

async function loadGeojsonFixtures(page: any) {
  const files = [
    path.resolve(__dirname, '../fixtures/polygon.geojson'),
    path.resolve(__dirname, '../fixtures/population_polygons.geojson'),
    path.resolve(__dirname, '../fixtures/timeseries_points.geojson'),
    path.resolve(__dirname, '../fixtures/adjacent_polygons.geojson'),
    path.resolve(__dirname, '../fixtures/split_line.geojson')
  ];
  for (const file of files) {
    await openAddDataModal(page);
    const uploadTab = await firstVisible(page.getByText(TAB_UPLOAD));
    await uploadTab.click();
    const uploadInput = page.locator('input[type="file"]').first();
    await expect(uploadInput).toBeAttached({timeout: 20_000});
    await uploadInput.setInputFiles(file);

    const base = path.basename(file).replace(/\\.geojson$/i, '');
    await expect(page.getByText(new RegExp(base, 'i')).first()).toBeVisible({timeout: 20_000});
  }
  await expect(page.getByText(/polygon/i).first()).toBeVisible({timeout: 20_000});
  await expect(page.getByText(/population_polygons|population polygons/i).first()).toBeVisible({timeout: 20_000});
  await expect(page.getByText(/split_line|split line/i).first()).toBeVisible({timeout: 20_000});
}

async function waitForToolRunner(page: any) {
  await page.waitForFunction(() => Boolean((window as any).__qmapRunTool), null, {timeout: 20_000});
}

async function runTool(page: any, toolName: string, args: Record<string, unknown> = {}) {
  try {
    const payload = await page.evaluate(
      async ({toolName, args}) => {
        const runner = (window as any).__qmapRunTool;
        if (!runner) {
          throw new Error('q-map tool runner is not available');
        }
        return runner(toolName, args);
      },
      {toolName, args}
    );
    const success = typeof payload?.result?.success === 'boolean' ? payload.result.success : null;
    const details = truncateText(payload?.result?.details || payload?.details || payload?.error || '');
    pushToolTrace({
      at: new Date().toISOString(),
      toolName,
      args: compactTraceValue(args) as Record<string, unknown>,
      success,
      details,
      error: '',
      result: compactTraceValue(payload?.result || payload)
    });
    return payload;
  } catch (error: any) {
    const message = truncateText(error?.message || error || '');
    pushToolTrace({
      at: new Date().toISOString(),
      toolName,
      args: compactTraceValue(args) as Record<string, unknown>,
      success: false,
      details: message,
      error: message,
      result: null
    });
    throw error;
  }
}

async function runToolExpectSuccess(page: any, toolName: string, args: Record<string, unknown> = {}) {
  const res = await runTool(page, toolName, args);
  if (!res?.result?.success) {
    const details = String(res?.result?.details || res?.details || res?.error || 'no details');
    throw new Error(`Tool "${toolName}" failed: ${details}`);
  }
  return res;
}

function getToolFailureDetails(res: any): string {
  return String(res?.result?.details || res?.details || res?.error || '').trim();
}

function isBackendUnavailableDetails(details: string): boolean {
  return /\b(unauthorized|forbidden|failed to fetch|fetch failed|networkerror|econnrefused)\b/i.test(
    String(details || '')
  );
}

function shouldSkipBackendInfraFailure(details: string): boolean {
  return !REQUIRE_BACKEND_AUTH && isBackendUnavailableDetails(details);
}

async function probeBackendTool(
  page: any,
  toolName: string,
  args: Record<string, unknown> = {}
): Promise<{success: boolean; details: string}> {
  const res = await runTool(page, toolName, args);
  return {
    success: Boolean(res?.result?.success),
    details: getToolFailureDetails(res)
  };
}

async function runToolExpectHandled(
  page: any,
  toolName: string,
  args: Record<string, unknown> = {},
  options?: {allowUnknownTool?: boolean}
) {
  let res: any;
  try {
    res = await runTool(page, toolName, args);
  } catch (error: any) {
    const message = String(error?.message || error || '');
    if (options?.allowUnknownTool && /Unknown q-map tool:/i.test(message)) {
      return {
        result: {
          success: false,
          details: message
        }
      };
    }
    throw error;
  }
  const hasBooleanResult = typeof res?.result?.success === 'boolean';
  const details = String(res?.result?.details || res?.details || res?.error || '').trim();
  expect(hasBooleanResult || details.length > 0).toBeTruthy();
  return res;
}

async function sendAssistantMessage(page: any, text: string) {
  await page
    .waitForFunction(() => {
      return Boolean(
        document.querySelector('input[placeholder*="prompt" i]') ||
          document.querySelector('textarea[placeholder*="prompt" i]') ||
          document.querySelector('[aria-label="Prompt"]')
      );
    })
    .catch(() => { /* ignore */ });

  const sendButton = page.getByRole('button', {name: /Send|Invia/i});
  const clickComposerSiblingButton = async (input: any) => {
    const candidate = input.locator(
      'xpath=ancestor::*[.//button][1]//button[not(@disabled)][1]'
    );
    const count = await candidate.count();
    if (!count) return false;
    const button = candidate.first();
    const visible = await button.isVisible().catch(() => false);
    const enabled = await button.isEnabled().catch(() => false);
    if (!visible || !enabled) return false;
    await button.click();
    return true;
  };
  const clickSendIfAvailable = async () => {
    const sendCount = await sendButton.count();
    if (!sendCount) return false;
    const button = await firstVisible(sendButton);
    const visible = await button.isVisible().catch(() => false);
    const enabled = await button.isEnabled().catch(() => false);
    if (visible && enabled) {
      await button.click();
      return true;
    }
    return false;
  };

  const promptByPlaceholder = page.getByPlaceholder(/Enter a prompt here/i);
  if (await promptByPlaceholder.count()) {
    const input = await firstVisible(promptByPlaceholder);
    await input.click();
    await input.fill(text);
    if (!(await clickComposerSiblingButton(input)) && !(await clickSendIfAvailable())) {
      await input.press('Enter');
    }
    return;
  }

  const promptTextboxes = page.getByRole('textbox', {name: /Prompt/i});
  if (await promptTextboxes.count()) {
    const input = await firstVisible(promptTextboxes);
    await input.click();
    await input.fill(text);
    if (!(await clickComposerSiblingButton(input)) && !(await clickSendIfAvailable())) {
      await input.press('Enter');
    }
    return;
  }

  const textareaCandidates = page.locator('textarea');
  if (await textareaCandidates.count()) {
    const input = await firstVisible(textareaCandidates);
    await input.click();
    await input.fill(text);
    if (!(await clickComposerSiblingButton(input)) && !(await clickSendIfAvailable())) {
      await input.press('Enter');
    }
    return;
  }

  const editableCandidates = page.locator('[contenteditable="true"]');
  if (await editableCandidates.count()) {
    const input = await firstVisible(editableCandidates);
    await input.click();
    await input.fill(text);
    if (!(await clickComposerSiblingButton(input)) && !(await clickSendIfAvailable())) {
      await page.keyboard.press('Enter');
    }
    return;
  }

  throw new Error('Assistant input not found');
}

function pickDatasetName(payload: any) {
  const normalize = (value: unknown) =>
    String(value || '')
      .trim()
      .replace(/^["']+|["']+$/g, '')
      .replace(/[.,;:]+$/g, '')
      .trim();
  const result = payload?.result || payload;
  const additionalData = payload?.additionalData || result?.additionalData || {};
  const explicit =
    result?.dataset ||
    result?.outputDatasetName ||
    result?.savedDatasetName ||
    result?.datasetName ||
    result?.name ||
    additionalData?.newDatasetName ||
    additionalData?.targetDatasetName ||
    additionalData?.datasetName ||
    additionalData?.outputDatasetName ||
    additionalData?.savedDatasetName ||
    '';
  if (String(explicit || '').trim()) return normalize(explicit);

  const details = String(result?.details || '').trim();
  if (!details) return '';
  const quotedDataset =
    details.match(/dataset\s+["']([^"']+)["']/i)?.[1] ||
    details.match(/as\s+["']([^"']+)["']/i)?.[1] ||
    details.match(/into\s+([A-Za-z0-9_.-]+)/i)?.[1] ||
    details.match(/in\s+([A-Za-z0-9_.-]+)/i)?.[1] ||
    details.match(/["']([^"']+)["']/)?.[1] ||
    '';
  return normalize(quotedDataset);
}

function pickH3Id(payload: any) {
  const result = payload?.result || payload;
  const direct = String(payload?.h3Id || result?.h3Id || '').trim();
  if (direct) return direct;
  const details = String(result?.details || '').trim();
  const parsed = details.match(/H3 cell\s+([0-9a-f]+)/i)?.[1] || '';
  return String(parsed || '').trim();
}

async function waitForDataset(page: any, datasetName: string, timeoutMs = 30_000) {
  const res = await runTool(page, 'waitForQMapDataset', {datasetName, timeoutMs});
  if (!res?.result?.success) {
    const datasets = await getDatasets(page);
    const summary = datasets.map(d => `${d.name}(${d.rowCount})`).join(', ');
    const details = res?.result?.details || 'no details';
    throw new Error(`waitForQMapDataset failed for "${datasetName}": ${details}. Available datasets: ${summary}`);
  }
  return res;
}

async function getDatasets(page: any) {
  return page.evaluate(() => {
    const getter = (window as any).__qmapGetDatasets;
    return typeof getter === 'function' ? getter() : [];
  });
}

async function getLayers(page: any) {
  return page.evaluate(() => {
    const getter = (window as any).__qmapGetLayers;
    return typeof getter === 'function' ? getter() : [];
  });
}

function findDataset(
  datasets: Array<{id: string; name: string}>,
  needle: string,
  options: {startsWith?: boolean; exclude?: string[]} = {}
) {
  const lower = needle.toLowerCase();
  const exclude = (options.exclude || []).map(value => value.toLowerCase());
  const accept = (dataset: {name: string}) => {
    const name = String(dataset?.name || '').toLowerCase();
    if (exclude.some(value => value && name.includes(value))) return false;
    if (name === lower) return true;
    if (options.startsWith && name.startsWith(lower)) return true;
    return name.includes(lower);
  };
  const found = datasets.find(accept);
  if (!found) {
    throw new Error(`Dataset not found for "${needle}". Available: ${datasets.map(d => d.name).join(', ')}`);
  }
  return found;
}

async function waitForLayerForDataset(page: any, datasetName: string, timeoutMs = 20_000) {
  await page.waitForFunction(
    ({datasetName}) => {
      const datasets = (window as any).__qmapGetDatasets?.() || [];
      const dataset = datasets.find((d: any) => d.name === datasetName || d.id === datasetName);
      if (!dataset) return false;
      const layers = (window as any).__qmapGetLayers?.() || [];
      return layers.some((layer: any) => String(layer?.datasetId || '') === String(dataset.id));
    },
    {datasetName},
    {timeout: timeoutMs}
  );
}

test.describe.configure({mode: 'serial'});

test.beforeEach(async () => {
  currentToolTrace = [];
});

test.afterEach(async ({}, testInfo) => { // eslint-disable-line no-empty-pattern
  if (testInfo.status === testInfo.expectedStatus) return;
  const artifact = {
    test: testInfo.title,
    file: testInfo.file,
    strictBackendAuth: REQUIRE_BACKEND_AUTH,
    lastToolRun: currentToolTrace[currentToolTrace.length - 1] || null,
    recentToolRuns: currentToolTrace.slice(-12)
  };
  const artifactPath = testInfo.outputPath('latest-tool-runtime-envelope.json');
  fs.writeFileSync(artifactPath, JSON.stringify(artifact, null, 2), 'utf8');
  await testInfo.attach('latest-tool-runtime-envelope', {
    path: artifactPath,
    contentType: 'application/json'
  });
});

test('q-map tools functional coverage', async ({page}) => {
  test.setTimeout(300_000);

  await page.addInitScript(() => {
    (window as any).__QMAP_E2E_TOOLS__ = true;
  });

  await page.goto('/');
  await openAssistant(page);
  await waitForToolRunner(page);
  await loadGeojsonFixtures(page);

  await page.waitForFunction(() => {
    const datasets = (window as any).__qmapGetDatasets?.() || [];
    return datasets.length >= 5;
  }, null, {timeout: 30_000});

  const datasets = await getDatasets(page);
  const polygon = findDataset(datasets, 'polygon', {startsWith: true, exclude: ['adjacent']});
  const population = findDataset(datasets, 'population_polygons', {startsWith: true});
  const timeseries = findDataset(datasets, 'timeseries_points', {startsWith: true});
  const adjacent = findDataset(datasets, 'adjacent_polygons', {startsWith: true});
  const splitLine = findDataset(datasets, 'split_line', {startsWith: true});
  const populationBaseRows = Number((population as any)?.rowCount || 0);
  expect(populationBaseRows).toBeGreaterThan(0);

  await runToolExpectSuccess(page, 'listQMapDatasets');
  const chartToolsRes = await runToolExpectSuccess(page, 'listQMapChartTools');
  const chartDetails = String(chartToolsRes?.result?.details || '').toLowerCase();
  expect(chartDetails).toContain('enabled chart tools');
  expect(chartDetails).toContain('categorybarstool');
  expect(chartDetails).toContain('wordcloudtool');
  await runToolExpectSuccess(page, 'histogramTool', {
    datasetName: population.name,
    variableName: 'population',
    numberOfBins: 8
  });
  await runToolExpectSuccess(page, 'boxplotTool', {
    datasetName: population.name,
    variableNames: ['population']
  });
  const categoriesRes = await runToolExpectSuccess(page, 'listQMapToolCategories');
  expect(String(categoriesRes?.result?.details || '').toLowerCase()).toContain('discovery');
  const discoveryToolsRes = await runToolExpectSuccess(page, 'listQMapToolsByCategory', {
    categoryKey: 'discovery',
    includeDescriptions: true
  });
  expect(String(discoveryToolsRes?.result?.details || '').toLowerCase()).toContain('category "discovery"');

  await runToolExpectSuccess(page, 'deriveQMapDatasetBbox', {datasetName: polygon.name});
  await runToolExpectSuccess(page, 'previewQMapDatasetRows', {datasetName: population.name, limit: 2});
  const rankRes = await runToolExpectSuccess(page, 'rankQMapDatasetRows', {
    datasetName: population.name,
    metricFieldName: 'population',
    topN: 100,
    sortDirection: 'desc',
    fields: ['name', 'population']
  });
  expect(String(rankRes?.result?.details || '').toLowerCase()).toContain('ranked top');
  const filterTextFallbackRes = await runToolExpectSuccess(page, 'createDatasetFromFilter', {
    datasetName: population.name,
    fieldName: 'name',
    operator: 'eq',
    value: 0,
    newDatasetName: 'population_name_rank_fallback_e2e'
  });
  const filterTextFallbackName = pickDatasetName(filterTextFallbackRes);
  expect(filterTextFallbackName).toBeTruthy();
  await waitForDataset(page, filterTextFallbackName);
  expect(String(filterTextFallbackRes?.result?.details || '').toLowerCase()).toContain(
    'auto-recovered text filter value'
  );
  await runToolExpectSuccess(page, 'distinctQMapFieldValues', {datasetName: population.name, fieldName: 'name'});
  await runToolExpectSuccess(page, 'searchQMapFieldValues', {
    datasetName: population.name,
    fieldName: 'name',
    contains: 'poly'
  });
  await runToolExpectSuccess(page, 'summarizeQMapTimeSeries', {
    datasetName: timeseries.name,
    timeFieldName: 'timestamp',
    valueFieldName: 'value',
    groupFieldName: 'sensor',
    groupValue: 'A',
    limit: 10
  });
  const aggregateTsRes = await runToolExpectSuccess(page, 'aggregateQMapTimeSeries', {
    datasetName: timeseries.name,
    timeFieldName: 'timestamp',
    valueFieldName: 'value',
    windowUnit: 'month',
    aggregation: 'avg',
    detectTrend: true
  });
  expect(['increasing', 'decreasing', 'stable']).toContain(aggregateTsRes?.result?.trend);
  expect(Number(aggregateTsRes?.result?.buckets_count ?? 0)).toBeGreaterThan(0);
  await runToolExpectSuccess(page, 'wordCloudTool', {datasetName: population.name, textFieldName: 'name'});
  await runToolExpectSuccess(page, 'categoryBarsTool', {datasetName: population.name, categoryFieldName: 'name'});
  await runToolExpectSuccess(page, 'grammarAnalyzeTool', {datasetName: population.name, textFieldName: 'name'});
  await runToolExpectSuccess(page, 'countQMapRows', {datasetName: population.name});
  await runToolExpectSuccess(page, 'debugQMapActiveFilters');

  const filteredRes = await runToolExpectSuccess(page, 'createDatasetFromFilter', {
    datasetName: population.name,
    fieldName: 'population',
    operator: 'gt',
    value: 1000
  });
  const filteredName = pickDatasetName(filteredRes);
  expect(filteredName).toBeTruthy();
  await waitForDataset(page, filteredName);
  const datasetsAfterFilter = await getDatasets(page);
  const filteredMeta = findDataset(datasetsAfterFilter as any, filteredName) as any;
  const filteredRows = Number(filteredMeta?.rowCount || 0);
  expect(filteredRows).toBeGreaterThan(0);
  expect(filteredRows).toBeLessThanOrEqual(populationBaseRows);

  const highExceedanceRes = await runToolExpectSuccess(page, 'createDatasetFromFilter', {
    datasetName: population.name,
    fieldName: 'population',
    operator: 'gt',
    value: 10000,
    newDatasetName: 'population_gt_10000_e2e'
  });
  const highExceedanceName = pickDatasetName(highExceedanceRes);
  expect(highExceedanceName).toBeTruthy();
  await waitForDataset(page, highExceedanceName);
  const datasetsAfterHighExceedance = await getDatasets(page);
  const highExceedanceMeta = findDataset(datasetsAfterHighExceedance as any, highExceedanceName) as any;
  const highExceedanceRows = Number(highExceedanceMeta?.rowCount || 0);
  expect(highExceedanceRows).toBeGreaterThan(0);
  expect(highExceedanceRows).toBeLessThan(filteredRows);

  const areaRes = await runToolExpectSuccess(page, 'createDatasetWithGeometryArea', {
    datasetName: polygon.name,
    areaFieldName: 'area_m2'
  });
  const areaName = pickDatasetName(areaRes);
  expect(areaName).toBeTruthy();
  await waitForDataset(page, areaName);
  const datasetsAfterArea = await getDatasets(page);
  const areaMeta = findDataset(datasetsAfterArea as any, areaName) as any;
  expect(areaMeta?.fields || []).toContain('area_m2');

  await runToolExpectSuccess(page, 'createDatasetWithGeometryArea', {
    datasetName: polygon.name,
    newDatasetName: areaName,
    areaFieldName: 'area_m2_alt'
  });
  await waitForDataset(page, areaName);
  const datasetsAfterAreaUpsert = await getDatasets(page);
  const areaMetaUpsert = findDataset(datasetsAfterAreaUpsert as any, areaName) as any;
  expect(areaMetaUpsert?.fields || []).toContain('area_m2_alt');
  expect(areaMetaUpsert?.fields || []).not.toContain('area_m2');

  const normalizedRes = await runToolExpectSuccess(page, 'createDatasetWithNormalizedField', {
    datasetName: population.name,
    numeratorFieldName: 'population',
    denominatorFieldName: 'population',
    outputFieldName: 'population_per_100k'
  });
  const normalizedName = pickDatasetName(normalizedRes);
  expect(normalizedName).toBeTruthy();
  await waitForDataset(page, normalizedName);

  const mergeRes = await runToolExpectSuccess(page, 'mergeQMapDatasets', {
    datasetNames: [polygon.name, population.name],
    newDatasetName: 'merged_fixture_dataset',
    showOnMap: false,
    geometryMode: 'auto'
  });
  const mergeName = pickDatasetName(mergeRes);
  expect(mergeName).toBeTruthy();
  await waitForDataset(page, mergeName);

  const bboxRes = await runToolExpectSuccess(page, 'drawQMapBoundingBox', {
    datasetName: population.name,
    showOnMap: false,
    newDatasetName: 'population_bbox_e2e'
  });
  const bboxName = pickDatasetName(bboxRes);
  expect(bboxName).toBeTruthy();
  await waitForDataset(page, bboxName);

  const reprojectRes = await runToolExpectSuccess(page, 'reprojectQMapDatasetCrs', {
    datasetName: polygon.name,
    sourceCrs: 'EPSG:4326',
    targetCrs: 'EPSG:3857'
  });
  const reprojectName = pickDatasetName(reprojectRes);
  expect(reprojectName).toBeTruthy();
  await waitForDataset(page, reprojectName);

  const clipRes = await runToolExpectSuccess(page, 'clipQMapDatasetByGeometry', {
    sourceDatasetName: population.name,
    clipDatasetName: polygon.name,
    mode: 'intersects',
    includeDistinctPropertyValueCounts: true
  });
  const clipName = pickDatasetName(clipRes);
  expect(clipName).toBeTruthy();
  await waitForDataset(page, clipName);
  const datasetsAfterClip = await getDatasets(page);
  const clipDatasetMeta = findDataset(datasetsAfterClip as any, clipName) as any;
  expect(Array.isArray(clipDatasetMeta?.fields)).toBeTruthy();
  expect(clipDatasetMeta.fields).toContain('name__test_polygon__count');

  const boundaryRes = await runToolExpectSuccess(page, 'clipDatasetByBoundary', {
    sourceDatasetName: population.name,
    boundaryDatasetName: polygon.name,
    mode: 'intersects'
  });
  const boundaryName = pickDatasetName(boundaryRes);
  expect(boundaryName).toBeTruthy();
  await waitForDataset(page, boundaryName);

  const joinRes = await runToolExpectSuccess(page, 'spatialJoinByPredicate', {
    leftDatasetName: polygon.name,
    rightDatasetName: population.name,
    predicate: 'intersects',
    rightValueField: 'population',
    aggregations: ['count', 'sum']
  });
  const joinName = pickDatasetName(joinRes);
  expect(joinName).toBeTruthy();
  await waitForDataset(page, joinName);

  const zonalRes = await runToolExpectSuccess(page, 'zonalStatsByAdmin', {
    adminDatasetName: polygon.name,
    valueDatasetName: population.name,
    valueField: 'population',
    aggregation: 'sum'
  });
  const zonalName = pickDatasetName(zonalRes);
  expect(zonalName).toBeTruthy();
  await waitForDataset(page, zonalName);

  const overlayRes = await runToolExpectSuccess(page, 'overlayDifference', {
    datasetAName: polygon.name,
    datasetBName: population.name
  });
  const overlayName = pickDatasetName(overlayRes);
  expect(overlayName).toBeTruthy();
  await waitForDataset(page, overlayName);

  const overlayUnionRes = await runToolExpectSuccess(page, 'overlayUnion', {
    datasetAName: polygon.name,
    datasetBName: population.name,
    newDatasetName: 'overlay_union_e2e'
  });
  expect(overlayUnionRes?.result?.success).toBeTruthy();
  await waitForDataset(page, 'overlay_union_e2e');

  const overlayIntersectionRes = await runToolExpectSuccess(page, 'overlayIntersection', {
    datasetAName: polygon.name,
    datasetBName: population.name,
    newDatasetName: 'overlay_intersection_e2e'
  });
  expect(overlayIntersectionRes?.result?.success).toBeTruthy();
  await waitForDataset(page, 'overlay_intersection_e2e');

  const overlaySymDiffRes = await runToolExpectSuccess(page, 'overlaySymmetricDifference', {
    datasetAName: polygon.name,
    datasetBName: population.name,
    newDatasetName: 'overlay_symdiff_e2e'
  });
  expect(overlaySymDiffRes?.result?.success).toBeTruthy();
  await waitForDataset(page, 'overlay_symdiff_e2e');

  const dissolveRes = await runToolExpectSuccess(page, 'dissolveQMapDatasetByField', {
    datasetName: population.name,
    groupByField: 'name',
    newDatasetName: 'population_dissolved_e2e'
  });
  expect(dissolveRes?.result?.success).toBeTruthy();
  await waitForDataset(page, 'population_dissolved_e2e');

  const simplifyRes = await runToolExpectSuccess(page, 'simplifyQMapDatasetGeometry', {
    datasetName: population.name,
    tolerance: 0.0001,
    minAreaM2: 0,
    newDatasetName: 'population_simplified_e2e'
  });
  expect(simplifyRes?.result?.success).toBeTruthy();
  await waitForDataset(page, 'population_simplified_e2e');

  const splitRes = await runToolExpectSuccess(page, 'splitQMapPolygonByLine', {
    polygonDatasetName: polygon.name,
    lineDatasetName: splitLine.name,
    lineBufferMeters: 1,
    newDatasetName: 'polygon_split_e2e'
  });
  expect(splitRes?.result?.success).toBeTruthy();
  await waitForDataset(page, 'polygon_split_e2e');

  const eraseRes = await runToolExpectSuccess(page, 'eraseQMapDatasetByGeometry', {
    sourceDatasetName: polygon.name,
    maskDatasetName: population.name,
    newDatasetName: 'polygon_erased_e2e'
  });
  expect(eraseRes?.result?.success).toBeTruthy();
  await waitForDataset(page, 'polygon_erased_e2e');

  const bufferRes = await runToolExpectSuccess(page, 'bufferAndSummarize', {
    sourceDatasetName: population.name,
    targetDatasetName: population.name,
    radiusKm: 5,
    targetValueField: 'population',
    aggregation: 'sum'
  });
  const bufferName = pickDatasetName(bufferRes);
  expect(bufferName).toBeTruthy();
  await waitForDataset(page, bufferName);

  const nearestRes = await runToolExpectSuccess(page, 'nearestFeatureJoin', {
    sourceDatasetName: population.name,
    targetDatasetName: population.name,
    includeTargetField: 'name'
  });
  const nearestName = pickDatasetName(nearestRes);
  expect(nearestName).toBeTruthy();
  await waitForDataset(page, nearestName);

  const adjacencyRes = await runToolExpectSuccess(page, 'adjacencyGraphFromPolygons', {
    datasetName: adjacent.name,
    predicate: 'touches',
    idField: 'id'
  });
  const adjacencyName = pickDatasetName(adjacencyRes);
  expect(adjacencyName).toBeTruthy();
  await waitForDataset(page, adjacencyName);

  const qcumberProbe = await probeBackendTool(page, 'listQCumberProviders', {});
  if (!qcumberProbe.success && shouldSkipBackendInfraFailure(qcumberProbe.details)) {
    test.info().annotations.push({
      type: 'warning',
      description:
        `Skipping q-cumber query coverage in this run (backend auth/connectivity not ready): ${qcumberProbe.details}. ` +
        'Set QMAP_E2E_REQUIRE_BACKEND_AUTH=true to enforce hard fail.'
    });
  } else {
    if (!qcumberProbe.success) {
      throw new Error(`Tool "listQCumberProviders" failed: ${qcumberProbe.details || 'no details'}`);
    }
    await runToolExpectSuccess(page, 'queryQCumberDataset', {
      providerId: 'local-assets-it',
      datasetId: 'kontur-boundaries-italia',
      filters: [{field: 'lv', op: 'eq', value: 4, values: []}],
      orderBy: 'population',
      orderDirection: 'desc',
      limit: 5,
      offset: 0,
      loadToMap: false
    });
    await runToolExpectSuccess(page, 'getQCumberDatasetHelp', {
      providerId: 'local-assets-it',
      datasetId: 'kontur-boundaries-italia'
    });
    await runToolExpectSuccess(page, 'queryQCumberTerritorialUnits', {
      providerId: 'local-assets-it',
      datasetId: 'kontur-boundaries-italia',
      filters: [{field: 'lv', op: 'eq', value: 4, values: []}],
      orderBy: 'population',
      orderDirection: 'desc',
      limit: 5,
      offset: 0,
      loadToMap: false,
      expectedAdminType: 'region'
    });
    await runToolExpectSuccess(page, 'queryQCumberTerritorialUnits', {
      providerId: 'local-assets-it',
      datasetId: 'kontur-boundaries-italia',
      filters: [{field: 'lv', op: 'eq', value: 9, values: []}],
      orderBy: 'population',
      orderDirection: 'desc',
      limit: 5,
      offset: 0,
      loadToMap: false,
      expectedAdminType: 'municipality/comune'
    });
    await runToolExpectSuccess(page, 'queryQCumberDatasetSpatial', {
      providerId: 'local-assets-it',
      datasetId: 'clc-2018-italia',
      filters: [{field: 'code_18', op: 'eq', value: '311', values: []}],
      orderBy: 'area_ha',
      orderDirection: 'desc',
      limit: 5,
      offset: 0,
      loadToMap: false
    });
  }

  const coverageRes = await runToolExpectSuccess(page, 'coverageQualityReport', {
    leftDatasetName: population.name,
    rightDatasetName: polygon.name,
    predicate: 'intersects'
  });
  const coverageFromReport = Number(coverageRes?.result?.report?.coveragePct);
  const coverageDetails = String(coverageRes?.result?.details || '');
  const coverageFromDetails = Number(coverageDetails.match(/coverage\s+([0-9]+(?:\.[0-9]+)?)%/i)?.[1]);
  const coveragePct = Number.isFinite(coverageFromReport) ? coverageFromReport : coverageFromDetails;
  expect(Number.isFinite(coveragePct)).toBeTruthy();
  expect(coveragePct).toBeGreaterThan(0);

  const tessRes = await runToolExpectSuccess(page, 'tassellateDatasetLayer', {
    datasetName: population.name,
    resolution: 4
  });
  const tessName = pickDatasetName(tessRes) || 'Tassellation';
  await waitForDataset(page, tessName, 60_000);

  await page.evaluate(
    ({datasetName}) => (window as any).__qmapSelectFirstFeature?.(datasetName),
    {datasetName: population.name}
  );
  await runToolExpectSuccess(page, 'tassellateSelectedGeometry', {resolution: 4});
  await waitForDataset(page, tessName, 60_000);

  const aggRes = await runToolExpectSuccess(page, 'aggregateDatasetToH3', {
    datasetName: population.name,
    resolution: 4,
    valueField: 'population',
    operations: ['count', 'sum']
  });
  const aggName = pickDatasetName(aggRes);
  expect(aggName).toBeTruthy();
  await waitForDataset(page, aggName, 60_000);
  await waitForLayerForDataset(page, aggName);

  const h3JoinRes = await runToolExpectSuccess(page, 'joinQMapDatasetsOnH3', {
    leftDatasetName: tessName,
    rightDatasetName: aggName,
    includeRightFields: ['sum', 'count'],
    metric: 'sum',
    joinType: 'left'
  });
  const h3JoinName = pickDatasetName(h3JoinRes);
  expect(h3JoinName).toBeTruthy();
  await waitForDataset(page, h3JoinName, 60_000);

  const normalizedH3Res = await runToolExpectSuccess(page, 'createDatasetWithNormalizedField', {
    datasetName: h3JoinName,
    numeratorFieldName: 'sum',
    denominatorFieldName: 'h3_area_ha',
    outputFieldName: 'sum_pct_area',
    multiplier: 100
  });
  const normalizedH3Name = pickDatasetName(normalizedH3Res);
  expect(normalizedH3Name).toBeTruthy();
  await waitForDataset(page, normalizedH3Name, 60_000);
  const datasetsAfterNormalizedH3 = await getDatasets(page);
  const normalizedH3Meta = findDataset(datasetsAfterNormalizedH3 as any, normalizedH3Name) as any;
  expect(normalizedH3Meta?.fields || []).toContain('sum_pct_area');
  expect(String(normalizedH3Meta?.id || '')).toBeTruthy();
  await runToolExpectSuccess(page, 'waitForQMapDataset', {
    datasetName: normalizedH3Name,
    timeoutMs: 60_000
  });

  const populateRes = await runToolExpectSuccess(page, 'populateTassellationFromAdminUnits', {
    tessellationDatasetName: tessName,
    sourceDatasetName: population.name,
    sourceValueField: 'population',
    resolution: 4
  });
  const populateName = pickDatasetName(populateRes);
  expect(populateName).toBeTruthy();
  await waitForDataset(page, populateName, 60_000);

  const populateAreaRes = await runToolExpectSuccess(page, 'populateTassellationFromAdminUnits', {
    tessellationDatasetName: tessName,
    sourceDatasetName: population.name,
    sourceValueField: 'population',
    resolution: 4,
    allocationMode: 'area_weighted'
  });
  const populateAreaName = pickDatasetName(populateAreaRes);
  expect(populateAreaName).toBeTruthy();
  await waitForDataset(page, populateAreaName, 60_000);

  const populateDiscreteRes = await runToolExpectSuccess(page, 'populateTassellationFromAdminUnits', {
    tessellationDatasetName: tessName,
    sourceDatasetName: population.name,
    sourceValueField: 'population',
    resolution: 4,
    allocationMode: 'discrete',
    allocationSubMode: 'centroid'
  });
  const populateDiscreteName = pickDatasetName(populateDiscreteRes);
  expect(populateDiscreteName).toBeTruthy();
  await waitForDataset(page, populateDiscreteName, 60_000);

  const paintRes = await runToolExpectSuccess(page, 'paintQMapH3Cell', {
    lat: 45.0,
    lng: 12.0,
    resolution: 6
  });
  const paintDataset = pickDatasetName(paintRes);
  expect(paintDataset).toBeTruthy();
  await waitForDataset(page, paintDataset);

  await runToolExpectSuccess(page, 'paintQMapH3Cells', {
    resolution: 6,
    points: [
      {lat: 45.0, lng: 12.0},
      {lat: 45.2, lng: 12.2}
    ]
  });

  const paintH3Id = pickH3Id(paintRes);
  expect(paintH3Id).toBeTruthy();
  await runToolExpectSuccess(page, 'paintQMapH3Ring', {
    centerH3: paintH3Id,
    k: 1
  });

  await runToolExpectSuccess(page, 'fitQMapToDataset', {datasetName: polygon.name});
  await runToolExpectSuccess(page, 'setQMapTooltipFields', {
    datasetName: population.name,
    fieldNames: ['name', 'population']
  });
  await runToolExpectSuccess(page, 'setQMapLayerColorByField', {
    datasetName: population.name,
    fieldName: 'population'
  });
  await runToolExpectSuccess(page, 'setQMapLayerColorByThresholds', {
    datasetName: population.name,
    fieldName: 'population',
    thresholds: [2000, 10000]
  });
  await runToolExpectSuccess(page, 'setQMapLayerColorByStatsThresholds', {
    datasetName: population.name,
    fieldName: 'population',
    strategy: 'quantiles',
    classes: 3
  });
  await runToolExpectSuccess(page, 'setQMapLayerSolidColor', {
    datasetName: population.name,
    fillColor: '#ff0000'
  });
  await runToolExpectSuccess(page, 'applyQMapStylePreset', {
    presetName: 'comuni_population',
    datasetName: population.name
  });
  await runToolExpectSuccess(page, 'setQMapLayerHeightByField', {
    datasetName: aggName,
    fieldName: 'count'
  });

  const layers = await getLayers(page);
  expect(layers.length).toBeGreaterThan(0);
  const layerA = layers[0];
  const layerB = layers[1] || layers[0];

  await runToolExpectSuccess(page, 'setQMapLayerSolidColor', {
    datasetName: population.name,
    fillColor: '#00ff00'
  });

  await runToolExpectSuccess(page, 'setQMapLayerVisibility', {
    layerNameOrId: layerA.name,
    visible: true
  });
  await runToolExpectSuccess(page, 'showOnlyQMapLayer', {layerNameOrId: layerB.name});
  await runToolExpectSuccess(page, 'setQMapLayerOrder', {
    layerNameOrId: layerA.name,
    referenceLayerNameOrId: layerB.name,
    position: 'above'
  });
  await runToolExpectSuccess(page, 'openQMapPanel', {panelId: 'layer'});

  const setFilterRes = await runTool(page, 'setQMapFieldEqualsFilter', {
    datasetName: population.name,
    fieldName: 'name',
    value: 'poly_low'
  });
  const setFilterDetails = getToolFailureDetails(setFilterRes);
  const setFilterBackendUnavailable = !setFilterRes?.result?.success && shouldSkipBackendInfraFailure(setFilterDetails);
  if (setFilterBackendUnavailable) {
    test.info().annotations.push({
      type: 'warning',
      description:
        `Skipping filter-materialization coverage in this run (backend auth/connectivity not ready): ${setFilterDetails}. ` +
        'Set QMAP_E2E_REQUIRE_BACKEND_AUTH=true to enforce hard fail.'
    });
  } else {
    if (!setFilterRes?.result?.success) {
      throw new Error(`Tool "setQMapFieldEqualsFilter" failed: ${setFilterDetails || 'no details'}`);
    }
    await runToolExpectSuccess(page, 'debugQMapActiveFilters', {datasetName: population.name});
    const currentFiltersRes = await runToolExpectSuccess(page, 'createDatasetFromCurrentFilters', {
      datasetName: population.name
    });
    const currentFiltersName = pickDatasetName(currentFiltersRes);
    expect(currentFiltersName).toBeTruthy();
    await waitForDataset(page, currentFiltersName);
  }

  // computeQMapBivariateCorrelation
  const bivariateRes = await runToolExpectSuccess(page, 'computeQMapBivariateCorrelation', {
    datasetName: population.name,
    fieldA: 'population',
    fieldB: 'population',
    weightType: 'queen',
    permutations: 99,
    significance: 0.05
  });
  expect(bivariateRes?.result?.success).toBe(true);
  const bivariateName = pickDatasetName(bivariateRes);
  if (bivariateName) {
    await waitForDataset(page, bivariateName);
    const bivaRows = await runToolExpectSuccess(page, 'countQMapRows', {datasetName: bivariateName});
    expect(Number(bivaRows?.result?.count ?? 0)).toBeGreaterThan(0);
  }

  // computeQMapDatasetDelta (same dataset as baseline and current → all stable)
  const deltaRes = await runToolExpectSuccess(page, 'computeQMapDatasetDelta', {
    baselineDatasetName: population.name,
    currentDatasetName: population.name,
    joinKeyField: 'name',
    numericFields: ['population'],
    includeUnchangedRows: true
  });
  expect(deltaRes?.result?.success).toBe(true);
  const deltaName = pickDatasetName(deltaRes);
  if (deltaName) {
    await waitForDataset(page, deltaName);
    const deltaRows = await runToolExpectSuccess(page, 'countQMapRows', {datasetName: deltaName});
    expect(Number(deltaRows?.result?.count ?? 0)).toBeGreaterThan(0);
  }

  // describeQMapField — pure execute, no component, no dataset mutation
  const describeRes = await runToolExpectSuccess(page, 'describeQMapField', {
    datasetName: population.name,
    fieldName: 'population',
    useActiveFilters: false,
    percentiles: [5, 25, 50, 75, 95]
  });
  expect(describeRes?.result?.success).toBe(true);
  expect(Number.isFinite(describeRes?.result?.mean)).toBe(true);
  expect(Number(describeRes?.result?.count ?? 0)).toBeGreaterThan(0);
  expect(Number.isFinite(describeRes?.result?.std)).toBe(true);

  // addComputedField — creates derived dataset with new computed column
  const computedRes = await runToolExpectSuccess(page, 'addComputedField', {
    datasetName: population.name,
    expression: 'population * 2',
    outputFieldName: 'population_doubled',
    useActiveFilters: false,
    showOnMap: true
  });
  expect(computedRes?.result?.success).toBe(true);
  const computedName = pickDatasetName(computedRes);
  if (computedName) {
    await waitForDataset(page, computedName);
    const computedRows = await runToolExpectSuccess(page, 'countQMapRows', {datasetName: computedName});
    expect(Number(computedRows?.result?.count ?? 0)).toBeGreaterThan(0);
  }

  // computeQMapHotspotAnalysis — Getis-Ord Gi* hotspot/coldspot analysis
  const hotspotRes = await runToolExpectSuccess(page, 'computeQMapHotspotAnalysis', {
    datasetName: population.name,
    valueField: 'population',
    weightType: 'queen',
    significance: 0.05
  });
  expect(hotspotRes?.result?.success).toBe(true);
  const hotspotName = pickDatasetName(hotspotRes);
  if (hotspotName) {
    await waitForDataset(page, hotspotName);
    const hotspotRows = await runToolExpectSuccess(page, 'countQMapRows', {datasetName: hotspotName});
    expect(Number(hotspotRows?.result?.count ?? 0)).toBeGreaterThan(0);
    const catalog: string[] = hotspotRes?.result?.fieldCatalog ?? [];
    expect(catalog).toContain('hotspot_class');
    expect(catalog).toContain('hotspot_z');
    expect(catalog).toContain('hotspot_p');
  }

  // computeQMapCompositeIndex — multi-field weighted composite score
  const compositeRes = await runToolExpectSuccess(page, 'computeQMapCompositeIndex', {
    datasetName: population.name,
    components: [
      {fieldName: 'population', weight: 0.6, direction: 'asc'},
      {fieldName: 'population', weight: 0.4, direction: 'desc'}
    ],
    outputFieldName: 'composite_score',
    normalize: true
  });
  expect(compositeRes?.result?.success).toBe(true);
  const compositeName = pickDatasetName(compositeRes);
  if (compositeName) {
    await waitForDataset(page, compositeName);
    const compositeRows = await runToolExpectSuccess(page, 'countQMapRows', {datasetName: compositeName});
    expect(Number(compositeRows?.result?.count ?? 0)).toBeGreaterThan(0);
  }

  // computeQMapDataQualityReport — read-only field quality audit, no dataset mutation
  const qualityRes = await runToolExpectSuccess(page, 'computeQMapDataQualityReport', {
    datasetName: population.name,
    outlierMethod: 'iqr'
  });
  expect(qualityRes?.result?.success).toBe(true);
  expect(Number(qualityRes?.result?.fieldsAudited ?? 0)).toBeGreaterThan(0);
  expect(Array.isArray(qualityRes?.result?.fieldReports)).toBe(true);
});

test('q-map base tools smoke coverage', async ({page}) => {
  test.setTimeout(180_000);

  await page.addInitScript(() => {
    (window as any).__QMAP_E2E_TOOLS__ = true;
  });

  await page.goto('/');
  await openAssistant(page);
  await waitForToolRunner(page);

  await runToolExpectHandled(page, 'basemap', {});
  await runToolExpectHandled(page, 'loadData', {});
  await runToolExpectHandled(page, 'mapBoundary', {});
  await runToolExpectHandled(page, 'saveDataToMap', {});
  await runToolExpectHandled(page, 'geocoding', {});
  await runToolExpectHandled(page, 'routing', {});
  await runToolExpectHandled(page, 'isochrone', {});
  await runToolExpectHandled(page, 'roads', {});
  await runToolExpectHandled(page, 'bubbleChartTool', {}, {allowUnknownTool: true});
  await runToolExpectHandled(page, 'pcpTool', {}, {allowUnknownTool: true});
  await runToolExpectHandled(page, 'lineChartTool', {}, {allowUnknownTool: true});
  await runToolExpectHandled(page, 'scatterplotTool', {}, {allowUnknownTool: true});
});

test('assistant response exposes structured diagnostics envelope', async ({page}) => {
  test.setTimeout(120_000);

  const requestId = 'req-e2e-structured-1';
  const chatId = 'chat-e2e-structured-1';
  const observedRequestBodies: any[] = [];

  await page.route(/\/chat(\/completions)?(\?.*)?$/, async route => {
    const body = route.request().postDataJSON();
    observedRequestBodies.push(body);
    const streamBody =
      `0:${JSON.stringify('Conferma operativa completata.')}\n` +
      `d:${JSON.stringify({
        finishReason: 'stop',
        usage: {promptTokens: 10, completionTokens: 5}
      })}\n`;
    await route.fulfill({
      status: 200,
      headers: {
        'content-type': 'text/plain; charset=utf-8',
        'access-control-expose-headers': 'x-q-assistant-request-id, x-q-assistant-chat-id',
        'x-q-assistant-request-id': requestId,
        'x-q-assistant-chat-id': chatId
      },
      body: streamBody
    });
  });

  await page.goto('/');
  await openAssistant(page);

  await sendAssistantMessage(page, 'Conferma esito operativo in formato sintetico.');
  await expect
    .poll(() => observedRequestBodies.length, {timeout: 20_000})
    .toBeGreaterThan(0);

  await expect(page.getByText(new RegExp(`\\[requestId:\\s*${requestId}\\]`, 'i')).first()).toBeVisible({
    timeout: 20_000
  });
  await expect(page.getByText(/\[executionSummary\]\s*\{/).first()).toBeVisible({timeout: 20_000});

  const executionSummary = await page.evaluate(() => {
    const match = document.body.innerText.match(/\[executionSummary\]\s*(\{[^\n]+\})/);
    if (!match) return null;
    try {
      return JSON.parse(match[1]);
    } catch {
      return {parseError: true};
    }
  });
  expect(executionSummary).toBeTruthy();
  expect((executionSummary as any)?.parseError).toBeUndefined();
  expect((executionSummary as any)?.requestId).toBe(requestId);
  expect((executionSummary as any)?.chatId).toBe(chatId);
  expect(typeof (executionSummary as any)?.status).toBe('string');
  expect(typeof (executionSummary as any)?.steps?.total).toBe('number');
  expect(typeof (executionSummary as any)?.steps?.completed).toBe('number');
  expect(Array.isArray(observedRequestBodies)).toBeTruthy();
  expect(observedRequestBodies.length).toBeGreaterThan(0);
  expect(Array.isArray(observedRequestBodies[0]?.messages)).toBeTruthy();
});

test.describe('q-map cloud tools', () => {
  test('cloud tool coverage', async ({page}) => {
    test.setTimeout(180_000);
    await page.addInitScript(() => {
      (window as any).__QMAP_E2E_TOOLS__ = true;
    });
    await page.goto('/');
    await openAssistant(page);
    await waitForToolRunner(page);

    const cloudMapsRes = await runTool(page, 'listQMapCloudMaps', {});
    const cloudMapsDetails = getToolFailureDetails(cloudMapsRes);
    if (!cloudMapsRes?.result?.success) {
      if (shouldSkipBackendInfraFailure(cloudMapsDetails)) {
        test.info().annotations.push({
          type: 'warning',
          description:
            `Skipping cloud tool coverage in this run (backend auth/connectivity not ready): ${cloudMapsDetails}. ` +
            'Set QMAP_E2E_REQUIRE_BACKEND_AUTH=true to enforce hard fail.'
        });
        return;
      }
      throw new Error(`Tool "listQMapCloudMaps" failed: ${cloudMapsDetails || 'no details'}`);
    }
    const loadCloudMapRes = await runTool(page, 'loadQMapCloudMap', {
      provider: 'q-storage-backend',
      mapId: ''
    });
    expect(loadCloudMapRes?.result?.success).toBeFalsy();
    expect(String(loadCloudMapRes?.result?.details || '')).toContain('Missing mapId');
    const loadCloudWaitRes = await runTool(page, 'loadCloudMapAndWait', {
      provider: 'q-storage-backend',
      mapId: ''
    });
    expect(loadCloudWaitRes?.result?.success).toBeFalsy();
    expect(String(loadCloudWaitRes?.result?.details || '')).toContain('Missing mapId');
    const providersRes = await runToolExpectSuccess(page, 'listQCumberProviders', {});
    const providers = Array.isArray((providersRes as any)?.result?.providers) ? (providersRes as any).result.providers : [];
    const providerId = String(providers[0]?.id || '').trim();
    if (!providerId) {
      throw new Error('No provider id available from listQCumberProviders');
    }
    await runToolExpectSuccess(page, 'listQCumberDatasets', {providerId});
  });

  test('listQCumberDatasets rejects invalid providerId literal', async ({page}) => {
    test.setTimeout(180_000);
    await page.addInitScript(() => {
      (window as any).__QMAP_E2E_TOOLS__ = true;
    });
    await page.goto('/');
    await openAssistant(page);
    await waitForToolRunner(page);

    const readinessProbe = await probeBackendTool(page, 'listQCumberProviders', {});
    if (!readinessProbe.success && shouldSkipBackendInfraFailure(readinessProbe.details)) {
      test.info().annotations.push({
        type: 'warning',
        description:
          `Skipping invalid providerId coverage in this run (backend auth/connectivity not ready): ${readinessProbe.details}. ` +
          'Set QMAP_E2E_REQUIRE_BACKEND_AUTH=true to enforce hard fail.'
      });
      return;
    }
    if (!readinessProbe.success) {
      throw new Error(`Tool "listQCumberProviders" failed: ${readinessProbe.details || 'no details'}`);
    }
    const res = await runTool(page, 'listQCumberDatasets', {providerId: '[object]'});
    expect(res?.result?.success).toBeFalsy();
    expect(String(res?.result?.details || '')).toContain('Invalid providerId');
  });

  test('parallel mutation race via __qmapRunTool produces no race condition', async ({page}) => {
    test.setTimeout(180_000);
    await page.addInitScript(() => {
      (window as any).__QMAP_E2E_TOOLS__ = true;
    });
    await page.goto('/');
    await openAssistant(page);
    await waitForToolRunner(page);
    await loadGeojsonFixtures(page);

    const datasets = (await getDatasets(page)) as any[];
    const population = datasets.find((d: any) => /population/i.test(d.name));
    const polygon = datasets.find((d: any) => /polygon/i.test(d.name) && !/population/i.test(d.name));
    expect(population).toBeTruthy();
    expect(polygon).toBeTruthy();

    // Launch 3 mutations in parallel — they should serialize and all succeed
    const [clip1, clip2, clip3] = await Promise.all([
      runTool(page, 'clipQMapDatasetByGeometry', {
        sourceDatasetName: population.name,
        clipDatasetName: polygon.name,
        mode: 'intersects',
        newDatasetName: 'race_test_1'
      }),
      runTool(page, 'clipQMapDatasetByGeometry', {
        sourceDatasetName: population.name,
        clipDatasetName: polygon.name,
        mode: 'centroid',
        newDatasetName: 'race_test_2'
      }),
      runTool(page, 'clipQMapDatasetByGeometry', {
        sourceDatasetName: population.name,
        clipDatasetName: polygon.name,
        mode: 'within',
        newDatasetName: 'race_test_3'
      })
    ]);

    // All three should have completed (success or handled failure, no crash)
    expect(clip1?.result).toBeTruthy();
    expect(clip2?.result).toBeTruthy();
    expect(clip3?.result).toBeTruthy();

    // At least the intersects clip should succeed (within may produce 0 rows)
    expect(clip1?.result?.success).toBe(true);
  });

  test('deferred recovery: tool without discovery returns deferred with nextAllowedTools', async ({
    page
  }) => {
    test.setTimeout(120_000);
    await page.addInitScript(() => {
      (window as any).__QMAP_E2E_TOOLS__ = true;
    });
    await page.goto('/');
    await openAssistant(page);
    await waitForToolRunner(page);

    // Call a mutation tool WITHOUT bypassing the turn state machine.
    // The turn state starts in 'discover' phase, so the tool should be deferred.
    const res = await page.evaluate(async () => {
      const runner = (window as any).__qmapRunToolWithStateMachine;
      if (!runner) throw new Error('__qmapRunToolWithStateMachine not available');
      // Return both llmResult and qmapToolResult for inspection
      const raw = await runner('clipQMapDatasetByGeometry', {
        sourceDatasetName: 'fake',
        clipDatasetName: 'fake',
        mode: 'intersects'
      });
      return raw;
    });

    const llmResult = res?.result || {};
    // Should be blocked/deferred because we're in 'discover' phase
    expect(llmResult.success).toBe(false);
    // Should contain phase gate information in details
    const details = String(llmResult.details || '');
    expect(details).toMatch(/discovery|snapshot/i);
    // Phase-gated block should include deferred metadata
    const hasPhaseInfo =
      llmResult.executionPhase || llmResult.nextAllowedTools || llmResult.deferredReason;
    expect(hasPhaseInfo).toBeTruthy();
  });

  test('listQCumberDatasets does not silently auto-fallback explicit unknown providerId', async ({
    page
  }) => {
    test.setTimeout(180_000);
    await page.addInitScript(() => {
      (window as any).__QMAP_E2E_TOOLS__ = true;
    });
    await page.goto('/');
    await openAssistant(page);
    await waitForToolRunner(page);

    const readinessProbe = await probeBackendTool(page, 'listQCumberProviders', {});
    if (!readinessProbe.success && shouldSkipBackendInfraFailure(readinessProbe.details)) {
      test.info().annotations.push({
        type: 'warning',
        description:
          `Skipping unknown-provider fallback coverage in this run (backend auth/connectivity not ready): ${readinessProbe.details}. ` +
          'Set QMAP_E2E_REQUIRE_BACKEND_AUTH=true to enforce hard fail.'
      });
      return;
    }
    if (!readinessProbe.success) {
      throw new Error(`Tool "listQCumberProviders" failed: ${readinessProbe.details || 'no details'}`);
    }
    const res = await runTool(page, 'listQCumberDatasets', {providerId: 'ckan-it-ispra'});
    expect(res?.result?.success).toBeFalsy();
    expect(String(res?.result?.details || '')).toContain('ckan-it-ispra');
    expect(String(res?.result?.details || '')).toContain('Available providers');
  });
});
