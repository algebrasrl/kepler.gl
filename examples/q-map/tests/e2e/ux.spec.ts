import fs from 'node:fs';
import path from 'node:path';
import {expect, test, type APIRequestContext, type Locator, type Page} from '@playwright/test';

const ADD_DATA_BUTTON = /Aggiungi dati|Add Data/i;
const ADD_DATA_MODAL_TITLE = /Add Data To Map|Aggiungi dati alla mappa/i;
const TAB_UPLOAD = /Carica file|Upload/i;
const TAB_TILESET = /Tileset/i;
const TAB_URL = /Load Map using URL|Carica mappa da URL/i;
const TAB_CLOUD = /Cloud storage|Archivio cloud/i;
const Q_STORAGE_PROVIDER_LABEL = /Q-storage(\s+User)?|Le mie mappe|My Maps/i;

function escapeRegex(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

async function firstVisible(locator: Locator): Promise<Locator> {
  const count = await locator.count();
  for (let i = 0; i < count; i += 1) {
    const candidate = locator.nth(i);
    if (await candidate.isVisible().catch(() => false)) {
      return candidate;
    }
  }
  return locator.first();
}

async function ensureDatasetPanel(page: Page) {
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

async function openAddDataModal(page: Page) {
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

async function goToCloudStorageTab(page: Page) {
  await openAddDataModal(page);
  const cloudTab = await firstVisible(page.getByText(TAB_CLOUD));
  await cloudTab.click();
}

async function openQCumberProvider(page: Page) {
  const providerButton = page.getByRole('button', {name: /Q-cumber|Q-cumber User/i}).first();
  if (await providerButton.isVisible().catch(() => false)) {
    await providerButton.click();
    return;
  }

  const userLabel = page.getByText(/Q-cumber|Q-cumber User/i).first();
  await expect(userLabel).toBeVisible();
  await userLabel.click();
}

async function openQStorageProvider(page: Page) {
  const providerButton = page.getByRole('button', {name: Q_STORAGE_PROVIDER_LABEL}).first();
  if (await providerButton.isVisible().catch(() => false)) {
    await providerButton.click();
    return;
  }

  const userLabel = page.getByText(Q_STORAGE_PROVIDER_LABEL).first();
  await expect(userLabel).toBeVisible();
  await userLabel.click();
}

async function seedStorageMap(request: APIRequestContext, mapTitle: string) {
  const samplePath = path.resolve(__dirname, '../fixtures/sample-storage-map.keplergl.json');
  const sampleMap = JSON.parse(fs.readFileSync(samplePath, 'utf8'));
  const res = await request.post('http://127.0.0.1:3005/maps', {
    data: {
      title: mapTitle,
      description: 'E2E UX map',
      isPublic: false,
      map: sampleMap.map,
      format: sampleMap.format || 'keplergl'
    }
  });
  expect(res.ok()).toBeTruthy();
}

async function clickCloudMapThumbByTitle(page: Page, mapTitle: string) {
  const mapTitleLocator = page.getByText(new RegExp(escapeRegex(mapTitle), 'i')).first();
  await expect(mapTitleLocator).toBeVisible({timeout: 20_000});
  const thumb = mapTitleLocator.locator('xpath=preceding-sibling::div[@title][1]').first();
  if (await thumb.isVisible().catch(() => false)) {
    await thumb.click();
  } else {
    await mapTitleLocator.click();
  }
}

async function loadStressorMapFromCloud(page: Page, request: APIRequestContext) {
  const mapTitle = `e2e-ux-geojson-${Date.now()}`;
  await seedStorageMap(request, mapTitle);

  await goToCloudStorageTab(page);
  await openQStorageProvider(page);
  await clickCloudMapThumbByTitle(page, mapTitle);

  await ensureDatasetPanel(page);
  await expect(page.locator('.source-data-title').first()).toBeVisible({timeout: 30_000});
  await expect(page.getByText(/\d+\s*rows/i).first()).toBeVisible({timeout: 30_000});

  const sidePanel = page.locator('.side-panel--container');
  const zoomToLayerButton = sidePanel.locator('.layer__zoom-to-layer').first();
  await expect(zoomToLayerButton).toBeVisible({timeout: 15_000});
  await zoomToLayerButton.click();
}

test.describe.configure({mode: 'serial'});

test('ux-1 app load and map render', async ({page}) => {
  await page.goto('/');
  await expect(page.getByText('Q-hive User')).toBeVisible();
  await expect(page.locator('canvas').first()).toBeVisible();
});

test('ux-2 add data modal tabs are visible', async ({page}) => {
  await page.goto('/');
  await openAddDataModal(page);
  await expect(await firstVisible(page.getByText(TAB_UPLOAD))).toBeVisible();
  await expect(await firstVisible(page.getByText(TAB_TILESET))).toBeVisible();
  await expect(await firstVisible(page.getByText(TAB_URL))).toBeVisible();
  await expect(await firstVisible(page.getByText(TAB_CLOUD))).toBeVisible();
});

test('ux-2b upload tab lists shapefile zip and geopackage formats', async ({page}) => {
  await page.goto('/');
  await openAddDataModal(page);
  await expect(page.getByText(/Shapefile ZIP/i).first()).toBeVisible();
  await expect(page.getByText(/GeoPackage/i).first()).toBeVisible();
  await expect(page.getByText(/^arrow$/i)).toHaveCount(0);
  await expect(page.getByText(/^parquet$/i)).toHaveCount(0);
});

test('ux-3 cloud storage provider cards are visible', async ({page}) => {
  await page.goto('/');
  await goToCloudStorageTab(page);
  await expect(page.getByText(/Q-cumber|Q-cumber User/i).first()).toBeVisible();
  await expect(page.getByText(Q_STORAGE_PROVIDER_LABEL).first()).toBeVisible();
});

test('ux-4 load stressor map updates dataset sidebar', async ({page, request}) => {
  await page.goto('/');
  await loadStressorMapFromCloud(page, request);
  await expect(page.getByText(/Datasets\(\d+\)/i)).toBeVisible();
  const sidePanel = page.locator('.side-panel--container');
  await expect(await firstVisible(sidePanel.getByText(/^Layer$/))).toBeVisible();
});

test('ux-5 layer style controls are visible after cloud load', async ({page, request}) => {
  await page.goto('/');
  await loadStressorMapFromCloud(page, request);
  const sidePanel = page.locator('.side-panel--container');
  await expect(sidePanel.getByText(/Layer Blending/i)).toBeVisible();
  await expect(sidePanel.getByText(/Map Overlay Blending/i)).toBeVisible();
});

test('ux-6 layer visibility toggle updates icon state', async ({page, request}) => {
  await page.goto('/');
  await loadStressorMapFromCloud(page, request);

  const sidePanel = page.locator('.side-panel--container');
  const visibilityToggle = sidePanel.locator('.layer__visibility-toggle').first();
  await expect(visibilityToggle).toBeVisible();

  const seenIcon = visibilityToggle.locator('.data-ex-icons-eyeseen');
  const unseenIcon = visibilityToggle.locator('.data-ex-icons-eyeunseen');
  await expect(seenIcon).toBeVisible();
  await expect(unseenIcon).toHaveCount(0);

  await visibilityToggle.click();
  await expect(unseenIcon).toBeVisible();
  await expect(seenIcon).toHaveCount(0);

  await visibilityToggle.click();
  await expect(seenIcon).toBeVisible();
});

test('ux-7 dataset table opens from sidebar and exposes columns', async ({page, request}) => {
  await page.goto('/');
  await loadStressorMapFromCloud(page, request);

  const sidePanel = page.locator('.side-panel--container');
  const sourceTitle = sidePanel.locator('.source-data-title').first();
  await expect(sourceTitle).toBeVisible();
  await sourceTitle.hover();

  const showDataTable = sidePanel.locator('.dataset-action.show-data-table').first();
  await expect(showDataTable).toBeVisible();
  await showDataTable.click({force: true});

  const datasetModal = page.locator('#dataset-modal');
  await expect(datasetModal).toBeVisible();
  await expect(datasetModal.getByText(/name|severity|stressor_class/i).first()).toBeVisible();

  await page.keyboard.press('Escape');
  await expect(datasetModal).not.toBeVisible();
});

test('ux-8 layer reorder via drag and drop updates panel order', async ({page, request}) => {
  await page.goto('/');
  await loadStressorMapFromCloud(page, request);

  const sidePanel = page.locator('.side-panel--container');
  const addLayerButton = sidePanel.getByRole('button', {name: /Add Layer/i});
  await expect(addLayerButton).toBeVisible();
  await addLayerButton.click();

  const layerPanels = sidePanel.locator('.layer-panel');
  await expect(layerPanels).toHaveCount(2, {timeout: 20_000});

  const orderBefore = await sidePanel
    .locator('input.layer__title__editor')
    .evaluateAll(nodes => nodes.map(n => (n as HTMLInputElement).value).filter(Boolean) as string[]);
  expect(orderBefore.length).toBeGreaterThanOrEqual(2);

  const sourcePanel = layerPanels.nth(1);
  const targetPanel = layerPanels.nth(0);
  await sourcePanel.hover();
  const sourceHandle = sourcePanel.locator('.layer__drag-handle');
  const sourceBox = await sourceHandle.boundingBox();
  const targetBox = await targetPanel.boundingBox();
  expect(sourceBox).not.toBeNull();
  expect(targetBox).not.toBeNull();
  await page.mouse.move(sourceBox!.x + sourceBox!.width / 2, sourceBox!.y + sourceBox!.height / 2);
  await page.mouse.down();
  await page.mouse.move(targetBox!.x + targetBox!.width / 2, targetBox!.y + 8, {steps: 12});
  await page.mouse.up();

  const orderAfter = await sidePanel
    .locator('input.layer__title__editor')
    .evaluateAll(nodes => nodes.map(n => (n as HTMLInputElement).value).filter(Boolean) as string[]);
  expect(orderAfter.length).toBeGreaterThanOrEqual(2);
  expect(orderAfter[0]).toBe(orderBefore[1]);
});

test('ux-9 geojson dataset exposes geometry column and renders layer type', async ({page, request}) => {
  await page.goto('/');
  await loadStressorMapFromCloud(page, request);

  const sidePanel = page.locator('.side-panel--container');
  await expect(sidePanel.getByText(/Geojson/i).first()).toBeVisible();

  const sourceTitle = sidePanel.locator('.source-data-title').first();
  await expect(sourceTitle).toBeVisible();
  await sourceTitle.hover();

  const showDataTable = sidePanel.locator('.dataset-action.show-data-table').first();
  await expect(showDataTable).toBeVisible();
  await showDataTable.click({force: true});

  const datasetModal = page.locator('#dataset-modal');
  await expect(datasetModal).toBeVisible();
  await expect(datasetModal.getByText(/_geojson/i).first()).toBeVisible({timeout: 15_000});

  await page.keyboard.press('Escape');
  await expect(datasetModal).not.toBeVisible();
});

test('ux-10 polygon geojson can be tessellated to H3 and creates Tassellation dataset', async ({
  page
}) => {
  await page.goto('/');
  await openAddDataModal(page);

  const uploadInput = page.locator('input[type="file"]').first();
  await expect(uploadInput).toBeAttached();
  await uploadInput.setInputFiles(path.resolve(__dirname, '../fixtures/polygon.geojson'));

  await ensureDatasetPanel(page);
  await expect(page.getByText(/polygon/i).first()).toBeVisible({timeout: 20_000});

  const canvas = page.locator('canvas').first();
  await expect(canvas).toBeVisible();
  const box = await canvas.boundingBox();
  expect(box).not.toBeNull();

  const mapDrawButton = page.locator('.map-control-button.map-draw').first();
  await expect(mapDrawButton).toBeVisible();
  await mapDrawButton.click();
  const editFeatureButton = page.locator('.edit-feature').first();
  await expect(editFeatureButton).toBeVisible();
  await editFeatureButton.click();

  const cx = box!.x + box!.width / 2;
  const cy = box!.y + box!.height / 2;
  await page.mouse.click(cx, cy);

  const mapPopover = page.locator('.map-popover').first();
  await expect(mapPopover).toBeVisible({timeout: 15_000});
  const selectGeometry = mapPopover.locator('.select-geometry').first();
  await expect(selectGeometry).toBeVisible();
  await selectGeometry.click();

  const featureActionPanel = page.locator('.feature-action-panel').first();
  await expect(featureActionPanel).toBeVisible({timeout: 15_000});
  const tassellationMenu = featureActionPanel.locator('.editor-tassellation-list').first();
  await expect(tassellationMenu).toBeVisible();
  await tassellationMenu.click();

  const h3Resolution = featureActionPanel
    .locator('.layer-panel-item')
    .filter({hasText: /^H3 7$/i})
    .first();
  await expect(h3Resolution).toBeVisible();
  await h3Resolution.click();

  await ensureDatasetPanel(page);
  await expect(page.getByText(/Tassellation/i).first()).toBeVisible({timeout: 30_000});
  await expect(page.getByText(/\d+\s*rows/i).first()).toBeVisible({timeout: 30_000});
});
