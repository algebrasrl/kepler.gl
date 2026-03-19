import fs from 'node:fs';
import path from 'node:path';
import {expect, test, type APIRequestContext, type Locator, type Page} from '@playwright/test';

const ADD_DATA_BUTTON = /Aggiungi dati|Add Data/i;
const ADD_DATA_MODAL_TITLE = /Add Data To Map|Aggiungi dati alla mappa/i;
const TAB_CLOUD = /Cloud storage|Archivio cloud/i;
const Q_STORAGE_PROVIDER_LABEL = /Q-storage(\s+User)?|Le mie mappe|My Maps/i;

function escapeRegex(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

async function firstVisible(locator: Locator): Promise<Locator> {
  const count = await locator.count();
  for (let i = 0; i < count; i += 1) {
    const candidate = locator.nth(i);
    if (await candidate.isVisible().catch(() => false)) return candidate;
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
    if ((await sideTabs.count()) >= 2) await sideTabs.nth(1).click();
  }

  await expect(roleButton.or(classButton)).toBeVisible();
}

async function goToFilterPanel(page: Page) {
  const filterTab = page.locator('.side-panel__tab[data-for="filter-nav"]').first();
  if (await filterTab.count()) {
    await filterTab.click();
  } else {
    const sideTabs = page.locator('.side-panel__tab');
    await sideTabs.nth(2).click();
  }
  await expect(page.locator('.filter-manager')).toBeVisible();
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

async function openProviderCard(page: Page, providerLabel: RegExp) {
  const providerButton = page.getByRole('button', {name: providerLabel}).first();
  if (await providerButton.isVisible().catch(() => false)) {
    await providerButton.click();
    return;
  }

  const userLabel = page.getByText(providerLabel).first();
  await expect(userLabel).toBeVisible();
  await userLabel.click();
}

async function openQCumberProvider(page: Page) {
  await openProviderCard(page, /Q-cumber|Q-cumber User/i);
}

async function openQStorageProvider(page: Page) {
  await openProviderCard(page, Q_STORAGE_PROVIDER_LABEL);
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

async function loadStressorFromQCumber(page: Page, request: APIRequestContext) {
  const mapTitle = `e2e-reg-basemap-${Date.now()}`;
  await seedStorageMap(request, mapTitle);

  await goToCloudStorageTab(page);
  await openQStorageProvider(page);
  await clickCloudMapThumbByTitle(page, mapTitle);
  await ensureDatasetPanel(page);
  await expect(page.locator('.source-data-title').first()).toBeVisible({timeout: 30_000});
}

async function uploadTextFile(
  page: Page,
  fileName: string,
  content: string,
  mimeType = 'text/csv'
) {
  await openAddDataModal(page);
  const uploadInput = page.locator('input[type="file"]').first();
  await expect(uploadInput).toBeAttached();
  await uploadInput.setInputFiles({
    name: fileName,
    mimeType,
    buffer: Buffer.from(content)
  });
  await ensureDatasetPanel(page);
}

async function uploadBinaryFile(page: Page, fileName: string, content: Buffer, mimeType = 'application/octet-stream') {
  await openAddDataModal(page);
  const uploadInput = page.locator('input[type="file"]').first();
  await expect(uploadInput).toBeAttached();
  await uploadInput.setInputFiles({
    name: fileName,
    mimeType,
    buffer: content
  });
}

async function uploadFixtureFile(page: Page, fixturePath: string) {
  await openAddDataModal(page);
  const uploadInput = page.locator('input[type="file"]').first();
  await expect(uploadInput).toBeAttached();
  await uploadInput.setInputFiles(fixturePath);
  await ensureDatasetPanel(page);
}

async function seedStorageMap(request: APIRequestContext, mapTitle: string) {
  const samplePath = path.resolve(__dirname, '../fixtures/sample-storage-map.keplergl.json');
  const sampleMap = JSON.parse(fs.readFileSync(samplePath, 'utf8'));
  const res = await request.post('http://127.0.0.1:3005/maps', {
    data: {
      title: mapTitle,
      description: 'E2E persisted map',
      isPublic: false,
      map: sampleMap.map,
      format: sampleMap.format || 'keplergl'
    }
  });
  expect(res.ok()).toBeTruthy();
}

test('reg-1 q-storage persistence: saved map can be loaded from cloud UI', async ({page, request}) => {
  const mapTitle = `e2e-storage-${Date.now()}`;
  await seedStorageMap(request, mapTitle);

  await page.goto('/');
  await goToCloudStorageTab(page);
  await openQStorageProvider(page);
  await clickCloudMapThumbByTitle(page, mapTitle);
  await ensureDatasetPanel(page);
  await expect(page.getByText(/Sample Cities q-hive/i).first()).toBeVisible({timeout: 20_000});
});

test('reg-2 cloud/backend errors (404, 422, timeout) show user notifications', async ({page, request}) => {
  const mapTitle = `e2e-storage-errors-${Date.now()}`;
  await seedStorageMap(request, mapTitle);

  await page.goto('/');
  await goToCloudStorageTab(page);
  await openQStorageProvider(page);
  const mapTitleRegex = new RegExp(escapeRegex(mapTitle), 'i');
  const mapCard = page.getByText(mapTitleRegex).first();
  await expect(mapCard).toBeVisible({timeout: 20_000});
  const mapThumb = mapCard.locator('xpath=preceding-sibling::div[@title][1]').first();

  await page.route('**/maps/*', route =>
    route.fulfill({status: 404, contentType: 'application/json', body: JSON.stringify({detail: 'Not found'})})
  );
  await mapThumb.click();
  await expect(page.locator('.notification-item').first()).toBeVisible({timeout: 15_000});
  await page.unroute('**/maps/*');

  await page.route('**/maps/*', route =>
    route.fulfill({
      status: 422,
      contentType: 'application/json',
      body: JSON.stringify({detail: 'Unprocessable'})
    })
  );
  await mapThumb.click();
  await expect(page.locator('.notification-item').first()).toBeVisible({timeout: 15_000});
  await page.unroute('**/maps/*');

  await page.route('**/maps/*', route => route.abort('timedout'));
  await mapThumb.click();
  await expect(page.locator('.notification-item').first()).toBeVisible({timeout: 15_000});
  await page.unroute('**/maps/*');
});

test('reg-3 dataset filters can be created and set to a categorical value', async ({page}) => {
  await page.goto('/');

  const csv = ['lat,lng,category,value', '45.4642,9.19,A,10', '41.9028,12.4964,B,20', '40.8518,14.2681,A,30'].join(
    '\n'
  );
  await uploadTextFile(page, 'filter_points.csv', csv);
  await expect(page.getByText(/filter_points/i).first()).toBeVisible({timeout: 15_000});

  await goToFilterPanel(page);
  const addFilter = page.locator('.add-filter-button').first();
  await expect(addFilter).toBeVisible();
  await addFilter.click();

  const filterPanel = page.locator('.filter-panel').first();
  await expect(filterPanel).toBeVisible({timeout: 10_000});

  const fieldSelector = filterPanel.locator('.field-selector .item-selector').first();
  await fieldSelector.click();
  await page.getByText(/^category$/i).first().click();

  const valueSelector = filterPanel.locator('.item-selector').nth(1);
  await valueSelector.click();
  await page.getByText(/^A$/).first().click();

  await expect(filterPanel.getByText(/category/i)).toBeVisible();
  await expect(filterPanel.getByText(/^A$/).first()).toBeVisible();
});

test('reg-4 quantitative styling controls are available for numeric geojson data', async ({page}) => {
  await page.goto('/');

  const geojsonPath = path.resolve(__dirname, '../fixtures/population_polygons.geojson');
  await openAddDataModal(page);
  const uploadInput = page.locator('input[type="file"]').first();
  await uploadInput.setInputFiles(geojsonPath);
  await ensureDatasetPanel(page);

  const sidePanel = page.locator('.side-panel--container');
  await expect(sidePanel.getByText(/population_polygons/i).first()).toBeVisible({timeout: 20_000});

  const layerPanel = sidePanel.locator('.layer-panel').first();
  await expect(layerPanel).toBeVisible();
  await layerPanel.click();
  const configToggle = sidePanel.locator('.layer__enable-config').first();
  await expect(configToggle).toBeVisible();
  await configToggle.click();

  const colorSection = sidePanel.getByText(/Fill Color/i).first();
  await expect(colorSection).toBeVisible();

  const colorBasedOnLabel = sidePanel.getByText(/color based on/i).first();
  await expect(colorBasedOnLabel).toBeVisible();
  await expect(sidePanel.getByText(/Select a field/i).first()).toBeVisible();
  await expect(sidePanel.getByText(/Opacity/i).first()).toBeVisible();

  const sourceTitle = sidePanel.locator('.source-data-title').first();
  await sourceTitle.hover();
  await sidePanel.locator('.dataset-action.show-data-table').first().click({force: true});
  const datasetModal = page.locator('#dataset-modal');
  await expect(datasetModal).toBeVisible();
  await expect(datasetModal.getByText(/^population$/i).first()).toBeVisible();
  await page.keyboard.press('Escape');
  await expect(datasetModal).not.toBeVisible();
});

test('reg-5 large dataset upload keeps UI responsive', async ({page}) => {
  await page.goto('/');

  const rows = ['lat,lng,value'];
  for (let i = 0; i < 12000; i += 1) {
    const lat = 36 + (i % 120) * 0.1;
    const lng = 6 + ((i * 7) % 140) * 0.1;
    rows.push(`${lat.toFixed(4)},${lng.toFixed(4)},${i}`);
  }
  await uploadTextFile(page, 'big_points.csv', rows.join('\n'));

  await expect(page.getByText(/12[, ]?000 rows/i).first()).toBeVisible({timeout: 30_000});

  const sideTabs = page.locator('.side-panel__tab');
  await sideTabs.nth(0).click();
  await expect(page.getByText(/Profilo Utente/i)).toBeVisible({timeout: 5_000});
  await sideTabs.nth(1).click();
  await expect(page.getByRole('button', {name: ADD_DATA_BUTTON})).toBeVisible({timeout: 5_000});
});

test('reg-6 mobile responsive: side panel and Add Data modal remain usable', async ({page}) => {
  await page.setViewportSize({width: 390, height: 844});
  await page.goto('/');

  await expect(page.locator('.side-panel--container')).toBeVisible();
  await openAddDataModal(page);

  const modal = page
    .locator('div')
    .filter({has: page.getByText(ADD_DATA_MODAL_TITLE)})
    .first();
  await expect(modal).toBeVisible();
  const box = await modal.boundingBox();
  expect(box).not.toBeNull();
  expect(box!.x).toBeGreaterThanOrEqual(0);
  expect(box!.y).toBeGreaterThanOrEqual(0);
  expect(box!.x + box!.width).toBeLessThanOrEqual(390);
});

test('reg-7 duplicate dataset names do not crash and keep dataset usable', async ({page}) => {
  await page.goto('/');

  const csvA = ['lat,lng,alpha_col', '45.46,9.19,one', '41.90,12.49,two'].join('\n');
  const csvB = ['lat,lng,beta_col', '40.85,14.26,red', '44.49,11.34,blue'].join('\n');

  await uploadTextFile(page, 'duplicate.csv', csvA);
  await uploadTextFile(page, 'duplicate.csv', csvB);

  await ensureDatasetPanel(page);
  await expect(page.getByText(/Datasets\(1\)/i)).toBeVisible({timeout: 20_000});

  const sidePanel = page.locator('.side-panel--container');
  const sourceTitle = sidePanel.locator('.source-data-title').first();
  await sourceTitle.hover();
  await sidePanel.locator('.dataset-action.show-data-table').first().click({force: true});
  const datasetModal = page.locator('#dataset-modal');
  await expect(datasetModal).toBeVisible();
  await expect(datasetModal.getByText(/duplicate\.csv/i).first()).toBeVisible();
  await page.keyboard.press('Escape');
  await expect(datasetModal).not.toBeVisible();
  await expect(sidePanel.getByText(/duplicate/i).first()).toBeVisible();
});

test('reg-8 basemap zoom/pan changes map coordinate+zoom readout', async ({page, request}) => {
  await page.goto('/');
  await loadStressorFromQCumber(page, request);

  const mapRegion = page.getByRole('region', {name: /Map/i}).first();
  await expect(mapRegion).toBeVisible();
  const box = await mapRegion.boundingBox();
  expect(box).not.toBeNull();
  const cx = box!.x + box!.width / 2;
  const cy = box!.y + box!.height / 2;
  await page.mouse.move(cx, cy);
  await page.mouse.down();
  await page.mouse.move(cx - 260, cy - 120, {steps: 16});
  await page.mouse.up();

  const before = await mapRegion.screenshot();

  const sidePanel = page.locator('.side-panel--container');
  const zoomToLayerButton = sidePanel.locator('.layer__zoom-to-layer').first();
  await expect(zoomToLayerButton).toBeVisible();
  await zoomToLayerButton.click();

  await page.waitForTimeout(800);
  await page.mouse.down();
  await page.mouse.move(cx + 100, cy + 40, {steps: 12});
  await page.mouse.up();
  const after = await mapRegion.screenshot();
  expect(before.equals(after)).toBeFalsy();
});

test('reg-9 upload supports csv/json/geojson fixtures', async ({page}) => {
  const fixtures = [
    {
      format: 'csv',
      fileName: 'upload_points.csv',
      expectedFields: [/^lat$/i, /^lng$/i, /^category$/i, /^value$/i]
    },
    {
      format: 'json',
      fileName: 'upload_points.json',
      expectedFields: [/^lat$/i, /^lng$/i, /^category$/i, /^value$/i]
    },
    {
      format: 'geojson',
      fileName: 'upload_points.geojson',
      expectedFields: [/^_geojson$/i, /^category$/i, /^value$/i]
    }
  ] as const;

  for (const fixture of fixtures) {
    await test.step(`upload ${fixture.format}`, async () => {
      await page.goto('/');
      const fixturePath = path.resolve(__dirname, `../fixtures/uploads/${fixture.fileName}`);
      await uploadFixtureFile(page, fixturePath);

      const sidePanel = page.locator('.side-panel--container');
      await expect(sidePanel.locator('.source-data-title').first()).toBeVisible({timeout: 30_000});
      await expect(sidePanel.getByText(/3 rows/i).first()).toBeVisible({timeout: 30_000});

      const sourceTitle = sidePanel.locator('.source-data-title').first();
      await sourceTitle.hover();
      await sidePanel.locator('.dataset-action.show-data-table').first().click({force: true});
      const datasetModal = page.locator('#dataset-modal');
      await expect(datasetModal).toBeVisible({timeout: 20_000});

      for (const fieldRegex of fixture.expectedFields) {
        await expect(datasetModal.getByText(fieldRegex).first()).toBeVisible();
      }

      await page.keyboard.press('Escape');
      await expect(datasetModal).not.toBeVisible();
    });
  }
});

test('reg-10 invalid shapefile zip shows handled upload error', async ({page}) => {
  await page.goto('/');
  await uploadBinaryFile(page, 'invalid-shapefile.zip', Buffer.from('not-a-real-zip'), 'application/zip');

  await expect(
    page
      .getByText(
        /DuckDB spatial extension is unavailable|Failed to process uploaded spatial file|Could not read the spatial file|No geometry features|Can not process uploaded file|Failed to fetch|IO Error|GDAL Error/i
      )
      .first()
  ).toBeVisible({timeout: 25_000});
});

test('reg-11 upload supports valid shapefile zip and geopackage fixtures', async ({page}) => {
  test.setTimeout(120_000);
  const fixtures = [
    {format: 'shapefile zip', fileName: 'Confini_PANE_2024.shp.zip'},
    {format: 'geopackage', fileName: 'h3_grid_res_3_kon_20230628.gpkg'}
  ] as const;

  for (const fixture of fixtures) {
    await test.step(`upload ${fixture.format}`, async () => {
      const invalidLatitudeMessages: string[] = [];
      const consoleListener = (message: any) => {
        const text = message?.text?.() || '';
        if (/invalid latitude/i.test(text)) {
          invalidLatitudeMessages.push(text);
        }
      };
      page.on('console', consoleListener);

      await page.goto('/');
      const fixturePath = path.resolve(__dirname, `../fixtures/uploads/${fixture.fileName}`);
      await uploadFixtureFile(page, fixturePath);

      const sidePanel = page.locator('.side-panel--container');
      await expect(sidePanel.locator('.source-data-title').first()).toBeVisible({timeout: 60_000});
      await expect(sidePanel.getByText(/\d[\d.,\s]*rows/i).first()).toBeVisible({timeout: 60_000});

      const sourceTitle = sidePanel.locator('.source-data-title').first();
      await sourceTitle.hover();
      await sidePanel.locator('.dataset-action.show-data-table').first().click({force: true});
      const datasetModal = page.locator('#dataset-modal');
      await expect(datasetModal).toBeVisible({timeout: 20_000});
      await expect(datasetModal.getByText(/^_geojson$/i).first()).toBeVisible({timeout: 20_000});

      await page.keyboard.press('Escape');
      await expect(datasetModal).not.toBeVisible();
      await page.waitForTimeout(500);
      expect(invalidLatitudeMessages).toEqual([]);
      page.off('console', consoleListener);
    });
  }
});
