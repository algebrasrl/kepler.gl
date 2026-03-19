import {expect, test, type Frame, type Page} from '@playwright/test';

const IFRAME_ID = 'qmap-iframe-harness';

async function createIframeHarness(page: Page): Promise<Frame> {
  await page.goto('/');
  await page.setContent(`
    <!doctype html>
    <html lang="en">
      <body>
        <iframe id="${IFRAME_ID}" srcdoc="<!doctype html><html><body>iframe harness</body></html>"></iframe>
      </body>
    </html>
  `);
  await page.evaluate(() => {
    (window as any).__qmapIframeMessages = [];
    window.addEventListener('message', event => {
      (window as any).__qmapIframeMessages.push(event.data);
    });
  });
  const iframeHandle = await page.waitForSelector(`#${IFRAME_ID}`);
  const frame = await iframeHandle.contentFrame();
  expect(frame).not.toBeNull();
  return frame as Frame;
}

async function moduleUrlFromPage(page: Page): Promise<string> {
  const baseURL = new URL(page.url());
  return new URL('/src/utils/iframe-export.ts', baseURL).toString();
}

test('iframe export posts cloud payload with action uuid and cloud reference', async ({page}) => {
  const frame = await createIframeHarness(page);
  const moduleUrl = await moduleUrlFromPage(page);
  const result = await frame.evaluate(async (url: string) => {
    const mod = await import(url);
    window.location.hash =
      '#mode=draw-on-map&double-setup=1&export_payload=subset&action_uuid=action-123&cloud_map_id=cloud-999&cloud_provider=q-storage-backend';
    const enabled = mod.isQMapIframeExportEnabled();
    const actionUuid = mod.getQMapIframeActionUuid();
    const cloudRef = mod.getQMapIframeCloudMapReference();
    const sent = mod.postQMapIframeExport({info: {title: 'Action map'}}, 'fallback-map', {
      cloudMapId: 'cloud-999',
      cloudProvider: 'q-storage-backend'
    });
    return {enabled, actionUuid, cloudRef, sent};
  }, moduleUrl);

  expect(result.enabled).toBe(true);
  expect(result.actionUuid).toBe('action-123');
  expect(result.cloudRef).toEqual({id: 'cloud-999', provider: 'q-storage-backend'});
  expect(result.sent).toBe(true);

  await expect.poll(() => page.evaluate(() => (window as any).__qmapIframeMessages.length)).toBeGreaterThan(0);
  const message = await page.evaluate(() => (window as any).__qmapIframeMessages.at(-1));
  expect(message.type).toBe('QMAP_IFRAME_CLOUD_EXPORT');
  expect(message.source).toBe('q-map');
  expect(message.version).toBe(1);
  expect(message.payload.instanceId).toBe('action-123');
  expect(message.payload.actionUuid).toBe('action-123');
  expect(message.payload.cloudMap).toEqual({id: 'cloud-999', provider: 'q-storage-backend'});
  expect(message.payload.format).toBe('keplergl');
  expect(message.payload.map).toBeUndefined();
  expect(message.payload.mapInfo).toMatchObject({title: 'Action map'});
});

test('iframe export falls back to legacy payload when cloud meta is missing', async ({page}) => {
  const frame = await createIframeHarness(page);
  const moduleUrl = await moduleUrlFromPage(page);
  const result = await frame.evaluate(async (url: string) => {
    const mod = await import(url);
    window.location.hash = '#mode=draw-on-map&double-setup=1&export_payload=subset&action_uuid=action-legacy';
    const sent = mod.postQMapIframeExport({info: {title: 'Legacy map'}, layers: []}, 'fallback-map');
    return {sent};
  }, moduleUrl);

  expect(result.sent).toBe(true);

  await expect.poll(() => page.evaluate(() => (window as any).__qmapIframeMessages.length)).toBeGreaterThan(0);
  const message = await page.evaluate(() => (window as any).__qmapIframeMessages.at(-1));
  expect(message.type).toBe('QMAP_IFRAME_EXPORT');
  expect(message.source).toBe('q-map');
  expect(message.version).toBe(1);
  expect(message.payload.instanceId).toBe('action-legacy');
  expect(message.payload.map).toMatchObject({info: {title: 'Legacy map'}});
});

test('iframe export in perimeter mode sends polygon perimeter and omits full map', async ({page}) => {
  const frame = await createIframeHarness(page);
  const moduleUrl = await moduleUrlFromPage(page);
  const result = await frame.evaluate(async (url: string) => {
    const mod = await import(url);
    window.location.hash =
      '#mode=draw-on-map&double-setup=1&export_payload=perimeter&action_uuid=action-perimeter&cloud_map_id=cloud-321&cloud_provider=q-storage-backend';
    const sent = mod.postQMapIframeExport(
      {
        info: {title: 'Perimeter map'},
        config: {
          config: {
            visState: {
              editor: {
                features: [
                  {
                    type: 'Feature',
                    geometry: {
                      type: 'Polygon',
                      coordinates: [[[12.3, 45.4], [12.4, 45.4], [12.4, 45.5], [12.3, 45.5], [12.3, 45.4]]]
                    },
                    properties: {name: 'perimeter-1'}
                  }
                ]
              }
            }
          }
        }
      },
      'fallback-map',
      {cloudMapId: 'cloud-321', cloudProvider: 'q-storage-backend'}
    );
    return {sent};
  }, moduleUrl);

  expect(result.sent).toBe(true);

  await expect.poll(() => page.evaluate(() => (window as any).__qmapIframeMessages.length)).toBeGreaterThan(0);
  const message = await page.evaluate(() => (window as any).__qmapIframeMessages.at(-1));
  expect(message.type).toBe('QMAP_IFRAME_CLOUD_EXPORT');
  expect(message.payload.map).toBeUndefined();
  expect(message.payload.perimeterFeatureCollection?.type).toBe('FeatureCollection');
  expect(message.payload.perimeterFeatureCollection?.features?.length).toBe(1);
});

test('iframe export in perimeter mode fails when no polygon perimeter is present', async ({page}) => {
  const frame = await createIframeHarness(page);
  const moduleUrl = await moduleUrlFromPage(page);
  const result = await frame.evaluate(async (url: string) => {
    const mod = await import(url);
    window.location.hash =
      '#mode=draw-on-map&double-setup=1&export_payload=perimeter&action_uuid=action-empty-perimeter&cloud_map_id=cloud-654&cloud_provider=q-storage-backend';
    return mod.postQMapIframeExportDetailed({info: {title: 'No perimeter'}}, 'fallback-map', {
      cloudMapId: 'cloud-654',
      cloudProvider: 'q-storage-backend'
    });
  }, moduleUrl);

  expect(result).toEqual({ok: false, reason: 'missing_perimeter'});
  const sentCount = await page.evaluate(() => (window as any).__qmapIframeMessages.length);
  expect(sentCount).toBe(0);
});

test('iframe cancel posts a close-only message without export payload', async ({page}) => {
  const frame = await createIframeHarness(page);
  const moduleUrl = await moduleUrlFromPage(page);
  const result = await frame.evaluate(async (url: string) => {
    const mod = await import(url);
    window.location.hash = '#mode=draw-on-map&double-setup=1&action_uuid=action-cancel';
    const sent = mod.postQMapIframeCancel();
    return {sent};
  }, moduleUrl);

  expect(result.sent).toBe(true);

  await expect.poll(() => page.evaluate(() => (window as any).__qmapIframeMessages.length)).toBeGreaterThan(0);
  const message = await page.evaluate(() => (window as any).__qmapIframeMessages.at(-1));
  expect(message.type).toBe('QMAP_IFRAME_CANCEL');
  expect(message.source).toBe('q-map');
  expect(message.version).toBe(1);
  expect(message.payload).toMatchObject({hash: '#mode=draw-on-map&double-setup=1&action_uuid=action-cancel'});
  expect(message.payload.map).toBeUndefined();
});

test('iframe export reports explicit failure reason outside iframe mode', async ({page}) => {
  await page.goto('/');
  const moduleUrl = await moduleUrlFromPage(page);
  const result = await page.evaluate(async (url: string) => {
    const mod = await import(url);
    window.location.hash = '#mode=draw-on-map&double-setup=1&action_uuid=action-outside';
    return mod.postQMapIframeExportDetailed({info: {title: 'Outside iframe'}}, 'fallback-map');
  }, moduleUrl);

  expect(result).toEqual({ok: false, reason: 'not_in_iframe'});
});
