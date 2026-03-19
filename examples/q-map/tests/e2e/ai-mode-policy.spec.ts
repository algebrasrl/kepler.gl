import {expect, test} from '@playwright/test';

async function openAssistant(page: any) {
  const aiToggle = page.getByText(/^AI$/).first();
  await expect(aiToggle).toBeVisible({timeout: 20_000});
  await aiToggle.click();
  await expect(page.getByText(/Assistente|Assistant/i).first()).toBeVisible({timeout: 20_000});
}

async function waitForToolRunner(page: any) {
  await page.waitForFunction(() => Boolean((window as any).__qmapRunTool), null, {timeout: 20_000});
}

async function runTool(page: any, toolName: string, args: Record<string, unknown> = {}) {
  return page.evaluate(
    async ({toolName, args}) => {
      const runner = (window as any).__qmapRunTool;
      if (!runner) {
        throw new Error('q-map tool runner is not available');
      }
      return runner(toolName, args);
    },
    {toolName, args}
  );
}

async function runToolSafe(page: any, toolName: string, args: Record<string, unknown> = {}) {
  try {
    const payload = await runTool(page, toolName, args);
    return {ok: true, payload};
  } catch (error) {
    return {ok: false, error: String((error as Error)?.message || error || '')};
  }
}

test('AI mode gating: assistant stays available and mode allowlists are enforced', async ({page}) => {
  await page.addInitScript(() => {
    (window as any).__QMAP_E2E_TOOLS__ = true;
  });
  await page.goto('/#mode=kepler');

  await openAssistant(page);
  await waitForToolRunner(page);

  const coreToolInKepler = await runTool(page, 'listQMapDatasets', {});
  expect(Boolean(coreToolInKepler?.result?.success)).toBeTruthy();

  const cloudToolInKepler = await runTool(page, 'loadCloudMapAndWait', {
    provider: 'q-storage-backend',
    mapId: '__non_existing_map__'
  });
  const details = String(cloudToolInKepler?.result?.details || '').toLowerCase();
  expect(details).not.toContain('unknown q-map tool');

  await page.evaluate(() => {
    window.location.hash = '#mode=draw-on-map';
  });

  await expect(page.getByText(/^AI$/).first()).toBeVisible({timeout: 20_000});
  await waitForToolRunner(page);

  const coreToolInDrawMode = await runTool(page, 'listQMapDatasets', {});
  expect(Boolean(coreToolInDrawMode?.result?.success)).toBeTruthy();

  const cloudToolInDrawMode = await runToolSafe(page, 'loadCloudMapAndWait', {
    provider: 'q-storage-backend',
    mapId: '__non_existing_map__'
  });
  expect(cloudToolInDrawMode.ok).toBeFalsy();
  expect(String(cloudToolInDrawMode.error || '').toLowerCase()).toContain('unknown q-map tool');
});
