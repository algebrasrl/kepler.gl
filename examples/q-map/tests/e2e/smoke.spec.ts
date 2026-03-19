import {expect, test} from '@playwright/test';

test('loads q-map shell and map canvas', async ({page}) => {
  await page.goto('/');
  await expect(page.getByText('Q-hive User')).toBeVisible();
  await expect(page.locator('canvas').first()).toBeVisible();
});
