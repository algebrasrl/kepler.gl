import {defineConfig, devices} from '@playwright/test';

const baseURL = process.env.PW_BASE_URL || 'http://127.0.0.1:8081';
const parsedBaseURL = new URL(baseURL);
const baseHost = parsedBaseURL.hostname || '127.0.0.1';
const basePort = parsedBaseURL.port || '8081';

export default defineConfig({
  testDir: './tests/e2e',
  testMatch: ['smoke.spec.ts', 'ux.spec.ts', 'ux-regression.spec.ts', 'tools.spec.ts'],
  timeout: 60_000,
  expect: {timeout: 15_000},
  fullyParallel: false,
  reporter: 'list',
  use: {
    baseURL,
    trace: 'on-first-retry',
    screenshot: 'only-on-failure'
  },
  webServer: {
    command: `yarn dev --host ${baseHost} --port ${basePort}`,
    url: baseURL,
    reuseExistingServer: true,
    timeout: 120_000
  },
  projects: [
    {
      name: 'chromium',
      use: {...devices['Desktop Chrome']}
    }
  ]
});
