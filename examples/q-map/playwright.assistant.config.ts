import {defineConfig, devices} from '@playwright/test';

const baseURL = process.env.PW_BASE_URL || 'http://127.0.0.1:8081';
const parsedBaseURL = new URL(baseURL);
const baseHost = parsedBaseURL.hostname || '127.0.0.1';
const basePort = parsedBaseURL.port || '8081';
const skipWebServer = String(process.env.PW_SKIP_WEBSERVER || '').trim() === '1';

export default defineConfig({
  testDir: './tests/e2e',
  testMatch: ['ai-mode-policy.spec.ts', 'assistant-live.spec.ts'],
  retries: 1,
  workers: 1,
  timeout: 60_000,
  expect: {timeout: 15_000},
  fullyParallel: false,
  reporter: 'list',
  use: {
    baseURL,
    trace: 'on-first-retry',
    screenshot: 'only-on-failure'
  },
  ...(skipWebServer
    ? {}
    : {
        webServer: {
          command: `yarn dev --host ${baseHost} --port ${basePort}`,
          url: baseURL,
          reuseExistingServer: true,
          timeout: 120_000
        }
      }),
  projects: [
    {
      name: 'chromium',
      use: {...devices['Desktop Chrome']}
    }
  ]
});
