import { chromium } from '@playwright/test';

function assert(condition, message) {
  if (!condition) throw new Error(message);
}

function readFixture() {
  const raw = process.env.QHIVE_PLATFORM_SMOKE_FIXTURE || '';
  if (!raw.trim()) {
    throw new Error('Missing QHIVE_PLATFORM_SMOKE_FIXTURE');
  }
  const parsed = JSON.parse(raw);
  if (!parsed || typeof parsed !== 'object') {
    throw new Error('Invalid QHIVE_PLATFORM_SMOKE_FIXTURE payload');
  }
  const baseUrl = String(parsed.baseUrl || '').trim();
  const sessionId = String(parsed.sessionId || '').trim();
  const editPath = String(parsed.editPath || '').trim();
  if (!baseUrl || !sessionId || !editPath) {
    throw new Error('Incomplete QHIVE_PLATFORM_SMOKE_FIXTURE payload');
  }
  return {baseUrl, sessionId, editPath};
}

const fixture = readFixture();
const browser = await chromium.launch({headless: true});
const context = await browser.newContext();
const page = await context.newPage();

const result = {
  postLoginUrl: null,
  pageTitle: null,
  iframeSrc: null,
  bodyClass: null,
  tokenPresent: false,
  tokenLength: 0,
  localStorageKeys: [],
  assistantMeStatus: null,
  assistantMeBody: null
};

try {
  const url = new URL(fixture.baseUrl);
  await context.addCookies([
    {
      name: 'sessionid',
      value: fixture.sessionId,
      domain: url.hostname,
      path: '/',
      httpOnly: true,
      secure: false,
      sameSite: 'Lax'
    }
  ]);

  await page.goto(`${fixture.baseUrl}${fixture.editPath}`, {
    waitUntil: 'domcontentloaded',
    timeout: 30000
  });
  await page.waitForLoadState('networkidle', {timeout: 30000});

  result.postLoginUrl = page.url();
  result.pageTitle = await page.title();

  await page.waitForSelector('[data-map-open]', {timeout: 30000});
  await page.getByRole('button', {name: 'Elabora'}).click();
  await page.waitForFunction(() => document.body.classList.contains('map-layout-active'), null, {
    timeout: 30000
  });
  result.bodyClass = await page.evaluate(() => document.body.className);

  const iframeHandle = await page.waitForSelector('[data-map-panel] iframe', {
    timeout: 30000,
    state: 'attached'
  });
  await page.waitForFunction(el => Boolean(el && el.getAttribute('src')), iframeHandle, {
    timeout: 30000
  });
  result.iframeSrc = await iframeHandle.getAttribute('src');

  const frame = await iframeHandle.contentFrame();
  assert(frame, 'iframe content frame not available');

  await frame.waitForLoadState('domcontentloaded', {timeout: 30000});
  await frame.waitForFunction(
    () => typeof window.__QMAP_AUTH_TOKEN__ === 'string' && window.__QMAP_AUTH_TOKEN__.length > 20,
    null,
    {timeout: 30000}
  );

  const frameState = await frame.evaluate(async () => {
    const token = typeof window.__QMAP_AUTH_TOKEN__ === 'string' ? window.__QMAP_AUTH_TOKEN__ : '';
    const storageKeys = Object.keys(window.localStorage).filter(key => key.includes('qmap'));
    const response = await fetch('/api/q-assistant/me', {
      credentials: 'same-origin',
      headers: {
        Authorization: `Bearer ${token}`,
        Accept: 'application/json'
      }
    });
    const body = await response.text();
    return {
      tokenPresent: token.length > 20,
      tokenLength: token.length,
      localStorageKeys: storageKeys.sort(),
      assistantMeStatus: response.status,
      assistantMeBody: body
    };
  });

  Object.assign(result, frameState);

  assert(
    result.iframeSrc?.startsWith('/map/') || result.iframeSrc?.startsWith(`${fixture.baseUrl}/map/`),
    `Unexpected iframe src: ${result.iframeSrc}`
  );
  assert(result.tokenPresent, 'Bootstrap token not present in iframe memory');
  assert(result.localStorageKeys.length === 0, `Unexpected qmap localStorage keys: ${result.localStorageKeys.join(',')}`);
  assert(result.assistantMeStatus === 200, `Unexpected /api/q-assistant/me status: ${result.assistantMeStatus}`);

  console.log(JSON.stringify({ok: true, result}, null, 2));
} catch (error) {
  console.error(JSON.stringify({ok: false, result, error: String(error)}, null, 2));
  process.exitCode = 1;
} finally {
  await context.close();
  await browser.close();
}
