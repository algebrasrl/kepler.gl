import fs from 'node:fs/promises';
import path from 'node:path';
import {expect, test, type Locator, type Page} from '@playwright/test';

async function firstVisible(locator: Locator): Promise<Locator> {
  const count = await locator.count();
  for (let i = 0; i < count; i += 1) {
    const candidate = locator.nth(i);
    if (await candidate.isVisible().catch(() => false)) return candidate;
  }
  return locator.first();
}

async function openAssistant(page: Page) {
  const aiToggle = page.getByText(/^AI$/).first();
  await expect(aiToggle).toBeVisible({timeout: 20_000});
  await aiToggle.click();
  await expect(page.getByText(/Assistente|Assistant/i).first()).toBeVisible({timeout: 20_000});
}

async function sendAssistantMessage(page: Page, text: string) {
  await page
    .waitForFunction(() => {
      return Boolean(
        document.querySelector('input[placeholder*="prompt" i]') ||
          document.querySelector('textarea[placeholder*="prompt" i]') ||
          document.querySelector('[aria-label="Prompt"]')
      );
    })
    .catch(() => {});

  const sendButton = page.getByRole('button', {name: /Send|Invia/i});

  const clickComposerSiblingButton = async (input: Locator) => {
    const candidate = input.locator('xpath=ancestor::*[.//button][1]//button[not(@disabled)][1]');
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
    if (!visible || !enabled) return false;
    await button.click();
    return true;
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
    if (!(await clickSendIfAvailable())) {
      await page.keyboard.press('Enter');
    }
    return;
  }

  throw new Error('Assistant input not found');
}

async function waitForExecutionSummaryCount(page: Page, count: number) {
  await page.waitForFunction(
    expected => {
      const text = document.body.innerText || '';
      const matches = [...text.matchAll(/\[executionSummary\]\s*(\{[^\n]+\})/g)];
      const parsed = matches
        .map(match => {
          try {
            return JSON.parse(match[1]);
          } catch {
            return null;
          }
        })
        .filter(Boolean);
      return parsed.length >= expected;
    },
    count,
    {timeout: 240_000}
  );
}

async function waitForAssistantComposerEnabled(page: Page) {
  await page.waitForFunction(() => {
    const el =
      (document.querySelector('textarea[placeholder*="prompt" i]') as HTMLTextAreaElement | null) ||
      (document.querySelector('input[placeholder*="prompt" i]') as HTMLInputElement | null) ||
      (document.querySelector('[aria-label="Prompt"]') as HTMLElement | null);
    if (!el) return false;
    const disabledAttr = (el as HTMLInputElement | HTMLTextAreaElement).disabled;
    const ariaDisabled = String(el.getAttribute('aria-disabled') || '').toLowerCase() === 'true';
    return !disabledAttr && !ariaDisabled;
  }, null, {timeout: 240_000});
}

test.describe.configure({mode: 'serial'});

test('assistant live: treviso smallest municipality flow completes without failed steps', async ({
  page
}) => {
  test.setTimeout(360_000);

  await page.goto('/');
  await openAssistant(page);

  await sendAssistantMessage(page, 'mostra il comune piu piccolo della provincia di treviso');
  await waitForExecutionSummaryCount(page, 1);
  await waitForAssistantComposerEnabled(page);

  await sendAssistantMessage(page, 'mostra su mappa');
  await waitForExecutionSummaryCount(page, 2);
  await waitForAssistantComposerEnabled(page);

  const extracted = await page.evaluate(() => {
    const text = document.body.innerText || '';
    const requestIds = [...text.matchAll(/\[requestId:\s*([^\]]+)\]/g)].map(match => String(match[1] || '').trim());
    const executionSummaries = [...text.matchAll(/\[executionSummary\]\s*(\{[^\n]+\})/g)].map(match => {
      try {
        return JSON.parse(match[1]);
      } catch {
        return {parseError: true};
      }
    });
    return {
      requestIds,
      executionSummaries
    };
  });

  const summaries = extracted.executionSummaries.slice(-2);
  const requestIds = extracted.requestIds.slice(-2);

  expect(summaries.length).toBe(2);
  expect(requestIds.length).toBe(2);
  expect((summaries[0] as any)?.parseError).toBeUndefined();
  expect((summaries[1] as any)?.parseError).toBeUndefined();
  expect(String((summaries[0] as any)?.status || '')).toBe('success');
  expect(String((summaries[1] as any)?.status || '')).toBe('success');
  expect(Number((summaries[0] as any)?.steps?.failed || 0)).toBe(0);
  expect(Number((summaries[1] as any)?.steps?.failed || 0)).toBe(0);

  const outputDir = path.resolve(process.cwd(), 'test-results/assistant-live');
  await fs.mkdir(outputDir, {recursive: true});
  await fs.writeFile(
    path.join(outputDir, 'treviso-smallest-map.json'),
    JSON.stringify(
      {
        capturedAt: new Date().toISOString(),
        prompts: [
          'mostra il comune piu piccolo della provincia di treviso',
          'mostra su mappa'
        ],
        requestIds,
        executionSummaries: summaries
      },
      null,
      2
    ),
    'utf-8'
  );
});
