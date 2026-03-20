#!/usr/bin/env node

function parseNumber(value, fallback) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function parseArgs(argv) {
  const out = {
    timeoutSec: parseNumber(process.env.QMAP_BACKEND_READY_TIMEOUT_SEC, 180),
    intervalMs: parseNumber(process.env.QMAP_BACKEND_READY_INTERVAL_MS, 2000),
    checks: [
      {name: 'q-assistant', url: 'http://localhost:3004/health'},
      {name: 'q-cumber-backend', url: 'http://localhost:3001/health'},
      {name: 'q-storage-backend', url: 'http://localhost:3005/health'},
      {name: 'kong', url: 'http://localhost:8001/status'}
    ]
  };

  for (let i = 0; i < argv.length; i += 1) {
    const arg = String(argv[i] || '');
    const value = argv[i + 1];
    if (arg === '--timeout-sec' && value)
      out.timeoutSec = Math.max(1, Math.trunc(parseNumber(value, out.timeoutSec)));
    if (arg === '--interval-ms' && value)
      out.intervalMs = Math.max(100, Math.trunc(parseNumber(value, out.intervalMs)));
  }
  return out;
}

async function waitHealth(url, timeoutSec, intervalMs) {
  const deadline = Date.now() + timeoutSec * 1000;
  let lastError = '';
  while (Date.now() <= deadline) {
    try {
      const controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), 5000);
      const res = await fetch(url, {signal: controller.signal});
      clearTimeout(timeout);
      if (res.ok) return {ok: true, status: res.status, error: ''};
      lastError = `status=${res.status}`;
    } catch (error) {
      const message = String(error?.message || error || 'fetch failed');
      lastError = message;
      if (/operation not permitted|eperm/i.test(message)) {
        return {
          ok: false,
          status: null,
          error:
            'sandbox localhost restriction (EPERM) detected; run this target with elevated/out-of-sandbox permissions'
        };
      }
    }
    await sleep(intervalMs);
  }
  return {ok: false, status: null, error: lastError || `timeout after ${timeoutSec}s`};
}

async function main() {
  const opts = parseArgs(process.argv.slice(2));

  process.stdout.write(
    `[backend-ready] health-check timeout=${opts.timeoutSec}s interval=${opts.intervalMs}ms\n`
  );
  for (const check of opts.checks) {
    const result = await waitHealth(check.url, opts.timeoutSec, opts.intervalMs);
    if (!result.ok) {
      process.stderr.write(`[backend-ready] FAIL ${check.name} ${check.url} (${result.error})\n`);
      process.stderr.write(
        '[backend-ready] Backends not healthy. Start them with: make dev-local\n'
      );
      process.exit(2);
    }
    process.stdout.write(`[backend-ready] OK ${check.name} ${check.url} status=${result.status}\n`);
  }

  process.stdout.write('[backend-ready] READY\n');
}

main().catch(error => {
  process.stderr.write(`[backend-ready] fatal: ${String(error?.message || error)}\n`);
  process.exit(1);
});
