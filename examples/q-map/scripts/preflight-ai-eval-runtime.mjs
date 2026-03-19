#!/usr/bin/env node
import {execFile} from 'node:child_process';
import {promisify} from 'node:util';

const execFileAsync = promisify(execFile);

function parseArgs(argv) {
  const out = {
    baseUrl:
      process.env.QMAP_AI_EVAL_BASE_URL ||
      process.env.EVAL_BASE_URL ||
      'http://localhost:8000/api/q-assistant',
    bearerToken: String(
      process.env.QMAP_AI_EVAL_BEARER_TOKEN || process.env.EVAL_BEARER_TOKEN || ''
    ).trim(),
    timeoutSec: 5,
    retries: Math.max(1, Number(process.env.QMAP_AI_EVAL_PREFLIGHT_RETRIES || 6) || 6),
    retryDelayMs: Math.max(100, Number(process.env.QMAP_AI_EVAL_PREFLIGHT_RETRY_DELAY_MS || 1500) || 1500)
  };
  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    const val = argv[i + 1];
    if (arg === '--base-url' && val) out.baseUrl = String(val);
    if (arg === '--bearer-token' && val) out.bearerToken = String(val).trim();
    if (arg === '--timeout-sec' && val) out.timeoutSec = Math.max(1, Number(val) || 5);
    if (arg === '--retries' && val) out.retries = Math.max(1, Number(val) || out.retries);
    if (arg === '--retry-delay-ms' && val) out.retryDelayMs = Math.max(100, Number(val) || out.retryDelayMs);
  }
  return out;
}

function normalizeBaseUrl(baseUrl) {
  return String(baseUrl || 'http://localhost:8000/api/q-assistant').replace(/\/+$/, '');
}

function isSandboxEpermError(message) {
  const text = String(message || '').toLowerCase();
  return text.includes('eperm') || text.includes('connect eperm') || text.includes('operation not permitted');
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function resolveAuthHeaders(token) {
  const bearerToken = String(token || '').trim();
  if (!bearerToken) return {};
  return {authorization: `Bearer ${bearerToken}`};
}

async function checkCurlHealth(url, timeoutSec, authHeaders) {
  try {
    const args = ['-sS', '-m', String(timeoutSec)];
    for (const [key, value] of Object.entries(authHeaders || {})) {
      args.push('-H', `${key}: ${value}`);
    }
    args.push(url);
    const {stdout} = await execFileAsync('curl', args, {
      maxBuffer: 1024 * 1024
    });
    const payload = String(stdout || '').trim();
    let ok = false;
    let error = '';
    try {
      const parsed = JSON.parse(payload);
      ok = parsed && parsed.ok === true;
      if (!ok && /unauthorized|forbidden/i.test(String(parsed?.message || parsed?.detail || ''))) {
        error = 'status=401 unauthorized';
      }
    } catch {
      ok = false;
    }
    return {ok, payload, error};
  } catch (error) {
    return {
      ok: false,
      payload: '',
      error: String(error?.stderr || error?.message || error)
    };
  }
}

async function checkNodeFetchHealth(url, timeoutSec, authHeaders) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutSec * 1000);
  try {
    const res = await fetch(url, {
      method: 'GET',
      headers: {
        accept: 'application/json',
        ...authHeaders
      },
      signal: controller.signal
    });
    const body = await res.text();
    let ok = false;
    try {
      const parsed = JSON.parse(body);
      ok = res.ok && parsed && parsed.ok === true;
    } catch {
      ok = false;
    }
    return {ok, status: res.status, body, error: ''};
  } catch (error) {
    return {
      ok: false,
      status: 0,
      body: '',
      error: String(error?.stack || error?.message || error)
    };
  } finally {
    clearTimeout(timeout);
  }
}

async function main() {
  const opts = parseArgs(process.argv.slice(2));
  const base = normalizeBaseUrl(opts.baseUrl);
  const healthUrl = `${base}/health`;
  const authHeaders = resolveAuthHeaders(opts.bearerToken);

  let curl = {ok: false, payload: '', error: ''};
  for (let attempt = 1; attempt <= opts.retries; attempt += 1) {
    curl = await checkCurlHealth(healthUrl, opts.timeoutSec, authHeaders);
    if (curl.ok) {
      process.stdout.write(`[preflight] curl /health OK (attempt ${attempt}/${opts.retries})\n`);
      break;
    }
    process.stdout.write(
      `[preflight] curl /health retry ${attempt}/${opts.retries} FAIL: ${curl.error || 'unexpected payload'}\n`
    );
    if (attempt < opts.retries) await sleep(opts.retryDelayMs);
  }
  if (!curl.ok && curl.payload) {
    process.stdout.write(`[preflight] curl payload: ${curl.payload}\n`);
  }

  let nodeFetch = {ok: false, status: 0, body: '', error: ''};
  for (let attempt = 1; attempt <= opts.retries; attempt += 1) {
    nodeFetch = await checkNodeFetchHealth(healthUrl, opts.timeoutSec, authHeaders);
    if (nodeFetch.ok) {
      process.stdout.write(`[preflight] node-fetch /health OK (attempt ${attempt}/${opts.retries})\n`);
      break;
    }
    process.stdout.write(
      `[preflight] node-fetch /health retry ${attempt}/${opts.retries} FAIL: ${nodeFetch.error || 'unexpected payload'}\n`
    );
    if (isSandboxEpermError(nodeFetch.error)) {
      process.stdout.write(
        '[preflight][hint] Detected probable sandbox localhost restriction (EPERM).\n' +
          '[preflight][hint] Re-run make targets with elevated/out-of-sandbox permissions.\n'
      );
      break;
    }
    if (attempt < opts.retries) await sleep(opts.retryDelayMs);
  }
  if (!nodeFetch.ok && nodeFetch.body) {
    process.stdout.write(`[preflight] node-fetch payload: ${nodeFetch.body}\n`);
  }

  const authFailure =
    /unauthorized|forbidden/i.test(String(curl.error || '')) || nodeFetch.status === 401 || nodeFetch.status === 403;
  if ((!curl.ok || !nodeFetch.ok) && authFailure && !opts.bearerToken) {
    process.stdout.write(
      '[preflight][hint] Endpoint requires bearer auth. Set QMAP_AI_EVAL_BEARER_TOKEN (or EVAL_BEARER_TOKEN).\n'
    );
  }

  if (!curl.ok || !nodeFetch.ok) {
    process.exit(2);
  }
  process.stdout.write(`[preflight] PASS baseUrl=${base}\n`);
}

main().catch(error => {
  process.stderr.write(`[preflight] fatal: ${String(error?.message || error)}\n`);
  process.exit(1);
});
