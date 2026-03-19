#!/usr/bin/env node

import {copyFileSync, existsSync, readFileSync, writeFileSync} from 'node:fs';
import path from 'node:path';
import {execSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';

const ROOT_DIR = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const BACKENDS_DIR = path.join(ROOT_DIR, 'backends');
const BACKENDS_ENV_PATH = path.join(BACKENDS_DIR, '.env');
const BACKENDS_ENV_EXAMPLE_PATH = path.join(BACKENDS_DIR, '.env.example');
const FRONTEND_ENV_PATH = path.join(ROOT_DIR, '.env.development.local');
const FRONTEND_ENV_EXAMPLE_PATH = path.join(ROOT_DIR, '.env.development.example');
const MINT_JWT_SCRIPT_PATH = path.join(BACKENDS_DIR, 'kong', 'scripts', 'mint-dev-jwt.py');

function parseArgs(argv) {
  const options = {
    domain: 'localhost',
    startFrontend: true,
    startBackend: true,
    mintToken: true
  };
  for (let i = 0; i < argv.length; i += 1) {
    const token = String(argv[i] || '').trim();
    if (!token) continue;
    if (token === '--no-frontend') {
      options.startFrontend = false;
      continue;
    }
    if (token === '--no-backend') {
      options.startBackend = false;
      continue;
    }
    if (token === '--no-token') {
      options.mintToken = false;
      continue;
    }
    if (token === '--domain') {
      const next = String(argv[i + 1] || '').trim();
      if (next) {
        options.domain = next;
        i += 1;
      }
      continue;
    }
    if (token.startsWith('--domain=')) {
      options.domain = token.slice('--domain='.length).trim() || options.domain;
      continue;
    }
  }
  return options;
}

function toGatewayBase(domain) {
  const normalized = String(domain || '').trim();
  const host = normalized || 'localhost';
  const hasScheme = /^https?:\/\//i.test(host);
  const baseHost = hasScheme ? host : `http://${host}`;
  return `${baseHost.replace(/\/+$/, '')}:8000`;
}

function runCommand(command, cwd = ROOT_DIR) {
  execSync(command, {
    cwd,
    stdio: 'inherit',
    env: process.env
  });
}

function runCommandCapture(command, cwd = ROOT_DIR) {
  return execSync(command, {
    cwd,
    stdio: ['ignore', 'pipe', 'inherit'],
    env: process.env
  })
    .toString()
    .trim();
}

function ensureBackendsEnvFile() {
  if (existsSync(BACKENDS_ENV_PATH)) return;
  copyFileSync(BACKENDS_ENV_EXAMPLE_PATH, BACKENDS_ENV_PATH);
  console.log('[dev-local] created backends/.env from .env.example');
}

function ensureFrontendEnvFile() {
  if (existsSync(FRONTEND_ENV_PATH)) return;
  if (existsSync(FRONTEND_ENV_EXAMPLE_PATH)) {
    copyFileSync(FRONTEND_ENV_EXAMPLE_PATH, FRONTEND_ENV_PATH);
    console.log('[dev-local] created .env.development.local from .env.development.example');
    return;
  }
  writeFileSync(FRONTEND_ENV_PATH, '', 'utf8');
  console.log('[dev-local] created empty .env.development.local');
}

function setEnvVar(content, key, value) {
  const escaped = key.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const line = `${key}=${value}`;
  const regex = new RegExp(`^${escaped}=.*$`, 'm');
  if (regex.test(content)) {
    return content.replace(regex, line);
  }
  const suffix = content.endsWith('\n') ? '' : '\n';
  return `${content}${suffix}${line}\n`;
}

function unsetEnvVar(content, key) {
  const escaped = key.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  return content.replace(new RegExp(`^${escaped}=.*(?:\\n|$)`, 'm'), '');
}

function updateFrontendEnv({gatewayBase, token}) {
  ensureFrontendEnvFile();
  const current = existsSync(FRONTEND_ENV_PATH) ? readFileSync(FRONTEND_ENV_PATH, 'utf8') : '';
  let next = current;
  next = setEnvVar(next, 'VITE_QMAP_AI_PROXY_BASE', `${gatewayBase}/api/q-assistant`);
  next = setEnvVar(next, 'VITE_QCUMBER_CLOUD_API_BASE', `${gatewayBase}/api/q-cumber`);
  next = setEnvVar(next, 'VITE_QSTORAGE_CLOUD_API_BASE', `${gatewayBase}/api/q-storage`);
  if (token) {
    next = setEnvVar(next, 'QMAP_AUTH_RUNTIME_TOKEN', token);
  }
  next = unsetEnvVar(next, 'VITE_QMAP_AUTH_BEARER_TOKEN');
  writeFileSync(FRONTEND_ENV_PATH, next, 'utf8');
  console.log('[dev-local] updated .env.development.local gateway/auth values');
}

function mintDevToken() {
  return runCommandCapture(`python3 "${MINT_JWT_SCRIPT_PATH}"`, BACKENDS_DIR);
}

function printSummary({gatewayBase, domain, startFrontend}) {
  const normalizedDomain = String(domain || '').trim();
  const hostForUi = normalizedDomain || 'localhost';
  const uiBase = /^https?:\/\//i.test(hostForUi) ? hostForUi : `http://${hostForUi}`;
  console.log('');
  console.log('[dev-local] ready');
  console.log(`  UI: ${uiBase}:8081`);
  console.log(`  Gateway: ${gatewayBase}`);
  console.log('  If needed, add /etc/hosts entry: 127.0.0.1 local.q-hive.it');
  if (!startFrontend) {
    console.log('  Start frontend manually: yarn --cwd examples/q-map dev --host 0.0.0.0 --port 8081');
  }
}

function main() {
  const options = parseArgs(process.argv.slice(2));
  const gatewayBase = toGatewayBase(options.domain);

  ensureBackendsEnvFile();

  if (options.startBackend) {
    console.log('[dev-local] starting backend stack (Kong default)...');
    runCommand('make -C backends up', ROOT_DIR);
  } else {
    console.log('[dev-local] skipping backend startup');
  }

  let token = '';
  if (options.mintToken) {
    console.log('[dev-local] minting local JWT for Kong...');
    token = mintDevToken();
  } else {
    console.log('[dev-local] skipping token mint');
  }

  updateFrontendEnv({gatewayBase, token});
  printSummary({gatewayBase, domain: options.domain, startFrontend: options.startFrontend});

  if (options.startFrontend) {
    console.log('[dev-local] starting frontend dev server...');
    runCommand('yarn dev --host 0.0.0.0 --port 8081', ROOT_DIR);
  }
}

main();
