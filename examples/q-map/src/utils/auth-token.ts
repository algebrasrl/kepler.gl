import {resolveQMapRuntimeAuthToken} from './runtime-config';

const DEFAULT_AUTH_TOKEN_STORAGE_KEYS = [
  'qmap_gateway_jwt',
  'qmap_auth_token',
  'qmap_access_token'
];

const TOKEN_OBJECT_CANDIDATE_KEYS = [
  'access_token',
  'accessToken',
  'token',
  'jwt',
  'id_token',
  'idToken'
];

const ALLOW_OPAQUE_BEARER_TOKENS =
  String(import.meta.env.VITE_QMAP_AUTH_ALLOW_OPAQUE_BEARER || '').trim().toLowerCase() === 'true';

type BrowserWindowWithAuth = Window & {
  __QMAP_AUTH_TOKEN__?: unknown;
};

function normalizeTokenString(value: unknown): string {
  const raw = String(value || '').trim();
  if (!raw) return '';
  if (/^bearer\s+/i.test(raw)) {
    return raw.replace(/^bearer\s+/i, '').trim();
  }
  return raw;
}

function isUsableToken(value: unknown): boolean {
  const token = normalizeTokenString(value);
  if (!token) return false;
  if (token.includes(' ')) return false;
  return token.length >= 16;
}

function decodeBase64Url(value: string): string {
  const normalized = value.replace(/-/g, '+').replace(/_/g, '/');
  const padded = normalized.padEnd(Math.ceil(normalized.length / 4) * 4, '=');
  return atob(padded);
}

function isJwtLikeToken(value: unknown): boolean {
  const token = normalizeTokenString(value);
  const segments = token.split('.');
  if (segments.length !== 3) return false;
  if (!segments.every(segment => segment.length > 0)) return false;
  try {
    const header = JSON.parse(decodeBase64Url(segments[0]));
    const payload = JSON.parse(decodeBase64Url(segments[1]));
    const headerOk = header && typeof header === 'object' && !Array.isArray(header);
    const payloadOk = payload && typeof payload === 'object' && !Array.isArray(payload);
    return Boolean(headerOk && payloadOk);
  } catch {
    return false;
  }
}

function parseJwtPayload(value: unknown): Record<string, unknown> | null {
  const token = normalizeTokenString(value);
  const segments = token.split('.');
  if (segments.length !== 3) return null;
  try {
    const payload = JSON.parse(decodeBase64Url(segments[1]));
    if (!payload || typeof payload !== 'object' || Array.isArray(payload)) return null;
    return payload as Record<string, unknown>;
  } catch {
    return null;
  }
}

function isJwtTokenActive(value: unknown): boolean {
  const payload = parseJwtPayload(value);
  if (!payload) return false;

  const now = Math.floor(Date.now() / 1000);
  const exp = Number(payload.exp);
  if (Number.isFinite(exp) && exp <= now) {
    return false;
  }

  const nbf = Number(payload.nbf);
  if (Number.isFinite(nbf) && nbf > now) {
    return false;
  }

  return true;
}

function isAcceptedAuthToken(value: unknown): boolean {
  if (!isUsableToken(value)) return false;
  if (ALLOW_OPAQUE_BEARER_TOKENS) return true;
  return isJwtLikeToken(value) && isJwtTokenActive(value);
}

function parseTokenFromObject(raw: unknown): string {
  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) return '';
  const payload = raw as Record<string, unknown>;
  for (const key of TOKEN_OBJECT_CANDIDATE_KEYS) {
    const candidate = payload[key];
    if (isUsableToken(candidate)) {
      return normalizeTokenString(candidate);
    }
  }
  return '';
}

function parseTokenFromStorageValue(value: unknown): string {
  if (isUsableToken(value)) {
    return normalizeTokenString(value);
  }
  const raw = String(value || '').trim();
  if (!raw || (raw[0] !== '{' && raw[0] !== '[')) {
    return '';
  }
  try {
    const parsed = JSON.parse(raw);
    if (isUsableToken(parsed)) {
      return normalizeTokenString(parsed);
    }
    return parseTokenFromObject(parsed);
  } catch {
    return '';
  }
}

function parseStorageKeysFromEnv(): string[] {
  const raw = String(import.meta.env.VITE_QMAP_AUTH_TOKEN_STORAGE_KEYS || '').trim();
  if (!raw) return DEFAULT_AUTH_TOKEN_STORAGE_KEYS;
  const values = raw
    .split(/[;,]/g)
    .map(value => value.trim())
    .filter(Boolean);
  return values.length ? values : DEFAULT_AUTH_TOKEN_STORAGE_KEYS;
}

function readTokenFromStorage(storage: Storage | null | undefined, keys: string[]): string {
  if (!storage) return '';
  for (const key of keys) {
    const value = storage.getItem(key);
    const token = parseTokenFromStorageValue(value);
    if (isAcceptedAuthToken(token)) return normalizeTokenString(token);
  }
  return '';
}

export function resolveQMapAuthBearerToken(): string {
  if (typeof window === 'undefined') {
    return '';
  }
  const browserWindow = window as BrowserWindowWithAuth;
  const keys = parseStorageKeysFromEnv();

  const directWindowToken = parseTokenFromStorageValue(
    browserWindow.__QMAP_AUTH_TOKEN__ || resolveQMapRuntimeAuthToken()
  );
  if (isAcceptedAuthToken(directWindowToken)) return normalizeTokenString(directWindowToken);

  const storageToken =
    readTokenFromStorage(window.sessionStorage, keys) || readTokenFromStorage(window.localStorage, keys);
  if (isAcceptedAuthToken(storageToken)) return normalizeTokenString(storageToken);

  return '';
}

export function setQMapAuthBearerToken(token: string): void {
  if (typeof window === 'undefined') {
    return;
  }
  const nextToken = normalizeTokenString(token);
  const browserWindow = window as BrowserWindowWithAuth;
  browserWindow.__QMAP_AUTH_TOKEN__ = isAcceptedAuthToken(nextToken) ? nextToken : '';
}

export function resolveQMapAuthorizationHeader(): string {
  const token = resolveQMapAuthBearerToken();
  if (!token) return '';
  return `Bearer ${token}`;
}
