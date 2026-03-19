import {resolveQMapAuthBearerToken, setQMapAuthBearerToken} from './auth-token';

const QMAP_AUTH_BOOTSTRAP_REQUEST_TYPE = 'QMAP_AUTH_BOOTSTRAP_REQUEST';
const QH_QMAP_AUTH_BOOTSTRAP_TYPE = 'QH_QMAP_AUTH_BOOTSTRAP';
const QMAP_IFRAME_MESSAGE_VERSION = 1;
const QMAP_IFRAME_MESSAGE_SOURCE = 'q-map';
const QH_QMAP_AUTH_MESSAGE_SOURCE = 'q-hive';
const QMAP_AUTH_BOOTSTRAP_TIMEOUT_MS = 1500;

type QMapBootstrapResponsePayload = {
  accessToken?: string;
  expiresAt?: number;
  issuedAt?: number;
  expiresIn?: number;
};

function resolveOriginFromUrl(rawValue: string | null | undefined): string {
  const candidate = String(rawValue || '').trim();
  if (!candidate) {
    return '';
  }
  try {
    return new URL(candidate).origin;
  } catch {
    return '';
  }
}

function resolveParentOrigin(): string {
  if (typeof document !== 'undefined') {
    const referrerOrigin = resolveOriginFromUrl(document.referrer);
    if (referrerOrigin) {
      return referrerOrigin;
    }
  }
  return '';
}

function isAuthBootstrapMessage(value: unknown): value is {
  type: typeof QH_QMAP_AUTH_BOOTSTRAP_TYPE;
  source: typeof QH_QMAP_AUTH_MESSAGE_SOURCE;
  version: typeof QMAP_IFRAME_MESSAGE_VERSION;
  payload: QMapBootstrapResponsePayload;
} {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    return false;
  }
  const message = value as Record<string, unknown>;
  if (message.type !== QH_QMAP_AUTH_BOOTSTRAP_TYPE) {
    return false;
  }
  if (message.source !== QH_QMAP_AUTH_MESSAGE_SOURCE) {
    return false;
  }
  if (message.version !== QMAP_IFRAME_MESSAGE_VERSION) {
    return false;
  }
  if (!message.payload || typeof message.payload !== 'object' || Array.isArray(message.payload)) {
    return false;
  }
  return true;
}

export async function bootstrapQMapAuthFromParent(): Promise<boolean> {
  if (resolveQMapAuthBearerToken()) {
    return true;
  }
  if (typeof window === 'undefined' || window.self === window.top || !window.parent) {
    return false;
  }
  const parentOrigin = resolveParentOrigin();
  if (!parentOrigin) {
    return false;
  }

  return await new Promise<boolean>(resolve => {
    let settled = false;
    const finish = (value: boolean) => {
      if (settled) {
        return;
      }
      settled = true;
      window.removeEventListener('message', onMessage);
      window.clearTimeout(timeoutId);
      resolve(value);
    };

    const onMessage = (event: MessageEvent) => {
      if (event.origin !== parentOrigin) {
        return;
      }
      if (!isAuthBootstrapMessage(event.data)) {
        return;
      }
      const token = String(event.data.payload.accessToken || '').trim();
      if (!token) {
        finish(false);
        return;
      }
      setQMapAuthBearerToken(token);
      finish(Boolean(resolveQMapAuthBearerToken()));
    };

    const timeoutId = window.setTimeout(() => finish(false), QMAP_AUTH_BOOTSTRAP_TIMEOUT_MS);
    window.addEventListener('message', onMessage);
    window.parent.postMessage(
      {
        type: QMAP_AUTH_BOOTSTRAP_REQUEST_TYPE,
        source: QMAP_IFRAME_MESSAGE_SOURCE,
        version: QMAP_IFRAME_MESSAGE_VERSION,
        payload: {
          sentAt: new Date().toISOString()
        }
      },
      parentOrigin
    );
  });
}
