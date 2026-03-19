import {resolveQMapAuthorizationHeader} from '../../utils/auth-token';

const JSON_RPC_VERSION = '2.0';
const MCP_PROTOCOL_VERSION = '2025-03-26';

type JsonRpcResponse<T> = {
  id?: string | number | null;
  jsonrpc?: string;
  result?: T;
  error?: {code?: number; message?: string; data?: unknown};
};

type McpTool = {
  name: string;
  description?: string;
};

class McpHttpClient {
  private baseUrl: string;
  private sessionId: string | null = null;
  private requestId = 1;
  private initialized = false;
  private initializePromise: Promise<void> | null = null;
  private toolsPromise: Promise<McpTool[]> | null = null;

  constructor(baseUrl: string) {
    this.baseUrl = baseUrl.replace(/\/+$/, '');
  }

  private nextId() {
    const id = this.requestId;
    this.requestId += 1;
    return id;
  }

  private async postRpc<T>(method: string, params?: Record<string, unknown>, isNotification = false): Promise<T> {
    const id = isNotification ? undefined : this.nextId();
    const authorizationHeader = resolveQMapAuthorizationHeader();
    const response = await fetch(`${this.baseUrl}/mcp`, {
      method: 'POST',
      headers: {
        Accept: 'application/json',
        'content-type': 'application/json',
        ...(authorizationHeader ? {Authorization: authorizationHeader} : {}),
        ...(this.sessionId ? {'mcp-session-id': this.sessionId} : {}),
        'mcp-protocol-version': MCP_PROTOCOL_VERSION
      },
      body: JSON.stringify({
        jsonrpc: JSON_RPC_VERSION,
        ...(id !== undefined ? {id} : {}),
        method,
        ...(params ? {params} : {})
      })
    });

    const sessionHeader = response.headers.get('mcp-session-id');
    if (sessionHeader) {
      this.sessionId = sessionHeader;
    }

    const raw = await response.text();
    if (!response.ok) {
      throw new Error(`MCP HTTP ${response.status}: ${raw}`);
    }

    if (!raw.trim()) {
      return undefined as T;
    }

    const payload = JSON.parse(raw) as JsonRpcResponse<T>;
    if (payload.error) {
      throw new Error(`MCP ${payload.error.code ?? ''}: ${payload.error.message || 'Unknown error'}`);
    }
    return payload.result as T;
  }

  private async initialize() {
    if (this.initialized) {
      return;
    }
    if (this.initializePromise) {
      return this.initializePromise;
    }
    this.initializePromise = (async () => {
      const result = await this.postRpc<{protocolVersion?: string}>('initialize', {
        protocolVersion: MCP_PROTOCOL_VERSION,
        capabilities: {},
        clientInfo: {
          name: 'q-map-ai',
          version: '0.0.1'
        }
      });
      if (!this.sessionId) {
        throw new Error(
          'MCP session ID not available after initialize. Check CORS expose headers for mcp-session-id.'
        );
      }
      if (result?.protocolVersion && result.protocolVersion !== MCP_PROTOCOL_VERSION) {
        // Keep client strict for now to avoid mixed protocol behavior.
        throw new Error(`Unsupported MCP protocol version: ${result.protocolVersion}`);
      }
      await this.postRpc('notifications/initialized', {}, true);
      this.initialized = true;
    })();
    return this.initializePromise;
  }

  private async getTools() {
    await this.initialize();
    if (!this.toolsPromise) {
      this.toolsPromise = this.postRpc<{tools?: McpTool[]}>('tools/list', {}).then(
        result => result?.tools || []
      );
    }
    return this.toolsPromise;
  }

  private async resolveToolName(candidates: string[]) {
    const tools = await this.getTools();
    for (const candidate of candidates) {
      const found = tools.find(tool => tool.name === candidate);
      if (found) {
        return found.name;
      }
    }
    throw new Error(`MCP tool not found. Tried: ${candidates.join(', ')}`);
  }

  async callToolParsed(candidates: string[], args: Record<string, unknown>) {
    await this.initialize();
    const toolName = await this.resolveToolName(candidates);
    const result = await this.postRpc<{content?: Array<{type?: string; text?: string}>}>('tools/call', {
      name: toolName,
      arguments: args
    });

    const text = (result?.content || []).find(item => item?.type === 'text' && typeof item.text === 'string')
      ?.text;
    if (!text) {
      return null;
    }
    try {
      return JSON.parse(text);
    } catch {
      return {text};
    }
  }
}

const CLIENTS = new Map<string, McpHttpClient>();

function getClient(baseUrl: string) {
  const key = baseUrl.replace(/\/+$/, '');
  if (!CLIENTS.has(key)) {
    CLIENTS.set(key, new McpHttpClient(key));
  }
  return CLIENTS.get(key)!;
}

export async function callMcpToolParsed(
  baseUrl: string,
  candidates: string[],
  args: Record<string, unknown> = {}
) {
  return getClient(baseUrl).callToolParsed(candidates, args);
}
