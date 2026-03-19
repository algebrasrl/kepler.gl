# q-assistant

FastAPI proxy backend for q-map custom AI assistant.

## Run

```bash
cd examples/q-map/backends/q-assistant
python -m venv .venv
source .venv/bin/activate
pip install -e .
q-assistant
```

Default server: `http://localhost:3004`

## Fast switch (Docker)

From `examples/q-map/backends`, use:

```bash
./switch-q-assistant.sh openrouter google/gemini-3-flash-preview
./switch-q-assistant.sh openai gpt-4o-mini
./switch-q-assistant.sh ollama qwen3-coder:30b http://host.docker.internal:11434
./switch-q-assistant.sh openai gpt-4o-mini --build
```

The script updates `.env` and clears `Q_ASSISTANT_AGENT_CHAIN` by default so provider/model selection is deterministic.
Use `--keep-chain` if you intentionally want to keep the existing fallback chain.

## Local Ollama

Use these env vars to run q-assistant against a local Ollama instance:

```bash
Q_ASSISTANT_PROVIDER=ollama
Q_ASSISTANT_MODEL=llama3.1
Q_ASSISTANT_BASE_URL=http://localhost:11434
# no API key required for local Ollama
```

Accepted `Q_ASSISTANT_BASE_URL` forms for Ollama are:
- `http://localhost:11434`
- `http://localhost:11434/api`
- `http://localhost:11434/v1`

`q-assistant` normalizes them automatically:
- `/chat` -> Ollama native `/api/chat`
- `/chat/completions` -> Ollama OpenAI-compatible `/v1/chat/completions`

## OpenRouter

Use these env vars to run q-assistant with OpenRouter as explicit provider:

```bash
Q_ASSISTANT_PROVIDER=openrouter
Q_ASSISTANT_MODEL=google/gemini-3-flash-preview
Q_ASSISTANT_BASE_URL=https://openrouter.ai/api/v1
OPENROUTER_API_KEY=...
# optional attribution headers
OPENROUTER_HTTP_REFERER=https://your-app.example
OPENROUTER_X_TITLE=q-map
```

Implementation note:
- OpenRouter and OpenAI traffic is executed via the official OpenAI Python SDK (`AsyncOpenAI`), with provider-specific `base_url`.
- OpenRouter is not using the OpenRouter beta SDK.
- This applies to both `/chat` and `/chat/completions`; non-OpenAI/OpenRouter providers keep their existing transport logic.
- Before upstream calls, OpenAI-style tool schemas are normalized so `required` only contains properties actually declared in each object schema (prevents Gemini/OpenRouter `INVALID_ARGUMENT` errors on tool declarations).
- For Gemini-family models (also when routed via OpenRouter), tool parameter schemas are additionally sanitized to a Gemini-compatible subset before dispatch.

### Agent Chain (Fallback)

You can configure a fallback chain of agents/models via env:

```bash
Q_ASSISTANT_AGENT_CHAIN=ollama|qwen3-coder:30b|http://host.docker.internal:11434,ollama|gpt-oss:20b|http://host.docker.internal:11434
```

Behavior:
- `q-assistant` tries agents in order.
- On retryable upstream errors (e.g. 429/5xx/timeout/model unavailable), it switches to the next one.
- If all fail, it returns: `Nessun agente disponibile per soddisfare la richiesta`.
- `/chat` includes `switchLog` in the response.
- `/chat/completions` includes `qAssistant.switchLog` (non-stream) or `x-q-assistant-*` headers (stream).

## Endpoints

- `GET /health`
- `POST /chat`
- `POST /chat/completions`
- `GET/POST/DELETE /mcp` (real MCP server endpoint via `fastapi-mcp`, HTTP transport)
- q-map MCP-oriented helper routes (also exposed via MCP wrapper):
  - `GET /qmap/mcp/list-cloud-maps?provider=q-storage-backend|q-cumber-backend`
  - `GET /qmap/mcp/cloud-status?provider=q-storage-backend|q-cumber-backend`
  - `GET /qmap/mcp/get-cloud-map?map_id=<id>&provider=q-storage-backend|q-cumber-backend`
  - `POST /qmap/mcp/build-load-cloud-map-action`
  - `POST /qmap/mcp/build-equals-filter-action`

q-map frontend uses these helper routes for cloud-map listing/loading and filter-action building, while map side-effects are applied in the browser (Redux dispatch).

`/chat` request shape:

```json
{
  "prompt": "string",
  "context": {"map": {}, "datasets": []},
  "agent": {
    "provider": "openai|openrouter|ollama",
    "model": "string",
    "baseUrl": "string",
    "apiKey": "string",
    "temperature": 0.0,
    "topP": 1.0
  }
}
```

`/chat` accepts only canonical payloads with `prompt`; OpenAI-style `messages`/`message` bodies are not supported on this endpoint (use `/chat/completions` for OpenAI-compatible payloads).

`/chat` response shape:

```json
{
  "answer": "string",
  "provider": "string",
  "model": "string"
}
```

## Env vars

- `Q_ASSISTANT_HOST` (default `0.0.0.0`)
- `Q_ASSISTANT_PORT` (default `3004`)
- `Q_ASSISTANT_CORS_ORIGINS` (comma-separated, default `http://localhost:8081`)
- `Q_ASSISTANT_PROVIDER` (default `openrouter`)
- `Q_ASSISTANT_MODEL` (default `google/gemini-3-flash-preview`)
- `Q_ASSISTANT_BASE_URL` (provider default; for ollama default is `http://localhost:11434`)
- `Q_ASSISTANT_AGENT_CHAIN` (optional fallback chain: `provider|model|baseUrl,...`)
- `Q_ASSISTANT_EXPLICIT_TOOL_ROUTING` (default `true`; force explicit tool name commands to the matching tool)
- `Q_ASSISTANT_ENABLE_QMAP_CONTEXT` (default `true`; allows runtime q-map context from header `x-qmap-context` on `/chat/completions`)
- `Q_ASSISTANT_QMAP_CONTEXT_MAX_CHARS` (default `12000`; max context chars injected into system message)
- `Q_ASSISTANT_CHAT_AUDIT_ENABLED` (default `true`; enable JSONL audit logging for `/chat` and `/chat/completions`)
- `Q_ASSISTANT_CHAT_AUDIT_LOG_PATH` (default `/tmp/q-assistant-chat-audit`; directory for per-session JSONL files; docker-compose default is `/var/log/q-assistant/chat-audit`)
- `Q_ASSISTANT_CHAT_AUDIT_MAX_CHARS` (default `0`; `0` disables compaction/truncation and keeps full serialized event)
- `Q_ASSISTANT_CHAT_AUDIT_MAX_LIST_ITEMS` (default `0`; `0` disables list slicing during sanitization)
- `Q_ASSISTANT_CHAT_AUDIT_MAX_STRING_CHARS` (default `0`; `0` disables string truncation during sanitization)
- `Q_ASSISTANT_CHAT_AUDIT_INCLUDE_PAYLOADS` (default `true`; include sanitized request/response payloads)
- `Q_ASSISTANT_CHAT_AUDIT_INCLUDE_CONTEXT` (default `false`; include sanitized `x-qmap-context` snapshot in audit)
- `Q_ASSISTANT_CHAT_AUDIT_STDOUT_ENABLED` (default `false`; mirrors each audit event to stdout as one JSON line for external log collectors)
- `Q_ASSISTANT_API_KEY`
- `OPENAI_API_KEY` (optional fallback when provider is `openai`)
- `OPENROUTER_API_KEY` (optional fallback when provider is `openrouter`)
- `OPENROUTER_HTTP_REFERER` (optional extra header for OpenRouter)
- `OPENROUTER_X_TITLE` (optional extra header for OpenRouter)
- `Q_ASSISTANT_TEMPERATURE`
- `Q_ASSISTANT_TOP_P`
- `Q_ASSISTANT_TIMEOUT` (seconds, default `45`)
- `Q_ASSISTANT_TOKEN_BUDGET_ENABLED` (default `true`; enable adaptive payload budget guardrails)
- `Q_ASSISTANT_TOKEN_BUDGET_CONTEXT_LIMIT` (default `0`; force prompt window size, `0` = infer by model)
- `Q_ASSISTANT_TOKEN_BUDGET_DEFAULT_CONTEXT_LIMIT` (default `128000`; fallback when model hint is unknown)
- `Q_ASSISTANT_TOKEN_BUDGET_RESERVED_OUTPUT_TOKENS` (default `4096`; prompt budget reserve for model output)
- `Q_ASSISTANT_TOKEN_BUDGET_WARN_RATIO` (default `0.6`)
- `Q_ASSISTANT_TOKEN_BUDGET_COMPACT_RATIO` (default `0.75`; apply compact profile)
- `Q_ASSISTANT_TOKEN_BUDGET_HARD_RATIO` (default `0.94`; apply hard profile)
- `Q_ASSISTANT_UPSTREAM_RETRY_ATTEMPTS` (default `2`; total attempts = retries + 1)
- `Q_ASSISTANT_UPSTREAM_RETRY_BASE_DELAY` (seconds, default `1.0`)
- `Q_ASSISTANT_UPSTREAM_RETRY_MAX_DELAY` (seconds, default `8`)
- `Q_ASSISTANT_UPSTREAM_RETRY_JITTER_RATIO` (default `0.2`; randomized backoff jitter)
- `Q_ASSISTANT_UPSTREAM_RETRY_TIMEOUT_INCREMENT` (seconds per retry, default `5`)
- `Q_ASSISTANT_QCUMBER_CLOUD_API_BASE` (default `http://127.0.0.1:3001`)
- `Q_ASSISTANT_QCUMBER_CLOUD_TOKEN` (optional bearer token for cloud API)
- `Q_ASSISTANT_QCUMBER_CLOUD_TIMEOUT` (seconds, default `20`)
- `Q_ASSISTANT_QSTORAGE_CLOUD_API_BASE` (default `http://127.0.0.1:3005`)
- `Q_ASSISTANT_QSTORAGE_CLOUD_TOKEN` (optional bearer token for cloud API)
- `Q_ASSISTANT_QSTORAGE_CLOUD_TIMEOUT` (seconds, default `20`)

### Token-Budget Guardrails

Compaction is intentionally context-preserving and provider-agnostic:

- preserve valid tool-call/tool-result sequencing (`assistant.tool_calls` + contiguous `role=tool` messages)
- reduce payload depth before removing execution context (tool schemas/tool payloads are compacted before aggressive message drops)
- keep key dataset metadata in compacted tool summaries (`providerId`, `datasetId`, `datasetName`, `loadedDatasetName`, `returned`, `totalMatched`, `count`)
- keep schema compaction non-aggressive (`required` normalized against `properties`) to avoid invalid tool schemas upstream
- inject a short `[OBJECTIVE_ANCHOR]` + `[OBJECTIVE_CRITERIA]` into system context before compaction so active goal survives history trimming

## Chat Audit Log (debug)

When `Q_ASSISTANT_CHAT_AUDIT_ENABLED=true`, q-assistant writes one JSON line per chat request into a per-session file:
- file pattern: `session-<sessionId>.jsonl` under `Q_ASSISTANT_CHAT_AUDIT_LOG_PATH`
- if no session id is found in payload/context, it falls back to `session-default.jsonl`

Each JSON line includes:

- stable parse envelope: `auditSchema=qmap.chat_audit.v1`, `eventType=chat.audit`, `service=q-assistant`, `outcome`
- `requestId` + stable `chatId` (session-level correlation key across multiple requests)
- endpoint (`/chat`, `/chat/completions`)
- latency (`durationMs`) and status
- selected provider/model and switch attempts
- per-attempt upstream retry diagnostics (`upstreamRetryTrace`) with status/error and backoff sleep timing
- requested tools + response tool-calls (when available)
- normalized parse helpers:
  - `responseToolCallsNormalized` + `responseToolCallNames`
  - `requestToolResultsSummary` (`total/success/failed/unknown/contractSchemaMismatch`)
- sanitized request/response payloads (redacts `apiKey`, `authorization`, `token`, ...)
- token-budget decision trace (`tokenBudget`) including thresholds, checks, and compaction actions

Effective upstream timeout for chat-completions path:

- `Q_ASSISTANT_TIMEOUT + (Q_ASSISTANT_UPSTREAM_RETRY_TIMEOUT_INCREMENT * Q_ASSISTANT_UPSTREAM_RETRY_ATTEMPTS)`

For full-fidelity audit (recommended for debugging tool chains), keep:
- `Q_ASSISTANT_CHAT_AUDIT_INCLUDE_PAYLOADS=true`
- `Q_ASSISTANT_CHAT_AUDIT_MAX_CHARS=0`
- `Q_ASSISTANT_CHAT_AUDIT_MAX_LIST_ITEMS=0`
- `Q_ASSISTANT_CHAT_AUDIT_MAX_STRING_CHARS=0`

Use `requestId` to correlate frontend/backend failures and decision-path issues.
Use `chatId` for thread/session correlation across an entire user chat.

With `examples/q-map/backends/docker-compose.yaml`, audit logs are host-readable via bind mount:
- host: `./logs/q-assistant`
- container: `/var/log/q-assistant`

For syslog/Loki/ELK-style ingestion, keep JSONL on disk and optionally set
`Q_ASSISTANT_CHAT_AUDIT_STDOUT_ENABLED=true` so container logging drivers can forward the same JSON events without adding provider-specific transport code in q-assistant.
