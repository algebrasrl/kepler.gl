# q-map Backends

This folder contains backend services used by `examples/q-map`.

## Services

- `kong` (default edge gateway, JWT/rate-limit) - port `8000`
- `q-assistant` (AI proxy, MCP helpers) - internal `3004` (`up-direct` publishes it)
- `q-cumber-backend` (read-only dataset/query backend; cloud maps intentionally limited) - internal `3001` (`up-direct` publishes it)
- `q-storage-backend` (per-user cloud maps) - internal `3005` (`up-direct` publishes it)

### Responsabilità per i dataset geospaziali

**`q-cumber-backend` + `q-cumber-postgis`** è l'unico riferimento per i dataset geospaziali
(CLC 2018, Kontur Boundaries, e qualsiasi nuovo dataset da aggiungere).
`q-cumber-postgis` è il container PostGIS dedicato, usato esclusivamente da `q-cumber-backend`
per rispondere alle query analitiche dell'AI.

**`q-storage-backend`** non gestisce geodati: salva e carica solo le configurazioni mappa
kepler.gl dell'utente (file JSON). Non ha PostGIS e non è coinvolto nelle analisi spaziali.

**`q-cumber-postgis/`** è il servizio PostGIS standalone. Contiene il Dockerfile, gli init
SQL e la directory `dumps/` con i pgdump per il bootstrap dei dati. All'avvio restora
automaticamente i dump presenti in `/dumps/`.

## Persistence on host

All backend persistent data is bind-mounted to local folders in this repo:
- `q-cumber-postgis` -> `examples/q-map/backends/q-cumber-postgis/data`
- `q-storage-backend` -> `examples/q-map/backends/q-storage-backend/data`

Active Python backends are also bind-mounted for live code editing (no image rebuild needed for source changes):
- `q-cumber-backend` -> `examples/q-map/backends/q-cumber-backend` mounted as `/app`
- `q-storage-backend` -> `examples/q-map/backends/q-storage-backend` mounted as `/app`
- `q-assistant` -> `examples/q-map/backends/q-assistant` mounted as `/app`

After code changes, use `docker compose restart <service>` (or `make up`) instead of `--build`, unless dependencies/Dockerfile changed.

Note: q-map frontend does not set provider/model. Provider/model selection and fallback are orchestrated by q-assistant (`Q_ASSISTANT_AGENT_CHAIN`).

Note: H3 runtime in the q-map frontend uses `h3-js` v4 (imported as `h3-js-v4`), with typings re-exported from `h3-js` in `examples/q-map/src/types/h3-js-v4.d.ts`. Only v4 API names are used.

## Start all default services

```bash
cd examples/q-map/backends
cp .env.example .env
make up
```

To prevent root-owned files on bind mounts (for example chat-audit logs), set in `.env`:

```bash
QMAP_DOCKER_UID=$(id -u)
QMAP_DOCKER_GID=$(id -g)
```

## Build and push images to a registry

The backend Makefile can build and push the production images directly to a
registry such as Quay.

Login once:

```bash
cd examples/q-map/backends
make registry-login
```

Preview the fully-qualified tags:

```bash
make image-refs IMAGE_TAG=2026.03
```

Build images:

```bash
make image-build IMAGE_TAG=2026.03
```

Build and push in one step:

```bash
make image-push IMAGE_TAG=2026.03
```

Current defaults target Quay under `quay.io/algebrasrl`.
Override `IMAGE_REGISTRY` / `IMAGE_NAMESPACE` only if you need a different registry path.

This covers:

- `q-cumber-postgis`
- `q-cumber-backend`
- `q-storage-backend`
- `q-assistant`

The resulting tags match the variables expected by `docker-stack.prod.yml` /
`.env.prod` (`QMAP_IMAGE_QCUMBER_POSTGIS`, `QMAP_IMAGE_QCUMBER_BACKEND`,
`QMAP_IMAGE_QSTORAGE_BACKEND`, `QMAP_IMAGE_QASSISTANT`).

## Makefile shortcuts

From `examples/q-map/backends`:

```bash
make help
make doctor
make up
make up-build
make up-direct
make up-direct-build
make qa-switch PROVIDER=openai MODEL=gpt-4o-mini
make qa-switch PROVIDER=ollama MODEL=qwen3-coder:30b BASE_URL=http://host.docker.internal:11434
make qa-switch-openai
make qa-switch-openrouter
make qa-switch-openrouter-deepseek
make qa-switch-openrouter-gemini
make qa-switch-ollama MODEL=qwen3-coder:30b
make test-backends
```

`make doctor` validates host `uid:gid` against `QMAP_DOCKER_UID/QMAP_DOCKER_GID` (from `.env` or defaults `1000:1000`) before startup commands.
If you need warning-only mode: `make doctor DOCTOR_STRICT=0`.

## Default Kong JWT gateway

Kong DB-less overlay is now the default runtime entrypoint and is available in:
- `docker-compose.edge-only.yaml`
- `docker-compose.kong.yaml`
- `kong/kong.yml`
- `kong/README.md`

Before startup, `make up` runs:

```bash
make render-kong-config
```

This renders `kong/kong.yml` from env (`QMAP_KONG_JWT_PRIMARY_*`, `QMAP_KONG_JWT_SECONDARY_*`, `QMAP_KONG_JWT_ALLOWED_AUDIENCES`) with:
- rotation-ready HS256 issuer/secret slots (primary + optional secondary),
- strict `iss` allowlist check,
- strict `aud` check (enabled by default).
- sandbox-safe pre-auth claim parsing in Kong `pre-function` (no untrusted Lua `require` dependency).
- optional backend claim enforcement on q-cumber/q-storage via `QCUMBER_JWT_*` and `QSTORAGE_JWT_*` envs (subject/roles policy).

Start backends + gateway:

```bash
make up
```

Gateway ports:
- `127.0.0.1:8000` (proxy)
- `127.0.0.1:8001` (admin; local-only binding)

In default mode (`make up`), both direct backend host ports (`3001/3004/3005`) and Kong gateway (`:8000`) are exposed.
Use `make up-direct` to start backends without Kong (direct ports only).

Gateway routes:
- `/api/q-assistant/*` -> `q-assistant:3004`
- `/api/q-cumber/*` -> `q-cumber-backend:3001`
- `/api/q-storage/*` -> `q-storage-backend:3005`

Recommended local domain mode (single coherent host):
- add `127.0.0.1 local.q-hive.it` to `/etc/hosts`
- use `http://local.q-hive.it:8000` as gateway base for all frontend API routes.

Frontend integration:

```bash
VITE_QMAP_AI_PROXY_BASE=http://localhost:8000/api/q-assistant
VITE_QCUMBER_CLOUD_API_BASE=http://localhost:8000/api/q-cumber
VITE_QSTORAGE_CLOUD_API_BASE=http://localhost:8000/api/q-storage
QMAP_AUTH_RUNTIME_TOKEN=<jwt>

# coherent single-host variant
VITE_QMAP_AI_PROXY_BASE=http://local.q-hive.it:8000/api/q-assistant
VITE_QCUMBER_CLOUD_API_BASE=http://local.q-hive.it:8000/api/q-cumber
VITE_QSTORAGE_CLOUD_API_BASE=http://local.q-hive.it:8000/api/q-storage
```

Token propagation fallback keys (if not using `QMAP_AUTH_RUNTIME_TOKEN`) are configured via:
- `VITE_QMAP_AUTH_TOKEN_STORAGE_KEYS`
- default lookup includes `qmap_gateway_jwt`, `qmap_auth_token`, `qmap_access_token`.
- frontend forwards only JWT-like bearer tokens by default (prevents Kong `Bad token; invalid JSON` from stale opaque tokens in storage).
- set `VITE_QMAP_AUTH_ALLOW_OPAQUE_BEARER=true` only if your edge expects opaque bearer tokens.

For local JWT quick-test and token minting, see `kong/README.md`.
Default `mint-dev-jwt.py` token TTL is 24h (`--ttl 86400`).

If you need to bypass gateway temporarily (debug only), use `make up-direct`.

## Same-origin local shell (intermediate setup before Hive embedding)

For local work on the future `q_hive`-style topology, q-map also ships an
intermediate same-origin shell in `examples/q-map/docker-compose.same-origin.yaml`.
It exposes one local entrypoint on `http://localhost:8080`:

- `/` -> `q-map-ui`
- `/api/q-assistant/*` -> Kong -> `q-assistant`
- `/api/q-cumber/*` -> Kong -> `q-cumber-backend`
- `/api/q-storage/*` -> Kong -> `q-storage-backend`

Start it from `examples/q-map`:

```bash
make same-origin-up
```

Useful commands:

```bash
make same-origin-ps
make same-origin-logs SERVICE=q-map-shell
make same-origin-down
```

Notes:

- this is an intermediate local shell, not the final Hive integration;
  in the final setup `q_hive` should own `/` and q-map will likely move
  behind a dedicated path such as `/map/`.
- local same-origin auth now reads only `QMAP_AUTH_RUNTIME_TOKEN`; rerun
  `make dev-local-prepare` if your `.env.development.local` still uses the
  removed `VITE_QMAP_AUTH_BEARER_TOKEN` key.
- the q-map UI image is built with relative API bases (`/api/q-assistant`,
  `/api/q-cumber`, `/api/q-storage`) so browser requests stay on the same
  origin and no direct backend port is needed from the frontend.

## Swarm production stack

Swarm-mode production recipe is provided in:
- `docker-stack.prod.yml`
- `.env.prod.example`

Typical flow:

```bash
cd examples/q-map/backends
cp .env.prod.example .env.prod
set -a
. ./.env.prod
set +a

# render Kong declarative config with production issuer/secret/audience vars
make render-kong-config

# build and push app images first (example tags)
docker build -t registry.example.com/qmap/q-cumber-postgis:2026.03 q-cumber-postgis
docker build -t registry.example.com/qmap/q-cumber-backend:2026.03 q-cumber-backend
docker build -t registry.example.com/qmap/q-storage-backend:2026.03 q-storage-backend
docker build -t registry.example.com/qmap/q-assistant:2026.03 q-assistant
docker push registry.example.com/qmap/q-cumber-postgis:2026.03
docker push registry.example.com/qmap/q-cumber-backend:2026.03
docker push registry.example.com/qmap/q-storage-backend:2026.03
docker push registry.example.com/qmap/q-assistant:2026.03

# initialize swarm once (if needed)
docker swarm init

# create required secrets (example)
printf "%s" "..." | docker secret create qmap_q_assistant_api_key -
printf "%s" "..." | docker secret create qmap_openrouter_api_key -
printf "%s" "..." | docker secret create qmap_openai_api_key -
printf "%s" "..." | docker secret create qmap_qcumber_backend_token -
printf "%s" "..." | docker secret create qmap_qstorage_api_token -

# deploy
docker stack deploy -c docker-stack.prod.yml --with-registry-auth qmap
```

Notes:
- In this stack only Kong is published (`:8000`); backend services stay internal on overlay network.
- `depends_on` is intentionally not relied upon (Swarm ignores startup ordering); readiness is handled through service healthchecks and restart policies.
- Stateful services use volumes; for multi-node clusters, use a shared/persistent volume driver and set placement constraints appropriately.
- CORS is centralized through one variable: `QMAP_UI_ORIGINS` (for all q-assistant/q-cumber/q-storage services).

## Quick provider/model switch

Use the helper script:

```bash
cd examples/q-map/backends
./switch-q-assistant.sh openrouter google/gemini-3-flash-preview
./switch-q-assistant.sh openrouter deepseek/deepseek-v3.2
./switch-q-assistant.sh openrouter nvidia/llama-3.1-nemotron-70b-instruct
./switch-q-assistant.sh openai gpt-4o-mini
./switch-q-assistant.sh ollama qwen3-coder:30b http://host.docker.internal:11434
./switch-q-assistant.sh openai gpt-4o-mini --build
```

What it does:

- updates `Q_ASSISTANT_PROVIDER`, `Q_ASSISTANT_MODEL`, `Q_ASSISTANT_BASE_URL` in `examples/q-map/backends/.env`
- clears `Q_ASSISTANT_AGENT_CHAIN` by default (to avoid silent chain override of provider/model)
- restarts `q-assistant` and prints `GET /health`
- provider credentials stay in `examples/q-map/backends/.env`: for `openrouter`, set `OPENROUTER_API_KEY` or `Q_ASSISTANT_API_KEY`, otherwise `q-assistant /chat/completions` and `make -C examples/q-map loop` fail with `401` even if `/health` is green
- caller bearer fallback is disabled by default; set `Q_ASSISTANT_ALLOW_CALLER_API_KEY_FALLBACK=true` only for explicit compatibility/debug scenarios

If you prefer manual profile blocks, `examples/q-map/backends/.env.providers.example` is still available.

## q-assistant reliability env (recommended)

Set in `examples/q-map/backends/.env`:

- `Q_ASSISTANT_TIMEOUT=45`
- `Q_ASSISTANT_UPSTREAM_RETRY_ATTEMPTS=2`
- `Q_ASSISTANT_UPSTREAM_RETRY_BASE_DELAY=1.0`
- `Q_ASSISTANT_UPSTREAM_RETRY_MAX_DELAY=8`
- `Q_ASSISTANT_UPSTREAM_RETRY_JITTER_RATIO=0.2`
- `Q_ASSISTANT_UPSTREAM_RETRY_TIMEOUT_INCREMENT=5`

Audit traces in `examples/q-map/backends/logs/q-assistant/chat-audit/*.jsonl` include `upstreamRetryTrace` for per-attempt diagnostics.
Retention defaults are enabled in q-assistant:

- `Q_ASSISTANT_CHAT_AUDIT_MAX_FILES=500`
- `Q_ASSISTANT_CHAT_AUDIT_MAX_AGE_DAYS=30`

## Stop

```bash
docker compose down
```
