# Q Map (q-hive + Vite)

This example is cloned from `examples/get-started-vite` and uses the q-hive branded map experience on top of `@kepler.gl/*` packages with Vite as the build tool.

## Branding

- q-map UI-facing labels and assistant identity are branded as `q-hive`.
- Technical compatibility identifiers remain unchanged where required (for example package imports under `@kepler.gl/*` and map payload format `keplergl`).

## Development

Node version used in this project: `Node 20`.

Before each commit, summarize pending q-map changes in `examples/q-map/CHANGELOG.md` (`Unreleased` section).

### Installation

```bash
# Install dependencies
yarn install
```

### Development Server

To start the development server with hot module replacement:

```bash
yarn dev
```

One-command local bootstrap (backend Kong + dev JWT + frontend env sync):

```bash
make dev-local
```

`dev-local` mints a development JWT with default duration 24 hours and writes it to
`QMAP_AUTH_RUNTIME_TOKEN` in `.env.development.local`.
If needed, you can mint a custom token manually:

```bash
cd backends
python3 kong/scripts/mint-dev-jwt.py --ttl 86400
```

`dev-local` uses local Docker Compose (`backends/docker-compose.yaml` + `backends/docker-compose.kong.yaml`), not Swarm stack deploy.
For local q-map Playwright runs and KPI loops, treat `make dev-local` / `make dev-local-prepare`
as the required bootstrap. The root-platform `make platform-up` stack is not a substitute for
the q-map dev server on `http://localhost:8081` and can leave the UI without the runtime JWT
expected by `tests/e2e/tools.spec.ts`, `make test-e2e-tools`, and local `make loop` runs.

Useful variants:

```bash
# prepare backend + token + .env.development.local only
make dev-local-prepare

# use coherent single-host domain mode
make dev-local DEV_LOCAL_DOMAIN=local.q-hive.it
```

### E2E Test Suites (Playwright)

This project uses two functional E2E suites:

- UX/functional suite (`playwright.ux.config.ts`):
  - `tests/e2e/smoke.spec.ts`
  - `tests/e2e/ux.spec.ts`
  - `tests/e2e/ux-regression.spec.ts`
  - `tests/e2e/tools.spec.ts`
- Assistant interaction suite (`playwright.assistant.config.ts`):
  - `tests/e2e/ai-mode-policy.spec.ts`

Run tests with Yarn:

```bash
# run full e2e suite
yarn test:e2e

# run UX/functional suite
yarn test:e2e:ux

# run UX/functional suite in Playwright UI
yarn test:e2e:ux:ui

# run assistant interaction suite
yarn test:e2e:assistant

# run assistant interaction suite in Playwright UI
yarn test:e2e:assistant:ui

# open Playwright UI for all specs
yarn test:e2e:ui

# run worker unit tests for heavy geometry/H3 operations
yarn test:unit:workers

# optional direct single-spec debug
yarn test:e2e tests/e2e/ai-mode-policy.spec.ts
```

Before running local Playwright suites against `http://127.0.0.1:8081`, bootstrap q-map auth with
`make dev-local` (or `make dev-local-prepare` if you will start Vite manually). If you only started
the root platform stack with `make platform-up`, browser-driven q-cumber/q-storage calls can fail
with `Missing bearer token` even while the full Hive login flow works elsewhere.

Audit/KPI inventory for future pre-release assessment lives in `docs/AUDIT_FILE_INVENTORY.md`.
`docs/KPI_WEEKLY_SUMMARY.md` is treated as a local generated artifact from
`make kpi-weekly-summary` / `make loop`, not as stable versioned documentation.

Current UX/functional coverage includes:

- app boot + map render
- Add Data modal tabs
- Cloud storage providers visibility
- loading cloud map `Stressor IT`
- layer style controls availability
- layer visibility toggle behavior
- dataset table modal opening and column presence
- layer reorder by drag-and-drop
- q-storage persistence load from cloud UI
- cloud error handling (404/422/timeout) notifications
- dataset filter creation and categorical value selection
- quantitative styling controls for numeric geojson fields
- large dataset upload responsiveness guardrail
- mobile/responsive modal + side panel usability
- duplicate dataset-name stability (no crash, dataset still usable)
- basemap viewport interaction validated through pan + zoom-to-layer
- direct q-map tool runner functional workflows (`tools.spec.ts`)

### Backend Tests (Python `unittest`)

Backend test suites live under:

- `examples/q-map/backends/q-cumber-backend/tests`
- `examples/q-map/backends/q-assistant/tests`
- `examples/q-map/backends/q-storage-backend/tests`

Run locally:

```bash
cd examples/q-map/backends/q-cumber-backend
python -m unittest -q \
  tests/test_provider_registry_and_storage.py \
  tests/test_dataset_adapters.py \
  tests/test_api_routing_metadata.py

cd ../q-assistant
python -m unittest -q \
  tests/test_explicit_tool_routing.py \
  tests/test_agent_skip_policy.py \
  tests/test_chat_audit_utils.py \
  tests/test_request_coercion.py \
  tests/test_chat_payload_compaction.py \
  tests/test_token_budget_compaction.py \
  tests/test_objective_anchor.py \
  tests/test_openai_stream_normalization.py \
  tests/test_openrouter_provider.py

cd ../q-storage-backend
python -m unittest -q tests/test_config_and_storage.py
```

Run inside backend containers (after rebuild):

```bash
cd examples/q-map/backends
docker compose up -d --build q-cumber-backend q-assistant q-storage-backend

docker exec q-map-q-cumber-backend python -m unittest -q \
  tests/test_provider_registry_and_storage.py \
  tests/test_dataset_adapters.py \
  tests/test_api_routing_metadata.py

docker exec q-map-q-assistant python -m unittest -q \
  tests/test_explicit_tool_routing.py \
  tests/test_agent_skip_policy.py \
  tests/test_chat_audit_utils.py \
  tests/test_request_coercion.py \
  tests/test_chat_payload_compaction.py \
  tests/test_token_budget_compaction.py \
  tests/test_objective_anchor.py \
  tests/test_openai_stream_normalization.py \
  tests/test_openrouter_provider.py

docker exec q-map-q-storage-backend python -m unittest -q tests/test_config_and_storage.py
```

Notes:

- `test_api_routing_metadata.py` verifies that q-cumber dataset catalog/help APIs expose backend `routing` metadata used by q-map tool selection.
- `q-cumber-backend` container installs dev extras (`pip install -e ".[dev]"`) so test dependencies are available after compose build.

### Backend Stack (Docker Compose)

All backend services are grouped in `examples/q-map/backends`.

```bash
cd examples/q-map/backends
cp .env.example .env
docker compose up -d --build
```

This starts:

- `q-cumber-backend` on `http://localhost:3001`
- `q-storage-backend` on `http://localhost:3005`
- `q-assistant` on `http://localhost:3004`
- `kong` on `http://localhost:8000`
- persistent backend data on host:
  - `examples/q-map/backends/q-cumber-postgis/data` (PostGIS)
  - `examples/q-map/backends/q-storage-backend/data` (q-storage maps)

Dev note: active Python backends (`q-assistant`, `q-cumber-backend`, `q-storage-backend`) are bind-mounted as `/app`, so source changes do not require image rebuild. Use `docker compose restart <service>` unless dependencies or Dockerfiles changed.

Image build/push note:

- `examples/q-map` keeps low-level image helpers for local/manual work.
- Platform release tagging and Quay publication for the complete `q_hive + q-map` stack are orchestrated only from the root [Makefile](/media/p/DATA/git-repos/hive/refactoring/q_hive/Makefile).

Quick q-assistant provider/model switch:

```bash
cd examples/q-map/backends
./switch-q-assistant.sh openrouter google/gemini-3-flash-preview
# equivalent Makefile shortcut
make qa-switch-openrouter
```

### UI + Backend via Docker (Prod-like)

You can run q-map UI as a container (Nginx serving Vite build) together with backends:

```bash
cd examples/q-map/backends
cp .env.example .env
docker compose -f docker-compose.yaml -f docker-compose.ui.yaml --profile ui up -d --build
```

For the future single-origin topology used by `q_hive`, the preferred local entrypoint is:

```bash
cd examples/q-map
make same-origin-up
```

This starts the same-origin shell on:

```bash
http://localhost:8080
```

Notes:

- `q-map-ui` is built from the full `kepler.gl` repo context, not only from `examples/q-map`.
- the UI image installs both repo-root `kepler.gl` dependencies and `examples/q-map` dependencies, matching the way local Vite dev/build resolves aliased `@kepler.gl/*` source packages plus q-map-specific frontend packages.
- same-origin auth now reads only `QMAP_AUTH_RUNTIME_TOKEN`; if your local `.env.development.local` still has the old `VITE_QMAP_AUTH_BEARER_TOKEN`, rerun `make dev-local-prepare` or rename the variable before `make same-origin-up`.
- the local health probe for the shell is `http://localhost:8080/healthz`.
- when embedded by `q_hive`, `q-map` now also supports a session-backed auth bootstrap flow:
  - parent `q_hive` fetches `/api/qmap/bootstrap/` with the current Django session
  - child `q-map` requests auth bootstrap from the parent via `postMessage`
  - parent responds with a short-lived JWT kept only in memory by `q-map`

### Iframe Contract With q_hive

When `q-map` is embedded in `q_hive`, the parent/child contract is:

- `q_hive` provides the iframe URL via `QH_MAP_IFRAME_URL`
- `q_hive` appends hash params:
  - `action_uuid`
  - `cloud_map_id`
  - `cloud_provider`
  - `export_payload` (`subset`, `perimeter`, `full`)
- in `geotoken` iframe sessions, q-map now auto-arms polygon draw and auto-generates the resolution-11 tessellation from editor polygons
- geotoken auto-tessellation is blocked when the union area of the drawn polygons exceeds `100 km2`; q-map removes any stale auto-generated tessellation dataset and shows a notification instead of running H3 generation
- `q-map` posts back only these versioned message envelopes:
  - `QMAP_IFRAME_EXPORT`
  - `QMAP_IFRAME_CLOUD_EXPORT`
  - `QMAP_IFRAME_CANCEL`
- every outbound message uses:
  - `source: "q-map"`
  - `version: 1`
  - `payload: {...}`
- `QMAP_IFRAME_CLOUD_EXPORT` now supports reduced payload mode:
  - `subset` (default): sends cloud reference + metadata, omits full `map`
  - `perimeter`: like `subset` plus `perimeterFeatureCollection` extracted from editor polygons
  - `full`: sends full `map` snapshot
- when cloud export is used (`QMAP_IFRAME_CLOUD_EXPORT`), payload includes:
  - `cloudMap.id`
  - `cloudMap.provider`
  - `mapInfo` (map snapshot metadata from `map.info`; typically title and optional version/hash/updatedAt fields when present)
  - `actionUuid` (from hash `action_uuid`, fallback to message instance id)
- target origin is resolved in this order:
  - `VITE_QMAP_IFRAME_EXPORT_TARGET_ORIGIN`
  - `document.referrer` origin
  - current `window.location.origin`

Current failure modes on the q-map side are explicit and fail closed:

- `missing_map`
- `missing_perimeter`
- `not_in_iframe`
- `missing_parent_window`
- `missing_target_origin`

This matches the current Hive listener, which already checks:

- `event.source === iframe.contentWindow`
- `event.origin === iframe origin derived from QH_MAP_IFRAME_URL`
- accepted message types only from `source === "q-map"`

For auth bootstrap, the parent/child contract is:

- `q-map` -> parent:
  - `type: "QMAP_AUTH_BOOTSTRAP_REQUEST"`
  - `source: "q-map"`
  - `version: 1`
- parent -> `q-map`:
  - `type: "QH_QMAP_AUTH_BOOTSTRAP"`
  - `source: "q-hive"`
  - `version: 1`
  - `payload.accessToken`

### Action-Locked Maps (q-storage + UI)

When q-map saves from iframe action sessions (`action_uuid` present), it attaches save metadata:

- `locked: true`
- `lockType: "action"`
- `actionUuid: <hash action_uuid>`
- `lockSource: "q_hive"`

q-storage policy for those maps:

- delete is always blocked (`403`)
- update is blocked (`403`) unless JWT includes `qh_action_map_write=true`
- lock metadata is immutable after first save

UI effects:

- `GET /maps` entries include `readOnly=true` for action-locked maps
- standalone q-map load path forces `uiState.readOnly=true` for action-locked maps
- regular maps (without action lock metadata) are still editable/deletable

### Build and Push `q-map-ui` Image

The q-map UI image is built from `examples/q-map/Dockerfile.ui` using the full `kepler.gl` repo as Docker build context, so local changes under `examples/q-map` and elsewhere in the repo are included in the container image exactly like local Vite/dev work.

Default image target:

```bash
quay.io/algebrasrl/q-map-ui:latest
```

Commands:

```bash
cd examples/q-map
make registry-login
make ui-image-refs IMAGE_TAG=2026.03
make ui-image-push IMAGE_TAG=2026.03
```

Override `IMAGE_REGISTRY`, `IMAGE_NAMESPACE`, or `IMAGE_TAG` if needed.

Then open:

- `http://localhost:8081`

Optional `q-assistant` retry env (for transient upstream AI errors):

- `Q_ASSISTANT_UPSTREAM_RETRY_ATTEMPTS` (default `2`, i.e. 3 total attempts)
- `Q_ASSISTANT_UPSTREAM_RETRY_BASE_DELAY` (default `1.0` seconds)
- `Q_ASSISTANT_UPSTREAM_RETRY_MAX_DELAY` (default `8` seconds)
- `Q_ASSISTANT_UPSTREAM_RETRY_JITTER_RATIO` (default `0.2`, randomized backoff jitter)
- `Q_ASSISTANT_UPSTREAM_RETRY_TIMEOUT_INCREMENT` (default `5` seconds per retry attempt)

Effective upstream request timeout in q-assistant is:

- `Q_ASSISTANT_TIMEOUT + (Q_ASSISTANT_UPSTREAM_RETRY_TIMEOUT_INCREMENT * Q_ASSISTANT_UPSTREAM_RETRY_ATTEMPTS)`

### AI Assistant Notes

- q-map frontend AI runtime uses q-assistant OpenAI-compatible endpoint (`POST /chat/completions`).
- Provider/model fallback order is backend-owned (`Q_ASSISTANT_AGENT_CHAIN` in `examples/q-map/backends/.env`); the frontend does not set provider/model and always defers orchestration to q-assistant.
- q-assistant audit logs include `upstreamRetryTrace` (per-attempt status/error/backoff timing) for precise incident analysis.
- q-assistant injects a compact runtime objective anchor (`[OBJECTIVE_ANCHOR]` + `[OBJECTIVE_CRITERIA]`) before token-budget compaction so active user intent remains explicit in long tool chains.
- q-assistant injects runtime guardrail hints (`[RUNTIME_GUARDRAIL]` / `[RUNTIME_NEXT_STEP]`) based on recent tool results to enforce coherent sequencing (`waitForQMapDataset` -> `countQMapRows`) and avoid repeated low-distinct color retries on the same field.
- for centering/zoom objectives, q-assistant guardrails prevent false centering claims when `fitQMapToDataset` failed (or no successful fit evidence exists): the agent must retry fit deterministically or report explicit limitation.
- when turn-state discovery gate failures repeat (`Hard-enforce turn state: discovery step is mandatory`), q-assistant loop-limits prune failing operational retries and force recovery through `listQMapDatasets`.

### AI Governance Baseline

- Governance baseline docs live in `docs/ai-governance/`:
  - `policy.md`
  - `risk-register.yaml`
  - `control-matrix.md`
  - `incident-runbook.md`
- Run the governance audit gate:

```bash
make ai-governance-audit
```
- q-map frontend final messages include `[executionSummary] {...}` (machine-readable per-turn outcome) and apply a fail-closed centering check: unverified centering claims are stripped and replaced by `[guardrail] centering_claim_blocked ...`.
- q-assistant guardrails also recover ranking failures caused by missing metric fields (`metric field not found`): inspect real dataset fields first, then retry ranking with an existing numeric metric.
- for ranking/superlative objectives, q-assistant guardrails require ordered ranking evidence (`rankQMapDatasetRows`) in final output, not only generic summary text.
- after unresolved `zonalStatsByAdmin` UI-freeze failures in forest-value workflows, q-assistant guardrails block population/name fallback coloring and require explicit limitation text instead of inferred ranking claims.
- when the ranking metric is flat (`distinct=1`), guardrails require explicit tie disclosure instead of unique top/bottom claims.
- for ranking objectives, a category chart on `name` without metric axis is treated as non-evidence and cannot replace ordered metric output.
- for "problemi/pressione ambientale" objectives, guardrails block silent fallback to population/name metrics unless explicitly requested.
- q-assistant runtime guardrails prefer stable dataset references (`datasetRef`, format `id:<datasetId>`) when available; hints still fall back to exact `datasetName`.
- q-assistant deduplicates repeated successful discovery turns (`listQMapDatasets`, `listQCumberProviders`, `listQCumberDatasets`, `getQCumberDatasetHelp`, schema discovery tools) before upstream chat-completions calls to reduce prompt size/cost and planning loops.
- q-map AI tessellation can materialize an intermediate filtered dataset before H3 generation.
- The intermediate dataset is technical and is added with `autoCreateLayers: false`, so only `Tassellation` is rendered by default.
- q-map AI should use q-cumber query tools for dynamic datasets (`listQCumberProviders` -> `listQCumberDatasets(providerId)` -> `getQCumberDatasetHelp(providerId,datasetId)` -> query tool selected by `routing.queryToolHint.preferredTool`); explicit `providerId` is recommended and effectively required when catalog has multiple providers (auto-select remains only for uniquely resolvable catalogs).
- If `providerId` is explicitly provided and invalid/unavailable, q-map must fail fast with a clear error (including available provider ids) and must not silently fallback to another provider.
- `queryQCumber*` tools accept only exact `datasetId` values from `listQCumberDatasets(providerId)`; invalid explicit ids fail fast with available catalog ids. When omitted, q-map attempts deterministic auto-selection from provider catalog routing metadata (fails fast if no unique candidate).
- `queryQCumberTerritorialUnits` is strict for administrative datasets only; on non-administrative datasets it returns a structured retry hint (`retryWithTool`/`retryWithArgs`) instead of silently downgrading behavior.
- q-map runtime executes at most one automatic retry using backend routing hints when a strict tool returns a retry directive; retry trace is attached in tool details/audit as `autoRetry`.
- `expectedAdminType` is for administrative queries only; it must not be sent with `queryQCumberDatasetSpatial`/thematic datasets (e.g. CLC land cover), otherwise invalid `lv` filters may be injected.
- `tassellateDatasetLayer` now guards against large thematic land-cover datasets (CLC/corine): it returns a structured retry to `aggregateDatasetToH3` (class-aware) to avoid H3 timeout/freeze paths.
- if a query uses generic `parent_id` and backend rejects that field, q-map runtime now auto-retries once by rewriting to backend parent-id candidates (from `routing/aiHints.aiProfile`) before surfacing an error.
- For administrative child queries (e.g. municipalities of a region/province), parent filters must be resolved from dataset metadata/routing candidates (for example `kontur_boundaries__lv4_id`), never hardcoded to `parent_id`.
- For named administrative places with ambiguous levels (e.g. same name at province+municipality level), q-map runtime now fails fast and asks for disambiguation via `expectedAdminType` (or explicit `lv`) before loading.
- `expectedAdminType` validation is strict when explicitly set and enforces the canonical level intent (`province -> lv=7`, `municipality -> lv=9`, etc.) without downgrading to sampled levels.
- q-cumber query tools are full-schema by design: `select` is not exposed in q-map runtime; backend responses keep all fields and any field filtering must happen client-side.
- For `queryQCumberDataset*` with `loadToMap=true`, q-map now auto-pages backend windows when `totalMatched > returned` (including over the 100k per-request cap) and materializes a full dataset in map state when possible.
- For thematic spatial queries with auto-injected `spatialBbox` (for example local-assets-it Italy default bbox), if the initial `loadToMap=true` query returns `totalMatched=0` and there are no user filters, q-map retries once without `spatialBbox` before finalizing an empty result.
- Ranking guardrails are enforced in q-map runtime: geometry/identifier fields (for example `_geojson`, `geom`, `gid`, `*_id`) are excluded as ranking metrics unless explicitly requested.
- Ranking fallback uses backend/frontend metadata candidates (`routing.orderByCandidates`, `aiHints.orderByCandidates`) and prefers meaningful numeric metrics (for example `population`) over identifiers.
- `zonalStatsByAdmin` is strict on canonical args: use `{adminDatasetName, valueDatasetName, valueField?, aggregation?, weightMode?, outputFieldName?, showOnMap?, newDatasetName?}` and do not use non-canonical keys like `targetDatasetName`, `adminNameField`, `targetValueFieldName`, `operations`.
- Off-map ranking (`loadToMap=false`) is only preferred for list/text answers; when map rendering or map transforms are needed, queries should run with `loadToMap=true`.
- dataset chaining should use `listQMapDatasets` output and prefer `datasetRef` (`id:<datasetId>`) over plain names to avoid ambiguities.
- q-map frontend runtime hard-enforces turn phases (`discover -> execute -> validate -> finalize`): each turn now captures a dataset snapshot pre-flight (equivalent to discovery) and stale snapshots are revalidated via `VITE_QMAP_AI_TURN_SNAPSHOT_TTL_MS` (default `180000` ms); `listQMapDatasets` remains the explicit refresh tool.
- q-cumber frontend requests use a timeout (`VITE_QCUMBER_BACKEND_TIMEOUT_MS`, default `45000` ms) to avoid indefinite pending states.
- icon-layer remote SVG icon fetch is disabled by default in q-map to avoid intermittent browser CORS noise from third-party CDN (`VITE_QMAP_DISABLE_REMOTE_SVG_ICONS=true`; set `false` to re-enable remote fetch).
- q-cumber map-loading is geometry-first by default: automatic point creation from lat/lon is disabled unless `inferPointsFromLatLon=true` (or env `VITE_QMAP_AI_QUERY_INCLUDE_LATLON_FALLBACK=true`).
- q-cumber cloud maps are intentionally limited (currently stressor only); do not rely on q-cumber cloud maps for Kontur boundaries.
- q-map runtime prompt also includes a live snapshot of loaded datasets/layers (exact dataset names, field names, and inferred field-role hints) to reduce invalid tool arguments.
- q-map runtime exposes tool-category introspection (`listQMapToolCategories`, `listQMapToolsByCategory`) so the assistant can narrow tool routing by functional class before execution.
- q-map runtime always exposes custom chart tools (`categoryBarsTool`, `wordCloudTool`) and, when present in base runtime, also exposes ECharts tools (`histogramTool`, `boxplotTool`, `bubbleChartTool`, `pcpTool`, `lineChartTool`, `scatterplotTool`) with policy gating (`safe`/`full`/`timeseries-safe`).
- In `safe` chart mode, line/scatter remain disabled by default; per-tool env toggles can override policy (`VITE_QMAP_AI_ENABLE_<TOOL_NAME>=true|false`).
- q-cumber dataset APIs now expose `aiHints` and `routing` (including `queryToolHint.preferredTool`) to help the assistant choose correct admin/H3 operations.
- for adding new territorial/thematic providers with metadata-driven routing (`ai.profile`, `fieldHints`, tests), follow `examples/q-map/backends/q-cumber-backend/PROVIDER_ONBOARDING.md`.
- for ambiguous administrative names (e.g. `Brescia`), assistant flow should disambiguate by level first and query with `name + lv` before `loadToMap`.
- q-map custom runtime disables generic SQL/spatial base tools (`genericQuery`, `filterDataset`, `spatialFilterTool`, `spatialJoinTool`, `dissolveTool`) and uses q-map specific tools instead.
- q-map custom runtime also excludes `tableTool` and `mergeTablesTool`; q-assistant backend enforces the same exclusion defensively before upstream tool-schema forwarding.
- Rationale for disabled/pruned tools: generic SQL/table/spatial legacy tools can bypass deterministic geospatial guardrails (admin-level validation, H3 coverage checks, dataset lineage, and bounded retries), causing non-reproducible or misleading analytical outputs.
- q-assistant detects repeated discovery-only loops (`listQCumberProviders`/`listQCumberDatasets` without progress) and prunes redundant discovery tool ads in subsequent turns to force concrete query progression.
- q-map AI includes style preset `comuni_population` (tool: `applyQMapStylePreset`) for readable municipal population choropleths.
- q-map AI includes `populateTassellationFromAdminUnits` to aggregate administrative values (e.g. population) to a tessellation resolution before H3 join; join coverage guardrails are adaptive (population-like fields use stricter minimum coverage).
- q-map AI includes `clipQMapDatasetByGeometry` for clipping/masking with predicates Turf (`intersects` / `within` / centroid-in-polygon); source datasets can be GeoJSON or H3 (`h3_id`/`h3__id`).
- Clip outputs can include per-row diagnostics for downstream analysis: `qmap_clip_match_count`, `qmap_clip_intersection_area_m2`, `qmap_clip_intersection_pct`.
- Clip outputs can also include clip-property distinct counters as `<clip_field>__count` (count of distinct matched values per output row).
- With `includeDistinctPropertyValueCounts=true`, clip outputs can also include per-value counters as `<clip_field>__<value>__count` (count of matched clip-side rows for each value).
- In thematic H3 joins (`joinQMapDatasetsOnH3`), numeric right fields use the selected metric, while categorical/theme fields (e.g. CLC labels/codes) are preserved with categorical-safe output instead of numeric averaging.
- For tessellation + thematic overlays (for example CLC on H3), prefer `aggregateDatasetToH3` + `joinQMapDatasetsOnH3` (or `populateTassellationFromAdminUnits*` for admin values) instead of clipping full thematic polygons against H3 cells.
- For named-boundary thematic overlays (e.g. Veneto forests), enforce exact boundary output with sequence: boundary tessellation (left) -> thematic H3 aggregate (right) -> `joinQMapDatasetsOnH3` (left join) -> final `clipQMapDatasetByGeometry` of the joined output by boundary polygon.
- q-map AI geometry-analysis toolbox also includes: `spatialJoinByPredicate`, `zonalStatsByAdmin`, `clipDatasetByBoundary`, `overlayDifference`, `bufferAndSummarize`, `nearestFeatureJoin`, `adjacencyGraphFromPolygons`, `coverageQualityReport`.
- Geometry-analysis tools support GeoJSON by default and now accept H3 datasets (`h3_id`/`h3__id`) where applicable (spatial join, zonal stats, overlay, buffer/nearest, coverage, adjacency).
- q-map AI includes `reprojectQMapDatasetCrs` for coordinate reference system transformation (`sourceCrs -> targetCrs`) on GeoJSON and/or lat/lon fields.
  - Implementation uses Web Worker for heavier workloads with automatic local fallback.
- q-map AI executes `aggregateDatasetToH3` and `populateTassellationFromAdminUnits*` in Web Worker for larger inputs, with deterministic local fallback if worker execution is unavailable or fails.
- q-map AI now auto-hides source layers for derived workflows (`aggregateDatasetToH3`, `joinQMapDatasetsOnH3`, `clipQMapDatasetByGeometry`, `populateTassellationFromAdminUnits*`) when the output is shown, to reduce map clutter/freeze risk (`VITE_QMAP_AI_AUTO_HIDE_SOURCE_LAYERS=false` to disable).
- q-cumber thematic geometry queries loaded to map disable auto-layer creation for very large rowsets (default threshold `15000`, env override `VITE_QMAP_AI_QUERY_MAX_AUTO_LAYER_GEOMETRY_ROWS`) so large sources like national CLC stay technical/off-layer by default.
- `aggregateDatasetToH3` uses a higher default feature cap (`100000`, env `VITE_QMAP_AI_H3_AGGREGATE_MAX_FEATURES`) to avoid partial national coverage when aggregating large thematic datasets (e.g. CLC forests).
- Long-running geospatial operations are resilient to assistant panel rerenders/unmounts: `aggregateDatasetToH3`, `populateTassellationFromAdminUnits*`, `clipQMapDatasetByGeometry`, `spatialJoinByPredicate`, `zonalStatsByAdmin`, `overlayDifference`, `bufferAndSummarize`, `nearestFeatureJoin`, `adjacencyGraphFromPolygons` use guarded async execution and cooperative yielding to reduce UI freeze risk.
- For async dataset-producing operations, keep the chain `waitForQMapDataset -> countQMapRows` before declaring success (prevents false positives on partially completed flows).

### Hash Preset URLs (Client-only)

`q-map` includes a hash preset parser in `src/utils/hash-preset.ts`.

Current behavior:

- Reads `preset` from URL hash (`base64url`-encoded JSON)
- Logs received/decoded payload in browser console
- Applies a safe UI whitelist on bootstrap and on `hashchange`:
  - `uiState.qmapMode` (via `setQMapMode` + existing q-map mode pipeline)
  - `uiState.activeSidePanel` (supports `null` to keep side panel closed)
  - `uiState.readOnly`
  - `uiState.locale`
  - `uiState.mapControls` (`show`/`active` only on known Kepler map controls)
- Applies map launch parameters on bootstrap and on `hashchange`:
  - viewport: `lat`/`lon`/`zoom`/`bearing`/`pitch` (or from preset `state.mapState`)
  - basemap: `basemap` (or `style`/`styleType`) or preset `state.mapStyle.styleType`

Supported hash forms:

- `#preset=<base64url>`
- `#/map?preset=<base64url>` (for hash-router style URLs)
- `#mode=kepler|draw-stressor|draw-on-map` (short mode-only form)
- `#/map?mode=kepler|draw-stressor|draw-on-map`
- short map params (with or without `preset`): `lat`, `lon`/`lng`, `zoom`/`z`, `bearing`/`brg`, `pitch`, `basemap`/`style`/`styleType`

Precedence (when keys collide):

1. Short hash params (`mode`, `lat`, `lon`, `zoom`, `bearing`, `pitch`, `basemap`)
2. Decoded `preset` JSON
3. q-map defaults / env defaults
4. mode fallback defaults (for example `draw-stressor` default basemap `satellite` only when no explicit basemap is provided)

Quick short-params example:

- `#/map?mode=draw-on-map&lat=45.46&lon=9.19&zoom=11&bearing=20&pitch=30&basemap=satellite`

Recommended payload shape:

```json
{
  "v": 1,
  "state": {
    "uiState": {
      "qmapMode": "kepler",
      "activeSidePanel": null
    },
    "mapState": {
      "latitude": 41.9,
      "longitude": 12.49,
      "zoom": 7,
      "bearing": 0,
      "pitch": 0
    },
    "mapStyle": {
      "styleType": "muted"
    }
  }
}
```

Useful presets (payload JSON):

- Kepler mode, all side panels closed:

```json
{
  "v": 1,
  "state": {
    "uiState": {
      "qmapMode": "kepler",
      "activeSidePanel": null
    }
  }
}
```

- Draw-stressor mode:

```json
{
  "v": 1,
  "state": {
    "uiState": {
      "qmapMode": "draw-stressor",
      "activeSidePanel": "layer"
    }
  }
}
```

- Read-only viewer mode:

```json
{
  "v": 1,
  "state": {
    "uiState": {
      "qmapMode": "kepler",
      "readOnly": true,
      "activeSidePanel": null
    }
  }
}
```

Generate URL programmatically (Node.js/backend):

```js
function toBase64UrlJson(value) {
  const json = JSON.stringify(value);
  return Buffer.from(json, 'utf8')
    .toString('base64')
    .replace(/\+/g, '-')
    .replace(/\//g, '_')
    .replace(/=+$/g, '');
}

function buildQMapPresetUrl(appBaseUrl, preset, useHashRoute = false) {
  const encoded = toBase64UrlJson(preset);
  const hash = useHashRoute ? `#/map?preset=${encoded}` : `#preset=${encoded}`;
  return `${String(appBaseUrl || '').replace(/\/+$/, '')}/${hash}`;
}

const preset = {v: 1, state: {uiState: {qmapMode: 'kepler', activeSidePanel: null}}};
const url = buildQMapPresetUrl('https://maps.example.com/q-map', preset, true);
```

Generate URL programmatically (Python/backend):

```python
import base64
import json

def to_base64url_json(value: dict) -> str:
    raw = json.dumps(value, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")

def build_qmap_preset_url(app_base_url: str, preset: dict, use_hash_route: bool = False) -> str:
    encoded = to_base64url_json(preset)
    hash_part = f"#/map?preset={encoded}" if use_hash_route else f"#preset={encoded}"
    return app_base_url.rstrip("/") + "/" + hash_part

preset = {"v": 1, "state": {"uiState": {"qmapMode": "draw-stressor", "activeSidePanel": "layer"}}}
url = build_qmap_preset_url("https://maps.example.com/q-map", preset, use_hash_route=True)
```

### Prompt-Driven Improvement Loop

- q-map now includes a prompt-driven continuous improvement runbook:
  - `SYSTEM_ENGINEERING_LOOP.md`
  - `tests/ai-eval/architecture-matrix.json`
- Convenience helpers are available in `Makefile`:
  - `make clean-loop`
  - `make ai-eval`
  - `make ai-eval-functional`
  - `make ai-matrix-audit`
  - `make ai-eval-all`
  - `make tool-coverage-audit`
  - `make quality-gate`
  - `make loop`
- Recommended start-clean sequence (from `examples/q-map`):
  - `make clean-loop`
  - `make loop RUN_ID=<baseline-tag>`
- Functional prompt catalog:
  - `tests/ai-eval/cases.functional.json`
- Fast eval commands (from `examples/q-map`):
  - `yarn ai:eval`
  - `yarn ai:eval:functional`
- Rule of thumb: every reproducible functional failure should be added as a new case in `cases.functional.json` before shipping the fix.

### Optional: Cloud Storage Providers

Configure cloud providers in `.env.development.local` (bootstrap from `.env.development.example`):

```bash
# Registered provider shown as "Q-cumber"
VITE_QCUMBER_CLOUD_API_BASE=http://localhost:8000/api/q-cumber
VITE_QCUMBER_CLOUD_DISPLAY_NAME=Q-cumber
VITE_QCUMBER_CLOUD_MANAGEMENT_URL=http://localhost:3001/maps
VITE_QCUMBER_CLOUD_TOKEN=

# Registered provider shown as "My Maps" (EN) / "Le mie mappe" (IT)
VITE_QSTORAGE_CLOUD_API_BASE=http://localhost:8000/api/q-storage
VITE_QSTORAGE_CLOUD_DISPLAY_NAME=Le mie mappe
VITE_QSTORAGE_CLOUD_MANAGEMENT_URL=http://localhost:3005/maps
VITE_QSTORAGE_CLOUD_TOKEN=
```

When cloud provider env vars are set, q-hive enables cloud integration through these endpoints:

- `GET /me` returns current user
- `GET /maps` returns map list
- `GET /maps/:id` downloads map payload
- `POST /maps` and `PUT /maps/:id` are required only for providers that support map persistence (e.g. `q-storage-backend`)

Provider behavior in this repo:

- `q-cumber-backend`: read-only dataset/query backend (cloud map exposure intentionally limited)
  - dynamic query filters also support `startswith`/`endswith` for name-based lookup workflows
  - dataset catalog (`/providers/{id}/datasets`) exposes per-dataset `aiHints` plus backend `routing` metadata for assistant/tool decisions
- `q-storage-backend`: read/write map persistence backend

#### Cloud Load Troubleshooting

- If importing/loading a cloud map triggers:
  - `Actions may not have an undefined "type" property`
- Check `src/reducers/src/provider-state-updaters.ts` (`loadCloudMapSuccessUpdater`).
- In the parse task error branch, map errors to an action creator:
  - `error => loadCloudMapError({error, provider, onError})`
- Do not return updater state objects (e.g. `exportFileErrorUpdater(...)`) from task `.bimap` callbacks.

For production deployment of both backends, see:

- `BACKEND_PRODUCTION.md`

Expected map payload shape for download:

```json
{
  "map": {
    "datasets": [],
    "config": {},
    "info": {}
  }
}
```

### PostGIS Data Bootstrap

`q-cumber-postgis` restores pgdump files from `backends/q-cumber-postgis/dumps/` at first
startup. To reset and reload data:

```bash
cd examples/q-map/backends
rm -rf q-cumber-postgis/data
docker compose up -d --build q-cumber-postgis
```

### Production Build

To create a production build and preview it:

```bash
# Create production build
yarn build

# Preview production build
yarn preview
```
