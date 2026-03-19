# Q Map Agent Notes

## Scope
This file is for future coding sessions in `examples/q-map`.
- Convention for this project: keep customizations in `examples/q-map/*` via injection/overrides.
- Convention for backend services: keep them under `examples/q-map/backends/*`.
- Avoid editing root Kepler packages under `src/*` unless explicitly requested.
- Track pending changes in `examples/q-map/CHANGELOG.md` before creating commits.

## Session Bootstrap (Mandatory)
When a user asks to read `examples/q-map/AGENT.md`, treat it as a request to bootstrap a prompt-driven improvement session, not only documentation lookup.

Bootstrap sequence (default):
1. Read context files in this order:
   - `examples/q-map/AGENT.md`
   - `examples/q-map/SYSTEM_ENGINEERING_LOOP.md`
   - `examples/q-map/tests/ai-eval/architecture-matrix.json`
2. Run context/coverage checks:
   - `make -C examples/q-map ai-matrix-audit`
   - `make -C examples/q-map tool-coverage-audit`
   - `make -C examples/q-map ai-governance-audit`
   - `make -C examples/q-map ai-threshold-audit`
   - `make -C examples/q-map ai-operational-audit` (after at least one functional report is available; enforces latency/transport KPI ceilings on latest functional report)
   - `make -C examples/q-map ai-trace-grade-audit` (after at least one functional report with requestIds-backed chat-audit evidence is available)
   - `make -C examples/q-map ai-passk-audit` (SKIP is acceptable until there are at least 3 adversarial reports; once history exists it enforces pass^k reliability on the held-out slice)
   - `make -C examples/q-map ai-variance-audit` (SKIP is acceptable when functional history is not yet large enough)
   - `make -C examples/q-map ai-area-variance-audit` (non-blocking warning gate for per-area KPI drift on latest baseline window)
   - `make -C examples/q-map changelog-audit`
3. Start from clean artifacts:
   - `make -C examples/q-map fix-test-perms`
   - `make -C examples/q-map clean-loop`
   - `clean-loop` now preserves `tests/ai-eval/results` by default so variance gates keep their baseline window; use `make -C examples/q-map clean-loop-hard` (or `CLEAN_LOOP_PURGE_EVAL_RESULTS=1`) only when you intentionally want to reset eval history.
   - `fix-test-perms` normalizes ownership/permissions for test/audit folders (`test-results`, `playwright-report`, `tests/ai-eval/results`, `backends/logs/q-assistant/chat-audit`) and should be preferred over ad-hoc manual `chown`.
4. Validate eval transport preflight before loop:
   - `make -C examples/q-map backend-ready`
   - `make -C examples/q-map ai-eval-preflight`
   - if backend is behind JWT-protected Kong edge, export `QMAP_AI_EVAL_BEARER_TOKEN=<jwt>` before preflight/loop.
   - if `Q_ASSISTANT_PROVIDER=openrouter`, ensure `OPENROUTER_API_KEY` (or `Q_ASSISTANT_API_KEY`) is set in `examples/q-map/backends/.env` and `q-assistant` has been recreated after the change; `/health` can be green while `/chat/completions` still returns `401` without provider credentials.
   - this checks q-assistant `/health` with both `curl` and Node `fetch` to catch sandbox-localhost `EPERM` early.
5. Run iterative engineering loop:
   - `make -C examples/q-map loop RUN_ID=<tag>`
6. Use matrix areas/KPI to prioritize fixes:
   - `ai_tool_orchestration`
   - `geo_processing`
   - `data_pipeline`
   - `map_ux`
   - `backend_reliability`

Execution-root discipline (mandatory):
- Prefer `make -C examples/q-map ...` from repo root, or run commands with `cwd=examples/q-map`.
- Do not run `node scripts/run-ai-eval.mjs` from repo root: that resolves runtime tool files against wrong paths and may fail with `ENOENT` (e.g. missing `src/features/qmap-ai/qmap-ai-assistant-component.tsx`).
- In restricted sandboxes, Node HTTP calls to `localhost` can fail with `EPERM` even when `curl` works; if `run-ai-eval` aborts with `preflight /health failed: fetch failed`, rerun the `make -C examples/q-map ...` command with elevated/out-of-sandbox permissions.
- Equivalent safe forms:
  - from repo root: `make -C examples/q-map ai-eval`
  - from `examples/q-map`: `node scripts/run-ai-eval.mjs --cases tests/ai-eval/cases.functional.json ...`

Working rule:
- Every change should improve or protect loop metrics (ai-eval + quality-gate) and keep tool coverage audits green.
- AI governance baseline is part of quality-gate; keep `make -C examples/q-map ai-governance-audit` green.
- `quality-gate` is expected to exercise `ai-eval-functional` before merge/release so governance controls tied to functional behavior are enforced by the gate, not only documented.
- `quality-gate` now also enforces `ai-operational-audit`, so latency/transport regressions fail before merge instead of only showing up in markdown summaries.
- `ai-eval-functional` reports now carry backend `requestId` references; `ai-trace-grade-audit` uses them to grade the matching `chat-audit` traces for critical cases.
- `make -C examples/q-map ai-eval-adversarial` runs the held-out/adversarial suite with deterministic case constraints disabled; use it when validating generalization beyond the deterministic functional benchmark.
- Changelog discipline is enforced by `make -C examples/q-map changelog-audit` (included in `quality-gate`): q-map technical changes must include `examples/q-map/CHANGELOG.md` update.
- Fast triage rule for failed evals:
  - `scripts/run-ai-eval.mjs` now prints `[ai-eval][fail]` blocks with prompt, observed toolCalls, required tools and assistant text; use this output first to decide whether to fix runtime routing, extend case expectations, or retire a stale case.

## Future Option: External Remediation Agent
- Candidate: `mini-swe-agent` (or equivalent) may be adopted later as an outer remediation loop for q-assistant/backend maintenance.
- Scope boundary: keep it out of runtime request path (`frontend -> q-assistant -> q-cumber/q-storage`); use only in dev/CI control plane.
- Recommended integration:
  - dedicated service/job (`remediation-runner`) on isolated branch/workspace
  - trigger on failed gates (`make -C examples/q-map loop`, `make -C examples/q-map quality-gate`) or scheduled backlog triage
  - required post-patch gates: `ai-matrix-audit`, `tool-coverage-audit`, `ai-governance-audit`, `changelog-audit`, backend `unittest`, `ai-eval-functional`
- Human review checkpoints (mandatory):
  - before enabling on real repo: approve scope, allowed commands, writable paths, secret handling, and execution mode (`confirm`, not autonomous merge)
  - before merge: review PR diff + gate results + metrics delta
  - optional periodic governance review (weekly/bi-weekly): acceptance rate, escaped regressions, rollback count

## Branding
- UI-facing branding in q-map uses `q-hive`.
- Keep technical compatibility keys unchanged where required (`@kepler.gl/*`, reducer state keys like `keplerGl`, payload format `keplergl`).

## Quick Start
- `cd examples/q-map`
- Node version: `20`
- `yarn install`
- `yarn dev`
- `yarn build` (known TS warning may fail on `react-virtualized` typing)
- E2E tests (Playwright):
  - local prerequisite: bootstrap q-map auth/env with `make -C examples/q-map dev-local` (or `make -C examples/q-map dev-local-prepare` before manual `yarn dev`)
  - do not treat the root-platform `make platform-up` stack as a replacement for q-map local Playwright/bootstrap: tests hitting `http://127.0.0.1:8081` still expect the q-map runtime JWT written to `.env.development.local`, otherwise backend-driven cloud/tool flows can fail with `Missing bearer token`
  - for `yarn test:e2e:ux`, ensure q-map backends are reachable on direct host ports (`3001/3004/3005`) because some specs seed q-storage via direct `http://127.0.0.1:3005/maps` calls:
    - `make -C examples/q-map dev-local-prepare`
    - `make -C examples/q-map/backends up-direct`
    - if q-storage seed returns `503 Auth is not configured`, rerun backend start with local-only override:
      - `QSTORAGE_ALLOW_INSECURE_DEFAULT_USER=true make -C examples/q-map/backends up-direct`
    - this override is for local test bootstrap only; do not treat it as production/default policy
  - all specs: `yarn test:e2e`
  - UX/functional suite: `yarn test:e2e:ux`
  - UX/functional suite (UI mode): `yarn test:e2e:ux:ui`
  - Assistant interaction suite: `yarn test:e2e:assistant`
  - Assistant interaction suite (UI mode): `yarn test:e2e:assistant:ui`
  - Playwright UI runner (all specs): `yarn test:e2e:ui`
  - worker unit tests (clip/H3/reproject/zonal workers): `yarn test:unit:workers`
  - direct single-spec debug (optional):
    - `yarn test:e2e tests/e2e/ai-mode-policy.spec.ts`
- Backend orchestration (Docker):
  - `cd examples/q-map/backends`
  - `cp .env.example .env`
  - `make up` (default: Kong edge-only, backend ports internal)
  - `make up-direct` (debug: backend direct ports exposed on host)
  - Compose files (source of truth):
    - `examples/q-map/backends/docker-compose.yaml` (core backend stack)
    - `examples/q-map/backends/docker-compose.edge-only.yaml` (gateway overlay, strips backend host ports)
    - `examples/q-map/backends/docker-compose.kong.yaml` (Kong edge overlay/profile)
    - `examples/q-map/backends/docker-compose.ui.yaml` (UI overlay/profile)
    - `examples/q-map/docker-compose.same-origin.yaml` (same-origin local shell: `/` -> `q-map-ui`, `/api/*` -> Kong/backends)
  - note: `docker-compose.local.yml` is not present in this repo snapshot; use `docker-compose.yaml`
  - quick status/log checks (run from `examples/q-map/backends`):
    - `make ps` (gateway mode) or `make ps-direct` (direct mode)
    - `docker compose -f docker-compose.yaml logs --tail=120 q-assistant`
    - `docker compose -f docker-compose.yaml logs --tail=120 q-cumber-backend`
    - `docker compose -f docker-compose.yaml logs --tail=120 q-storage-backend`
  - persistent data is bind-mounted on host under:
    - `q-cumber-postgis/data` (PostGIS)
    - `q-storage-backend/data` (q-storage maps)
  - quick q-assistant provider/model switch:
    - `./switch-q-assistant.sh openrouter google/gemini-3-flash-preview`
    - `make -C examples/q-map/backends qa-switch-openrouter`
  - UI + backend prod-like stack:
    - `docker compose -f docker-compose.yaml -f docker-compose.ui.yaml --profile ui up -d --build`
    - UI available at `http://localhost:8081`
  - preferred local single-origin shell from `examples/q-map`:
    - `make same-origin-up`
    - `make same-origin-ps`
    - `make same-origin-logs SERVICE=q-map-shell`
    - `make same-origin-down`
    - shell available at `http://localhost:8080`
  - same-origin auth reads only `QMAP_AUTH_RUNTIME_TOKEN`; if local auth stops working after this cleanup, regenerate `.env.development.local` with `make dev-local-prepare`
  - same note for local Vite/Playwright on `:8081`: regenerate `.env.development.local` with `make dev-local-prepare` before debugging bearer-token failures instead of assuming the root `platform-up` login stack covers q-map local auth bootstrap
  - important Docker build rule for `q-map-ui`:
    - `examples/q-map/Dockerfile.ui` must build from the full `kepler.gl` repo context, not only `examples/q-map`
    - reason: container UI must include local modifications under both `examples/q-map/*` and root `kepler.gl/src/*`, matching local Vite/dev behavior
    - when editing compose files for the UI image, keep `dockerfile: examples/q-map/Dockerfile.ui` and a repo-root build context
  - deployment guardrail:
    - pre-production images are built in an external pipeline; do not introduce local-test architectural bias in Dockerfiles/entrypoints/base image assumptions
    - keep Dockerfile changes environment-neutral and ask for confirmation when unsure about deployment intent
  - q-map UI image helpers from `examples/q-map`:
    - `make registry-login`
    - `make ui-image-refs IMAGE_TAG=<tag>`
    - `make ui-image-build IMAGE_TAG=<tag>`
    - `make ui-image-push IMAGE_TAG=<tag>`
    - default target is `quay.io/algebrasrl/q-map--ui:<tag>`
    - platform release tagging/publish policy belongs to the root `q_hive/Makefile`; keep `examples/q-map` focused on local dev, KPI loops, and low-level image build primitives.
- Backend unit tests (Python `unittest`):
  - **Always run via `docker exec` on running containers** — do NOT use local venv or run `python`/`python3` directly; the local environment may be missing shared dependencies (e.g. `q_backends_shared`). Code is bind-mounted so changes are reflected without rebuild; after Python-only source changes use `docker compose restart <service>` (no rebuild needed).
  - Containerized (preferred; after `docker compose up -d --build q-cumber-backend q-assistant q-storage-backend`):
    - `docker exec q-map-q-cumber-backend python -m unittest -q tests/test_provider_registry_and_storage.py tests/test_dataset_adapters.py tests/test_api_routing_metadata.py tests/test_jwt_auth.py`
    - `docker exec q-map-q-assistant python -m unittest -q tests/test_explicit_tool_routing.py tests/test_agent_skip_policy.py tests/test_chat_audit_utils.py tests/test_request_coercion.py tests/test_chat_payload_compaction.py tests/test_token_budget_compaction.py tests/test_objective_anchor.py tests/test_runtime_guardrails.py tests/test_openai_stream_normalization.py tests/test_openrouter_provider.py`
    - `docker exec q-map-q-storage-backend python -m unittest -q tests/test_config_and_storage.py tests/test_jwt_auth.py`
- Mapbox token for geocoder/base map:
  - Set `VITE_MAPBOX_TOKEN` in `.env.development.local`
- Local IT datasets (q-cumber provider):
  - Provider id: `local-assets-it`
  - Dataset `kontur-boundaries-italia` (admin boundaries, lv levels, population)
  - Dataset `clc-2018-italia` (Corine Land Cover 2018)
  - Onboarding guide for adding new territorial/thematic providers: `examples/q-map/backends/q-cumber-backend/PROVIDER_ONBOARDING.md`
- Cloud storage in UI (optional):
  - Set `VITE_QCUMBER_CLOUD_*` vars in `.env.development.local` (see below)
- Tileset provider presets (optional):
  - Set `VITE_QCUMBER_TILESET_BASE` in `.env.development.local` (default `http://localhost:3002`)
  - In `Add Data To Map -> Tileset`, a `Provider` selector is shown above `Tileset Type`
  - Selecting `Q-cumber` pre-fills vector tileset/metadata URLs

## Stack
- Vite + React + Redux
- Kepler embedded via `@kepler.gl/components`
- Entry: `src/main.tsx`

## Project-Specific Wiring
- Custom side-panel logo uses component injection:
  - `src/components/panel-header.jsx`
  - `src/components/side-bar.jsx`
  - Injected in `src/main.tsx` with:
    - `injectComponents`
    - `PanelHeaderFactory -> CustomPanelHeaderFactory`
    - `SidebarFactory -> CustomSidebarFactory`
    - `CustomPanelsFactory -> QMapCustomPanelsFactory`
    - `LoadDataModalFactory -> QMapLoadDataModalFactory`
- Theme tokens are centralized in:
  - `src/theme/q-map-theme.ts`
- Cloud providers are wired in:
  - `src/cloud-providers/index.ts` -> `getQMapCloudProviders()`
  - `src/cloud-providers/custom-cloud-provider.ts`
  - passed to `<KeplerGl cloudProviders={...} />` in `src/main.tsx`
  - Current registrations in q-map UI: `q-cumber-backend`, `q-storage-backend` (env-gated)
- Tileset provider UI override is wired with component injection in:
  - `src/components/load-tileset.tsx`
  - injected in `src/main.tsx` via `LoadTileSetFactory -> QMapLoadTilesetFactory`
- Load Map using URL is added without touching root code:
  - `src/components/load-data-modal.tsx` (adds custom loading method order/tab)
  - `src/components/load-remote-map.tsx` (URL fetch form; forwards as file to existing loader path)
  - appears as dedicated `Load Map using URL` section in Add Data modal
- Draw controls in q-map:
  - Kepler default draw panel (`Select/Polygon/Rectangle/Raggio`) is kept for `kepler` mode in `src/components/map-draw-panel.tsx`.
  - `draw-stressor` mode uses custom draw menus in `src/components/draw-tool-controls.tsx`, wired through `src/components/map-control.tsx` (`replaceMapControl`):
    - `PE` (draw perimeter target)
    - `OP` (draw operations target)
    - `PE` submenu tools: `polygon`, `rectangle`, `radius`, `clear`
    - `OP` submenu tools: `point`, `line`, `polygon`, `rectangle`, `radius`, `clear`
  - note: do not inject `MapDrawPanelFactory` directly in `src/main.tsx` together with `replaceMapControl` (double-injection warning)
- q-map mode system (`kepler`, `draw-stressor`, `draw-on-map`) is centralized in:
  - `src/mode/qmap-mode.ts`
  - mode selector UI control: `src/components/mode-selector-control.tsx` (top-right map controls, dropdown style like drawing selector)
  - mode action: `src/features/qmap-mode/actions.ts` (`setQMapMode`)
  - runtime mode state in `uiState`: `qmapMode`
  - user context keys in `uiState` (for mode availability policy): `qmapUserType`, `qmapUserGroupSlug`
  - mode policy currently returns all modes for standard user context; future specialization should branch in `getQMapModeOptionsForUser(...)` by `userType/groupSlug`
  - mode effects are enforced at runtime:
    - side panel tabs filtered in `src/components/panel-toggle.tsx`
    - built-in map controls visibility set via `uiState.mapControls` in `src/main.tsx` reducer plugin
    - basemap default for `draw-stressor` is `satellite` (on initial boot in that mode and on mode switch to `draw-stressor` via `mapStyleChange`)
    - custom controls (`H3`, `AI`, custom draw menus `PE/OP`) are mode-gated in `src/components/h3-paint-control.tsx`, `src/features/qmap-ai/control.tsx`, and `src/components/draw-tool-controls.tsx`
    - Add Data modal methods are mode-gated in `src/components/load-data-modal.tsx`:
      - `draw-stressor` exposes only `upload` (`Carica file`)
    - `draw-on-map` keeps only draw/split/3d/locale built-in controls and H3 custom control enabled; AI/custom draw menus disabled
    - entering `draw-on-map` sets `uiState.activeSidePanel = null` (left panel starts closed; `layer` panel remains available if opened manually)
- Hash preset bootstrap/parser:
  - `src/utils/hash-preset.ts` parses `preset` from URL hash as base64url JSON
  - supported hash forms: `#preset=<base64url>`, `#/map?preset=<base64url>`, `#mode=<kepler|draw-stressor|draw-on-map>`, `#/map?mode=<kepler|draw-stressor|draw-on-map>`
  - supported short map params: `lat`, `lon`/`lng`, `zoom`/`z`, `bearing`/`brg`, `pitch`, `basemap`/`style`/`styleType`
  - logger + preset apply are wired in `src/main.tsx` at bootstrap and on `hashchange`
  - apply whitelist: `uiState.qmapMode`, `activeSidePanel` (supports `null`), `readOnly`, `locale`, `mapControls` (`show`/`active` on known controls)
  - map launch apply: viewport (`latitude`,`longitude`,`zoom`,`bearing`,`pitch`) + `mapStyle.styleType` (basemap)
  - precedence on collisions: short hash params > preset JSON > q-map defaults (mode fallback defaults last)
- Draw persistence model:
  - live editing uses Kepler editor feature buffer (`visState.editor.features`).
  - q-map syncs editor features to datasets by target/tool via:
    - runtime helpers in `src/features/qmap-draw/runtime.ts`
    - middleware orchestration in `src/features/qmap-draw/middleware.ts`
    - wiring in `src/main.tsx` (factory injection only)
  - in `geotoken`, q-map auto-arms polygon draw on startup/mode entry and auto-generates dataset `Tassellation_r11` from the current editor polygons.
  - geotoken auto-tessellation is fail-closed on perimeter size: if the union area of drawn polygons exceeds `100 km2`, q-map must not launch the H3 worker, must remove any stale auto-generated tessellation dataset, and must surface the limit via Kepler notification.
  - sync is append/upsert by `feature_id` per dataset: closing/reopening draw tools does not wipe existing dataset rows.
  - switching subtool clears editor "shadow" features of the previous target/tool (dataset rows are preserved).
  - exiting `PE`/`OP` tools clears editor "shadow" features for that target (session reset), while datasets remain unchanged.
  - draw datasets stay visible while drawing (no automatic layer hide during active tool sessions).
  - to restart from empty data, remove the related draw dataset from Dataset list.
  - dataset naming:
    - `stressor_perimeter__{point|line|polygon|rectangle|radius}`
    - `stressor_operations__{point|line|polygon|rectangle|radius}`
  - deleting one of these datasets from dataset list also removes matching editor features (prevents orphan geometries on map).
- Custom cursor/double-click behavior for point/line draw is controlled via `mapDraw.settings` flags consumed in `src/components/src/map-container.tsx` (intentional root edit requested by product UX).
- Custom side-panel tab `Profilo` is wired with component injection:
  - `src/components/custom-panels.tsx`
  - injected in `src/main.tsx` via `CustomPanelsFactory -> QMapCustomPanelsFactory`
  - panel id: `profile` (translation key: `sidebar.panels.profile`)
  - panel fetches user profile from q-assistant `GET /me` and renders:
    - `name`, `email`, `registeredAt`, `country`
- Geometry context menu (`Select geometry`) is customized via injection:
  - `src/components/feature-action-panel.tsx`
  - injected in `src/main.tsx` via `FeatureActionPanelFactory -> QMapFeatureActionPanelFactory`
  - adds submenu `Tassellazione` with H3 resolutions `4..11`
  - selecting a resolution tessellates selected polygon/multipolygon and writes/updates dataset `Tassellation` (`h3_id`, `h3_resolution`)
  - H3 runtime uses `h3-js` v4 (`h3-js-v4` alias); use v4 API names (`latLngToCell`, `cellToLatLng`, `polygonToCells`, etc.), not legacy v3 names.
  - TypeScript types for the alias are re-exported from `h3-js` in `src/types/h3-js-v4.d.ts`.
- Side-panel logo spacing is customized in:
  - `src/components/panel-header.jsx` (`side-panel-logo__logo` inline `marginTop` / `marginBottom`)
- Upload tab URL import:
  - Implemented as dedicated Add Data method (`Load Map using URL`) via injected `QMapLoadDataModalFactory`.
  - It fetches remote data and forwards it to the existing file loader path.
- Upload format policy in q-map UX:
  - Upload method is wrapped by `src/components/file-upload-with-url.tsx` (wired in `src/components/load-data-modal.tsx`).
  - UX-advertised formats: `CSV`, `JSON`, `GeoJSON`, `Shapefile ZIP` (`.zip`), `GeoPackage` (`.gpkg`).
  - `Arrow`/`Parquet` are intentionally disabled in q-map runtime (no loader support in bundle).
  - `.zip`/`.gpkg` are converted client-side with DuckDB-WASM (`ST_READ` + `ST_AsGeoJSON`) before forwarding to Kepler loader.
  - DuckDB init has runtime fallback from threaded bundle to single-thread (`mvp`) when browser thread/pthread resources are unavailable.
  - ZIP shapefiles use `.prj`-based reprojection to `EPSG:4326` via `proj4`; GPKG uses DuckDB metadata discovery (`ST_Read_Meta`) + `ST_Transform(..., 'EPSG:4326')`; out-of-range lon/lat output is rejected with explicit upload error instead of rendering broken layers.
  - ZIP shapefile uploads support archives with duplicated inner suffixes like `*.shp.shp` (normalized before import), and BigInt properties are serialized safely in output GeoJSON.
  - `arrow`/`parquet` remain hidden in Upload UI and are also disabled at parser/bundle level.
- Bundle notes:
  - AI panel is lazy-loaded (`src/features/qmap-ai/control.tsx` -> dynamic import of `panel`/assistant runtime).
  - Upload panel is lazy-loaded in Add Data modal (`src/components/load-data-modal.tsx` -> dynamic import of `file-upload-with-url`).
  - Map engine chunking is consolidated via Vite `manualChunks` (`map-engine`).
  - Vite applies a local pre-transform patch for `@loaders.gl/mvt` (`convert-feature.js`) to remove unreachable code in `MultiLineString` conversion and silence dev warning noise.

## Custom AI Tool (q-map)
- q-map uses OpenAssistant/Kepler runtime with q-map wrapper components:
  - `reducer.ts` mounted under `demo.qmapAi`
  - `control.tsx` injected in map controls (toggle button + embedded panel)
  - AI panel is rendered inside map controls (not as external overlay), with drag/resize handles
  - `panel.tsx` contains assistant UI shell/content
  - `qmap-ai-assistant-component.tsx` wraps assistant runtime (turn state, validation, message normalization)
  - `context/tool-context.ts` defines `QMapToolContext` interface (consolidated DI for all tool builders)
  - `context/tool-context-provider.ts` builds `QMapToolContext` from Redux store + config
  - `hooks/use-tool-registry.ts` instantiates all tools from context and applies mode/chart policy
  - `middleware/tool-pipeline.ts` 5-stage execution pipeline (preprocess → policy → dedup → circuit-breaker → execute → postprocess); ALL tool calls are serialized through `AsyncMutex` in FIFO order (no parallel execution); per-tool circuit breaker caps any tool at 3 calls per user message (counter persists across sub-requests, resets on new user message)
  - `middleware/cache.ts` bounded collections, async mutex, mutation idempotency; q-cumber query tools excluded from stateless dedup cache (loadToMap mutates map state)
  - `services/execution-tracking.ts` barrel for post-validation, tool component runtime, execution trace
  - `services/qcumber-api.ts` thin TypeScript client for q-cumber via q-assistant proxy
  - `utils/dataset-resolve.ts` dataset resolution, lineage, field lookup
  - `utils/geometry-ops.ts` async chunking, bounds, geometry, coordinate ops
  - `utils/dataset-metadata.ts` color, layer queries, field classification
  - `cloud-tools.tsx` adds q-map cloud tools (q-cumber queries routed via backend proxy)
  - `tool-registry.ts` owns tool-category introspection factories (`listQMapToolCategories`, `listQMapToolsByCategory`)
  - `tool-groups.ts` owns grouped tool-registry composition (`base + cloud + custom`) with duplicate-name guardrails
  - `tool-shim.ts` local identity replacement for `@openassistant/utils extendedTool`
- Recommended env vars in `examples/q-map/.env.development.local`:
  - `VITE_QMAP_AI_PROXY_BASE` (default `http://localhost:8000/api/q-assistant`)
  - `VITE_QMAP_AI_TEMPERATURE` (optional numeric string)
  - `VITE_QMAP_AI_TOP_P` (optional numeric string)
  - `VITE_QMAP_AI_TURN_SNAPSHOT_TTL_MS` (optional; dataset snapshot TTL for hard turn-state enforcement, default `180000`)
  - `VITE_QMAP_MODE` (`kepler` | `draw-stressor` | `draw-on-map`, default/fallback `kepler`)
- Runtime/tool model:
  - Frontend AI panel uses OpenAssistant runtime (`AiAssistant`) with provider config from Redux (`demo.aiAssistant`).
  - Runtime prompt includes an auto-generated dataset/layer context snapshot (exact dataset names + field names + inferred role hints) to reduce tool-call ambiguity.
  - Tool runtime is hardened for imperfect data: layer color tools resolve field objects live by `fieldName` (no stale serialized field objects), enforce safe color ranges, and run inside a local error boundary to avoid breaking the map UI.
  - q-map frontend talks to q-assistant through OpenAI-compatible endpoint `POST /chat/completions`.
  - q-assistant performs provider/model fallback chain server-side (`Q_ASSISTANT_AGENT_CHAIN`), starting from the first configured agent.
  - Base toolset comes from `setupLLMTools` (`src/ai-assistant/src/tools/tools.tsx`):
    - Kepler tools: `basemap`, `addLayer`, `updateLayerColor`, `loadData`, `mapBoundary`, `saveDataToMap`
    - Echarts tools: `boxplotTool`, `bubbleChartTool`, `histogramTool`, `pcpTool`, `scatterplotTool`
    - Geo tools (selection): `spatialJoinTool`, `spatialFilterTool`, `gridTool`, `bufferTool`, `dissolveTool`, `geocoding`, `routing`, `isochrone`, `roads`, `lisaTool`, `globalMoranTool`
    - Query tools: `filterDataset`, `genericQuery`, `tableTool`, `mergeTablesTool`
  - Tool runtime in panel includes default Kepler tools + q-map tools backed by real MCP calls:
    - Frontend MCP client: `src/features/qmap-ai/mcp-client.ts`
    - `listQMapCloudMaps` -> MCP tool `list_qmap_cloud_maps`
      - q-cumber cloud maps may be empty in q-cumber read-only mode.
      - For Kontur/CLC data, prefer q-cumber dataset query tools.
    - `getQCumberDatasetHelp` -> q-cumber backend endpoint `GET /providers/{provider_id}/datasets/{dataset_id}/help`
      - returns `aiHints` plus backend `routing` (`queryToolHint.preferredTool`, dataset class, candidate fields, `metricProfile`)
      - `routing.metricProfile` is the preferred source for anti-bias metric orchestration (numerator/denominator candidates, recommended derived metrics, caveats)
      - use this response before choosing between `queryQCumberTerritorialUnits`, `queryQCumberDatasetSpatial`, or fallback `queryQCumberDataset`
    - `loadQMapCloudMap` -> MCP tool `build_load_cloud_map_action`, then local `loadCloudMap` dispatch
    - `loadCloudMapAndWait` -> MCP map resolution + local `loadCloudMap` dispatch + wait until datasets are present in visState
    - Dataset reference convention: use `listQMapDatasets` and pass `datasetRef` (`id:<datasetId>`) or exact `datasetName` to downstream tools; prefer `datasetRef` to avoid name collisions.
    - Frontend runtime hard-enforces turn execution phases (`discover -> execute -> validate -> finalize`): each assistant turn captures a pre-flight dataset snapshot (discovery equivalent), validation blocks non-validation tools while pending checks run, and stale snapshots must be refreshed before continuing.
    - q-assistant runtime guardrails use `datasetRef` when present in tool results; otherwise they fall back to `datasetName`.
    - q-assistant removes older duplicate successful discovery turns (`listQMapDatasets`, `listQCumberProviders`, `listQCumberDatasets`, `getQCumberDatasetHelp`, schema discovery) before upstream chat calls to keep context lean.
    - `setQMapFieldEqualsFilter` -> MCP tool `build_equals_filter_action`, then local filter dispatch
      - Filter IDs are per field (`qmap_<dataset>_<field>`), so multiple filters on same dataset are combined (AND) instead of overwritten.
    - `applyQMapStylePreset` -> applies predefined styling recipe (currently `comuni_population`) with layer isolation + population choropleth defaults
    - `createDatasetFromFilter` -> local dataset materialization from filtered rows, then local `addDataToMap` dispatch
    - `createDatasetFromCurrentFilters` -> local dataset materialization from active UI filters, then local `addDataToMap` dispatch
    - `reprojectQMapDatasetCrs` -> CRS reprojection (`sourceCrs -> targetCrs`) for GeoJSON geometries and/or lat/lon columns, materializing a derived dataset
      - runtime execution prefers Web Worker for larger inputs, with automatic local fallback if worker fails/unavailable
    - Read-only record inspection tools (q-map local):
      - `previewQMapDatasetRows`
      - `rankQMapDatasetRows`
      - `distinctQMapFieldValues`
      - `searchQMapFieldValues`
      - `countQMapRows`
    - `tassellateSelectedGeometry` -> tessellates currently selected geometry into H3 cells and upserts `Tassellation`
    - `tassellateDatasetLayer` -> tessellates dataset/layer geometries into H3 cells and upserts `Tassellation`; output dataset auto-creates a layer by default (`showOnMap=false` to skip), and intermediate filtered dataset is always added without auto-created layer
    - `clipQMapDatasetByGeometry` -> clips/masks datasets with predicates (`intersects`/`centroid`/`within`) and materializes a derived dataset with active-filter support; source can be GeoJSON or H3 (`h3_id`/`h3__id`), clip dataset must provide GeoJSON polygons (Turf default engine)
    - clipping diagnostics fields can be included per output row: `qmap_clip_match_count`, `qmap_clip_intersection_area_m2`, `qmap_clip_intersection_pct`
    - clipping can also append distinct-value counters from clip-side properties as `<clip_field>__count` (number of distinct matched values per output feature)
    - when `includeDistinctPropertyValueCounts=true`, clipping can append per-value match counters as `<clip_field>__<value>__count`
    - `clipDatasetByBoundary` -> convenience wrapper for boundary clipping (source clipped by boundary dataset)
    - `spatialJoinByPredicate` -> geometry/H3-based join with predicates (`intersects|within|contains|touches`) and aggregated metrics
    - `zonalStatsByAdmin` -> zonal statistics on administrative geometry/H3 inputs with `area_weighted|intersects|centroid` weighting
      - when both inputs expose H3 fields at aligned resolution, runtime uses an H3 fast-path (cell-key join/aggregation) to avoid geometry-intersection cost on the UI thread
      - in geometry mode, runtime executes worker-first and only falls back to local loops below budget thresholds; above threshold it fails fast with actionable guidance (no browser-freeze fallback)
    - `overlayDifference` -> overlay outputs (`intersection`, `a_minus_b`, `b_minus_a`) for polygon/H3 gap analysis
    - `bufferAndSummarize` -> buffer source geometry/H3 features and summarize target counts/values inside buffers
    - `nearestFeatureJoin` -> nearest-neighbor join with distance metrics on geometry/H3 features
    - `adjacencyGraphFromPolygons` -> adjacency edge-list generation for polygon or H3 datasets
    - `coverageQualityReport` -> spatial-match quality diagnostics (coverage/null-rate) on geometry/H3 inputs
    - `aggregateDatasetToH3` -> aggregates geometry/H3 datasets to a target H3 resolution with configurable operations and weight mode
    - `joinQMapDatasetsOnH3` -> joins datasets on H3 with coverage guardrails; low-coverage joins fail fast (adaptive minimum coverage is stricter for population-like fields)
      - numeric fields use requested metric (`avg|sum|max|first`), while categorical/thematic fields (e.g. CLC class labels/codes) are preserved with categorical-safe output (no class averaging)
    - `populateTassellationFromAdminUnits` -> dedicated admin-units -> tessellation enrichment flow (aggregate to target H3 + guarded join)
      - runtime execution prefers Web Worker on larger inputs; deterministic local fallback is used on worker failure/unavailability
      - long-running jobs should survive assistant rerenders (loading-state updates must not cancel in-flight aggregate/populate operations)
    - Statistical analysis tools (backed by `simple-statistics`):
      - `regressQMapFields` -> linear regression between two numeric fields (slope, intercept, R², equation); optional derived dataset with `predicted` and `residual` columns (`showOnMap=true`)
      - `classifyQMapFieldBreaks` -> Ckmeans natural-break classification (Jenks-like) of a numeric field into 2-10 classes; optional derived dataset with classification field (`showOnMap=true`)
      - `correlateQMapFields` -> pairwise Pearson correlation matrix between 2-10 numeric fields with strength classification (strong/moderate/weak/negligible); read-only, no dataset mutation
    - Regulatory compliance tools (backed by `data/regulatory-thresholds.json`, D.Lgs. 155/2010):
      - `listRegulatoryThresholds` -> list air quality limits (D.Lgs. 155/2010 + WHO AQG 2021 + EU 2030) for 12 pollutants: PM10, PM2.5, NO2, O3, SO2, CO, Benzene, Pb, As, Cd, Ni, BaP; includes critical levels for vegetation and O3 AOT40; read-only, no dataset needed
      - `checkRegulatoryCompliance` -> per-station compliance check against regulatory thresholds; supports `includeWho=true` for WHO comparison and `includeEu2030=true` for future EU limits; reports exceedance counts, compliance rate, and applicable limit references
    - Exposure and interpolation tools:
      - `assessPopulationExposure` -> joins measurement stations with nearby admin boundaries (haversine buffer); aggregates exposed population per station with optional regulatory compliance check; supports `showOnMap=true` for derived dataset
      - `interpolateIDW` -> Inverse Distance Weighting interpolation from point measurements to H3 hexagonal grid; creates continuous surface estimate with configurable power/neighbors/search radius; output is H3 layer auto-detected by Kepler
  - Real MCP endpoint is `/mcp` (HTTP transport).
  - Reference implementation for MCP TS clients:
    - https://github.com/modelcontextprotocol/typescript-sdk?tab=readme-ov-file#writing-mcp-clients
  - Tool wiring uses `setupLLMTools` from `@kepler.gl/ai-assistant/tools/tools` plus q-map additions in `cloud-tools.tsx`.
  - Runtime registry composition best practice:
    - tool builders accept `(ctx: QMapToolContext)` and are instantiated in `hooks/use-tool-registry.ts`
    - custom tools are grouped by domain (`discovery`, `datasetOps`, `stylingUi`, `spatialAnalysis`, `h3Processing`) in `runtime-tool-groups.ts`
    - merge groups through `buildQMapToolsWithoutCategoryIntrospection(...)` in `tool-groups.ts`
    - keep `strict` duplicate check enabled in dev (`strict: Boolean(import.meta.env.DEV)`) so duplicate tool keys fail fast during local refactors
  - Disabled base tools in q-map runtime (by design): `genericQuery`, `filterDataset`, `queryDataset`, `runQuery`, `runSqlOnDataset`, `sqlQuery`, `tableTool`, `mergeTablesTool`, `updateLayerColor`, `addLayer`, `spatialFilterTool`, `spatialJoinTool`, `dissolveTool`.
  - Keep those disabled tools off by default: they bypass q-map deterministic guardrails and can produce non-reproducible analytical outputs.
  - Chart tools are runtime-policy gated (safe/full/timeseries-safe): q-map custom charts (`categoryBarsTool`, `wordCloudTool`) remain available, while base ECharts chart tools are exposed only when present in the active runtime.
  - Runtime tool introspection is category-aware: use `listQMapToolCategories` then `listQMapToolsByCategory` to constrain tool routing by functional class (discovery/query/styling/analysis/charts/etc.).
  - Use q-map tool `summarizeQMapTimeSeries` for timeseries questions (safe fallback without chart UI side-effects).
  - Prompt strategy is data-agnostic: choose maps via `aiHints`/dataset inspection and backend routing metadata, avoid hardcoded assumptions about specific seeded datasets.
  - q-cumber dataset listing performance rule: do not force eager metadata recomputation on `GET /providers/{provider_id}/datasets`; list uses cached `aiHints` metadata, while deeper refresh happens on-demand via dataset help/query endpoints.
  - `listQCumberDatasets` should use explicit `providerId`; if omitted, runtime auto-select is allowed only when catalog resolution is uniquely unambiguous (single provider), otherwise it fails fast and requires explicit provider selection. Recommended deterministic order remains `listQCumberProviders` -> `listQCumberDatasets(providerId=...)` -> `getQCumberDatasetHelp(providerId,datasetId)` -> query via `routing.queryToolHint.preferredTool`.
  - If `providerId` is explicitly provided and invalid/unavailable, runtime must not silently fall back to a different provider; fail fast with clear error and available provider ids.
  - If q-cumber returns `provider not found` during tool execution, runtime must not switch provider implicitly in-turn; fail fast and require explicit provider re-resolution (`listQCumberProviders` -> explicit `providerId`).
  - `queryQCumberTerritorialUnits` must remain strict: use only for administrative datasets; do not silently downgrade to thematic behavior in-tool.
  - If a strict query tool returns retry directives (`retryWithTool`, `retryWithArgs`), runtime may execute one automatic retry using routing hints; keep retry trace visible in tool details/audit (`autoRetry`).
  - Never pass `expectedAdminType` in thematic spatial flows (`queryQCumberDatasetSpatial`, e.g. CLC/land-cover), because thematic datasets may not expose administrative level fields like `lv`.
  - `tassellateDatasetLayer` should not be used directly on very large thematic land-cover datasets (CLC/corine): runtime now emits retry directives to `aggregateDatasetToH3` with class grouping to prevent timeout-heavy tessellation paths.
  - For H3 tessellation + large thematic overlays (e.g. CLC forests on regional/national H3), prefer `aggregateDatasetToH3` + `joinQMapDatasetsOnH3`; avoid clipping full thematic polygon layers against H3 when a pure H3 join path is available.
  - For named-boundary thematic H3 outputs (e.g. "boschi in Veneto"), final dataset must be boundary-exact with deterministic sequence: boundary tessellation (left) -> thematic H3 aggregate (right) -> `joinQMapDatasetsOnH3` (left join) -> final `clipQMapDatasetByGeometry` by boundary polygon. Do not expose raw thematic H3 aggregate as final layer.
  - For clip + thematic outputs, keep both semantic thematic fields and clip diagnostics (`qmap_clip_*`) in the final dataset unless the user explicitly asks to drop diagnostics.
  - If a query uses generic `parent_id` and backend rejects the field, q-map runtime performs one deterministic rewrite retry using backend parent-id candidates (`routing.parentIdFieldCandidates` + `aiHints.aiProfile.adminWorkflows`) before returning failure.
  - `queryQCumber*` tools in q-map do not expose `select` anymore: the backend query is always full-schema and field reduction must happen in q-map/Kepler layer logic.
  - `queryQCumberDataset*` with `loadToMap=true` auto-pages backend windows (`limit/offset`) when `totalMatched > returned`; this includes results beyond the per-request 100000 cap.
  - In analytical geometry tools (`spatialJoinByPredicate`, `zonalStatsByAdmin`, `clip*`, `overlayDifference`, `bufferAndSummarize`, `nearestFeatureJoin`, `adjacencyGraphFromPolygons`, `aggregateDatasetToH3`, `tassellateDatasetLayer`), avoid silent truncation by default: keep `max*Features` unset unless an explicit cap is requested, and always state when output is truncated.
  - For thematic spatial queries with auto-injected `spatialBbox` (for example local-assets-it default Italy bbox), if the first `loadToMap=true` query returns `totalMatched=0` with no user filters, runtime retries once without `spatialBbox` before returning empty results.
  - `queryQCumber*` tools should pass exact `datasetId` values from `listQCumberDatasets(providerId)`; invalid explicit ids fail fast with available catalog ids. If omitted, runtime attempts deterministic dataset auto-selection from provider catalog metadata and fails fast when candidate is ambiguous.
  - For child administrative queries (e.g. comuni di una regione/provincia), resolve parent filter fields from metadata/routing candidates; do not hardcode `parent_id`.
  - For ambiguous named administrative entities (same name across multiple levels), q-map query runtime fails fast unless level intent is explicit (`expectedAdminType` or `lv`), to avoid loading cross-level matches.
  - Derived-tool layer hygiene: when output is shown, runtime auto-hides source layers for `aggregateDatasetToH3`, `joinQMapDatasetsOnH3`, `clipQMapDatasetByGeometry`, `populateTassellationFromAdminUnits*` unless explicitly disabled by env.
  - Layer hygiene also applies to analytical derived tools (`zonalStatsByAdmin`, `spatialJoinByPredicate`, `overlayDifference`, `bufferAndSummarize`, `nearestFeatureJoin`): avoid leaving all intermediate/source layers visible by default.
  - Derived dataset materialization tools (`createDatasetFromFilter`, `createDatasetFromCurrentFilters`, `createDatasetWithGeometryArea`, `reprojectQMapDatasetCrs`) default to dataset-only outputs (`showOnMap=false`) unless explicitly requested as final visible output.
  - For q-cumber query chains, prefer `loadToMap=true` + `showOnMap=false` for intermediate datasets and reserve `showOnMap=true` for final user-visible output.
  - For final analytical outputs, keep result field naming semantic and analysis-specific (use explicit tool params like `outputFieldName`/`targetValueFieldName`/`outputAreaField` when available) and align tooltip fields with `setQMapTooltipFields`.
  - Long-running geometry tools should remain rerender-safe: use guarded async execution (`hasRunRef`/execution key), cancellation on unmount, and cooperative yielding for heavy loops to avoid stale writes and UI freezes.
  - `zonalStatsByAdmin` has an execution-cost guardrail for geometry mode (`VITE_QMAP_AI_ZONAL_MAX_LOCAL_PAIR_EVAL`); when estimated pair evaluations exceed budget, runtime fails fast with actionable guidance instead of freezing the browser.
  - In worker geometry paths (`zonalStatsByAdmin`, clip/overlay variants), Turf 7 intersection calls must use `featureCollection([a,b])`-safe wrappers (with legacy-signature fallback) to avoid false all-zero `area_weighted` outputs.
  - q-assistant runtime guardrails include metric-field-missing recovery: when ranking fails with `Metric field "..." not found`, force `previewQMapDatasetRows` and retry ranking on an existing numeric metric field before finalizing.
  - For ranking/superlative objectives, q-assistant runtime guardrails require ranking evidence in final text (ordered rows from `rankQMapDatasetRows`), not only generic summary.
  - After unresolved `zonalStatsByAdmin` UI-freeze failures in forest-value workflows, guardrails must block population/name fallback coloring and must not allow inferred ranking claims without computed evidence.
  - For `zonalStatsByAdmin`, use canonical args only: `{adminDatasetName, valueDatasetName, valueField?, aggregation?, weightMode?, outputFieldName?, showOnMap?, newDatasetName?}`. Do not use non-canonical keys like `targetDatasetName`, `adminNameField`, `targetValueFieldName`, `operations`.
  - If ranking metric is flat (`distinct=1`), final text must explicitly state ties instead of claiming a unique top/bottom region.
  - For ranking objectives, a category chart on `name` without metric axis is not valid evidence; guardrails must force metric-based evidence before finalization.
  - For "problemi/pressione ambientale" objectives, runtime guardrails must not silently switch to population/name fallback metrics unless explicitly requested by the user.
  - Prompt-driven evolution policy: each reproducible functional failure from audit/chat must be codified as a case in `tests/ai-eval/cases.functional.json` and validated via `yarn ai:eval:functional` before considering the fix complete.
  - `expectedAdminType` enforcement is strict when explicitly provided (e.g. `province -> lv=7`, `municipality -> lv=9`) and is not downgraded from sampled levels.
  - Ranking metric guardrail: avoid geometry/identifier fields (`_geojson`, `geom`, `gid`, `*_id`) unless explicitly requested; prefer metadata candidates (`routing.orderByCandidates`, `aiHints.orderByCandidates`) with semantic numeric metrics first.
  - Ranking/superlative answers must be backed by explicitly ordered tool output (backend `orderBy` or frontend preview with `orderBy`/`sortDirection`); alphabetical/default previews are not valid ranking evidence.
  - `previewQMapDatasetRows` without `orderBy` is inspection-only (`head sample`): do not use it as analytical/ranking evidence on large datasets.
  - Duplicate-query guardrail: if the same q-cumber query (same tool + normalized args) already succeeded in the active workflow, reuse the previous result instead of issuing a new backend query.
  - q-assistant backend loop guardrail: when repeated discovery-only cycles are detected (`listQCumberProviders`/`listQCumberDatasets` without progress), backend prunes redundant discovery tools from advertised schema to force concrete query progression.
  - q-assistant backend runtime-tool guardrail: `tableTool` and `mergeTablesTool` are pruned from advertised tool schema before upstream calls (defensive layer for stale/cached frontends).
  - For async dataset materialization (`zonalStatsByAdmin` and similar), on `waitForQMapDataset` timeout allow at most one retry with longer timeout; avoid re-running the full workflow in repeated loops.
  - Ranking queries should stay off-map (`loadToMap=false`) only for list/text responses; if map output or map transforms are required, execute with `loadToMap=true`.
  - q-cumber frontend query timeout is configurable with `VITE_QCUMBER_BACKEND_TIMEOUT_MS` (default `45000` ms) to fail fast instead of hanging indefinitely.
  - `queryQCumberDataset` uses geometry-first loading: automatic point inference from lat/lon is disabled by default; enable only with `inferPointsFromLatLon=true` (or env `VITE_QMAP_AI_QUERY_INCLUDE_LATLON_FALLBACK=true`).
  - For ambiguous administrative names (e.g. `Brescia`), disambiguate administrative level first and query using `name + lv` before `loadToMap`.

## q-assistant Backend (AI Proxy)
- Backend package path:
  - `examples/q-map/backends/q-assistant`
- Run:
  - `cd examples/q-map/backends/q-assistant`
  - `python -m venv .venv`
  - `source .venv/bin/activate`
  - `pip install -e .`
  - `q-assistant`
- Default backend URL:
  - `http://localhost:3004`
- Endpoints:
  - `GET /health`
  - `GET /me` (profile payload for q-map Profile panel)
  - `POST /chat` (non-streaming JSON endpoint; useful for direct backend integration/tests)
  - `POST /chat/completions` (OpenAI-compatible endpoint used by q-map frontend runtime; `parallel_tool_calls: false` is enforced in payload coercion to prevent batch tool chains with stale dataset IDs)
  - `GET/POST/DELETE /mcp` (real MCP server endpoint via `fastapi-mcp`, HTTP transport)
  - q-cumber proxy endpoints (frontend routes q-cumber calls through q-assistant; caller bearer token is forwarded to q-cumber for auth):
    - `GET /qcumber/providers` (list q-cumber providers)
    - `GET /qcumber/providers/{id}/datasets` (list datasets for provider)
    - `GET /qcumber/providers/{id}/datasets/{did}` (dataset help/metadata)
    - `POST /qcumber/query` (query q-cumber dataset)
  - q-map MCP helper endpoints:
    - `GET /qmap/mcp/list-cloud-maps`
    - `GET /qmap/mcp/cloud-status` (debug cloud config visibility from q-assistant)
    - `GET /qmap/mcp/get-cloud-map?map_id=<id>`
    - `POST /qmap/mcp/build-load-cloud-map-action`
    - `POST /qmap/mcp/build-equals-filter-action`
  - Response headers (chat endpoints):
    - `x-q-assistant-request-id` (trace id for helpdesk/audit correlation)
    - `x-q-assistant-chat-id` (stable chat/session id across multiple requests in the same conversation)
  - Request headers (chat endpoints):
    - `x-q-assistant-session-id` (frontend tab/session id used for per-session audit file split)
- Supports provider forwarding for:
  - `openai`, `openrouter`, `ollama`
  - Q-cumber cloud env for MCP helper endpoints:
    - `Q_ASSISTANT_QCUMBER_CLOUD_API_BASE`
    - `Q_ASSISTANT_QCUMBER_CLOUD_TOKEN` (optional)
    - `Q_ASSISTANT_QCUMBER_CLOUD_TIMEOUT`
  - Upstream AI retry env (transient network/5xx/429 hardening):
    - `Q_ASSISTANT_UPSTREAM_RETRY_ATTEMPTS` (default `2`, i.e. 3 total attempts)
    - `Q_ASSISTANT_UPSTREAM_RETRY_BASE_DELAY` (default `1.0` seconds)
    - `Q_ASSISTANT_UPSTREAM_RETRY_MAX_DELAY` (default `8` seconds)
    - `Q_ASSISTANT_UPSTREAM_RETRY_JITTER_RATIO` (default `0.2`, randomized backoff jitter)
    - `Q_ASSISTANT_UPSTREAM_RETRY_TIMEOUT_INCREMENT` (default `5` seconds per retry attempt, extends effective request timeout)
  - Token budget env (prompt window guardrails):
    - `Q_ASSISTANT_TOKEN_BUDGET_ENABLED` (default `true`)
    - `Q_ASSISTANT_TOKEN_BUDGET_CONTEXT_LIMIT` (`0` = infer by model hint)
    - `Q_ASSISTANT_TOKEN_BUDGET_DEFAULT_CONTEXT_LIMIT` (fallback context window)
    - `Q_ASSISTANT_TOKEN_BUDGET_RESERVED_OUTPUT_TOKENS` (prompt budget reserve for completion)
    - `Q_ASSISTANT_TOKEN_BUDGET_WARN_RATIO` (default `0.6`)
    - `Q_ASSISTANT_TOKEN_BUDGET_COMPACT_RATIO` (default `0.75`)
    - `Q_ASSISTANT_TOKEN_BUDGET_HARD_RATIO` (default `0.94`)
    - Guardrail behavior:
      - preserve contiguous `assistant(tool_calls)` + `tool` message chains
      - preserve at least one recent `user` turn after compaction (provider compatibility for function-calling)
      - preserve dataset/provider keys in compacted tool payload summaries
      - keep schema compaction non-aggressive and normalize `required` against `properties`
      - keep active intent explicit via runtime `[OBJECTIVE_ANCHOR]` + `[OBJECTIVE_CRITERIA]` injected before compaction
      - inject runtime step guardrails (`[RUNTIME_GUARDRAIL]` / `[RUNTIME_NEXT_STEP]`) from latest tool outcomes to enforce `waitForQMapDataset -> countQMapRows` after dataset mutations, enforce final `showOnlyQMapLayer` after validated final outputs, and block repeated low-distinct color retries with identical args
      - for centering/zoom objectives, do not allow final “mappa centrata” claims when `fitQMapToDataset` failed or no successful fit evidence exists
      - for repeated `Hard-enforce turn state: discovery step is mandatory` failures, prune failing operational retries and recover via `listQMapDatasets` before further steps
      - frontend post-response layer emits `[executionSummary] {...}` and enforces fail-closed centering claims (`[guardrail] centering_claim_blocked ...`) when tool evidence does not confirm fit success
      - runtime guardrails are selected via weighted rule scoring (`Selected rule <ruleId> (score=<n>)`) instead of fixed first-match branching
  - Backend chain env (model/provider fallback priority):
    - `Q_ASSISTANT_AGENT_CHAIN` (ordered list `provider|model|baseUrl`, first entry is highest priority)
- Scope guardrail for q-map work:
  - Prefer changes under `examples/q-map/*`.
  - Avoid editing root kepler code (`src/*`) unless explicitly requested.
- Styling:
  - AI panel wrapper uses `.qmap-custom-ai-panel` in `src/app.css`.

## AI Troubleshooting
- Panel opens but no chat/config progression:
  - Verify `VITE_QMAP_AI_PROXY_BASE` is set (default `http://localhost:8000/api/q-assistant`).
  - Open browser console/network and confirm `POST /chat/completions` reaches your backend.
- `run-ai-eval` preflight fails but backend is up:
  - Symptom: `preflight /health failed: fetch failed` from Node-based eval scripts.
  - Cause: restricted sandbox blocks Node socket connect (`EPERM`) to `localhost`.
  - Check gateway path quickly with `curl -sS -m 5 -H "Authorization: Bearer <jwt>" http://localhost:8000/api/q-assistant/health`.
  - Run eval/loop commands outside sandbox or with elevated permissions.
- Provider/API errors (401/403/500):
  - Check backend auth/token and upstream provider config in your backend service.
- `AI_RetryError: Failed after 3 attempts. Last error: Service Unavailable`:
  - This is typically an upstream transient failure (429/5xx/timeout) surfaced by the OpenAssistant runtime.
  - Increase q-assistant retries in `examples/q-map/backends/.env`:
    - `Q_ASSISTANT_UPSTREAM_RETRY_ATTEMPTS=4`
    - `Q_ASSISTANT_UPSTREAM_RETRY_BASE_DELAY=1.5`
    - `Q_ASSISTANT_UPSTREAM_RETRY_MAX_DELAY=20`
    - `Q_ASSISTANT_UPSTREAM_RETRY_JITTER_RATIO=0.2`
    - `Q_ASSISTANT_UPSTREAM_RETRY_TIMEOUT_INCREMENT=5`
  - Configure a fallback chain so q-assistant can switch provider/model automatically:
    - `Q_ASSISTANT_AGENT_CHAIN=openrouter|google/gemini-3-flash-preview|https://openrouter.ai/api/v1,openai|gpt-4o-mini|https://api.openai.com/v1,ollama|qwen3-coder:30b|http://host.docker.internal:11434`
  - Restart backend stack after env changes:
    - `cd examples/q-map/backends && docker compose up -d --build q-assistant`
  - For Python source-only changes (q-assistant / q-cumber-backend / q-storage-backend), code is bind-mounted in compose; prefer `docker compose restart <service>` (no rebuild).
  - For Python dependency changes (`pyproject.toml` / `setup.py` — new packages added), a full rebuild is required: `docker compose -f docker-compose.yaml build --no-cache <service>` then `docker compose up -d <service>`; restart alone is not enough and will cause `ModuleNotFoundError` on startup.
- Mock fallback appears instead of real answer:
  - `VITE_QMAP_AI_PROXY_BASE` is missing or empty.

## AI Security Notes
- Do not keep provider API keys in q-map frontend env (`VITE_*`).
- Keep provider secrets server-side in the `q-assistant` backend env (`Q_ASSISTANT_API_KEY`).

## AI Quick Validation Flow
- Start app: `cd examples/q-map && yarn dev`.
- Open AI panel from map control.
- Send a simple prompt.
- Verify response is returned from your `/chat/completions` backend endpoint.
- Verify `requestId` is visible in assistant response prefix and/or response header `x-q-assistant-request-id`.
- Verify audit file is session-scoped: `examples/q-map/backends/logs/q-assistant/chat-audit/session-<sessionId>.jsonl`.
- Verify retry diagnostics are present when enabled: audit events include `upstreamRetryTrace` with per-attempt status/error/sleep timing.
- Verify token diagnostics are present: audit events include `requestPayloadTokenEstimate`, `upstreamUsage`, and `tokenBudget`.
- Verify workflow quality diagnostics are present: audit events include `qualityMetrics` (`postCreateWaitCountOk`, `finalLayerIsolatedAfterCount`, `pendingIsolationAfterCount`, `waitTimeoutCount`, `falseSuccessClaimCount`, `workflowScore`).

## E2E Suite Split
- Playwright configs:
  - `playwright.config.ts` (all specs)
  - `playwright.ux.config.ts` (UX/functional suite)
  - `playwright.assistant.config.ts` (assistant interaction suite, serial + retry)
- UX/functional specs (non-live assistant):
  - `tests/e2e/smoke.spec.ts`
  - `tests/e2e/ux.spec.ts`
  - `tests/e2e/ux-regression.spec.ts`
  - `tests/e2e/tools.spec.ts`
- Assistant interaction specs:
  - `tests/e2e/ai-mode-policy.spec.ts`
- Fixtures:
  - `tests/fixtures/polygon.geojson`
  - `tests/fixtures/population_polygons.geojson`
  - `tests/fixtures/timeseries_points.geojson`
  - `tests/fixtures/adjacent_polygons.geojson`
- Scope:
  - Keep UX/functional tests deterministic (no live LLM dependency).
  - Keep assistant interaction tests isolated in `playwright.assistant.config.ts` (`workers: 1`, `retries: 1`).
- Current `ux.spec.ts` coverage:
  - App loads and map canvas renders.
  - `Add Data To Map` modal opens and tabs are visible.
  - Cloud provider cards are visible in `Cloud storage`.
  - Loading a q-cumber local-assets dataset updates datasets/layers.
  - Layer basic style section is reachable.
  - Layer visibility toggle changes state.
  - Dataset table modal opens and shows columns.
  - Layer reorder via drag and drop changes layer order.
  - GeoJSON visualization check (`_geojson` column + layer type).
  - H3 tessellation from selected polygon feature.
- Current `ux-regression.spec.ts` coverage:
  - q-storage persisted map can be loaded via cloud UI.
  - Cloud error handling paths (404/422/timeout) emit notifications.
  - Filter creation flow and categorical value selection.
  - Quantitative styling controls for numeric geojson fields.
  - Large dataset upload keeps UI responsive.
  - Mobile viewport usability for side panel and Add Data modal.
  - Duplicate dataset-name upload flow remains stable (no crash).
  - Basemap viewport interaction validated via pan + zoom-to-layer.
- Current `tools.spec.ts` coverage:
  - End-to-end validation of q-map tool runner bridge (`__qmapRunTool`).
  - Dataset/geometry tool workflows with deterministic fixture inputs.
  - Success/result contract assertions for map-tool side effects.

## Custom Reducer Extension
- `keplerGl` reducer is extended via `.plugin(...)` in `src/main.tsx`.
- Custom action:
  - `TOGGLE_QMAP_READ_ONLY`
- No UI control is wired by default; dispatch this action from host code when needed.
- Default viewport override (q-map):
  - `minZoom: 3.5` in `src/main.tsx` initial `mapState`.

## Layout/Overflow Guard
- To prevent map overlays (legend, controls) from expanding page bounds:
  - `src/app.css` forces `html, body, #root` to full viewport and `overflow: hidden`.

## Localization (Important)
- `q-map` default locale is set in `src/main.tsx` (`uiState.locale`).
- Local kepler packages are consumed through Vite aliases in `vite.config.ts`:
  - `@kepler.gl/* -> ../../src/*/src`
- No `portal:` resolutions are used in `package.json`.

## Vite/Deck Stability (Important)
- `q-map` uses explicit aliases in `vite.config.ts` for:
  - `@deck.gl/*`
  - `@luma.gl/*`
  - `@math.gl/*`
- Reason: avoid duplicate module instances that can cause deck runtime errors like:
  - `Model needs a program`
  - `this.state.model is undefined`
- If these errors reappear, verify aliases still point to `examples/q-map/node_modules/*`.

## Q-cumber Cloud Backend
- Backend package path:
  - `examples/q-map/backends/q-cumber-backend`
- Run:
  - `cd examples/q-map/backends/q-cumber-backend`
  - `python -m venv .venv`
  - `source .venv/bin/activate`
  - `pip install -e .`
  - `qmap-qcumber-cloud`
- Default backend URL:
  - `http://localhost:3001`
- Required q-map env vars (`examples/q-map/.env.development.local`):
  - `VITE_QCUMBER_CLOUD_API_BASE=http://localhost:3001`
  - `VITE_QCUMBER_CLOUD_DISPLAY_NAME=Q-cumber`
  - `VITE_QCUMBER_CLOUD_MANAGEMENT_URL=http://localhost:3001/maps`
  - `VITE_QCUMBER_CLOUD_TOKEN=` (optional; set only if backend auth enabled)
  - `VITE_QCUMBER_TILESET_BASE=http://localhost:3002` (for Tileset provider preset)
- Backend auth env var:
  - `QCUMBER_BACKEND_TOKEN` (default empty; if set, must match `VITE_QCUMBER_CLOUD_TOKEN`)
  - If empty, backend runs without bearer auth (dev mode).
- PostGIS-only mode (important):
  - q-cumber backend serves datasets only from PostGIS tables declared in provider descriptors (`source.type=postgis`).
  - Remote provider sources (`ckan`, `esri`, `geoapi`, `wfs`, `q-cumber API`) are not used by runtime.
- Provider catalog endpoints:
  - `GET /providers/locales`
  - `GET /providers?locale=it`
  - `GET /providers/{provider_id}`
  - `GET /providers/{provider_id}/datasets`
  - `GET /providers/{provider_id}/datasets/{dataset_id}/help`
- Provider descriptors:
  - Active descriptor under `QCUMBER_PROVIDERS_DIR` (default `./provider-descriptors`):
    - `provider-descriptors/it/local-assets-it.json`
  - Active PostGIS datasets:
    - `qvt.clc_2018`
    - `qvt.kontur_boundaries`
  - Use `GET /providers/{provider_id}/datasets` and `POST /datasets/query` for data access.
  - Dataset catalog/help/query responses include `aiHints` and backend `routing` metadata to keep assistant logic dataset-agnostic for hierarchy/field-role decisions.
  - Query filters support: `eq`, `ne`, `gt`, `gte`, `lt`, `lte`, `in`, `contains`, `startswith`, `endswith`, `is_null`, `not_null`.
- Write/import endpoints are intentionally disabled in read-only mode:
  - `POST /providers/{provider_id}/import` -> `405`
  - `POST /providers/{provider_id}/import-all` -> `405`

## Q-storage Cloud Backend (Per-user)
- Backend package path:
  - `examples/q-map/backends/q-storage-backend`
- Run:
  - `cd examples/q-map/backends/q-storage-backend`
  - `python -m venv .venv`
  - `source .venv/bin/activate`
  - `pip install -e .`
  - `qmap-qstorage-cloud`
- Default backend URL:
  - `http://localhost:3005`
- Required q-map env vars (`examples/q-map/.env.development.local`):
  - `VITE_QSTORAGE_CLOUD_API_BASE=http://localhost:3005`
  - `VITE_QSTORAGE_CLOUD_DISPLAY_NAME=Q-storage`
  - `VITE_QSTORAGE_CLOUD_MANAGEMENT_URL=http://localhost:3005/maps`
  - `VITE_QSTORAGE_CLOUD_TOKEN=` (optional)
- Backend auth/profile env vars:
  - `QSTORAGE_API_TOKEN` (single-token mode)
  - `QSTORAGE_TOKEN_USERS_JSON` (multi-user token map; preferred for per-user isolation)
  - `QSTORAGE_USER_*` (dev fallback profile when auth is disabled)
- Storage layout:
  - maps are isolated per user in `data/users/<user_id>/maps`.

## q-cumber-postgis (PostGIS)
- Package path:
  - `examples/q-map/backends/q-cumber-postgis`
- Stack:
  - PostGIS image (`postgis/postgis:16-3.4`), no gdal
  - Data bootstrap via pgdump restore from `dumps/` directory
- Init scripts:
  - `sql/init/001_extensions.sql` — PostGIS extension + qvt schema
  - `sql/init/010_restore_dumps.sh` — delegates to shared restore script on first init
  - `scripts/restore-dumps.sh` — per-dump idempotent restore (marker files in `PGDATA/.dump-markers/`)
  - `scripts/entrypoint-wrapper.sh` — runs incremental restore on every container start (not just first init)
- Datasets in `qvt` schema:
  - `qvt.clc_2018` — Corine Land Cover Italia
  - `qvt.kontur_boundaries` — confini amministrativi
  - `qvt.opas_stations` — stazioni ISPRA qualità aria
  - `qvt.opas_measurements` — snapshot ultime 48h
  - `qvt.opas_hourly` — serie orarie (partizionata per mese)
- Data sources:
  - imported from local assets:
    - `assets/U2018_CLC2018_V2020_20u1_italia_intersects.geojson`
    - `assets/kontur_boundaries_20230628_with_lv_id_italia_gpkg.json`
- Operational notes:
  - First bootstrap can take time due to large GeoJSON import (`~452MB` CLC file).
  - Martin may restart a few times while Postgres initializes; this is expected.
  - No `cell-lookup` service and no H3/polyfill logic are part of current stack.
  - Adding new pgdump files to `dumps/`: they are automatically restored on next container start (entrypoint-wrapper detects missing markers and restores incrementally).
  - Manual sync for running container: `make -C examples/q-map/backends pgdump-sync`.
  - Full reset: `make -C examples/q-map/backends clean-postgis` then `make up`.

## Runtime Guardrail: Cloud Map Tools
- Cloud map tools (`listQMapCloudMaps`, `loadCloudMapAndWait`, `loadQMapCloudMap`) are pruned at runtime unless the user objective explicitly mentions cloud, saved, or personal maps.
- Intent detection: `_objective_mentions_cloud_or_saved_maps()` in `objective_intent.py`.
- Enforcement: `build_cloud_tools_require_explicit_request_decision()` in `runtime_loop_limit_rules.py`.
- Rationale: prevents territorial/analytical queries from loading saved maps with stale layers, H3 grids, non-zero pitch.

## Workflow Orchestration Architecture

### Design Principles

The q-map AI runtime follows the **"deterministic code for transitions, LLM for reasoning"**
pattern recommended by current agentic AI literature:

- **Anthropic — "Building Effective Agents"** (https://www.anthropic.com/research/building-effective-agents):
  use the simplest architecture that works; prefer deterministic workflows over fully autonomous agents;
  prompt chaining and routing are the most reliable patterns.
- **"Blueprint First, Model Second"** (https://arxiv.org/pdf/2508.02721):
  directed graphs with deterministic and agentic nodes; transitions branch on conditions, not LLM decisions.
- **"Stop Letting the LLM Drive Your State Machine"** (https://voxam.hashnode.dev/stop-letting-llm-drive-voice-agent-state-machine):
  separate probabilistic understanding (LLM) from deterministic routing (code); assume the LLM will
  hallucinate tool calls and design the architecture to handle it.

### Current Implementation

**Frontend (tool-pipeline.ts):**
- Full FIFO serialization via `AsyncMutex` — all tool calls execute one at a time regardless of
  how many the model emits in a batch. This compensates for providers that ignore
  `parallel_tool_calls=false` (e.g. Gemini/OpenRouter).
- Per-tool circuit breaker (max 3 calls per tool per user message) prevents infinite loops on
  local-only tools (countQMapRows, waitForQMapDataset) that bypass backend guardrails.
- Stateless dedup cache for read-only tools; mutation idempotency cache for dataset-creating tools.

**Backend (q-assistant runtime guardrails):**
- **State-machine transitions via `forced_tool_choice_name`:** after discovery completes
  (listProviders + listDatasets both succeeded), the backend forces `tool_choice` to
  `getQCumberDatasetHelp` or `queryQCumberTerritorialUnits`. The model cannot stop after
  discovery — it must proceed with the query phase.
- **Cloud map routing guard:** cloud map tools are pruned unless the user explicitly asks for
  saved/personal maps.
- **Post-create validation:** after dataset-creating tools, the backend forces
  `waitForQMapDataset → countQMapRows` before allowing styling/fit/ranking.
- **Identical-call circuit breaker:** repeated successful tool calls with identical args are
  pruned and the model is forced to use existing evidence.

### Extending the State Machine

To add a new deterministic transition:

1. Add a `build_<state>_force_<next>_decision()` function in `runtime_loop_limit_rules.py`.
   Follow the pattern of `build_post_discovery_force_query_decision`:
   - Check preconditions in `results` (which tools succeeded)
   - Check that the target step hasn't happened yet
   - Return `RuntimeLoopRuleDecision(forced_tool_choice_name=<tool>)` with guidance lines
2. Import and wire it in `runtime_guardrails.py` → `enforce_runtime_tool_loop_limits()`.
3. Add unit tests in `tests/test_runtime_guardrails.py`.

**Implemented transitions:**
- `filter+wait+count → fitQMapToDataset` (`build_post_filter_force_fit_decision`): after superlative
  winner is isolated (rank→filter) and validated (wait+count), forces `fitQMapToDataset` and strips
  all other tools from the request to work around providers that ignore `tool_choice`. Dual enforcement
  with the candidate scoring rule `admin_superlative_isolated_winner_requires_fit` (score=159).

**Candidate transitions to consider:**
- `query → createDatasetWithGeometryArea` (when objective mentions area/superficie/smallest/largest)
- `area → rankQMapDatasetRows` (after area dataset created, force ranking)
- `rank → createDatasetFromFilter` (after ranking, force filter on top-1 result)

Each transition should only fire when the objective clearly requires it. Use
`_objective_targets_admin_units`, `_objective_requires_ranked_output`, and similar intent
detectors in `objective_intent.py` to gate transitions.

### Backend Payload Controls

- `parallel_tool_calls: false` is injected in `openai_chat_payload.py` when tools are present.
  Not reliably supported by all providers — frontend FIFO serialization is the real enforcement.
- `tool_choice: {"type": "function", "function": {"name": "..."}}` forces a specific tool call.
  Applied via `forced_tool_choice_name` in `RuntimeLoopRuleDecision`.
- `tool_choice: "required"` (not yet implemented) would force at least one tool call without
  specifying which. Useful for "the model must do something" transitions.

## Known Pitfalls
- If UI shows raw translation keys:
  - Verify `localeMessages` in `src/main.tsx` contains the custom ids used by q-map components.
  - Verify `vite.config.ts` still aliases `@kepler.gl/localization` to `../../src/localization/src`.
  - Restart dev server after localization changes.
- If `yarn` errors about project/workspaces:
  - Keep `examples/q-map/yarn.lock` present to treat this folder as standalone.
- If cloud sample maps do not change:
  - Seeder is additive by ID; existing saved maps are not overwritten.
  - Remove old sample JSON files in backend `QMAP_DATA_DIR/maps` if you need a clean seed.
- If `My Cloud` does not appear in Cloud Storage UI:
  - This is expected: `custom-cloud` is currently not registered in q-map UI.
  - Use Q-cumber env vars or re-enable `custom-cloud` registration in `src/cloud-providers/index.ts`.
- If cloud load fails with `Failed to create a new dataset due to data verification errors`:
  - Check that `downloadMap` response `format` matches `map.datasets[*].data` shape.
  - `geojson` expects GeoJSON object, `csv` expects CSV string, `row` expects array of row objects, `keplergl` expects saved dataset schema.
- If cloud map import/load crashes UI with Redux error `Actions may not have an undefined "type" property`:
  - Symptom often appears while loading imported sensor datasets from Cloud Storage.
  - Root cause is in `src/reducers/src/provider-state-updaters.ts` error branch for `PARSE_LOAD_MAP_RESPONSE_TASK`:
    returning `exportFileErrorUpdater(...)` (state object) from `.bimap` instead of an action.
  - Required fix in root kepler reducer:
    - use `error => loadCloudMapError({error, provider, onError})`
    - do not return updater state objects inside task `.bimap` action mapping.

## Adding a New Cloud Provider
- Add provider class extending `Provider` from `@kepler.gl/cloud-providers`.
- Implement methods at minimum:
  - `login`, `logout`, `getUser`, `getAccessToken`
  - `uploadMap`, `listMaps`, `downloadMap`, `getManagementUrl`
- Register provider instance in:
  - `src/cloud-providers/index.ts` (`getQMapCloudProviders`)
- Ensure it is passed via:
  - `cloudProviders` prop in `src/main.tsx`
- Backend contract expected by current provider implementation:
  - `GET /me`
  - `GET /maps`
  - `GET /maps/:id`
  - Optional write endpoints for persistence-enabled providers:
    - `POST /maps`
    - `PUT /maps/:id`
- `downloadMap` should return:
  - `{map: {...}, format: 'geojson'|'csv'|'row'|'keplergl'}`
- Keep provider wiring opt-in via env vars so q-map still runs without cloud backend.

## Main Files to Touch
- App bootstrap: `src/main.tsx`
- Theme: `src/theme/q-map-theme.ts`
- Side-panel logo/header override: `src/components/panel-header.jsx`
- Global layout constraints: `src/app.css`
- Cloud provider wiring: `src/cloud-providers/index.ts`
- Custom provider implementation: `src/cloud-providers/custom-cloud-provider.ts`
- iframe integration contract with `q_hive`:
  - outbound message envelope is versioned and fixed:
    - `source: "q-map"`
    - `version: 1`
    - `type in {QMAP_IFRAME_EXPORT, QMAP_IFRAME_CLOUD_EXPORT, QMAP_IFRAME_CANCEL}`
    - `payload` only
  - parent hash params used by q_hive:
    - `action_uuid`
    - `cloud_map_id`
    - `cloud_provider`
    - `export_payload in {subset, perimeter, full}`
  - `QMAP_IFRAME_CLOUD_EXPORT` payload modes:
    - `subset` (default): send cloud ref + `mapInfo`, omit full `map`
    - `perimeter`: subset + `perimeterFeatureCollection` from editor polygons
    - `full`: include full `map`
  - cloud export payload must include:
    - `cloudMap.id`, `cloudMap.provider`
    - `actionUuid` (hash-based when available)
    - `mapInfo` (from `map.info`)
  - parent origin resolution order:
    - `VITE_QMAP_IFRAME_EXPORT_TARGET_ORIGIN`
    - `document.referrer` origin
    - current `window.location.origin`
  - fail-closed reasons exposed by `src/utils/iframe-export.ts`:
    - `missing_map`
    - `missing_perimeter`
    - `not_in_iframe`
    - `missing_parent_window`
    - `missing_target_origin`
  - keep this aligned with `q_hive/static/js/project.js`, which validates both `event.source` and `event.origin`
  - action-lock persistence behavior (when `action_uuid` exists on save):
    - q-map saves with metadata `locked=true`, `lockType=action`, `actionUuid`, `lockSource=q_hive`
    - q-storage blocks delete always, and blocks update unless JWT claim `qh_action_map_write=true`
    - list/load should expose read-only UX for action-locked maps; normal maps remain mutable
