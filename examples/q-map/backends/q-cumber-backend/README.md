# q-cumber-backend (PostGIS Read-Only)

Read-only backend for `examples/q-map` that serves datasets only via PostGIS tables.

## Scope

This refactor intentionally disables remote provider ingestion and all map write/import endpoints.
Supported datasets are only those declared in `provider-descriptors/*` with `source.type=postgis`.

Legacy provider descriptors were preserved in:
- `examples/provider-descriptors-legacy/`

## Run

```bash
cd examples/q-map/backends/q-cumber-backend
python -m venv .venv
source .venv/bin/activate
pip install -e .
qmap-qcumber-cloud
```

Default URL: `http://localhost:3001`

## Docker Compose

The main q-map compose (`examples/q-map/backends/docker-compose.yaml`) includes a dedicated
PostGIS container (`q-cumber-postgis/`) bootstrapped via pgdump restore.

At first startup, `010_restore_dumps.sh` restores all `.pgdump` files from `/dumps/`.
Data persists on the host volume — subsequent restarts skip the restore.

Per aggiungere nuovi dataset geospaziali: creare un provider descriptor in
`provider-descriptors/it/`, caricare i dati in PostGIS, e fare un pgdump in
`q-cumber-postgis/dumps/`.

## Active datasets

Default provider descriptor:
- `provider-descriptors/it/local-assets-it.json`

Datasets:
- `clc-2018-italia` -> `qvt.clc_2018`
- `kontur-boundaries-italia` -> `qvt.kontur_boundaries`

AI descriptor hints (optional):
- each dataset descriptor can define `ai.fieldHints` (per field)
- each dataset descriptor can also define `ai.profile` (dataset-level workflow/routing hints)
- supported keys include: `description`, `semanticRole`, `unit`, `type`, `filterOps`, `aliases`, `virtual`, `sortable`, `rankable`, `adminLevel`, `example|examples|enumValues`
- these hints are merged with inferred dataset metadata and exposed in:
  - `GET /providers/{provider_id}/datasets` -> `items[].aiHints.fieldCatalog`
  - `GET /providers/{provider_id}/datasets/{dataset_id}/help` -> `aiHints.fieldCatalog`
- dataset-level profile is exposed as `aiHints.aiProfile` (not as a top-level `aiProfile` field), so frontend agents can consume workflow hints without API shape changes.
- routing metadata also exposes ranked `orderByCandidates` and parent-level candidates used by q-map AI query tools; candidates prioritize semantic numeric metrics (e.g. `population`) and deprioritize identifier/geometry fields.
- routing metadata now also exposes explicit query-tool guidance for orchestration:
  - `routing.queryToolHint` (`preferredTool`, `confidence`, `source`, `reason`, `requiresSpatialBbox`, `expectedAdminTypeSupported`, `forbiddenAdminConstraints`)
- geometry sample values in inferred metadata are compacted (`[geojson omitted]`) to keep help payloads smaller.

CLC enrichment:
- rows with `code_18` are enriched with virtual fields `clc_name_en` and `clc_name_it`
- mapping source: `data/reference/clc_code_18_labels.json`
- AI helpers also expose full thematic code support for CLC:
  - `aiHints.fieldCatalog` for `code_18` includes `enumValues` with all available CLC codes
  - `aiHints.aiProfile.thematicCodeHierarchy` includes level taxonomy by code width (L1/L2/L3) and `allCodes`

## Onboarding nuovi provider/dataset

Guida operativa dedicata:
- `examples/q-map/backends/q-cumber-backend/PROVIDER_ONBOARDING.md`

## API

Read endpoints:
- `GET /health`
- `GET /me`
- `GET /maps`
- `GET /maps/{id}`
- `GET /providers/locales`
- `GET /providers?locale=it`
- `GET /providers/{provider_id}`
- `GET /providers/{provider_id}/datasets`
- `GET /providers/{provider_id}/datasets/{dataset_id}/help`
- `POST /datasets/query`
  - supports optional `spatialBbox: [minLon,minLat,maxLon,maxLat]` prefilter (EPSG:4326)
  - executes spatial filtering in PostGIS (`ST_Intersects`)

Write/import endpoints are intentionally disabled (read-only mode).

## Runtime guardrails

At runtime, dataset querying enforces:
- only `source.type` in `postgis|postgres|postgresql`
- every dataset must declare `source.table`
- geometry filtering is always executed against PostGIS geometry column

Any dataset not backed by PostGIS is rejected.

## Environment

See `.env.example`:
- `QCUMBER_BACKEND_TOKEN`
- `QCUMBER_JWT_AUTH_ENABLED`
- `QCUMBER_JWT_HS256_SECRETS`
- `QCUMBER_JWT_ALLOWED_ISSUERS`
- `QCUMBER_JWT_ALLOWED_AUDIENCES`
- `QCUMBER_JWT_REQUIRE_AUDIENCE`
- `QCUMBER_JWT_ROLES_CLAIM_PATHS`
- `QCUMBER_JWT_ALLOWED_SUBJECTS`
- `QCUMBER_JWT_READ_ROLES`
- `QCUMBER_USER_NAME`
- `QCUMBER_USER_EMAIL`
- `QCUMBER_DATA_DIR`
- `QCUMBER_PROVIDERS_DIR`
- `QCUMBER_CORS_ORIGINS`
- `QCUMBER_AI_HINTS_CACHE_TTL_SECONDS`
- `QCUMBER_POSTGIS_DSN` (optional, full DSN)
- `QCUMBER_POSTGIS_HOST`
- `QCUMBER_POSTGIS_PORT`
- `QCUMBER_POSTGIS_DB`
- `QCUMBER_POSTGIS_USER`
- `QCUMBER_POSTGIS_PASSWORD`

When `QCUMBER_JWT_AUTH_ENABLED=true`, JWT claim validation takes precedence over `QCUMBER_BACKEND_TOKEN` and optional role-gating is enforced for read endpoints via `QCUMBER_JWT_READ_ROLES`.
