# q-map Q-storage Backend

FastAPI backend package for personal cloud map storage in `examples/q-map`.

## Endpoints

- `GET /health`
- `GET /me`
- `GET /maps`
- `POST /maps`
- `PUT /maps/{map_id}`
- `GET /maps/{map_id}`
- `DELETE /maps/{map_id}`

## Auth modes

- `QSTORAGE_JWT_AUTH_ENABLED=true`: JWT claim mode (HS256). `sub` is used as map owner id and role-based policy is enforced (`QSTORAGE_JWT_READ_ROLES`, `QSTORAGE_JWT_WRITE_ROLES`).
- `QSTORAGE_TOKEN_USERS_JSON` set: multi-user mode by bearer token (each token resolves to a user profile).
- `QSTORAGE_API_TOKEN` set: single shared token mode.
- neither set: dev mode (no auth, default local user).

When JWT mode is enabled it takes precedence over static token modes.

Maps are stored per-user under `data/users/<user_id>/maps`.
In docker-compose (`examples/q-map/backends/docker-compose.yaml`) this directory is bind-mounted to `/data`, so maps are visible on host in `examples/q-map/backends/q-storage-backend/data/users/<user_id>/maps`.

## Action-locked maps

When q-map is used from q_hive iframe action flows, maps can be saved with metadata lock:

- `metadata.lockType = "action"`
- `metadata.locked = true`

Policy:

- `DELETE /maps/{id}` is always blocked (`403`) for action-locked maps.
- `PUT /maps/{id}` is blocked (`403`) unless JWT auth is enabled and token claim `qh_action_map_write=true` is present (the q_hive iframe bootstrap token includes this claim).
- lock metadata is immutable once set on a map (cannot be removed/changed by later updates).
- `GET /maps` marks action-locked items with `readOnly=true` and includes `metadata`.
- `GET /maps/{id}` returns stored `metadata` so frontend can enforce read-only UX.
- maps without `lockType="action"` are unaffected and keep normal update/delete behavior.

## Run locally

```bash
cd examples/q-map/backends/q-storage-backend
python -m venv .venv
source .venv/bin/activate
pip install -e .
qmap-qstorage-cloud
```

Default server URL: `http://localhost:3005`.

## Frontend env wiring (optional)

Add to `examples/q-map/.env.development`:

```bash
VITE_QSTORAGE_CLOUD_API_BASE=http://localhost:3005
VITE_QSTORAGE_CLOUD_DISPLAY_NAME=Q-storage
VITE_QSTORAGE_CLOUD_MANAGEMENT_URL=http://localhost:3005/maps
VITE_QSTORAGE_CLOUD_TOKEN=
```
