# Kong Gateway Blueprint (q-map)

This folder contains the default Kong DB-less gateway layer for q-map backends.

## What it does

- Publishes a single edge endpoint on `http://localhost:8000`.
- Protects backend routes with JWT validation (`jwt` plugin).
- Applies ACL (`qmap-users`) and rate limits per consumer.
- Keeps backend services on internal Docker network names:
  - `q-assistant:3004`
  - `q-cumber-backend:3001`
  - `q-storage-backend:3005`

## Routes

- `http://localhost:8000/api/q-assistant/*` -> `q-assistant`
- `http://localhost:8000/api/q-cumber/*` -> `q-cumber-backend`
- `http://localhost:8000/api/q-storage/*` -> `q-storage-backend`

## Local quick start

From `examples/q-map/backends`:

```bash
make up
```

`make up` now renders `kong/kong.yml` from env (`kong/scripts/render-kong-config.py`) with:
- primary/secondary HS256 issuer+secret entries (rotation-ready),
- strict issuer allowlist checks,
- strict audience checks (configurable),
- sandbox-safe pre-auth claim extraction in `pre-function` (no Lua `require` in untrusted sandbox).

Mint a dev token (HS256):

```bash
python3 kong/scripts/mint-dev-jwt.py
```

Default token duration is 24 hours (`--ttl 86400`).
Use `--ttl <seconds>` to override, for example:

```bash
python3 kong/scripts/mint-dev-jwt.py --ttl 3600
```

Use it:

```bash
TOKEN="<paste-token>"
curl -sS -H "Authorization: Bearer $TOKEN" \
  http://localhost:8000/api/q-assistant/health
```

## Frontend switch

Set q-map frontend AI base URL to gateway route:

```bash
VITE_QMAP_AI_PROXY_BASE=http://localhost:8000/api/q-assistant
```

Enable JWT propagation from UX to Kong (one of these options):

```bash
# static dev token
QMAP_AUTH_RUNTIME_TOKEN=<jwt>

# or runtime token in storage (default key lookup includes qmap_gateway_jwt)
localStorage.setItem("qmap_gateway_jwt", "<jwt>")
```

Note: q-map frontend forwards JWT-like bearer tokens only by default.  
If you intentionally use opaque bearer tokens, set `VITE_QMAP_AUTH_ALLOW_OPAQUE_BEARER=true`.

## Important notes

- Do not commit real JWT secrets; use env-driven render vars (`QMAP_KONG_JWT_PRIMARY_*`, `QMAP_KONG_JWT_SECONDARY_*`).
- Rotation flow: set secondary issuer/secret, deploy, switch token issuer to secondary, then rotate previous primary.
- Audience enforcement is enabled by default (`QMAP_KONG_JWT_REQUIRE_AUDIENCE=true`).
- Keep `untrusted_lua` sandboxed; avoid `KONG_UNTRUSTED_LUA=on` in production profiles.
- For full OIDC/JWKS trust chains, use enterprise OIDC plugin or equivalent verified plugin chain.
- UX bearer propagation is now wired for assistant chat/MCP/profile/cloud requests; backend claim-based authorization mapping remains follow-up hardening in TODO Step K3.
