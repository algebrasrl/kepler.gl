#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
from pathlib import Path


def _parse_bool(raw: str | None, default: bool) -> bool:
    if raw is None:
        return default
    normalized = str(raw).strip().lower()
    if normalized in {"1", "true", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "no", "n", "off"}:
        return False
    return default


def _parse_csv(raw: str | None, default_csv: str) -> list[str]:
    source = default_csv if raw is None else str(raw)
    values: list[str] = []
    for token in source.split(","):
        item = token.strip()
        if item and item not in values:
            values.append(item)
    return values


def _yaml_single_quoted(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _lua_string(value: str) -> str:
    return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'


def _lua_set(values: list[str]) -> str:
    if not values:
        return "{}"
    rendered = ", ".join(f"[{_lua_string(item)}]=true" for item in values)
    return "{ " + rendered + " }"


def _render_jwt_secrets(
    *,
    primary_issuer: str,
    primary_secret: str,
    secondary_issuer: str,
    secondary_secret: str,
) -> str:
    lines = [
        "jwt_secrets:",
        "  - consumer: qmap-ux",
        f"    key: {_yaml_single_quoted(primary_issuer)}",
        "    algorithm: HS256",
        f"    secret: {_yaml_single_quoted(primary_secret)}",
    ]

    if secondary_issuer and secondary_secret:
        lines.extend(
            [
                "  - consumer: qmap-ux",
                f"    key: {_yaml_single_quoted(secondary_issuer)}",
                "    algorithm: HS256",
                f"    secret: {_yaml_single_quoted(secondary_secret)}",
            ]
        )
    return "\n".join(lines)


def _render_pre_function_plugin(*, allowed_issuers: list[str], allowed_audiences: list[str], require_audience: bool) -> str:
    lua_lines = [
        "local method = kong.request.get_method()",
        'if method == "OPTIONS" then',
        "  return",
        "end",
        "local auth = kong.request.get_header(\"authorization\")",
        "if type(auth) ~= \"string\" then",
        "  return kong.response.exit(401, { message = \"Missing bearer token\" })",
        "end",
        "local token = auth:match(\"^[Bb]earer%s+(.+)$\")",
        "if not token then",
        "  return kong.response.exit(401, { message = \"Missing bearer token\" })",
        "end",
        "local _, payload_b64 = token:match(\"^([^.]+)%.([^.]+)%.([^.]+)$\")",
        "if not payload_b64 then",
        "  return kong.response.exit(401, { message = \"Malformed JWT\" })",
        "end",
        "local function b64url_decode(input)",
        "  if type(input) ~= \"string\" then",
        "    return nil",
        "  end",
        "  local normalized = input:gsub(\"-\", \"+\"):gsub(\"_\", \"/\")",
        "  local mod = #normalized % 4",
        "  if mod == 2 then",
        "    normalized = normalized .. \"==\"",
        "  elseif mod == 3 then",
        "    normalized = normalized .. \"=\"",
        "  elseif mod ~= 0 then",
        "    return nil",
        "  end",
        "  return ngx.decode_base64(normalized)",
        "end",
        "local payload_raw = b64url_decode(payload_b64)",
        "if not payload_raw then",
        "  return kong.response.exit(401, { message = \"Malformed JWT payload\" })",
        "end",
        "local function json_extract_string(raw, key)",
        "  if type(raw) ~= \"string\" or type(key) ~= \"string\" then",
        "    return nil",
        "  end",
        "  local pattern = '\"' .. key .. '\"%s*:%s*\"([^\"]+)\"'",
        "  return raw:match(pattern)",
        "end",
        "local function json_extract_string_array(raw, key)",
        "  if type(raw) ~= \"string\" or type(key) ~= \"string\" then",
        "    return nil",
        "  end",
        "  local pattern = '\"' .. key .. '\"%s*:%s*%[(.-)%]'",
        "  local array_raw = raw:match(pattern)",
        "  if type(array_raw) ~= \"string\" then",
        "    return nil",
        "  end",
        "  local values = {}",
        "  for value in array_raw:gmatch('\"([^\"]+)\"') do",
        "    values[#values + 1] = value",
        "  end",
        "  return values",
        "end",
        f"local allowed_issuers = {_lua_set(allowed_issuers)}",
        f"local allowed_audiences = {_lua_set(allowed_audiences)}",
        f"local require_audience = {str(require_audience).lower()}",
        "local iss = json_extract_string(payload_raw, \"iss\")",
        "if type(iss) ~= \"string\" or not allowed_issuers[iss] then",
        "  return kong.response.exit(401, { message = \"Invalid issuer claim\" })",
        "end",
        "if require_audience then",
        "  local aud_ok = false",
        "  local aud_single = json_extract_string(payload_raw, \"aud\")",
        "  if type(aud_single) == \"string\" then",
        "    aud_ok = allowed_audiences[aud_single] == true",
        "  end",
        "  if not aud_ok then",
        "    local aud_list = json_extract_string_array(payload_raw, \"aud\")",
        "    if type(aud_list) == \"table\" then",
        "      for _, value in ipairs(aud_list) do",
        "        if type(value) == \"string\" and allowed_audiences[value] == true then",
        "          aud_ok = true",
        "          break",
        "        end",
        "      end",
        "    end",
        "  end",
        "  if not aud_ok then",
        "    return kong.response.exit(401, { message = \"Invalid audience claim\" })",
        "  end",
        "end",
    ]

    lines = [
        "  - name: pre-function",
        "    config:",
        "      access:",
        "        - |",
    ]
    lines.extend("          " + line for line in lua_lines)
    return "\n".join(lines)


def _render_service_jwt_plugins() -> str:
    return """      - name: jwt
        config:
          key_claim_name: iss
          run_on_preflight: false
          claims_to_verify:
            - exp
      - name: acl
        config:
          allow:
            - qmap-users
          hide_groups_header: true
      - name: rate-limiting
        config:
          minute: 300
          policy: local
          limit_by: consumer"""


def _render_service_jwt_plugins_cumber() -> str:
    return """      - name: jwt
        config:
          key_claim_name: iss
          run_on_preflight: false
          claims_to_verify:
            - exp
      - name: acl
        config:
          allow:
            - qmap-users
          hide_groups_header: true
      - name: rate-limiting
        config:
          minute: 600
          policy: local
          limit_by: consumer"""


def build_config(
    *,
    primary_issuer: str,
    primary_secret: str,
    secondary_issuer: str,
    secondary_secret: str,
    extra_issuers: list[str],
    allowed_audiences: list[str],
    require_audience: bool,
) -> str:
    active_issuers = [primary_issuer]
    if secondary_issuer and secondary_secret and secondary_issuer not in active_issuers:
        active_issuers.append(secondary_issuer)
    for value in extra_issuers:
        if value not in active_issuers:
            active_issuers.append(value)

    jwt_secrets_block = _render_jwt_secrets(
        primary_issuer=primary_issuer,
        primary_secret=primary_secret,
        secondary_issuer=secondary_issuer,
        secondary_secret=secondary_secret,
    )
    pre_function_plugin = _render_pre_function_plugin(
        allowed_issuers=active_issuers,
        allowed_audiences=allowed_audiences,
        require_audience=require_audience,
    )

    return f"""_format_version: "3.0"
_transform: true

# DB-less Kong blueprint for q-map local integration.
# Generated by kong/scripts/render-kong-config.py

consumers:
  - username: qmap-ux
    custom_id: qmap-ux

{jwt_secrets_block}

acls:
  - consumer: qmap-ux
    group: qmap-users

plugins:
  - name: correlation-id
    config:
      header_name: x-request-id
      generator: uuid
      echo_downstream: true

  - name: cors
    config:
      origins:
        - http://localhost:8081
        - http://127.0.0.1:8081
        - http://local.q-hive.it
        - http://local.q-hive.it:8081
        - https://local.q-hive.it
        - https://local.q-hive.it:8081
      methods:
        - GET
        - POST
        - PUT
        - PATCH
        - DELETE
        - OPTIONS
      headers:
        - Authorization
        - Content-Type
        - x-q-assistant-session-id
        - x-qmap-context
      exposed_headers:
        - x-request-id
        - x-q-assistant-request-id
        - x-q-assistant-chat-id
      credentials: true
      max_age: 3600

{pre_function_plugin}

services:
  - name: q-assistant
    host: q-assistant
    port: 3004
    protocol: http
    routes:
      - name: q-assistant-route
        paths:
          - /api/q-assistant
        strip_path: true
        preserve_host: false
        methods:
          - GET
          - POST
          - PUT
          - PATCH
          - DELETE
          - OPTIONS
    plugins:
{_render_service_jwt_plugins()}

  - name: q-cumber
    host: q-cumber-backend
    port: 3001
    protocol: http
    routes:
      - name: q-cumber-route
        paths:
          - /api/q-cumber
        strip_path: true
        preserve_host: false
        methods:
          - GET
          - POST
          - OPTIONS
    plugins:
{_render_service_jwt_plugins_cumber()}

  - name: q-storage
    host: q-storage-backend
    port: 3005
    protocol: http
    routes:
      - name: q-storage-route
        paths:
          - /api/q-storage
        strip_path: true
        preserve_host: false
        methods:
          - GET
          - POST
          - PUT
          - PATCH
          - DELETE
          - OPTIONS
    plugins:
{_render_service_jwt_plugins()}
"""


def main() -> None:
    parser = argparse.ArgumentParser(description="Render Kong declarative config for q-map.")
    parser.add_argument(
        "--output",
        default=str(Path(__file__).resolve().parents[1] / "kong.yml"),
        help="Output kong.yml path.",
    )
    args = parser.parse_args()

    primary_issuer = os.getenv("QMAP_KONG_JWT_PRIMARY_ISSUER", "qmap-ux").strip() or "qmap-ux"
    primary_secret = os.getenv("QMAP_KONG_JWT_PRIMARY_SECRET", "dev-secret-change-me").strip() or "dev-secret-change-me"
    secondary_issuer = os.getenv("QMAP_KONG_JWT_SECONDARY_ISSUER", "qmap-ux-next").strip()
    secondary_secret = os.getenv("QMAP_KONG_JWT_SECONDARY_SECRET", "").strip()
    extra_issuers = _parse_csv(os.getenv("QMAP_KONG_JWT_EXTRA_ISSUERS"), "")
    allowed_audiences = _parse_csv(os.getenv("QMAP_KONG_JWT_ALLOWED_AUDIENCES"), "q-map")
    require_audience = _parse_bool(os.getenv("QMAP_KONG_JWT_REQUIRE_AUDIENCE"), True)

    rendered = build_config(
        primary_issuer=primary_issuer,
        primary_secret=primary_secret,
        secondary_issuer=secondary_issuer,
        secondary_secret=secondary_secret,
        extra_issuers=extra_issuers,
        allowed_audiences=allowed_audiences,
        require_audience=require_audience,
    )

    output_path = Path(args.output).resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(rendered, encoding="utf-8")
    print(
        "[render-kong-config] wrote "
        f"{output_path} issuers={primary_issuer}"
        + (f",{secondary_issuer}" if secondary_issuer and secondary_secret else "")
        + f" audiences={','.join(allowed_audiences)} requireAudience={str(require_audience).lower()}"
    )


if __name__ == "__main__":
    main()
