#!/usr/bin/env python3
from __future__ import annotations

import argparse
import base64
import hashlib
import hmac
import json
import os
import time


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _json(data: dict) -> bytes:
    return json.dumps(data, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def mint_hs256(payload: dict, secret: str) -> str:
    header = {"alg": "HS256", "typ": "JWT"}
    encoded_header = _b64url(_json(header))
    encoded_payload = _b64url(_json(payload))
    signing_input = f"{encoded_header}.{encoded_payload}".encode("ascii")
    signature = hmac.new(secret.encode("utf-8"), signing_input, hashlib.sha256).digest()
    return f"{encoded_header}.{encoded_payload}.{_b64url(signature)}"


def main() -> None:
    default_issuer = os.getenv("QMAP_KONG_JWT_PRIMARY_ISSUER", "qmap-ux")
    default_secret = os.getenv("QMAP_KONG_JWT_PRIMARY_SECRET", "dev-secret-change-me")
    default_audience = (
        next(
            (
                token.strip()
                for token in str(os.getenv("QMAP_KONG_JWT_ALLOWED_AUDIENCES", "q-map")).split(",")
                if token.strip()
            ),
            "q-map",
        )
    )
    default_roles = str(
        os.getenv("QMAP_DEV_JWT_ROLES", "qmap-reader,qmap-editor,qmap-admin")
    ).strip()

    parser = argparse.ArgumentParser(description="Mint a local HS256 JWT token for q-map Kong gateway tests.")
    parser.add_argument("--iss", default=default_issuer, help="Token issuer; must match rendered kong.yml issuer mapping.")
    parser.add_argument("--sub", default="qmap-local-user", help="Token subject.")
    parser.add_argument(
        "--ttl",
        type=int,
        default=86400,
        help="Token TTL in seconds (default: 86400 = 24h).",
    )
    parser.add_argument(
        "--secret",
        default=default_secret,
        help="HS256 secret; must match rendered kong.yml jwt secret for local tests.",
    )
    parser.add_argument("--aud", default=default_audience, help="Audience claim.")
    parser.add_argument(
        "--roles",
        default=default_roles,
        help=(
            "Comma-separated role claims added as top-level `roles` in the token payload. "
            "Set empty string to omit roles."
        ),
    )
    args = parser.parse_args()

    now = int(time.time())
    roles = [token.strip() for token in str(args.roles).split(",") if token.strip()]
    payload = {
        "iss": args.iss,
        "sub": args.sub,
        "aud": args.aud,
        "iat": now,
        "exp": now + max(60, int(args.ttl)),
    }
    if roles:
        payload["roles"] = roles
    print(mint_hs256(payload, args.secret))


if __name__ == "__main__":
    main()
