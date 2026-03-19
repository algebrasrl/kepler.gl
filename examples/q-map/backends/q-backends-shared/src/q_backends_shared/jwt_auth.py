from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time
from typing import Any


class JwtValidationError(ValueError):
    pass


def _decode_b64url(value: str) -> bytes:
    padding = "=" * (-len(value) % 4)
    return base64.urlsafe_b64decode(f"{value}{padding}".encode("ascii"))


def _decode_json_segment(segment: str, *, label: str) -> dict[str, Any]:
    try:
        decoded = _decode_b64url(segment)
    except Exception as exc:  # pragma: no cover - explicit fast-fail path
        raise JwtValidationError(f"Invalid {label} encoding") from exc
    try:
        parsed = json.loads(decoded)
    except Exception as exc:
        raise JwtValidationError(f"Invalid {label} payload") from exc
    if not isinstance(parsed, dict):
        raise JwtValidationError(f"Invalid {label} payload")
    return parsed


def _extract_audiences(payload: dict[str, Any]) -> tuple[str, ...]:
    raw = payload.get("aud")
    if isinstance(raw, str):
        value = raw.strip()
        return (value,) if value else ()
    if isinstance(raw, list):
        out: list[str] = []
        seen: set[str] = set()
        for entry in raw:
            value = str(entry or "").strip()
            if not value or value in seen:
                continue
            seen.add(value)
            out.append(value)
        return tuple(out)
    return ()


def _validate_hs256_signature(token: str, *, secrets: tuple[str, ...]) -> tuple[dict[str, Any], dict[str, Any]]:
    parts = token.split(".")
    if len(parts) != 3:
        raise JwtValidationError("Invalid bearer token format")

    header_segment, payload_segment, signature_segment = parts
    header = _decode_json_segment(header_segment, label="jwt header")
    payload = _decode_json_segment(payload_segment, label="jwt payload")

    alg = str(header.get("alg") or "").strip()
    if alg != "HS256":
        raise JwtValidationError("Unsupported jwt alg")

    signing_input = f"{header_segment}.{payload_segment}".encode("ascii")
    valid_signature = False
    for secret in secrets:
        secret_value = str(secret or "")
        if not secret_value:
            continue
        digest = hmac.new(secret_value.encode("utf-8"), signing_input, hashlib.sha256).digest()
        expected = base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")
        if hmac.compare_digest(signature_segment, expected):
            valid_signature = True
            break
    if not valid_signature:
        raise JwtValidationError("Invalid jwt signature")
    return header, payload


def decode_and_validate_jwt(
    token: str,
    *,
    hs256_secrets: tuple[str, ...],
    allowed_issuers: tuple[str, ...] = (),
    allowed_audiences: tuple[str, ...] = (),
    require_audience: bool = False,
    allowed_subjects: tuple[str, ...] = (),
) -> dict[str, Any]:
    if not hs256_secrets:
        raise JwtValidationError("JWT auth misconfigured: missing HS256 secrets")
    _, payload = _validate_hs256_signature(token, secrets=hs256_secrets)

    now_ts = int(time.time())
    exp = payload.get("exp")
    if exp is None:
        raise JwtValidationError("Missing exp claim")
    try:
        exp_ts = int(float(exp))
    except Exception as exc:
        raise JwtValidationError("Invalid exp claim") from exc
    if exp_ts <= now_ts:
        raise JwtValidationError("JWT expired")

    nbf = payload.get("nbf")
    if nbf is not None:
        try:
            nbf_ts = int(float(nbf))
        except Exception as exc:
            raise JwtValidationError("Invalid nbf claim") from exc
        if nbf_ts > now_ts:
            raise JwtValidationError("JWT not active yet")

    subject = str(payload.get("sub") or "").strip()
    if not subject:
        raise JwtValidationError("Missing sub claim")
    if allowed_subjects and subject not in allowed_subjects:
        raise JwtValidationError("JWT subject not allowed")

    if allowed_issuers:
        issuer = str(payload.get("iss") or "").strip()
        if issuer not in allowed_issuers:
            raise JwtValidationError("JWT issuer not allowed")

    audiences = _extract_audiences(payload)
    if require_audience and not audiences:
        raise JwtValidationError("Missing aud claim")
    if allowed_audiences:
        if not audiences:
            raise JwtValidationError("Missing aud claim")
        if not set(audiences).intersection(allowed_audiences):
            raise JwtValidationError("JWT audience not allowed")

    return payload


def _read_claim_path(payload: dict[str, Any], path: str) -> Any:
    current: Any = payload
    for part in path.split("."):
        key = part.strip()
        if not key or not isinstance(current, dict):
            return None
        if key not in current:
            return None
        current = current.get(key)
    return current


def extract_roles(payload: dict[str, Any], claim_paths: tuple[str, ...]) -> tuple[str, ...]:
    out: list[str] = []
    seen: set[str] = set()
    for claim_path in claim_paths:
        path = str(claim_path or "").strip()
        if not path:
            continue
        value = _read_claim_path(payload, path)
        if isinstance(value, str):
            role = value.strip()
            if role and role not in seen:
                seen.add(role)
                out.append(role)
            continue
        if not isinstance(value, list):
            continue
        for raw_role in value:
            role = str(raw_role or "").strip()
            if not role or role in seen:
                continue
            seen.add(role)
            out.append(role)
    return tuple(out)
