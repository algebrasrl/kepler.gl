from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path

from q_backends_shared.config_utils import parse_bool, parse_csv_set, parse_origins


@dataclass(frozen=True)
class UserProfile:
    id: str
    name: str
    email: str
    registered_at: str
    country: str


@dataclass(frozen=True)
class Settings:
    api_token: str
    default_user: UserProfile
    token_users: dict[str, UserProfile]
    data_dir: Path
    cors_origins: list[str]
    port: int
    allow_insecure_default_user: bool = False
    jwt_auth: "JwtAuthSettings" = field(default_factory=lambda: JwtAuthSettings())


@dataclass(frozen=True)
class JwtAuthSettings:
    enabled: bool = False
    hs256_secrets: tuple[str, ...] = ()
    allowed_issuers: tuple[str, ...] = ()
    allowed_audiences: tuple[str, ...] = ()
    require_audience: bool = False
    roles_claim_paths: tuple[str, ...] = ("roles", "realm_access.roles", "resource_access.q-map.roles")
    allowed_subjects: tuple[str, ...] = ()
    read_roles: tuple[str, ...] = ()
    write_roles: tuple[str, ...] = ("qmap-editor", "qmap-admin")


def sanitize_user_id(value: str) -> str:
    cleaned = "".join(ch.lower() if ch.isalnum() else "-" for ch in value).strip("-")
    return cleaned or "user"


def _build_user_profile(raw: dict[str, object], fallback_id: str = "user") -> UserProfile:
    user_id = sanitize_user_id(str(raw.get("id") or raw.get("email") or fallback_id))
    return UserProfile(
        id=user_id,
        name=str(raw.get("name") or "Q-storage User"),
        email=str(raw.get("email") or f"{user_id}@example.com"),
        registered_at=str(raw.get("registeredAt") or raw.get("registrationDate") or ""),
        country=str(raw.get("country") or "")
    )


def parse_token_users(raw: str | None) -> dict[str, UserProfile]:
    if not raw:
        return {}

    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    token_users: dict[str, UserProfile] = {}

    if isinstance(parsed, dict):
        # Supports {"tokenA": {"name": "..."}}
        for token, user in parsed.items():
            if not isinstance(token, str) or not token.strip():
                continue
            if not isinstance(user, dict):
                continue
            token_users[token.strip()] = _build_user_profile(user, fallback_id=token)
        return token_users

    if isinstance(parsed, list):
        # Supports [{"token": "...", "name": "..."}]
        for entry in parsed:
            if not isinstance(entry, dict):
                continue
            token = str(entry.get("token") or "").strip()
            if not token:
                continue
            token_users[token] = _build_user_profile(entry, fallback_id=token)

    return token_users


def load_settings() -> Settings:
    data_dir = Path(os.getenv("QSTORAGE_DATA_DIR", "./data")).expanduser().resolve()
    default_user = UserProfile(
        id=sanitize_user_id(os.getenv("QSTORAGE_USER_ID", "local-user")),
        name=os.getenv("QSTORAGE_USER_NAME", "Q-storage User"),
        email=os.getenv("QSTORAGE_USER_EMAIL", "qstorage@example.com"),
        registered_at=os.getenv("QSTORAGE_USER_REGISTERED_AT", ""),
        country=os.getenv("QSTORAGE_USER_COUNTRY", "")
    )
    return Settings(
        api_token=os.getenv("QSTORAGE_API_TOKEN", ""),
        default_user=default_user,
        token_users=parse_token_users(os.getenv("QSTORAGE_TOKEN_USERS_JSON")),
        data_dir=data_dir,
        cors_origins=parse_origins(os.getenv("QSTORAGE_CORS_ORIGINS")),
        port=int(os.getenv("QSTORAGE_PORT", "3005")),
        allow_insecure_default_user=parse_bool(
            os.getenv("QSTORAGE_ALLOW_INSECURE_DEFAULT_USER"), default=False
        ),
        jwt_auth=JwtAuthSettings(
            enabled=parse_bool(os.getenv("QSTORAGE_JWT_AUTH_ENABLED"), default=False),
            hs256_secrets=parse_csv_set(os.getenv("QSTORAGE_JWT_HS256_SECRETS")),
            allowed_issuers=parse_csv_set(os.getenv("QSTORAGE_JWT_ALLOWED_ISSUERS")),
            allowed_audiences=parse_csv_set(os.getenv("QSTORAGE_JWT_ALLOWED_AUDIENCES")),
            require_audience=parse_bool(os.getenv("QSTORAGE_JWT_REQUIRE_AUDIENCE"), default=False),
            roles_claim_paths=parse_csv_set(os.getenv("QSTORAGE_JWT_ROLES_CLAIM_PATHS"))
            or JwtAuthSettings().roles_claim_paths,
            allowed_subjects=parse_csv_set(os.getenv("QSTORAGE_JWT_ALLOWED_SUBJECTS")),
            read_roles=parse_csv_set(os.getenv("QSTORAGE_JWT_READ_ROLES")),
            write_roles=parse_csv_set(os.getenv("QSTORAGE_JWT_WRITE_ROLES"))
            or JwtAuthSettings().write_roles,
        ),
    )
