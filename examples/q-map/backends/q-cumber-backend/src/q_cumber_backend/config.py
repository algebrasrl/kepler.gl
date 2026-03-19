from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

from q_backends_shared.config_utils import parse_bool, parse_csv_set, parse_origins


@dataclass(frozen=True)
class Settings:
    api_token: str
    user_name: str
    user_email: str
    data_dir: Path
    providers_dir: Path
    cors_origins: list[str]
    ai_hints_cache_ttl_seconds: int
    postgis_dsn: str
    postgis_host: str
    postgis_port: int
    postgis_db: str
    postgis_user: str
    postgis_password: str
    postgis_pool_min_size: int = 1
    postgis_pool_max_size: int = 5
    ckan_api_key: str = ""
    workers: int = 1
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


def load_settings() -> Settings:
    data_dir = Path(os.getenv("QCUMBER_DATA_DIR", "./data")).expanduser().resolve()
    providers_dir = Path(
        os.getenv("QCUMBER_PROVIDERS_DIR", "./provider-descriptors")
    ).expanduser().resolve()
    api_token = os.getenv("QCUMBER_BACKEND_TOKEN", "")
    return Settings(
        api_token=api_token,
        user_name=os.getenv("QCUMBER_USER_NAME", "Q-cumber User"),
        user_email=os.getenv("QCUMBER_USER_EMAIL", "qcumber@example.com"),
        data_dir=data_dir,
        providers_dir=providers_dir,
        cors_origins=parse_origins(os.getenv("QCUMBER_CORS_ORIGINS")),
        ai_hints_cache_ttl_seconds=max(
            60,
            int(os.getenv("QCUMBER_AI_HINTS_CACHE_TTL_SECONDS", "3600"))
        ),
        postgis_dsn=os.getenv("QCUMBER_POSTGIS_DSN", "").strip(),
        postgis_host=os.getenv("QCUMBER_POSTGIS_HOST", "host.docker.internal").strip(),
        postgis_port=max(1, int(os.getenv("QCUMBER_POSTGIS_PORT", "5434"))),
        postgis_db=os.getenv("QCUMBER_POSTGIS_DB", "qvt").strip(),
        postgis_user=os.getenv("QCUMBER_POSTGIS_USER", "qvt").strip(),
        postgis_password=os.getenv("QCUMBER_POSTGIS_PASSWORD", "qvt").strip(),
        postgis_pool_min_size=max(1, int(os.getenv("QCUMBER_POSTGIS_POOL_MIN", "1"))),
        postgis_pool_max_size=max(2, int(os.getenv("QCUMBER_POSTGIS_POOL_MAX", "5"))),
        ckan_api_key=os.getenv("QCUMBER_CKAN_API_KEY", "").strip(),
        workers=max(1, int(os.getenv("QCUMBER_WORKERS", "1"))),
        jwt_auth=JwtAuthSettings(
            enabled=parse_bool(os.getenv("QCUMBER_JWT_AUTH_ENABLED"), default=False),
            hs256_secrets=parse_csv_set(os.getenv("QCUMBER_JWT_HS256_SECRETS")),
            allowed_issuers=parse_csv_set(os.getenv("QCUMBER_JWT_ALLOWED_ISSUERS")),
            allowed_audiences=parse_csv_set(os.getenv("QCUMBER_JWT_ALLOWED_AUDIENCES")),
            require_audience=parse_bool(os.getenv("QCUMBER_JWT_REQUIRE_AUDIENCE"), default=False),
            roles_claim_paths=parse_csv_set(os.getenv("QCUMBER_JWT_ROLES_CLAIM_PATHS"))
            or JwtAuthSettings().roles_claim_paths,
            allowed_subjects=parse_csv_set(os.getenv("QCUMBER_JWT_ALLOWED_SUBJECTS")),
            read_roles=parse_csv_set(os.getenv("QCUMBER_JWT_READ_ROLES")),
        ),
    )
