from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from fastapi import Depends, FastAPI, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
import uvicorn

from .config import Settings, UserProfile, load_settings, sanitize_user_id
from .jwt_auth import JwtValidationError, decode_and_validate_jwt, extract_roles
from .models import CloudUser, DownloadMapResponse, MapListResponse, SaveMapRequest
from .storage import MapStore

QH_ACTION_MAP_WRITE_CLAIM = "qh_action_map_write"


@dataclass(frozen=True)
class AuthContext:
    user: UserProfile
    roles: tuple[str, ...] = ()
    subject: str = ""
    claims: dict[str, Any] | None = None


def create_app(settings: Settings | None = None) -> FastAPI:
    app_settings = settings or load_settings()
    store = MapStore(app_settings.data_dir)
    auth_scheme = HTTPBearer(auto_error=False)

    app = FastAPI(title="q-map Q-storage cloud backend", version="0.1.0")

    app.add_middleware(
        CORSMiddleware,
        allow_origins=app_settings.cors_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"]
    )

    def _resolve_jwt_context(credentials: HTTPAuthorizationCredentials | None) -> AuthContext:
        if not credentials:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Missing bearer token",
            )
        try:
            claims = decode_and_validate_jwt(
                credentials.credentials,
                hs256_secrets=app_settings.jwt_auth.hs256_secrets,
                allowed_issuers=app_settings.jwt_auth.allowed_issuers,
                allowed_audiences=app_settings.jwt_auth.allowed_audiences,
                require_audience=app_settings.jwt_auth.require_audience,
                allowed_subjects=app_settings.jwt_auth.allowed_subjects,
            )
        except JwtValidationError as exc:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=str(exc)) from exc

        subject = str(claims.get("sub") or "").strip()
        user_id = sanitize_user_id(subject)
        user = UserProfile(
            id=user_id,
            name=str(claims.get("name") or claims.get("preferred_username") or user_id),
            email=str(claims.get("email") or f"{user_id}@example.com"),
            registered_at=str(claims.get("registeredAt") or claims.get("registered_at") or ""),
            country=str(claims.get("country") or ""),
        )
        return AuthContext(
            user=user,
            roles=extract_roles(claims, app_settings.jwt_auth.roles_claim_paths),
            subject=subject,
            claims=claims,
        )

    def _normalize_claim_bool(value: Any) -> bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return value == 1
        if isinstance(value, str):
            return value.strip().lower() in {"1", "true", "yes", "on"}
        return False

    def _is_action_locked_map(metadata: Any) -> bool:
        if not isinstance(metadata, dict):
            return False
        if str(metadata.get("lockType") or "").strip() != "action":
            return False
        if "locked" not in metadata:
            return True
        return bool(metadata.get("locked"))

    def _can_update_action_locked_map(auth: AuthContext) -> bool:
        claims = auth.claims if isinstance(auth.claims, dict) else {}
        return _normalize_claim_bool(claims.get(QH_ACTION_MAP_WRITE_CLAIM))

    def _ensure_roles(auth: AuthContext, required_roles: tuple[str, ...], *, action: str) -> None:
        if not app_settings.jwt_auth.enabled:
            return
        if not required_roles:
            return
        if set(auth.roles).intersection(required_roles):
            return
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Insufficient role for {action}. Required one of: {', '.join(required_roles)}",
        )

    def resolve_auth(
        credentials: HTTPAuthorizationCredentials | None = Depends(auth_scheme)
    ) -> AuthContext:
        if app_settings.jwt_auth.enabled:
            return _resolve_jwt_context(credentials)

        if app_settings.token_users:
            if not credentials:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Missing bearer token"
                )
            matched_user = app_settings.token_users.get(credentials.credentials)
            if not matched_user:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Invalid bearer token"
                )
            return AuthContext(user=matched_user)

        if app_settings.api_token:
            if not credentials:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Missing bearer token"
                )
            if credentials.credentials != app_settings.api_token:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Invalid bearer token"
                )

        # No auth mechanism configured — return default user (matches q-cumber behavior)
        return AuthContext(user=app_settings.default_user)

    def require_read(auth: AuthContext = Depends(resolve_auth)) -> AuthContext:
        _ensure_roles(auth, app_settings.jwt_auth.read_roles, action="read")
        return auth

    def require_write(auth: AuthContext = Depends(resolve_auth)) -> AuthContext:
        _ensure_roles(auth, app_settings.jwt_auth.write_roles, action="write")
        return auth

    @app.get("/health")
    async def health() -> dict:
        return {"ok": True}

    @app.get("/me", response_model=CloudUser)
    async def me(auth: AuthContext = Depends(require_read)) -> CloudUser:
        user = auth.user
        return CloudUser(
            id=user.id,
            name=user.name,
            email=user.email,
            registeredAt=user.registered_at,
            country=user.country
        )

    @app.get("/maps", response_model=MapListResponse)
    async def list_maps(auth: AuthContext = Depends(require_read)) -> MapListResponse:
        user = auth.user
        return MapListResponse(items=store.list_maps(user.id))

    @app.post("/maps")
    async def create_map(payload: SaveMapRequest, auth: AuthContext = Depends(require_write)) -> dict:
        user = auth.user
        stored = store.create_map(user.id, payload)
        return {
            "id": stored.id,
            "title": stored.title,
            "description": stored.description,
            "loadParams": {"id": stored.id, "path": f"/maps/{stored.id}"},
            "info": {"id": stored.id},
            "metadata": stored.metadata,
        }

    @app.put("/maps/{map_id}")
    async def update_map(
        map_id: str,
        payload: SaveMapRequest,
        auth: AuthContext = Depends(require_write)
    ) -> dict:
        user = auth.user
        current = store.get_map(user.id, map_id)
        if not current:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Map not found")
        if (
            app_settings.jwt_auth.enabled
            and _is_action_locked_map(current.metadata)
            and not _can_update_action_locked_map(auth)
        ):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Action-locked map is read-only outside q_hive iframe sessions.",
            )

        stored = store.update_map(user.id, map_id, payload)
        if not stored:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Map not found")

        return {
            "id": stored.id,
            "title": stored.title,
            "description": stored.description,
            "loadParams": {"id": stored.id, "path": f"/maps/{stored.id}"},
            "info": {"id": stored.id},
            "metadata": stored.metadata,
        }

    @app.get("/maps/{map_id}", response_model=DownloadMapResponse)
    async def download_map(
        map_id: str,
        auth: AuthContext = Depends(require_read)
    ) -> DownloadMapResponse:
        user = auth.user
        stored = store.get_map(user.id, map_id)
        if not stored:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Map not found")

        return DownloadMapResponse(
            id=stored.id,
            map=stored.map,
            format=stored.format,
            metadata=stored.metadata,
        )

    @app.delete("/maps/{map_id}")
    async def delete_map(
        map_id: str,
        auth: AuthContext = Depends(require_write)
    ) -> dict:
        user = auth.user
        current = store.get_map(user.id, map_id)
        if not current:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Map not found")
        if _is_action_locked_map(current.metadata):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Action-locked map cannot be deleted.",
            )

        deleted = store.delete_map(user.id, map_id)
        if not deleted:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Map not found")
        return {"id": map_id, "deleted": True}

    return app


app = create_app()


def run() -> None:
    settings = load_settings()
    uvicorn.run("q_storage_backend.main:app", host="0.0.0.0", port=settings.port, reload=True)


if __name__ == "__main__":
    run()
