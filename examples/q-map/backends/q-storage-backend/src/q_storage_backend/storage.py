from __future__ import annotations

import json
import time
import uuid
from pathlib import Path

from .models import MapListItem, SaveMapRequest, StoredMap


def _normalize_metadata(value) -> dict:
    if not isinstance(value, dict):
        return {}
    return dict(value)


def _is_action_locked_metadata(value) -> bool:
    metadata = _normalize_metadata(value)
    if str(metadata.get("lockType") or "").strip() != "action":
        return False
    locked = metadata.get("locked")
    if locked is None:
        return True
    return bool(locked)


class MapStore:
    def __init__(self, data_dir: Path):
        self._root = data_dir
        self._users_dir = self._root / "users"
        self._users_dir.mkdir(parents=True, exist_ok=True)

    def list_maps(self, user_id: str) -> list[MapListItem]:
        items: list[MapListItem] = []
        maps_dir = self._maps_dir_for(user_id)
        for file_path in sorted(maps_dir.glob("*.json")):
            stored = self._read(file_path)
            items.append(
                MapListItem(
                    id=stored.id,
                    title=stored.title,
                    description=stored.description,
                    imageUrl=stored.thumbnail,
                    updatedAt=stored.updatedAt,
                    privateMap=not stored.isPublic,
                    readOnly=_is_action_locked_metadata(stored.metadata),
                    metadata=_normalize_metadata(stored.metadata),
                    loadParams={"id": stored.id, "path": f"/maps/{stored.id}"}
                )
            )

        return sorted(items, key=lambda item: item.updatedAt, reverse=True)

    def create_map(self, user_id: str, payload: SaveMapRequest) -> StoredMap:
        map_id = str(uuid.uuid4())
        now = int(time.time() * 1000)
        stored = StoredMap(
            id=map_id,
            title=payload.title,
            description=payload.description,
            isPublic=payload.isPublic,
            map=payload.map,
            format=payload.format,
            thumbnail=payload.thumbnail,
            metadata=_normalize_metadata(payload.metadata),
            createdAt=now,
            updatedAt=now
        )
        self._write(user_id, stored)
        return stored

    def update_map(self, user_id: str, map_id: str, payload: SaveMapRequest) -> StoredMap | None:
        current = self.get_map(user_id, map_id)
        if not current:
            return None

        next_metadata = (
            _normalize_metadata(payload.metadata)
            if payload.metadata is not None
            else _normalize_metadata(current.metadata)
        )
        if _is_action_locked_metadata(current.metadata):
            # Lock semantics are immutable once assigned.
            next_metadata = _normalize_metadata(current.metadata)

        updated = StoredMap(
            id=current.id,
            title=payload.title,
            description=payload.description,
            isPublic=payload.isPublic,
            map=payload.map,
            format=payload.format,
            thumbnail=payload.thumbnail,
            metadata=next_metadata,
            createdAt=current.createdAt,
            updatedAt=int(time.time() * 1000)
        )
        self._write(user_id, updated)
        return updated

    def get_map(self, user_id: str, map_id: str) -> StoredMap | None:
        path = self._path_for(user_id, map_id)
        if not path.exists():
            return None
        return self._read(path)

    def delete_map(self, user_id: str, map_id: str) -> bool:
        path = self._path_for(user_id, map_id)
        if not path.exists():
            return False
        path.unlink()
        return True

    def _maps_dir_for(self, user_id: str) -> Path:
        maps_dir = self._users_dir / user_id / "maps"
        maps_dir.mkdir(parents=True, exist_ok=True)
        return maps_dir

    def _path_for(self, user_id: str, map_id: str) -> Path:
        return self._maps_dir_for(user_id) / f"{map_id}.json"

    def _write(self, user_id: str, stored: StoredMap) -> None:
        path = self._path_for(user_id, stored.id)
        path.write_text(stored.model_dump_json(indent=2), encoding="utf-8")

    def _read(self, path: Path) -> StoredMap:
        data = json.loads(path.read_text(encoding="utf-8"))
        return StoredMap(**data)
