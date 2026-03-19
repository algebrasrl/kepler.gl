from __future__ import annotations

import json
from pathlib import Path

from .models import MapListItem, StoredMap


class MapStore:
    def __init__(self, data_dir: Path):
        self._root = data_dir
        self._maps_dir = self._root / "maps"
        self._maps_dir.mkdir(parents=True, exist_ok=True)

    def list_maps(self) -> list[MapListItem]:
        items: list[MapListItem] = []
        for file_path in sorted(self._maps_dir.glob("*.json")):
            stored = self._read(file_path)
            items.append(
                MapListItem(
                    id=stored.id,
                    title=stored.title,
                    description=stored.description,
                    imageUrl=stored.thumbnail,
                    updatedAt=stored.updatedAt,
                    privateMap=not stored.isPublic,
                    loadParams={"id": stored.id, "path": f"/maps/{stored.id}"},
                )
            )
        return sorted(items, key=lambda item: item.updatedAt, reverse=True)

    def get_map(self, map_id: str) -> StoredMap | None:
        path = self._path_for(map_id)
        if not path.exists():
            return None
        return self._read(path)

    def _path_for(self, map_id: str) -> Path:
        return self._maps_dir / f"{map_id}.json"

    def _read(self, path: Path) -> StoredMap:
        data = json.loads(path.read_text(encoding="utf-8"))
        return StoredMap(**data)
