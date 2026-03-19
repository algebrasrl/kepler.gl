from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class CloudUser(BaseModel):
    id: str
    name: str
    email: str
    registeredAt: str = ""
    country: str = ""


class SaveMapRequest(BaseModel):
    title: str = Field(default="Untitled map")
    description: str = Field(default="")
    isPublic: bool = False
    map: dict
    format: str = "keplergl"
    thumbnail: str | None = None
    metadata: dict[str, Any] | None = None


class StoredMap(BaseModel):
    id: str
    title: str
    description: str
    isPublic: bool
    map: dict
    format: str = "keplergl"
    thumbnail: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    createdAt: int
    updatedAt: int


class MapListItem(BaseModel):
    id: str
    title: str
    description: str
    imageUrl: str | None = None
    updatedAt: int
    privateMap: bool
    readOnly: bool = False
    metadata: dict[str, Any] = Field(default_factory=dict)
    loadParams: dict


class MapListResponse(BaseModel):
    items: list[MapListItem]


class DownloadMapResponse(BaseModel):
    id: str
    map: dict
    format: str = "keplergl"
    metadata: dict[str, Any] = Field(default_factory=dict)
