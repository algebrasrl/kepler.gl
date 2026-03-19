from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


class CloudUser(BaseModel):
    name: str
    email: str


class SaveMapRequest(BaseModel):
    title: str = Field(default="Untitled map")
    description: str = Field(default="")
    isPublic: bool = False
    map: dict
    format: str = "keplergl"
    thumbnail: str | None = None


class StoredMap(BaseModel):
    id: str
    title: str
    description: str
    isPublic: bool
    map: dict
    format: str = "keplergl"
    thumbnail: str | None = None
    createdAt: int
    updatedAt: int


class MapListItem(BaseModel):
    id: str
    title: str
    description: str
    imageUrl: str | None = None
    updatedAt: int
    privateMap: bool
    loadParams: dict


class MapListResponse(BaseModel):
    items: list[MapListItem]


class DownloadMapResponse(BaseModel):
    id: str
    map: dict
    format: str = "keplergl"


class ProviderCatalogItem(BaseModel):
    id: str
    name: str
    locale: str
    category: str = "environmental"
    organizationType: str = "agency"
    region: str | None = None
    country: str | None = None
    portalUrl: str | None = None
    apiType: str | None = None
    apiBaseUrl: str | None = None
    capabilities: list[str] = []
    formats: list[str] = []
    tags: list[str] = []
    notes: str | None = None


class ProviderListResponse(BaseModel):
    items: list[ProviderCatalogItem]


class ProviderLocalesResponse(BaseModel):
    locales: list[str]


class ProviderDatasetItem(BaseModel):
    id: str
    providerId: str
    name: str
    description: str | None = None
    url: str | None = None
    source: dict[str, Any] | None = None
    format: str | None = None
    tags: list[str] = []
    ai: dict[str, Any] | None = None
    aiHints: dict[str, Any] | None = None
    routing: dict[str, Any] | None = None


class ProviderDatasetListResponse(BaseModel):
    providerId: str
    items: list[ProviderDatasetItem]


class DatasetHelpResponse(BaseModel):
    providerId: str
    datasetId: str
    datasetName: str
    aiHints: dict[str, Any] | None = None
    routing: dict[str, Any] | None = None



class DatasetQueryFilter(BaseModel):
    field: str
    op: Literal[
        "eq",
        "ne",
        "gt",
        "gte",
        "lt",
        "lte",
        "in",
        "contains",
        "startswith",
        "endswith",
        "is_null",
        "not_null",
    ] = "eq"
    value: Any = None
    values: list[Any] | None = None


class DatasetQueryRequest(BaseModel):
    providerId: str
    datasetId: str
    select: list[str] | None = None
    filters: list[DatasetQueryFilter] | None = None
    # [minLon, minLat, maxLon, maxLat] in EPSG:4326
    spatialBbox: list[float] | None = Field(default=None, min_length=4, max_length=4)
    orderBy: str | None = None
    orderDirection: Literal["asc", "desc"] = "asc"
    limit: int = Field(default=1000, ge=1, le=100000)
    offset: int = Field(default=0, ge=0)


class KeplerDatasetField(BaseModel):
    name: str
    type: str


class KeplerDatasetData(BaseModel):
    fields: list[KeplerDatasetField]
    rows: list[list[Any]]


class KeplerDatasetInfo(BaseModel):
    id: str
    label: str


class KeplerDatasetPayload(BaseModel):
    info: KeplerDatasetInfo
    data: KeplerDatasetData


class DatasetQueryResponse(BaseModel):
    providerId: str
    datasetId: str
    totalMatched: int
    returned: int
    fields: list[str]
    rows: list[dict[str, Any]]
    dataset: KeplerDatasetPayload
    aiHints: dict[str, Any] | None = None
    routing: dict[str, Any] | None = None
