from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol


@dataclass
class SourceResult:
    """Uniform result returned by every DataSource implementation."""

    dataset_label: str
    fields: list[str]
    rows: list[dict[str, Any]]
    total_matched: int
    columns: list[str] = field(default_factory=list)


class DataSource(Protocol):
    """Protocol that every source backend must satisfy."""

    def query(
        self,
        source_cfg: dict[str, Any],
        *,
        dataset_label: str,
        select_fields: list[str] | None,
        filters: list[Any] | None,
        spatial_bbox: tuple[float, float, float, float] | None,
        order_by: str | None,
        order_direction: str,
        limit: int,
        offset: int,
    ) -> SourceResult:
        """Execute a filtered query and return rows + metadata."""
        ...

    def fetch_columns(self, source_cfg: dict[str, Any]) -> list[str]:
        """Return column names for the underlying table/resource."""
        ...
