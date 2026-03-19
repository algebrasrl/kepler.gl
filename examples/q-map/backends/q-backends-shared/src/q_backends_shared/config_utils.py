from __future__ import annotations

DEFAULT_ORIGINS = [
    "http://localhost:8081",
    "http://localhost:8082",
    "http://127.0.0.1:8081",
    "http://127.0.0.1:8082"
]


def parse_origins(raw: str | None, default: list[str] = DEFAULT_ORIGINS) -> list[str]:
    if not raw:
        return default
    return [origin.strip() for origin in raw.split(",") if origin.strip()]


def parse_csv_set(raw: str | None) -> tuple[str, ...]:
    if not raw:
        return ()
    seen: set[str] = set()
    out: list[str] = []
    for token in str(raw).split(","):
        value = token.strip()
        if not value or value in seen:
            continue
        seen.add(value)
        out.append(value)
    return tuple(out)


def parse_bool(raw: str | None, *, default: bool = False) -> bool:
    if raw is None:
        return default
    value = str(raw).strip().lower()
    if not value:
        return default
    return value in {"1", "true", "yes", "on"}
