#!/usr/bin/env python3
"""Build a denormalized GeoJSON dataset of stressor entities.

Usage:
  python3 denormalize_stressors.py
  python3 denormalize_stressors.py --output stressors_operations_flat.geojson
  python3 denormalize_stressors.py --stressors-output stressors_entities_all.geojson
  python3 denormalize_stressors.py --only-characterized
  python3 denormalize_stressors.py --no-include-operation-geometry
"""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Denormalize q-cumber stressor JSON datasets into GeoJSON."
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=Path(__file__).resolve().parent,
        help="Folder containing qcumber_* JSON files (default: script folder).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path(__file__).resolve().parent / "stressors_operations_flat.geojson",
        help="Output GeoJSON for flat operations (one feature per operation).",
    )
    parser.add_argument(
        "--stressors-output",
        type=Path,
        default=Path(__file__).resolve().parent / "stressors_entities_denormalized.geojson",
        help="Output GeoJSON for denormalized stressor entities.",
    )
    parser.add_argument(
        "--include-operation-geometry",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Include operation geometry inside operation details (default: true).",
    )
    parser.add_argument(
        "--only-characterized",
        action="store_true",
        help="Emit only stressors present in qcumber_characterized_stressors.json.",
    )
    parser.add_argument(
        "--operations-include-null-geometry",
        action="store_true",
        help="Include operations with null geometry in flat operations output.",
    )
    return parser.parse_args()


def load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def safe_int(value: Any) -> Optional[int]:
    try:
        if value is None:
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def first_not_none(values: Iterable[Any]) -> Any:
    for value in values:
        if value is not None:
            return value
    return None


def canonical_operation_category(
    category: Optional[Dict[str, Any]], categories_by_cod: Dict[int, Dict[str, Any]]
) -> Optional[Dict[str, Any]]:
    if not isinstance(category, dict):
        return None
    cod = safe_int(category.get("cod"))
    canonical = deepcopy(categories_by_cod.get(cod, {})) if cod is not None else {}
    merged = deepcopy(category)
    for key, value in canonical.items():
        merged.setdefault(key, value)
    return merged


def canonical_indicator(
    indicator: Optional[Dict[str, Any]], indicators_by_cod: Dict[int, Dict[str, Any]]
) -> Optional[Dict[str, Any]]:
    if not isinstance(indicator, dict):
        return None
    cod = safe_int(indicator.get("cod"))
    canonical = deepcopy(indicators_by_cod.get(cod, {})) if cod is not None else {}
    merged = deepcopy(indicator)
    for key, value in canonical.items():
        merged.setdefault(key, value)
    return merged


def denormalize_operation(
    operation: Dict[str, Any],
    categories_by_cod: Dict[int, Dict[str, Any]],
    indicators_by_cod: Dict[int, Dict[str, Any]],
    include_operation_geometry: bool,
) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "id": operation.get("id"),
        "name": operation.get("name"),
        "is_generic": operation.get("is_generic"),
        "url": operation.get("url"),
        "category": canonical_operation_category(operation.get("category"), categories_by_cod),
    }

    drivers: List[Dict[str, Any]] = []
    for driver in operation.get("drivers") or []:
        if not isinstance(driver, dict):
            continue
        item = deepcopy(driver)
        item["indicator"] = canonical_indicator(driver.get("indicator"), indicators_by_cod)
        drivers.append(item)
    out["drivers"] = drivers

    emission_factors: List[Dict[str, Any]] = []
    for ef in operation.get("emission_factors") or []:
        if not isinstance(ef, dict):
            continue
        item = deepcopy(ef)
        item["indicator"] = canonical_indicator(ef.get("indicator"), indicators_by_cod)
        emission_factors.append(item)
    out["emission_factors"] = emission_factors

    if include_operation_geometry:
        out["geometry"] = deepcopy(operation.get("geometry"))

    return out


def unique_sorted_dicts(items: Iterable[Dict[str, Any]], sort_key: str) -> List[Dict[str, Any]]:
    indexed: Dict[Any, Dict[str, Any]] = {}
    for item in items:
        key = item.get(sort_key)
        if key is not None and key not in indexed:
            indexed[key] = item
    return [indexed[key] for key in sorted(indexed)]


def pick_feature_geometry(
    characterized: Optional[Dict[str, Any]], operations: List[Dict[str, Any]]
) -> Optional[Dict[str, Any]]:
    if characterized and characterized.get("geometry") is not None:
        return deepcopy(characterized.get("geometry"))
    for operation in operations:
        geometry = operation.get("geometry")
        if geometry is not None:
            return deepcopy(geometry)
    return None


def build_feature(
    stressor_id: int,
    characterized: Optional[Dict[str, Any]],
    stressor_stub: Optional[Dict[str, Any]],
    operations: List[Dict[str, Any]],
    categories_by_cod: Dict[int, Dict[str, Any]],
    indicators_by_cod: Dict[int, Dict[str, Any]],
    include_operation_geometry: bool,
) -> Dict[str, Any]:
    base = characterized or (stressor_stub if isinstance(stressor_stub, dict) else {})
    base_category = base.get("category") if isinstance(base, dict) else None

    operations_denorm = [
        denormalize_operation(op, categories_by_cod, indicators_by_cod, include_operation_geometry)
        for op in sorted(operations, key=lambda op: (op.get("id") is None, op.get("id")))
    ]

    op_categories = unique_sorted_dicts(
        (
            denorm["category"]
            for denorm in operations_denorm
            if isinstance(denorm.get("category"), dict)
        ),
        "cod",
    )

    driver_indicators = unique_sorted_dicts(
        (
            driver.get("indicator")
            for denorm in operations_denorm
            for driver in denorm.get("drivers", [])
            if isinstance(driver.get("indicator"), dict)
        ),
        "cod",
    )

    emission_indicators = unique_sorted_dicts(
        (
            ef.get("indicator")
            for denorm in operations_denorm
            for ef in denorm.get("emission_factors", [])
            if isinstance(ef.get("indicator"), dict)
        ),
        "cod",
    )

    properties: Dict[str, Any] = {
        "stressor_id": stressor_id,
        "stressor_name": base.get("name"),
        "stressor_permalink": base.get("permalink"),
        "stressor_url": base.get("url"),
        "stressor_category": deepcopy(base_category) if isinstance(base_category, dict) else None,
        "is_characterized": first_not_none([base.get("is_characterized"), characterized is not None]),
        "is_specific": base.get("is_specific"),
        "is_generic": base.get("is_generic"),
        "is_typological": base.get("is_typological"),
        "dossiers_count": base.get("dossiers_count"),
        "created_at": base.get("created_at"),
        "updated_at": base.get("updated_at"),
        "source_id": base.get("source_id"),
        "source_origin": base.get("source_origin"),
        "source_uri": base.get("source_uri"),
        "source_rev": base.get("source_rev"),
        "source_datetime": base.get("source_datetime"),
        "meta": deepcopy(base.get("meta")),
        "barycenter": deepcopy(base.get("barycenter")),
        "bounding_box": deepcopy(base.get("bounding_box")),
        "partition": deepcopy(base.get("partition")),
        "typological_operation_types": deepcopy(base.get("typological_operation_types")),
        "typological_operations_with_average_drivers": deepcopy(
            base.get("typological_operations_with_average_drivers")
        ),
        "operations_count": len(operations_denorm),
        "operations_count_specific": sum(1 for op in operations_denorm if not op.get("is_generic")),
        "operations_count_generic": sum(1 for op in operations_denorm if op.get("is_generic")),
        "operation_categories": op_categories,
        "driver_indicators": driver_indicators,
        "emission_indicators": emission_indicators,
        "operations": operations_denorm,
    }

    feature = {
        "type": "Feature",
        "geometry": pick_feature_geometry(characterized, operations),
        "properties": properties,
    }
    return feature


def build_operation_feature(
    operation: Dict[str, Any],
    stressor_base: Dict[str, Any],
    categories_by_cod: Dict[int, Dict[str, Any]],
    indicators_by_cod: Dict[int, Dict[str, Any]],
) -> Dict[str, Any]:
    denorm_operation = denormalize_operation(
        operation=operation,
        categories_by_cod=categories_by_cod,
        indicators_by_cod=indicators_by_cod,
        include_operation_geometry=False,
    )
    stressor_category = (
        deepcopy(stressor_base.get("category")) if isinstance(stressor_base.get("category"), dict) else None
    )

    properties = {
        "operation_id": denorm_operation.get("id"),
        "operation_name": denorm_operation.get("name"),
        "operation_url": denorm_operation.get("url"),
        "operation_is_generic": denorm_operation.get("is_generic"),
        "operation_category": denorm_operation.get("category"),
        "drivers": denorm_operation.get("drivers"),
        "emission_factors": denorm_operation.get("emission_factors"),
        "stressor_id": stressor_base.get("id"),
        "stressor_name": stressor_base.get("name"),
        "stressor_url": stressor_base.get("url"),
        "stressor_permalink": stressor_base.get("permalink"),
        "stressor_category": stressor_category,
        "stressor_is_characterized": stressor_base.get("is_characterized"),
        "stressor_is_specific": stressor_base.get("is_specific"),
        "stressor_is_generic": stressor_base.get("is_generic"),
        "stressor_is_typological": stressor_base.get("is_typological"),
        "stressor_dossiers_count": stressor_base.get("dossiers_count"),
    }

    return {
        "type": "Feature",
        "geometry": deepcopy(operation.get("geometry")),
        "properties": properties,
    }


def run(args: argparse.Namespace) -> None:
    input_dir = args.input_dir.resolve()
    output_path = args.output.resolve()
    stressors_output_path = args.stressors_output.resolve()

    characterized_path = input_dir / "qcumber_characterized_stressors.json"
    generic_ops_path = input_dir / "qcumber_generic_operations.json"
    specific_ops_path = input_dir / "qcumber_specific_operations.json"
    op_categories_path = input_dir / "qcumber_operation_categories.json"
    indicators_path = input_dir / "qcumber_indicators.json"

    characterized_rows: List[Dict[str, Any]] = load_json(characterized_path)
    generic_ops: List[Dict[str, Any]] = load_json(generic_ops_path)
    specific_ops: List[Dict[str, Any]] = load_json(specific_ops_path)
    op_categories: List[Dict[str, Any]] = load_json(op_categories_path)
    indicators: List[Dict[str, Any]] = load_json(indicators_path)

    categories_by_cod: Dict[int, Dict[str, Any]] = {}
    for row in op_categories:
        cod = safe_int(row.get("cod")) if isinstance(row, dict) else None
        if cod is not None:
            categories_by_cod[cod] = deepcopy(row)

    indicators_by_cod: Dict[int, Dict[str, Any]] = {}
    for row in indicators:
        cod = safe_int(row.get("cod")) if isinstance(row, dict) else None
        if cod is not None:
            indicators_by_cod[cod] = deepcopy(row)

    operations = generic_ops + specific_ops
    operations_by_stressor: Dict[int, List[Dict[str, Any]]] = defaultdict(list)
    stressor_stub_by_id: Dict[int, Dict[str, Any]] = {}
    for op in operations:
        stressor = op.get("stressor")
        if not isinstance(stressor, dict):
            continue
        stressor_id = safe_int(stressor.get("id"))
        if stressor_id is None:
            continue
        operations_by_stressor[stressor_id].append(op)
        stressor_stub_by_id.setdefault(stressor_id, deepcopy(stressor))

    characterized_by_id: Dict[int, Dict[str, Any]] = {}
    for row in characterized_rows:
        sid = safe_int(row.get("id"))
        if sid is not None:
            characterized_by_id[sid] = row

    if args.only_characterized:
        all_stressor_ids = sorted(characterized_by_id)
    else:
        all_stressor_ids = sorted(set(characterized_by_id) | set(operations_by_stressor))

    features = [
        build_feature(
            stressor_id=sid,
            characterized=characterized_by_id.get(sid),
            stressor_stub=stressor_stub_by_id.get(sid),
            operations=operations_by_stressor.get(sid, []),
            categories_by_cod=categories_by_cod,
            indicators_by_cod=indicators_by_cod,
            include_operation_geometry=args.include_operation_geometry,
        )
        for sid in all_stressor_ids
    ]

    feature_collection = {
        "type": "FeatureCollection",
        "name": "qcumber_stressors_denormalized",
        "metadata": {
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "input_dir": str(input_dir),
            "source_counts": {
                "characterized_stressors": len(characterized_rows),
                "generic_operations": len(generic_ops),
                "specific_operations": len(specific_ops),
                "operation_categories": len(op_categories),
                "indicators": len(indicators),
            },
            "options": {
                "only_characterized": args.only_characterized,
                "include_operation_geometry": args.include_operation_geometry,
            },
            "feature_count": len(features),
        },
        "features": features,
    }

    stressors_output_path.parent.mkdir(parents=True, exist_ok=True)
    with stressors_output_path.open("w", encoding="utf-8") as f:
        json.dump(feature_collection, f, ensure_ascii=False)

    print(f"Wrote {len(features)} stressor features to {stressors_output_path}")

    operation_features: List[Dict[str, Any]] = []
    skipped_null_geometry = 0
    for operation in sorted(operations, key=lambda op: (op.get("id") is None, op.get("id"))):
        stressor = operation.get("stressor")
        stressor_id = safe_int(stressor.get("id")) if isinstance(stressor, dict) else None
        if stressor_id is None:
            continue
        if args.only_characterized and stressor_id not in characterized_by_id:
            continue
        geometry = operation.get("geometry")
        if geometry is None and not args.operations_include_null_geometry:
            skipped_null_geometry += 1
            continue
        stressor_base = characterized_by_id.get(stressor_id) or stressor_stub_by_id.get(stressor_id) or {
            "id": stressor_id
        }
        operation_features.append(
            build_operation_feature(
                operation=operation,
                stressor_base=stressor_base,
                categories_by_cod=categories_by_cod,
                indicators_by_cod=indicators_by_cod,
            )
        )

    operations_feature_collection = {
        "type": "FeatureCollection",
        "name": "qcumber_stressor_operations_flat",
        "metadata": {
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "input_dir": str(input_dir),
            "source_counts": {
                "generic_operations": len(generic_ops),
                "specific_operations": len(specific_ops),
            },
            "options": {
                "only_characterized": args.only_characterized,
                "operations_include_null_geometry": args.operations_include_null_geometry,
            },
            "feature_count": len(operation_features),
            "skipped_null_geometry": skipped_null_geometry,
        },
        "features": operation_features,
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as f:
        json.dump(operations_feature_collection, f, ensure_ascii=False)

    print(
        f"Wrote {len(operation_features)} operation features to {output_path}"
    )


if __name__ == "__main__":
    run(parse_args())
