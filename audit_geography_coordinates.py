#!/usr/bin/env python3
"""Audit Valdris map coordinates and basic hydrology consistency.

Usage:
  python audit_geography_coordinates.py
  python audit_geography_coordinates.py --json

This is a read-only audit tool. It does not modify geography.json.
"""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path


DETAIL_VIEW_WIDTH = 800
DETAIL_VIEW_HEIGHT = 600
DETAIL_VIEW_PADDING = 25

SOURCE_HINT_WORDS = (
    "rise",
    "rises",
    "rising",
    "source",
    "sources",
    "foothill",
    "foothills",
    "slope",
    "slopes",
    "mountain",
    "mountains",
    "highland",
    "highlands",
)

OUTLET_HINT_WORDS = (
    "empt",
    "meet",
    "meets",
    "mouth",
    "delta",
    "coast",
    "ocean",
    "sea",
    "merge",
    "merges",
    "harbor",
)


def _load_json(path: Path) -> dict:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise SystemExit(f"Expected top-level object in {path}")
    return data


def _norm(text: str) -> str:
    return str(text or "").strip().lower()


def _detail_view_config(geography: dict, continent_id: str) -> dict:
    raw = (((geography or {}).get("map_coordinates") or {}).get(continent_id) or {}).get("detail_view") or {}
    width = int(raw.get("width") or DETAIL_VIEW_WIDTH)
    height = int(raw.get("height") or DETAIL_VIEW_HEIGHT)
    padding = int(raw.get("padding") or DETAIL_VIEW_PADDING)
    usable_width = int(raw.get("usable_width") or (width - (padding * 2)))
    usable_height = int(raw.get("usable_height") or (height - (padding * 2)))
    return {
        "width": width,
        "height": height,
        "padding": padding,
        "usable_width": max(1, usable_width),
        "usable_height": max(1, usable_height),
    }


def _scale_miles_config(geography: dict, continent: dict, continent_id: str) -> tuple[float, float]:
    raw = (((geography or {}).get("map_coordinates") or {}).get(continent_id) or {}).get("scale_miles") or {}
    dimensions = continent.get("geo_dimensions") or {}
    width = float(raw.get("width") or dimensions.get("width_miles") or 0.0)
    height = float(raw.get("height") or dimensions.get("height_miles") or 0.0)
    return width, height


def _assignment_position_pct(assignment: dict, continent_id: str) -> dict:
    explicit = ((((assignment or {}).get("map_coordinates") or {}).get(continent_id) or {}).get("position_pct") or {})
    if isinstance(explicit, dict) and "x" in explicit and "y" in explicit:
        return explicit
    return (assignment or {}).get("position_pct") or {}


def _detail_svg_coords(position_pct: dict, detail_view: dict) -> tuple[float, float]:
    x = float(detail_view["padding"]) + (float(position_pct.get("x", 0.0)) / 100.0) * float(detail_view["usable_width"])
    y = float(detail_view["padding"]) + (float(position_pct.get("y", 0.0)) / 100.0) * float(detail_view["usable_height"])
    return x, y


def _continent_lookup(geography: dict) -> dict[str, dict]:
    return {
        str(continent.get("id") or ""): continent
        for continent in geography.get("continents", [])
        if isinstance(continent, dict) and str(continent.get("id") or "")
    }


def _macro_region_lookup(geography: dict) -> dict[str, dict]:
    return {
        str(region.get("id") or ""): region
        for region in geography.get("macro_regions", [])
        if isinstance(region, dict) and str(region.get("id") or "")
    }


def _coastal_regions(geography: dict) -> set[str]:
    out: set[str] = set()
    for feature in geography.get("natural_features", []):
        if not isinstance(feature, dict):
            continue
        if _norm(feature.get("type")) not in {"sea", "ocean"}:
            continue
        for region_id in feature.get("adjacent_regions", []) or []:
            if region_id:
                out.add(str(region_id))
    return out


def _mountain_adjacent_regions(geography: dict) -> set[str]:
    out: set[str] = set()
    for feature in geography.get("natural_features", []):
        if not isinstance(feature, dict):
            continue
        if _norm(feature.get("type")) != "mountain_range":
            continue
        for region_id in feature.get("adjacent_regions", []) or []:
            if region_id:
                out.add(str(region_id))
    return out


def _distance(a: tuple[float, float], b: tuple[float, float]) -> float:
    return math.hypot(a[0] - b[0], a[1] - b[1])


def _find_place_issues(
    assignments: list[dict],
    continent_id: str,
    macro_regions: dict[str, dict],
    detail_view: dict,
    collision_radius_px: float,
    near_radius_px: float,
    width_miles: float,
    height_miles: float,
) -> dict:
    by_name = {
        str(a.get("place_name") or ""): a
        for a in assignments
        if isinstance(a, dict) and str(a.get("place_name") or "")
    }

    selected = []
    unknown_region_assignments = []
    out_of_bounds = []
    inherited_sublocations = []

    for assignment in assignments:
        if not isinstance(assignment, dict):
            continue
        region_id = str(assignment.get("macro_region") or "")
        region = macro_regions.get(region_id)
        if region is None:
            unknown_region_assignments.append({
                "place_name": assignment.get("place_name"),
                "macro_region": region_id,
            })
            continue
        if str(region.get("continent") or "") != continent_id:
            continue

        parent_name = str(assignment.get("parent_place_name") or "").strip()
        inherits = assignment.get("inherits_position_from_parent") is True
        if inherits and parent_name and parent_name in by_name:
            inherited_sublocations.append({
                "place_name": assignment.get("place_name"),
                "parent_place_name": parent_name,
            })
            continue

        position_pct = _assignment_position_pct(assignment, continent_id)
        x_pct = float(position_pct.get("x", -1))
        y_pct = float(position_pct.get("y", -1))
        if x_pct < 0 or x_pct > 100 or y_pct < 0 or y_pct > 100:
            out_of_bounds.append({
                "place_name": assignment.get("place_name"),
                "macro_region": region_id,
                "position_pct": {"x": x_pct, "y": y_pct},
            })
            continue

        svg_x, svg_y = _detail_svg_coords(position_pct, detail_view)
        selected.append({
            "place_name": str(assignment.get("place_name") or "").strip(),
            "macro_region": region_id,
            "confidence": str(assignment.get("confidence") or "").strip().lower() or "unknown",
            "position_pct": {"x": x_pct, "y": y_pct},
            "detail_svg": {"x": round(svg_x, 2), "y": round(svg_y, 2)},
            "world_miles": {
                "x": round((x_pct / 100.0) * width_miles, 1),
                "y": round((y_pct / 100.0) * height_miles, 1),
            },
        })

    collisions = []
    near_overlaps = []
    exact_position_groups: dict[tuple[float, float], list[str]] = {}

    for row in selected:
        key = (row["position_pct"]["x"], row["position_pct"]["y"])
        exact_position_groups.setdefault(key, []).append(row["place_name"])

    for idx, left in enumerate(selected):
        left_xy = (left["detail_svg"]["x"], left["detail_svg"]["y"])
        for right in selected[idx + 1 :]:
            right_xy = (right["detail_svg"]["x"], right["detail_svg"]["y"])
            dist_px = _distance(left_xy, right_xy)
            dist_mi = _distance(
                (left["world_miles"]["x"], left["world_miles"]["y"]),
                (right["world_miles"]["x"], right["world_miles"]["y"]),
            )
            pair = {
                "left": left["place_name"],
                "right": right["place_name"],
                "left_region": left["macro_region"],
                "right_region": right["macro_region"],
                "distance_px": round(dist_px, 2),
                "distance_miles": round(dist_mi, 1),
            }
            if dist_px <= collision_radius_px:
                collisions.append(pair)
            elif dist_px <= near_radius_px:
                near_overlaps.append(pair)

    exact_duplicates = [
        {
            "position_pct": {"x": pos[0], "y": pos[1]},
            "places": sorted(names),
        }
        for pos, names in exact_position_groups.items()
        if len(names) > 1
    ]

    collisions.sort(key=lambda row: (row["distance_px"], row["left"], row["right"]))
    near_overlaps.sort(key=lambda row: (row["distance_px"], row["left"], row["right"]))
    exact_duplicates.sort(key=lambda row: (row["position_pct"]["x"], row["position_pct"]["y"]))
    selected.sort(key=lambda row: (row["macro_region"], row["place_name"]))

    return {
        "places": selected,
        "unknown_region_assignments": unknown_region_assignments,
        "out_of_bounds": out_of_bounds,
        "inherited_sublocations": inherited_sublocations,
        "exact_duplicates": exact_duplicates,
        "collisions": collisions,
        "near_overlaps": near_overlaps,
    }


def _find_river_issues(geography: dict) -> dict:
    coastal_regions = _coastal_regions(geography)
    mountain_regions = _mountain_adjacent_regions(geography)
    river_rows = []
    source_risks = []
    outlet_risks = []

    for feature in geography.get("natural_features", []):
        if not isinstance(feature, dict):
            continue
        if _norm(feature.get("type")) != "river":
            continue

        flows_through = [str(region_id) for region_id in feature.get("flows_through", []) or [] if region_id]
        description = _norm(feature.get("description"))
        source_regions = [str(region_id) for region_id in feature.get("source_regions", []) or [] if region_id]
        source_feature_ids = [str(fid) for fid in feature.get("source_feature_ids", []) or [] if fid]
        outlet_regions = [str(region_id) for region_id in feature.get("outlet_regions", []) or [] if region_id]
        outlet_feature_ids = [str(fid) for fid in feature.get("outlet_feature_ids", []) or [] if fid]
        outlet_river_ids = [str(fid) for fid in feature.get("outlet_river_ids", []) or [] if fid]
        source_type = str(feature.get("source_type") or "").strip().lower()
        outlet_to_coast = bool(feature.get("outlet_to_coast"))
        touches_coast = any(region_id in coastal_regions for region_id in flows_through)
        touches_mountains = any(region_id in mountain_regions for region_id in flows_through)
        has_source_hint = any(word in description for word in SOURCE_HINT_WORDS)
        has_outlet_hint = any(word in description for word in OUTLET_HINT_WORDS)
        has_explicit_source = bool(source_regions or source_feature_ids or source_type)
        has_explicit_outlet = bool(outlet_regions or outlet_feature_ids or outlet_river_ids or outlet_to_coast)

        row = {
            "id": str(feature.get("id") or ""),
            "name": str(feature.get("name") or ""),
            "flows_through": flows_through,
            "source_regions": source_regions,
            "source_feature_ids": source_feature_ids,
            "source_type": source_type,
            "outlet_regions": outlet_regions,
            "outlet_feature_ids": outlet_feature_ids,
            "outlet_river_ids": outlet_river_ids,
            "outlet_to_coast": outlet_to_coast,
            "touches_coast_region": touches_coast,
            "touches_mountain_adjacent_region": touches_mountains,
            "has_source_hint": has_source_hint,
            "has_outlet_hint": has_outlet_hint,
            "has_explicit_source": has_explicit_source,
            "has_explicit_outlet": has_explicit_outlet,
        }
        river_rows.append(row)

        if not touches_mountains and not has_source_hint and not has_explicit_source:
            source_risks.append(row)
        if not touches_coast and not has_outlet_hint and not has_explicit_outlet:
            outlet_risks.append(row)

    river_rows.sort(key=lambda row: row["name"])
    source_risks.sort(key=lambda row: row["name"])
    outlet_risks.sort(key=lambda row: row["name"])

    return {
        "coastal_regions": sorted(coastal_regions),
        "mountain_adjacent_regions": sorted(mountain_regions),
        "rivers": river_rows,
        "source_risks": source_risks,
        "outlet_risks": outlet_risks,
    }


def summarize(
    geography: dict,
    continent_id: str = "valdris",
    collision_radius_px: float = 10.0,
    near_radius_px: float = 18.0,
) -> dict:
    continents = _continent_lookup(geography)
    continent = continents.get(continent_id)
    if continent is None:
        raise SystemExit(f"Missing continent: {continent_id}")

    dimensions = continent.get("geo_dimensions") or {}
    detail_view = _detail_view_config(geography, continent_id)
    width_miles, height_miles = _scale_miles_config(geography, continent, continent_id)
    macro_regions = _macro_region_lookup(geography)

    place_summary = _find_place_issues(
        assignments=geography.get("place_assignments", []) or [],
        continent_id=continent_id,
        macro_regions=macro_regions,
        detail_view=detail_view,
        collision_radius_px=collision_radius_px,
        near_radius_px=near_radius_px,
        width_miles=width_miles,
        height_miles=height_miles,
    )
    river_summary = _find_river_issues(geography)

    counts_by_region: dict[str, int] = {}
    for row in place_summary["places"]:
        counts_by_region[row["macro_region"]] = counts_by_region.get(row["macro_region"], 0) + 1

    return {
        "continent": {
            "id": continent_id,
            "name": continent.get("name"),
            "width_miles": width_miles,
            "height_miles": height_miles,
            "detail_view": detail_view,
        },
        "thresholds": {
            "collision_radius_px": collision_radius_px,
            "near_radius_px": near_radius_px,
        },
        "place_summary": {
            "count": len(place_summary["places"]),
            "counts_by_region": dict(sorted(counts_by_region.items())),
            "unknown_region_assignments": place_summary["unknown_region_assignments"],
            "out_of_bounds": place_summary["out_of_bounds"],
            "inherited_sublocations": place_summary["inherited_sublocations"],
            "exact_duplicates": place_summary["exact_duplicates"],
            "collisions": place_summary["collisions"],
            "near_overlaps": place_summary["near_overlaps"],
            "places": place_summary["places"],
        },
        "river_summary": river_summary,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Audit geography coordinates and river plausibility")
    ap.add_argument("--geography", default="geography.json", help="Path to geography.json")
    ap.add_argument("--continent", default="valdris", help="Continent id to audit")
    ap.add_argument("--collision-radius", type=float, default=10.0, help="Pixel radius for hard collisions")
    ap.add_argument("--near-radius", type=float, default=18.0, help="Pixel radius for near overlaps")
    ap.add_argument("--json", action="store_true", help="Print machine-readable JSON")
    args = ap.parse_args()

    geography = _load_json(Path(args.geography))
    summary = summarize(
        geography=geography,
        continent_id=args.continent,
        collision_radius_px=args.collision_radius,
        near_radius_px=args.near_radius,
    )

    if args.json:
        print(json.dumps(summary, ensure_ascii=True, indent=2))
        return 0

    continent = summary["continent"]
    places = summary["place_summary"]
    rivers = summary["river_summary"]

    print(f"Coordinate audit for {continent['name']} ({continent['id']})")
    print(
        f"Detail view: {continent['detail_view']['width']}x{continent['detail_view']['height']} px "
        f"with {continent['detail_view']['padding']} px padding"
    )
    print(f"Physical size: {int(continent['width_miles'])} x {int(continent['height_miles'])} miles")
    print("")
    print(f"Mapped places: {places['count']}")
    for region_id, count in places["counts_by_region"].items():
        print(f"  {region_id:16s} {count}")

    if places["inherited_sublocations"]:
        print(f"Inherited sublocations (excluded from collision checks): {len(places['inherited_sublocations'])}")

    print("")
    print(
        f"Exact duplicate coordinates: {len(places['exact_duplicates'])}"
        f" | hard collisions <= {summary['thresholds']['collision_radius_px']:.1f}px: {len(places['collisions'])}"
        f" | near overlaps <= {summary['thresholds']['near_radius_px']:.1f}px: {len(places['near_overlaps'])}"
    )

    if places["exact_duplicates"]:
        print("")
        print("Exact duplicate positions:")
        for row in places["exact_duplicates"][:12]:
            pos = row["position_pct"]
            joined = ", ".join(row["places"])
            print(f"  ({pos['x']:.1f}, {pos['y']:.1f}) -> {joined}")

    if places["collisions"]:
        print("")
        print("Hard collisions:")
        for row in places["collisions"][:20]:
            print(
                f"  {row['left']} <-> {row['right']}"
                f" [{row['left_region']} / {row['right_region']}]"
                f" at {row['distance_px']:.2f}px (~{row['distance_miles']:.1f} mi)"
            )

    if places["near_overlaps"]:
        print("")
        print("Near overlaps:")
        for row in places["near_overlaps"][:20]:
            print(
                f"  {row['left']} <-> {row['right']}"
                f" [{row['left_region']} / {row['right_region']}]"
                f" at {row['distance_px']:.2f}px (~{row['distance_miles']:.1f} mi)"
            )

    if places["unknown_region_assignments"] or places["out_of_bounds"]:
        print("")
        if places["unknown_region_assignments"]:
            print("Assignments with unknown macro regions:")
            for row in places["unknown_region_assignments"]:
                print(f"  {row['place_name']} -> {row['macro_region']}")
        if places["out_of_bounds"]:
            print("Out-of-bounds assignments:")
            for row in places["out_of_bounds"]:
                pos = row["position_pct"]
                print(f"  {row['place_name']} -> ({pos['x']}, {pos['y']})")

    print("")
    print(f"Rivers audited: {len(rivers['rivers'])}")
    print(f"Potential source metadata gaps: {len(rivers['source_risks'])}")
    print(f"Potential outlet metadata gaps: {len(rivers['outlet_risks'])}")

    if rivers["source_risks"]:
        print("")
        print("Rivers missing clear source anchoring:")
        for row in rivers["source_risks"]:
            print(f"  {row['name']} -> regions: {', '.join(row['flows_through']) or '(none)'}")

    if rivers["outlet_risks"]:
        print("")
        print("Rivers missing clear outlet/coast anchoring:")
        for row in rivers["outlet_risks"]:
            print(f"  {row['name']} -> regions: {', '.join(row['flows_through']) or '(none)'}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())