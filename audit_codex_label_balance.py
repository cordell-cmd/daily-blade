#!/usr/bin/env python3
"""Audit codex label balance and identify underrepresented categories.

Usage:
  python audit_codex_label_balance.py
  python audit_codex_label_balance.py --json
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

TRACKED_LABELS = [
    "weapons",
    "rituals",
    "artifacts",
    "relics",
    "substances",
    "factions",
    "lore",
    "magic",
    "characters",
    "places",
    "events",
    "flora_fauna",
    "polities",
    "provinces",
    "districts",
    "regions",
]


def _named_count(items) -> int:
    if not isinstance(items, list):
        return 0
    return sum(1 for it in items if isinstance(it, dict) and str(it.get("name") or "").strip())


def summarize(codex: dict) -> dict:
    counts = {label: _named_count((codex or {}).get(label, [])) for label in TRACKED_LABELS}
    values = sorted(counts.values())
    if not values:
        median_count = 0
    elif len(values) % 2 == 1:
        median_count = values[len(values) // 2]
    else:
        median_count = (values[len(values) // 2 - 1] + values[len(values) // 2]) / 2.0

    target_min_count = max(3, int(round(max(1.0, float(median_count)) * 0.60)))

    weak = []
    for label in TRACKED_LABELS:
        c = int(counts.get(label, 0))
        if c >= target_min_count:
            continue
        deficit = target_min_count - c
        if c <= int(target_min_count * 0.35):
            sev = "high"
        elif c <= int(target_min_count * 0.65):
            sev = "medium"
        else:
            sev = "low"
        weak.append({
            "label": label,
            "count": c,
            "target_min_count": target_min_count,
            "deficit": deficit,
            "severity": sev,
        })

    weak.sort(key=lambda x: (int(x.get("deficit") or 0), str(x.get("label") or "")), reverse=True)
    return {
        "counts": counts,
        "median_count": median_count,
        "target_min_count": target_min_count,
        "underrepresented": weak,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Audit codex label balance")
    ap.add_argument("--codex", default="codex.json", help="Path to codex.json")
    ap.add_argument("--json", action="store_true", help="Print machine-readable JSON")
    args = ap.parse_args()

    codex_path = Path(args.codex)
    if not codex_path.exists():
        raise SystemExit(f"Missing codex file: {codex_path}")

    codex = json.loads(codex_path.read_text(encoding="utf-8"))
    summary = summarize(codex if isinstance(codex, dict) else {})

    if args.json:
        print(json.dumps(summary, ensure_ascii=True, indent=2))
        return 0

    counts = summary["counts"]
    print("Codex label counts:")
    for label in sorted(counts.keys()):
        print(f"  {label:14s} {int(counts[label])}")

    print("")
    print(f"Median count: {summary['median_count']}")
    print(f"Target floor: {summary['target_min_count']}")

    weak = summary["underrepresented"]
    if not weak:
        print("No underrepresented labels detected.")
        return 0

    print("")
    print("Underrepresented labels:")
    for row in weak:
        print(
            f"  {row['label']:14s} {row['count']:4d} "
            f"(target {row['target_min_count']}, deficit {row['deficit']}, severity {row['severity']})"
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
