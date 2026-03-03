#!/usr/bin/env python3
"""audit_world_events.py

Generate a concise machine-readable summary of the most active large-scale events
("world event arcs") from codex.json.

This is meant to be run in GitHub Actions (or locally) to produce a persistent
"what's happening in the world" register, akin to a novel's unfolding arc list.

Output: world-events.json
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone


CODEX_FILE = "codex.json"
OUTPUT_FILE = "world-events.json"


def _load_json(path: str):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _write_json(path: str, data) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=True, indent=2)


def main() -> int:
    if not os.path.exists(CODEX_FILE):
        raise SystemExit(f"ERROR: {CODEX_FILE} not found")

    # Import generator helpers (they already know how to infer geo + arc stage).
    import generate_stories as gs

    codex = _load_json(CODEX_FILE)
    events = codex.get("events", []) if isinstance(codex, dict) else []
    if not isinstance(events, list):
        events = []

    loc_names = gs._canon_loc_names_from_codex(codex)  # type: ignore[attr-defined]
    known_dates = gs._load_known_issue_dates()  # type: ignore[attr-defined]

    rows = []
    for e in events:
        if not isinstance(e, dict):
            continue
        name = str(e.get("name") or "").strip()
        if not name:
            continue

        geo = gs._infer_event_geo_from_codex(e, loc_names)  # type: ignore[attr-defined]
        arc = gs._event_arc_metrics(e, known_dates)  # type: ignore[attr-defined]

        scope = str(geo.get("scope") or "regional")
        epicenter = str(geo.get("epicenter") or "unknown")
        mentions = geo.get("mentions") if isinstance(geo.get("mentions"), dict) else {}

        mentioned_places = mentions.get("places") if isinstance(mentions.get("places"), list) else []
        mentioned_regions = mentions.get("regions") if isinstance(mentions.get("regions"), list) else []
        mentioned_realms = mentions.get("realms") if isinstance(mentions.get("realms"), list) else []
        mentioned_continents = mentions.get("continents") if isinstance(mentions.get("continents"), list) else []

        story_apps = e.get("story_appearances") if isinstance(e.get("story_appearances"), list) else []

        rows.append({
            "name": name,
            "tagline": str(e.get("tagline") or "").strip(),
            "event_type": str(e.get("event_type") or "").strip(),
            "scope": scope,
            "epicenter": epicenter,
            "stage": str(arc.get("stage") or "seed"),
            "intensity": int(arc.get("intensity") or 1),
            "resolved": bool(arc.get("resolved")),
            "last_seen": str(arc.get("last_date") or "").strip() or None,
            "recent_issues": int(arc.get("recent_count") or 0),
            "trend": int(arc.get("trend") or 0),
            "referenced_locations": {
                "places": mentioned_places[:12],
                "regions": mentioned_regions[:12],
                "realms": mentioned_realms[:12],
                "continents": mentioned_continents[:8],
            },
            "story_appearances": story_apps[-12:],
        })

    def _scope_rank(scope: str) -> int:
        s = (scope or "").strip().lower()
        return {"world": 4, "continental": 3, "regional": 2, "city": 1}.get(s, 2)

    # Prefer big, unresolved, high-intensity, recently-active arcs.
    rows.sort(
        key=lambda r: (
            0 if not r.get("resolved") else 1,
            -_scope_rank(str(r.get("scope") or "regional")),
            -int(r.get("intensity") or 1),
            -(int(r.get("recent_issues") or 0)),
            str(r.get("last_seen") or ""),
            str(r.get("name") or ""),
        )
    )

    # Keep the file compact but useful.
    top = rows[:50]

    out = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": CODEX_FILE,
        "count": len(top),
        "events": top,
    }
    _write_json(OUTPUT_FILE, out)
    print(f"✓ Wrote {OUTPUT_FILE} ({len(top)} events)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
