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

    # De-dupe by (name, event_type). This protects against codex merge mishaps
    # (e.g., whitespace variants) while preserving useful continuity signals.
    def _norm(s: str) -> str:
        return str(s or "").strip().lower()

    def _uniq_story_apps(apps):
        if not isinstance(apps, list):
            return []
        out = []
        seen = set()
        for a in apps:
            if not isinstance(a, dict):
                continue
            d = str(a.get("date") or "").strip()
            t = str(a.get("title") or "").strip()
            if not t:
                continue
            k = (d, t)
            if k in seen:
                continue
            seen.add(k)
            out.append({"date": d, "title": t})
        return out

    def _uniq_list(a, b, limit=None):
        a = a if isinstance(a, list) else []
        b = b if isinstance(b, list) else []
        out = list(a)
        seen = {str(x).strip().lower() for x in a if str(x).strip()}
        for x in b:
            k = str(x).strip().lower()
            if not k or k in seen:
                continue
            seen.add(k)
            out.append(x)
            if isinstance(limit, int) and len(out) >= limit:
                break
        return out

    merged: dict[tuple[str, str], dict] = {}
    for r in rows:
        if not isinstance(r, dict):
            continue
        k = (_norm(r.get("name")), _norm(r.get("event_type")))
        if not any(k):
            continue

        if k not in merged:
            merged[k] = r
            continue

        ex = merged[k]
        # Prefer richer metadata.
        if not ex.get("tagline") and r.get("tagline"):
            ex["tagline"] = r.get("tagline")
        if not ex.get("epicenter") or str(ex.get("epicenter") or "").strip().lower() == "unknown":
            if r.get("epicenter") and str(r.get("epicenter") or "").strip().lower() != "unknown":
                ex["epicenter"] = r.get("epicenter")

        # Keep strongest arc signals.
        ex["resolved"] = bool(ex.get("resolved")) and bool(r.get("resolved"))
        ex["intensity"] = max(int(ex.get("intensity") or 1), int(r.get("intensity") or 1))
        ex["recent_issues"] = max(int(ex.get("recent_issues") or 0), int(r.get("recent_issues") or 0))
        ex["trend"] = max(int(ex.get("trend") or 0), int(r.get("trend") or 0), key=abs)
        ex["last_seen"] = max(str(ex.get("last_seen") or ""), str(r.get("last_seen") or "")) or None

        # Merge location references.
        ex_loc = ex.get("referenced_locations") if isinstance(ex.get("referenced_locations"), dict) else {}
        r_loc = r.get("referenced_locations") if isinstance(r.get("referenced_locations"), dict) else {}
        ex["referenced_locations"] = {
            "places": _uniq_list(ex_loc.get("places"), r_loc.get("places"), limit=12),
            "regions": _uniq_list(ex_loc.get("regions"), r_loc.get("regions"), limit=12),
            "realms": _uniq_list(ex_loc.get("realms"), r_loc.get("realms"), limit=12),
            "continents": _uniq_list(ex_loc.get("continents"), r_loc.get("continents"), limit=8),
        }

        # Merge story appearances and keep the last 12.
        ex_apps = _uniq_story_apps(ex.get("story_appearances"))
        r_apps = _uniq_story_apps(r.get("story_appearances"))
        ex["story_appearances"] = (ex_apps + [a for a in r_apps if a not in ex_apps])[-12:]

    rows = list(merged.values())

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
