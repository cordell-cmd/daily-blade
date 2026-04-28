#!/usr/bin/env python3
"""world_state.py

Persistent world-state tracking for Daily Blade.

This module is intentionally deterministic and additive:
- It reads canon from codex.json.
- It infers per-issue event deltas from codex events updated today.
- It writes a durable world-state snapshot to world-state.json.

It does not replace existing codex/world-events tracking; it extends it.
"""

from __future__ import annotations

import json
import os
import re
from datetime import datetime, timezone


def _slug(value: str) -> str:
    s = re.sub(r"[^a-zA-Z0-9]+", "_", str(value or "").strip().lower()).strip("_")
    return s or "unknown"


def _load_json(path: str, default):
    if not os.path.exists(path):
        return default
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def _clamp(n: int, lo: int, hi: int) -> int:
    return max(lo, min(hi, int(n)))


def _find_story_mentions(stories: list[dict], name: str, date_key: str) -> list[dict]:
    if not isinstance(stories, list):
        return []
    nm = str(name or "").strip().lower()
    if not nm:
        return []
    out = []
    for s in stories:
        if not isinstance(s, dict):
            continue
        title = str(s.get("title") or "").strip()
        blob = (str(s.get("title") or "") + " " + str(s.get("text") or "")).lower()
        if title and nm in blob:
            out.append({"date": date_key, "title": title})
    return out


def _event_is_updated_today(event_row: dict, date_key: str) -> bool:
    apps = event_row.get("story_appearances") if isinstance(event_row, dict) else None
    if not isinstance(apps, list):
        return False
    for a in apps:
        if not isinstance(a, dict):
            continue
        if str(a.get("date") or "").strip() == date_key:
            return True
    return False


def _estimate_event_intensity(event_row: dict) -> int:
    raw = event_row.get("intensity")
    if isinstance(raw, int):
        return _clamp(raw, 1, 10)
    apps = event_row.get("story_appearances") if isinstance(event_row, dict) else None
    n = len(apps) if isinstance(apps, list) else 0
    if n >= 8:
        return 9
    if n >= 5:
        return 7
    if n >= 3:
        return 5
    if n >= 2:
        return 4
    return 3


def _normalize_stage(raw_stage: str) -> str:
    stage = str(raw_stage or "active").strip().lower()
    if stage in {"seed", "simmering", "rising", "climax"}:
        stage = {
            "seed": "rumor",
            "simmering": "brewing",
            "rising": "active",
            "climax": "crisis",
        }[stage]
    if stage not in {"rumor", "brewing", "active", "crisis", "aftermath", "resolved"}:
        return "active"
    return stage


def _event_consequence_tier(
    event_row: dict,
    intensity: int,
    scope: str,
    stage: str,
    faction_ids: list[str],
    region_ids: list[str],
    character_ids: list[str],
) -> str:
    """Classify events into flavor/local/world-shaping tiers.

    Flavor: atmosphere/personal/rumor beats with no broad system mutation.
    Local consequential: limited character/place fallout.
    World-shaping: broad systemic shifts (factions/regions/arcs).
    """
    explicit = str(event_row.get("consequence_tier") or event_row.get("tier") or "").strip().lower()
    if explicit in {"flavor", "local_consequential", "world_shaping"}:
        return explicit

    if (
        intensity >= 8
        or scope in {"world", "continental"}
        or stage in {"crisis", "resolved", "aftermath"}
    ):
        return "world_shaping"

    has_local_anchors = bool(region_ids or character_ids or faction_ids)
    if (
        intensity <= 4
        and stage in {"rumor", "brewing"}
        and not has_local_anchors
    ):
        return "flavor"

    return "local_consequential"


def _derive_issue_number(codex: dict, date_key: str) -> int:
    # Conservative fallback: monotonic issue number from appearance count in archive index
    idx = _load_json("archive/index.json", {"dates": []})
    dates = idx.get("dates") if isinstance(idx, dict) else []
    if isinstance(dates, list) and date_key in dates:
        return max(1, len(dates))
    return max(1, len(dates) + 1)


def _make_initial_state(codex: dict, issue_number: int) -> dict:
    factions = {}
    for f in codex.get("factions", []) if isinstance(codex, dict) else []:
        if not isinstance(f, dict):
            continue
        fid = str(f.get("id") or _slug(f.get("name") or "faction"))
        factions[fid] = {
            "id": fid,
            "name": str(f.get("name") or "Unknown Faction"),
            "power": 50,
            "cohesion": 50,
            "wealth": 50,
            "reputation": 0,
            "territories": [],
            "activeGoals": [],
            "enemies": [],
            "allies": [],
        }

    regions = {}
    for r in codex.get("regions", []) if isinstance(codex, dict) else []:
        if not isinstance(r, dict):
            continue
        rid = str(r.get("id") or _slug(r.get("name") or "region"))
        regions[rid] = {
            "id": rid,
            "name": str(r.get("name") or "Unknown Region"),
            "stability": 60,
            "prosperity": 50,
            "danger": 35,
            "controllerFactionId": None,
            "tags": [],
            "recentEventIds": [],
        }

    characters = {}
    for c in codex.get("characters", []) if isinstance(codex, dict) else []:
        if not isinstance(c, dict):
            continue
        cid = str(c.get("id") or _slug(c.get("name") or "character"))
        status = str(c.get("status") or "stable").strip().lower()
        if status not in {"rising", "stable", "wounded", "missing", "fallen", "dead"}:
            status = "stable"
        characters[cid] = {
            "id": cid,
            "name": str(c.get("name") or "Unknown Character"),
            "role": str(c.get("role") or "Unknown"),
            "factionId": None,
            "influence": 35,
            "status": status,
            "traits": list(c.get("traits") or []) if isinstance(c.get("traits"), list) else [],
            "goals": [],
            "rivals": [],
            "allies": [],
            "homeRegionId": None,
        }

    arcs = {}
    for e in codex.get("events", []) if isinstance(codex, dict) else []:
        if not isinstance(e, dict):
            continue
        name = str(e.get("name") or "").strip()
        if not name:
            continue
        eid = str(e.get("id") or _slug(name))
        stage = str(e.get("stage") or "rumor").strip().lower()
        if stage in {"seed", "simmering", "rising", "climax"}:
            stage = {
                "seed": "rumor",
                "simmering": "brewing",
                "rising": "active",
                "climax": "crisis",
            }[stage]
        if stage not in {"rumor", "brewing", "active", "crisis", "aftermath", "resolved"}:
            stage = "rumor"
        arcs[eid] = {
            "id": eid,
            "name": name,
            "scope": str(e.get("scope") or "regional").strip().lower() or "regional",
            "stage": stage,
            "intensity": _estimate_event_intensity(e),
            "summary": str(e.get("significance") or e.get("tagline") or "").strip(),
            "factionsInvolved": [],
            "regionsInvolved": [],
            "charactersInvolved": [],
            "causeEventIds": [],
            "effects": [],
            "startedIssue": issue_number,
            "lastUpdatedIssue": issue_number,
            "resolvedIssue": issue_number if bool(e.get("resolved")) else None,
            "isActive": not bool(e.get("resolved")),
        }

    return {
        "schemaVersion": "1.0",
        "issueNumber": issue_number,
        "updatedAt": datetime.now(timezone.utc).isoformat(),
        "factions": factions,
        "regions": regions,
        "characters": characters,
        "arcs": arcs,
        "events": [],
    }


def _apply_delta(state: dict, delta: dict, issue_number: int) -> dict:
    if not isinstance(delta, dict):
        return state

    for fid, change in (delta.get("factionChanges") or {}).items():
        f = state.get("factions", {}).get(fid)
        if not isinstance(f, dict) or not isinstance(change, dict):
            continue
        for k, lo, hi in [("power", 0, 100), ("cohesion", 0, 100), ("wealth", 0, 100), ("reputation", -100, 100)]:
            if isinstance(change.get(k), int):
                f[k] = _clamp(int(f.get(k, 0)) + int(change[k]), lo, hi)

    for rid, change in (delta.get("regionChanges") or {}).items():
        r = state.get("regions", {}).get(rid)
        if not isinstance(r, dict) or not isinstance(change, dict):
            continue
        for k in [("stability", 0, 100), ("prosperity", 0, 100), ("danger", 0, 100)]:
            key, lo, hi = k
            if isinstance(change.get(key), int):
                r[key] = _clamp(int(r.get(key, 0)) + int(change[key]), lo, hi)

    for cid, change in (delta.get("characterChanges") or {}).items():
        c = state.get("characters", {}).get(cid)
        if not isinstance(c, dict) or not isinstance(change, dict):
            continue
        if isinstance(change.get("influence"), int):
            c["influence"] = _clamp(int(c.get("influence", 0)) + int(change["influence"]), 0, 100)
        if isinstance(change.get("status"), str):
            c["status"] = change["status"]

    for aid, change in (delta.get("arcChanges") or {}).items():
        a = state.get("arcs", {}).get(aid)
        if not isinstance(a, dict) or not isinstance(change, dict):
            continue
        if isinstance(change.get("stage"), str):
            a["stage"] = change["stage"]
        if isinstance(change.get("intensity"), int):
            a["intensity"] = _clamp(change["intensity"], 1, 10)
        if "isActive" in change:
            a["isActive"] = bool(change.get("isActive"))
        a["lastUpdatedIssue"] = issue_number

    state["issueNumber"] = issue_number
    state["updatedAt"] = datetime.now(timezone.utc).isoformat()
    return state


def _story_event_from_codex_event(event_row: dict, state: dict, date_key: str, issue_number: int) -> dict:
    name = str(event_row.get("name") or "Unnamed Event").strip() or "Unnamed Event"
    event_id = str(event_row.get("id") or _slug(name))
    intensity = _estimate_event_intensity(event_row)

    faction_index = {
        str(v.get("name") or "").strip().lower(): k
        for k, v in (state.get("factions") or {}).items()
        if isinstance(v, dict)
    }
    region_index = {
        str(v.get("name") or "").strip().lower(): k
        for k, v in (state.get("regions") or {}).items()
        if isinstance(v, dict)
    }
    char_index = {
        str(v.get("name") or "").strip().lower(): k
        for k, v in (state.get("characters") or {}).items()
        if isinstance(v, dict)
    }

    participants = event_row.get("participants") if isinstance(event_row.get("participants"), list) else []
    affected_regions = event_row.get("affected_regions") if isinstance(event_row.get("affected_regions"), list) else []

    faction_ids = []
    character_ids = []
    for p in participants:
        key = str(p or "").strip().lower()
        if not key:
            continue
        if key in faction_index and faction_index[key] not in faction_ids:
            faction_ids.append(faction_index[key])
        if key in char_index and char_index[key] not in character_ids:
            character_ids.append(char_index[key])

    region_ids = []
    for r in affected_regions:
        key = str(r or "").strip().lower()
        if key in region_index and region_index[key] not in region_ids:
            region_ids.append(region_index[key])

    scope = str(event_row.get("scope") or "regional").strip().lower() or "regional"
    arc_stage = _normalize_stage(str(event_row.get("stage") or "active"))
    tier = _event_consequence_tier(
        event_row=event_row,
        intensity=intensity,
        scope=scope,
        stage=arc_stage,
        faction_ids=faction_ids,
        region_ids=region_ids,
        character_ids=character_ids,
    )

    faction_delta = {}
    if tier == "world_shaping":
        for fid in faction_ids[:2]:
            faction_delta[fid] = {
                "power": 2 if intensity >= 6 else 1,
                "cohesion": -3 if intensity >= 6 else -1,
                "reputation": -2 if intensity >= 8 else 1,
            }

    region_delta = {}
    if tier == "world_shaping":
        for rid in region_ids[:2]:
            region_delta[rid] = {
                "stability": -6 if intensity >= 7 else -3,
                "danger": 8 if intensity >= 7 else 4,
                "prosperity": -4 if intensity >= 7 else -1,
            }
    elif tier == "local_consequential":
        for rid in region_ids[:1]:
            region_delta[rid] = {
                "stability": -2 if intensity >= 6 else -1,
                "danger": 3 if intensity >= 6 else 1,
                "prosperity": -1,
            }

    char_delta = {}
    if tier in {"local_consequential", "world_shaping"}:
        for cid in character_ids[:2]:
            char_delta[cid] = {
                "influence": 4 if intensity >= 7 else 2,
                "status": "wounded" if intensity >= 9 else state.get("characters", {}).get(cid, {}).get("status", "stable"),
            }

    arc_delta = {}
    if tier == "world_shaping":
        arc_delta = {
            event_id: {
                "stage": arc_stage,
                "intensity": intensity,
                "isActive": not bool(event_row.get("resolved")),
            }
        }

    if tier == "flavor":
        consequence_notes = [
            "Flavor event: atmospheric/personal texture only.",
            "No persistent simulation metrics were mutated.",
        ]
    elif tier == "local_consequential":
        consequence_notes = [
            "Local consequential event: limited character/place fallout.",
            "World-scale faction/arc metrics were not mutated.",
        ]
    else:
        consequence_notes = [
            "World-shaping event: broad persistent simulation metrics were mutated.",
            "Derived from codex event updated this issue.",
        ]

    delta = {
        "factionChanges": faction_delta,
        "regionChanges": region_delta,
        "characterChanges": char_delta,
        "arcChanges": arc_delta,
        "notes": consequence_notes,
    }

    return {
        "id": f"evt_{_slug(name)}_{issue_number}",
        "issueNumber": issue_number,
        "title": name,
        "summary": str(event_row.get("significance") or event_row.get("outcome") or event_row.get("tagline") or "").strip(),
        "scope": scope,
        "intensity": intensity,
        "tags": ["world-state", "consequence", "codex-event", tier],
        "consequenceTier": tier,
        "factionIds": faction_ids,
        "regionIds": region_ids,
        "characterIds": character_ids,
        "causeEventIds": [],
        "arcIds": [event_id],
        "consequences": delta,
        "happenedAtIso": datetime.now(timezone.utc).isoformat(),
        "date": date_key,
    }


def _has_persistent_delta(delta: dict) -> bool:
    if not isinstance(delta, dict):
        return False
    for key in ["factionChanges", "regionChanges", "characterChanges", "arcChanges"]:
        val = delta.get(key)
        if isinstance(val, dict) and len(val) > 0:
            return True
    return False


def sync_world_state_from_codex_and_stories(
    codex_path: str,
    date_key: str,
    stories: list[dict] | None = None,
    output_path: str = "world-state.json",
) -> dict:
    """Sync persistent world state from codex + today's stories.

    Returns a summary dict safe for logging.
    """
    codex = _load_json(codex_path, {})
    if not isinstance(codex, dict):
        return {"updated": False, "reason": "invalid_codex"}

    issue_number = _derive_issue_number(codex, date_key)
    state = _load_json(output_path, None)
    if not isinstance(state, dict) or not state:
        state = _make_initial_state(codex, issue_number)

    # Keep entity catalogs refreshed from codex so state can evolve with canon growth.
    refreshed = _make_initial_state(codex, issue_number)
    state["factions"] = refreshed.get("factions", {})
    state["regions"] = refreshed.get("regions", {})
    state["characters"] = refreshed.get("characters", {})
    for arc_id, arc in (refreshed.get("arcs") or {}).items():
        if arc_id not in (state.get("arcs") or {}):
            state.setdefault("arcs", {})[arc_id] = arc

    today_events = []
    for e in codex.get("events", []) if isinstance(codex.get("events"), list) else []:
        if not isinstance(e, dict):
            continue
        if not _event_is_updated_today(e, date_key):
            continue
        today_events.append(_story_event_from_codex_event(e, state, date_key, issue_number))

    observed = len(today_events)
    applied = 0
    for evt in today_events:
        delta = evt.get("consequences") or {}
        if _has_persistent_delta(delta):
            state = _apply_delta(state, delta, issue_number)
            applied += 1
        state.setdefault("events", []).append(evt)
        for rid in evt.get("regionIds") or []:
            reg = state.get("regions", {}).get(rid)
            if isinstance(reg, dict):
                reg.setdefault("recentEventIds", []).append(evt["id"])
                reg["recentEventIds"] = reg["recentEventIds"][-25:]

    state["issueNumber"] = issue_number
    state["updatedAt"] = datetime.now(timezone.utc).isoformat()
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=True, indent=2)

    return {
        "updated": True,
        "issue_number": issue_number,
        "events_observed": observed,
        "events_applied": applied,
        "world_state_file": output_path,
    }
