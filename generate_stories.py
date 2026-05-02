#!/usr/bin/env python3
"""
generate_stories.py
Calls the Claude API to generate 10 sword-and-sorcery stories,
saves them to stories.json (today's edition) and to archive/<date>.json.
Updates archive/index.json with the running list of available dates.
Maintains lore.json (world bible), characters.json (UI character list),
and codex.json (full entity codex: characters, places, events, weapons, artifacts).
Run daily via GitHub Actions.
"""

import os
import json
import sys
import re
import random
import hashlib
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
import anthropic
from backfill_character_temporal import refresh_character_temporal
from build_alliances import refresh_alliances
from build_lineages import refresh_lineages
from simulate_character_lifecycle import simulate_lifecycle
from world_time import build_world_clock
from world_state import sync_world_state_from_codex_and_stories


def _parse_date_key(date_key: str):
    s = (date_key or "").strip()
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except Exception:
        return None


def _maybe_load_dotenv():
    """Best-effort load of a local .env file for development.

    GitHub Actions already supplies ANTHROPIC_API_KEY via secrets, but local
    runs (different terminals, VS Code tasks, tool runners) often don't inherit
    exported env vars. This makes local behavior more consistent.
    """
    try:
        from dotenv import load_dotenv  # type: ignore
    except Exception:
        return
    load_dotenv(override=False)

# ── Text sanitation (prevents control-char tofu/rectangles in UI) ──────
_CONTROL_CHAR_RE = re.compile(r"[\x00-\x08\x0B\x0C\x0E-\x1F\x7F-\x9F]")

# ── Config ────────────────────────────────────────────────────────────────
MODEL           = "claude-haiku-4-5-20251001"
NUM_STORIES     = 10
OUTPUT_FILE     = "stories.json"
ARCHIVE_DIR     = "archive"
ARCHIVE_IDX     = "archive/index.json"
LORE_FILE       = "lore.json"
CHARACTERS_FILE = "characters.json"
CODEX_FILE      = "codex.json"
GEOGRAPHY_FILE  = "geography.json"
WORLD_STATE_FILE = "world-state.json"
CHARACTER_TEMPORAL_FILE = "character-temporal.json"
CHARACTER_LIFECYCLE_LOG_FILE = "character-lifecycle-log.json"
LINEAGES_FILE = "lineages.json"
ALLIANCES_FILE = "alliances.json"

# Issue date/timezone: used for archive filenames and 'already generated' checks.
# Default matches the previous workflow's intended schedule (US/Eastern).
ISSUE_TIMEZONE = (os.environ.get("ISSUE_TIMEZONE") or "America/New_York").strip()

# Geography / hierarchy controls
MAX_CONTINENTS = int(os.environ.get("MAX_CONTINENTS", "7"))

# World-event arcs: inject a small set of large-scale events into the generation prompt
# so multiple stories/characters can be influenced by shared pressures.
ENABLE_WORLD_EVENT_ARCS = os.environ.get("ENABLE_WORLD_EVENT_ARCS", "1").strip().lower() in {"1", "true", "yes", "y"}
WORLD_EVENT_ARCS_MAX = int(os.environ.get("WORLD_EVENT_ARCS_MAX", "2"))
# Arc persistence tuning (does not force arcs; only influences which arcs are highlighted)
WORLD_EVENT_ARC_ACTIVE_DAYS = int(os.environ.get("WORLD_EVENT_ARC_ACTIVE_DAYS", "14"))
WORLD_EVENT_ARC_INTENSITY_MAX = int(os.environ.get("WORLD_EVENT_ARC_INTENSITY_MAX", "5"))
# Event arc dossiers: summarize prior tales via an API call instead of raw text injection.
ENABLE_EVENT_ARC_DOSSIER = os.environ.get("ENABLE_EVENT_ARC_DOSSIER", "1").strip().lower() in {"1", "true", "yes", "y"}
EVENT_ARC_DOSSIER_MAX_TALES = int(os.environ.get("EVENT_ARC_DOSSIER_MAX_TALES", "0"))  # 0 = no limit
EVENT_ARC_DOSSIER_MAX_CHARS_PER_STORY = int(os.environ.get("EVENT_ARC_DOSSIER_MAX_CHARS_PER_STORY", "2000"))
EVENT_ARC_DOSSIER_MAX_TOKENS = int(os.environ.get("EVENT_ARC_DOSSIER_MAX_TOKENS", "1500"))

# Entity directory: compact one-line summaries of codex entities injected into the generation prompt.
# Uses a total character budget rather than per-category caps.  In early months every entity fits;
# once the codex outgrows the budget, only the oldest/rarest entities are dropped.
ENTITY_DIR_MAX_CHARS = int(os.environ.get("ENTITY_DIR_MAX_CHARS", "400000"))  # ~100K tokens

# Subgenres are generated dynamically by the AI for each story

# Optional lore consistency controls
ENABLE_LORE_REVISION_PASS = os.environ.get("ENABLE_LORE_REVISION_PASS", "0").strip().lower() in {"1", "true", "yes", "y"}

# Canon checker: audits for contradictions against referenced canon, and can auto-rewrite stories.
ENABLE_CANON_CHECKER = os.environ.get("ENABLE_CANON_CHECKER", "0").strip().lower() in {"1", "true", "yes", "y"}
CANON_CHECKER_MODE = os.environ.get("CANON_CHECKER_MODE", "rewrite").strip().lower()  # rewrite | report

# Content guardrails: block stories involving child death/targeted harm.
ENABLE_CHILD_HARM_GUARD = os.environ.get("ENABLE_CHILD_HARM_GUARD", "1").strip().lower() in {"1", "true", "yes", "y"}
CHILD_HARM_MAX_REWRITES = int(os.environ.get("CHILD_HARM_MAX_REWRITES", "2"))

# Content guardrails: block rape/sexual assault and explicit sex depictions.
ENABLE_SEXUAL_CONTENT_GUARD = os.environ.get("ENABLE_SEXUAL_CONTENT_GUARD", "1").strip().lower() in {"1", "true", "yes", "y"}
SEXUAL_CONTENT_MAX_REWRITES = int(os.environ.get("SEXUAL_CONTENT_MAX_REWRITES", "2"))

# Quality guardrails: reduce overused motifs in a day's set.
ENABLE_MOTIF_GUARD = os.environ.get("ENABLE_MOTIF_GUARD", "1").strip().lower() in {"1", "true", "yes", "y"}
MOTIF_MAX_REWRITES = int(os.environ.get("MOTIF_MAX_REWRITES", "1"))

# Continuity check: catch character-trait swaps (e.g. oath assigned to wrong character).
ENABLE_CONTINUITY_CHECK = os.environ.get("ENABLE_CONTINUITY_CHECK", "1").strip().lower() in {"1", "true", "yes", "y"}
CONTINUITY_MAX_REWRITES = int(os.environ.get("CONTINUITY_MAX_REWRITES", "1"))

# Existing-entity updates: extract updates for ALREADY KNOWN entities referenced today.
# This is how we can learn status changes (dead -> reanimated, etc.) even though the "NEW lore" extractor skips known names.
ENABLE_EXISTING_CHARACTER_UPDATES = os.environ.get(
    "ENABLE_EXISTING_CHARACTER_UPDATES",
    "1",
).strip().lower() in {"1", "true", "yes", "y"}

# Lore extraction: batch stories to avoid truncated output.
# Haiku 3.5 max output is 8192 tokens; a single call can't cover 10 stories × 20 categories.
EXTRACTION_BATCH_SIZE = int(os.environ.get("EXTRACTION_BATCH_SIZE", "3"))
EXTRACTION_MAX_TOKENS = int(os.environ.get("EXTRACTION_MAX_TOKENS", "8192"))

# Prompt size controls (do not limit lore growth; only limit what we *send* each run).
# Set to 0 to disable that section.
LORE_SPOTLIGHT_MAX_PER_CATEGORY = int(os.environ.get("LORE_SPOTLIGHT_MAX_PER_CATEGORY", "0"))
LORE_RANDOM_SPOTLIGHT_PER_CATEGORY = int(os.environ.get("LORE_RANDOM_SPOTLIGHT_PER_CATEGORY", "0"))

# Reuse planner: Haiku decides whether to reuse, and selects from a random candidate list.
ENABLE_REUSE_PLANNER = os.environ.get("ENABLE_REUSE_PLANNER", "1").strip().lower() in {"1", "true", "yes", "y"}
REUSE_CANDIDATES_PER_CATEGORY = int(os.environ.get("REUSE_CANDIDATES_PER_CATEGORY", "20"))
REUSE_MAX_PER_CATEGORY = int(os.environ.get("REUSE_MAX_PER_CATEGORY", "1"))
REUSE_DEFAULT_INTENSITY = os.environ.get("REUSE_DEFAULT_INTENSITY", "cameo").strip().lower()

# Reuse dossier: when we intentionally reuse an entity, scan its prior appearance tales and inject a compact dossier.
ENABLE_REUSE_DOSSIER = os.environ.get("ENABLE_REUSE_DOSSIER", "1").strip().lower() in {"1", "true", "yes", "y"}
REUSE_DOSSIER_MAX_APPEARANCES = int(os.environ.get("REUSE_DOSSIER_MAX_APPEARANCES", "0"))  # 0 = no limit (full backstory)
REUSE_DOSSIER_MAX_CHARS_PER_STORY = int(os.environ.get("REUSE_DOSSIER_MAX_CHARS_PER_STORY", "4000"))
REUSE_DOSSIER_MAX_TOKENS = int(os.environ.get("REUSE_DOSSIER_MAX_TOKENS", "1200"))
# Progressive dossier scaling: once total tale input exceeds this char limit,
# older tales are progressively clipped so recent tales stay full-fidelity.
REUSE_DOSSIER_MAX_TOTAL_INPUT_CHARS = int(os.environ.get("REUSE_DOSSIER_MAX_TOTAL_INPUT_CHARS", "80000"))  # ~20K tokens
REUSE_ALLOWED_CATEGORIES_RAW = os.environ.get("REUSE_ALLOWED_CATEGORIES", "all").strip()


def get_reuse_allowed_categories(lore: dict):
    """Return the categories that the reuse planner may consider.

    By default this is "all" list-like categories present in lore.
    Set REUSE_ALLOWED_CATEGORIES to a comma-separated list to restrict.
    """
    raw = (REUSE_ALLOWED_CATEGORIES_RAW or "all").strip()
    if not raw or raw.lower() in {"all", "*"}:
        cats = []
        if isinstance(lore, dict):
            for k, v in lore.items():
                if k in {"worlds", "version", "last_updated"}:
                    continue
                if isinstance(v, list):
                    cats.append(k)
        return sorted(set(cats))
    return [c.strip() for c in raw.split(",") if c.strip()]

# ── Lore helpers ──────────────────────────────────────────────────────────
def load_lore():
    """Load the existing lore bible, or return a minimal skeleton."""
    if os.path.exists(LORE_FILE):
        with open(LORE_FILE, "r", encoding="utf-8") as f:
            lore = json.load(f)
            if isinstance(lore, dict):
                lore.pop("subcontinents", None)
            return lore
    return {
        "version": "1.0",
        "worlds": [],
        "hemispheres": [],
        "continents": [],
        "realms": [],
        "polities": [],
        "provinces": [],
        "districts": [],
        "characters": [],
        "places": [],
        "events": [],
        "weapons": [],
        "deities_and_entities": [],
        "artifacts": [],
        "factions": [],
        "lore": [],
        "flora_fauna": [],
        "magic": [],
        "relics": [],
        "regions": [],
        "substances": []
    }


def load_geography():
    """Load the geography file, or return an empty skeleton."""
    if os.path.exists(GEOGRAPHY_FILE):
        with open(GEOGRAPHY_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def seed_geo_entities_from_geography(lore: dict, geo: dict) -> dict:
    """Seed hemisphere/continent entities into lore from geography.json.

    Purpose: Continents/hemispheres are canonical facts and should exist in the
    codex even if today's stories don't explicitly name them.
    """
    if not isinstance(lore, dict) or not isinstance(geo, dict):
        return lore

    lore.setdefault("hemispheres", [])
    lore.setdefault("continents", [])

    hemispheres = geo.get("hemispheres")
    continents = geo.get("continents")
    if not isinstance(hemispheres, list):
        hemispheres = []
    if not isinstance(continents, list):
        continents = []

    # Index existing lore entities by name (case-insensitive).
    hemi_by_name = {
        str(h.get("name") or "").strip().lower(): h
        for h in (lore.get("hemispheres") or [])
        if isinstance(h, dict) and str(h.get("name") or "").strip()
    }
    cont_by_name = {
        str(c.get("name") or "").strip().lower(): c
        for c in (lore.get("continents") or [])
        if isinstance(c, dict) and str(c.get("name") or "").strip()
    }

    hemi_name_by_id = {
        str(h.get("id") or "").strip(): str(h.get("name") or "").strip()
        for h in hemispheres
        if isinstance(h, dict) and str(h.get("id") or "").strip() and str(h.get("name") or "").strip()
    }

    def _fill_missing(target: dict, key: str, value):
        if value in (None, "", [], {}):
            return
        if key not in target or target.get(key) in (None, "", [], {}):
            target[key] = value

    # Seed hemispheres
    for h in hemispheres:
        if not isinstance(h, dict):
            continue
        name = str(h.get("name") or "").strip()
        if not name:
            continue
        entry = hemi_by_name.get(name.lower())
        if entry is None:
            entry = {
                "id": str(h.get("id") or _make_snake_id(name)),
                "name": name,
                "tagline": "",
                "description": str(h.get("description") or "").strip(),
                "function": "Global climate/season logic.",
                "status": "known",
                "notes": "",
            }
            lore["hemispheres"].append(entry)
            hemi_by_name[name.lower()] = entry

        _fill_missing(entry, "id", str(h.get("id") or _make_snake_id(name)))
        _fill_missing(entry, "description", str(h.get("description") or "").strip())
        climate_band = str(h.get("climate_band") or "").strip()
        if climate_band:
            _fill_missing(entry, "notes", f"Climate band: {climate_band}.")

    # Seed continents (bounded)
    max_n = max(0, int(MAX_CONTINENTS or 0))
    for c in continents[:max_n] if max_n else continents:
        if not isinstance(c, dict):
            continue
        name = str(c.get("name") or "").strip()
        if not name:
            continue

        entry = cont_by_name.get(name.lower())
        if entry is None:
            entry = {
                "id": str(c.get("id") or _make_snake_id(name)),
                "name": name,
                "tagline": "",
                "description": str(c.get("description") or "").strip(),
                "hemispheres": [hemi_name_by_id.get(hid, hid) for hid in (c.get("hemispheres") or []) if str(hid or "").strip()],
                "climate_zones": c.get("climate_zones", []) if isinstance(c.get("climate_zones"), list) else [],
                "function": "Macro-biome and travel logic.",
                "status": str(c.get("status") or "unknown").strip() or "unknown",
                "notes": "",
            }
            lore["continents"].append(entry)
            cont_by_name[name.lower()] = entry

        _fill_missing(entry, "id", str(c.get("id") or _make_snake_id(name)))
        _fill_missing(entry, "description", str(c.get("description") or "").strip())
        _fill_missing(entry, "status", str(c.get("status") or "unknown").strip() or "unknown")
        hz = [hemi_name_by_id.get(hid, hid) for hid in (c.get("hemispheres") or []) if str(hid or "").strip()]
        if hz:
            _fill_missing(entry, "hemispheres", hz)
        cz = c.get("climate_zones") if isinstance(c.get("climate_zones"), list) else []
        if cz:
            _fill_missing(entry, "climate_zones", cz)
    return lore


def build_geography_context(geo):
    """Build a concise geography summary for the generation prompt.

    Gives the model spatial awareness of the world: continents, macro-regions,
    natural features, and established place→region assignments so it can write
    geographically consistent stories.
    """
    if not geo or not isinstance(geo, dict):
        return ""

    lines = []
    planet = geo.get("planet", {})
    if planet.get("name"):
        lines.append(f"=== GEOGRAPHY OF {planet['name'].upper()} ===")
        lines.append(f"Planet: {planet['name']} — {planet.get('description', '')[:200]}")
        lines.append("")

    # Continents summary
    continents = geo.get("continents", [])
    if continents:
        known = [c for c in continents if c.get("status") == "explored"]
        other = [c for c in continents if c.get("status") != "explored"]
        if known:
            for c in known:
                lines.append(f"Known Continent: {c['name']} — {c.get('description', '')[:150]}")
                gdim = c.get("geo_dimensions", {})
                if gdim:
                    lines.append(
                        f"  Scale: ~{gdim.get('width_miles', '?')} miles wide × "
                        f"~{gdim.get('height_miles', '?')} miles tall "
                        f"(roughly the size of Australia). "
                        f"Use realistic travel times: a horse covers ~30 mi/day, "
                        f"a caravan ~15 mi/day, a ship ~100 mi/day."
                    )
        if other:
            other_names = ", ".join(
                f"{c['name']} ({c.get('status', 'unknown')})"
                for c in other
            )
            lines.append(f"Other continents (mostly unexplored): {other_names}")
        lines.append("")

    # Macro-regions on the known continent
    regions = geo.get("macro_regions", [])
    if regions:
        lines.append("Macro-Regions of the known continent (use these for geographic placement):")
        for r in regions:
            lines.append(
                f"• {r['name']} — {r.get('climate', '?')}. "
                f"{r.get('description', '')[:120]}"
            )
        lines.append("")

    # Natural features
    features = geo.get("natural_features", [])
    if features:
        lines.append("Key Natural Features:")
        for f in features:
            ftype = f.get("type", "feature")
            lines.append(f"• {f['name']} ({ftype}) — {f.get('description', '')[:100]}")
        lines.append("")

    # Place→region assignments (only high/medium confidence)
    assignments = geo.get("place_assignments", [])
    if assignments:
        region_map = {}
        for a in assignments:
            conf = (a.get("confidence") or "").lower()
            if conf not in {"high", "medium"}:
                continue
            rname = a.get("macro_region", "unknown")
            pname = a.get("place_name", "")
            if pname:
                region_map.setdefault(rname, []).append(pname)
        if region_map:
            # Look up region display names
            region_display = {r["id"]: r["name"] for r in regions}
            lines.append("Established Place → Region assignments (respect these):")
            for rid, places in sorted(region_map.items()):
                display = region_display.get(rid, rid)
                lines.append(f"• {display}: {', '.join(places)}")
            lines.append("")

    # Routes
    routes = geo.get("routes", [])
    if routes:
        lines.append("Known Routes:")
        for rt in routes:
            dist = rt.get('distance_miles')
            travel = rt.get('travel_note', '')
            dist_str = f", ~{dist} mi" if dist else ""
            travel_str = f" — {travel}" if travel else ""
            lines.append(
                f"• {rt['name']}: {rt.get('from_place', '?')} → {rt.get('to_place', '?')} "
                f"({rt.get('type', 'road')}{dist_str}){travel_str}"
            )
        lines.append("")

    lines.append("GEOGRAPHIC RULES:")
    lines.append("- When placing a story, specify which macro-region it occurs in when feasible.")
    lines.append("- Respect established place→region assignments listed above.")
    lines.append("- New places should fit logically into the existing geographic framework.")
    lines.append("- Mention terrain, climate, or landmarks that match the region's description.")
    lines.append("- Use realistic distances and travel times consistent with the continent's scale.")
    lines.append("  E.g. Pelimor to the Ashen Wastes is ~600 miles (3-4 weeks by caravan).")
    lines.append("  Crossing Valdris coast-to-coast is ~2500 miles — a journey of many months.")

    return "\n".join(lines).strip()


def _truthy_non_unknown(val: str) -> bool:
    v = (val or "").strip()
    if not v:
        return False
    return v.lower() not in {"unknown", "n/a", "na", "none"}


def sanitize_text(value: str) -> str:
    if value is None:
        return ""
    s = str(value)
    s = s.replace("\r\n", "\n").replace("\r", "\n")
    return _CONTROL_CHAR_RE.sub("", s)


def sanitize_stories(stories):
    out = []
    for s in (stories or []):
        if not isinstance(s, dict):
            continue
        out.append({
            "title": sanitize_text(s.get("title", "Untitled")),
            "text": sanitize_text(s.get("text", "")),
            "subgenre": sanitize_text(s.get("subgenre", "Sword & Sorcery")),
        })
    return out


def _make_snake_id(name: str) -> str:
    base = re.sub(r"[^a-zA-Z0-9]+", "_", (name or "").strip().lower()).strip("_")
    return base or "unknown"


def ensure_home_location_entities_exist(lore, date_key: str):
    """Auto-create placeholder geo entities referenced by character home_* anchors.

    This reduces UX dead-ends when a story reveals a new domain (e.g., a region someone rules)
    via a character update, but the NEW-lore extractor fails to add that region/place yet.
    """
    if not isinstance(lore, dict):
        return lore

    places = lore.get("places") or []
    regions = lore.get("regions") or []
    realms = lore.get("realms") or []

    place_names = {str(p.get("name", "")).strip().lower() for p in places if isinstance(p, dict) and p.get("name")}
    region_names = {str(r.get("name", "")).strip().lower() for r in regions if isinstance(r, dict) and r.get("name")}
    realm_names = {str(rm.get("name", "")).strip().lower() for rm in realms if isinstance(rm, dict) and rm.get("name")}

    for c in lore.get("characters") or []:
        if not isinstance(c, dict):
            continue

        home_place = sanitize_text(c.get("home_place", "")).strip()
        home_region = sanitize_text(c.get("home_region", "")).strip()
        home_realm = sanitize_text(c.get("home_realm", "")).strip()

        if _truthy_non_unknown(home_realm) and home_realm.lower() not in realm_names:
            realms.append({
                "id": _make_snake_id(home_realm),
                "name": home_realm,
                "tagline": "",
                "continent": "unknown",
                "capital": "unknown",
                "function": "Auto-added from character home_realm.",
                "taxation": "unknown",
                "military": "unknown",
                "status": "unknown",
                "notes": "",
                "first_date": date_key,
                "appearances": 1,
            })
            realm_names.add(home_realm.lower())

        if _truthy_non_unknown(home_region) and home_region.lower() not in region_names:
            regions.append({
                "id": _make_snake_id(home_region),
                "name": home_region,
                "tagline": "",
                "continent": "unknown",
                "realm": home_realm if _truthy_non_unknown(home_realm) else "unknown",
                "climate": "unknown",
                "terrain": "unknown",
                "function": "Auto-added from character home_region.",
                "status": "unknown",
                "notes": "",
                "first_date": date_key,
                "appearances": 1,
            })
            region_names.add(home_region.lower())

        if _truthy_non_unknown(home_place) and home_place.lower() not in place_names:
            places.append({
                "id": _make_snake_id(home_place),
                "name": home_place,
                "tagline": "",
                "place_type": "unknown",
                "world": "known_world",
                "hemisphere": "unknown",
                "continent": "unknown",
                "realm": home_realm if _truthy_non_unknown(home_realm) else "unknown",
                "province": "unknown",
                "region": home_region if _truthy_non_unknown(home_region) else "unknown",
                "district": "unknown",
                "atmosphere": "",
                "description": "Auto-added from character home_place.",
                "status": "unknown",
                "notes": "",
                "first_date": date_key,
                "appearances": 1,
            })
            place_names.add(home_place.lower())

    lore["places"] = places
    lore["regions"] = regions
    lore["realms"] = realms
    return lore


def ensure_place_parent_chain(lore: dict):
    """Ensure geo entities declare a complete parent chain (may be 'unknown').

    Applies to:
    - places (parents only)
    - regions/districts/provinces/realms/continents/hemispheres
    
    Notes:
    - We don't force a specific 'world' name; we only ensure the field exists.
    - For geo categories (e.g., realms), we default the self-level field (realm) to item.name if missing.
    """
    if not isinstance(lore, dict):
        return lore

    def _ensure_chain(obj: dict):
        obj.setdefault("world", "The Known World")
        if not _truthy_non_unknown(obj.get("world") or ""):
            obj["world"] = "The Known World"
        obj.setdefault("hemisphere", "unknown")
        obj.setdefault("continent", "unknown")
        obj.setdefault("realm", "unknown")
        obj.setdefault("province", "unknown")
        obj.setdefault("region", "unknown")
        obj.setdefault("district", "unknown")

    # Places: parent chain only.
    places = lore.get("places")
    if isinstance(places, list):
        for p in places:
            if isinstance(p, dict):
                _ensure_chain(p)

    # Geo hierarchy categories: chain + self-level field.
    self_field_by_cat = {
        "hemispheres": "hemisphere",
        "continents": "continent",
        "realms": "realm",
        "provinces": "province",
        "districts": "district",
        "regions": "region",
    }
    for cat, self_field in self_field_by_cat.items():
        items = lore.get(cat)
        if not isinstance(items, list):
            continue
        for it in items:
            if not isinstance(it, dict):
                continue
            _ensure_chain(it)
            nm = (it.get("name") or "").strip()
            if nm and not _truthy_non_unknown(it.get(self_field) or ""):
                it[self_field] = nm
    return lore


def enforce_continent_limit(lore: dict):
    """Keep continent count bounded and prevent places from referencing trimmed continents."""
    if not isinstance(lore, dict):
        return lore
    conts = lore.get("continents")
    if not isinstance(conts, list):
        conts = []
        lore["continents"] = conts

    max_n = max(0, int(MAX_CONTINENTS or 0))
    if max_n and len(conts) > max_n:
        conts[:] = conts[:max_n]

    allowed = {((c.get("name") or "").strip().lower()) for c in conts if isinstance(c, dict) and (c.get("name") or "").strip()}
    if not allowed:
        return lore

    for p in lore.get("places", []) or []:
        if not isinstance(p, dict):
            continue
        continent = (p.get("continent") or "").strip()
        if _truthy_non_unknown(continent) and continent.lower() not in allowed:
            p["continent"] = "unknown"

    for r in lore.get("regions", []) or []:
        if not isinstance(r, dict):
            continue
        continent = (r.get("continent") or "").strip()
        if _truthy_non_unknown(continent) and continent.lower() not in allowed:
            r["continent"] = "unknown"

    for realm in lore.get("realms", []) or []:
        if not isinstance(realm, dict):
            continue
        continent = (realm.get("continent") or "").strip()
        if _truthy_non_unknown(continent) and continent.lower() not in allowed:
            realm["continent"] = "unknown"

    return lore

def save_lore(lore, date_key):
    lore["last_updated"] = date_key
    with open(LORE_FILE, "w", encoding="utf-8") as f:
        json.dump(lore, f, ensure_ascii=True, indent=2)

def build_lore_context(lore):
    """Format the lore bible into a concise prompt string for the story generator."""
    lines = []

    # Worlds
    if lore.get("worlds"):
        lines.append("=== WORLDS ===")
        for w in lore["worlds"]:
            lines.append(f"• {w['name']}: {w['description']}")
        lines.append("")

    # Lore rules (from first world, if present)
    if lore.get("worlds") and lore["worlds"][0].get("rules"):
        lines.append("=== LORE RULES (must be respected) ===")
        for rule in lore["worlds"][0]["rules"]:
            lines.append(f"• {rule}")
        lines.append("")

    # Reserved character names
    if lore.get("characters"):
        lines.append("=== EXISTING CHARACTERS (reserved names — you may reuse these characters, but their established lore must be respected) ===")
        for c in lore["characters"]:
            status_note = f" [{c.get('status', 'unknown')}]" if c.get('status') else ""
            bio_short = c.get('bio', '')[:200]
            lines.append(f"• {c['name']} ({c.get('role','?')}){status_note}: {bio_short}")
        lines.append("")

    # Reserved place names
    if lore.get("places"):
        lines.append("=== EXISTING PLACES (reserved names — you may revisit these, but their established lore must be respected) ===")
        for p in lore["places"]:
            lines.append(f"• {p['name']}: {p.get('description','')[:150]}")
        lines.append("")

    # Deities and entities
    if lore.get("deities_and_entities"):
        lines.append("=== DEITIES & ENTITIES ===")
        for d in lore["deities_and_entities"]:
            lines.append(f"• {d['name']} ({d.get('type','entity')}): {d.get('description','')[:150]}")
        lines.append("")

    # Artifacts
    if lore.get("artifacts"):
        lines.append("=== ARTIFACTS ===")
        for a in lore["artifacts"]:
            lines.append(f"• {a['name']}: {a.get('description','')[:150]}")
        lines.append("")

    return "\n".join(lines)


def _safe_sorted_by_appearances(items):
    def _key(x):
        try:
            return int(x.get("appearances", 0) or 0)
        except Exception:
            return 0
    return sorted(items or [], key=_key, reverse=True)


def _stable_seed_int(seed_text: str, salt: str) -> int:
    payload = (seed_text or "") + "|" + (salt or "")
    digest = hashlib.sha256(payload.encode("utf-8")).digest()
    # Use 64 bits for deterministic RNG seed.
    return int.from_bytes(digest[:8], "big", signed=False)


def _sample_candidates(items, k: int, seed_text: str, salt: str):
    items = [x for x in (items or []) if isinstance(x, dict) and (x.get("name") or "").strip()]
    if not items or k <= 0:
        return []
    rng = random.Random(_stable_seed_int(seed_text, salt))
    k = min(int(k), len(items))
    return rng.sample(items, k=k)


def build_reuse_plan_prompt(today_str, world_date_label, lore, candidates_by_category):
    """Ask the model whether to reuse canon today, and if so pick from provided candidates."""
    world = lore.get("worlds", [{}])[0] if lore.get("worlds") else {}
    rules = world.get("rules", []) if isinstance(world.get("rules", []), list) else []
    rules_block = "\n".join([f"- {r}" for r in rules]) if rules else "- (none)"

    candidate_lines = []
    for cat, items in (candidates_by_category or {}).items():
        if not items:
            continue
        candidate_lines.append(f"=== {cat.upper()} CANDIDATES ===")
        for it in items:
            name = (it.get("name") or "").strip()
            if not name:
                continue
            hint = (it.get("tagline") or it.get("role") or it.get("place_type") or it.get("artifact_type") or it.get("weapon_type") or "").strip()
            if hint:
                candidate_lines.append(f"- {name} — {hint}")
            else:
                candidate_lines.append(f"- {name}")
        candidate_lines.append("")

    allowed_cats = ", ".join(sorted(candidates_by_category.keys())) if candidates_by_category else "(none)"

    return f"""You are planning today's issue of an ongoing sword-and-sorcery universe.

Archive date: {today_str}
World date in Edhra: {world_date_label}

Canon rules:
{rules_block}

Decision: for today's 10 stories, should we intentionally reuse any existing canon entities (characters/places/relics/etc), or tell entirely new tales?

Important:
- Reuse is optional.
- If you choose reuse, pick ONLY from the candidates listed below.
- Keep reuse light: at most {REUSE_MAX_PER_CATEGORY} selection(s) per category.

Available categories today: {allowed_cats}

Reuse intensity:
- cameo: light touch; brief presence or mention; do NOT make them the protagonist or primary location; avoid major new canon changes.
- central: the entity can meaningfully drive plot; still respect established canon.

Return ONLY valid JSON like:
{{
  "reuse": true,
  "selections": {{
        "characters": [{{"name": "Name", "intensity": "cameo"}}],
        "places": [{{"name": "Name", "intensity": "cameo"}}],
        "relics": [{{"name": "Name", "intensity": "cameo"}}]
  }},
  "rationale": "1-2 sentences"
}}

Notes:
- For backwards compatibility, each selection may also be a bare string "Name".
- If intensity is omitted, default to "{REUSE_DEFAULT_INTENSITY}".

CANDIDATES:
{os.linesep.join(candidate_lines).strip()}
"""


def normalize_reuse_plan(plan, candidates_by_category):
    """Ensure planner output is safe: selections must be within candidates and within max counts."""
    safe = {"reuse": False, "selections": {}, "rationale": ""}
    if not isinstance(plan, dict):
        return safe
    safe["reuse"] = bool(plan.get("reuse"))
    safe["rationale"] = (plan.get("rationale") or "").strip()[:400]
    selections = plan.get("selections") if isinstance(plan.get("selections"), dict) else {}

    candidate_name_sets = {}
    for cat, items in (candidates_by_category or {}).items():
        candidate_name_sets[cat] = { (it.get("name") or "").strip().lower() for it in (items or []) if (it.get("name") or "").strip() }

    def _normalize_intensity(raw: str) -> str:
        val = (raw or "").strip().lower()
        if val in {"cameo", "central"}:
            return val
        return (REUSE_DEFAULT_INTENSITY or "cameo") if (REUSE_DEFAULT_INTENSITY or "") in {"cameo", "central"} else "cameo"

    out = {}
    for cat, raw_list in selections.items():
        if cat not in candidate_name_sets:
            continue
        if not isinstance(raw_list, list):
            continue

        picked = []
        picked_names_lower = set()
        for entry in raw_list:
            name = ""
            intensity = ""
            if isinstance(entry, str):
                name = entry
            elif isinstance(entry, dict):
                name = entry.get("name") or ""
                intensity = entry.get("intensity") or ""
            else:
                continue

            nm = (name or "").strip()
            if not nm:
                continue
            if nm.lower() not in candidate_name_sets[cat]:
                continue
            if nm.lower() in picked_names_lower:
                continue

            picked.append({
                "name": nm,
                "intensity": _normalize_intensity(intensity),
            })
            picked_names_lower.add(nm.lower())
            if len(picked) >= REUSE_MAX_PER_CATEGORY:
                break

        if picked:
            out[cat] = picked

    safe["selections"] = out
    if not out:
        safe["reuse"] = False
    return safe


def get_full_canon_entries_for_selections(lore, selections):
    """Return full lore entries for selected names by category."""
    out = {}
    if not isinstance(selections, dict):
        return out
    for cat, names in selections.items():
        if not isinstance(names, list) or not names:
            continue
        lore_items = lore.get(cat, []) or []
        idx = { (it.get("name") or "").strip().lower(): it for it in lore_items if isinstance(it, dict) and (it.get("name") or "").strip() }
        picked = []
        for n in names:
            name = ""
            if isinstance(n, str):
                name = n
            elif isinstance(n, dict):
                name = n.get("name") or ""
            it = idx.get((name or "").strip().lower())
            if it:
                picked.append(it)
        if picked:
            out[cat] = picked
    return out


def load_codex_file():
    if os.path.exists(CODEX_FILE):
        try:
            with open(CODEX_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


CODEX_BALANCE_TRACKED_LABELS = [
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


CODEX_BALANCE_CONTEXT_HINTS = {
    "weapons": [
        "war, duels, champions, raids, militias, mercenaries, border conflict",
        "forging/craft traditions, inheritance, discovery in ruins, ceremonial arming, oath-binding, magical creation",
        "named armaments with lineage, prior wielders, famous battles, makers, or transformative handoffs",
    ],
    "rituals": [
        "temples, cults, coronations, funerals, oaths, celestial events, curses, healing",
        "seasonal observances and codified rites worth repeating in future lore",
    ],
    "substances": [
        "alchemy, healing, mining, trade, poison, dyes, incense, fuel, resins, powders",
        "named ingredients/materials with identifiable properties and usage",
    ],
    "artifacts": [
        "ruins, inheritance, vaults, expeditions, dynastic relic chains",
        "distinct non-weapon objects with provenance and consequence",
    ],
    "relics": [
        "old wars, temple holdings, saintly remains, cursed heirlooms",
        "named objects with enduring mythic/religious/occult significance",
    ],
    "flora_fauna": [
        "wilderness, trade caravans, hunting grounds, druidic circles, monster routes",
        "named species/variants that recur beyond a one-off mention",
    ],
    "polities": [
        "crowns, councils, regencies, succession disputes, tax edicts, border law",
        "named governing institutions tied to a realm or seat",
    ],
    "provinces": [
        "tax zones, marches, governorships, logistics and jurisdiction",
        "named territorial units that sit between realm and district",
    ],
    "districts": [
        "city wards, quarters, neighborhoods, docks, markets, temple rows",
        "named intra-city areas with clear function",
    ],
}

CODEX_BALANCE_PRIORITY_ORDER = [
    "weapons",
    "flora_fauna",
    "districts",
]


def _list_named_count(items) -> int:
    if not isinstance(items, list):
        return 0
    return sum(
        1
        for it in items
        if isinstance(it, dict) and str(it.get("name") or "").strip()
    )


def summarize_codex_label_balance(codex: dict, tracked_labels=None) -> dict:
    labels = [x for x in (tracked_labels or CODEX_BALANCE_TRACKED_LABELS) if str(x).strip()]
    counts = {}
    for label in labels:
        counts[label] = _list_named_count((codex or {}).get(label, [])) if isinstance(codex, dict) else 0

    values = sorted(counts.values())
    if not values:
        median_count = 0
    elif len(values) % 2 == 1:
        median_count = values[len(values) // 2]
    else:
        median_count = (values[len(values) // 2 - 1] + values[len(values) // 2]) / 2.0

    target_min_count = max(3, int(round(max(1.0, float(median_count)) * 0.60)))

    underrepresented = []
    for label in labels:
        c = int(counts.get(label, 0))
        if c >= target_min_count:
            continue
        deficit = target_min_count - c
        if c <= int(target_min_count * 0.35):
            severity = "high"
        elif c <= int(target_min_count * 0.65):
            severity = "medium"
        else:
            severity = "low"
        underrepresented.append(
            {
                "label": label,
                "count": c,
                "target_min_count": target_min_count,
                "deficit": deficit,
                "severity": severity,
                "context_hints": CODEX_BALANCE_CONTEXT_HINTS.get(label, []),
            }
        )

    underrepresented.sort(key=lambda x: (int(x.get("deficit") or 0), str(x.get("label") or "")), reverse=True)

    return {
        "tracked_labels": labels,
        "counts": counts,
        "median_count": median_count,
        "target_min_count": target_min_count,
        "underrepresented": underrepresented,
    }


def build_codex_balance_guidance_section(codex_balance: dict) -> str:
    if not isinstance(codex_balance, dict):
        return ""
    weak = codex_balance.get("underrepresented")
    if not isinstance(weak, list) or not weak:
        return ""

    severity_rank = {"high": 0, "medium": 1, "low": 2}
    priority_rank = {label: idx for idx, label in enumerate(CODEX_BALANCE_PRIORITY_ORDER)}

    def _ranked_rows(rows: list[dict]) -> list[dict]:
        def _sort_key(row: dict):
            label = str(row.get("label") or "").strip()
            severity = str(row.get("severity") or "low").strip().lower()
            deficit = int(row.get("deficit") or 0)
            if label in priority_rank:
                return (0, priority_rank[label], severity_rank.get(severity, 9), -deficit, label)
            return (1, severity_rank.get(severity, 9), -deficit, label)

        return sorted((row for row in rows if isinstance(row, dict)), key=_sort_key)

    ranked = _ranked_rows(weak)
    ranked_labels = {str(row.get("label") or "").strip(): row for row in ranked}

    lines = []
    lines.append("CODEX LABEL BALANCE SIGNALS (soft priority, quality-first):")
    lines.append("- Some codex labels are underrepresented. Increase opportunities ONLY when they naturally fit the story context.")
    lines.append("- Do not force filler entries. Only introduce a new codex entity when it is distinct, named, and worth recurring continuity.")
    lines.append("- Context cues are examples, not limits. Creative authority remains with the model; use any organic context that supports a meaningful named entity.")
    if any(label in ranked_labels for label in CODEX_BALANCE_PRIORITY_ORDER):
        ordered = [label for label in CODEX_BALANCE_PRIORITY_ORDER if label in ranked_labels]
        lines.append(f"- If several natural opportunities compete today, prefer them in this order: {', '.join(ordered)}.")

    weapon_row = ranked_labels.get("weapons")
    if weapon_row:
        weapon_severity = str(weapon_row.get("severity") or "").lower()
        if weapon_severity == "high":
            lines.append(
                "- Weapons emphasis: let about 2 stories naturally feature a DISTINCT named storied weapon that matters to reputation, inheritance, conflict, or handoff."
            )
        elif weapon_severity == "medium":
            lines.append(
                "- Weapons emphasis: let about 1 story naturally hinge on a DISTINCT named storied weapon with clear lineage, consequence, or handoff."
            )
        else:
            lines.append(
                "- Weapons maintenance: if a story already wants a storied armament, lean into it, but do not chase a daily weapon quota."
            )

    flora_row = ranked_labels.get("flora_fauna")
    if flora_row:
        flora_severity = str(flora_row.get("severity") or "").lower()
        if flora_severity in {"high", "medium"}:
            lines.append(
                "- Flora & fauna emphasis: let about 1 story naturally include a named species, beast, grove, tree-kind, or dangerous plant that materially affects travel, trade, survival, wonder, or magic."
            )
            lines.append(
                "- Forested, overgrown, druidic, caravan-route, or beast-haunted settings are especially useful when they fit the issue naturally."
            )
        else:
            lines.append(
                "- Flora & fauna maintenance: when wilderness or trade-route stories already fit the issue, prefer a named creature or plant with a real narrative role."
            )

    district_row = ranked_labels.get("districts")
    if district_row:
        district_severity = str(district_row.get("severity") or "").lower()
        if district_severity == "high":
            lines.append(
                "- District emphasis: when a city appears, make about 1 story naturally hinge on a named ward/quarter/market/dock/temple row whose function matters to the conflict."
            )
        else:
            lines.append(
                "- District maintenance: whenever a city story already exists, prefer a named ward, quarter, market, dock, or temple row when it fits naturally."
            )

    top = ranked[:6]
    for row in top:
        label = str(row.get("label") or "").strip()
        count = int(row.get("count") or 0)
        target = int(row.get("target_min_count") or 0)
        severity = str(row.get("severity") or "low")
        lines.append(f"- Priority label: {label} ({count} entries; target floor ~{target}; severity={severity}).")
        hints = row.get("context_hints") if isinstance(row.get("context_hints"), list) else []
        for h in hints[:2]:
            if str(h).strip():
                lines.append(f"  - Natural-fit cue: {str(h).strip()}")

    lines.append("- If no natural context exists, skip the label. Story quality and coherence outrank balancing quotas.")
    return "\n".join(lines).strip()


def _codex_entry_map(codex):
    out = {}
    if not isinstance(codex, dict):
        return out
    for cat, items in codex.items():
        if not isinstance(items, list):
            continue
        idx = {}
        for it in items:
            if not isinstance(it, dict):
                continue
            nm = (it.get("name") or "").strip()
            if nm:
                idx[nm.lower()] = it
        if idx:
            out[cat] = idx
    return out


def _load_archive_day(date_key: str):
    path = os.path.join(ARCHIVE_DIR, f"{date_key}.json")
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def load_story_by_date_and_title(date_key: str, title: str):
    day = _load_archive_day(date_key)
    stories = day.get("stories") if isinstance(day, dict) else None
    if not isinstance(stories, list):
        return None
    wanted = (title or "").strip().lower()
    for s in stories:
        if not isinstance(s, dict):
            continue
        if (s.get("title") or "").strip().lower() == wanted:
            return s
    return None


# ── Recent-theme lookback (cross-day diversity) ───────────────────────────
RECENT_THEME_LOOKBACK_DAYS = 3          # how many past issues to scan


def _get_recent_story_themes(today_str: str, lookback_days: int = RECENT_THEME_LOOKBACK_DAYS) -> str:
    """Return a compact summary of titles + subgenres from the last N archived days.

    Used to inject cross-day diversity awareness into the generation prompt so
    the model avoids repeating the same core concepts every day.
    """
    all_dates = _load_known_issue_dates()
    # Exclude today (may not be archived yet) and take the last N.
    past_dates = [d for d in all_dates if d < today_str][-lookback_days:]
    if not past_dates:
        return ""

    lines: list[str] = []
    for dkey in past_dates:
        day = _load_archive_day(dkey)
        stories = day.get("stories") if isinstance(day, dict) else None
        if not isinstance(stories, list):
            continue
        titles_subs = []
        for s in stories:
            if not isinstance(s, dict):
                continue
            t = (s.get("title") or "").strip()
            sg = (s.get("subgenre") or "").strip()
            if t:
                titles_subs.append(f'"{t}" [{sg}]' if sg else f'"{t}"')
        if titles_subs:
            lines.append(f"{dkey}: {', '.join(titles_subs)}")

    return "\n".join(lines)


def _clip_text(text: str, max_chars: int) -> str:
    t = text or ""
    max_chars = int(max_chars or 0)
    if max_chars <= 0 or len(t) <= max_chars:
        return t
    # Keep head+tail for some context.
    head = int(max_chars * 0.7)
    tail = max_chars - head
    return t[:head].rstrip() + "\n…\n" + t[-tail:].lstrip()


def gather_prior_tales_for_entity(
    codex_entry,
    max_appearances: int,
    max_chars_per_story: int,
    max_total_chars: int = 0,
):
    """Gather prior tales for an entity, with progressive clipping for scale.

    Parameters
    ----------
    codex_entry : dict
        The codex entry with ``story_appearances``.
    max_appearances : int
        Max number of appearances to load.  0 = no limit.
    max_chars_per_story : int
        Initial per-story text clip limit.
    max_total_chars : int
        Soft cap on the combined text of all returned tales.  When exceeded,
        older tales are progressively clipped (keeping the most recent tales
        at full fidelity) so the total fits within budget.  0 = no limit.
    """
    apps = codex_entry.get("story_appearances") if isinstance(codex_entry, dict) else None
    if not isinstance(apps, list) or not apps:
        return []

    max_appearances = int(max_appearances or 0)
    if max_appearances > 0:
        apps = apps[-max_appearances:]

    out = []
    for app in apps:
        if not isinstance(app, dict):
            continue
        date_key = (app.get("date") or "").strip()
        title = (app.get("title") or "").strip()
        if not date_key or not title:
            continue
        story = load_story_by_date_and_title(date_key, title)
        if not story:
            continue
        out.append({
            "date": date_key,
            "title": story.get("title", title),
            "subgenre": story.get("subgenre", ""),
            "text": _clip_text(story.get("text", ""), max_chars_per_story),
        })

    # ── Progressive clipping: keep recent tales full, clip older ones ──
    max_total_chars = int(max_total_chars or 0)
    if max_total_chars > 0 and len(out) > 1:
        total = sum(len(t.get("text", "")) for t in out)
        if total > max_total_chars:
            # Strategy: split tales into "recent" (last 30%) kept at full
            # fidelity, and "older" (first 70%) that get progressively
            # clipped.  Oldest tales get clipped the most.
            recent_count = max(1, len(out) // 3)  # keep ~33% recent at full
            older = out[:-recent_count]
            recent = out[-recent_count:]
            recent_chars = sum(len(t.get("text", "")) for t in recent)
            budget_for_older = max(0, max_total_chars - recent_chars)

            if budget_for_older <= 0:
                # Even recent tales exceed budget — clip them uniformly
                per_story = max(200, max_total_chars // len(recent))
                for t in recent:
                    t["text"] = _clip_text(t["text"], per_story)
                # Drop all older tales — metadata only
                for t in older:
                    t["text"] = f"[{t['date']}] (tale omitted for brevity)"
            else:
                # Distribute budget_for_older across older tales with a
                # linear ramp: oldest gets the least, most recent of the
                # older group gets the most.
                n = len(older)
                # Weights: 1, 2, 3, ..., n  (oldest=1, newest=n)
                weight_sum = n * (n + 1) // 2
                for idx, t in enumerate(older):
                    weight = idx + 1  # oldest=1, newest=n
                    per_tale_budget = max(
                        200,  # minimum: keep at least a snippet
                        int(budget_for_older * weight / weight_sum),
                    )
                    t["text"] = _clip_text(t["text"], per_tale_budget)

    return out


def build_reuse_dossier_prompt(entity_name: str, category: str, canon_entry, prior_tales):
    canon_json = json.dumps(canon_entry, ensure_ascii=False, sort_keys=True)
    tales_payload = json.dumps(prior_tales or [], ensure_ascii=False, indent=2)
    return f"""You are an archivist building a canon dossier for a recurring sword-and-sorcery universe.

Entity category: {category}
Entity name: {entity_name}

AUTHORITATIVE CANON JSON (highest priority):
{canon_json}

PRIOR TALES (read all; do not invent facts not supported by these texts):
{tales_payload}

Task: Produce a compact dossier for the writer to ensure full continuity.

Constraints:
- Only include facts supported by the canon JSON and/or prior tales.
- If the prior tales contain ambiguity, keep it ambiguous.
- Prefer the canon JSON if there is any conflict.
- Focus on stable identity, status, relationships, motivations, known possessions, and unresolved threads.

Return ONLY plain text, with these sections:
CANON SUMMARY:
- (bullets)

OPEN THREADS:
- (bullets)

DO-NOT-CHANGE (continuity locks):
- (bullets)
"""


def build_event_arc_dossier_prompt(event_entry: dict, prior_tales: list):
    """Build a prompt that asks the model to summarize the narrative arc so far
    for a world event, producing a compact dossier the story writer can use."""
    name = (event_entry.get("name") or "unnamed event").strip()
    canon_json = json.dumps(event_entry, ensure_ascii=False, sort_keys=True)
    tales_payload = json.dumps(prior_tales or [], ensure_ascii=False, indent=2)
    return f"""You are an archivist summarizing the narrative arc of a world event in a sword-and-sorcery universe.

Event name: {name}

AUTHORITATIVE CANON JSON:
{canon_json}

PRIOR TALES IN CHRONOLOGICAL ORDER ({len(prior_tales or [])} tales):
{tales_payload}

Task: Produce a compact *arc summary* that a story writer can use to write the next installment.

Constraints:
- Only include facts supported by the canon JSON and/or prior tales.
- Preserve chronological flow: what happened first, then what, then what.
- Note any escalation, reversals, or turning points.
- Note which characters, factions, and places are involved and their current status.
- Highlight unresolved threads and the current state of the conflict.
- Keep it concise: aim for a tight narrative summary, not a retelling.

Return ONLY plain text, with these sections:
ARC SO FAR:
(chronological narrative summary, 3-8 sentences)

KEY PLAYERS:
- (bullets: name + current status/role)

CURRENT STATE:
- (1-3 bullets: where things stand right now)

OPEN THREADS:
- (bullets: unresolved plot points the next story could develop)
"""


def allowed_reuse_name_set(full_entries_by_cat):
    allowed = set()
    for items in (full_entries_by_cat or {}).values():
        for it in items or []:
            nm = (it.get("name") or "").strip()
            if nm:
                allowed.add(nm.lower())
    return allowed


def _last_seen_date_from_entry(entry: dict) -> str:
    if not isinstance(entry, dict):
        return ""
    best = ""
    apps = entry.get("story_appearances") if isinstance(entry.get("story_appearances"), list) else []
    for a in apps:
        if not isinstance(a, dict):
            continue
        d = str(a.get("date") or "").strip()
        if d and d > best:
            best = d
    if best:
        return best
    return str(entry.get("first_date") or "").strip()


def build_reused_character_temporal_snippets(
    reused_entries: dict,
    reuse_details: dict,
    temporal_path: str,
    current_date_key: str,
) -> list[dict]:
    chars = reused_entries.get("characters") if isinstance(reused_entries, dict) else []
    if not isinstance(chars, list) or not chars:
        return []
    if not os.path.exists(temporal_path):
        return []

    try:
        payload = json.load(open(temporal_path, "r", encoding="utf-8"))
    except Exception:
        return []

    rows = payload.get("characters") if isinstance(payload, dict) else []
    if not isinstance(rows, list):
        rows = []

    temporal_by_name = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        nm = str(row.get("name") or "").strip().lower()
        temporal = row.get("temporal") if isinstance(row.get("temporal"), dict) else None
        if nm and temporal and nm not in temporal_by_name:
            temporal_by_name[nm] = temporal

    issue_dates = _load_known_issue_dates()
    today = str(current_date_key or "").strip()
    if today and today not in issue_dates:
        issue_dates.append(today)
        issue_dates = sorted(set(issue_dates))
    issue_index = {d: i + 1 for i, d in enumerate(issue_dates)}
    current_issue_index = int(issue_index.get(today, len(issue_dates) or 1))
    world_days_per_issue = int(payload.get("world_days_per_issue") or 10)

    intensity_by_name = {}
    details = reuse_details.get("characters") if isinstance(reuse_details, dict) else []
    if isinstance(details, list):
        for d in details:
            if not isinstance(d, dict):
                continue
            nm = str(d.get("name") or "").strip().lower()
            if nm:
                intensity_by_name[nm] = str(d.get("intensity") or "cameo").strip().lower()

    out = []
    for ch in chars:
        if not isinstance(ch, dict):
            continue
        name = str(ch.get("name") or "").strip()
        if not name:
            continue
        temporal = temporal_by_name.get(name.lower())
        if not isinstance(temporal, dict):
            continue
        health = temporal.get("health") if isinstance(temporal.get("health"), dict) else {}
        active = [str(x or "").strip() for x in (health.get("active_conditions") or []) if str(x or "").strip()]
        chronic = [str(x or "").strip() for x in (health.get("chronic_conditions") or []) if str(x or "").strip()]
        last_seen_date = _last_seen_date_from_entry(ch)
        issues_since = None
        if last_seen_date and last_seen_date in issue_index:
            issues_since = max(0, current_issue_index - int(issue_index[last_seen_date]))
        world_days_since = (issues_since * world_days_per_issue) if isinstance(issues_since, int) else None
        years_since = round(world_days_since / 365.0, 2) if isinstance(world_days_since, int) else None

        out.append({
            "name": name,
            "intensity": intensity_by_name.get(name.lower(), "cameo"),
            "age_now": temporal.get("current_age_years"),
            "life_stage": temporal.get("life_stage"),
            "alive": temporal.get("alive"),
            "deceased_date": temporal.get("deceased_date"),
            "aging_profile": temporal.get("aging_profile"),
            "condition_profile": health.get("condition_profile"),
            "active_conditions": active[:4],
            "chronic_conditions": chronic[:4],
            "last_seen_date": last_seen_date,
            "issues_since_last_seen": issues_since,
            "world_days_since_last_seen": world_days_since,
            "years_since_last_seen": years_since,
        })

    out.sort(key=lambda item: str(item.get("name") or "").lower())
    return out


def build_reused_character_temporal_section(reused_character_temporal: list[dict]) -> str:
    rows = reused_character_temporal if isinstance(reused_character_temporal, list) else []
    rows = [r for r in rows if isinstance(r, dict) and str(r.get("name") or "").strip()]
    if not rows:
        return ""

    lines = [
        "REUSED CHARACTER TEMPORAL SNAPSHOTS (continuity hints):",
        "Use these only for characters you already choose to reuse. Keep references subtle and story-first.",
    ]
    for row in rows:
        name = str(row.get("name") or "").strip()
        age_now = row.get("age_now")
        age_text = f"{float(age_now):.1f}" if isinstance(age_now, (int, float)) else "unknown"
        life_stage = str(row.get("life_stage") or "unknown").strip()
        alive = bool(row.get("alive", True))
        status = "deceased" if not alive else "alive"
        if not alive and row.get("deceased_date"):
            status = f"deceased on {row.get('deceased_date')}"
        aging_profile = str(row.get("aging_profile") or "unknown").strip()
        condition_profile = str(row.get("condition_profile") or "unknown").strip()
        active = ", ".join(row.get("active_conditions") or []) or "none"
        chronic = ", ".join(row.get("chronic_conditions") or []) or "none"
        gap = "unknown"
        if isinstance(row.get("issues_since_last_seen"), int):
            gap = f"{int(row.get('issues_since_last_seen'))} issue(s)"
            if isinstance(row.get("world_days_since_last_seen"), int):
                gap += f" (~{int(row.get('world_days_since_last_seen'))} world days)"
        lines.append(
            f"- {name} [{str(row.get('intensity') or 'cameo')}]: age {age_text}; stage={life_stage}; status={status}; "
            f"aging={aging_profile}; body={condition_profile}; active={active}; chronic={chronic}; gap_since_seen={gap}."
        )
    return "\n".join(lines)


def _pick_top_plus_random(items, top_n: int, random_n: int, seed_text: str, salt: str):
    items = items or []
    top_n = max(0, int(top_n or 0))
    random_n = max(0, int(random_n or 0))
    if not items:
        return []

    ordered = _safe_sorted_by_appearances(items)
    top = ordered[:top_n] if top_n else []
    if random_n <= 0:
        return top

    remaining = [x for x in ordered if x not in top]
    if not remaining:
        return top

    rng = random.Random(_stable_seed_int(seed_text, salt))
    k = min(random_n, len(remaining))
    sampled = rng.sample(remaining, k=k)
    return top + sampled


def build_spotlight_section(lore, seed_text: str, top_per_category=20, random_per_category=3):
    """Return a compact canon snippet for a small subset of entities.

    This is the main way the model can keep reused entities consistent without sending the entire lore bible.
    It scales with lore growth because selection is bounded (top + rotating sample).
    """
    lines = []

    def add_section(title, cat_key, render_fn):
        picked = _pick_top_plus_random(
            lore.get(cat_key, []) or [],
            top_n=top_per_category,
            random_n=random_per_category,
            seed_text=seed_text,
            salt=cat_key,
        )
        if not picked:
            return
        lines.append(title)
        for item in picked:
            try:
                lines.append(render_fn(item))
            except Exception:
                continue
        lines.append("")

    add_section(
        "=== CANON SPOTLIGHT: CHARACTERS ===",
        "characters",
        lambda c: "• "
            + f"{c.get('name','Unknown')}"
            + (f" ({c.get('role','?')})" if c.get("role") else "")
            + (f" [{c.get('status','unknown')}]" if c.get("status") else "")
            + (f" Traits: {', '.join(c.get('traits', [])[:6])}." if isinstance(c.get("traits"), list) and c.get("traits") else "")
            + (f" Bio: {(c.get('bio','') or '')[:220]}" if (c.get("bio") or "").strip() else ""),
    )

    add_section(
        "=== CANON SPOTLIGHT: PLACES ===",
        "places",
        lambda p: "• "
            + f"{p.get('name','Unknown')}"
            + (f" ({p.get('place_type','')})" if p.get("place_type") else "")
            + (f" Status: {p.get('status')}" if p.get("status") else "")
            + (f" — {(p.get('description','') or '')[:240]}" if (p.get("description") or "").strip() else ""),
    )

    add_section(
        "=== CANON SPOTLIGHT: RELICS ===",
        "relics",
        lambda r: "• "
            + f"{r.get('name','Unknown')}"
            + (f" Origin: {(r.get('origin','') or '')[:120]}" if (r.get("origin") or "").strip() else "")
            + (f" Power: {(r.get('power','') or '')[:140]}" if (r.get("power") or "").strip() else "")
            + (f" Curse: {(r.get('curse','') or '')[:140]}" if (r.get("curse") or "").strip() else ""),
    )

    add_section(
        "=== CANON SPOTLIGHT: WEAPONS ===",
        "weapons",
        lambda w: "• "
            + f"{w.get('name','Unknown')}"
            + (f" Type: {w.get('weapon_type')}" if w.get("weapon_type") else "")
            + (f" Origin: {(w.get('origin','') or '')[:140]}" if (w.get("origin") or "").strip() else "")
            + (f" Powers: {(w.get('powers','') or '')[:180]}" if (w.get("powers") or "").strip() else "")
            + (f" Holder: {w.get('last_known_holder')}" if w.get("last_known_holder") else ""),
    )

    add_section(
        "=== CANON SPOTLIGHT: ARTIFACTS ===",
        "artifacts",
        lambda a: "• "
            + f"{a.get('name','Unknown')}"
            + (f" Type: {a.get('artifact_type')}" if a.get("artifact_type") else "")
            + (f" Powers: {(a.get('powers','') or '')[:180]}" if (a.get("powers") or "").strip() else "")
            + (f" Holder: {a.get('last_known_holder')}" if a.get("last_known_holder") else ""),
    )

    add_section(
        "=== CANON SPOTLIGHT: FACTIONS ===",
        "factions",
        lambda f: "• "
            + f"{f.get('name','Unknown')}"
            + (f" Alignment: {f.get('alignment')}" if f.get("alignment") else "")
            + (f" Goals: {(f.get('goals','') or '')[:200]}" if (f.get("goals") or "").strip() else "")
            + (f" Leader: {f.get('leader')}" if f.get("leader") else ""),
    )

    return "\n".join(lines).strip()


def build_generation_lore_context(lore, seed_text: str):
    """Rich lore context for generation: world rules + compact entity directory.

    This gives the model awareness of the full established world so it can
    organically reference existing characters, places, factions, etc. and
    avoid accidentally contradicting canon or re-inventing existing entities.
    """
    parts = []
    # Worlds + rules are the most important global canon.
    if lore.get("worlds"):
        w0 = lore["worlds"][0]
        parts.append("=== WORLD ===")
        parts.append(f"• {w0.get('name','The Known World')}: {w0.get('description','')}")
        if w0.get("tone"):
            parts.append(f"• Tone: {w0.get('tone','')}")
        if w0.get("rules"):
            parts.append("")
            parts.append("=== LORE RULES (must be respected) ===")
            parts.extend([f"• {r}" for r in w0.get("rules", [])])

    # ── Compact entity directory from the codex ──
    # Goal: the model knows WHO and WHAT exists so it can weave a living world.
    # Each entity gets a one-line summary (name + key trait/role/location).
    # Uses a total character budget (ENTITY_DIR_MAX_CHARS) rather than per-category
    # caps.  All categories are included, sorted by recency.  In early months every
    # entity fits; once the world outgrows the budget, only the oldest/rarest entities
    # are dropped.
    codex = load_codex_file()
    if isinstance(codex, dict):
        _dir_sections = [
            ("characters", "KNOWN CHARACTERS", lambda it: _compact_char_line(it)),
            ("places", "KNOWN PLACES", lambda it: _compact_place_line(it)),
            ("factions", "KNOWN FACTIONS", lambda it: _compact_faction_line(it)),
            ("regions", "KNOWN REGIONS", lambda it: _compact_generic_line(it)),
            ("polities", "KNOWN POLITIES", lambda it: _compact_generic_line(it)),
            ("artifacts", "KNOWN ARTIFACTS", lambda it: _compact_generic_line(it)),
            ("relics", "KNOWN RELICS", lambda it: _compact_generic_line(it)),
            ("flora_fauna", "KNOWN CREATURES & FLORA", lambda it: _compact_generic_line(it)),
            ("magic", "KNOWN MAGIC TYPES", lambda it: _compact_generic_line(it)),
            ("events", "KNOWN EVENTS", lambda it: _compact_generic_line(it)),
            ("lore", "KNOWN LORE", lambda it: _compact_generic_line(it)),
            ("substances", "KNOWN SUBSTANCES", lambda it: _compact_generic_line(it)),
            ("rituals", "KNOWN RITUALS", lambda it: _compact_generic_line(it)),
            ("weapons", "KNOWN WEAPONS", lambda it: _compact_weapon_line(it)),
        ]
        budget = max(1000, ENTITY_DIR_MAX_CHARS)
        budget_used = 0
        budget_exhausted = False
        for cat, header, formatter in _dir_sections:
            if budget_exhausted:
                break
            items = codex.get(cat, [])
            if not isinstance(items, list) or not items:
                continue
            # Sort by most recent story appearance (most recently used first).
            items_sorted = sorted(items, key=_entity_last_appearance_sort_key, reverse=True)
            total_count = len(items_sorted)
            lines = []
            for it in items_sorted:
                if not isinstance(it, dict):
                    continue
                line = formatter(it)
                if not line:
                    continue
                line_cost = len(line) + 1  # +1 for newline
                if budget_used + line_cost > budget:
                    budget_exhausted = True
                    break
                lines.append(line)
                budget_used += line_cost
            if lines:
                hdr_line = ""
                if len(lines) < total_count:
                    hdr_line = f"=== {header} (showing {len(lines)} most recent of {total_count}) ==="
                else:
                    hdr_line = f"=== {header} ({len(lines)} entries) ==="
                budget_used += len(hdr_line) + 2  # header + blank line
                parts.append("")
                parts.append(hdr_line)
                parts.extend(lines)

    return "\n".join([p for p in parts if p is not None]).strip()


def _entity_last_appearance_sort_key(it: dict) -> str:
    """Return the most recent story_appearances date for sorting, or '' for never-seen."""
    if not isinstance(it, dict):
        return ""
    apps = it.get("story_appearances")
    if not isinstance(apps, list) or not apps:
        return ""
    dates = []
    for a in apps:
        if isinstance(a, dict):
            d = str(a.get("date") or "").strip()
            if d:
                dates.append(d)
    return max(dates) if dates else ""


def _compact_char_line(it: dict) -> str:
    """One-line summary: Name — role/title, status, location."""
    nm = (it.get("name") or "").strip()
    if not nm:
        return ""
    bits = [nm]
    role = (it.get("role") or it.get("title") or "").strip()
    if role:
        bits.append(role)
    status = (it.get("status") or "").strip()
    if status and status.lower() not in {"unknown", ""}:
        bits.append(f"[{status}]")
    loc = (it.get("place") or it.get("region") or it.get("realm") or "").strip()
    if loc and loc.lower() != "unknown":
        bits.append(f"({loc})")
    return "• " + " — ".join(bits[:2]) + (" " + " ".join(bits[2:]) if len(bits) > 2 else "")


def _compact_place_line(it: dict) -> str:
    """One-line summary: Name — type/description snippet, region."""
    nm = (it.get("name") or "").strip()
    if not nm:
        return ""
    bits = [nm]
    desc = (it.get("description") or it.get("bio") or "").strip()
    if desc:
        # First sentence or first 80 chars
        short = desc.split(".")[0].strip()
        if len(short) > 80:
            short = short[:77] + "..."
        bits.append(short)
    region = (it.get("region") or it.get("realm") or "").strip()
    if region and region.lower() != "unknown":
        bits.append(f"({region})")
    return "• " + " — ".join(bits[:2]) + (" " + bits[2] if len(bits) > 2 else "")


def _compact_faction_line(it: dict) -> str:
    """One-line summary: Name — description snippet."""
    nm = (it.get("name") or "").strip()
    if not nm:
        return ""
    desc = (it.get("description") or it.get("bio") or "").strip()
    short = ""
    if desc:
        short = desc.split(".")[0].strip()
        if len(short) > 100:
            short = short[:97] + "..."
    return f"• {nm}" + (f" — {short}" if short else "")


def _compact_generic_line(it: dict) -> str:
    """One-line summary: Name — first sentence of description."""
    nm = (it.get("name") or "").strip()
    if not nm:
        return ""
    desc = (it.get("description") or it.get("bio") or it.get("significance") or "").strip()
    short = ""
    if desc:
        short = desc.split(".")[0].strip()
        if len(short) > 100:
            short = short[:97] + "..."
    return f"• {nm}" + (f" — {short}" if short else "")


def _compact_weapon_line(it: dict) -> str:
    """One-line summary for storied weapons: name, type, provenance, power, holder."""
    nm = (it.get("name") or "").strip()
    if not nm:
        return ""

    bits = [nm]
    weapon_type = (it.get("weapon_type") or "").strip()
    if weapon_type:
        bits.append(weapon_type)

    lore_bits = []
    origin = (it.get("origin") or "").strip()
    if origin and origin.lower() != "unknown":
        lore_bits.append(origin.split(".")[0].strip())

    powers = (it.get("powers") or "").strip()
    if powers and powers.lower() != "unknown":
        lore_bits.append(powers.split(".")[0].strip())

    holder = (it.get("last_known_holder") or "").strip()
    if holder and holder.lower() != "unknown":
        lore_bits.append(f"Holder: {holder}")

    if lore_bits:
        joined = " | ".join(lore_bits)
        if len(joined) > 140:
            joined = joined[:137] + "..."
        bits.append(joined)

    return "• " + " — ".join(bits[:2]) + (" — " + bits[2] if len(bits) > 2 else "")


def _canon_loc_names_from_codex(codex: dict) -> dict[str, list[str]]:
    if not isinstance(codex, dict):
        return {}
    out: dict[str, list[str]] = {}
    for cat in ["places", "districts", "provinces", "regions", "realms", "continents", "hemispheres", "worlds", "polities"]:
        items = codex.get(cat, [])
        if not isinstance(items, list):
            continue
        names = []
        for it in items:
            if not isinstance(it, dict):
                continue
            nm = (it.get("name") or "").strip()
            if nm:
                names.append(nm)
        if names:
            out[cat] = names
    return out


def _load_known_issue_dates() -> list[str]:
    """Best-effort list of available issue dates (YYYY-MM-DD)."""
    dates: list[str] = []
    if os.path.exists(ARCHIVE_IDX):
        try:
            idx = json.load(open(ARCHIVE_IDX, "r", encoding="utf-8"))
            raw = idx.get("dates") if isinstance(idx, dict) else None
            if isinstance(raw, list):
                dates = [str(x or "").strip() for x in raw if str(x or "").strip()]
        except Exception:
            dates = []

    # Ensure today exists if stories.json has a date.
    try:
        if os.path.exists(OUTPUT_FILE):
            day = json.load(open(OUTPUT_FILE, "r", encoding="utf-8"))
            d = str(day.get("date") or "").strip() if isinstance(day, dict) else ""
            if d and d not in dates:
                dates.append(d)
    except Exception:
        pass

    # Sort lexicographically (YYYY-MM-DD). Archive index is typically already ordered.
    dates = sorted(set(dates))
    return dates


def _event_is_resolved(event: dict) -> bool:
    """Heuristic: decide whether an event feels resolved/ended."""
    if not isinstance(event, dict):
        return False
    blob = " ".join([
        str(event.get("tagline") or ""),
        str(event.get("outcome") or ""),
        str(event.get("significance") or ""),
        str(event.get("notes") or ""),
    ]).lower()
    # Strong resolution tokens.
    if re.search(r"\b(ended|over|resolved|concluded|peace\s+signed|sealed\b|banished|departed|destroyed|extinguished)\b", blob):
        return True
    return False


def _event_arc_metrics(event: dict, known_dates: list[str]) -> dict:
    """Compute arc recency, trend, and a coarse stage/intensity from appearances."""
    apps = event.get("story_appearances") if isinstance(event, dict) else None
    if not isinstance(apps, list) or not apps:
        return {"last_date": "", "recent_count": 0, "intensity": 1, "stage": "seed"}

    dates = []
    for a in apps:
        if not isinstance(a, dict):
            continue
        d = str(a.get("date") or "").strip()
        if d:
            dates.append(d)
    if not dates:
        return {"last_date": "", "recent_count": 0, "intensity": 1, "stage": "seed"}

    dates = sorted(set(dates))
    last_date = dates[-1]

    # Compute recency in "issues" using archive index ordering.
    idx = {d: i for i, d in enumerate(known_dates or [])}
    if last_date in idx:
        days_ago = (len(known_dates) - 1) - idx[last_date]
    else:
        # Fallback: compare calendar dates.
        last_dt = _parse_date_key(last_date)
        today_dt = _parse_date_key(known_dates[-1]) if known_dates else None
        if last_dt and today_dt:
            days_ago = max(0, (today_dt - last_dt).days)
        else:
            days_ago = 999

    # Recent appearances window.
    active_days = max(1, int(WORLD_EVENT_ARC_ACTIVE_DAYS or 14))
    recent_dates = set()
    if known_dates:
        tail = known_dates[-active_days:]
        recent_dates = set(tail)
    recent_count = sum(1 for d in dates if d in recent_dates) if recent_dates else 0

    # Trend: compare last 5 issues vs prior 5 issues.
    trend = 0
    if known_dates and len(known_dates) >= 6:
        tail = known_dates[-5:]
        prev = known_dates[-10:-5] if len(known_dates) >= 10 else known_dates[:-5]
        tail_n = sum(1 for d in dates if d in set(tail))
        prev_n = sum(1 for d in dates if d in set(prev))
        trend = tail_n - prev_n

    resolved = _event_is_resolved(event)

    # Intensity: coarse 1..max from recency + recent_count + trend.
    # Not forcing anything: this only influences prompt flavor.
    intensity = 1
    if resolved:
        intensity = 1
    else:
        if recent_count >= 4:
            intensity = 5
        elif recent_count == 3:
            intensity = 4
        elif recent_count == 2:
            intensity = 3
        elif recent_count == 1:
            intensity = 2
        else:
            intensity = 1

        if days_ago <= 1:
            intensity = min(intensity + 1, int(WORLD_EVENT_ARC_INTENSITY_MAX or 5))
        if trend >= 2:
            intensity = min(intensity + 1, int(WORLD_EVENT_ARC_INTENSITY_MAX or 5))

    intensity = max(1, min(intensity, int(WORLD_EVENT_ARC_INTENSITY_MAX or 5)))

    # Stage: how it should read in-story.
    if resolved:
        stage = "aftermath"
    elif intensity <= 1:
        stage = "seed"
    elif intensity == 2:
        stage = "simmering"
    elif intensity == 3:
        stage = "rising"
    elif intensity == 4:
        stage = "crisis"
    else:
        stage = "climax"

    return {
        "last_date": last_date,
        "days_ago": days_ago,
        "recent_count": int(recent_count),
        "trend": int(trend),
        "resolved": bool(resolved),
        "intensity": int(intensity),
        "stage": stage,
    }


def _infer_event_geo_from_codex(event: dict, loc_names: dict[str, list[str]]) -> dict:
    """Infer a rough epicenter + affected scope for an event from its text.

    This keeps the system robust even when events don't yet store explicit geo fields.
    """
    if not isinstance(event, dict):
        return {"scope": "regional", "epicenter": "unknown", "mentions": {}}

    blob = "\n".join([
        str(event.get("name") or "").strip(),
        str(event.get("tagline") or "").strip(),
        str(event.get("significance") or "").strip(),
        str(event.get("outcome") or "").strip(),
    ]).strip()

    mentions: dict[str, list[str]] = {}
    priority = [
        ("places", "place"),
        ("districts", "district"),
        ("provinces", "province"),
        ("regions", "region"),
        ("realms", "realm"),
        ("continents", "continent"),
        ("worlds", "world"),
    ]
    for cat, _ in priority:
        found = []
        for nm in loc_names.get(cat, []) or []:
            if entity_name_mentioned_in_text(nm, blob):
                found.append(nm)
        if found:
            # Deduplicate while preserving order.
            seen = set()
            uniq = []
            for x in found:
                xl = x.lower()
                if xl in seen:
                    continue
                seen.add(xl)
                uniq.append(x)
            mentions[cat] = uniq

    # Include explicit affected_* lists if present (preferred over text inference).
    for key, cat in [
        ("affected_places", "places"),
        ("affected_regions", "regions"),
        ("affected_realms", "realms"),
    ]:
        vals = event.get(key)
        if isinstance(vals, list):
            cleaned = [str(x or "").strip() for x in vals if str(x or "").strip()]
            if cleaned:
                mentions.setdefault(cat, [])
                existing = {str(x or "").strip().lower() for x in mentions.get(cat, [])}
                for x in cleaned:
                    xl = x.lower()
                    if xl in existing:
                        continue
                    existing.add(xl)
                    mentions[cat].append(x)

    # Prefer explicit epicenter fields if present.
    epicenter = "unknown"
    explicit_place = (event.get("epicenter_place") or "").strip()
    explicit_region = (event.get("epicenter_region") or "").strip()
    explicit_realm = (event.get("epicenter_realm") or "").strip()
    if explicit_place:
        epicenter = explicit_place
    elif explicit_region:
        epicenter = explicit_region
    elif explicit_realm:
        epicenter = explicit_realm
    else:
        for cat, _ in priority:
            if mentions.get(cat):
                epicenter = mentions[cat][0]
                break

    scope = (event.get("scope") or "").strip().lower()
    if scope not in {"city", "regional", "continental", "world"}:
        scope = "regional"
        if mentions.get("worlds"):
            scope = "world"
        elif mentions.get("continents"):
            scope = "continental"
        elif mentions.get("realms") or mentions.get("regions"):
            scope = "regional"
        elif mentions.get("places") or mentions.get("districts") or mentions.get("provinces"):
            scope = "city"

    return {"scope": scope, "epicenter": epicenter, "mentions": mentions}


def backfill_event_geo_fields(codex: dict) -> int:
    """Best-effort backfill of event geo fields into codex.json.

    Purpose: make large-scale, cross-geography arcs explicit and queryable even
    when older event entries only had free-text fields.

    Only fills missing fields; never overwrites explicit author/model-provided
    values.
    """
    if not isinstance(codex, dict):
        return 0
    events = codex.get("events")
    if not isinstance(events, list) or not events:
        return 0

    loc_names = _canon_loc_names_from_codex(codex)
    # Fast membership sets to classify epicenter into place/region/realm.
    place_set = {str(x).strip().lower() for x in (loc_names.get("places") or []) if str(x).strip()}
    region_set = {str(x).strip().lower() for x in (loc_names.get("regions") or []) if str(x).strip()}
    realm_set = {str(x).strip().lower() for x in (loc_names.get("realms") or []) if str(x).strip()}

    updated = 0
    for e in events:
        if not isinstance(e, dict):
            continue

        geo = _infer_event_geo_from_codex(e, loc_names)
        scope = str(geo.get("scope") or "").strip().lower()
        epicenter = str(geo.get("epicenter") or "").strip()
        mentions = geo.get("mentions") if isinstance(geo.get("mentions"), dict) else {}

        if scope and not str(e.get("scope") or "").strip():
            e["scope"] = scope
            updated += 1

        # Epicenter: fill only if empty.
        if epicenter:
            ep_lc = epicenter.strip().lower()
            if not str(e.get("epicenter_place") or "").strip() and ep_lc in place_set:
                e["epicenter_place"] = epicenter
                updated += 1
            if not str(e.get("epicenter_region") or "").strip() and ep_lc in region_set:
                e["epicenter_region"] = epicenter
                updated += 1
            if not str(e.get("epicenter_realm") or "").strip() and ep_lc in realm_set:
                e["epicenter_realm"] = epicenter
                updated += 1

        # Affected lists: fill from inferred mentions when absent.
        def _fill_list(key: str, cat: str, limit: int = 12) -> None:
            nonlocal updated
            cur = e.get(key)
            if isinstance(cur, list) and cur:
                return
            vals = mentions.get(cat)
            if isinstance(vals, list) and vals:
                cleaned = [str(x or "").strip() for x in vals if str(x or "").strip()]
                if cleaned:
                    e[key] = cleaned[:limit]
                    updated += 1

        _fill_list("affected_places", "places", limit=16)
        _fill_list("affected_regions", "regions", limit=16)
        _fill_list("affected_realms", "realms", limit=12)

    return updated


def _entity_geo_anchors(category: str, item: dict) -> dict[str, str]:
    """Extract conservative geo anchors for an entity from codex fields."""
    if not isinstance(item, dict):
        return {}

    def _get(key: str) -> str:
        return str(item.get(key) or "").strip()

    anchors = {
        "world": _get("world"),
        "hemisphere": _get("hemisphere"),
        "continent": _get("continent"),
        "realm": _get("realm"),
        "province": _get("province"),
        "region": _get("region"),
        "district": _get("district"),
        "place": _get("place"),
    }

    if category == "characters":
        anchors["place"] = _get("home_place") or anchors.get("place", "")
        anchors["region"] = _get("home_region") or anchors.get("region", "")
        anchors["realm"] = _get("home_realm") or anchors.get("realm", "")

    if category in {"places", "districts", "provinces", "regions", "realms", "continents", "hemispheres", "worlds"}:
        nm = _get("name")
        if nm:
            if category == "places":
                anchors["place"] = anchors.get("place") or nm
            elif category == "districts":
                anchors["district"] = anchors.get("district") or nm
            elif category == "provinces":
                anchors["province"] = anchors.get("province") or nm
            elif category == "regions":
                anchors["region"] = anchors.get("region") or nm
            elif category == "realms":
                anchors["realm"] = anchors.get("realm") or nm
            elif category == "continents":
                anchors["continent"] = anchors.get("continent") or nm
            elif category == "hemispheres":
                anchors["hemisphere"] = anchors.get("hemisphere") or nm
            elif category == "worlds":
                anchors["world"] = anchors.get("world") or nm

    return {k: v for k, v in anchors.items() if _truthy_non_unknown(v)}


def _in_event_scope(entity_anchors: dict[str, str], event_geo: dict) -> bool:
    if not entity_anchors or not isinstance(event_geo, dict):
        return False

    scope = str(event_geo.get("scope") or "regional").strip().lower()
    mentions = event_geo.get("mentions") if isinstance(event_geo.get("mentions"), dict) else {}

    def _has(cat: str, value: str) -> bool:
        vals = mentions.get(cat)
        if not isinstance(vals, list) or not vals:
            return False
        vl = value.lower()
        return any(str(x or "").strip().lower() == vl for x in vals)

    if scope == "city":
        return (
            ("place" in entity_anchors and _has("places", entity_anchors["place"]))
            or ("district" in entity_anchors and _has("districts", entity_anchors["district"]))
            or ("province" in entity_anchors and _has("provinces", entity_anchors["province"]))
        )

    if scope == "regional":
        return (
            ("region" in entity_anchors and _has("regions", entity_anchors["region"]))
            or ("realm" in entity_anchors and _has("realms", entity_anchors["realm"]))
            or ("place" in entity_anchors and _has("places", entity_anchors["place"]))
        )

    if scope == "continental":
        return (
            ("continent" in entity_anchors and _has("continents", entity_anchors["continent"]))
            or ("realm" in entity_anchors and _has("realms", entity_anchors["realm"]))
            or ("region" in entity_anchors and _has("regions", entity_anchors["region"]))
        )

    if scope == "world":
        if "world" in entity_anchors and _has("worlds", entity_anchors["world"]):
            return True
        return True

    return False


def _select_world_event_arcs(today_str: str, codex=None) -> list:
    """Pick a small set of active/important events deterministically per day.

    Returns a list of event dicts from the codex.  The same seed produces the
    same selection on a given day, so repeated calls are idempotent.
    """
    if codex is None:
        codex = load_codex_file()
    events = codex.get("events", []) if isinstance(codex, dict) else []
    if not isinstance(events, list) or not events:
        return []

    known_dates = _load_known_issue_dates()
    loc_names = _canon_loc_names_from_codex(codex)
    rng = random.Random(_stable_seed_int(today_str, "world_event_arcs"))
    scored = []
    for e in events:
        if not isinstance(e, dict):
            continue
        nm = (e.get("name") or "").strip()
        if not nm:
            continue
        sig = (e.get("significance") or "")
        out = (e.get("outcome") or "")
        arc = _event_arc_metrics(e, known_dates)
        geo = _infer_event_geo_from_codex(e, loc_names)
        scope = str(geo.get("scope") or "regional").strip().lower()

        # Prefer large-scale arcs for the issue-wide section.
        if scope == "world":
            scope_weight = 1.35
        elif scope == "continental":
            scope_weight = 1.25
        elif scope == "regional":
            scope_weight = 1.0
        elif scope in {"city", "local"}:
            scope_weight = 0.7
        else:
            scope_weight = 1.0

        weight = (1.0 + min(3.0, (len(sig) + len(out)) / 400.0)) * scope_weight
        if arc.get("resolved"):
            weight *= 0.7
        else:
            weight *= (1.0 + 0.18 * int(arc.get("intensity") or 1))
            if int(arc.get("days_ago") or 999) <= 2:
                weight *= 1.25
        weight *= (0.92 + 0.16 * rng.random())
        scored.append((weight, e, arc))

    scored.sort(key=lambda x: x[0], reverse=True)

    picked = []
    seen = set()
    for _weight, event_row, _arc in scored:
        name_key = (event_row.get("name") or "").strip().lower()
        type_key = (event_row.get("event_type") or "").strip().lower()
        dedupe_key = (name_key, type_key)
        if not any(dedupe_key):
            continue
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        picked.append(event_row)
        if len(picked) >= max(0, int(WORLD_EVENT_ARCS_MAX or 0)):
            break

    return picked


def build_world_event_arcs_section(today_str: str, lore: dict, event_arc_dossiers=None) -> str:
    """Build a small, issue-wide world-events section for the generation prompt.

    Parameters
    ----------
    event_arc_dossiers : dict | None
        Optional mapping of ``event_name_lower -> dossier_text`` produced by
        pre-summarization API calls.  When present and non-empty for an event,
        the compact dossier replaces raw tale injection.
    """
    if not ENABLE_WORLD_EVENT_ARCS:
        return ""

    codex = load_codex_file()
    picked = _select_world_event_arcs(today_str, codex)
    if not picked:
        return ""

    loc_names = _canon_loc_names_from_codex(codex)
    known_dates = _load_known_issue_dates()

    lines = []
    lines.append("ISSUE-WIDE WORLD EVENTS (shared continuity / cross-story pressures):")
    lines.append("- Treat these as real background pressures in the world. Some stories may be directly inside the affected area; others may only hear rumor.")
    lines.append("- If a story is set within an event's scope, show at least ONE concrete effect (refugees, rationing, conscription, tolls, cults, broken trade, riots, shadow-markets, etc.).")
    lines.append("- You may let minor characters cross paths across stories due to these pressures, but do NOT reuse the same protagonist or primary location.")
    lines.append("- If you organically introduce a NEW large-scale event, let it persist across future issues: escalate from hints → consequences → turning points → aftermath, then either resolve it or let it cool into lasting scars.")
    lines.append("- Arc pacing mechanic (organic):")
    lines.append(f"  - Canonical intensity scale is 1-{int(WORLD_EVENT_ARC_INTENSITY_MAX or 5)}: 1=seed, 2=simmering, 3=rising, 4=crisis, 5=climax. Resolved events read as aftermath.")
    lines.append("  - seed/simmering: subtle signs, rumors, odd shortages, new cult whispers; easy to miss.")
    lines.append("  - rising/crisis: unmistakable consequences, travel disruption, faction moves, villains/saints emerging.")
    lines.append("  - climax/aftermath: a breaking point or a scar; show what changed and what remains unresolved.")
    lines.append("")

    for i, e in enumerate(picked, start=1):
        nm = (e.get("name") or "Unknown").strip() or "Unknown"
        et = (e.get("event_type") or "").strip()
        tag = (e.get("tagline") or "").strip()
        geo = _infer_event_geo_from_codex(e, loc_names)
        arc = _event_arc_metrics(e, known_dates)
        scope = geo.get("scope", "regional")
        epic = geo.get("epicenter", "unknown")

        lines.append(f"{i}) {nm}")
        meta = []
        if tag:
            meta.append(f"Tagline: {tag}")
        if et:
            meta.append(f"Type: {et}")
        meta.append(f"Scope: {scope}")
        if epic and epic != "unknown":
            meta.append(f"Epicenter: {epic}")
        if meta:
            lines.append("   " + " | ".join(meta))

        # Arc status hint: helps the model persist/slow-burn/escalate across days without forcing.
        stage = str(arc.get("stage") or "seed")
        intensity = int(arc.get("intensity") or 1)
        last_seen = str(arc.get("last_date") or "").strip()
        recent_count = int(arc.get("recent_count") or 0)
        arc_bits = [f"Arc: {stage}", f"Intensity: {intensity}/{int(WORLD_EVENT_ARC_INTENSITY_MAX or 5)}"]
        if last_seen:
            arc_bits.append(f"Last seen: {last_seen}")
        if recent_count:
            arc_bits.append(f"Recent issues: {recent_count}")
        lines.append("   " + " | ".join(arc_bits))

        scope_lc = str(scope or "").strip().lower()
        if scope_lc in {"city", "local"}:
            lines.append("   Visibility: most effects are localized; outsiders hear rumor or see displaced people.")
        elif scope_lc in {"regional", "region"}:
            lines.append("   Visibility: travel and trade disruptions; neighboring places feel second-order effects.")
        elif scope_lc in {"realm", "kingdom", "nation"}:
            lines.append("   Visibility: policy/edicts, taxes, conscription, border controls; distant places feel price shocks.")
        elif scope_lc in {"continent"}:
            lines.append("   Visibility: multi-region instability; supply chains fracture; refugees and mercenary work surge.")
        elif scope_lc in {"hemisphere", "world"}:
            lines.append("   Visibility: widespread scarcity and fear; even unrelated stories should carry a trace (rumor, shortages, omens).")

        # Provide a short list of canon location names referenced by the event, if any.
        refs = []
        for cat, label in [("places", "Places"), ("regions", "Regions"), ("realms", "Realms"), ("continents", "Continents")]:
            vals = geo.get("mentions", {}).get(cat) if isinstance(geo.get("mentions"), dict) else None
            if isinstance(vals, list) and vals:
                refs.append(f"{label}: {', '.join(vals[:4])}")
        if refs:
            lines.append("   Referenced canon locations: " + " · ".join(refs))

        # Clip long fields to keep prompt lean.
        significance = (e.get("significance") or "").strip()
        outcome = (e.get("outcome") or "").strip()
        if significance:
            lines.append("   Significance: " + _clip_text(significance, 240).replace("\n", " "))
        if outcome:
            lines.append("   Outcome/aftershocks: " + _clip_text(outcome, 240).replace("\n", " "))

        # ── Event arc context: prefer pre-computed dossier, fallback to raw tales ──
        event_arc_dossiers = event_arc_dossiers or {}
        event_name_lc = (e.get("name") or "").strip().lower()
        dossier_text = (event_arc_dossiers.get(event_name_lc) or "").strip()
        if dossier_text:
            lines.append("   ARC DOSSIER (summarized from prior tales):")
            for dline in dossier_text.split("\n"):
                lines.append(f"     {dline}")
        else:
            # Fallback: inject raw tales (capped for scale)
            event_tales = gather_prior_tales_for_entity(
                e,
                max_appearances=EVENT_ARC_DOSSIER_MAX_TALES,
                max_chars_per_story=EVENT_ARC_DOSSIER_MAX_CHARS_PER_STORY,
                max_total_chars=REUSE_DOSSIER_MAX_TOTAL_INPUT_CHARS,
            )
            if event_tales:
                lines.append(f"   PRIOR STORY APPEARANCES ({len(event_tales)} tales — read these to understand the arc so far):")
                for tale in event_tales:
                    tdate = tale.get("date", "")
                    ttitle = tale.get("title", "")
                    ttext = tale.get("text", "")
                    lines.append(f"   [{tdate}] \"{ttitle}\":")
                    for tline in ttext.split("\n"):
                        lines.append(f"     {tline.strip()}")

        # Optional cross-category canon ingredients within the event radius.
        try:
            suggestions = []
            suggestion_cats = [
                "characters",
                "factions",
                "artifacts",
                "weapons",
                "relics",
                "substances",
                "magic",
                "flora_fauna",
            ]
            participants = e.get("participants")
            participant_set = {str(x or "").strip().lower() for x in participants} if isinstance(participants, list) else set()

            for cat in suggestion_cats:
                items = codex.get(cat, []) if isinstance(codex, dict) else []
                if not isinstance(items, list) or not items:
                    continue

                pool = []
                seen = set()

                # Include any participant matches for this category.
                if participant_set:
                    for it in items:
                        if not isinstance(it, dict):
                            continue
                        nm_it = (it.get("name") or "").strip()
                        if nm_it and nm_it.lower() in participant_set:
                            k = nm_it.lower()
                            if k in seen:
                                continue
                            seen.add(k)
                            pool.append(it)

                # Include geo-anchored in-scope items.
                for it in items:
                    if not isinstance(it, dict):
                        continue
                    nm_it = (it.get("name") or "").strip()
                    if not nm_it:
                        continue
                    anchors = _entity_geo_anchors(cat, it)
                    if not anchors:
                        continue
                    if not _in_event_scope(anchors, geo):
                        continue
                    k = nm_it.lower()
                    if k in seen:
                        continue
                    seen.add(k)
                    pool.append(it)

                if not pool:
                    continue

                pool = _safe_sorted_by_appearances(pool)
                names = []
                for it in pool[:2]:
                    nm_it = (it.get("name") or "").strip()
                    if nm_it:
                        names.append(nm_it)
                if names:
                    suggestions.append((cat, names))

            if suggestions:
                lines.append("   Optional in-scope canon ingredients (use if relevant):")
                for cat, names in suggestions:
                    label = cat.replace("_", "/")
                    lines.append(f"   - {label}: {', '.join(names)}")
        except Exception:
            pass
        lines.append("")

    return "\n".join(lines).strip()

# ── Story generation prompt ──────────────────────────────────────────────
def build_prompt(today_str, world_date_label, lore, reused_entries=None, reuse_details=None, event_arc_dossiers=None, codex_balance=None, reused_character_temporal=None):
    lore_context = build_generation_lore_context(lore, seed_text=today_str)
    reused_entries = reused_entries or {}
    reuse_details = reuse_details or {}
    reused_character_temporal = reused_character_temporal or []

    world_events_section = build_world_event_arcs_section(today_str, lore, event_arc_dossiers=event_arc_dossiers)

    # ── Cross-day diversity: recent titles/subgenres ──
    recent_themes_raw = _get_recent_story_themes(today_str)
    recent_themes_section = ""
    if recent_themes_raw.strip():
        recent_themes_section = f"""
RECENT STORIES (from the last few days — DO NOT repeat these same core concepts):
{recent_themes_raw}
- You may reference the same WORLD (locations, characters) but choose DIFFERENT themes, conflicts, and plot shapes.
- If the recent list is heavy on one motif (e.g. shadow trade, oaths, demon pacts), deliberately steer AWAY from that motif today.
"""

    reuse_section = ""
    if reused_entries:
        blocks = []
        blocks.append("AUTHORITATIVE CANON ENTRIES YOU MAY REUSE TODAY:")
        blocks.append("If you reuse any of these names, they MUST match these exact facts.")
        for cat, items in reused_entries.items():
            if not items:
                continue
            blocks.append(f"=== {cat.upper()} ===")
            for it in items:
                nm = (it.get("name") or "").strip() if isinstance(it, dict) else ""
                details = None
                if nm:
                    for d in reuse_details.get(cat, []) or []:
                        if isinstance(d, dict) and (d.get("name") or "").strip().lower() == nm.lower():
                            details = d
                            break
                if details:
                    intensity = (details.get("intensity") or "").strip().lower()
                    if intensity:
                        blocks.append(f"INTENDED REUSE INTENSITY: {intensity}")
                    if details.get("appearance_count") is not None:
                        blocks.append(f"PRIOR APPEARANCES COUNT: {details.get('appearance_count')}")
                    dossier = (details.get("dossier") or "").strip()
                    if dossier:
                        blocks.append("PRIOR-TALES DOSSIER (derived from scanning prior appearances):")
                        blocks.append(dossier)
                blocks.append(json.dumps(it, ensure_ascii=False, sort_keys=True))
        reuse_section = "\n" + "\n".join(blocks) + "\n"

    reused_temporal_section = ""
    temporal_block = build_reused_character_temporal_section(reused_character_temporal)
    if temporal_block:
        reused_temporal_section = "\n" + temporal_block + "\n"
    lore_section = ""
    if lore_context.strip():
        lore_section = f"""
EXISTING LORE — READ CAREFULLY BEFORE WRITING:
{lore_context}

LORE CONSISTENCY RULES:
- If you use an existing character name, their personality, status, and background must match the established lore above.
- If you use an existing place name, its geography, atmosphere, and known history must be consistent with established lore.
- If you reuse a character with travel_scope=local or regional, do not place them in distant realms without making travel a clear, plausible part of the story.
- Do not contradict established lore rules (magic costs, shadow-magic, etc.).
- You MAY introduce entirely new characters, places, and entities — but they must fit the world's tone and rules.
- Stories may share the same world but use different characters and locations.
- World-crossing events (characters moving between worlds) are extremely rare and require major magical cause.
ADDITIONAL CANON GUARDRAILS:
- Continuity is GOOD: recurring characters/places are encouraged.
- If you reuse a previously-established name, it MUST refer to that same entity and not contradict their known status, role, bio, or history.
- If you're unsure about an established fact, keep it ambiguous rather than contradicting canon.

HOMONYMS / CONTEXTUAL NAMING (important for UI + canon clarity):
- If a canon proper noun is ALSO a common noun/material (e.g., a place named "Blackthorn" vs blackthorn wood), use context-appropriate casing.
    - Use lowercase for materials/common nouns ("blackthorn", "blackthorn wood").
    - Use the canonical capitalization ONLY when you mean the proper noun/place.
- Avoid using existing place/realm names as common nouns for unrelated things (materials, adjectives) unless you explicitly mean the place.

CHARACTER-TRAIT CONTINUITY (within each story):
- When you introduce a named character with a specific trait (oath, profession, magical ability, curse, bond, nickname, weapon, title), that trait MUST stay with that character for the entire story.
- Do NOT accidentally swap traits between characters. For example:
    - If Kovin is introduced as "bound by oath", it is Kovin (not another character) who is later "freed from his oath".
    - If Gareth is introduced as "a locksmith", he is the one who picks locks — not someone else.
- Before finalizing each story, mentally verify: every trait mentioned in the resolution matches the character it was introduced with.

COHESION / CONSEQUENCES:
- Ensure the ending reflects the consequences of major actions earlier in the story.
    - If a ritual of dismissal releases a ghost, the ghost cannot still be ruling in the final lines unless you explicitly state the rite failed or was subverted.

SOVEREIGNTY / CROWN CONSISTENCY:
- Treat "the Crown" / "the Throne" as an institution of rulership tied to a specific realm (and usually a seat/capital).
- If a story involves a crown, explicitly name which realm/region it governs.
- Do NOT imply two different sole crown-holders for the same realm/region at the same time.
    - If there are multiple, state co-rulers (e.g., king and queen) OR a contested claim (pretender/regent/usurper) explicitly.
"""

    if world_events_section.strip():
        lore_section = (lore_section or "") + "\n\n" + world_events_section + "\n"

    # ── Geography context ──
    geo = load_geography()
    geo_section = ""
    geo_ctx = build_geography_context(geo)
    if geo_ctx.strip():
        geo_section = f"\n{geo_ctx}\n"

    codex_balance_section = ""
    cb = build_codex_balance_guidance_section(codex_balance)
    if cb:
        codex_balance_section = f"\n{cb}\n"

    return f"""You are a pulp fantasy writer in the tradition of Robert E. Howard, Clark Ashton Smith, and Fritz Leiber.
Generate exactly 10 original short sword-and-sorcery stories.
Each story should be vivid, action-packed, and around 120–160 words long.

ORIGINALITY / COPYRIGHT SAFETY:
- Create ONLY original characters, places, factions, creatures, spells, artifacts, and titles.
- Do NOT use or reference recognizable copyrighted names or specific settings from existing works (e.g., no Gandalf, no Middle-earth, etc.).
- Generic fantasy archetypes are fine (wizards, dragons, elves, dwarves, etc.), but names and particulars must be newly invented.

Archive date is {today_str}. Use it only as an out-of-world production label.
The in-world Edhran date is {world_date_label}.
- Any in-world temporal reference must use the Edhran calendar date above, never Gregorian months, weekdays, or real-world calendar names.
{lore_section}
{geo_section}
{codex_balance_section}
{reuse_section}
{reused_temporal_section}
{recent_themes_section}
Respond with ONLY valid JSON — no prose before or after — matching this exact structure:
[
  {{ "title": "Story Title Here", "subgenre": "Two or Three Word Label", "text": "Full story text here…" }},
  …9 more entries…
]

EVENT CAUSALITY + CONSEQUENCE RULES (non-negotiable):
- Every generated event beat must be caused by at least one prior world condition: active arc, prior event fallout, faction rivalry, regional instability, scarcity, policy shock, or character goal.
- Use a layered event system across the issue. Not every event should change world-scale simulation metrics:
    1) flavor events: atmosphere/rumor/oddity/personal texture; no required persistent world-state mutation,
    2) local consequential events: limited fallout for a person/place/district/situation,
    3) world-shaping events: broad, persistent shifts (faction metrics, regional conditions, or arc pressure/stage).
- Require world-state mutation only when the event scale justifies it (world-shaping tier).
- If an event feels intensity 7+ (historic), it should usually behave as world-shaping and include at least one materially irreversible shift (seizure, wound, death, annexation, betrayal, collapse, revelation, or lasting scar).
- Avoid empty summary phrasing like "tensions rose" unless you also state exactly what changed and for whom.

CONTENT GUARDRAILS (must follow):
- Do NOT write stories involving child death or targeted harm to children.
    - No infanticide, no parents killing children, no child sacrifice, no child murder, no violence directed at a child.
- Do NOT write stories involving child kidnapping/abduction/trafficking or children held hostage.
- Avoid plots centered on a dead child, even off-screen.
- Children may be mentioned only in non-exploitative background context when the harm is NOT targeted and is a broad tragedy.
    - Allowed examples (brief background only): plague/disease, famine, war, a natural disaster, or an indiscriminate death-magic catastrophe affecting many people.
    - Not allowed: a specific child being killed, poisoned, sacrificed, or abused.

- Do NOT write rape/sexual assault or sexual violence.
- Do NOT depict explicit sex acts or create vivid visuals of sex.
    - Sexual/romantic tension is fine; allusion is fine.
    - Keep any intimacy off-screen / fade-to-black; avoid explicit anatomy or explicit action verbs.

TONE + FANTASY VARIETY — MANDATORY DISTRIBUTION RULES:
These are HARD constraints, not suggestions. Follow them exactly.

1. TONE MIX (across the 10 stories):
   - At MOST 3 stories may be grim / tragic / horror / damnation.
   - At LEAST 2 stories must be LIGHTER in tone: adventure, wonder, comic irony, clever trickery, heroic triumph, or exploration.
   - At LEAST 1 story must have a genuine love / romance thread (sweet, tragic, or bittersweet — keep it pulp-fantasy).
   - At LEAST 1 story should end with an unambiguously positive or hopeful outcome.
   - The remaining stories can be any tone (bittersweet, morally gray, tense, mysterious, etc.).

2. THEME CAPS (no single motif may dominate the set):
   - At MOST 2 stories may center on demon pacts / demonic bargains.
    - At MOST 1 story may center on shadow theft / shadow markets / shadow manipulation.
    - At MOST 1 story may center on broken oaths or oath-consequences.
    - At MOST 1 story may center on cursed objects / cursed gold / debt-horror.
   - At MOST 1 story may center on a living fortress / sentient architecture.
   - At MOST 1 story may center on bone carving / necromantic animation.
   - If the RECENT STORIES section above shows heavy use of a motif, use ZERO of that motif today.
   EXCEPTION — ACTIVE WORLD EVENT ARCS: If the ISSUE-WIDE WORLD EVENTS section lists an active
   event at "rising", "crisis", or "climax" stage, MORE stories may reflect that event's motif
   (up to 4 stories showing its effects). This is expected — large-scale events naturally
   dominate the news. But even then, the affected stories should explore DIFFERENT facets
   (military, civilian, economic, romantic, political, comedic, etc.) rather than repeating
   the same plot shape.

3. REQUIRED BREADTH — touch on AT LEAST 5 DIFFERENT conflict types from this non-exhaustive list:
   - Political intrigue / court scheming / succession crisis
   - Sea voyage / piracy / coastal adventure
   - Heist / caper / thieves' guild rivalry
   - Romance / forbidden love / marriage-pact
   - Nature / druidism / wilderness survival / beast-bonding
   - Fellowship / a band of companions on a quest
    - Novice's first trial / a young adult's first true ordeal (avoid child protagonists)
   - War / battlefield tactics / siege (at army scale)
   - Divine intervention / temples / priest-warrior conflicts
   - Music, art, or beauty as a source of power
   - Exploration / lost world / first contact with an unknown culture
   - Non-human POV (dragon, fae, construct, spirit, beast)
   - Trade / merchant adventure / economic rivalry (non-magical)
   - Comedy of errors / trickster tale / con game
   You are NOT limited to this list — invent fresh angles freely.

4. PROTAGONIST VARIETY:
   - No more than 3 protagonists should be lone antiheroes.
   - Include at least one non-human or inhuman protagonist (fae, dwarf, orc, dragon, golem, spirit, etc.).
   - Include at least one protagonist who is part of a group (party, crew, warband, family).
    - At least one protagonist should be inexperienced or at the start of their journey (young adult; avoid child protagonists).

5. SETTING VARIETY:
   - Use at least 3 clearly different types of terrain or environment (city, sea, forest, desert, mountain, tundra, underground, sky, swamp, ruin, etc.).
   - Not every location needs to be cursed or sinister. Some should feel wondrous, beautiful, or alive.

CITY DISTRICTS (important for codex depth):
- If a story is set in a city (even briefly), name the specific district/ward/quarter/neighborhood (e.g., "Ropewalk Quarter", "Saltward", "Old Wall", "Lantern Market").
- Districts are intra-city areas; treat them as quarters/wards within a city, not provinces.

LEGENDARY WEAPONS / STORIED ARMAMENTS:
- In 2–3 stories today, feature a NAMED weapon or war-implement with real mythic gravitas: a king-spear, oath-blade, tyrant's bow, siege hammer, assassin's knife, saint-killing axe, or similarly storied armament.
- These should feel like weapons people remember and speak about: passed between rulers or champions, hidden after famous battles, feared for a curse, sought for a buried claim, or bound to a vow, bloodline, cult, prophecy, or betrayal.
- Keep the magic uncanny rather than generic: binding, memory, doom, recognition, hunger, oath-taking, ghost-guidance, storm-calling, kingmaking, gate-opening, name-cutting, etc. Avoid reducing every special weapon to simple glowing elemental power.
- Not every such weapon must dominate the plot, but when one appears it should carry lineage, reputation, or consequence.
- If a storied weapon appears, hint at at least one of these: origin, prior wielder, famous battle, price of use, reason it was hidden, or what changes when it changes hands.

FANTASY TOOLBOX (use the full range — these are NOT a limit; invent freely):
- Non-human peoples (elves, dwarves, goblin-kind, smallfolk, orcs/ogres/trolls — or wholly new lineages).
- Mythic creatures (a dragon/wyrm or similarly iconic beast — or a brand-new apex terror).
- Fae/fairy influence (a fae court, fairy realm, or a fae-bargain — or any other uncanny otherworld).
- Stories may center on ANY fantasy focus (not just people): creatures, artifacts/weapons/relics, or places can be the "main character".
- Magic is welcome but not mandatory: aim for a mix of sorcery and non-magic conflict (steel, politics, survival, travel, rivalries).

REUSE INTENSITY RULES (only applies when an entry is labeled):
- If you see "INTENDED REUSE INTENSITY: cameo" for an entity, keep it light: brief appearance/mention, not the protagonist or primary location, and avoid major new canon changes for that entity.
- If you see "INTENDED REUSE INTENSITY: central" for an entity, it may meaningfully drive plot, but MUST remain consistent with canon and the dossier.

Guidelines:
- Heroes and antiheroes with colorful names (barbarians, sell-swords, sorcerers, thieves)
- Vivid exotic settings: crumbling empires, cursed ruins, blasted steppes, sorcerous cities — but also thriving markets, verdant forests, coral reefs, mountain monasteries, sky-citadels
- Stakes that feel epic: ancient evil, demonic pacts, dying gods, vengeful sorcery — but also personal: lost love, family honor, a dare, a wager, a dream
- Each story must be complete with a beginning, conflict, and satisfying (or ironic) ending
- Vary protagonists, locations, and types of magic/conflict across all 10 stories
- Use dramatic, muscular prose — short punchy sentences mixed with lush description
- Avoid modern slang; use archaic flavor without being unreadable
- No two stories should share a protagonist or primary location
- For each story, invent a vivid 2-4 word subgenre label that captures its specific flavor.
  You are NOT limited to any fixed list — be creative. Examples of the kind of variety to aim for:
  Sword & Sorcery, Dark Fantasy, Political Intrigue, Forbidden Alchemy, Lost World,
  Sea Rover, Fae Romance, Trickster's Gambit, Druid's Trial, War Drums, Dragon's Court,
    Pirate's Honor, First Trial, Merchant Prince, Wilderness Hunt — or anything
  that fits. The label should feel like a pulp magazine category."""


_CHILDLIKE_RE = re.compile(
    r"\b(child|children|kid|kids|boy|girl|infant|baby|toddler|young\s+(?:son|daughter)|little\s+(?:son|daughter))\b",
    re.IGNORECASE,
)
_CHILD_OWN_RE = re.compile(r"\b(her|his|their|my|your|our)\s+own\s+(child|baby|infant|toddler)\b", re.IGNORECASE)
_CHILD_DEATH_RE = re.compile(r"\b(died|die|dead|death|corpse|funeral|buried)\b", re.IGNORECASE)
_CHILD_ABDUCTION_RE = re.compile(
    r"\b(kidnap(?:ped|ping)?|abduct(?:ed|ion)?|snatch(?:ed|ing)?|carried\s+off|spirited\s+away|ransom(?:ed)?|held\s+hostage|hostage|traffick(?:ed|ing)?)\b",
    re.IGNORECASE,
)
_CHILD_VIOLENCE_RE = re.compile(
    r"\b(kill|killed|killing|murder|murdered|slay|slain|stab|stabbed|strangle|strangled|smother|smothered|drown|drowned|poison|poisoned|sacrifice|sacrificed|burned\s+alive|butcher|butchered)\b",
    re.IGNORECASE,
)
_BROAD_TRAGEDY_CAUSE_RE = re.compile(
    r"\b("
    r"plague|epidemic|pox|fever|sickness|disease|illness|rot\s+king|"
    r"famine|"
    r"war|battle|siege|campaign|invasion|massacre|slaughter|"
    r"drought|flood|fire|wildfire|earthquake|storm|blizzard|landslide|tidal\s+wave|"
    r"death\s+magic|necromanc|miasma|doom\s+fog|black\s+wind"
    r")\b",
    re.IGNORECASE,
)
_MASS_CONTEXT_RE = re.compile(
    r"\b(village|town|city|realm|region|province|district|many|dozens|scores|hundreds|thousands|the\s+people|the\s+populace|crowds)\b",
    re.IGNORECASE,
)


def _natural_mass_context(text: str) -> bool:
    s = (text or "")
    # We treat only broad, indiscriminate mass-casualty contexts as an exception.
    # This is intentionally conservative: it allows brief allusions to war/plague/etc
    # without permitting targeted child harm.
    return bool(_BROAD_TRAGEDY_CAUSE_RE.search(s) and _MASS_CONTEXT_RE.search(s))


def _near(text: str, a: re.Pattern, b: re.Pattern, window: int = 90) -> bool:
    s = text or ""
    for ma in a.finditer(s):
        start = max(0, ma.start() - window)
        end = min(len(s), ma.end() + window)
        if b.search(s[start:end]):
            return True
    return False


def child_harm_violations_for_story(story: dict) -> list:
    title = (story.get("title") or "") if isinstance(story, dict) else ""
    text = (story.get("text") or "") if isinstance(story, dict) else ""
    blob = f"{title}\n\n{text}".strip()
    if not blob:
        return []

    violations = []
    has_child = bool(_CHILDLIKE_RE.search(blob) or _CHILD_OWN_RE.search(blob))
    if not has_child:
        return []

    if _CHILD_OWN_RE.search(blob) and _CHILD_VIOLENCE_RE.search(blob):
        violations.append("targeted harm to a child (own child + violence)")

    if _near(blob, _CHILDLIKE_RE, _CHILD_ABDUCTION_RE, window=140) or _near(blob, _CHILD_OWN_RE, _CHILD_ABDUCTION_RE, window=220):
        violations.append("child kidnapping/abduction/hostage")

    if _near(blob, _CHILDLIKE_RE, _CHILD_VIOLENCE_RE, window=120) or _near(blob, _CHILD_OWN_RE, _CHILD_VIOLENCE_RE, window=200):
        violations.append("violence directed at a child")

    if _near(blob, _CHILDLIKE_RE, _CHILD_DEATH_RE, window=120):
        if not _natural_mass_context(blob):
            violations.append("child death without broad mass-tragedy context")

    return sorted(set(violations))


def find_child_harm_violations(stories: list) -> list:
    out = []
    for i, s in enumerate(stories or []):
        if not isinstance(s, dict):
            continue
        v = child_harm_violations_for_story(s)
        if v:
            snippet = ((s.get("text") or "").strip()[:240]).replace("\n", " ")
            out.append({
                "index": i,
                "title": s.get("title", "Untitled"),
                "violations": v,
                "snippet": snippet,
            })
    return out


def build_child_harm_rewrite_prompt(stories: list, violations: list) -> str:
    problems = []
    for v in violations:
        problems.append(f"- Story #{v.get('index')+1}: {v.get('title')} — {', '.join(v.get('violations') or [])}")

    payload = json.dumps(stories, ensure_ascii=False, indent=2)
    return f"""You are editing a list of 10 original pulp fantasy stories.

Goal: Remove ANY child death or targeted harm to children.

HARD RULES:
- Do NOT depict or imply violence against children.
- Do NOT include infanticide, child murder, child sacrifice, or a parent killing a child.
- Do NOT include child kidnapping/abduction/trafficking or children held hostage.
- Avoid plots centered on a dead child, even off-screen.

NARROW EXCEPTION (allowed only as brief background):
- A broad mass-tragedy event affecting many people (e.g., plague/disease, famine, war alluded-to, natural disaster, or indiscriminate death-magic catastrophe), described briefly and without exploitation.

What to fix:
{chr(10).join(problems)}

EDITING INSTRUCTIONS:
- Rewrite ONLY the violating stories; keep all other stories unchanged.
- Preserve the overall tone and ~120–160 word length per story.
- Keep JSON structure identical: a list of 10 objects with keys title/subgenre/text.
- Respond with ONLY valid JSON; no prose.

STORIES JSON INPUT:
{payload}
"""


_SEXUAL_VIOLENCE_RE = re.compile(
    r"\b(rape|raped|raping|rapist|sexual\s+assault|assaulted\s+her|assaulted\s+him|forced\s+himself\s+on|forced\s+herself\s+on|violated\s+her|violated\s+him|ravish|ravished|defile|defiled|molest|molested)\b",
    re.IGNORECASE,
)
_EXPLICIT_SEX_ACT_RE = re.compile(
    r"\b(intercourse|copulat|fornicat|thrust(?:ing)?|orgasm|climax|came\b|moan(?:ed|ing)?\b|writh(?:ing)?\b)\b",
    re.IGNORECASE,
)
_EXPLICIT_ANATOMY_RE = re.compile(
    r"\b(penis|vagina|clitoris|genitals|nipple(?:s)?|bare\s+breasts?)\b",
    re.IGNORECASE,
)


def sexual_content_violations_for_story(story: dict) -> list:
    title = (story.get("title") or "") if isinstance(story, dict) else ""
    text = (story.get("text") or "") if isinstance(story, dict) else ""
    blob = f"{title}\n\n{text}".strip()
    if not blob:
        return []

    violations = []
    if _SEXUAL_VIOLENCE_RE.search(blob):
        violations.append("rape/sexual assault or sexual violence")

    # Explicit sex depiction: either explicit anatomy, or explicit act language.
    if _EXPLICIT_ANATOMY_RE.search(blob) or _EXPLICIT_SEX_ACT_RE.search(blob):
        violations.append("explicit sex depiction")

    return sorted(set(violations))


def find_sexual_content_violations(stories: list) -> list:
    out = []
    for i, s in enumerate(stories or []):
        if not isinstance(s, dict):
            continue
        v = sexual_content_violations_for_story(s)
        if v:
            snippet = ((s.get("text") or "").strip()[:240]).replace("\n", " ")
            out.append({
                "index": i,
                "title": s.get("title", "Untitled"),
                "violations": v,
                "snippet": snippet,
            })
    return out


def build_sexual_content_rewrite_prompt(stories: list, violations: list) -> str:
    problems = []
    for v in violations:
        problems.append(f"- Story #{v.get('index')+1}: {v.get('title')} — {', '.join(v.get('violations') or [])}")

    payload = json.dumps(stories, ensure_ascii=False, indent=2)
    return f"""You are editing a list of 10 original pulp fantasy stories.

Goal: Remove rape/sexual assault AND remove explicit sex depictions.

HARD RULES:
- Do NOT depict or imply rape/sexual assault/sexual violence.
- Do NOT depict explicit sex acts or create vivid visuals of sex.
- Sexual/romantic tension is allowed; allusion is allowed.
- Keep intimacy off-screen (fade-to-black).

What to fix:
{chr(10).join(problems)}

EDITING INSTRUCTIONS:
- Rewrite ONLY the violating stories; keep all other stories unchanged.
- Preserve the overall tone and ~120–160 word length per story.
- Keep JSON structure identical: a list of 10 objects with keys title/subgenre/text.
- Respond with ONLY valid JSON; no prose.

STORIES JSON INPUT:
{payload}
"""


_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+")


def _fallback_safe_story_text(title: str) -> str:
    t = (title or "").strip() or "Untitled"
    return (
        f"They tell the tale of {t} in whispers, not for what was shown, but for what was refused. "
        "A pact is offered in a lamplit room; a hand is taken, then released. "
        "Desire becomes a temptation, then a lever—ten words that could ruin a life, or save a city. "
        "The hero chooses the harder currency: restraint, dignity, and consequence. "
        "When the door finally closes, the story turns away. What follows happens off-screen, "
        "leaving only footsteps, a steady breath, and a promise kept. "
        "By dawn, there is no scandal to feast on—only a new scar in the world and a price paid in silence."
    )


def sanitize_story_for_sexual_content(story: dict) -> dict:
    """Deterministically remove explicit sexual content from a story.

    This is a backstop to keep daily generation from hard-failing. It prefers
    removing only sentences containing explicit patterns; if anything remains
    violating, it replaces the story text with a safe, fade-to-black fallback.
    """
    if not isinstance(story, dict):
        return story

    title = str(story.get("title") or "").strip() or "Untitled"
    text = str(story.get("text") or "")

    patterns = (_SEXUAL_VIOLENCE_RE, _EXPLICIT_ANATOMY_RE, _EXPLICIT_SEX_ACT_RE)

    parts = [p.strip() for p in _SENTENCE_SPLIT_RE.split(text) if p.strip()]
    kept = []
    for p in parts:
        if any(rx.search(p) for rx in patterns):
            continue
        kept.append(p)

    sanitized = " ".join(kept).strip()
    new_story = dict(story)
    new_story["text"] = sanitized

    if sexual_content_violations_for_story(new_story):
        new_story["text"] = _fallback_safe_story_text(title)

    return new_story


# ── Motif overuse guardrail (quality) ───────────────────────────────────
# Detects when a small set of motifs (shadow theft/markets, oath-consequences,
# debt-horror) repeats too often across the day's 10 stories.

_MOTIF_SHADOW_RE = re.compile(r"\bshadow(?:s)?\b", re.IGNORECASE)
_MOTIF_SHADOW_TRADE_RE = re.compile(
    r"\b(steal|stole|stolen|theft|thief|thieves|market|broker|pawn|trade|ledger|debt|owed|interest|usury|collector)\b",
    re.IGNORECASE,
)
_MOTIF_OATH_RE = re.compile(r"\b(oath|oaths|vow|vows|sworn|forsworn|pledge|geas)\b", re.IGNORECASE)
_MOTIF_DEBT_RE = re.compile(r"\b(debt|ledger|owed|interest|usury|collector|indenture)\b", re.IGNORECASE)
_MOTIF_CURSE_RE = re.compile(r"\b(curse|cursed|hex|accursed|doom|damnation|blood[-\s]?price)\b", re.IGNORECASE)

_MOTIF_CAPS = {
    "shadow-theft": 1,
    "oath": 1,
    "debt-horror": 1,
}


def motifs_for_story(story: dict) -> set:
    if not isinstance(story, dict):
        return set()
    title = (story.get("title") or "")
    text = (story.get("text") or "")
    blob = f"{title}\n\n{text}".strip()
    if not blob:
        return set()

    motifs = set()
    if _near(blob, _MOTIF_SHADOW_RE, _MOTIF_SHADOW_TRADE_RE, window=140):
        motifs.add("shadow-theft")
    if _MOTIF_OATH_RE.search(blob):
        motifs.add("oath")
    if _near(blob, _MOTIF_DEBT_RE, _MOTIF_CURSE_RE, window=160):
        motifs.add("debt-horror")
    return motifs


def find_motif_overuse_violations(stories: list) -> list:
    if not stories:
        return []

    motif_to_indices = {k: [] for k in _MOTIF_CAPS.keys()}
    story_motifs = []
    for i, s in enumerate(stories):
        m = motifs_for_story(s)
        story_motifs.append(m)
        for motif in m:
            if motif in motif_to_indices:
                motif_to_indices[motif].append(i)

    violations_by_story = {}  # index -> set[str]
    for motif, idxs in motif_to_indices.items():
        cap = int(_MOTIF_CAPS.get(motif, 0))
        if cap >= 0 and len(idxs) > cap:
            for i in idxs[cap:]:
                violations_by_story.setdefault(i, set()).add(f"overused motif: {motif}")

    out = []
    for i in sorted(violations_by_story.keys()):
        s = stories[i] if 0 <= i < len(stories) else {}
        if not isinstance(s, dict):
            continue
        snippet = ((s.get("text") or "").strip()[:240]).replace("\n", " ")
        out.append({
            "index": i,
            "title": s.get("title", "Untitled"),
            "violations": sorted(violations_by_story[i]),
            "motifs": sorted(story_motifs[i] or []),
            "snippet": snippet,
        })
    return out


def build_motif_rewrite_prompt(stories: list, violations: list) -> str:
    problems = []
    for v in violations:
        motifs = ", ".join(v.get("motifs") or [])
        problems.append(f"- Story #{v.get('index')+1}: {v.get('title')} — {', '.join(v.get('violations') or [])} (motifs: {motifs})")

    payload = json.dumps(stories, ensure_ascii=False, indent=2)
    caps_str = ", ".join(f"{k} ≤ {v}" for k, v in _MOTIF_CAPS.items())
    return f"""You are editing a list of 10 original pulp fantasy stories.

Goal: Reduce repetitive motifs across the set.

Hard caps for today's set: {caps_str}

Motif definitions:
- shadow-theft: stealing shadows, shadow markets, shadow brokers, shadow-debts/ledgers.
- oath: broken oaths, geasa, vow-consequences as the central driver.
- debt-horror: cursed debt/ledgers/collectors where the debt is supernatural or doom-laden.

HARD RULES:
- Rewrite ONLY the violating stories listed below; keep all other stories unchanged.
- When rewriting, REMOVE the flagged motif(s) entirely from that story (do not simply rename them).
- Replace them with a different conflict type and a fresh central hook.
- Preserve ~120–160 word length per story and keep the same JSON structure.
- Maintain existing safety rules: no child harm or child kidnapping/abduction; no rape/explicit sex.
- Respond with ONLY valid JSON; no prose.

What to fix:
{chr(10).join(problems)}

STORIES JSON INPUT:
{payload}
"""


# ── Character-trait continuity check ────────────────────────────────
# Catches when a trait introduced for one character is later applied to a
# different character (e.g. Kovin is "bound by oath" but Gareth gets
# "freed from his oath").

_INTRO_PATTERN = re.compile(
    r"\b([A-Z][a-z]+(?:\s+(?:the\s+)?[A-Z][a-z]+)?)\s*"      # character name
    r"\(([^)]{4,80})\)",                                        # parenthetical trait
    re.UNICODE,
)

_TRAIT_KEYWORDS = re.compile(
    r"\b(oath|bound|sworn|locksmith|thief|scout|sellsword|sorcerer|sorceres|alchemist|"
    r"priest|druid|ranger|knight|captain|healer|blacksmith|merchant|assassin|"
    r"spy|bard|navigator|pilot|berserker|archer|mage|wizard|shaman|monk|"
    r"cursed|blessed|exiled|banished|blinded|mute|crippled|immortal|undead|"
    r"fae.blood|half.blood|dragon.blood|were.wolf|shape.shift|"
    r"freed?\s+from\s+(?:his|her|their)\s+oath|cut\s+\w+\s+free)\b",
    re.IGNORECASE,
)


def _extract_character_traits(text):
    """Extract character -> set-of-traits from parenthetical introductions.

    Returns dict: { 'Gareth': {'dwarf locksmith', 'worked the deepvault'}, ... }
    """
    chars = {}  # type: dict[str, set]
    for m in _INTRO_PATTERN.finditer(text):
        name = m.group(1).strip()
        trait = m.group(2).strip().lower()
        chars.setdefault(name, set()).add(trait)
    return chars


def _find_trait_swap_candidates(text):
    """Heuristic: detect when a trait word introduced near one character
    appears later near a different character.

    Returns list of dicts describing suspected swaps.
    """
    char_traits = _extract_character_traits(text)
    if len(char_traits) < 2:
        return []

    suspects = []
    sentences = re.split(r'(?<=[.!?])\s+', text)

    for sent in sentences:
        # Find character names mentioned in this sentence.
        names_in_sent = []
        for name in char_traits:
            if re.search(r'\b' + re.escape(name) + r'\b', sent):
                names_in_sent.append(name)

        if len(names_in_sent) != 1:
            continue  # ambiguous or no character — skip
        active_name = names_in_sent[0]

        # Check if this sentence applies a trait-keyword that was introduced for
        # a DIFFERENT character.
        for other_name, other_traits in char_traits.items():
            if other_name == active_name:
                continue
            for trait in other_traits:
                # Check trait keywords appear in this sentence.
                trait_words = set(re.findall(r'\b\w{4,}\b', trait))
                for tw in trait_words:
                    if re.search(r'\b' + re.escape(tw) + r'\b', sent, re.IGNORECASE):
                        suspects.append({
                            "active_char": active_name,
                            "trait_owner": other_name,
                            "trait": trait,
                            "keyword": tw,
                            "sentence": sent.strip()[:200],
                        })
                        break
    return suspects


def find_continuity_issues(stories):
    """Run the character-trait swap heuristic on each story.

    Returns list of { index, title, suspects: [...] } for stories with issues.
    """
    out = []
    for i, s in enumerate(stories or []):
        if not isinstance(s, dict):
            continue
        text = (s.get("text") or "").strip()
        if not text:
            continue
        suspects = _find_trait_swap_candidates(text)
        if suspects:
            out.append({
                "index": i,
                "title": s.get("title", "Untitled"),
                "suspects": suspects,
            })
    return out


def build_continuity_rewrite_prompt(stories, issues):
    """Build an LLM prompt to fix character-trait swap issues."""
    problems = []
    for iss in issues:
        idx = iss.get("index", 0)
        title = iss.get("title", "Untitled")
        for s in iss.get("suspects", []):
            problems.append(
                f"- Story #{idx+1} \"{title}\": The trait \"{s['trait']}\" was introduced "
                f"for {s['trait_owner']}, but sentence near {s['active_char']} uses "
                f"keyword \"{s['keyword']}\". Possible swap."
            )

    payload = json.dumps(stories, ensure_ascii=False, indent=2)
    return f"""You are editing a list of 10 original pulp fantasy stories.

Goal: Fix character-trait continuity errors where a trait, role, or oath
introduced for one named character is accidentally applied to a different
character later in the same story.

SUSPECTED ISSUES:
{chr(10).join(problems)}

EDITING INSTRUCTIONS:
- For each flagged story, re-read it carefully. If a trait (oath, profession,
  curse, bond, etc.) introduced for Character A is later applied to Character B,
  fix it so the trait stays with the correct character throughout.
- If the heuristic flagged a false positive (no actual error), leave that story unchanged.
- Do NOT change stories that have no issues.
- Preserve the overall tone and ~120-160 word length per story.
- Keep JSON structure identical: a list of 10 objects with keys title/subgenre/text.
- Respond with ONLY valid JSON; no prose.

STORIES JSON INPUT:
{payload}
"""


def find_canon_collisions(stories, lore, allowed_names_lower):
    """Return referenced canon entries not in the allowed reuse set.

    This is the key safeguard against accidental name reuse out of context.
    """
    referenced = find_referenced_canon_entries(stories, lore)
    collisions = {}
    allowed = allowed_names_lower or set()
    for cat, items in (referenced or {}).items():
        bad = []
        for it in items or []:
            nm = (it.get("name") or "").strip()
            if nm and nm.lower() not in allowed:
                bad.append(it)
        if bad:
            collisions[cat] = bad

    # Also catch partial-name collisions for characters (e.g., story uses "Kess"
    # when canon has "Kess of the Drowned Hollows"). These are not detectable
    # by signature-key matching.
    first_tok_bad = _find_first_token_character_collisions(stories, lore, allowed)
    if first_tok_bad:
        collisions.setdefault("characters", [])
        collisions["characters"].extend(first_tok_bad)
    return collisions


def build_collision_rename_prompt(stories, collisions_by_cat):
    """Prompt for minimal rewrite that renames accidental canon collisions to NEW names."""
    stories_payload = json.dumps(stories, ensure_ascii=False, indent=2)
    lines = []
    for cat, items in (collisions_by_cat or {}).items():
        if not items:
            continue
        lines.append(f"=== ACCIDENTAL COLLISIONS: {cat.upper()} ===")
        for it in items:
            nm = (it.get("name") or "").strip()
            if nm:
                lines.append(f"- {nm}")
        lines.append("")

    return f"""You are an editor.

Problem: the stories accidentally reused existing canon names listed below, but they were NOT intended as canon continuity.

Task: revise ONLY as needed to rename those names to brand new names (and adjust any related references) while preserving plot and style.

Constraints:
- Smallest possible edits.
- Do NOT change titles or subgenre.
- Do NOT introduce any of the collision names again.
- Keep story lengths roughly similar.

Collision names to rename:
{os.linesep.join(lines).strip()}

Return ONLY valid JSON in the exact same array structure as input.

STORIES JSON INPUT:
{stories_payload}
"""

# ── Lore extraction prompt ───────────────────────────────────────────────
def build_lore_extraction_prompt(stories, existing_lore, codex_balance=None):
    def _extract_name_candidates(stories, max_candidates=140):
        """Heuristic list of capitalized name-like candidates from story text.

        Purpose: help the model avoid missing one-off named entities (esp. places)
        during the lore extraction pass.
        """
        stop_single = {
            "a", "an", "and", "are", "as", "at", "be", "been", "but", "by", "each", "for",
            "from", "had", "has", "have", "he", "her", "hers", "him", "his", "i", "if",
            "in", "into", "is", "it", "its", "like", "me", "my", "no", "not", "now",
            "of", "off", "on", "one", "or", "our", "out", "she", "so", "some", "soon",
            "than", "that", "the", "their", "then", "there", "these", "they", "this",
            "those", "three", "to", "too", "two", "under", "up", "upon", "was", "we",
            "were", "what", "when", "who", "why", "will", "with", "you", "your",
            "above", "below",
        }
        # Allow multi-word titles like "High Magistrate" (don’t stopword-filter phrases).
        bad_single = {
            "anything", "everything", "nothing", "someone", "something",
            "yes", "yours", "mine", "ours", "theirs",
        }

        # Capitalized word / phrase matcher, with apostrophes, unicode quotes, and hyphens.
        # NOTE: avoid spanning newlines to prevent merging title + first sentence.
        # Examples: Xul'thyris, Thul-Kâr, Castle Greymarch, High Magistrate, Kael the Nameless
        cand_re = re.compile(
            r"\b[A-Z][\w’'\-]+(?:(?:(?:[ \t]+(?:of|the|and|in|on|at|to|for)[ \t]+)|[ \t]+)[A-Z][\w’'\-]+){1,4}\b"
            r"|\b[A-Z][\w’'\-]{2,}\b"
        )

        # Catch important object phrases that often appear with a lowercase common noun
        # but should still be treated as named artifacts/relics (e.g., "the idol of Khar-Zul").
        # Keep the surface form from the story as closely as possible.
        object_of_re = re.compile(
            r"\b(?:the\s+)?(idol|crown|throne|blade|dagger|sword|sabre|saber|knife|axe|hammer|mace|spear|lance|bow|glaive|halberd|scythe|ring|tome|amulet|chalice|mask|orb|eye|eyes)\s+of\s+"
            r"([A-Z][\w’'\-]+(?:[ \t\-]+[A-Z][\w’'\-]+){0,4})\b"
        )

        # Catch possessive named items like "Morthaxes's gold" or "Karesh's crown".
        # This helps surface named treasures/materials that are otherwise easy to miss.
        possessive_item_re = re.compile(
            r"\b([A-Z][\w’'\-]+(?:[ \t\-]+[A-Z][\w’'\-]+){0,2})['’]s\s+(gold|silver|hoard|treasure|coin|coins|crown|blade|debt|ledger)\b"
        )

        candidates = []
        seen = set()
        for s in stories or []:
            title = (s.get("title") or "")
            text = (s.get("text") or "")
            for chunk in [title] + (text.splitlines() if text else []):
                if not chunk.strip():
                    continue

                for m in object_of_re.finditer(chunk):
                    cand = (m.group(0) or "").strip()
                    if not cand:
                        continue
                    if cand.lower().startswith("the "):
                        cand = cand[4:].strip()
                    cand_norm = " ".join(cand.split())
                    key = cand_norm.lower()
                    if key in seen:
                        continue
                    seen.add(key)
                    candidates.append(cand_norm)
                    if len(candidates) >= max_candidates:
                        return candidates

                for m in possessive_item_re.finditer(chunk):
                    cand = (m.group(0) or "").strip()
                    if not cand:
                        continue
                    cand_norm = " ".join(cand.split())
                    key = cand_norm.lower()
                    if key in seen:
                        continue
                    seen.add(key)
                    candidates.append(cand_norm)
                    if len(candidates) >= max_candidates:
                        return candidates

                for m in cand_re.finditer(chunk):
                    cand = (m.group(0) or "").strip()
                    if not cand:
                        continue
                    cand_norm = " ".join(cand.split())
                    key = cand_norm.lower()
                    if key in seen:
                        continue

                    # Filter obvious false positives for single-word candidates.
                    if " " not in cand_norm:
                        if key in stop_single or key in bad_single:
                            continue
                        # Avoid picking up sentence-start pronouns that slip through.
                        if len(cand_norm) <= 2:
                            continue

                    seen.add(key)
                    candidates.append(cand_norm)
                    if len(candidates) >= max_candidates:
                        return candidates
        return candidates

    existing_chars     = {c["name"].lower() for c in existing_lore.get("characters", [])}
    existing_places    = {p["name"].lower() for p in existing_lore.get("places", [])}
    existing_events    = {e["name"].lower() for e in existing_lore.get("events", [])}
    existing_rituals   = {r["name"].lower() for r in existing_lore.get("rituals", []) if isinstance(r, dict) and r.get("name")}
    existing_weapons   = {w["name"].lower() for w in existing_lore.get("weapons", [])}
    existing_deities   = {d["name"].lower() for d in existing_lore.get("deities_and_entities", [])}
    existing_artifacts   = {a["name"].lower() for a in existing_lore.get("artifacts",   [])}
    existing_factions    = {f["name"].lower() for f in existing_lore.get("factions",    [])}
    existing_polities    = {p["name"].lower() for p in existing_lore.get("polities",    [])}
    existing_lore_items  = {l["name"].lower() for l in existing_lore.get("lore",         [])}
    existing_flora_fauna = {x["name"].lower() for x in existing_lore.get("flora_fauna",  [])}
    existing_magic       = {m["name"].lower() for m in existing_lore.get("magic",        [])}
    existing_relics      = {r["name"].lower() for r in existing_lore.get("relics",       [])}
    existing_regions     = {g["name"].lower() for g in existing_lore.get("regions",      [])}
    existing_substances  = {s["name"].lower() for s in existing_lore.get("substances",   [])}

    existing_continents   = {c["name"].lower() for c in existing_lore.get("continents",   []) if isinstance(c, dict) and c.get("name")}
    existing_realms       = {r["name"].lower() for r in existing_lore.get("realms",       []) if isinstance(r, dict) and r.get("name")}
    existing_provinces    = {p["name"].lower() for p in existing_lore.get("provinces",    []) if isinstance(p, dict) and p.get("name")}
    existing_districts    = {d["name"].lower() for d in existing_lore.get("districts",    []) if isinstance(d, dict) and d.get("name")}
    existing_hemispheres  = {h["name"].lower() for h in existing_lore.get("hemispheres",  []) if isinstance(h, dict) and h.get("name")}

    def _known_summary(items, limit=50):
        items = sorted({str(x).strip() for x in (items or set()) if str(x).strip()})
        if not items:
            return "none"
        if len(items) <= limit:
            return ", ".join(items)
        sample = ", ".join(items[:limit])
        return f"{len(items)} known; sample: {sample}"

    stories_text = "\n\n".join(
        f"STORY {i+1}: {s['title']}\n{s['text']}"
        for i, s in enumerate(stories)
    )

    name_candidates = _extract_name_candidates(stories)
    candidates_block = "\n".join([f"- {c}" for c in name_candidates]) if name_candidates else "- (none)"

    underrep_lines = []
    weak = codex_balance.get("underrepresented") if isinstance(codex_balance, dict) else []
    if isinstance(weak, list) and weak:
        underrep_lines.append("UNDERREPRESENTED LABEL WATCHLIST (quality-first):")
        underrep_lines.append("- Be extra vigilant for these labels when the story naturally supports them.")
        underrep_lines.append("- Do NOT invent filler; only extract distinct named entities with clear narrative role.")
        for row in weak[:8]:
            if not isinstance(row, dict):
                continue
            label = str(row.get("label") or "").strip()
            if not label:
                continue
            count = int(row.get("count") or 0)
            target = int(row.get("target_min_count") or 0)
            underrep_lines.append(f"- {label}: current {count}, target floor ~{target}.")
    underrep_block = "\n".join(underrep_lines).strip()

    return f"""You are a lore archivist for a sword-and-sorcery story universe.
Analyze the following stories and extract lore elements — characters, places, events, weapons, artifacts, factions, polities (governments/crowns/thrones), lore, flora/fauna, magic, relics, regions, substances, and geo hierarchy entries (hemisphere/continent/realm/province/region/district) — that appear in these stories.

Important note on districts:
- Districts are named intra-city areas (wards/quarters/neighborhoods) within a city.
- If a city appears, extract any named district/ward/quarter mentioned.

Priority: COMPLETENESS over novelty.
- It is OK if you re-extract something that already exists; the merge step will deduplicate.
- When in doubt, include the entry (you may use 'unknown' fields).

Be exhaustive:
- Do not miss named wars, sieges, rituals, treaties, curses, plagues, or disasters.
- If a story names a domain/territory ruled by someone, capture it as a Region/Realm/Place as appropriate.
- Prefer creating an entry even if details are sparse; use 'unknown' for unknown fields.

Coverage checklist (per story):
- List every named proper noun that appears to refer to a person, place, region/realm, faction, event, ritual, spell/ability, relic/artifact/weapon, or creature.
- If a story refers to a governing institution ("the Crown", "the Throne", "the Regency", "the Council"), capture it as a Polity tied to a realm/seat.
- Do NOT classify a realm's rulership institution as a Faction unless it is explicitly a distinct factional group.
- Ensure each named thing is represented in at least one output category.
- Do NOT treat sentence-start directional or positional words like "Above" or "Below" as entities unless the story clearly establishes them as proper nouns.
- Treat patterns like "X of Y" and "The X of Y" as likely names; include them when they read like a title, place, or event.
- Treat important objects described as "the <object> of <ProperName>" (even if <object> is lowercase) as named artifacts/relics and include them (e.g., "the idol of Khar-Zul").
- Be especially careful not to miss storied weapons: named blades, spears, bows, axes, hammers, knives, or similar arms with history, titles, curses, prior wielders, or famous battles attached.
- If an object is primarily wielded as a weapon, put it in "weapons" even if it is magical; use "artifacts" or "relics" only when it is not mainly a weapon.

Naming rules:
- The `name` field MUST match the surface form used in the story text as closely as possible.
- Do NOT add disambiguating suffixes/prefixes like "(as Region)", "(the place)", "(event)", etc.
- If a person/entity name appears only in possessive form (e.g. "Morthaxes's"), use the base name without the trailing possessive for the entity name ("Morthaxes").
- Keep apostrophes that are part of the canonical name itself (e.g. "Xul'thyris").

EXISTING CANON (reference only; non-exhaustive; ok to repeat):
- Characters: {_known_summary(existing_chars)}
- Places: {_known_summary(existing_places)}
- Events: {_known_summary(existing_events)}
- Rituals: {_known_summary(existing_rituals)}
- Weapons: {_known_summary(existing_weapons)}
- Deities/Entities: {_known_summary(existing_deities)}
- Artifacts: {_known_summary(existing_artifacts)}
- Factions: {_known_summary(existing_factions)}
- Polities (Crowns/Governments): {_known_summary(existing_polities)}
- Lore & Legends: {_known_summary(existing_lore_items)}
- Flora & Fauna: {_known_summary(existing_flora_fauna)}
- Magic & Abilities: {_known_summary(existing_magic)}
- Relics & Cursed Items: {_known_summary(existing_relics)}
- Continents: {_known_summary(existing_continents)}
- Hemispheres: {_known_summary(existing_hemispheres)}
- Realms: {_known_summary(existing_realms)}
- Provinces: {_known_summary(existing_provinces)}
- Regions: {_known_summary(existing_regions)}
- Districts: {_known_summary(existing_districts)}
- Substances & Materials: {_known_summary(existing_substances)}

GEOGRAPHY CONSTRAINTS:
- We are grounding this universe on ONE main planet/world named Edhra.
- The known continent is Valdris. Its macro-regions are: The Steppe Marches (north), The Shattered West (northwest), The Sunken Marches (west), The Hearthlands (center), The Ashen Reach (east), The Iron Coast (south).
- The number of continents must remain low and bounded. Maximum continents: {MAX_CONTINENTS}.
- Prefer to assign new places to an existing continent/realm/region when plausible.
- When extracting places, assign the "region" field to the most fitting Valdris macro-region name if possible.
- You MAY create a new continent only if truly necessary, and you must not exceed the maximum.

STORIES TO ANALYZE:
{stories_text}

NAME CANDIDATES (for completeness; ignore common words like "The" / "But"):
{candidates_block}

{underrep_block}

Hard requirement:
- For every candidate above that is truly a named entity in the story text (person/place/realm/title/institution/event/ritual/spell/object/creature), ensure it appears in at least one output category.
- Do NOT omit one-off names just because they appear only once.
- Do NOT drop or simplify punctuation/diacritics in names (keep apostrophes, hyphens, accents).
- Do NOT create duplicate entities for shortened references: if a character is "Kael the Nameless" and the story also says "Kael", output ONE character entry with the most complete name and list the shorter forms in "aliases".
- If a story mentions a named language/dialect/script (e.g. "Old Tongue"), include it under "lore" with category "language".

Classification guidance:
- weapons is for named or clearly singular armaments used for fighting. A weapon can still be magical, sacred, cursed, royal, or legendary and remain a weapon.
- artifacts and relics are for powerful objects that are not primarily weapons, or whose main narrative role is ceremonial, devotional, archival, or occult rather than martial.
- flora_fauna is for species or organisms that exist in the world. Ordinary real-world animals (crows, horses, wolves) MAY be included, but must be clearly distinguished from magical variants:
  - If an animal appears only as a character's magical disguise, familiar form, or shapeshifted body, file the transformation ability under "magic" (as a spell/technique) or mention it in the character's bio. Do NOT list it as flora_fauna.
  - If an ordinary animal also exists naturally in this world (e.g. crows roost on Pelimor's rooftops), it CAN be flora_fauna with rarity "common" and type "creature" (not "spirit" or "familiar").
  - Magical or world-specific variants (e.g. "shadow-crow", "iron wolf", "wyvern") are always flora_fauna with appropriate rarity ("rare" or "legendary").

Respond with ONLY valid JSON in this exact structure (use empty arrays if nothing new was found):
{{
  "characters": [
    {{
      "id": "snake_case_id",
      "name": "Full Name",
            "aliases": ["Short form", "Epithetless form"],
      "tagline": "Three punchy evocative words. (e.g. Cursed. Reckless. Hunted.)",
      "role": "Role (e.g. Thief, Warlord, Sorceress)",
      "world": "known_world",
      "status": "active / dead / cursed / unknown / etc",
            "home_place": "Name of a city/town/structure they are based in, or 'unknown'",
            "home_region": "Region name, or 'unknown'",
            "home_realm": "Realm name, or 'unknown'",
            "travel_scope": "local|regional|realmwide|interrealm|unknown",
      "bio": "2-3 sentence bio based strictly on what appears in the story.",
      "traits": ["trait1", "trait2", "trait3"],
      "known_locations": ["place names mentioned"],
      "affiliations": ["groups or individuals"],
      "notes": "Any story hooks or unresolved threads."
    }}
  ],
  "places": [
    {{
      "id": "snake_case_id",
      "name": "Place Name",
      "tagline": "Three evocative words describing this place.",
            "place_type": "city / village / fortress / ruin / temple / wilderness / etc",
      "world": "known_world",
                        "parent_place": "Name of the immediate containing place (e.g. a city for a district; a district for a neighborhood) or 'unknown'",
            "hemisphere": "Name or 'unknown'",
            "continent": "Name or 'unknown'",
            "realm": "Name or 'unknown'",
            "province": "Name or 'unknown'",
            "region": "Name or 'unknown'",
            "district": "Name or 'unknown'",
      "atmosphere": "One sentence mood/tone description.",
      "description": "Description based on the story.",
      "status": "active / ruins / unknown / etc",
      "notes": "Any story hooks."
    }}
  ],
  "events": [
    {{
      "id": "snake_case_id",
      "name": "Event Name",
      "tagline": "Three evocative words describing this event.",
      "event_type": "battle / war / ritual / uprising / catastrophe / etc",
            "scope": "city|regional|continental|world (best guess based on story scope)",
            "epicenter_place": "Place name, or 'unknown'",
            "epicenter_region": "Region name, or 'unknown'",
            "epicenter_realm": "Realm name, or 'unknown'",
            "affected_places": ["place names impacted"],
            "affected_regions": ["region names impacted"],
            "affected_realms": ["realm names impacted"],
            "radius": "Optional plain-English radius (e.g. 'citywide', 'across the realm', 'three days by road')",
      "participants": ["character or faction names involved"],
      "outcome": "What happened — who won or lost, what changed.",
      "significance": "Why this matters to the world.",
      "notes": "Any unresolved threads or consequences."
    }}
  ],
    "rituals": [
        {{
            "id": "snake_case_id",
            "name": "Ritual Name",
            "tagline": "Three evocative words describing this ritual.",
            "ritual_type": "dismissal / binding / summoning / oath / warding / sacrifice / etc",
            "performed_by": ["character/faction names involved"],
            "requirements": "Key components, constraints, or setup (or 'unknown').",
            "effect": "What it does, based strictly on the story.",
            "cost": "What it costs, corrupts, or demands (or 'unknown').",
            "notes": "Any unresolved threads or consequences."
        }}
    ],
  "weapons": [
    {{
      "id": "snake_case_id",
      "name": "Weapon Name",
      "tagline": "Three evocative words describing this weapon.",
      "weapon_type": "sword / axe / spear / bow / staff / etc",
            "origin": "Where it came from, who forged it, or what line/battle/oath it is tied to.",
            "powers": "Any magical, uncanny, symbolic, or legendary properties.",
            "last_known_holder": "Who had it last, or 'unknown'.",
      "status": "active / destroyed / lost / sealed",
            "notes": "Any story hooks, prior wielders, famous battles, curses, prices of use, or hidden history."
    }}
  ],
  "deities_and_entities": [
    {{
      "id": "snake_case_id",
      "name": "Name",
      "type": "deity / demon / spirit / undead / etc",
      "world": "known_world",
      "description": "Description from the story.",
      "status": "active / dormant / destroyed / etc",
      "notes": "Any hooks."
    }}
  ],
  "artifacts": [
    {{
      "id": "snake_case_id",
      "name": "Artifact Name",
      "tagline": "Three evocative words describing this artifact.",
      "artifact_type": "ring / tome / idol / amulet / etc",
      "origin": "Where it came from.",
      "powers": "What it does, based on the story.",
      "last_known_holder": "Who had it last.",
      "status": "active / destroyed / sealed / lost",
      "notes": "Any hooks."
    }}
  ],
  "factions": [
    {{
      "id": "snake_case_id",
      "name": "Faction Name",
      "tagline": "Three evocative words.",
      "alignment": "lawful / neutral / chaotic / corrupt / villainous",
      "goals": "What they seek or protect.",
      "leader": "Who leads them.",
      "status": "active / disbanded / rising / fallen",
      "notes": "Any hooks."
    }}
  ],
    "polities": [
        {{
            "id": "snake_case_id",
            "name": "Polity / Crown Name (e.g., The Crown of X, The Regency of Y, The High Council)",
            "tagline": "Three evocative words.",
            "polity_type": "crown / monarchy / regency / council / empire / republic / theocracy / etc",
            "realm": "Realm name governed, or 'unknown'",
            "region": "Region name governed, or 'unknown'",
            "seat": "Seat/capital/place of rule, or 'unknown'",
            "sovereigns": ["name(s) of ruler(s) if stated"],
            "claimants": ["pretenders/usurpers/regents if stated"],
            "status": "stable / contested / fallen / usurped / unknown",
            "description": "How this government rules; what it demands; how it is seen.",
            "notes": "Any hooks."
        }}
    ],
  "lore": [
    {{
      "id": "snake_case_id",
      "name": "Lore Entry Name",
      "tagline": "Three evocative words.",
      "category": "legend / prophecy / history / myth",
      "source": "Who told it or where it originates.",
      "status": "confirmed / rumored / forgotten",
      "notes": "Any hooks."
    }}
  ],
  "flora_fauna": [
    {{
      "id": "snake_case_id",
      "name": "Creature or Plant Name",
      "tagline": "Three evocative words.",
      "type": "creature / beast / plant / fungus / spirit",
      "rarity": "common / rare / legendary",
      "habitat": "Where it lives.",
      "status": "thriving / endangered / extinct",
      "notes": "Any hooks."
    }}
  ],
  "magic": [
    {{
      "id": "snake_case_id",
      "name": "Spell or Ability Name",
      "tagline": "Three evocative words.",
      "type": "spell / ritual / passive / curse / technique",
      "element": "fire / shadow / time / blood / void / etc",
      "difficulty": "novice / adept / master / forbidden",
      "status": "known / lost / forbidden",
      "notes": "Any hooks."
    }}
  ],
  "relics": [
    {{
      "id": "snake_case_id",
      "name": "Relic Name",
      "tagline": "Three evocative words.",
      "origin": "Where it came from.",
      "power": "What it does.",
      "curse": "What it costs or corrupts.",
      "status": "active / dormant / destroyed / sealed",
      "notes": "Any hooks."
    }}
  ],
  "regions": [
    {{
      "id": "snake_case_id",
            "name": "Region Name",
      "tagline": "Three evocative words.",
            "continent": "Name or 'unknown'",
            "realm": "Name or 'unknown'",
            "climate": "arctic / desert / temperate / volcanic / blighted / etc",
            "terrain": "mountains / forest / plains / sea / ruins / etc",
            "function": "What this region IS for in the world bible (climate zone, travel reality, hazards, culture vibe).",
            "status": "stable / contested / fallen / cursed",
      "notes": "Any hooks."
    }}
  ],
    "realms": [
        {{
            "id": "snake_case_id",
            "name": "Realm Name",
            "tagline": "Three evocative words.",
            "description": "1–2 sentence description.",
            "continent": "Name or 'unknown'",
            "capital": "Capital city or 'unknown'",
            "function": "What this realm IS for (sovereignty, law, diplomacy, taxes).",
            "taxation": "How they tax/collect/tribute.",
            "military": "How they defend/expand (legions, marches, navy, etc).",
            "status": "stable / contested / fallen / cursed",
            "notes": "Any hooks."
        }}
    ],
    "continents": [
        {{
            "id": "snake_case_id",
            "name": "Continent Name",
            "tagline": "Three evocative words.",
            "description": "1–2 sentence description.",
            "function": "What this continent IS for (macro-biomes, cultural sphere, long-distance travel logic).",
            "status": "stable / fragmented / unknown",
            "notes": "Any hooks."
        }}
    ],
    "hemispheres": [
        {{
            "id": "snake_case_id",
            "name": "Hemisphere Name",
            "tagline": "Three evocative words.",
            "description": "1–2 sentence description.",
            "function": "What this hemisphere IS for (climate/season logic at global scale).",
            "status": "known / unknown",
            "notes": "Any hooks."
        }}
    ],
    "provinces": [
        {{
            "id": "snake_case_id",
            "name": "Province / Territory Name",
            "tagline": "Three evocative words.",
            "description": "1–2 sentence description.",
            "realm": "Realm Name or 'unknown'",
            "region": "Region Name or 'unknown'",
            "function": "What this province IS for (tax zone, administration, logistics).",
            "status": "stable / contested / unknown",
            "notes": "Any hooks."
        }}
    ],
    "districts": [
        {{
            "id": "snake_case_id",
            "name": "District Name",
            "tagline": "Three evocative words.",
            "description": "1–2 sentence description.",
            "parent_place": "City/Place that contains this district, or 'unknown'",
            "province": "Province Name or 'unknown'",
            "region": "Region Name or 'unknown'",
            "function": "What this district IS for (ward/quarter/neighborhood, docks, market, temple row, etc).",
            "status": "stable / contested / unknown",
            "notes": "Any hooks."
        }}
    ],
  "substances": [
    {{
      "id": "snake_case_id",
      "name": "Substance or Material Name",
      "tagline": "Three evocative words.",
      "type": "poison / metal / herb / elixir / mineral / etc",
      "rarity": "common / rare / legendary",
      "properties": "What it does.",
      "use": "How it is typically used.",
      "notes": "Any hooks."
    }}
  ]
}}"""

# ── Lore merging ────────────────────────────────────────────────────────
def _strip_trailing_parenthetical(name: str) -> str:
    if not name:
        return ""
    # Strip one trailing parenthetical qualifier: "Name (as Region)" -> "Name"
    return re.sub(r"\s*\([^)]*\)\s*$", "", str(name)).strip()


def _norm_entity_key(name: str) -> str:
    if not name:
        return ""
    s = str(name).strip().replace("’", "'")
    s = _strip_trailing_parenthetical(s)
    s = re.sub(r"\s+", " ", s)
    return s.casefold()


def _character_alias_keys(name: str) -> set:
    """Return a set of safe alias keys derived from a canonical character name."""
    key = _norm_entity_key(name)
    out = set([key]) if key else set()
    if not key:
        return out

    if key.startswith("the "):
        out.add(key[4:].strip())

    the_idx = key.find(" the ")
    if the_idx > 2:
        out.add(key[:the_idx].strip())

    comma_idx = key.find(",")
    if comma_idx > 2:
        out.add(key[:comma_idx].strip())

    base = _norm_entity_key(_strip_trailing_parenthetical(name))
    if base and base != key:
        out.add(base)
    return {x for x in out if x and len(x) >= 2}


def _is_descriptor_placeholder_character_name(name: str) -> bool:
    """Return True for generic role labels like 'Young Scholar'."""
    raw = str(name or "").strip()
    if not raw:
        return False
    tokens = re.findall(r"[A-Za-z][A-Za-z'\-]*", raw.replace("’", "'"))
    if not tokens:
        return False

    descriptor_words = {
        "young", "old", "aged", "ancient", "displaced", "junior", "senior",
        "nameless", "faceless", "scarred", "cursed", "transformed", "hidden",
        "wandering", "lost", "last", "unknown", "mysterious", "refugee",
        "scholar", "scribe", "merchant", "widow", "captain", "commander",
        "auditor", "apprentice", "keeper", "warden", "sorcerer", "forger",
        "cartographer", "alchemist", "thief", "broker", "trader", "woman",
        "man", "child", "girl", "boy", "prince", "princess", "lord", "lady",
        "king", "queen", "emperor", "empress", "warrior", "hunter", "archer",
        "initiate", "merchant-prince", "bone-singer", "shade-merchant",
        "deepkin", "sailor", "diplomat", "network", "voice",
    }
    stop_words = {"the", "a", "an", "of", "and", "from", "to", "for", "in", "on", "at"}
    content = [t.casefold() for t in tokens if t.casefold() not in stop_words]
    if not content:
        return False
    return all(t in descriptor_words for t in content)


def _looks_like_specific_character_name(name: str) -> bool:
    """Return True for names that look more specific than role placeholders."""
    raw = str(name or "").strip()
    if not raw:
        return False
    if _is_descriptor_placeholder_character_name(raw):
        return False
    tokens = re.findall(r"[A-Za-z][A-Za-z'\-]*", raw.replace("’", "'"))
    if not tokens:
        return False
    first = tokens[0]
    return first[:1].isupper()


def _should_skip_character_auto_add(lore: dict, name: str, descriptor: str) -> bool:
    """Reject abstract/event-like concepts being auto-promoted into characters."""
    descriptor_tokens = {
        token.casefold()
        for token in re.findall(r"[A-Za-z][A-Za-z'\-]*", str(descriptor or "").replace("’", "'"))
    }
    name_tokens = {
        token.casefold()
        for token in re.findall(r"[A-Za-z][A-Za-z'\-]*", str(name or "").replace("’", "'"))
    }
    concept_tokens = descriptor_tokens | name_tokens
    abstract_terms = {
        "mechanism", "ritual", "working", "event", "cycle", "phenomenon",
        "process", "aftermath", "collapse", "convergence", "threshold", "reversal",
        "question", "questions", "questioning", "riddle", "stasis", "recursion",
    }
    if not (concept_tokens & abstract_terms):
        return False

    candidate_keys = _character_alias_keys(name)
    if not candidate_keys:
        return False

    non_character_categories = (
        "events",
        "rituals",
        "magic",
        "lore",
        "relics",
        "artifacts",
        "deities_and_entities",
    )
    known_non_character_keys = set()
    for cat in non_character_categories:
        for item in (lore.get(cat) or []):
            if not isinstance(item, dict):
                continue
            item_name = str(item.get("name") or "").strip()
            if item_name:
                known_non_character_keys.add(_norm_entity_key(item_name))
            for alias in (item.get("aliases") or []):
                alias_key = _norm_entity_key(alias)
                if alias_key:
                    known_non_character_keys.add(alias_key)

    return any(key in known_non_character_keys for key in candidate_keys)


def _should_promote_character_name(existing_name: str, incoming_name: str) -> bool:
    """Return True when the incoming character name should become canonical."""
    exk = _norm_entity_key(existing_name)
    ink = _norm_entity_key(incoming_name)
    if not exk or not ink or exk == ink:
        return False

    if (" the " in ink or "," in ink) and (" the " not in exk and "," not in exk) and ink.split(" ")[0] == exk.split(" ")[0]:
        return True

    if _is_descriptor_placeholder_character_name(existing_name) and _looks_like_specific_character_name(incoming_name):
        return True

    return False


def _resolve_character_target(existing_chars: list, incoming_name: str):
    """Resolve an incoming character name to an existing canonical entry when safe.

    Prefers matching by exact name, explicit aliases, or epithet/"the" reduction.
    Also supports safe single-token->full-name mapping when unambiguous.
    """
    inc_key = _norm_entity_key(incoming_name)
    if not inc_key:
        return None

    by_key = {}
    alias_hits = []
    for c in existing_chars or []:
        if not isinstance(c, dict):
            continue
        nm = (c.get("name") or "").strip()
        if not nm:
            continue
        if _norm_entity_key(nm) == inc_key:
            return c
        for k in _character_alias_keys(nm):
            by_key.setdefault(k, c)

        aliases = c.get("aliases")
        if isinstance(aliases, list):
            for a in aliases:
                ak = _norm_entity_key(a)
                if ak:
                    by_key.setdefault(ak, c)
                    if ak == inc_key:
                        alias_hits.append(c)

    if len({id(x) for x in alias_hits}) == 1 and alias_hits:
        return alias_hits[0]

    if inc_key in by_key:
        return by_key[inc_key]

    return None


def _first_token_collision_candidates(lore: dict) -> dict:
    """Return mapping of unique first-name tokens -> canonical character entry.

    We only include tokens that map to exactly one canonical character to reduce
    false positives. This supports catching stories that introduce a new
    character with the same first name as an existing canon character.
    """
    out = {}
    if not isinstance(lore, dict):
        return out
    chars = lore.get("characters") or []
    if not isinstance(chars, list) or not chars:
        return out

    buckets = {}
    for c in chars:
        if not isinstance(c, dict):
            continue
        nm = str(c.get("name") or "").strip()
        if not nm:
            continue
        key = _norm_entity_key(nm)
        if not key:
            continue
        tok = key.split(" ")[0]
        if not tok or tok in {"the", "a", "an"}:
            continue
        if len(tok) < 3:
            continue
        buckets.setdefault(tok, []).append(c)

    for tok, items in buckets.items():
        if len({id(x) for x in items}) == 1:
            out[tok] = items[0]
    return out


def _find_first_token_character_collisions(stories: list, lore: dict, allowed_names_lower: set) -> list:
    """Detect unique canon first-name tokens used without the full canonical name.

    Example: if canon has "Kess of the Drowned Hollows" and a new story uses
    "Kess" (but not the full canonical name), we treat "Kess" as an accidental
    canon name collision and ask the rename pass to rename it.
    """
    if not isinstance(stories, list) or not stories:
        return []

    allowed = allowed_names_lower or set()
    tok_to_char = _first_token_collision_candidates(lore)
    if not tok_to_char:
        return []

    collisions = []
    for s in stories:
        if not isinstance(s, dict):
            continue
        blob = str((s.get("title") or "").strip()) + "\n" + str((s.get("text") or "").strip())
        if not blob.strip():
            continue
        blob_norm = _norm_text_for_matching(blob)

        for tok, canon in tok_to_char.items():
            canon_name = str((canon or {}).get("name") or "").strip()
            display_tok = (canon_name.split()[0] if canon_name and canon_name.split() else tok)
            if canon_name and canon_name.casefold() in allowed:
                continue
            if canon_name and entity_name_mentioned_in_text(canon_name, blob):
                continue
            aliases = (canon or {}).get("aliases")
            if isinstance(aliases, list):
                if any(entity_name_mentioned_in_text(str(a or "").strip(), blob) for a in aliases if str(a or "").strip()):
                    continue

            if re.search(r"(?<![a-z0-9])" + re.escape(tok) + r"(?![a-z0-9])", blob_norm):
                collisions.append({
                    "name": display_tok,
                    "canon": canon_name,
                    "reason": "used unique canon first-name token without full canonical name",
                })

    # Deduplicate by collision name.
    seen = set()
    out = []
    for it in collisions:
        nm = str(it.get("name") or "")
        key = nm.casefold() if nm else ""
        if nm and key not in seen:
            out.append(it)
            seen.add(key)
    return out


# ── Cross-category entity sync ────────────────────────────────────────────
_ARTICLE_RE = re.compile(r'^(the|a|an)\s+', re.IGNORECASE)


def _strip_articles(name: str) -> str:
    """Strip leading articles for fuzzy cross-category matching."""
    return _ARTICLE_RE.sub('', name).strip()


def sync_cross_category_appearances(codex: dict) -> int:
    """Sync story_appearances & name across entries that represent the same entity
    in different codex categories.

    Matching heuristic (must match on *all* of):
      1. Article-stripped, casefolded name  (e.g. "The Lamia" ↔ "Lamia")
      2. OR explicit alias overlap.

    When a match is found:
      • The union of all story_appearances is pushed to every matching entry.
      • The "canonical" name (longest / most specific) is adopted everywhere.
      • The appearances count is recomputed.

    Returns the number of entries that were updated.
    """
    if not isinstance(codex, dict):
        return 0

    # Categories that hold named entities with story_appearances.
    ENTITY_CATS = [
        "characters", "places", "events", "rituals", "weapons",
        "artifacts", "factions", "lore", "flora_fauna", "magic",
        "relics", "regions", "substances", "polities",
        "hemispheres", "continents", "realms",
        "provinces", "districts",
    ]

    # Build a map: normalised-key → [(category, entry), ...]
    groups = {}  # type: dict[str, list[tuple[str, dict]]]

    for cat in ENTITY_CATS:
        items = codex.get(cat)
        if not isinstance(items, list):
            continue
        for item in items:
            if not isinstance(item, dict):
                continue
            raw_name = (item.get("name") or "").strip()
            if not raw_name:
                continue
            # Primary key: article-stripped + casefolded
            key = _strip_articles(_norm_entity_key(raw_name))
            if not key:
                continue
            groups.setdefault(key, []).append((cat, item))

            # Also index by each alias so "The Lamia" alias on characters
            # links to "Lamia" in flora_fauna.
            for alias in (item.get("aliases") or []):
                if cat == "characters" and _is_descriptor_placeholder_character_name(alias):
                    continue
                akey = _strip_articles(_norm_entity_key(alias))
                if akey and akey != key:
                    groups.setdefault(akey, []).append((cat, item))

    updated_count = 0

    for key, members in groups.items():
        if len(members) < 2:
            continue

        # De-dup (same object can appear under multiple keys).
        seen_ids = set()
        unique = []
        for cat, entry in members:
            eid = id(entry)
            if eid not in seen_ids:
                seen_ids.add(eid)
                unique.append((cat, entry))
        if len(unique) < 2:
            continue

        # ── Union story_appearances across all entries ──────────────
        all_apps = {}  # (date, title) → dict
        for _cat, entry in unique:
            for app in (entry.get("story_appearances") or []):
                if not isinstance(app, dict):
                    continue
                d = str(app.get("date", "") or "").strip()
                t = str(app.get("title", "") or "").strip()
                if d and t:
                    all_apps[(d, t)] = {"date": d, "title": t}

        if not all_apps:
            continue

        merged_apps = sorted(all_apps.values(), key=lambda a: (a["date"], a["title"]))

        # ── Pick canonical name (longest form, preferring explicit articles) ─
        names = [(entry.get("name") or "").strip() for _c, entry in unique]
        canonical = max(names, key=len) if names else ""

        # ── Push updates to each entry ──────────────────────────────
        for _cat, entry in unique:
            old_apps = entry.get("story_appearances") or []
            old_name = (entry.get("name") or "").strip()

            changed = False

            # Sync story_appearances
            if len(merged_apps) != len(old_apps):
                entry["story_appearances"] = list(merged_apps)
                entry["appearances"] = len(merged_apps)
                changed = True
            else:
                # Same length but maybe different content
                old_set = {(str(a.get("date", "")), str(a.get("title", "")))
                           for a in old_apps if isinstance(a, dict)}
                new_set = {(a["date"], a["title"]) for a in merged_apps}
                if old_set != new_set:
                    entry["story_appearances"] = list(merged_apps)
                    entry["appearances"] = len(merged_apps)
                    changed = True

            # Sync name to canonical form
            if canonical and old_name != canonical:
                # Keep the old name as an alias
                aliases = entry.get("aliases")
                if not isinstance(aliases, list):
                    aliases = []
                if old_name and old_name not in aliases and old_name != canonical:
                    aliases.append(old_name)
                # Also add canonical to aliases if not already present
                entry["aliases"] = aliases
                entry["name"] = canonical
                changed = True

            if changed:
                updated_count += 1

    return updated_count


def merge_lore(existing_lore, new_lore, date_key):
    """Merge newly extracted lore into the existing lore, skipping duplicates by name."""
    for category in [
        "hemispheres",
        "continents",
        "realms",
        "polities",
        "provinces",
        "districts",
        "characters",
        "places",
        "events",
        "rituals",
        "weapons",
        "deities_and_entities",
        "artifacts",
        "factions",
        "lore",
        "flora_fauna",
        "magic",
        "relics",
        "regions",
        "substances",
    ]:
        if category == "characters":
            existing_list = existing_lore.get("characters") or []
            if not isinstance(existing_list, list):
                existing_list = []
                existing_lore["characters"] = existing_list

            for item in new_lore.get("characters", []) or []:
                if not isinstance(item, dict):
                    continue
                incoming_name = (item.get("name") or "").strip()
                if not incoming_name:
                    continue

                target = _resolve_character_target(existing_list, incoming_name)
                if target is None:
                    # Tag with first appearance date
                    item["first_date"] = date_key
                    item["appearances"] = 1
                    # Normalize aliases array if present
                    if "aliases" in item and not isinstance(item.get("aliases"), list):
                        item["aliases"] = []
                    existing_list.append(item)
                    continue

                # Merge into existing canonical character.
                target["appearances"] = target.get("appearances", 1) + 1

                # Prefer the more complete name as canonical.
                existing_name = (target.get("name") or "").strip()
                if existing_name and incoming_name:
                    exk = _norm_entity_key(existing_name)
                    ink = _norm_entity_key(incoming_name)
                    if exk and ink and exk != ink:
                        # Upgrade if incoming is more specific than the existing canonical form.
                        if _should_promote_character_name(existing_name, incoming_name):
                            # Preserve old as alias
                            aliases = target.get("aliases")
                            if not isinstance(aliases, list):
                                aliases = []
                            if existing_name not in aliases:
                                aliases.append(existing_name)
                            target["aliases"] = aliases
                            target["name"] = incoming_name

                        # Otherwise keep canonical name, but record incoming as alias.
                        aliases = target.get("aliases")
                        if not isinstance(aliases, list):
                            aliases = []
                        if incoming_name != target.get("name") and incoming_name not in aliases:
                            aliases.append(incoming_name)
                        target["aliases"] = aliases

                # Fill any missing fields without overwriting established canon.
                for k, v in item.items():
                    if k in {"name", "first_date", "appearances"}:
                        continue
                    if v is None:
                        continue
                    existing_v = target.get(k)
                    if (
                        k not in target
                        or existing_v is None
                        or existing_v == ""
                        or (isinstance(existing_v, str) and existing_v.strip().lower() == "unknown")
                        or existing_v == []
                        or existing_v == {}
                    ):
                        target[k] = v
        else:
            existing_names = {
                item["name"].lower() for item in existing_lore.get(category, [])
            }
            for item in new_lore.get(category, []):
                if item.get("name", "").lower() not in existing_names:
                    # Tag with first appearance date
                    item["first_date"] = date_key
                    item["appearances"] = 1
                    existing_lore.setdefault(category, []).append(item)
                    existing_names.add(item["name"].lower())
                else:
                    # Increment appearance count for existing entries
                    for existing_item in existing_lore.get(category, []):
                        if existing_item["name"].lower() == item["name"].lower():
                            existing_item["appearances"] = existing_item.get("appearances", 1) + 1
                            break
    ensure_place_parent_chain(existing_lore)
    enforce_continent_limit(existing_lore)
    return existing_lore


def warn_polity_conflicts(lore: dict):
    """Print warnings for potentially contradictory crown/sovereignty claims.

    This is heuristic and non-fatal. It aims to catch the most common issue:
    multiple different sole sovereigns implied for the same realm/region.
    """
    if not isinstance(lore, dict):
        return
    polities = lore.get("polities") or []
    if not isinstance(polities, list) or not polities:
        return

    def _clean(v: str) -> str:
        return (v or "").strip()

    def _is_unknown(v: str) -> bool:
        return not _clean(v) or _clean(v).lower() in {"unknown", "n/a", "na", "none"}

    def _status_allows_conflict(status: str) -> bool:
        s = (_clean(status)).lower()
        return any(w in s for w in ("contested", "disputed", "usurped", "civil war", "succession"))

    def _sovereign_list(p: dict):
        raw = p.get("sovereigns")
        if isinstance(raw, list):
            out = [str(x).strip() for x in raw if str(x).strip()]
            return [x for x in out if not _is_unknown(x)]
        if isinstance(raw, str):
            x = raw.strip()
            return [x] if x and not _is_unknown(x) else []
        return []

    buckets = {}  # key -> list of (name, sovereigns, status)
    for p in polities:
        if not isinstance(p, dict):
            continue
        name = _clean(p.get("name") or "")
        if not name:
            continue
        realm = _clean(p.get("realm") or "")
        region = _clean(p.get("region") or "")
        seat = _clean(p.get("seat") or "")
        key = None
        if not _is_unknown(realm):
            key = f"realm:{realm.lower()}"
        elif not _is_unknown(region):
            key = f"region:{region.lower()}"
        elif not _is_unknown(seat):
            key = f"seat:{seat.lower()}"
        else:
            continue

        sovs = _sovereign_list(p)
        if not sovs:
            continue
        buckets.setdefault(key, []).append((name, sovs, _clean(p.get("status") or "")))

    for key, items in buckets.items():
        if len(items) < 2:
            continue
        if any(_status_allows_conflict(status) for _, _, status in items):
            continue

        all_names = sorted({n for _, sovs, _ in items for n in sovs})
        if len(all_names) <= 1:
            continue

        # Allow a single polity that explicitly lists co-rulers.
        has_corulers = any(len(set(sovs)) >= 2 for _, sovs, _ in items)
        if has_corulers and len(all_names) == 2:
            continue

        print(
            f"WARNING: Possible crown/sovereignty conflict for {key}: "
            + "; ".join([f"{pol_name} -> {', '.join(sovs)}" for pol_name, sovs, _ in items]),
            file=sys.stderr,
        )


def _count_story_mentions(stories, name: str) -> int:
    if not stories or not name:
        return 0
    key = _signature_key_for_name(name)
    if not key or len(key) < 4:
        return 0
    return sum(
        1
        for s in stories
        if key in ((s.get("title", "") + " " + s.get("text", "")).lower())
    )


def build_existing_character_updates_prompt(stories, lore, referenced_characters):
    """Prompt to update already-known characters based on today's stories.

    Important: treat canon as current truth. If a story is clearly a prequel/flashback,
    do NOT change the current status — only add a note.
    """
    stories_payload = json.dumps(stories, ensure_ascii=False, indent=2)

    canon_chars = []
    for c in referenced_characters or []:
        canon_chars.append(json.dumps(c, ensure_ascii=False, sort_keys=True))

    world_rules = []
    if lore.get("worlds") and lore["worlds"][0].get("rules"):
        world_rules = lore["worlds"][0]["rules"]
    rules_block = "\n".join([f"- {r}" for r in world_rules]) if world_rules else "- (none)"

    return f"""You are the lore archivist updating an ongoing sword-and-sorcery universe.

Your job: for ALREADY KNOWN characters referenced today, extract ONLY NEW facts revealed by today's stories.

Key focus: STATUS TRACKING.
- If the stories clearly kill a character, mark them dead.
- If the stories clearly resurrect/reanimate/raise a character, update status accordingly (reanimated/undead/revived/etc).
- If a character is shown alive but their current canon status is dead, treat it as a PREQUEL/FLASHBACK unless the story explicitly resurrects them.
- Never contradict canon; when uncertain, leave status unchanged.

Secondary focus: TRAVEL SCOPE.
- We track how much of a traveler a character is so distant-realm meetings stay plausible.
- Set travel_scope only when the stories provide clear evidence.
- Values:
    - local: mostly stays within one city/settlement/structure
    - regional: travels within a region
    - realmwide: travels broadly within a realm
    - interrealm: travels between realms
    - unknown: insufficient evidence

Canon rules:
{rules_block}

Authoritative canon character entries (current truth):
{os.linesep.join(canon_chars) if canon_chars else '(none)'}

Return ONLY valid JSON with this structure:
{{
  "characters": [
    {{
      "name": "Character Name (must match canon)",
      "status": "new current status (only if it should change)",
      "should_update_current_status": true,
            "travel_scope": "local|regional|realmwide|interrealm|unknown (only if you have evidence)",
            "home_place": "Optional: update if clearly established, else omit",
            "home_region": "Optional: update if clearly established, else omit",
            "home_realm": "Optional: update if clearly established, else omit",
            "travel_note": "Optional: 1 sentence justification for travel_scope/home fields",
      "event": {{
                "type": "death|resurrection|reanimation|undeath|prequel|other",
        "story_title": "Which story caused the change",
        "note": "One sentence describing what happened",
        "evidence": "Short quote-like paraphrase of the story text"
      }},
      "notes_append": "Optional: add 1-2 sentences as a hook or clarification"
    }}
  ]
}}

Constraints:
- Only include characters that appear in the canon list above.
- If no status change, omit the character unless you are recording a meaningful event (e.g., prequel appearance while canon-dead).
- Keep notes concise.

STORIES JSON INPUT:
{stories_payload}
"""


def apply_existing_character_updates(lore, updates, date_key, stories=None):
    """Apply status updates + history to lore.json characters."""
    if not updates or not isinstance(updates, dict):
        return lore

    chars = lore.get("characters", []) or []
    idx = { (c.get("name") or "").strip().lower(): c for c in chars if c.get("name") }

    for up in updates.get("characters", []) or []:
        name = (up.get("name") or "").strip()
        if not name:
            continue
        key = name.lower()
        if key not in idx:
            continue

        current = idx[key]
        old_status = current.get("status")
        new_status = (up.get("status") or "").strip()
        should_update = bool(up.get("should_update_current_status"))
        event = up.get("event") if isinstance(up.get("event"), dict) else {}

        # Always increment appearances if the character is referenced today.
        mention_count = _count_story_mentions(stories or [], name)
        if mention_count:
            try:
                current["appearances"] = int(current.get("appearances", 0) or 0) + int(mention_count)
            except Exception:
                current["appearances"] = (current.get("appearances") or 0) + mention_count

        event_type = (event.get("type") or "").strip() or "other"
        has_event_payload = any((event.get("story_title"), event.get("note"), event.get("evidence")))

        if has_event_payload:
            hist = current.get("status_history")
            if not isinstance(hist, list):
                hist = []

            # For non-status-change events (e.g. prequel), record from==to.
            to_status_for_event = new_status if (should_update and new_status) else (old_status or "unknown")
            hist.append({
                "date": date_key,
                "from_status": old_status or "unknown",
                "to_status": to_status_for_event,
                "type": event_type,
                "story_title": (event.get("story_title") or ""),
                "note": (event.get("note") or ""),
                "evidence": (event.get("evidence") or ""),
            })
            current["status_history"] = hist

        if should_update and new_status and (not old_status or old_status.strip().lower() != new_status.lower()):
            current["status"] = new_status

        notes_append = (up.get("notes_append") or "").strip()
        if notes_append:
            existing_notes = (current.get("notes") or "").strip()
            if existing_notes:
                current["notes"] = existing_notes.rstrip() + "\n" + notes_append
            else:
                current["notes"] = notes_append

        # Travel scope + home anchoring (only update when explicitly provided).
        travel_scope = (up.get("travel_scope") or "").strip().lower()
        if travel_scope in {"local", "regional", "realmwide", "interrealm", "unknown"}:
            current["travel_scope"] = travel_scope

        for field in ["home_place", "home_region", "home_realm"]:
            if field in up:
                val = (up.get(field) or "").strip()
                if val:
                    current[field] = val

        travel_note = (up.get("travel_note") or "").strip()
        if travel_note:
            existing = (current.get("travel_notes") or "").strip()
            stamp = f"[{date_key}] {travel_note}"
            current["travel_notes"] = (existing + "\n" + stamp).strip() if existing else stamp

    lore["characters"] = chars
    return lore

# ── Codex file update ────────────────────────────────────────────────────
def update_codex_file(lore, date_key, stories=None, assume_all_from_stories: bool = False):
    """Merge today's lore into codex.json, covering all entity types with story appearances."""
    stories = stories or []

    # ── Load existing codex ──────────────────────────────────────────────
    codex = {
        "last_updated": date_key,
        "hemispheres": [],
        "continents": [],
        "realms": [],
        "polities": [],
        "provinces": [],
        "districts": [],
        "characters": [],
        "places": [],
        "events": [],
        "rituals": [],
        "weapons": [],
        "deities_and_entities": [],
        "artifacts": [],
        "factions": [],
        "lore": [],
        "flora_fauna": [],
        "magic": [],
        "relics": [],
        "regions": [],
        "substances": [],
    }
    if os.path.exists(CODEX_FILE):
        try:
            with open(CODEX_FILE, "r", encoding="utf-8") as f:
                codex = json.load(f)
            if isinstance(codex, dict):
                codex.pop("subcontinents", None)
                codex.setdefault("deities_and_entities", [])
        except (json.JSONDecodeError, IOError):
            pass

    # ── Helper: find stories that mention an entity by name ─────────────
    def stories_for(name):
        # In single-story audit mode, everything extracted is from that story.
        if assume_all_from_stories and len(stories) == 1:
            only_title = str((stories[0] or {}).get("title", "") or "").strip()
            if only_title:
                return [{"date": date_key, "title": only_title}]

        def _norm_blob(s: str) -> str:
            return (
                str(s or "")
                .replace("\u2019", "'")
                .replace("\u2018", "'")
                .replace("\u2011", "-")
                .lower()
            )

        text_blobs = [
            _norm_blob((s.get("text", "") or "") + " " + (s.get("title", "") or ""))
            for s in stories
            if isinstance(s, dict)
        ]

        # Mention detection: strict surface-form phrase match with boundaries.
        # This avoids substring false positives like "crow" matching "crown".
        raw_name = _strip_trailing_parenthetical(str(name or "").strip())

        def _phrase_in_blob(phrase: str, blob: str) -> bool:
            phrase = (phrase or "").strip()
            if not phrase:
                return False
            return bool(re.search(r"(?<![a-z0-9])" + re.escape(_norm_blob(phrase)) + r"(?![a-z0-9])", blob))

        def _mentions(blob: str) -> bool:
            if not blob:
                return False
            return _phrase_in_blob(raw_name, blob)

        hits = []
        for s, blob in zip([s for s in stories if isinstance(s, dict)], text_blobs):
            if _mentions(blob):
                hits.append({"date": date_key, "title": s.get("title", "")})
        return hits

    # ── Helper: resolve world name from lore worlds list ─────────────────
    def resolve_world(raw_world):
        return next(
            (w["name"] for w in lore.get("worlds", []) if w["id"] == raw_world),
            raw_world or "The Known World"
        )

    def merge_named_category(cat_key: str, field_keys: list):
        existing = {i.get("name", "").lower(): i for i in codex.get(cat_key, []) if isinstance(i, dict) and i.get("name")}

        def _should_overwrite(v) -> bool:
            if v is None:
                return False
            if isinstance(v, str):
                return _truthy_non_unknown(v)
            if isinstance(v, (list, dict)):
                return len(v) > 0
            return True

        for item in lore.get(cat_key, []) or []:
            if not isinstance(item, dict):
                continue
            name = (item.get("name") or "").strip()
            if not name:
                continue
            name_low = name.lower()
            today_apps = stories_for(name)
            if name_low in existing:
                ex = existing[name_low]
                for k in field_keys:
                    if k in item and _should_overwrite(item.get(k)):
                        ex[k] = item.get(k)
                prior = ex.get("story_appearances", [])
                new_ones = [a for a in today_apps if not any(p["date"] == a["date"] and p["title"] == a["title"] for p in prior)]
                if new_ones:
                    ex["story_appearances"] = prior + new_ones
                # Keep appearances aligned with unique story_appearances when present.
                apps = ex.get("story_appearances", [])
                if isinstance(apps, list) and apps:
                    ex["appearances"] = len(apps)
                    if not (ex.get("first_story") or "").strip():
                        ex["first_story"] = apps[0].get("title", "")
                    if not (ex.get("first_date") or "").strip():
                        ex["first_date"] = apps[0].get("date", date_key)
            else:
                first_title = today_apps[0]["title"] if today_apps else ""
                base = {
                    "name": name,
                    "tagline": item.get("tagline", ""),
                    "first_story": first_title,
                    "first_date": date_key,
                    "appearances": len(today_apps) or 1,
                    "story_appearances": today_apps,
                }
                for k in field_keys:
                    if k not in base:
                        base[k] = item.get(k, "")
                existing[name_low] = base
        codex[cat_key] = list(existing.values())

    def _ensure_alias_list(obj: dict):
        aliases = obj.get("aliases")
        if aliases is None:
            return
        if not isinstance(aliases, list):
            obj["aliases"] = []

    def _merge_aliases(target: dict, incoming_aliases):
        if incoming_aliases is None:
            return
        if not isinstance(incoming_aliases, list):
            return
        cur = target.get("aliases")
        if not isinstance(cur, list):
            cur = []
        seen = {str(a).strip() for a in cur if str(a).strip()}
        for a in incoming_aliases:
            a = (str(a).strip() if a is not None else "")
            if a and a not in seen and a != target.get("name"):
                cur.append(a)
                seen.add(a)
        if cur:
            target["aliases"] = cur

    def _find_existing_character(existing_map: dict, incoming_name: str, incoming_aliases=None):
        key = _norm_entity_key(incoming_name)
        if not key:
            return None

        exact_alias_hits = []
        for obj in existing_chars_list:
            if not isinstance(obj, dict):
                continue
            nm = (obj.get("name") or "").strip()
            if nm and _norm_entity_key(nm) == key:
                return obj
            aliases = obj.get("aliases")
            if isinstance(aliases, list):
                for alias in aliases:
                    if _norm_entity_key(alias) == key:
                        exact_alias_hits.append(obj)
                        break
        if len({id(x) for x in exact_alias_hits}) == 1 and exact_alias_hits:
            return exact_alias_hits[0]

        if key in existing_map:
            return existing_map[key]
        # Try explicit aliases provided by lore.
        if isinstance(incoming_aliases, list):
            for a in incoming_aliases:
                if _is_descriptor_placeholder_character_name(a):
                    continue
                ak = _norm_entity_key(a)
                if ak and ak in existing_map:
                    return existing_map[ak]
        # Try epithetless alias ("X the Y" -> "X").
        the_idx = key.find(" the ")
        if the_idx > 2:
            base = key[:the_idx].strip()
            if base in existing_map:
                return existing_map[base]
        return None

    # ── Merge characters ─────────────────────────────────────────────────
    existing_chars_list = codex.get("characters", [])
    if not isinstance(existing_chars_list, list):
        existing_chars_list = []
    # Build a key->obj map including canonical keys and alias keys.
    existing_chars = {}
    for obj in existing_chars_list:
        if not isinstance(obj, dict):
            continue
        nm = (obj.get("name") or "").strip()
        if not nm:
            continue
        _ensure_alias_list(obj)
        for k in _character_alias_keys(nm):
            existing_chars.setdefault(k, obj)
        if isinstance(obj.get("aliases"), list):
            for a in obj.get("aliases"):
                ak = _norm_entity_key(a)
                if ak:
                    existing_chars.setdefault(ak, obj)

    for c in lore.get("characters", []):
        name = (c.get("name") or "Unknown").strip()
        aliases_in = c.get("aliases") if isinstance(c, dict) else None
        world = resolve_world(c.get("world", ""))
        today_appearances = stories_for(name)

        ex = _find_existing_character(existing_chars, name, aliases_in)
        if ex is not None:
            ex["role"]   = c.get("role",   ex.get("role",   "Unknown"))
            ex["status"] = c.get("status", ex.get("status", "Unknown"))
            if c.get("travel_scope"):
                ex["travel_scope"] = c.get("travel_scope", ex.get("travel_scope", "unknown"))
            if c.get("home_place"):
                ex["home_place"] = c.get("home_place", ex.get("home_place", ""))
            if c.get("home_region"):
                ex["home_region"] = c.get("home_region", ex.get("home_region", ""))
            if c.get("home_realm"):
                ex["home_realm"] = c.get("home_realm", ex.get("home_realm", ""))
            if isinstance(c.get("status_history"), list):
                ex["status_history"] = c.get("status_history", ex.get("status_history", []))
            ex["world"]  = world
            ex["bio"]    = c.get("bio",    ex.get("bio",    ""))
            ex["traits"] = c.get("traits", ex.get("traits", []))
            if c.get("tagline") and not ex.get("tagline"):
                ex["tagline"] = c["tagline"]

            _merge_aliases(ex, aliases_in)

            # Prefer the more complete name as canonical.
            ex_name = (ex.get("name") or "").strip()
            exk = _norm_entity_key(ex_name)
            ink = _norm_entity_key(name)
            if exk and ink and exk != ink:
                if _should_promote_character_name(ex_name, name):
                    _merge_aliases(ex, [ex_name])
                    ex["name"] = name
                else:
                    _merge_aliases(ex, [name])

            prior = ex.get("story_appearances", [])
            new_ones = [a for a in today_appearances
                        if not any(p["date"] == a["date"] and p["title"] == a["title"] for p in prior)]
            if new_ones:
                ex["story_appearances"] = prior + new_ones
            apps = ex.get("story_appearances", [])
            if isinstance(apps, list) and apps:
                ex["appearances"] = len(apps)
                if not (ex.get("first_story") or "").strip():
                    ex["first_story"] = apps[0].get("title", "")
                if not (ex.get("first_date") or "").strip():
                    ex["first_date"] = apps[0].get("date", date_key)
        else:
            first_title = today_appearances[0]["title"] if today_appearances else ""
            new_obj = {
                "name":              name,
                "aliases":           c.get("aliases", []) if isinstance(c.get("aliases"), list) else [],
                "tagline":           c.get("tagline", ""),
                "role":              c.get("role", "Unknown"),
                "status":            c.get("status", "Unknown"),
                "travel_scope":       c.get("travel_scope", "unknown"),
                "home_place":         c.get("home_place", ""),
                "home_region":        c.get("home_region", ""),
                "home_realm":         c.get("home_realm", ""),
                "status_history":     c.get("status_history", []) if isinstance(c.get("status_history"), list) else [],
                "world":             world,
                "bio":               c.get("bio", ""),
                "traits":            c.get("traits", []),
                "first_story":       first_title,
                "first_date":        date_key,
                "appearances":       len(today_appearances) or 1,
                "story_appearances": today_appearances,
            }

            _ensure_alias_list(new_obj)
            codex.setdefault("characters", []).append(new_obj)
            # Update index maps for subsequent merges in this run.
            for k in _character_alias_keys(new_obj.get("name")):
                existing_chars.setdefault(k, new_obj)
            for a in (new_obj.get("aliases") or []):
                ak = _norm_entity_key(a)
                if ak:
                    existing_chars.setdefault(ak, new_obj)

    # Consolidate duplicate character rows by canonical name and drop obvious
    # abstract/non-character concepts that already live in non-character categories.
    def _non_character_name_keys(src: dict) -> set:
        keys = set()
        if not isinstance(src, dict):
            return keys
        for cat in ("events", "rituals", "magic", "lore", "relics", "artifacts", "deities_and_entities"):
            for item in (src.get(cat) or []):
                if not isinstance(item, dict):
                    continue
                nm = str(item.get("name") or "").strip()
                if nm:
                    keys.add(_norm_entity_key(nm))
                aliases = item.get("aliases") if isinstance(item.get("aliases"), list) else []
                for alias in aliases:
                    ak = _norm_entity_key(alias)
                    if ak:
                        keys.add(ak)
        return keys

    def _is_abstract_character_concept(name: str, role: str, bio: str) -> bool:
        text = " ".join([str(name or ""), str(role or ""), str(bio or "")]).casefold()
        if not text.strip():
            return False
        markers = (
            "questioning", "questions", "question", "riddle", "ritual", "working",
            "cycle", "convergence", "ascension", "phenomenon", "recursive",
        )
        return any(m in text for m in markers)

    def _dedupe_story_apps_local(apps):
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

    def _merge_unique_list_local(a, b):
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
        return out

    def _merge_character_rows(target: dict, incoming: dict) -> dict:
        if not isinstance(target, dict):
            return incoming if isinstance(incoming, dict) else target
        if not isinstance(incoming, dict):
            return target

        _ensure_alias_list(target)
        _merge_aliases(target, incoming.get("aliases") if isinstance(incoming.get("aliases"), list) else [])

        # Prefer richer text fields while preserving known values.
        for key in ("role", "tagline", "bio"):
            cur = str(target.get(key) or "").strip()
            inc = str(incoming.get(key) or "").strip()
            if inc and (not cur or len(inc) > len(cur)):
                target[key] = inc

        for key in ("status", "travel_scope", "home_place", "home_region", "home_realm", "world"):
            inc = incoming.get(key)
            if isinstance(inc, str):
                if _truthy_non_unknown(inc) or not str(target.get(key) or "").strip():
                    target[key] = inc
            elif inc not in (None, "", [], {}):
                target[key] = inc

        target["traits"] = _merge_unique_list_local(target.get("traits", []), incoming.get("traits", []))

        sh_cur = target.get("status_history") if isinstance(target.get("status_history"), list) else []
        sh_inc = incoming.get("status_history") if isinstance(incoming.get("status_history"), list) else []
        sh_out = list(sh_cur)
        sh_seen = {
            (
                str(x.get("date") or "").strip(),
                str(x.get("to_status") or "").strip(),
                str(x.get("story_title") or "").strip(),
                str(x.get("note") or "").strip(),
            )
            for x in sh_cur if isinstance(x, dict)
        }
        for row in sh_inc:
            if not isinstance(row, dict):
                continue
            k = (
                str(row.get("date") or "").strip(),
                str(row.get("to_status") or "").strip(),
                str(row.get("story_title") or "").strip(),
                str(row.get("note") or "").strip(),
            )
            if k in sh_seen:
                continue
            sh_seen.add(k)
            sh_out.append(row)
        if sh_out:
            target["status_history"] = sh_out

        apps = _dedupe_story_apps_local((target.get("story_appearances") or []) + (incoming.get("story_appearances") or []))
        apps.sort(key=lambda a: (str(a.get("date") or ""), str(a.get("title") or "")))
        target["story_appearances"] = apps
        if apps:
            target["appearances"] = len(apps)
            target["first_date"] = apps[0].get("date") or target.get("first_date") or date_key
            target["first_story"] = apps[0].get("title") or target.get("first_story") or ""
        else:
            try:
                target["appearances"] = max(int(target.get("appearances") or 0), int(incoming.get("appearances") or 0), 1)
            except Exception:
                target["appearances"] = 1
            if not str(target.get("first_date") or "").strip():
                target["first_date"] = str(incoming.get("first_date") or date_key)
            if not str(target.get("first_story") or "").strip():
                target["first_story"] = str(incoming.get("first_story") or "")
        return target

    non_character_keys = _non_character_name_keys(lore) | _non_character_name_keys(codex)
    deduped_chars = []
    by_name_key = {}
    for obj in codex.get("characters", []) or []:
        if not isinstance(obj, dict):
            continue
        nm = str(obj.get("name") or "").strip()
        if not nm:
            continue
        nm_key = _norm_entity_key(nm)
        if not nm_key:
            continue

        if _is_abstract_character_concept(nm, str(obj.get("role") or ""), str(obj.get("bio") or "")) and nm_key in non_character_keys:
            continue

        existing = by_name_key.get(nm_key)
        if existing is None:
            _ensure_alias_list(obj)
            by_name_key[nm_key] = obj
            deduped_chars.append(obj)
            continue

        _merge_character_rows(existing, obj)

    codex["characters"] = deduped_chars

    # ── Geo hierarchy categories ─────────────────────────────────────────
    merge_named_category("hemispheres", ["tagline", "description", "function", "status", "notes"]) 
    merge_named_category("continents", ["tagline", "description", "hemispheres", "climate_zones", "function", "status", "notes"]) 
    merge_named_category("realms", ["tagline", "description", "continent", "capital", "function", "taxation", "military", "status", "notes"]) 
    merge_named_category("polities", ["polity_type", "realm", "region", "seat", "sovereigns", "claimants", "status", "description", "notes"]) 
    merge_named_category("provinces", ["tagline", "description", "realm", "region", "function", "status", "notes"]) 
    merge_named_category("districts", ["tagline", "description", "parent_place", "province", "region", "function", "status", "notes"]) 

    # ── Deities / Entities ───────────────────────────────────────────────
    merge_named_category("deities_and_entities", ["type", "world", "tagline", "description", "status", "notes", "aliases"]) 

    # ── Rituals ─────────────────────────────────────────────────────────
    merge_named_category("rituals", ["ritual_type", "performed_by", "requirements", "effect", "cost", "notes"])

    # ── Merge places ─────────────────────────────────────────────────────
    existing_places = {p["name"].lower(): p for p in codex.get("places", [])}
    for p in lore.get("places", []):
        name = p.get("name", "Unknown")
        name_low = name.lower()
        today_appearances = stories_for(name)
        if name_low in existing_places:
            ex = existing_places[name_low]
            incoming_parent_place = str(p.get("parent_place") or "").strip()
            if _truthy_non_unknown(incoming_parent_place):
                ex["parent_place"] = incoming_parent_place

            # Only allow non-unknown incoming geo fields to overwrite existing ones.
            for k in ["hemisphere", "continent", "realm", "province", "region", "district"]:
                incoming = str(p.get(k) or "").strip()
                if _truthy_non_unknown(incoming):
                    ex[k] = incoming
                else:
                    if k not in ex or ex.get(k) in (None, ""):
                        ex[k] = "unknown"

            ex["description"] = p.get("description", ex.get("description", ""))
            ex["status"]      = p.get("status",      ex.get("status",      "unknown"))
            if p.get("tagline") and not ex.get("tagline"):
                ex["tagline"] = p["tagline"]
            if p.get("place_type") and not ex.get("place_type"):
                ex["place_type"] = p["place_type"]
            if p.get("atmosphere") and not ex.get("atmosphere"):
                ex["atmosphere"] = p["atmosphere"]
            prior = ex.get("story_appearances", [])
            new_ones = [a for a in today_appearances
                        if not any(p2["date"] == a["date"] and p2["title"] == a["title"] for p2 in prior)]
            if new_ones:
                ex["appearances"] = ex.get("appearances", 1) + len(new_ones)
                ex["story_appearances"] = prior + new_ones
        else:
            first_title = today_appearances[0]["title"] if today_appearances else ""
            existing_places[name_low] = {
                "name":              name,
                "tagline":           p.get("tagline", ""),
                "place_type":        p.get("place_type", ""),
                "world":             resolve_world(p.get("world", "")),
                "parent_place":      p.get("parent_place", ""),
                "hemisphere":        p.get("hemisphere", "unknown"),
                "continent":         p.get("continent", "unknown"),
                "realm":             p.get("realm", "unknown"),
                "province":          p.get("province", "unknown"),
                "region":            p.get("region", "unknown"),
                "district":          p.get("district", "unknown"),
                "atmosphere":        p.get("atmosphere", ""),
                "description":       p.get("description", ""),
                "status":            p.get("status", "unknown"),
                "first_story":       first_title,
                "first_date":        date_key,
                "appearances":       len(today_appearances) or 1,
                "story_appearances": today_appearances,
            }
    codex["places"] = list(existing_places.values())

    for p in codex.get("places", []) or []:
        if isinstance(p, dict):
            p.pop("subcontinent", None)

    # ── Merge events ─────────────────────────────────────────────────────
    def _norm_key(s: str) -> str:
        return str(s or "").strip().lower()

    def _dedupe_story_appearances(apps: list[dict]) -> list[dict]:
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

    def _merge_unique_list(a, b) -> list:
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
        return out

    def _merge_event(ex: dict, incoming: dict) -> dict:
        if not isinstance(ex, dict):
            ex = {}
        if not isinstance(incoming, dict):
            return ex

        # Prefer existing, fill blanks from incoming.
        for k in ["tagline", "event_type", "outcome", "significance"]:
            if (not ex.get(k)) and incoming.get(k):
                ex[k] = incoming.get(k)

        for k in [
            "scope",
            "epicenter_place",
            "epicenter_region",
            "epicenter_realm",
            "radius",
        ]:
            if (ex.get(k) in (None, "", [], {})) and (incoming.get(k) not in (None, "", [], {})):
                ex[k] = incoming.get(k)

        # Merge list fields (stable).
        for k in ["participants", "affected_places", "affected_regions", "affected_realms", "aliases"]:
            ex[k] = _merge_unique_list(ex.get(k), incoming.get(k))

        # Merge story appearances and keep counts coherent.
        prior = _dedupe_story_appearances(ex.get("story_appearances") or [])
        inc = _dedupe_story_appearances(incoming.get("story_appearances") or [])
        ex["story_appearances"] = _dedupe_story_appearances(prior + inc)

        # Preserve first_story/first_date if present; otherwise backfill.
        if not ex.get("first_story") and incoming.get("first_story"):
            ex["first_story"] = incoming.get("first_story")
        if not ex.get("first_date") and incoming.get("first_date"):
            ex["first_date"] = incoming.get("first_date")

        # Keep the highest known appearance count, but never below unique story appearances.
        try:
            ex_app = int(ex.get("appearances") or 0)
        except Exception:
            ex_app = 0
        try:
            in_app = int(incoming.get("appearances") or 0)
        except Exception:
            in_app = 0
        ex["appearances"] = max(ex_app, in_app, len(ex.get("story_appearances") or []), 1)

        return ex

    existing_events: dict[str, dict] = {}
    for e in codex.get("events", []) or []:
        if not isinstance(e, dict):
            continue
        k = _norm_key(e.get("name"))
        if not k:
            continue
        if k in existing_events:
            existing_events[k] = _merge_event(existing_events[k], e)
        else:
            existing_events[k] = e

    for e in lore.get("events", []):
        name = str(e.get("name", "Unknown") or "Unknown").strip() or "Unknown"
        name_low = _norm_key(name)
        today_appearances = stories_for(name)
        if name_low in existing_events:
            ex = existing_events[name_low]
            # Preserve richer metadata when present.
            if e.get("tagline") and not ex.get("tagline"):
                ex["tagline"] = e.get("tagline")
            if e.get("event_type") and not ex.get("event_type"):
                ex["event_type"] = e.get("event_type")
            if e.get("participants") and not ex.get("participants"):
                ex["participants"] = e.get("participants")

            # Optional geo/scope fields (used for large-scale continuity arcs).
            for k in [
                "scope",
                "epicenter_place",
                "epicenter_region",
                "epicenter_realm",
                "affected_places",
                "affected_regions",
                "affected_realms",
                "radius",
            ]:
                if k in e and e.get(k) not in (None, "", [], {}):
                    if ex.get(k) in (None, "", [], {}):
                        ex[k] = e.get(k)

            ex["outcome"]      = e.get("outcome",      ex.get("outcome",      ""))
            ex["significance"] = e.get("significance", ex.get("significance", ""))
            prior = ex.get("story_appearances", [])
            new_ones = [a for a in today_appearances
                        if not any(p["date"] == a["date"] and p["title"] == a["title"] for p in prior)]
            if new_ones:
                ex["appearances"] = ex.get("appearances", 1) + len(new_ones)
                ex["story_appearances"] = prior + new_ones
        else:
            first_title = today_appearances[0]["title"] if today_appearances else ""
            existing_events[name_low] = {
                "name":              name,
                "tagline":           e.get("tagline", ""),
                "event_type":        e.get("event_type", ""),
                "scope":             e.get("scope", ""),
                "epicenter_place":   e.get("epicenter_place", ""),
                "epicenter_region":  e.get("epicenter_region", ""),
                "epicenter_realm":   e.get("epicenter_realm", ""),
                "affected_places":   e.get("affected_places", []),
                "affected_regions":  e.get("affected_regions", []),
                "affected_realms":   e.get("affected_realms", []),
                "radius":            e.get("radius", ""),
                "participants":      e.get("participants", []),
                "outcome":           e.get("outcome", ""),
                "significance":      e.get("significance", ""),
                "first_story":       first_title,
                "first_date":        date_key,
                "appearances":       len(today_appearances) or 1,
                "story_appearances": today_appearances,
            }
    # Final pass: ensure story_appearances are de-duped for all merged events.
    for ex in existing_events.values():
        if isinstance(ex, dict) and "story_appearances" in ex:
            ex["story_appearances"] = _dedupe_story_appearances(ex.get("story_appearances") or [])
            try:
                ex["appearances"] = max(int(ex.get("appearances") or 1), len(ex["story_appearances"]) or 1)
            except Exception:
                ex["appearances"] = len(ex["story_appearances"]) or 1

    codex["events"] = list(existing_events.values())

    # ── Merge weapons ────────────────────────────────────────────────────
    existing_weapons = {w["name"].lower(): w for w in codex.get("weapons", [])}
    for w in lore.get("weapons", []):
        name = w.get("name", "Unknown")
        name_low = name.lower()
        today_appearances = stories_for(name)
        if name_low in existing_weapons:
            ex = existing_weapons[name_low]
            ex["powers"]            = w.get("powers",            ex.get("powers",            ""))
            ex["last_known_holder"] = w.get("last_known_holder", ex.get("last_known_holder", ""))
            ex["status"]            = w.get("status",            ex.get("status",            "unknown"))
            prior = ex.get("story_appearances", [])
            new_ones = [a for a in today_appearances
                        if not any(p["date"] == a["date"] and p["title"] == a["title"] for p in prior)]
            if new_ones:
                ex["appearances"] = ex.get("appearances", 1) + len(new_ones)
                ex["story_appearances"] = prior + new_ones
        else:
            first_title = today_appearances[0]["title"] if today_appearances else ""
            existing_weapons[name_low] = {
                "name":              name,
                "tagline":           w.get("tagline", ""),
                "weapon_type":       w.get("weapon_type", ""),
                "origin":            w.get("origin", ""),
                "powers":            w.get("powers", ""),
                "last_known_holder": w.get("last_known_holder", ""),
                "status":            w.get("status", "unknown"),
                "first_story":       first_title,
                "first_date":        date_key,
                "appearances":       len(today_appearances) or 1,
                "story_appearances": today_appearances,
            }
    codex["weapons"] = list(existing_weapons.values())

    # ── Merge artifacts ──────────────────────────────────────────────────
    existing_artifacts = {a["name"].lower(): a for a in codex.get("artifacts", [])}
    for a in lore.get("artifacts", []):
        name = a.get("name", "Unknown")
        name_low = name.lower()
        today_appearances = stories_for(name)
        if name_low in existing_artifacts:
            ex = existing_artifacts[name_low]
            ex["powers"]            = a.get("powers",            ex.get("powers",            ""))
            ex["last_known_holder"] = a.get("last_known_holder", ex.get("last_known_holder", ""))
            ex["status"]            = a.get("status",            ex.get("status",            "unknown"))
            prior = ex.get("story_appearances", [])
            new_ones = [app for app in today_appearances
                        if not any(p["date"] == app["date"] and p["title"] == app["title"] for p in prior)]
            if new_ones:
                ex["appearances"] = ex.get("appearances", 1) + len(new_ones)
                ex["story_appearances"] = prior + new_ones
        else:
            first_title = today_appearances[0]["title"] if today_appearances else ""
            existing_artifacts[name_low] = {
                "name":              name,
                "tagline":           a.get("tagline", ""),
                "artifact_type":     a.get("artifact_type", ""),
                "origin":            a.get("origin", ""),
                "powers":            a.get("powers", ""),
                "last_known_holder": a.get("last_known_holder", ""),
                "status":            a.get("status", "unknown"),
                "first_story":       first_title,
                "first_date":        date_key,
                "appearances":       len(today_appearances) or 1,
                "story_appearances": today_appearances,
            }
    codex["artifacts"] = list(existing_artifacts.values())
    # ── Merge factions ──────────────────────────────────────────────
    existing_factions = {x["name"].lower(): x for x in codex.get("factions", [])}
    for f in lore.get("factions", []):
        name = f.get("name", "Unknown")
        name_low = name.lower()
        today_appearances = stories_for(name)
        if name_low in existing_factions:
            ex = existing_factions[name_low]
            ex["goals"]         = f.get("goals",         ex.get("goals",         ""))
            ex["leader"]        = f.get("leader",        ex.get("leader",        ""))
            ex["status"]        = f.get("status",        ex.get("status",        "unknown"))
            prior = ex.get("story_appearances", [])
            new_ones = [app for app in today_appearances
                        if not any(p["date"] == app["date"] and p["title"] == app["title"] for p in prior)]
            if new_ones:
                ex["appearances"] = ex.get("appearances", 1) + len(new_ones)
                ex["story_appearances"] = prior + new_ones
        else:
            first_title = today_appearances[0]["title"] if today_appearances else ""
            existing_factions[name_low] = {
                "name":              name,
                "tagline":           f.get("tagline", ""),
                "alignment":          f.get("alignment", ""),
                "goals":              f.get("goals", ""),
                "leader":             f.get("leader", ""),
                "status":            f.get("status", "unknown"),
                "first_story":       first_title,
                "first_date":        date_key,
                "appearances":       len(today_appearances) or 1,
                "story_appearances": today_appearances,
            }
    codex["factions"] = list(existing_factions.values())

    # ── Merge lore ──────────────────────────────────────────────────
    existing_lore = {x["name"].lower(): x for x in codex.get("lore", [])}
    for lo in lore.get("lore", []):
        name = lo.get("name", "Unknown")
        name_low = name.lower()
        today_appearances = stories_for(name)
        if name_low in existing_lore:
            ex = existing_lore[name_low]
            ex["source"]        = lo.get("source",        ex.get("source",        ""))
            ex["status"]        = lo.get("status",        ex.get("status",        "unknown"))
            prior = ex.get("story_appearances", [])
            new_ones = [app for app in today_appearances
                        if not any(p["date"] == app["date"] and p["title"] == app["title"] for p in prior)]
            if new_ones:
                ex["appearances"] = ex.get("appearances", 1) + len(new_ones)
                ex["story_appearances"] = prior + new_ones
        else:
            first_title = today_appearances[0]["title"] if today_appearances else ""
            existing_lore[name_low] = {
                "name":              name,
                "tagline":           lo.get("tagline", ""),
                "category":           lo.get("category", ""),
                "source":             lo.get("source", ""),
                "status":            lo.get("status", "unknown"),
                "first_story":       first_title,
                "first_date":        date_key,
                "appearances":       len(today_appearances) or 1,
                "story_appearances": today_appearances,
            }
    codex["lore"] = list(existing_lore.values())

    # ── Merge flora_fauna ───────────────────────────────────────────
    existing_flora_fauna = {x["name"].lower(): x for x in codex.get("flora_fauna", [])}
    for ff in lore.get("flora_fauna", []):
        name = ff.get("name", "Unknown")
        name_low = name.lower()
        today_appearances = stories_for(name)
        if name_low in existing_flora_fauna:
            ex = existing_flora_fauna[name_low]
            ex["habitat"]       = ff.get("habitat",       ex.get("habitat",       ""))
            ex["rarity"]        = ff.get("rarity",        ex.get("rarity",        ""))
            ex["status"]        = ff.get("status",        ex.get("status",        "unknown"))
            prior = ex.get("story_appearances", [])
            new_ones = [app for app in today_appearances
                        if not any(p["date"] == app["date"] and p["title"] == app["title"] for p in prior)]
            if new_ones:
                ex["appearances"] = ex.get("appearances", 1) + len(new_ones)
                ex["story_appearances"] = prior + new_ones
        else:
            first_title = today_appearances[0]["title"] if today_appearances else ""
            existing_flora_fauna[name_low] = {
                "name":              name,
                "tagline":           ff.get("tagline", ""),
                "type":               ff.get("type", ""),
                "rarity":             ff.get("rarity", ""),
                "habitat":            ff.get("habitat", ""),
                "status":            ff.get("status", "unknown"),
                "first_story":       first_title,
                "first_date":        date_key,
                "appearances":       len(today_appearances) or 1,
                "story_appearances": today_appearances,
            }
    codex["flora_fauna"] = list(existing_flora_fauna.values())

    # ── Merge magic ─────────────────────────────────────────────────
    existing_magic = {x["name"].lower(): x for x in codex.get("magic", [])}
    for mg in lore.get("magic", []):
        name = mg.get("name", "Unknown")
        name_low = name.lower()
        today_appearances = stories_for(name)
        if name_low in existing_magic:
            ex = existing_magic[name_low]
            ex["element"]       = mg.get("element",       ex.get("element",       ""))
            ex["difficulty"]    = mg.get("difficulty",    ex.get("difficulty",    ""))
            ex["status"]        = mg.get("status",        ex.get("status",        "unknown"))
            prior = ex.get("story_appearances", [])
            new_ones = [app for app in today_appearances
                        if not any(p["date"] == app["date"] and p["title"] == app["title"] for p in prior)]
            if new_ones:
                ex["appearances"] = ex.get("appearances", 1) + len(new_ones)
                ex["story_appearances"] = prior + new_ones
        else:
            first_title = today_appearances[0]["title"] if today_appearances else ""
            existing_magic[name_low] = {
                "name":              name,
                "tagline":           mg.get("tagline", ""),
                "type":               mg.get("type", ""),
                "element":            mg.get("element", ""),
                "difficulty":         mg.get("difficulty", ""),
                "status":            mg.get("status", "unknown"),
                "first_story":       first_title,
                "first_date":        date_key,
                "appearances":       len(today_appearances) or 1,
                "story_appearances": today_appearances,
            }
    codex["magic"] = list(existing_magic.values())

    # ── Merge relics ────────────────────────────────────────────────
    existing_relics = {x["name"].lower(): x for x in codex.get("relics", [])}

    def _base_name_for_crosscat(n: str) -> str:
        return _norm_entity_key(_strip_trailing_parenthetical(str(n or "")))

    character_name_bases = {
        _base_name_for_crosscat(c.get("name", ""))
        for c in (codex.get("characters") or [])
        if isinstance(c, dict) and (c.get("name") or "").strip()
    }

    for rl in lore.get("relics", []):
        name = _strip_trailing_parenthetical(rl.get("name", "Unknown"))
        if _base_name_for_crosscat(name) in character_name_bases:
            # Prevent cross-category drift where a person gets extracted into relics.
            # The character entry should carry the story appearance.
            continue
        name_low = name.lower()
        today_appearances = stories_for(name)
        if name_low in existing_relics:
            ex = existing_relics[name_low]
            ex["power"]         = rl.get("power",         ex.get("power",         ""))
            ex["curse"]         = rl.get("curse",         ex.get("curse",         ""))
            ex["status"]        = rl.get("status",        ex.get("status",        "unknown"))
            prior = ex.get("story_appearances", [])
            new_ones = [app for app in today_appearances
                        if not any(p["date"] == app["date"] and p["title"] == app["title"] for p in prior)]
            if new_ones:
                ex["appearances"] = ex.get("appearances", 1) + len(new_ones)
                ex["story_appearances"] = prior + new_ones
        else:
            first_title = today_appearances[0]["title"] if today_appearances else ""
            existing_relics[name_low] = {
                "name":              name,
                "tagline":           rl.get("tagline", ""),
                "origin":             rl.get("origin", ""),
                "power":              rl.get("power", ""),
                "curse":              rl.get("curse", ""),
                "status":            rl.get("status", "unknown"),
                "first_story":       first_title,
                "first_date":        date_key,
                "appearances":       len(today_appearances) or 1,
                "story_appearances": today_appearances,
            }
    codex["relics"] = list(existing_relics.values())

    # ── Merge regions ───────────────────────────────────────────────
    existing_regions = {x["name"].lower(): x for x in codex.get("regions", [])}
    for rg in lore.get("regions", []):
        name = rg.get("name", "Unknown")
        name_low = name.lower()
        today_appearances = stories_for(name)
        if name_low in existing_regions:
            ex = existing_regions[name_low]
            ex["continent"]     = rg.get("continent",     ex.get("continent",     "unknown"))
            ex["realm"]         = rg.get("realm",         ex.get("realm",         "unknown"))
            ex["ruler"]         = rg.get("ruler",         ex.get("ruler",         ""))
            ex["climate"]       = rg.get("climate",       ex.get("climate",       ""))
            ex["terrain"]       = rg.get("terrain",       ex.get("terrain",       ""))
            ex["function"]      = rg.get("function",      ex.get("function",      ""))
            ex["status"]        = rg.get("status",        ex.get("status",        "unknown"))
            ex["notes"]         = rg.get("notes",         ex.get("notes",         ""))
            prior = ex.get("story_appearances", [])
            new_ones = [app for app in today_appearances
                        if not any(p["date"] == app["date"] and p["title"] == app["title"] for p in prior)]
            if new_ones:
                ex["appearances"] = ex.get("appearances", 1) + len(new_ones)
                ex["story_appearances"] = prior + new_ones
        else:
            first_title = today_appearances[0]["title"] if today_appearances else ""
            existing_regions[name_low] = {
                "name":              name,
                "tagline":           rg.get("tagline", ""),
                "continent":         rg.get("continent", "unknown"),
                "realm":             rg.get("realm", "unknown"),
                "climate":            rg.get("climate", ""),
                "terrain":            rg.get("terrain", ""),
                "ruler":              rg.get("ruler", ""),
                "function":           rg.get("function", ""),
                "status":            rg.get("status", "unknown"),
                "notes":             rg.get("notes", ""),
                "first_story":       first_title,
                "first_date":        date_key,
                "appearances":       len(today_appearances) or 1,
                "story_appearances": today_appearances,
            }
    codex["regions"] = list(existing_regions.values())

    # ── Merge substances ────────────────────────────────────────────
    existing_substances = {x["name"].lower(): x for x in codex.get("substances", [])}
    for sub in lore.get("substances", []):
        name = sub.get("name", "Unknown")
        name_low = name.lower()
        today_appearances = stories_for(name)
        if name_low in existing_substances:
            ex = existing_substances[name_low]
            ex["properties"]    = sub.get("properties",    ex.get("properties",    ""))
            ex["use"]           = sub.get("use",           ex.get("use",           ""))
            ex["status"]        = sub.get("status",        ex.get("status",        "unknown"))
            prior = ex.get("story_appearances", [])
            new_ones = [app for app in today_appearances
                        if not any(p["date"] == app["date"] and p["title"] == app["title"] for p in prior)]
            if new_ones:
                ex["appearances"] = ex.get("appearances", 1) + len(new_ones)
                ex["story_appearances"] = prior + new_ones
        else:
            first_title = today_appearances[0]["title"] if today_appearances else ""
            existing_substances[name_low] = {
                "name":              name,
                "tagline":           sub.get("tagline", ""),
                "type":               sub.get("type", ""),
                "rarity":             sub.get("rarity", ""),
                "properties":         sub.get("properties", ""),
                "use":                sub.get("use", ""),
                "status":            sub.get("status", "unknown"),
                "first_story":       first_title,
                "first_date":        date_key,
                "appearances":       len(today_appearances) or 1,
                "story_appearances": today_appearances,
            }
    codex["substances"] = list(existing_substances.values())

    # ── Backstop: ensure character home_* geo anchors exist ─────────────
    # Motivation: The extractor often captures a character's home_place/home_region/home_realm,
    # but fails to emit the corresponding place/region/realm entry. Since story badges and
    # codex browsing rely on the entity lists (not embedded strings inside character bios),
    # we create minimal placeholders here.
    def _uniq_story_apps(apps):
        if not isinstance(apps, list):
            return []
        out = []
        seen = set()
        for a in apps:
            if not isinstance(a, dict):
                continue
            d = str(a.get("date", "") or "").strip() or str(date_key)
            t = str(a.get("title", "") or "").strip()
            if not t:
                continue
            key = (d, t)
            if key in seen:
                continue
            seen.add(key)
            out.append({"date": d, "title": t})
        return out

    def _ensure_home_geo_from_characters(codex: dict):
        if not isinstance(codex, dict):
            return

        realms = codex.get("realms")
        if not isinstance(realms, list):
            realms = []
            codex["realms"] = realms
        regions = codex.get("regions")
        if not isinstance(regions, list):
            regions = []
            codex["regions"] = regions
        places = codex.get("places")
        if not isinstance(places, list):
            places = []
            codex["places"] = places

        realm_by_name = {
            str(r.get("name") or "").strip().lower(): r
            for r in realms
            if isinstance(r, dict) and str(r.get("name") or "").strip()
        }
        region_by_name = {
            str(r.get("name") or "").strip().lower(): r
            for r in regions
            if isinstance(r, dict) and str(r.get("name") or "").strip()
        }
        place_by_name = {
            str(p.get("name") or "").strip().lower(): p
            for p in places
            if isinstance(p, dict) and str(p.get("name") or "").strip()
        }

        for c in codex.get("characters", []) or []:
            if not isinstance(c, dict):
                continue
            story_apps = _uniq_story_apps(c.get("story_appearances"))
            first_story = str(c.get("first_story") or "").strip()
            first_date = str(c.get("first_date") or "").strip() or str(date_key)
            if first_story and not story_apps:
                story_apps = [{"date": first_date, "title": first_story}]

            home_realm = str(c.get("home_realm") or "").strip()
            home_region = str(c.get("home_region") or "").strip()
            home_place = str(c.get("home_place") or "").strip()

            if _truthy_non_unknown(home_realm) and home_realm.lower() not in realm_by_name:
                realm_by_name[home_realm.lower()] = {
                    "name": home_realm,
                    "tagline": "",
                    "continent": "unknown",
                    "capital": "unknown",
                    "function": "Auto-added from character home_realm.",
                    "taxation": "unknown",
                    "military": "unknown",
                    "status": "unknown",
                    "notes": "",
                    "first_story": story_apps[0]["title"] if story_apps else "",
                    "first_date": story_apps[0]["date"] if story_apps else first_date,
                    "story_appearances": story_apps,
                    "appearances": max(1, len(story_apps) or 1),
                }

            if _truthy_non_unknown(home_region) and home_region.lower() not in region_by_name:
                region_by_name[home_region.lower()] = {
                    "name": home_region,
                    "tagline": "",
                    "continent": "unknown",
                    "realm": home_realm if _truthy_non_unknown(home_realm) else "unknown",
                    "climate": "",
                    "terrain": "",
                    "ruler": "",
                    "function": "Auto-added from character home_region.",
                    "status": "unknown",
                    "notes": "",
                    "first_story": story_apps[0]["title"] if story_apps else "",
                    "first_date": story_apps[0]["date"] if story_apps else first_date,
                    "story_appearances": story_apps,
                    "appearances": max(1, len(story_apps) or 1),
                }

            if _truthy_non_unknown(home_place) and home_place.lower() not in place_by_name:
                place_by_name[home_place.lower()] = {
                    "name": home_place,
                    "tagline": "",
                    "place_type": "unknown",
                    "world": "The Known World",
                    "parent_place": "",
                    "hemisphere": "unknown",
                    "continent": "unknown",
                    "realm": home_realm if _truthy_non_unknown(home_realm) else "unknown",
                    "province": "unknown",
                    "region": home_region if _truthy_non_unknown(home_region) else "unknown",
                    "district": "unknown",
                    "atmosphere": "",
                    "description": "Auto-added from character home_place.",
                    "status": "unknown",
                    "notes": "",
                    "first_story": story_apps[0]["title"] if story_apps else "",
                    "first_date": story_apps[0]["date"] if story_apps else first_date,
                    "story_appearances": story_apps,
                    "appearances": max(1, len(story_apps) or 1),
                }

        codex["realms"] = list(realm_by_name.values())
        codex["regions"] = list(region_by_name.values())
        codex["places"] = list(place_by_name.values())

    _ensure_home_geo_from_characters(codex)

    # ── Backstop: place-like surface phrases in today's stories ─────────
    # Motivation: Even with a "completeness" prompt, the extractor can miss
    # institutional places like "Vault Archives". We add a minimal placeholder
    # when we see a strong surface-form phrase.
    def _ensure_place_like_phrases(codex: dict, stories: list[dict]):
        if not isinstance(codex, dict):
            return
        if not isinstance(stories, list) or not stories:
            return

        places = codex.get("places")
        if not isinstance(places, list):
            places = []
            codex["places"] = places

        place_by_name = {
            str(p.get("name") or "").strip().lower(): p
            for p in places
            if isinstance(p, dict) and str(p.get("name") or "").strip()
        }

        # Strong signals only — avoid adding generic titlecase phrases.
        KEYWORDS = {
            "archive": "library / archive",
            "archives": "library / archive",
            "vault": "vault",
            "vaults": "vault",
            "market": "market",
            "markets": "market",
            "temple": "temple",
            "shrine": "shrine",
            "tower": "tower",
            "keep": "fortress",
            "citadel": "fortress",
            "castle": "fortress",
        }
        KEYWORD_PRIORITY = [
            "archives",
            "archive",
            "market",
            "markets",
            "vault",
            "vaults",
            "temple",
            "shrine",
            "citadel",
            "castle",
            "keep",
            "tower",
        ]
        STOP_PREFIXES = {"the ", "a ", "an "}
        STOP_PHRASES = {
            "the known world",
            "the known lands",
        }

        phrase_re = re.compile(r"\b[A-Z][A-Za-z0-9’'\-]+(?:\s+[A-Z][A-Za-z0-9’'\-]+){1,3}\b")

        added = 0
        for s in stories:
            if not isinstance(s, dict):
                continue
            title = str(s.get("title") or "").strip()
            text = str(s.get("text") or "")
            if not title and not text:
                continue

            blob = (title + "\n" + text)
            for m in phrase_re.finditer(blob):
                phrase = str(m.group(0) or "").strip()
                if not phrase:
                    continue
                pl = phrase.strip().lower()
                if pl in STOP_PHRASES:
                    continue
                if any(pl.startswith(pfx) for pfx in STOP_PREFIXES):
                    # We already index "the X" as an alias in the UI; keep canonical name clean.
                    continue
                if pl in place_by_name:
                    continue

                words = [w.strip("\"“”‘’'()[]{}.,!?;:") for w in phrase.split() if w.strip()]
                wl = [w.lower() for w in words]
                hint = None
                for w in KEYWORD_PRIORITY:
                    if w in wl:
                        hint = KEYWORDS[w]
                        break
                if not hint:
                    continue

                # Minimal placeholder with appearance linking for this story.
                place_by_name[pl] = {
                    "name": phrase,
                    "tagline": "",
                    "place_type": hint,
                    "world": "The Known World",
                    "parent_place": "",
                    "hemisphere": "unknown",
                    "continent": "unknown",
                    "realm": "unknown",
                    "province": "unknown",
                    "region": "unknown",
                    "district": "unknown",
                    "atmosphere": "",
                    "description": "Auto-added from story surface phrase.",
                    "status": "unknown",
                    "notes": "",
                    "first_story": title,
                    "first_date": date_key,
                    "story_appearances": [{"date": date_key, "title": title}] if title else [],
                    "appearances": 1,
                }
                added += 1
                if added >= 25:
                    break
            if added >= 25:
                break

        codex["places"] = list(place_by_name.values())

    _ensure_place_like_phrases(codex, stories)

    # ── Normalize story appearances (fallback to first_story/first_date) ──
    def ensure_story_appearances(items):
        for item in items:
            first_story = item.get("first_story")
            first_date  = item.get("first_date") or date_key
            story_apps  = item.get("story_appearances")

            # Normalize story_appearances to a unique list.
            if not isinstance(story_apps, list):
                story_apps = []
            uniq = []
            seen = set()
            for a in story_apps:
                if not isinstance(a, dict):
                    continue
                d = str(a.get("date", "") or "").strip() or first_date
                t = str(a.get("title", "") or "").strip()
                if not t:
                    continue
                key = (d, t)
                if key in seen:
                    continue
                seen.add(key)
                uniq.append({"date": d, "title": t})
            story_apps = uniq
            item["story_appearances"] = story_apps

            if first_story and (not isinstance(story_apps, list) or len(story_apps) == 0):
                item["story_appearances"] = [{"date": first_date, "title": first_story}]

            # If we have story appearances but first_story/first_date are missing, backfill them.
            if isinstance(item.get("story_appearances"), list) and item["story_appearances"]:
                if not (item.get("first_story") or "").strip():
                    item["first_story"] = item["story_appearances"][0].get("title", "")
                if not (item.get("first_date") or "").strip():
                    item["first_date"] = item["story_appearances"][0].get("date", date_key)

            if isinstance(item.get("story_appearances"), list):
                # Appearances tracks how many distinct stories an entity appears in.
                item["appearances"] = len(item["story_appearances"]) or 1

    for cat in [
        "characters",
        "places",
        "events",
        "rituals",
        "weapons",
        "deities_and_entities",
        "artifacts",
        "factions",
        "lore",
        "flora_fauna",
        "magic",
        "relics",
        "regions",
        "substances",
    ]:
        ensure_story_appearances(codex.get(cat, []))

    # Ensure geo entities carry a complete parent chain so the UI can render
    # meaningful hierarchy even when some levels are unknown.
    ensure_place_parent_chain(codex)
    enforce_continent_limit(codex)

    # Backfill event scope/epicenters/affected lists for better arc tracking.
    ev_updates = backfill_event_geo_fields(codex)
    if ev_updates:
        print(f"\u2713 Event geo backfill: filled {ev_updates} field(s)")

    # Sync story_appearances & names across categories for the same entity.
    synced = sync_cross_category_appearances(codex)
    if synced:
        print(f"\u2713 Cross-category sync: updated {synced} entries")

    # Attach lightweight liveness metadata so codex entries can evolve over issues
    # without changing existing tracking fields.
    def _attach_codex_liveness_meta(codex_obj: dict):
        known_dates = _load_known_issue_dates()
        issue_number = len(known_dates) if date_key in known_dates else (len(known_dates) + 1)

        def _importance_from_appearances(n: int) -> int:
            n = max(0, int(n or 0))
            if n >= 20:
                return 5
            if n >= 10:
                return 4
            if n >= 5:
                return 3
            if n >= 2:
                return 2
            return 1

        def _story_touched_today(item: dict) -> bool:
            apps = item.get("story_appearances")
            if not isinstance(apps, list):
                return False
            for a in apps:
                if isinstance(a, dict) and str(a.get("date") or "").strip() == date_key:
                    return True
            return False

        def _default_status(cat: str, item: dict) -> str:
            if cat == "events":
                stage = str(item.get("stage") or "").strip().lower()
                if stage:
                    return "resolved" if stage in {"resolved", "aftermath"} else f"{stage}"
                outcome = str(item.get("outcome") or "").strip()
                return outcome[:120] if outcome else "active"
            status = str(item.get("status") or "").strip()
            if status:
                return status
            return "active"

        target_categories = [
            "characters",
            "places",
            "events",
            "weapons",
            "artifacts",
            "factions",
            "lore",
            "regions",
            "polities",
        ]

        for cat in target_categories:
            items = codex_obj.get(cat)
            if not isinstance(items, list):
                continue
            for it in items:
                if not isinstance(it, dict):
                    continue

                apps = it.get("story_appearances") if isinstance(it.get("story_appearances"), list) else []
                appearances = max(int(it.get("appearances") or 0), len(apps), 1)
                touched_today = _story_touched_today(it)

                it["importance"] = _importance_from_appearances(appearances)
                if touched_today or ("lastChangedIssue" not in it):
                    it["lastChangedIssue"] = issue_number
                if not str(it.get("currentStatus") or "").strip() or touched_today:
                    it["currentStatus"] = _default_status(cat, it)

                if cat == "events":
                    if "relatedFactionIds" not in it:
                        participants = it.get("participants") if isinstance(it.get("participants"), list) else []
                        it["relatedFactionIds"] = [str(x).strip() for x in participants[:8] if str(x).strip()]
                    if "relatedRegionIds" not in it:
                        affected = it.get("affected_regions") if isinstance(it.get("affected_regions"), list) else []
                        it["relatedRegionIds"] = [str(x).strip() for x in affected[:8] if str(x).strip()]
                    if "relatedArcIds" not in it:
                        nm = str(it.get("name") or "").strip()
                        it["relatedArcIds"] = [_make_snake_id(nm)] if nm else []

    _attach_codex_liveness_meta(codex)

    codex["last_updated"] = date_key
    with open(CODEX_FILE, "w", encoding="utf-8") as f:
        json.dump(codex, f, ensure_ascii=True, indent=2)
    print(
        f"\u2713 Saved {CODEX_FILE} ("
        f"{len(codex['characters'])} chars, "
        f"{len(codex['places'])} places, "
        f"{len(codex['events'])} events, "
        f"{len(codex.get('rituals', []))} rituals, "
        f"{len(codex['weapons'])} weapons, "
        f"{len(codex['artifacts'])} artifacts, "
        f"{len(codex.get('factions', []))} factions, "
        f"{len(codex.get('lore', []))} lore, "
        f"{len(codex.get('flora_fauna', []))} flora/fauna, "
        f"{len(codex.get('magic', []))} magic, "
        f"{len(codex.get('relics', []))} relics, "
        f"{len(codex.get('regions', []))} regions, "
        f"{len(codex.get('substances', []))} substances)"
    )

# ── Characters file update (legacy) ──────────────────────────────────────
def update_characters_file(lore, date_key, stories=None):
    """Merge today's lore characters into characters.json, preserving history."""
    stories = stories or []

    existing_chars = {}
    if os.path.exists(CHARACTERS_FILE):
        try:
            with open(CHARACTERS_FILE, "r", encoding="utf-8") as f:
                for ch in json.load(f).get("characters", []):
                    existing_chars[ch["name"].lower()] = ch
        except (json.JSONDecodeError, IOError):
            pass

    def stories_for(name):
        first = name.split()[0].lower()
        return [
            {"date": date_key, "title": s.get("title", "")}
            for s in stories
            if first in (s.get("text", "") + " " + s.get("title", "")).lower()
        ]

    for c in lore.get("characters", []):
        name = c.get("name", "Unknown")
        name_low = name.lower()
        world = next(
            (w["name"] for w in lore.get("worlds", []) if w["id"] == c.get("world")),
            c.get("world", "The Known World")
        )
        today_appearances = stories_for(name)
        if name_low in existing_chars:
            ex = existing_chars[name_low]
            ex["role"]   = c.get("role",   ex.get("role",   "Unknown"))
            ex["status"] = c.get("status", ex.get("status", "Unknown"))
            ex["world"]  = world
            ex["bio"]    = c.get("bio",    ex.get("bio",    ""))
            ex["traits"] = c.get("traits", ex.get("traits", []))
            if c.get("tagline") and not ex.get("tagline"):
                ex["tagline"] = c["tagline"]
            prior = ex.get("story_appearances", [])
            new_ones = [a for a in today_appearances
                        if not any(p["date"] == a["date"] and p["title"] == a["title"] for p in prior)]
            if new_ones:
                ex["appearances"] = ex.get("appearances", 1) + len(new_ones)
                ex["story_appearances"] = prior + new_ones
        else:
            first_title = today_appearances[0]["title"] if today_appearances else ""
            existing_chars[name_low] = {
                "name":              name,
                "tagline":           c.get("tagline", ""),
                "role":              c.get("role", "Unknown"),
                "status":            c.get("status", "Unknown"),
                "world":             world,
                "bio":               c.get("bio", ""),
                "traits":            c.get("traits", []),
                "first_story":       first_title,
                "first_date":        date_key,
                "appearances":       len(today_appearances) or 1,
                "story_appearances": today_appearances,
            }

    output = {"last_updated": date_key, "characters": list(existing_chars.values())}
    with open(CHARACTERS_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=True, indent=2)
    print(f"\u2713 Saved {CHARACTERS_FILE} ({len(output['characters'])} characters total)")

def parse_json_response(raw):
    """Strip markdown fences and extract JSON from a Claude response."""
    raw = raw.strip()
    if "```" in raw:
        m = re.search(r"```(?:json)?\s*([\s\S]*?)```", raw)
        if m:
            raw = m.group(1).strip()

    # Prefer a real JSON parse that tolerates extra trailing text.
    # Claude sometimes returns: { ... }\n\n(brief explanation)
    decoder = json.JSONDecoder()
    candidate_starts = sorted({i for i in (raw.find("{"), raw.find("[")) if i != -1})

    # If the first occurrences aren't usable, fall back to scanning for any '{'/'['.
    if not candidate_starts:
        raise ValueError("No JSON structure found in response")

    # Add additional candidate starts by scanning the whole string.
    # This is still cheap for our response sizes and makes parsing robust.
    for idx, ch in enumerate(raw):
        if ch in "{[":
            candidate_starts.append(idx)
    candidate_starts = sorted(set(candidate_starts))

    last_error = None
    for start in candidate_starts:
        try:
            obj, _end = decoder.raw_decode(raw[start:])
            return obj
        except Exception as e:
            last_error = e

    raise ValueError(f"No JSON structure found in response (last error: {last_error})")


def _looks_like_story_dict(obj: object) -> bool:
    if not isinstance(obj, dict):
        return False
    has_title = any(k in obj for k in ("title", "name", "heading"))
    has_text = any(k in obj for k in ("text", "story", "body", "content"))
    return has_title and has_text


def extract_story_items(obj: object, _depth: int = 0):
    """Return a list of story dicts from common Claude response shapes.

    Claude usually returns a JSON array of {title,text,subgenre} objects, but it
    sometimes wraps that list in additional dict layers (e.g. {"data":{...}}) or
    returns a dict keyed by numeric/story_* keys.
    """
    if _depth > 6:
        return None

    if isinstance(obj, str):
        s = obj.strip()
        if not s:
            return None
        try:
            parsed = json.loads(s)
        except Exception:
            return None
        return extract_story_items(parsed, _depth=_depth + 1)

    if isinstance(obj, list):
        if any(_looks_like_story_dict(x) for x in obj):
            return obj
        # Some models may return a list with a single wrapper object.
        for x in obj:
            found = extract_story_items(x, _depth=_depth + 1)
            if isinstance(found, list) and found:
                return found
        return None

    if isinstance(obj, dict):
        # Prefer direct keys.
        for key in ("stories", "tales", "items", "entries", "results"):
            if key in obj:
                found = extract_story_items(obj.get(key), _depth=_depth + 1)
                if isinstance(found, list) and found:
                    return found

        # Common wrappers.
        for key in ("data", "output", "payload", "response", "result"):
            if key in obj:
                found = extract_story_items(obj.get(key), _depth=_depth + 1)
                if isinstance(found, list) and found:
                    return found

        # A dict keyed by numeric/story_* keys.
        values = list(obj.values())
        if values and all(isinstance(v, dict) for v in values):
            def _k(k):
                m = re.search(r"(\d+)", str(k))
                return int(m.group(1)) if m else 10**9

            ordered = [obj[k] for k in sorted(obj.keys(), key=_k)]
            if any(_looks_like_story_dict(v) for v in ordered):
                return ordered

        # A single story object.
        if _looks_like_story_dict(obj):
            return [obj]

    return None


def coerce_story_dict(obj: object) -> dict:
    """Normalize a single story dict to {title,text,subgenre}."""
    if not isinstance(obj, dict):
        return {"title": "Untitled", "text": "", "subgenre": "Sword & Sorcery"}

    title = obj.get("title") or obj.get("name") or obj.get("heading") or "Untitled"
    text = obj.get("text")
    if text is None:
        text = obj.get("story") or obj.get("body") or obj.get("content") or ""

    subgenre = obj.get("subgenre") or obj.get("genre") or obj.get("sub_genre") or "Sword & Sorcery"
    return {"title": title, "text": text, "subgenre": subgenre}


def _story_title_key(title: object) -> str:
    return re.sub(r"\s+", " ", str(title or "").strip()).lower()


def build_story_json_reformat_prompt(raw_response_text: str, num_stories: int = NUM_STORIES) -> str:
    return f"""You previously generated sword-and-sorcery stories, but the JSON shape may be wrapped or inconsistent.

Task:
- Output ONLY valid JSON.
- The JSON must be a single array of exactly {int(num_stories)} objects.
- Each object must have: title (string), text (string), subgenre (string).
- If the previous response contains fewer than {int(num_stories)} stories, invent additional stories to reach {int(num_stories)}.

Here is the prior response (verbatim):
---
{raw_response_text}
---
"""


def build_missing_stories_prompt(today_str: str, world_date_label: str, lore: dict, missing: int, existing_titles=None, event_arc_dossiers=None) -> str:
    existing_titles = existing_titles or []
    lore_context = build_generation_lore_context(lore, seed_text=today_str)
    world_events_section = build_world_event_arcs_section(today_str, lore, event_arc_dossiers=event_arc_dossiers)
    avoid = "\n".join(f"- {t}" for t in existing_titles if t)
    avoid_section = f"\nDo not reuse these existing titles:\n{avoid}\n" if avoid else ""
    return f"""Generate {int(missing)} additional sword-and-sorcery stories for the issue dated {today_str}.

The in-world Edhran date is {world_date_label}. Use only that calendar for any temporal references inside the stories; never use Gregorian month or weekday names.

Format rules:
- Output ONLY valid JSON.
- The JSON must be a single array of exactly {int(missing)} objects.
- Each object must have: title (string), text (string), subgenre (string).
{avoid_section}
Canon guidance:
{world_events_section}

Existing lore context (use as inspiration; do not contradict):
{lore_context}
"""


def extend_with_missing_story_batches(
    client,
    today_str: str,
    world_date_label: str,
    lore: dict,
    stories: list[dict],
    event_arc_dossiers=None,
) -> list[dict]:
    """Fill missing stories with smaller follow-up batches.

    The one-shot recovery path can still underfill when the model is already
    struggling with a large prompt. Smaller batches are more reliable and let us
    accumulate progress instead of aborting after one short response.
    """
    stories = list(stories or [])
    batch_size = max(1, int(os.environ.get("MISSING_STORY_BATCH_SIZE", "3") or 3))
    max_passes = max(1, int(os.environ.get("MISSING_STORY_MAX_PASSES", "5") or 5))
    passes = 0

    while len(stories) < NUM_STORIES and passes < max_passes:
        missing = NUM_STORIES - len(stories)
        request_n = min(missing, batch_size)
        existing_titles = [s.get("title") for s in stories if isinstance(s, dict)]
        seen_titles = {_story_title_key(title) for title in existing_titles if title}

        print(
            f"WARNING: Still missing {missing} story(ies); requesting {request_n} more.",
            file=sys.stderr,
        )

        try:
            extra_msg = client.messages.create(
                model=MODEL,
                max_tokens=max(1024, min(4096, 900 * request_n)),
                messages=[{
                    "role": "user",
                    "content": build_missing_stories_prompt(
                        today_str,
                        world_date_label,
                        lore,
                        request_n,
                        existing_titles=existing_titles,
                        event_arc_dossiers=event_arc_dossiers,
                    ),
                }],
            )
            extra_raw = extra_msg.content[0].text.strip()
            extra_parsed = parse_json_response(extra_raw)
            extra_items = extract_story_items(extra_parsed) or []
        except Exception as e:
            print(f"WARNING: Extra-story batch failed: {e}", file=sys.stderr)
            passes += 1
            continue

        new_stories = []
        for item in extra_items:
            story = coerce_story_dict(item)
            title_key = _story_title_key(story.get("title"))
            if title_key and title_key in seen_titles:
                continue
            if title_key:
                seen_titles.add(title_key)
            new_stories.append(story)
            if len(new_stories) >= request_n:
                break

        if not new_stories:
            print("WARNING: Extra-story batch returned no usable stories.", file=sys.stderr)
            passes += 1
            continue

        stories.extend(new_stories)
        passes += 1

    return stories


def normalize_extracted_lore(extracted):
    """Normalize lore extraction output into the expected dict-of-arrays structure.

    Claude is prompted to return a dict, but in practice it can occasionally return
    a top-level JSON array. This normalizer coerces common shapes into a safe
    dictionary for downstream processing.
    """
    expected_keys = [
        "characters",
        "places",
        "events",
        "rituals",
        "weapons",
        "deities_and_entities",
        "artifacts",
        "factions",
        "lore",
        "flora_fauna",
        "magic",
        "relics",
        "regions",
        "realms",
        "continents",
        "hemispheres",
        "provinces",
        "districts",
        "substances",
    ]
    empty = {k: [] for k in expected_keys}

    def _canonicalize_object_of_name(name: str) -> str:
        """Canonicalize important object-of names.

        Examples:
        - "the idol of Khar-Zul" -> "Idol of Khar-Zul"
        - "idol of Khar-Zul"     -> "Idol of Khar-Zul"

        We only touch this narrow pattern to avoid rewriting names like
        "The Lamia's Pearl".
        """
        if not name:
            return ""
        s = str(name).strip()
        m = re.match(
            r"^(?:the\s+)?(idol|crown|throne|blade|dagger|ring|tome|amulet|chalice|mask|orb|eye|eyes)\s+of\s+(.+)$",
            s,
            flags=re.IGNORECASE,
        )
        if not m:
            return s
        obj = (m.group(1) or "").strip().lower()
        rest = (m.group(2) or "").strip()
        if not obj or not rest:
            return s
        return obj.capitalize() + " of " + rest

    def _looks_like_named_artifact(name: str) -> bool:
        return bool(
            re.match(
                r"^\s*(?:the\s+)?(idol|crown|throne|blade|dagger|ring|tome|amulet|chalice|mask|orb|eye|eyes)\s+of\s+",
                str(name or ""),
                flags=re.IGNORECASE,
            )
        )

    def _postprocess(normalized: dict) -> dict:
        # Canonicalize artifact names (narrow pattern only).
        artifacts_out = []
        for a in normalized.get("artifacts", []) or []:
            if not isinstance(a, dict):
                continue
            a2 = dict(a)
            a2["name"] = _canonicalize_object_of_name(a2.get("name", ""))
            artifacts_out.append(a2)

        # Move misclassified object-of items out of relics into artifacts.
        relics_in = normalized.get("relics", []) or []
        relics_out = []
        for rl in relics_in:
            if not isinstance(rl, dict):
                continue
            name = _canonicalize_object_of_name(rl.get("name", ""))
            if _looks_like_named_artifact(name):
                powers = str(rl.get("power", "") or "").strip()
                curse = str(rl.get("curse", "") or "").strip()
                if curse:
                    powers = (powers + ("\n\n" if powers else "") + f"Curse: {curse}").strip()
                artifacts_out.append({
                    "name": name,
                    "tagline": rl.get("tagline", ""),
                    "artifact_type": "idol" if name.lower().startswith("idol of ") else "",
                    "origin": rl.get("origin", ""),
                    "powers": powers,
                    "last_known_holder": "",
                    "status": rl.get("status", "unknown"),
                })
                continue
            rl2 = dict(rl)
            rl2["name"] = name
            relics_out.append(rl2)

        # Deduplicate artifacts by case-insensitive name.
        deduped_artifacts = {}
        for a in artifacts_out:
            if not isinstance(a, dict):
                continue
            nm = str(a.get("name", "")).strip()
            if not nm:
                continue
            key = nm.lower()
            if key not in deduped_artifacts:
                deduped_artifacts[key] = a
                continue
            ex = deduped_artifacts[key]
            # Prefer non-empty fields; append powers if both exist and differ.
            for fld in ["tagline", "artifact_type", "origin", "last_known_holder", "status"]:
                if not ex.get(fld) and a.get(fld):
                    ex[fld] = a.get(fld)
            p1 = str(ex.get("powers", "") or "").strip()
            p2 = str(a.get("powers", "") or "").strip()
            if p2 and p2 != p1:
                ex["powers"] = (p1 + ("\n\n" if p1 else "") + p2).strip()

        normalized["artifacts"] = list(deduped_artifacts.values())
        normalized["relics"] = relics_out
        return normalized

    if isinstance(extracted, dict):
        normalized = dict(extracted)
        for key in expected_keys:
            if key not in normalized:
                normalized[key] = []
            elif not isinstance(normalized[key], list):
                normalized[key] = []
        return _postprocess(normalized)

    if isinstance(extracted, list):
        buckets = {k: [] for k in expected_keys}
        for item in extracted:
            if not isinstance(item, dict):
                continue

            # Heuristic bucketing by distinctive keys.
            if "place_type" in item or "atmosphere" in item or "continent" in item or "realm" in item:
                buckets["places"].append(item)
                continue
            if "event_type" in item or "participants" in item or "outcome" in item:
                buckets["events"].append(item)
                continue
            if "weapon_type" in item or "last_known_holder" in item:
                buckets["weapons"].append(item)
                continue
            if "artifact_type" in item:
                buckets["artifacts"].append(item)
                continue
            if "alignment" in item or "leader" in item or "goals" in item:
                buckets["factions"].append(item)
                continue
            if "rarity" in item or "habitat" in item:
                buckets["flora_fauna"].append(item)
                continue
            if "element" in item or "difficulty" in item:
                buckets["magic"].append(item)
                continue
            if "curse" in item and "power" in item:
                buckets["relics"].append(item)
                continue
            if "category" in item and "source" in item:
                buckets["lore"].append(item)
                continue

            # Default fallback.
            buckets["characters"].append(item)

        print(
            "WARNING: Lore extraction returned a JSON array; coerced into the expected dict structure.",
            file=sys.stderr,
        )
        coerced = dict(empty)
        for key, value in buckets.items():
            if value:
                coerced[key] = value
        return coerced

    print(
        f"WARNING: Lore extraction returned unexpected type: {type(extracted).__name__}; ignoring.",
        file=sys.stderr,
    )
    return dict(empty)


def _merge_extracted_batches(batches: list[dict]) -> dict:
    """Merge multiple extraction batch dicts into a single combined dict.

    Each batch is a normalized dict-of-arrays (from normalize_extracted_lore).
    We simply concatenate the arrays for each category.
    """
    expected_keys = [
        "characters", "places", "events", "rituals", "weapons",
        "deities_and_entities", "artifacts", "factions", "lore",
        "flora_fauna", "magic", "relics", "regions", "realms",
        "continents", "hemispheres", "provinces",
        "districts", "substances", "polities",
    ]
    merged: dict = {k: [] for k in expected_keys}
    for batch in batches:
        if not isinstance(batch, dict):
            continue
        for k in expected_keys:
            arr = batch.get(k)
            if isinstance(arr, list):
                merged[k].extend(arr)
    return merged


def _extract_lore_batched(client, stories: list[dict], lore: dict, codex_balance=None) -> dict:
    """Extract lore from stories in batches to avoid output-token truncation.

    Splits the story list into batches of EXTRACTION_BATCH_SIZE, calls the
    extraction prompt for each batch, and merges all results.
    """
    batch_size = max(1, EXTRACTION_BATCH_SIZE)
    max_tokens = max(2048, EXTRACTION_MAX_TOKENS)

    # Split stories into batches
    batches_of_stories: list[list[dict]] = []
    for i in range(0, len(stories), batch_size):
        batches_of_stories.append(stories[i : i + batch_size])

    total_batches = len(batches_of_stories)
    if total_batches > 1:
        print(f"  Splitting {len(stories)} stories into {total_batches} extraction batches of ≤{batch_size}…")

    extracted_batches: list[dict] = []

    for batch_idx, batch_stories in enumerate(batches_of_stories, 1):
        batch_label = f"batch {batch_idx}/{total_batches}" if total_batches > 1 else "extraction"
        titles = [s.get("title", "?") for s in batch_stories]
        if total_batches > 1:
            print(f"  [{batch_label}] Extracting from: {', '.join(titles)}")

        try:
            msg = client.messages.create(
                model=MODEL,
                max_tokens=max_tokens,
                messages=[{
                    "role": "user",
                    "content": build_lore_extraction_prompt(batch_stories, lore, codex_balance=codex_balance),
                }],
            )

            raw_text = msg.content[0].text.strip()
            stop = msg.stop_reason

            # Detect truncation
            if stop == "max_tokens":
                print(
                    f"  ⚠ [{batch_label}] Response truncated (hit {max_tokens} token limit). "
                    f"Attempting to parse partial output…",
                    file=sys.stderr,
                )

            parsed = parse_json_response(raw_text)
            normalized = normalize_extracted_lore(parsed)
            extracted_batches.append(normalized)

            cat_counts = {
                k: len(v) for k, v in normalized.items()
                if isinstance(v, list) and v
            }
            total_entities = sum(cat_counts.values())
            if total_batches > 1:
                print(f"  [{batch_label}] Extracted {total_entities} entities across {len(cat_counts)} categories")
            if total_entities == 0:
                print(
                    f"  ⚠ [{batch_label}] Zero entities extracted — possible output issue",
                    file=sys.stderr,
                )

        except (ValueError, json.JSONDecodeError) as e:
            print(
                f"  ⚠ [{batch_label}] Could not parse extraction JSON: {e}",
                file=sys.stderr,
            )
            print(
                f"  Skipping this batch; other batches will still be merged.",
                file=sys.stderr,
            )

    if not extracted_batches:
        print("WARNING: All extraction batches failed; no new lore extracted.", file=sys.stderr)
        return {k: [] for k in [
            "characters", "places", "events", "rituals", "weapons",
            "deities_and_entities", "artifacts", "factions", "lore",
            "flora_fauna", "magic", "relics", "regions", "realms",
            "continents", "hemispheres", "provinces",
            "districts", "substances", "polities",
        ]}

    merged = _merge_extracted_batches(extracted_batches)
    total = sum(len(v) for v in merged.values() if isinstance(v, list))
    print(f"  ✓ Total extracted across all batches: {total} entities")
    return merged


def _signature_key_for_name(name: str) -> str:
    """Create a lightweight search key for detecting entity mentions."""
    if not name:
        return ""
    _SKIP = {"the", "a", "an", "of", "and", "to", "in", "on", "at"}
    words = [w.strip("()[]{}.,!?\"'“”‘’:-").lower() for w in name.split()]
    sig = [w for w in words if w and w not in _SKIP]
    if len(sig) >= 2:
        return sig[0] + " " + sig[1]
    if sig:
        return sig[0]
    return words[0] if words else ""


def _norm_text_for_matching(s: str) -> str:
    return (
        str(s or "")
        .replace("\u2019", "'")
        .replace("\u2018", "'")
        .replace("\u2011", "-")
        .lower()
    )


def _tokens_for_entity_name(name: str, max_tokens: int = 4) -> list[str]:
    """Extract significant word tokens from an entity name for mention checks."""
    _SKIP = {"the", "a", "an", "of", "and", "to", "in", "on", "at", "for", "from", "by", "with"}
    nm = _strip_trailing_parenthetical(str(name or "").strip())
    nm = _norm_text_for_matching(nm)
    toks = [
        t
        for t in re.findall(r"[a-z0-9]+(?:[-‑][a-z0-9]+)?", nm)
        if t and t not in _SKIP
    ]
    return toks[: max(1, int(max_tokens or 4))] if toks else ([nm] if nm else [])


def entity_name_mentioned_in_text(name: str, text: str) -> bool:
    """Return True if name appears in text with token/phrase boundaries.

    This is deliberately conservative: it avoids substring matches like "crow" in "crown",
    while still allowing small re-orderings like "ritual of dismissal".
    """
    blob = _norm_text_for_matching(text)
    nm = _strip_trailing_parenthetical(str(name or "").strip())
    if not nm or not blob:
        return False

    nm_norm = _norm_text_for_matching(nm)
    return bool(re.search(r"(?<![a-z0-9])" + re.escape(nm_norm) + r"(?![a-z0-9])", blob))


def filter_lore_to_stories(lore: dict, stories: list[dict]) -> dict:
    """Drop extracted entities that are not mentioned in the provided stories."""
    if not isinstance(lore, dict):
        return lore
    if not isinstance(stories, list) or not stories:
        return lore

    blob = "\n\n".join(
        str((s.get("title", "") or "").strip()) + "\n" + str((s.get("text", "") or "").strip())
        for s in stories
        if isinstance(s, dict)
    )

    def _keep_item(cat: str, it: dict) -> bool:
        nm = (it.get("name") or "").strip()
        if not nm:
            return False
        # Characters may have explicit aliases; allow any alias to satisfy grounding.
        if cat == "characters":
            if entity_name_mentioned_in_text(nm, blob):
                return True
            aliases = it.get("aliases")
            if isinstance(aliases, list):
                for a in aliases:
                    if entity_name_mentioned_in_text(str(a or "").strip(), blob):
                        return True
            return False
        return entity_name_mentioned_in_text(nm, blob)

    for cat, arr in list(lore.items()):
        if not isinstance(arr, list):
            continue
        kept = []
        for it in arr:
            if not isinstance(it, dict):
                continue
            if _keep_item(cat, it):
                kept.append(it)
        lore[cat] = kept
    return lore


def _descriptor_key(text: str) -> str:
    """Collapse descriptive character labels into a stable comparison key."""
    s = _norm_text_for_matching(str(text or "").strip())
    if not s:
        return ""
    s = re.sub(r"^(?:the|a|an)\s+", "", s)
    s = re.sub(r"[^a-z0-9\s'\-]", " ", s)
    tokens = [t for t in s.split() if t]
    if not tokens:
        return ""

    cut_words = {
        "who", "which", "that", "with", "without", "from", "under", "over",
        "near", "in", "at", "by", "into", "across", "through", "during",
        "after", "before", "behind", "beside", "named", "called", "tasked",
        "drawn", "born", "leading", "studying", "guarding", "working", "haunted",
        "obsessed", "tracking", "following", "bearing", "seeking", "carrying",
    }
    trimmed = []
    for tok in tokens:
        if tok in cut_words:
            break
        trimmed.append(tok)
        if len(trimmed) >= 5:
            break
    return " ".join(trimmed or tokens[:5]).strip()


def _looks_like_descriptor_name(name: str) -> bool:
    """Return True for extracted placeholder names like 'young scholar'."""
    raw = str(name or "").strip()
    if not raw:
        return False
    if raw == raw.lower():
        return True
    key = _descriptor_key(raw)
    return bool(key) and key == _norm_text_for_matching(raw)


def _extract_named_character_mentions(stories: list[dict]) -> list[dict]:
    """Find explicit name+descriptor introductions in story text."""
    if not isinstance(stories, list) or not stories:
        return []

    pattern_named = re.compile(
        r"\b(?:(?i:a|an|the))[ \t]+([A-Za-z][A-Za-z'\-]*(?:[ \t]+[A-Za-z][A-Za-z'\-]*){0,5})[ \t]+(?i:named)[ \t]+"
        r"([A-Z][\w’'\-]+(?:[ \t]+[A-Z][\w’'\-]+){0,3})\b"
    )
    pattern_appositive = re.compile(
        r"\b([A-Z][\w’'\-]+(?:[ \t]+[A-Z][\w’'\-]+){0,3}),[ \t]+(?:(?i:a|an|the))[ \t]+([^,.;:!?\n]{3,80})"
    )
    bad_appositive_names = {
        "After", "Before", "But", "Later", "Meanwhile", "Now", "Soon",
        "Still", "Then", "There", "When", "While",
    }

    found = []
    seen = set()
    for story in stories:
        if not isinstance(story, dict):
            continue
        story_title = str(story.get("title") or "").strip()
        chunks = []
        if story_title:
            chunks.append(story_title)
        chunks.extend(str(story.get("text") or "").splitlines())

        for chunk in chunks:
            if not chunk.strip():
                continue

            for match in pattern_named.finditer(chunk):
                descriptor = " ".join((match.group(1) or "").split()).strip()
                name = " ".join((match.group(2) or "").split()).strip()
                key = _descriptor_key(descriptor)
                if not name or not key:
                    continue
                seen_key = (name.casefold(), key)
                if seen_key in seen:
                    continue
                found.append({"name": name, "descriptor": descriptor, "descriptor_key": key, "story": story_title})
                seen.add(seen_key)

            for match in pattern_appositive.finditer(chunk):
                name = " ".join((match.group(1) or "").split()).strip()
                descriptor = " ".join((match.group(2) or "").split()).strip()
                key = _descriptor_key(descriptor)
                if not name or not key:
                    continue
                if name in bad_appositive_names:
                    continue
                if " named " in f" {descriptor.casefold()} ":
                    continue
                seen_key = (name.casefold(), key)
                if seen_key in seen:
                    continue
                found.append({"name": name, "descriptor": descriptor, "descriptor_key": key, "story": story_title})
                seen.add(seen_key)

    return found


def ensure_named_character_mentions_present(lore: dict, stories: list[dict]) -> dict:
    """Promote explicit named introductions when extraction kept only a descriptor."""
    if not isinstance(lore, dict):
        return lore

    mentions = _extract_named_character_mentions(stories)
    if not mentions:
        return lore

    chars = lore.setdefault("characters", [])
    if not isinstance(chars, list):
        lore["characters"] = []
        chars = lore["characters"]

    by_name = {}
    for char in chars:
        if not isinstance(char, dict):
            continue
        name = str(char.get("name") or "").strip()
        if not name:
            continue
        for key in _character_alias_keys(name):
            by_name.setdefault(key, char)
        aliases = char.get("aliases")
        if isinstance(aliases, list):
            for alias in aliases:
                alias_key = _norm_entity_key(alias)
                if alias_key:
                    by_name.setdefault(alias_key, char)

    removals = set()
    for mention in mentions:
        name = mention["name"]
        name_key = _norm_entity_key(name)
        descriptor = mention["descriptor"]
        descriptor_key = mention["descriptor_key"]
        if not name_key or not entity_name_mentioned_in_text(name, "\n\n".join(str((s.get("text") or "").strip()) + "\n" + str((s.get("title") or "").strip()) for s in stories if isinstance(s, dict))):
            continue

        target = by_name.get(name_key)
        placeholder = None
        for char in chars:
            if not isinstance(char, dict):
                continue
            existing_name = str(char.get("name") or "").strip()
            if not existing_name:
                continue
            existing_key = _descriptor_key(existing_name)
            if existing_key != descriptor_key:
                continue
            if not _looks_like_descriptor_name(existing_name):
                continue
            placeholder = char
            break

        if target is None and placeholder is not None:
            old_name = str(placeholder.get("name") or "").strip()
            aliases = placeholder.get("aliases")
            if not isinstance(aliases, list):
                aliases = []
            for alias in [old_name, descriptor]:
                alias = str(alias or "").strip()
                if _is_descriptor_placeholder_character_name(alias):
                    continue
                if alias and alias != name and alias not in aliases:
                    aliases.append(alias)
            placeholder["aliases"] = aliases
            placeholder["name"] = name
            if not placeholder.get("id"):
                placeholder["id"] = _make_snake_id(name)
            target = placeholder
            by_name[name_key] = target
        elif target is not None:
            aliases = target.get("aliases")
            if not isinstance(aliases, list):
                aliases = []
            for alias in [descriptor]:
                alias = str(alias or "").strip()
                if _is_descriptor_placeholder_character_name(alias):
                    continue
                if alias and alias != name and alias not in aliases:
                    aliases.append(alias)
            if aliases:
                target["aliases"] = aliases
            if placeholder is not None and placeholder is not target:
                removals.add(id(placeholder))

        if target is None:
            if _should_skip_character_auto_add(lore, name, descriptor):
                continue
            aliases = [] if _is_descriptor_placeholder_character_name(descriptor) else [descriptor]
            chars.append({
                "id": _make_snake_id(name),
                "name": name,
                "aliases": aliases,
                "tagline": "Named. Present. Emerging.",
                "role": descriptor.title(),
                "world": "The Known World",
                "status": "active",
                "home_place": "unknown",
                "home_region": "unknown",
                "home_realm": "unknown",
                "travel_scope": "unknown",
                "bio": f"A named {descriptor} explicitly identified in the story text.",
                "traits": [],
                "known_locations": [],
                "affiliations": [],
                "notes": "Auto-added because the story explicitly names this character.",
            })
            by_name[name_key] = chars[-1]

    if removals:
        lore["characters"] = [
            char for char in chars
            if not (isinstance(char, dict) and id(char) in removals)
        ]
    return lore


def ensure_named_leaders_present(lore: dict, stories: list[dict]) -> dict:
    """Ensure named leaders mentioned in story text exist as entities.

    Motivation: The extraction model sometimes captures a faction but forgets to
    emit the named leader as a character/entity. This adds a small deterministic
    backstop for obvious cases (e.g., "The Rot-King").
    """
    if not isinstance(lore, dict):
        return lore
    if not isinstance(stories, list) or not stories:
        return lore

    blob = "\n\n".join(
        str((s.get("title", "") or "").strip()) + "\n" + str((s.get("text", "") or "").strip())
        for s in stories
        if isinstance(s, dict)
    )
    if not blob.strip():
        return lore

    def _norm_name(x: str) -> str:
        return _norm_text_for_matching(_strip_trailing_parenthetical(str(x or "").strip()))

    existing = set()
    for cat in ("characters", "deities_and_entities", "factions"):
        arr = lore.get(cat)
        if not isinstance(arr, list):
            continue
        for it in arr:
            if isinstance(it, dict) and str(it.get("name") or "").strip():
                existing.add(_norm_name(it.get("name")))

    # Candidate names from extracted factions' leader fields.
    leaders: list[str] = []
    facs = lore.get("factions")
    if isinstance(facs, list):
        for f in facs:
            if not isinstance(f, dict):
                continue
            leader = str(f.get("leader") or "").strip()
            if not leader:
                continue
            if leader.strip().lower() in {"unknown", "none", "n/a"}:
                continue
            leaders.append(leader)

    if not leaders:
        return lore

    lore.setdefault("characters", [])
    if not isinstance(lore.get("characters"), list):
        lore["characters"] = []

    for leader in leaders:
        key = _norm_name(leader)
        if not key or key in existing:
            continue

        # Only add if actually mentioned in the story text.
        if not entity_name_mentioned_in_text(leader, blob):
            # Try without leading "The".
            if _norm_text_for_matching(leader).startswith("the "):
                alt = leader.strip()[4:].strip()
                if alt and entity_name_mentioned_in_text(alt, blob):
                    leader = "The " + alt
                else:
                    continue
            else:
                continue

        first_story = ""
        for s in stories:
            if not isinstance(s, dict):
                continue
            sb = str((s.get("title", "") or "").strip()) + "\n" + str((s.get("text", "") or "").strip())
            if entity_name_mentioned_in_text(leader, sb):
                first_story = str(s.get("title") or "").strip()
                break

        aliases = []
        if _norm_text_for_matching(leader).startswith("the "):
            base = leader.strip()[4:].strip()
            if base:
                aliases.append(base)

        lore["characters"].append({
            "id": _make_snake_id(leader),
            "name": leader,
            "aliases": aliases,
            "tagline": "Named. Threatening. Looming.",
            "role": "Leader / Ruler",
            "world": "The Known World",
            "status": "active",
            "home_place": "unknown",
            "home_region": "unknown",
            "home_realm": "unknown",
            "travel_scope": "unknown",
            "bio": "A named leader referenced in the issue's stories; details unknown beyond the text.",
            "traits": ["commanding", "dangerous", "enigmatic"],
            "known_locations": [],
            "affiliations": [],
            "notes": "Auto-added because a faction leader was named in-story.",
            **({"first_story": first_story} if first_story else {}),
        })

        existing.add(_norm_name(leader))

    return lore


def find_referenced_canon_entries(stories, lore):
    """Find existing lore entries referenced in story text/title.

    Returns: dict[category] -> list[entry]
    """
    haystack = "\n\n".join(
        (s.get("title", "") + "\n" + s.get("text", ""))
        for s in (stories or [])
    ).lower()
    if not haystack.strip():
        return {}

    categories = [
        "characters",
        "places",
        "events",
        "weapons",
        "deities_and_entities",
        "artifacts",
        "factions",
        "lore",
        "flora_fauna",
        "magic",
        "relics",
        "regions",
        "substances",
    ]
    referenced = {}

    for cat in categories:
        items = lore.get(cat, []) or []
        hits = []
        for item in items:
            name = (item.get("name") or "").strip()
            if not name:
                continue
            key = _signature_key_for_name(name)
            if not key or len(key) < 4:
                # avoid extreme false positives on tiny keys
                continue
            if key in haystack:
                hits.append(item)
        if hits:
            referenced[cat] = hits
    return referenced


def build_lore_revision_prompt(stories, lore, referenced_entries):
    """Prompt a minimal-edit revision pass that aligns any reused canon with the lore bible."""
    world_rules = []
    if lore.get("worlds") and lore["worlds"][0].get("rules"):
        world_rules = lore["worlds"][0]["rules"]

    canon_lines = []
    for cat, items in referenced_entries.items():
        label = cat.replace("_", " ").upper()
        canon_lines.append(f"=== CANON: {label} ===")
        for it in items:
            # Include the full entry as JSON for precision, but only for referenced entities.
            canon_lines.append(json.dumps(it, ensure_ascii=False, sort_keys=True))
        canon_lines.append("")

    stories_payload = json.dumps(stories, ensure_ascii=False, indent=2)
    rules_block = "\n".join([f"- {r}" for r in world_rules]) if world_rules else "- (none)"

    return f"""You are an editor for an ongoing sword-and-sorcery universe.

Goal: revise the stories ONLY as needed so they DO NOT contradict established canon.

Tone / variety guardrails:
- Preserve each story's existing tone (romantic, comedic, wondrous, political, grim, etc.).
- Do NOT darken the story or inject extra gore, cruelty, necromancy, or grim sorcery unless it is already present in that story.
- Do NOT add new magical systems, prophecies, demons, curses, or eldritch horrors as part of a canon fix.
- Keep the broader fantasy palette intact; do not homogenize all stories toward one vibe.

Canon rules:
{rules_block}

Canon entries referenced today (these must be treated as authoritative):
{os.linesep.join(canon_lines).strip()}

Editing constraints:
- Make the smallest possible changes to fix canon conflicts.
- Keep each story's title and subgenre unchanged.
- Do NOT introduce any additional reserved/canon names that are not already in the stories.
- If a detail would conflict, rewrite that detail to be ambiguous or consistent.

Return ONLY valid JSON in the exact same array structure as input (10 objects with title/subgenre/text).

STORIES JSON INPUT:
{stories_payload}
"""


def build_canon_checker_prompt(stories, lore, referenced_entries, mode="rewrite"):
    """Ask the model to flag contradictions vs canon and optionally rewrite with minimal edits."""
    world_rules = []
    if lore.get("worlds") and lore["worlds"][0].get("rules"):
        world_rules = lore["worlds"][0]["rules"]

    canon_lines = []
    for cat, items in referenced_entries.items():
        label = cat.replace("_", " ").upper()
        canon_lines.append(f"=== CANON: {label} ===")
        for it in items:
            canon_lines.append(json.dumps(it, ensure_ascii=False, sort_keys=True))
        canon_lines.append("")

    stories_payload = json.dumps(stories, ensure_ascii=False, indent=2)
    rules_block = "\n".join([f"- {r}" for r in world_rules]) if world_rules else "- (none)"
    mode = (mode or "rewrite").strip().lower()
    wants_rewrite = mode != "report"

    return f"""You are the canon checker for an ongoing sword-and-sorcery universe.

Task: identify contradictions between today's stories and the established canon entries referenced today.

Tone / variety guardrails:
- Preserve the tone and subgenre of each story.
- If rewriting, do NOT darken the content or add new grim sorcery/necromancy to resolve a contradiction.
- Prefer small factual/continuity tweaks (names, dates, places, affiliations, outcomes) over inventing new lore.

Canon rules:
{rules_block}

Authoritative canon entries (only these are guaranteed referenced today):
{os.linesep.join(canon_lines).strip()}

Output requirements:
- Respond with ONLY valid JSON.
- Always include an `issues` array. Each issue must include:
  - story_index (0-9)
  - title
  - severity (low|medium|high)
  - contradictions (array of short bullet-like strings)
  - suggested_fix (string)
""" + (
        """
- Also include `stories`: the revised stories array, with the SMALLEST possible edits to remove contradictions.
- Do NOT change titles or subgenre labels.
- Keep prose style and length roughly similar.
 - Keep romance/comedy/wonder beats intact when present.
""" if wants_rewrite else """
- Do NOT rewrite the stories.
""") + f"""

STORIES JSON INPUT:
{stories_payload}
"""

# ── Archive helpers ────────────────────────────────────────────────────────
def ensure_archive_dir():
    os.makedirs(ARCHIVE_DIR, exist_ok=True)

def load_archive_index():
    if os.path.exists(ARCHIVE_IDX):
        with open(ARCHIVE_IDX, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"dates": []}

def save_archive_index(idx):
    with open(ARCHIVE_IDX, "w", encoding="utf-8") as f:
        json.dump(idx, f, ensure_ascii=True, indent=2)


def _truthy_env(name: str) -> bool:
    return (os.environ.get(name) or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def _is_anthropic_usage_limit_error(exc: Exception) -> bool:
    text = str(exc or "").strip().lower()
    if not text:
        return False
    return (
        "usage limits" in text
        or "you have reached your specified api usage limits" in text
        or ("invalid_request_error" in text and "regain access on" in text)
    )


def _extract_anthropic_reset_hint(exc: Exception) -> str:
    text = str(exc or "")
    m = re.search(r"regain access on\s+([^.'}\n]+)", text, flags=re.IGNORECASE)
    return (m.group(1) or "").strip() if m else ""


def _issue_now() -> datetime:
    """Current datetime in the configured issue timezone.

    Falls back to UTC if the timezone name is invalid.
    """
    try:
        tz = ZoneInfo(ISSUE_TIMEZONE)
    except Exception:
        tz = timezone.utc
    return datetime.now(tz)


def _already_generated_for_date(date_key: str) -> bool:
    """Return True if the archive file exists and looks complete for date_key."""
    archive_file = os.path.join(ARCHIVE_DIR, f"{date_key}.json")
    if not os.path.exists(archive_file):
        return False
    try:
        with open(archive_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        if (data.get("date") or "").strip() != date_key:
            return False
        stories = data.get("stories")
        return isinstance(stories, list) and len(stories) >= int(NUM_STORIES)
    except Exception:
        return False

# ── Main ──────────────────────────────────────────────────────────────────
def main():
    _maybe_load_dotenv()
    issue_now = _issue_now()
    today_str = issue_now.strftime("%B %d, %Y")
    date_key = issue_now.strftime("%Y-%m-%d")
    world_clock = build_world_clock()
    world_date_label = world_clock.format_world_date(date_key) or date_key
    print(f"Generating stories for {date_key} (tz={ISSUE_TIMEZONE})...")

    if _already_generated_for_date(date_key) and not _truthy_env("FORCE_REGENERATE"):
        print(f"\u2713 Already generated for {date_key}; skipping (set FORCE_REGENERATE=1 to override).")
        return

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("ERROR: ANTHROPIC_API_KEY environment variable not set.", file=sys.stderr)
        sys.exit(1)

    # ── Load existing lore ────────────────────────────────────────────────
    lore = load_lore()
    geo = load_geography()
    lore = seed_geo_entities_from_geography(lore, geo)
    ensure_place_parent_chain(lore)
    enforce_continent_limit(lore)
    print(f"\u2713 Loaded lore ({len(lore.get('characters', []))} characters, "
          f"{len(lore.get('places', []))} places)")

    codex_for_balance = load_codex_file()
    codex_balance = summarize_codex_label_balance(codex_for_balance)
    weak_labels = codex_balance.get("underrepresented") if isinstance(codex_balance, dict) else []
    if isinstance(weak_labels, list) and weak_labels:
        watch = ", ".join(
            f"{str(x.get('label') or '')}:{int(x.get('count') or 0)}"
            for x in weak_labels[:6]
            if isinstance(x, dict)
        )
        if watch:
            print(f"\u2713 Codex balance watchlist: {watch}")
    else:
        print("\u2713 Codex balance: no underrepresented labels below threshold")

    client = anthropic.Anthropic(api_key=api_key)

    # ── Optional: Reuse planning (decide reuse + select candidates) ──────
    reuse_plan = {"reuse": False, "selections": {}, "rationale": ""}
    reused_entries = {}
    allowed_names_lower = set()
    reuse_details = {}
    if ENABLE_REUSE_PLANNER:
        candidates_by_cat = {}
        for cat in get_reuse_allowed_categories(lore):
            items = lore.get(cat, []) or []
            if not items:
                continue
            candidates_by_cat[cat] = _sample_candidates(
                items,
                k=REUSE_CANDIDATES_PER_CATEGORY,
                seed_text=date_key,
                salt=f"reuse_candidates:{cat}",
            )

        if candidates_by_cat:
            print("Planning reuse for today's stories...")
            plan_msg = client.messages.create(
                model=MODEL,
                max_tokens=1024,
                messages=[{"role": "user", "content": build_reuse_plan_prompt(today_str, world_date_label, lore, candidates_by_cat)}],
            )
            plan_raw = plan_msg.content[0].text.strip()
            try:
                plan = parse_json_response(plan_raw)
                reuse_plan = normalize_reuse_plan(plan, candidates_by_cat)
                if reuse_plan.get("reuse"):
                    reused_entries = get_full_canon_entries_for_selections(lore, reuse_plan.get("selections", {}))
                    allowed_names_lower = allowed_reuse_name_set(reused_entries)
                    print(f"\u2713 Reuse plan: {reuse_plan.get('selections', {})}")
                else:
                    print("\u2713 Reuse plan: no intentional reuse")
            except (ValueError, json.JSONDecodeError) as e:
                print(f"WARNING: Could not parse reuse plan JSON: {e}", file=sys.stderr)
                print("Continuing with no intentional reuse.", file=sys.stderr)

    # ── Optional: Build reuse dossiers from prior appearances ───────────
    # Goal: if we reuse (e.g.) Vraxen in story #N, the model has effectively
    # scanned all Vraxen tales and can stay consistent without prompt spam.
    if reused_entries:
        # Map intended intensity from the reuse plan (normalized selections).
        intended_intensity = {}
        try:
            for cat, picked in (reuse_plan.get("selections") or {}).items():
                if not isinstance(picked, list):
                    continue
                for entry in picked:
                    if isinstance(entry, dict):
                        nm = (entry.get("name") or "").strip()
                        intensity = (entry.get("intensity") or "").strip().lower()
                    elif isinstance(entry, str):
                        nm = entry.strip()
                        intensity = ""
                    else:
                        continue
                    if nm:
                        if intensity not in {"cameo", "central"}:
                            intensity = (REUSE_DEFAULT_INTENSITY or "cameo").strip().lower()
                        if intensity not in {"cameo", "central"}:
                            intensity = "cameo"
                        intended_intensity[(cat, nm.lower())] = intensity
        except Exception:
            intended_intensity = {}

        codex = load_codex_file() if ENABLE_REUSE_DOSSIER else {}
        codex_map = _codex_entry_map(codex) if ENABLE_REUSE_DOSSIER else {}

        for cat, items in reused_entries.items():
            if not items:
                continue
            for it in items:
                if not isinstance(it, dict):
                    continue
                nm = (it.get("name") or "").strip()
                if not nm:
                    continue

                intensity = intended_intensity.get((cat, nm.lower()), (REUSE_DEFAULT_INTENSITY or "cameo").strip().lower())
                if intensity not in {"cameo", "central"}:
                    intensity = "cameo"

                detail = {
                    "name": nm,
                    "intensity": intensity,
                }

                if ENABLE_REUSE_DOSSIER:
                    codex_entry = (codex_map.get(cat, {}) or {}).get(nm.lower())
                    if isinstance(codex_entry, dict):
                        apps = codex_entry.get("story_appearances")
                        detail["appearance_count"] = len(apps) if isinstance(apps, list) else 0
                        prior_tales = gather_prior_tales_for_entity(
                            codex_entry,
                            max_appearances=REUSE_DOSSIER_MAX_APPEARANCES,
                            max_chars_per_story=REUSE_DOSSIER_MAX_CHARS_PER_STORY,
                            max_total_chars=REUSE_DOSSIER_MAX_TOTAL_INPUT_CHARS,
                        )
                        if prior_tales:
                            print(f"Scanning {len(prior_tales)} prior tale(s) for reused {cat[:-1] if cat.endswith('s') else cat}: {nm}...")
                            dossier_prompt = build_reuse_dossier_prompt(nm, cat, it, prior_tales)
                            try:
                                dossier_msg = client.messages.create(
                                    model=MODEL,
                                    max_tokens=REUSE_DOSSIER_MAX_TOKENS,
                                    messages=[{"role": "user", "content": dossier_prompt}],
                                )
                                detail["dossier"] = dossier_msg.content[0].text.strip()
                            except Exception as e:
                                print(f"WARNING: Reuse dossier build failed for {nm}: {e}", file=sys.stderr)
                    else:
                        detail["appearance_count"] = 0

                reuse_details.setdefault(cat, []).append(detail)

    # ── Optional: Pre-compute event arc dossiers ─────────────────────────
    # When ENABLE_EVENT_ARC_DOSSIER is on and an event has prior tale
    # appearances, we summarize its narrative arc via an API call so the
    # generation prompt gets a compact dossier instead of raw story text.
    event_arc_dossiers = {}
    reused_character_temporal = build_reused_character_temporal_snippets(
        reused_entries=reused_entries,
        reuse_details=reuse_details,
        temporal_path=CHARACTER_TEMPORAL_FILE,
        current_date_key=date_key,
    )
    if reused_character_temporal:
        print(f"✓ Temporal continuity snippets for reused characters: {len(reused_character_temporal)}")

    if ENABLE_WORLD_EVENT_ARCS and ENABLE_EVENT_ARC_DOSSIER:
        selected_events = _select_world_event_arcs(today_str)
        for evt in selected_events:
            evt_name = (evt.get("name") or "").strip()
            if not evt_name:
                continue
            event_tales = gather_prior_tales_for_entity(
                evt,
                max_appearances=EVENT_ARC_DOSSIER_MAX_TALES,
                max_chars_per_story=EVENT_ARC_DOSSIER_MAX_CHARS_PER_STORY,
                max_total_chars=REUSE_DOSSIER_MAX_TOTAL_INPUT_CHARS,
            )
            if not event_tales:
                continue
            print(f"Building arc dossier for event \"{evt_name}\" ({len(event_tales)} tales)...")
            dossier_prompt = build_event_arc_dossier_prompt(evt, event_tales)
            try:
                dossier_msg = client.messages.create(
                    model=MODEL,
                    max_tokens=EVENT_ARC_DOSSIER_MAX_TOKENS,
                    messages=[{"role": "user", "content": dossier_prompt}],
                )
                dossier_text = dossier_msg.content[0].text.strip()
                if dossier_text:
                    event_arc_dossiers[evt_name.lower()] = dossier_text
                    print(f"✓ Arc dossier for \"{evt_name}\": {len(dossier_text)} chars")
            except Exception as e:
                print(f"WARNING: Event arc dossier failed for \"{evt_name}\": {e}", file=sys.stderr)

    # ── CALL 1: Generate stories with lore context ───────────────────────
    print("Calling Claude to generate stories...")
    message = client.messages.create(
        model=MODEL,
        max_tokens=4096,
        messages=[{"role": "user", "content": build_prompt(today_str, world_date_label, lore, reused_entries=reused_entries, reuse_details=reuse_details, event_arc_dossiers=event_arc_dossiers, codex_balance=codex_balance, reused_character_temporal=reused_character_temporal)}]
    )
    raw = message.content[0].text.strip()
    try:
        stories_raw = parse_json_response(raw)
    except (ValueError, json.JSONDecodeError) as e:
        print(f"ERROR: Could not parse story JSON: {e}", file=sys.stderr)
        print("Raw response:", raw[:500], file=sys.stderr)
        sys.exit(1)

    story_items = extract_story_items(stories_raw)
    if not isinstance(story_items, list):
        print("ERROR: Parsed story JSON did not contain a story list in a known shape.", file=sys.stderr)
        if isinstance(stories_raw, dict):
            keys = ", ".join(sorted(str(k) for k in stories_raw.keys())[:40])
            print(f"Top-level keys: {keys}", file=sys.stderr)
        print(f"Parsed type: {type(stories_raw)}", file=sys.stderr)
        sys.exit(1)

    stories = [coerce_story_dict(s) for s in story_items[:NUM_STORIES]]

    # If we didn't get all expected stories, try a lightweight repair:
    # 1) Ask Claude to reformat the prior response into an exact JSON array.
    # 2) If still short, ask for only the missing number of additional stories.
    if len(stories) < NUM_STORIES:
        print(
            f"WARNING: Only parsed {len(stories)}/{NUM_STORIES} stories; attempting JSON repair.",
            file=sys.stderr,
        )
        try:
            repair_msg = client.messages.create(
                model=MODEL,
                max_tokens=4096,
                messages=[{"role": "user", "content": build_story_json_reformat_prompt(raw, NUM_STORIES)}],
            )
            repair_raw = repair_msg.content[0].text.strip()
            repaired = parse_json_response(repair_raw)
            repaired_items = extract_story_items(repaired) or []
            repaired_stories = [coerce_story_dict(s) for s in repaired_items[:NUM_STORIES]]
            if len(repaired_stories) >= len(stories):
                stories = repaired_stories
        except Exception as e:
            print(f"WARNING: JSON repair attempt failed: {e}", file=sys.stderr)

    if len(stories) < NUM_STORIES:
        stories = extend_with_missing_story_batches(
            client,
            today_str,
            world_date_label,
            lore,
            stories,
            event_arc_dossiers=event_arc_dossiers,
        )

    if len(stories) < NUM_STORIES:
        print(f"ERROR: Only {len(stories)}/{NUM_STORIES} stories available; aborting.", file=sys.stderr)
        sys.exit(1)

    print(f"\u2713 Generated {len(stories)} stories")

    # ── Optional: Content guardrail (block child death/targeted harm) ───
    if ENABLE_CHILD_HARM_GUARD:
        for attempt in range(max(0, CHILD_HARM_MAX_REWRITES) + 1):
            violations = find_child_harm_violations(stories)
            if not violations:
                break
            if attempt >= max(0, CHILD_HARM_MAX_REWRITES):
                print("ERROR: Child-harm guardrail could not be satisfied after rewrites.", file=sys.stderr)
                for v in violations:
                    print(f" - Story #{v.get('index')+1}: {v.get('title')} — {', '.join(v.get('violations') or [])}", file=sys.stderr)
                sys.exit(1)
            print(f"\u26a0\ufe0f Child-harm guardrail: rewriting {len(violations)} story(ies) (attempt {attempt+1}/{CHILD_HARM_MAX_REWRITES})...")
            rewrite_msg = client.messages.create(
                model=MODEL,
                max_tokens=4096,
                messages=[{"role": "user", "content": build_child_harm_rewrite_prompt(stories, violations)}],
            )
            rewrite_raw = rewrite_msg.content[0].text.strip()
            try:
                revised = parse_json_response(rewrite_raw)
                revised_items = extract_story_items(revised) or []
                revised_stories = [coerce_story_dict(s) for s in revised_items[:NUM_STORIES]]
                if len(revised_stories) == len(stories):
                    stories = revised_stories
            except (ValueError, json.JSONDecodeError) as e:
                print(f"WARNING: Could not parse child-safety rewrite JSON: {e}", file=sys.stderr)
                print("Continuing to next rewrite attempt with original stories.", file=sys.stderr)

    # ── Optional: Content guardrail (block rape/sexual assault + explicit sex) ─
    if ENABLE_SEXUAL_CONTENT_GUARD:
        for attempt in range(max(0, SEXUAL_CONTENT_MAX_REWRITES) + 1):
            violations = find_sexual_content_violations(stories)
            if not violations:
                break
            if attempt >= max(0, SEXUAL_CONTENT_MAX_REWRITES):
                print("WARNING: Sexual-content guardrail could not be satisfied after rewrites; sanitizing deterministically.", file=sys.stderr)
                for v in violations:
                    print(f" - Story #{v.get('index')+1}: {v.get('title')} — {', '.join(v.get('violations') or [])}", file=sys.stderr)
                for v in violations:
                    try:
                        idx = int(v.get("index"))
                    except Exception:
                        continue
                    if 0 <= idx < len(stories) and isinstance(stories[idx], dict):
                        stories[idx] = sanitize_story_for_sexual_content(stories[idx])
                # After sanitization, proceed even if the model couldn't rewrite cleanly.
                break
            print(f"\u26a0\ufe0f Sexual-content guardrail: rewriting {len(violations)} story(ies) (attempt {attempt+1}/{SEXUAL_CONTENT_MAX_REWRITES})...")
            rewrite_msg = client.messages.create(
                model=MODEL,
                max_tokens=4096,
                messages=[{"role": "user", "content": build_sexual_content_rewrite_prompt(stories, violations)}],
            )
            rewrite_raw = rewrite_msg.content[0].text.strip()
            try:
                revised = parse_json_response(rewrite_raw)
                revised_items = extract_story_items(revised) or []
                revised_stories = [coerce_story_dict(s) for s in revised_items[:NUM_STORIES]]
                if len(revised_stories) == len(stories):
                    stories = revised_stories
            except (ValueError, json.JSONDecodeError) as e:
                print(f"WARNING: Could not parse sexual-safety rewrite JSON: {e}", file=sys.stderr)
                print("Continuing to next rewrite attempt with original stories.", file=sys.stderr)

    # ── Optional: Motif overuse guardrail (reduce repetition) ───────────
    if ENABLE_MOTIF_GUARD:
        for attempt in range(max(0, MOTIF_MAX_REWRITES) + 1):
            violations = find_motif_overuse_violations(stories)
            if not violations:
                break
            if attempt >= max(0, MOTIF_MAX_REWRITES):
                print("WARNING: Motif-overuse guardrail could not be satisfied after rewrites; proceeding.", file=sys.stderr)
                for v in violations:
                    print(f" - Story #{v.get('index')+1}: {v.get('title')} — {', '.join(v.get('violations') or [])}", file=sys.stderr)
                break
            print(f"\u26a0\ufe0f Motif-overuse guardrail: rewriting {len(violations)} story(ies) (attempt {attempt+1}/{MOTIF_MAX_REWRITES})...")
            rewrite_msg = client.messages.create(
                model=MODEL,
                max_tokens=4096,
                messages=[{"role": "user", "content": build_motif_rewrite_prompt(stories, violations)}],
            )
            rewrite_raw = rewrite_msg.content[0].text.strip()
            try:
                revised = parse_json_response(rewrite_raw)
                revised_items = extract_story_items(revised) or []
                revised_stories = [coerce_story_dict(s) for s in revised_items[:NUM_STORIES]]
                if len(revised_stories) == len(stories):
                    stories = revised_stories
            except (ValueError, json.JSONDecodeError) as e:
                print(f"WARNING: Could not parse motif-rewrite JSON: {e}", file=sys.stderr)
                print("Continuing to next rewrite attempt with original stories.", file=sys.stderr)

    # ── Character-trait continuity check ─────────────────────────────────
    if ENABLE_CONTINUITY_CHECK:
        for attempt in range(max(0, CONTINUITY_MAX_REWRITES) + 1):
            issues = find_continuity_issues(stories)
            if not issues:
                if attempt == 0:
                    print("\u2713 Continuity check: no character-trait swaps detected")
                break
            if attempt >= max(0, CONTINUITY_MAX_REWRITES):
                total_suspects = sum(len(i.get("suspects", [])) for i in issues)
                print(f"WARNING: Continuity check found {total_suspects} suspected trait swap(s) after {attempt} rewrite(s); proceeding anyway.", file=sys.stderr)
                for iss in issues:
                    for s in iss.get("suspects", []):
                        print(f"  - Story #{iss['index']+1} \"{iss['title']}\": "
                              f"trait \"{s['trait']}\" ({s['trait_owner']}) used near {s['active_char']}",
                              file=sys.stderr)
                break
            total_suspects = sum(len(i.get("suspects", [])) for i in issues)
            print(f"\u26a0\ufe0f Continuity check: {total_suspects} suspected trait swap(s); rewriting (attempt {attempt+1}/{CONTINUITY_MAX_REWRITES})...")
            rewrite_msg = client.messages.create(
                model=MODEL,
                max_tokens=4096,
                messages=[{"role": "user", "content": build_continuity_rewrite_prompt(stories, issues)}],
            )
            rewrite_raw = rewrite_msg.content[0].text.strip()
            try:
                revised = parse_json_response(rewrite_raw)
                revised_items = extract_story_items(revised) or []
                revised_stories = [coerce_story_dict(s) for s in revised_items[:NUM_STORIES]]
                if len(revised_stories) == len(stories):
                    stories = revised_stories
                    print("\u2713 Continuity rewrite applied")
            except (ValueError, json.JSONDecodeError) as e:
                print(f"WARNING: Could not parse continuity rewrite JSON: {e}", file=sys.stderr)
                print("Continuing with original stories.", file=sys.stderr)

    # ── Collision renamer: prevent accidental canon name reuse ───────────
    collisions = find_canon_collisions(stories, lore, allowed_names_lower)
    if collisions:
        total = sum(len(v) for v in collisions.values())
        print(f"\u26a0\ufe0f Detected {total} accidental canon name collision(s); renaming...")
        rename_msg = client.messages.create(
            model=MODEL,
            max_tokens=4096,
            messages=[{
                "role": "user",
                "content": build_collision_rename_prompt(stories, collisions),
            }],
        )
        rename_raw = rename_msg.content[0].text.strip()
        try:
            revised = parse_json_response(rename_raw)
            revised_items = extract_story_items(revised) or []
            revised_stories = [coerce_story_dict(s) for s in revised_items[:NUM_STORIES]]
            if len(revised_stories) == len(stories):
                stories = revised_stories
                print("\u2713 Renamed accidental canon collisions")
        except (ValueError, json.JSONDecodeError) as e:
            print(f"WARNING: Could not parse collision-rename JSON: {e}", file=sys.stderr)
            print("Continuing with original stories.", file=sys.stderr)

    # ── Optional: Revision pass to enforce canon for any reused entities ─
    if ENABLE_LORE_REVISION_PASS:
        referenced = find_referenced_canon_entries(stories, lore)
        if referenced:
            total_hits = sum(len(v) for v in referenced.values())
            print(f"Lore revision enabled: detected {total_hits} referenced canon entities; revising stories...")
            revision_message = client.messages.create(
                model=MODEL,
                max_tokens=4096,
                messages=[{
                    "role": "user",
                    "content": build_lore_revision_prompt(stories, lore, referenced),
                }],
            )
            revision_raw = revision_message.content[0].text.strip()
            try:
                revised = parse_json_response(revision_raw)
                revised_items = extract_story_items(revised) or []
                revised_stories = [coerce_story_dict(s) for s in revised_items[:NUM_STORIES]]
                if len(revised_stories) == len(stories):
                    stories = revised_stories
                    print("\u2713 Revised stories for canon consistency")
            except (ValueError, json.JSONDecodeError) as e:
                print(f"WARNING: Could not parse lore revision JSON: {e}", file=sys.stderr)
                print("Continuing with original stories.", file=sys.stderr)

    # ── Optional: Canon checker (flag contradictions; optionally rewrite) ─
    if ENABLE_CANON_CHECKER:
        referenced = find_referenced_canon_entries(stories, lore)
        if referenced:
            total_hits = sum(len(v) for v in referenced.values())
            print(f"Canon checker enabled: auditing against {total_hits} referenced canon entities...")
            check_message = client.messages.create(
                model=MODEL,
                max_tokens=4096,
                messages=[{
                    "role": "user",
                    "content": build_canon_checker_prompt(stories, lore, referenced, mode=CANON_CHECKER_MODE),
                }],
            )
            check_raw = check_message.content[0].text.strip()
            try:
                check_result = parse_json_response(check_raw)
                issues = check_result.get("issues", []) if isinstance(check_result, dict) else []
                if issues:
                    high = sum(1 for i in issues if (i.get("severity") or "").lower() == "high")
                    print(f"\u26a0\ufe0f Canon issues found: {len(issues)} (high severity: {high})")
                else:
                    print("\u2713 Canon checker: no issues reported")

                if CANON_CHECKER_MODE != "report":
                    revised_any = check_result.get("stories") if isinstance(check_result, dict) else None
                    revised_items = extract_story_items(revised_any) or []
                    if revised_items:
                        revised_stories = [coerce_story_dict(s) for s in revised_items[:NUM_STORIES]]
                        if len(revised_stories) == len(stories):
                            stories = revised_stories
                            print("\u2713 Applied canon-safe rewrites")
            except (ValueError, json.JSONDecodeError) as e:
                print(f"WARNING: Could not parse canon checker JSON: {e}", file=sys.stderr)
                print("Continuing without canon checker changes.", file=sys.stderr)

    # ── Sanitize story text (removes stray control characters) ─────────
    stories = sanitize_stories(stories)

    # ── Optional: Update existing characters (death/resurrection/etc.) ───
    if ENABLE_EXISTING_CHARACTER_UPDATES:
        referenced = find_referenced_canon_entries(stories, lore)
        referenced_chars = referenced.get("characters", []) if isinstance(referenced, dict) else []
        if referenced_chars:
            print(f"Updating existing characters from today's stories ({len(referenced_chars)} referenced)...")
            updates_message = client.messages.create(
                model=MODEL,
                max_tokens=2048,
                messages=[{
                    "role": "user",
                    "content": build_existing_character_updates_prompt(stories, lore, referenced_chars),
                }],
            )
            updates_raw = updates_message.content[0].text.strip()
            try:
                updates = parse_json_response(updates_raw)
                lore = apply_existing_character_updates(lore, updates, date_key, stories=stories)
                lore = ensure_home_location_entities_exist(lore, date_key)
                changed = len((updates or {}).get("characters", []) or []) if isinstance(updates, dict) else 0
                if changed:
                    print(f"\u2713 Applied {changed} character status update(s)")
                else:
                    print("\u2713 No character status changes detected")
            except (ValueError, json.JSONDecodeError) as e:
                print(f"WARNING: Could not parse existing-character updates JSON: {e}", file=sys.stderr)
                print("Continuing without character status updates.", file=sys.stderr)

    # ── CALL 2: Extract new lore from generated stories (batched) ───────
    print("Calling Claude to extract lore from new stories...")
    new_lore = _extract_lore_batched(client, stories, lore, codex_balance=codex_balance)
    new_lore = filter_lore_to_stories(new_lore, stories)
    new_lore = ensure_named_character_mentions_present(new_lore, stories)
    new_lore = ensure_named_leaders_present(new_lore, stories)
    for char in new_lore.get("characters", []):
        if not isinstance(char, dict):
            continue
        nm = (char.get("name") or "").strip()
        if not nm:
            continue
        nm_low = nm.lower()
        for s in stories:
            title_low = (s.get("title", "") or "").lower()
            text_low = (s.get("text", "") or "").lower()
            if nm_low in text_low or nm_low in title_low:
                char["first_story"] = s.get("title", "")
                break
    lore = merge_lore(lore, new_lore, date_key)
    warn_polity_conflicts(lore)
    print(
        f"\u2713 Extracted "
        f"{len(new_lore.get('characters', []))} chars, "
        f"{len(new_lore.get('places', []))} places, "
        f"{len(new_lore.get('events', []))} events, "
        f"{len(new_lore.get('weapons', []))} weapons, "
        f"{len(new_lore.get('artifacts', []))} artifacts"
    )

    # ── Save today's stories.json ─────────────────────────────────────────
    output = {
        "date":         date_key,
        "generated_at": issue_now.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "stories":      stories
    }
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=True, indent=2)
    print(f"\u2713 Saved {len(stories)} stories to {OUTPUT_FILE}")

    # ── Save to archive/<date>.json ──────────────────────────────────────
    ensure_archive_dir()
    archive_file = os.path.join(ARCHIVE_DIR, f"{date_key}.json")
    with open(archive_file, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=True, indent=2)
    print(f"\u2713 Archived to {archive_file}")

    # ── Update archive/index.json ─────────────────────────────────────────
    idx = load_archive_index()
    if date_key not in idx["dates"]:
        idx["dates"].insert(0, date_key)
    idx["dates"].sort(reverse=True)
    save_archive_index(idx)
    print(f"\u2713 Updated {ARCHIVE_IDX} ({len(idx['dates'])} dates total)")

    # ── Save lore.json ─────────────────────────────────────────────────────
    save_lore(lore, date_key)
    print(f"\u2713 Saved {LORE_FILE} ({len(lore.get('characters', []))} characters total)")

    # ── Update characters.json (legacy) ───────────────────────────────────
    update_characters_file(lore, date_key, stories)

    # ── Update codex.json ──────────────────────────────────────────────────
    update_codex_file(lore, date_key, stories)

    # ── Sync persistent world-state snapshot (additive simulation layer) ───
    try:
        ws = sync_world_state_from_codex_and_stories(
            codex_path=CODEX_FILE,
            date_key=date_key,
            stories=stories,
            output_path=WORLD_STATE_FILE,
        )
        if ws.get("updated"):
            print(
                f"\u2713 Saved {WORLD_STATE_FILE} "
                f"(issue #{ws.get('issue_number')}, observed {ws.get('events_observed', 0)} events, "
                f"applied {ws.get('events_applied')} persistent deltas)"
            )
        else:
            print(f"WARNING: world-state sync skipped: {ws.get('reason', 'unknown')}", file=sys.stderr)
    except Exception as e:
        print(f"WARNING: world-state sync failed: {e}", file=sys.stderr)

    # ── Refresh temporal sidecars and lifecycle state ─────────────────────
    try:
        temporal_payload = refresh_character_temporal(
            codex_path=CODEX_FILE,
            output_path=CHARACTER_TEMPORAL_FILE,
            age_mode="auto",
            model=MODEL,
        )
        print(
            f"\u2713 Saved {CHARACTER_TEMPORAL_FILE} "
            f"({temporal_payload.get('count', 0)} characters; age adjudication auto)"
        )
    except Exception as e:
        print(f"WARNING: temporal refresh failed: {e}", file=sys.stderr)

    try:
        temporal_payload, log_payload = simulate_lifecycle(
            codex_path=CODEX_FILE,
            temporal_path=CHARACTER_TEMPORAL_FILE,
            log_path=CHARACTER_LIFECYCLE_LOG_FILE,
            mode="auto",
            lookback_issues=6,
            max_candidates=120,
            model=MODEL,
        )
        print(
            f"\u2713 Simulated lifecycle "
            f"({log_payload.get('count', 0)} events; mode={log_payload.get('mode')})"
        )
    except Exception as e:
        print(f"WARNING: lifecycle simulation failed: {e}", file=sys.stderr)

    try:
        lineage_payload = refresh_lineages(
            codex_path=CODEX_FILE,
            temporal_path=CHARACTER_TEMPORAL_FILE,
            output_path=LINEAGES_FILE,
        )
        print(f"\u2713 Saved {LINEAGES_FILE} ({lineage_payload.get('count', 0)} lineages)")
    except Exception as e:
        print(f"WARNING: lineage refresh failed: {e}", file=sys.stderr)

    try:
        alliance_payload = refresh_alliances(
            codex_path=CODEX_FILE,
            temporal_path=CHARACTER_TEMPORAL_FILE,
            output_path=ALLIANCES_FILE,
        )
        print(f"\u2713 Saved {ALLIANCES_FILE} ({alliance_payload.get('count', 0)} alliances)")
    except Exception as e:
        print(f"WARNING: alliance refresh failed: {e}", file=sys.stderr)

if __name__ == "__main__":
    try:
        main()
    except anthropic.BadRequestError as e:
        in_github_actions = (os.environ.get("GITHUB_ACTIONS") or "").strip().lower() == "true"
        if not in_github_actions or not _is_anthropic_usage_limit_error(e):
            raise
        reset_hint = _extract_anthropic_reset_hint(e)
        print(
            "WARNING: Anthropic API usage limit reached; skipping story generation for this run.",
            file=sys.stderr,
        )
        if reset_hint:
            print(f"INFO: Anthropic reports access returns {reset_hint}.", file=sys.stderr)
        raise SystemExit(0)
