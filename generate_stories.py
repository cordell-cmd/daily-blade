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

# Existing-entity updates: extract updates for ALREADY KNOWN entities referenced today.
# This is how we can learn status changes (dead -> reanimated, etc.) even though the "NEW lore" extractor skips known names.
ENABLE_EXISTING_CHARACTER_UPDATES = os.environ.get(
    "ENABLE_EXISTING_CHARACTER_UPDATES",
    "1",
).strip().lower() in {"1", "true", "yes", "y"}

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
REUSE_DOSSIER_MAX_APPEARANCES = int(os.environ.get("REUSE_DOSSIER_MAX_APPEARANCES", "25"))  # 0 = no limit
REUSE_DOSSIER_MAX_CHARS_PER_STORY = int(os.environ.get("REUSE_DOSSIER_MAX_CHARS_PER_STORY", "4000"))
REUSE_DOSSIER_MAX_TOKENS = int(os.environ.get("REUSE_DOSSIER_MAX_TOKENS", "1200"))
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
            return json.load(f)
    return {
        "version": "1.0",
        "worlds": [],
        "hemispheres": [],
        "continents": [],
        "subcontinents": [],
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
                "subcontinent": "unknown",
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
    - regions/districts/provinces/realms/subcontinents/continents/hemispheres
    
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
        obj.setdefault("subcontinent", "unknown")
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
        "subcontinents": "subcontinent",
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


def build_reuse_plan_prompt(today_str, lore, candidates_by_category):
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

Today's date: {today_str}

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


def _clip_text(text: str, max_chars: int) -> str:
    t = text or ""
    max_chars = int(max_chars or 0)
    if max_chars <= 0 or len(t) <= max_chars:
        return t
    # Keep head+tail for some context.
    head = int(max_chars * 0.7)
    tail = max_chars - head
    return t[:head].rstrip() + "\n…\n" + t[-tail:].lstrip()


def gather_prior_tales_for_entity(codex_entry, max_appearances: int, max_chars_per_story: int):
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


def allowed_reuse_name_set(full_entries_by_cat):
    allowed = set()
    for items in (full_entries_by_cat or {}).values():
        for it in items or []:
            nm = (it.get("name") or "").strip()
            if nm:
                allowed.add(nm.lower())
    return allowed


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
    """Scalable lore context for generation: world rules (no full-lore dump)."""
    parts = []
    # Worlds + rules are the most important global canon.
    if lore.get("worlds"):
        w0 = lore["worlds"][0]
        parts.append("=== WORLD ===")
        parts.append(f"• {w0.get('name','The Known World')}: {w0.get('description','')}")
        if w0.get("rules"):
            parts.append("")
            parts.append("=== LORE RULES (must be respected) ===")
            parts.extend([f"• {r}" for r in w0.get("rules", [])])

    return "\n".join([p for p in parts if p is not None]).strip()


def _canon_loc_names_from_codex(codex: dict) -> dict[str, list[str]]:
    if not isinstance(codex, dict):
        return {}
    out: dict[str, list[str]] = {}
    for cat in ["places", "districts", "provinces", "regions", "realms", "subcontinents", "continents", "hemispheres", "worlds", "polities"]:
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
        ("subcontinents", "subcontinent"),
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
        elif mentions.get("continents") or mentions.get("subcontinents"):
            scope = "continental"
        elif mentions.get("realms") or mentions.get("regions"):
            scope = "regional"
        elif mentions.get("places") or mentions.get("districts") or mentions.get("provinces"):
            scope = "city"

    return {"scope": scope, "epicenter": epicenter, "mentions": mentions}


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
        "subcontinent": _get("subcontinent"),
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

    if category in {"places", "districts", "provinces", "regions", "realms", "continents", "subcontinents", "hemispheres", "worlds"}:
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
            elif category == "subcontinents":
                anchors["subcontinent"] = anchors.get("subcontinent") or nm
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
            or ("subcontinent" in entity_anchors and _has("subcontinents", entity_anchors["subcontinent"]))
            or ("realm" in entity_anchors and _has("realms", entity_anchors["realm"]))
            or ("region" in entity_anchors and _has("regions", entity_anchors["region"]))
        )

    if scope == "world":
        if "world" in entity_anchors and _has("worlds", entity_anchors["world"]):
            return True
        return True

    return False


def build_world_event_arcs_section(today_str: str, lore: dict) -> str:
    """Build a small, issue-wide world-events section for the generation prompt."""
    if not ENABLE_WORLD_EVENT_ARCS:
        return ""

    codex = load_codex_file()
    events = codex.get("events", []) if isinstance(codex, dict) else []
    if not isinstance(events, list) or not events:
        return ""

    loc_names = _canon_loc_names_from_codex(codex)
    known_dates = _load_known_issue_dates()

    # Pick a small set of active/important events deterministically per day.
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

        # Base importance from descriptive heft, then bias toward active arcs.
        weight = 1.0 + min(3.0, (len(sig) + len(out)) / 400.0)
        # Active arcs get a boost; resolved arcs get a slight penalty.
        if arc.get("resolved"):
            weight *= 0.7
        else:
            weight *= (1.0 + 0.18 * int(arc.get("intensity") or 1))
            if int(arc.get("days_ago") or 999) <= 2:
                weight *= 1.25
        # Tiny jitter so ties don't always pick the same.
        weight *= (0.92 + 0.16 * rng.random())
        scored.append((weight, e, arc))

    scored.sort(key=lambda x: x[0], reverse=True)
    picked = [e for _, e, _ in scored[: max(1, min(WORLD_EVENT_ARCS_MAX, 4))]]
    picked = picked[: max(0, int(WORLD_EVENT_ARCS_MAX or 0))]
    if not picked:
        return ""

    lines = []
    lines.append("ISSUE-WIDE WORLD EVENTS (shared continuity / cross-story pressures):")
    lines.append("- Treat these as real background pressures in the world. Some stories may be directly inside the affected area; others may only hear rumor.")
    lines.append("- If a story is set within an event's scope, show at least ONE concrete effect (refugees, rationing, conscription, tolls, cults, broken trade, riots, shadow-markets, etc.).")
    lines.append("- You may let minor characters cross paths across stories due to these pressures, but do NOT reuse the same protagonist or primary location.")
    lines.append("- If you organically introduce a NEW large-scale event, let it persist across future issues: escalate from hints → consequences → turning points → aftermath, then either resolve it or let it cool into lasting scars.")
    lines.append("- Arc pacing mechanic (organic):")
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
        elif scope_lc in {"continent", "subcontinent"}:
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
def build_prompt(today_str, lore, reused_entries=None, reuse_details=None):
    lore_context = build_generation_lore_context(lore, seed_text=today_str)
    reused_entries = reused_entries or {}
    reuse_details = reuse_details or {}

    world_events_section = build_world_event_arcs_section(today_str, lore)

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
    return f"""You are a pulp fantasy writer in the tradition of Robert E. Howard, Clark Ashton Smith, and Fritz Leiber.
Generate exactly 10 original short sword-and-sorcery stories.
Each story should be vivid, action-packed, and around 120–160 words long.

ORIGINALITY / COPYRIGHT SAFETY:
- Create ONLY original characters, places, factions, creatures, spells, artifacts, and titles.
- Do NOT use or reference recognizable copyrighted names or specific settings from existing works (e.g., no Gandalf, no Middle-earth, etc.).
- Generic fantasy archetypes are fine (wizards, dragons, elves, dwarves, etc.), but names and particulars must be newly invented.

Today's date is {today_str}. Use this as subtle creative inspiration if you like.
{lore_section}
{reuse_section}
Respond with ONLY valid JSON — no prose before or after — matching this exact structure:
[
  {{ "title": "Story Title Here", "subgenre": "Two or Three Word Label", "text": "Full story text here…" }},
  …9 more entries…
]

CONTENT GUARDRAILS (must follow):
- Do NOT write stories involving child death or targeted harm to children.
    - No infanticide, no parents killing children, no child sacrifice, no child murder, no violence directed at a child.
- Avoid plots centered on a dead child, even off-screen.
- Children may be mentioned only in non-exploitative background context when the harm is NOT targeted and is a broad tragedy.
    - Allowed examples: a plague, famine, or natural disaster affecting a town/village/region (many people), described briefly.
    - Not allowed: a specific child being killed, poisoned, sacrificed, or abused.

- Do NOT write rape/sexual assault or sexual violence.
- Do NOT depict explicit sex acts or create vivid visuals of sex.
    - Sexual/romantic tension is fine; allusion is fine.
    - Keep any intimacy off-screen / fade-to-black; avoid explicit anatomy or explicit action verbs.

TONE + FANTASY VARIETY (creative palette; use your judgment):
- Avoid monotone issues: aim for a MIX of tones, not 10 grim macabre tales.
    - Include some lighter/adventurous/wondrous pieces (mystery, heist, exploration, comic irony, heroic triumph).
    - Include at least one love-story / romance thread (can be sweet, tragic, or bittersweet; keep it pulp-fantasy).
- Magic is welcome but not mandatory: aim for a mix of sorcery and non-magic conflict (steel, politics, survival, bargains, travel, rivalries).
- Use the full fantasy toolbox when it fits the world rules.
    - The examples below are NOT a limit; you are encouraged to invent new kinds of peoples, creatures, cultures, magics, and wonders.
    - Non-human peoples (elves, dwarves, goblin-kind, smallfolk, orcs/ogres/trolls — or wholly new lineages).
    - Mythic creatures (a dragon/wyrm or similarly iconic beast — or a brand-new apex terror).
    - Fae/fairy influence (a fae court, fairy realm, or a fae-bargain — or any other uncanny otherworld).
- Stories may center on ANY fantasy focus (not just people): creatures, artifacts/weapons/relics, or places can be the "main character".

REUSE INTENSITY RULES (only applies when an entry is labeled):
- If you see "INTENDED REUSE INTENSITY: cameo" for an entity, keep it light: brief appearance/mention, not the protagonist or primary location, and avoid major new canon changes for that entity.
- If you see "INTENDED REUSE INTENSITY: central" for an entity, it may meaningfully drive plot, but MUST remain consistent with canon and the dossier.

Guidelines:
- Heroes and antiheroes with colorful names (barbarians, sell-swords, sorcerers, thieves)
- Vivid exotic settings: crumbling empires, cursed ruins, blasted steppes, sorcerous cities
- Stakes that feel epic: ancient evil, demonic pacts, dying gods, vengeful sorcery
- Each story must be complete with a beginning, conflict, and satisfying (or ironic) ending
- Vary protagonists, locations, and types of magic/conflict across all 10 stories
- Use dramatic, muscular prose — short punchy sentences mixed with lush description
- Avoid modern slang; use archaic flavor without being unreadable
- No two stories should share a protagonist or primary location
- For each story, invent a vivid 2-4 word subgenre label that captures its specific flavor.
  You are NOT limited to any fixed list — be creative. Examples of the kind of variety to aim for:
  Sword & Sorcery, Dark Fantasy, Political Intrigue, Forbidden Alchemy, Lost World, Blood Oath,
  Ghost Empire, Thieves' War, Demon Pact, Sea Sorcery, Witch Hunt, Siege & Betrayal — or anything
  that fits. The label should feel like a pulp magazine category."""


_CHILD_TERM_RE = re.compile(r"\b(child|children|kid|kids|boy|girl|infant|baby|toddler|son|daughter)\b", re.IGNORECASE)
_CHILD_OWN_RE = re.compile(r"\b(her|his|their|my|your|our)\s+own\s+(child|son|daughter|baby|infant)\b", re.IGNORECASE)
_CHILD_DEATH_RE = re.compile(r"\b(died|die|dead|death|corpse|funeral|buried)\b", re.IGNORECASE)
_CHILD_VIOLENCE_RE = re.compile(
    r"\b(kill|killed|killing|murder|murdered|slay|slain|stab|stabbed|strangle|strangled|smother|smothered|drown|drowned|poison|poisoned|sacrifice|sacrificed|burned\s+alive|butcher|butchered)\b",
    re.IGNORECASE,
)
_NATURAL_CAUSE_RE = re.compile(
    r"\b(plague|epidemic|pox|fever|sickness|disease|illness|famine|drought|flood|fire|wildfire|earthquake|storm|blizzard|landslide|tidal\s+wave)\b",
    re.IGNORECASE,
)
_MASS_CONTEXT_RE = re.compile(
    r"\b(village|town|city|realm|region|province|district|many|dozens|scores|hundreds|thousands|the\s+people|the\s+populace|crowds)\b",
    re.IGNORECASE,
)


def _natural_mass_context(text: str) -> bool:
    s = (text or "")
    if re.search(r"\b(plague|epidemic)\b", s, flags=re.IGNORECASE):
        return True
    return bool(_NATURAL_CAUSE_RE.search(s) and _MASS_CONTEXT_RE.search(s))


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
    has_child = bool(_CHILD_TERM_RE.search(blob) or _CHILD_OWN_RE.search(blob))
    if not has_child:
        return []

    if _CHILD_OWN_RE.search(blob) and _CHILD_VIOLENCE_RE.search(blob):
        violations.append("targeted harm to a child (own child + violence)")

    if _near(blob, _CHILD_TERM_RE, _CHILD_VIOLENCE_RE, window=120) or _near(blob, _CHILD_OWN_RE, _CHILD_VIOLENCE_RE, window=200):
        violations.append("violence directed at a child")

    if _near(blob, _CHILD_TERM_RE, _CHILD_DEATH_RE, window=120):
        if not _natural_mass_context(blob):
            violations.append("child death without broad natural-disaster/illness context")

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
- Avoid plots centered on a dead child, even off-screen.

NARROW EXCEPTION (allowed only as brief background):
- A broad tragedy like plague/famine/natural disaster affecting many people, described briefly and without exploitation.

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
def build_lore_extraction_prompt(stories, existing_lore):
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
            r"\b(?:the\s+)?(idol|crown|throne|blade|dagger|ring|tome|amulet|chalice|mask|orb|eye|eyes)\s+of\s+"
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
    existing_subcontinents = {s["name"].lower() for s in existing_lore.get("subcontinents", []) if isinstance(s, dict) and s.get("name")}
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

    return f"""You are a lore archivist for a sword-and-sorcery story universe.
Analyze the following stories and extract lore elements — characters, places, events, weapons, artifacts, factions, polities (governments/crowns/thrones), lore, flora/fauna, magic, relics, regions, substances, and geo hierarchy entries (hemisphere/continent/subcontinent/realm/province/district) — that appear in these stories.

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
- Treat patterns like "X of Y" and "The X of Y" as likely names; include them when they read like a title, place, or event.
- Treat important objects described as "the <object> of <ProperName>" (even if <object> is lowercase) as named artifacts/relics and include them (e.g., "the idol of Khar-Zul").

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
- Subcontinents: {_known_summary(existing_subcontinents)}
- Realms: {_known_summary(existing_realms)}
- Provinces: {_known_summary(existing_provinces)}
- Regions: {_known_summary(existing_regions)}
- Districts: {_known_summary(existing_districts)}
- Substances & Materials: {_known_summary(existing_substances)}

GEOGRAPHY CONSTRAINTS:
- We are grounding this universe on ONE main planet/world.
- The number of continents must remain low and bounded. Maximum continents: {MAX_CONTINENTS}.
- Prefer to assign new places to an existing continent/realm/region when plausible.
- You MAY create a new continent only if truly necessary, and you must not exceed the maximum.

STORIES TO ANALYZE:
{stories_text}

NAME CANDIDATES (for completeness; ignore common words like "The" / "But"):
{candidates_block}

Hard requirement:
- For every candidate above that is truly a named entity in the story text (person/place/realm/title/institution/event/ritual/spell/object/creature), ensure it appears in at least one output category.
- Do NOT omit one-off names just because they appear only once.
- Do NOT drop or simplify punctuation/diacritics in names (keep apostrophes, hyphens, accents).
- Do NOT create duplicate entities for shortened references: if a character is "Kael the Nameless" and the story also says "Kael", output ONE character entry with the most complete name and list the shorter forms in "aliases".
- If a story mentions a named language/dialect/script (e.g. "Old Tongue"), include it under "lore" with category "language".

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
      "place_type": "city / fortress / ruin / temple / wilderness / etc",
      "world": "known_world",
            "hemisphere": "Name or 'unknown'",
            "continent": "Name or 'unknown'",
            "subcontinent": "Name or 'unknown'",
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
      "origin": "Where it came from or who forged it.",
      "powers": "Any magical or legendary properties.",
      "last_known_holder": "Who had it last.",
      "status": "active / destroyed / lost / sealed",
      "notes": "Any story hooks."
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
            "function": "What this continent IS for (macro-biomes, cultural sphere, long-distance travel logic).",
            "status": "stable / fragmented / unknown",
            "notes": "Any hooks."
        }}
    ],
    "subcontinents": [
        {{
            "id": "snake_case_id",
            "name": "Subcontinent Name",
            "tagline": "Three evocative words.",
            "continent": "Name or 'unknown'",
            "function": "What this subcontinent IS for (trade zone, shared language sphere, coast vs interior).",
            "status": "stable / contested / unknown",
            "notes": "Any hooks."
        }}
    ],
    "hemispheres": [
        {{
            "id": "snake_case_id",
            "name": "Hemisphere Name",
            "tagline": "Three evocative words.",
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
            "province": "Province Name or 'unknown'",
            "region": "Region Name or 'unknown'",
            "function": "What this district IS for (military defense zone, patrol boundary, terrain management).",
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


def _resolve_character_target(existing_chars: list, incoming_name: str):
    """Resolve an incoming character name to an existing canonical entry when safe.

    Prefers matching by exact name, explicit aliases, or epithet/"the" reduction.
    Also supports safe single-token->full-name mapping when unambiguous.
    """
    inc_key = _norm_entity_key(incoming_name)
    if not inc_key:
        return None

    by_key = {}
    first_token_buckets = {}
    for c in existing_chars or []:
        if not isinstance(c, dict):
            continue
        nm = (c.get("name") or "").strip()
        if not nm:
            continue
        for k in _character_alias_keys(nm):
            by_key.setdefault(k, c)

        aliases = c.get("aliases")
        if isinstance(aliases, list):
            for a in aliases:
                ak = _norm_entity_key(a)
                if ak:
                    by_key.setdefault(ak, c)

        tok = _norm_entity_key(nm).split(" ")[0] if _norm_entity_key(nm) else ""
        if tok:
            first_token_buckets.setdefault(tok, []).append(c)

    if inc_key in by_key:
        return by_key[inc_key]

    # If incoming is a single token, map to the ONLY existing multi-word name that shares it.
    # This avoids creating a second entry for "Kael" when "Kael the Nameless" already exists.
    if " " not in inc_key:
        bucket = first_token_buckets.get(inc_key) or []
        multi = [c for c in bucket if isinstance(c.get("name"), str) and len(c.get("name").split()) >= 2]
        if len(multi) == 1:
            return multi[0]

    return None


def merge_lore(existing_lore, new_lore, date_key):
    """Merge newly extracted lore into the existing lore, skipping duplicates by name."""
    for category in [
        "hemispheres",
        "continents",
        "subcontinents",
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
                        # Upgrade if incoming looks like a more complete epithet/title form.
                        if (" the " in ink or "," in ink) and (" the " not in exk and "," not in exk) and ink.split(" ")[0] == exk.split(" ")[0]:
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
                    if k not in target or target.get(k) in {"", [], {}, None, "unknown"}:
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
        "subcontinents": [],
        "realms": [],
        "polities": [],
        "provinces": [],
        "districts": [],
        "characters": [],
        "places": [],
        "events": [],
        "rituals": [],
        "weapons": [],
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
                    if k in item and (item.get(k) is not None):
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
        if key in existing_map:
            return existing_map[key]
        # Try explicit aliases provided by lore.
        if isinstance(incoming_aliases, list):
            for a in incoming_aliases:
                ak = _norm_entity_key(a)
                if ak and ak in existing_map:
                    return existing_map[ak]
        # Try epithetless alias ("X the Y" -> "X").
        the_idx = key.find(" the ")
        if the_idx > 2:
            base = key[:the_idx].strip()
            if base in existing_map:
                return existing_map[base]
        # Single-token -> match exactly one multi-word canonical name.
        if " " not in key:
            candidates = []
            for k, obj in existing_map.items():
                if k.split(" ")[0] == key and " " in k:
                    candidates.append(obj)
            if len({id(x) for x in candidates}) == 1:
                return candidates[0]
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
                if (" the " in ink or "," in ink) and (" the " not in exk and "," not in exk) and ink.split(" ")[0] == exk.split(" ")[0]:
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

    # De-dupe obvious alias duplicates (safe case: "X" vs "X the Y")
    uniq = []
    seen_ids = set()
    for obj in codex.get("characters", []) or []:
        if not isinstance(obj, dict):
            continue
        if id(obj) in seen_ids:
            continue
        uniq.append(obj)
        seen_ids.add(id(obj))
    codex["characters"] = uniq

    # ── Geo hierarchy categories ─────────────────────────────────────────
    merge_named_category("hemispheres", ["function", "status", "notes"]) 
    merge_named_category("continents", ["function", "status", "notes"]) 
    merge_named_category("subcontinents", ["continent", "function", "status", "notes"]) 
    merge_named_category("realms", ["continent", "capital", "function", "taxation", "military", "status", "notes"]) 
    merge_named_category("polities", ["polity_type", "realm", "region", "seat", "sovereigns", "claimants", "status", "description", "notes"]) 
    merge_named_category("provinces", ["realm", "region", "function", "status", "notes"]) 
    merge_named_category("districts", ["province", "region", "function", "status", "notes"]) 

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
            ex["hemisphere"] = p.get("hemisphere", ex.get("hemisphere", "unknown"))
            ex["continent"] = p.get("continent", ex.get("continent", "unknown"))
            ex["subcontinent"] = p.get("subcontinent", ex.get("subcontinent", "unknown"))
            ex["realm"] = p.get("realm", ex.get("realm", "unknown"))
            ex["province"] = p.get("province", ex.get("province", "unknown"))
            ex["region"] = p.get("region", ex.get("region", "unknown"))
            ex["district"] = p.get("district", ex.get("district", "unknown"))
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
                "hemisphere":        p.get("hemisphere", "unknown"),
                "continent":         p.get("continent", "unknown"),
                "subcontinent":      p.get("subcontinent", "unknown"),
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

    # ── Merge events ─────────────────────────────────────────────────────
    existing_events = {e["name"].lower(): e for e in codex.get("events", [])}
    for e in lore.get("events", []):
        name = e.get("name", "Unknown")
        name_low = name.lower()
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
                if k in e and e.get(k) not in {None, "", [], {}}:
                    if ex.get(k) in {None, "", [], {}}:
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
        "subcontinents",
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
            "id": to_snake_case(leader),
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
    ensure_place_parent_chain(lore)
    enforce_continent_limit(lore)
    print(f"\u2713 Loaded lore ({len(lore.get('characters', []))} characters, "
          f"{len(lore.get('places', []))} places)")

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
                messages=[{"role": "user", "content": build_reuse_plan_prompt(today_str, lore, candidates_by_cat)}],
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

    # ── CALL 1: Generate stories with lore context ───────────────────────
    print("Calling Claude to generate stories...")
    message = client.messages.create(
        model=MODEL,
        max_tokens=4096,
        messages=[{"role": "user", "content": build_prompt(today_str, lore, reused_entries=reused_entries, reuse_details=reuse_details)}]
    )
    raw = message.content[0].text.strip()
    try:
        stories_raw = parse_json_response(raw)
    except (ValueError, json.JSONDecodeError) as e:
        print(f"ERROR: Could not parse story JSON: {e}", file=sys.stderr)
        print("Raw response:", raw[:500], file=sys.stderr)
        sys.exit(1)

    # Attach sub-genre labels
    if isinstance(stories_raw, dict) and isinstance(stories_raw.get("stories"), list):
        story_items = stories_raw.get("stories") or []
    else:
        story_items = stories_raw
    if not isinstance(story_items, list):
        print("ERROR: Parsed story JSON was not a list (or a dict with a 'stories' list).", file=sys.stderr)
        print(f"Parsed type: {type(stories_raw)}", file=sys.stderr)
        sys.exit(1)

    stories = []
    for i, s in enumerate(story_items[:NUM_STORIES]):
        if not isinstance(s, dict):
            continue
        stories.append({
            "title":    s.get("title",    "Untitled"),
            "text":     s.get("text",     ""),
            "subgenre": s.get("subgenre", "Sword & Sorcery")
        })
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
                revised_stories = []
                for s in revised[:NUM_STORIES]:
                    revised_stories.append({
                        "title": s.get("title", "Untitled"),
                        "text": s.get("text", ""),
                        "subgenre": s.get("subgenre", "Sword & Sorcery"),
                    })
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
                print("ERROR: Sexual-content guardrail could not be satisfied after rewrites.", file=sys.stderr)
                for v in violations:
                    print(f" - Story #{v.get('index')+1}: {v.get('title')} — {', '.join(v.get('violations') or [])}", file=sys.stderr)
                sys.exit(1)
            print(f"\u26a0\ufe0f Sexual-content guardrail: rewriting {len(violations)} story(ies) (attempt {attempt+1}/{SEXUAL_CONTENT_MAX_REWRITES})...")
            rewrite_msg = client.messages.create(
                model=MODEL,
                max_tokens=4096,
                messages=[{"role": "user", "content": build_sexual_content_rewrite_prompt(stories, violations)}],
            )
            rewrite_raw = rewrite_msg.content[0].text.strip()
            try:
                revised = parse_json_response(rewrite_raw)
                revised_stories = []
                for s in revised[:NUM_STORIES]:
                    revised_stories.append({
                        "title": s.get("title", "Untitled"),
                        "text": s.get("text", ""),
                        "subgenre": s.get("subgenre", "Sword & Sorcery"),
                    })
                if len(revised_stories) == len(stories):
                    stories = revised_stories
            except (ValueError, json.JSONDecodeError) as e:
                print(f"WARNING: Could not parse sexual-safety rewrite JSON: {e}", file=sys.stderr)
                print("Continuing to next rewrite attempt with original stories.", file=sys.stderr)

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
            revised_stories = []
            for s in revised[:NUM_STORIES]:
                revised_stories.append({
                    "title": s.get("title", "Untitled"),
                    "text": s.get("text", ""),
                    "subgenre": s.get("subgenre", "Sword & Sorcery"),
                })
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
                revised_stories = []
                for s in revised[:NUM_STORIES]:
                    revised_stories.append({
                        "title": s.get("title", "Untitled"),
                        "text": s.get("text", ""),
                        "subgenre": s.get("subgenre", "Sword & Sorcery"),
                    })
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
                    revised = check_result.get("stories") if isinstance(check_result, dict) else None
                    if isinstance(revised, list) and revised:
                        revised_stories = []
                        for s in revised[:NUM_STORIES]:
                            revised_stories.append({
                                "title": s.get("title", "Untitled"),
                                "text": s.get("text", ""),
                                "subgenre": s.get("subgenre", "Sword & Sorcery"),
                            })
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

    # ── CALL 2: Extract new lore from generated stories ──────────────────
    print("Calling Claude to extract lore from new stories...")
    lore_message = client.messages.create(
        model=MODEL,
        max_tokens=4096,
        messages=[{
            "role": "user",
            "content": build_lore_extraction_prompt(stories, lore)
        }]
    )
    lore_raw = lore_message.content[0].text.strip()
    try:
        new_lore_raw = parse_json_response(lore_raw)
        new_lore = normalize_extracted_lore(new_lore_raw)
        new_lore = filter_lore_to_stories(new_lore, stories)
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
    except (ValueError, json.JSONDecodeError) as e:
        print(f"WARNING: Could not parse lore extraction JSON: {e}", file=sys.stderr)
        print("Continuing without updating lore.", file=sys.stderr)

    # ── Save today's stories.json ─────────────────────────────────────────
    output = {
        "date":         date_key,
        "generated_at": today.strftime("%Y-%m-%dT%H:%M:%SZ"),
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

if __name__ == "__main__":
    main()
