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
from datetime import datetime, timezone
import anthropic

# ── Config ────────────────────────────────────────────────────────────────
MODEL           = "claude-haiku-4-5-20251001"
NUM_STORIES     = 10
OUTPUT_FILE     = "stories.json"
ARCHIVE_DIR     = "archive"
ARCHIVE_IDX     = "archive/index.json"
LORE_FILE       = "lore.json"
CHARACTERS_FILE = "characters.json"
CODEX_FILE      = "codex.json"

# Subgenres are generated dynamically by the AI for each story

# ── Lore helpers ──────────────────────────────────────────────────────────
def load_lore():
    """Load the existing lore bible, or return a minimal skeleton."""
    if os.path.exists(LORE_FILE):
        with open(LORE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {
        "version": "1.0",
        "worlds": [],
        "characters": [],
        "places": [],
        "events": [],
        "weapons": [],
        "deities_and_entities": [],
        "artifacts": []
    }

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

# ── Story generation prompt ──────────────────────────────────────────────
def build_prompt(today_str, lore):
    lore_context = build_lore_context(lore)
    lore_section = ""
    if lore_context.strip():
        lore_section = f"""
EXISTING LORE — READ CAREFULLY BEFORE WRITING:
{lore_context}

LORE CONSISTENCY RULES:
- If you use an existing character name, their personality, status, and background must match the established lore above.
- If you use an existing place name, its geography, atmosphere, and known history must be consistent with established lore.
- Do not contradict established lore rules (magic costs, shadow-magic, etc.).
- You MAY introduce entirely new characters, places, and entities — but they must fit the world's tone and rules.
- Stories may share the same world but use different characters and locations.
- World-crossing events (characters moving between worlds) are extremely rare and require major magical cause.
"""
    return f"""You are a pulp fantasy writer in the tradition of Robert E. Howard, Clark Ashton Smith, and Fritz Leiber.
Generate exactly 10 original short sword-and-sorcery stories.
Each story should be vivid, action-packed, and around 120–160 words long.

Today's date is {today_str}. Use this as subtle creative inspiration if you like.
{lore_section}
Respond with ONLY valid JSON — no prose before or after — matching this exact structure:
[
  {{ "title": "Story Title Here", "subgenre": "Two or Three Word Label", "text": "Full story text here…" }},
  …9 more entries…
]

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

# ── Lore extraction prompt ───────────────────────────────────────────────
def build_lore_extraction_prompt(stories, existing_lore):
    existing_chars     = {c["name"].lower() for c in existing_lore.get("characters", [])}
    existing_places    = {p["name"].lower() for p in existing_lore.get("places", [])}
    existing_events    = {e["name"].lower() for e in existing_lore.get("events", [])}
    existing_weapons   = {w["name"].lower() for w in existing_lore.get("weapons", [])}
    existing_deities   = {d["name"].lower() for d in existing_lore.get("deities_and_entities", [])}
    existing_artifacts = {a["name"].lower() for a in existing_lore.get("artifacts", [])}

    stories_text = "\n\n".join(
        f"STORY {i+1}: {s['title']}\n{s['text']}"
        for i, s in enumerate(stories)
    )

    return f"""You are a lore archivist for a sword-and-sorcery story universe.
Analyze the following stories and extract NEW lore elements — characters, places, notable events,
mythical weapons, and artifacts that appear in these stories but are NOT already in the existing lore lists.

ALREADY KNOWN (do NOT re-extract these):
- Characters: {', '.join(existing_chars) if existing_chars else 'none'}
- Places: {', '.join(existing_places) if existing_places else 'none'}
- Events: {', '.join(existing_events) if existing_events else 'none'}
- Weapons: {', '.join(existing_weapons) if existing_weapons else 'none'}
- Deities/Entities: {', '.join(existing_deities) if existing_deities else 'none'}
- Artifacts: {', '.join(existing_artifacts) if existing_artifacts else 'none'}

STORIES TO ANALYZE:
{stories_text}

Respond with ONLY valid JSON in this exact structure (use empty arrays if nothing new was found):
{{
  "characters": [
    {{
      "id": "snake_case_id",
      "name": "Full Name",
      "tagline": "Three punchy evocative words. (e.g. Cursed. Reckless. Hunted.)",
      "role": "Role (e.g. Thief, Warlord, Sorceress)",
      "world": "known_world",
      "status": "active / dead / cursed / unknown / etc",
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
      "participants": ["character or faction names involved"],
      "outcome": "What happened — who won or lost, what changed.",
      "significance": "Why this matters to the world.",
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
  ]
}}"""

# ── Lore merging ────────────────────────────────────────────────────────
def merge_lore(existing_lore, new_lore, date_key):
    """Merge newly extracted lore into the existing lore, skipping duplicates by name."""
    for category in ["characters", "places", "events", "weapons", "deities_and_entities", "artifacts"]:
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
    return existing_lore

# ── Codex file update ────────────────────────────────────────────────────
def update_codex_file(lore, date_key, stories=None):
    """Merge today's lore into codex.json, covering all entity types with story appearances."""
    stories = stories or []

    # ── Load existing codex ──────────────────────────────────────────────
    codex = {
        "last_updated": date_key,
        "characters": [],
        "places": [],
        "events": [],
        "weapons": [],
        "artifacts": [],
    }
    if os.path.exists(CODEX_FILE):
        try:
            with open(CODEX_FILE, "r", encoding="utf-8") as f:
                codex = json.load(f)
        except (json.JSONDecodeError, IOError):
            pass

    # ── Helper: find stories that mention an entity by name ─────────────
    def stories_for(name):
        first = name.split()[0].lower()
        return [
            {"date": date_key, "title": s.get("title", "")}
            for s in stories
            if first in (s.get("text", "") + " " + s.get("title", "")).lower()
        ]

    # ── Helper: resolve world name from lore worlds list ─────────────────
    def resolve_world(raw_world):
        return next(
            (w["name"] for w in lore.get("worlds", []) if w["id"] == raw_world),
            raw_world or "The Known World"
        )

    # ── Merge characters ─────────────────────────────────────────────────
    existing_chars = {c["name"].lower(): c for c in codex.get("characters", [])}
    for c in lore.get("characters", []):
        name = c.get("name", "Unknown")
        name_low = name.lower()
        world = resolve_world(c.get("world", ""))
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
    codex["characters"] = list(existing_chars.values())

    # ── Merge places ─────────────────────────────────────────────────────
    existing_places = {p["name"].lower(): p for p in codex.get("places", [])}
    for p in lore.get("places", []):
        name = p.get("name", "Unknown")
        name_low = name.lower()
        today_appearances = stories_for(name)
        if name_low in existing_places:
            ex = existing_places[name_low]
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

    codex["last_updated"] = date_key
    with open(CODEX_FILE, "w", encoding="utf-8") as f:
        json.dump(codex, f, ensure_ascii=True, indent=2)
    print(
        f"\u2713 Saved {CODEX_FILE} ("
        f"{len(codex['characters'])} chars, "
        f"{len(codex['places'])} places, "
        f"{len(codex['events'])} events, "
        f"{len(codex['weapons'])} weapons, "
        f"{len(codex['artifacts'])} artifacts)"
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
    # Find outermost [ ] for arrays or { } for objects
    for open_ch, close_ch in [("[", "]"), ("{", "}")]:
        start = raw.find(open_ch)
        end   = raw.rfind(close_ch)
        if start != -1 and end != -1:
            return json.loads(raw[start:end + 1])
    raise ValueError("No JSON structure found in response")

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

# ── Main ──────────────────────────────────────────────────────────────────
def main():
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("ERROR: ANTHROPIC_API_KEY environment variable not set.", file=sys.stderr)
        sys.exit(1)

    today     = datetime.now(timezone.utc)
    today_str = today.strftime("%B %d, %Y")
    date_key  = today.strftime("%Y-%m-%d")
    print(f"Generating stories for {date_key}...")

    # ── Load existing lore ────────────────────────────────────────────────
    lore = load_lore()
    print(f"\u2713 Loaded lore ({len(lore.get('characters', []))} characters, "
          f"{len(lore.get('places', []))} places)")

    client = anthropic.Anthropic(api_key=api_key)

    # ── CALL 1: Generate stories with lore context ───────────────────────
    print("Calling Claude to generate stories...")
    message = client.messages.create(
        model=MODEL,
        max_tokens=4096,
        messages=[{"role": "user", "content": build_prompt(today_str, lore)}]
    )
    raw = message.content[0].text.strip()
    try:
        stories_raw = parse_json_response(raw)
    except (ValueError, json.JSONDecodeError) as e:
        print(f"ERROR: Could not parse story JSON: {e}", file=sys.stderr)
        print("Raw response:", raw[:500], file=sys.stderr)
        sys.exit(1)

    # Attach sub-genre labels
    stories = []
    for i, s in enumerate(stories_raw[:NUM_STORIES]):
        stories.append({
            "title":    s.get("title",    "Untitled"),
            "text":     s.get("text",     ""),
            "subgenre": s.get("subgenre", "Sword & Sorcery")
        })
    print(f"\u2713 Generated {len(stories)} stories")

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
        new_lore = parse_json_response(lore_raw)
        for char in new_lore.get("characters", []):
            for s in stories:
                if char["name"].lower() in s["text"].lower() or char["name"].lower() in s["title"].lower():
                    char["first_story"] = s["title"]
                    break
        lore = merge_lore(lore, new_lore, date_key)
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
