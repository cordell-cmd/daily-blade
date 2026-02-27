#!/usr/bin/env python3
"""
backfill_codex.py
One-time script to populate codex.json from all existing stories.

Reads stories.json (current batch) + all archive/<date>.json files,
calls the Claude API to extract characters, places, events, weapons,
and artifacts from every story, then merges results into codex.json.

Also migrates any existing characters.json data so nothing is lost.

Run manually via GitHub Actions (workflow_dispatch) or locally
"""

import os
import json
import sys
import re
from datetime import datetime, timezone
import anthropic

# ── Config ────────────────────────────────────────────────────────────────
MODEL           = "claude-haiku-4-5-20251001"
OUTPUT_FILE     = "stories.json"
ARCHIVE_DIR     = "archive"
ARCHIVE_IDX     = "archive/index.json"
LORE_FILE       = "lore.json"
CHARACTERS_FILE = "characters.json"
CODEX_FILE      = "codex.json"

# ── Helpers ───────────────────────────────────────────────────────────────
def parse_json_response(raw):
    """Strip markdown fences and extract JSON from a Claude response."""
    raw = raw.strip()
    if "```" in raw:
        m = re.search(r"```(?:json)?\s*([\s\S]*?)```", raw)
        if m:
            raw = m.group(1).strip()
    for open_ch, close_ch in [("{", "}"), ("[", "]")]:
        start = raw.find(open_ch)
        end   = raw.rfind(close_ch)
        if start != -1 and end != -1:
            return json.loads(raw[start:end + 1])
    raise ValueError("No JSON structure found in response")

def load_all_stories():
    """Load all stories from stories.json and every archive/<date>.json."""
    all_stories = []   # list of (date_key, story_dict)

    # Load current stories.json
    if os.path.exists(OUTPUT_FILE):
        with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        date_key = data.get("date", "unknown")
        for s in data.get("stories", []):
            all_stories.append((date_key, s))
        print(f"\u2713 Loaded {len(data.get('stories',[]))} stories from {OUTPUT_FILE} ({date_key})")

    # Load archive stories
    if os.path.exists(ARCHIVE_IDX):
        with open(ARCHIVE_IDX, "r", encoding="utf-8") as f:
            idx = json.load(f)
        for date_key in idx.get("dates", []):
            archive_file = os.path.join(ARCHIVE_DIR, f"{date_key}.json")
            # Skip if it's the same date as stories.json (already loaded)
            if os.path.exists(archive_file):
                with open(archive_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                stories = data.get("stories", [])
                # Avoid duplicating the current stories.json content
                archive_date = data.get("date", date_key)
                existing_dates = {d for d, _ in all_stories}
                if archive_date not in existing_dates:
                    for s in stories:
                        all_stories.append((archive_date, s))
                    print(f"  + {len(stories)} stories from archive/{date_key}.json")

    print(f"\u2713 Total: {len(all_stories)} stories across all dates")
    return all_stories

def build_extraction_prompt(stories_with_dates, existing_codex):
    """Build the extraction prompt for a batch of stories."""
    existing_chars     = {c["name"].lower() for c in existing_codex.get("characters", [])}
    existing_places    = {p["name"].lower() for p in existing_codex.get("places", [])}
    existing_events    = {e["name"].lower() for e in existing_codex.get("events", [])}
    existing_weapons   = {w["name"].lower() for w in existing_codex.get("weapons", [])}
    existing_artifacts   = {a["name"].lower() for a in existing_codex.get("artifacts",   [])}
    existing_factions    = {f["name"].lower() for f in existing_codex.get("factions",    [])}
    existing_lore_items  = {l["name"].lower() for l in existing_codex.get("lore",         [])}
    existing_flora_fauna = {x["name"].lower() for x in existing_codex.get("flora_fauna",  [])}
    existing_magic       = {m["name"].lower() for m in existing_codex.get("magic",        [])}
    existing_relics      = {r["name"].lower() for r in existing_codex.get("relics",       [])}
    existing_regions     = {g["name"].lower() for g in existing_codex.get("regions",      [])}
    existing_substances  = {s["name"].lower() for s in existing_codex.get("substances",   [])}

    stories_text = "\n\n".join(
        f"STORY (date: {date_key}): {s['title']}\n{s['text']}"
        for date_key, s in stories_with_dates
    )

    return f"""You are a lore archivist for a sword-and-sorcery story universe.
Analyze the following stories and extract ALL notable lore elements:
- Named characters (heroes, villains, sorcerers, warlords, etc.)
- Named places (cities, ruins, temples, fortresses, wildernesses)
- Notable events (battles, wars, rituals, catastrophes — things that have a name or are referenced as a historical event)
- Named mythical/legendary weapons (swords, axes, staves with special names or powers)
- Named artifacts and magical objects (rings, tomes, idols, amulets, etc.)

ALREADY KNOWN (do NOT re-extract these — only extract truly NEW ones):
- Characters: {', '.join(sorted(existing_chars)) if existing_chars else 'none yet'}
- Places: {', '.join(sorted(existing_places)) if existing_places else 'none yet'}
- Events: {', '.join(sorted(existing_events)) if existing_events else 'none yet'}
- Weapons: {', '.join(sorted(existing_weapons)) if existing_weapons else 'none yet'}
- Artifacts: {', '.join(sorted(existing_artifacts)) if existing_artifacts else 'none yet'}
- Factions: {', '.join(sorted(existing_factions)) if existing_factions else 'none yet'}
- Lore & Legends: {', '.join(sorted(existing_lore_items)) if existing_lore_items else 'none yet'}
- Flora & Fauna: {', '.join(sorted(existing_flora_fauna)) if existing_flora_fauna else 'none yet'}
- Magic & Abilities: {', '.join(sorted(existing_magic)) if existing_magic else 'none yet'}
- Relics & Cursed Items: {', '.join(sorted(existing_relics)) if existing_relics else 'none yet'}
- Regions & Realms: {', '.join(sorted(existing_regions)) if existing_regions else 'none yet'}
- Substances & Materials: {', '.join(sorted(existing_substances)) if existing_substances else 'none yet'}

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
      "world": "The Known World",
      "status": "active / dead / cursed / unknown / etc",
      "bio": "2-3 sentence bio based strictly on what appears in the story.",
      "traits": ["trait1", "trait2", "trait3"],
      "first_story": "Exact story title where they first appear",
      "notes": "Any story hooks or unresolved threads."
    }}
  ],
  "places": [
    {{
      "id": "snake_case_id",
      "name": "Place Name",
      "tagline": "Three evocative words describing this place.",
      "place_type": "city / fortress / ruin / temple / wilderness / etc",
      "world": "The Known World",
      "atmosphere": "One sentence mood/tone description.",
      "description": "Description based on the story.",
      "status": "active / ruins / unknown / etc",
      "first_story": "Exact story title where it first appears",
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
      "first_story": "Exact story title where it first appears",
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
      "first_story": "Exact story title where it first appears",
      "notes": "Any story hooks."
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
      "first_story": "Exact story title where it first appears",
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
      "first_story": "Exact story title where it first appears",
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
      "first_story": "Exact story title where it first appears",
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
      "first_story": "Exact story title where it first appears",
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
      "first_story": "Exact story title where it first appears",
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
      "first_story": "Exact story title where it first appears",
      "notes": "Any hooks."
    }}
  ],
  "regions": [
    {{
      "id": "snake_case_id",
      "name": "Region or Realm Name",
      "tagline": "Three evocative words.",
      "climate": "arctic / desert / temperate / volcanic / blighted / etc",
      "terrain": "mountains / forest / plains / sea / ruins / etc",
      "ruler": "Who controls it.",
      "status": "stable / contested / fallen / cursed",
      "first_story": "Exact story title where it first appears",
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
      "status": "available / rare / depleted",
      "first_story": "Exact story title where it first appears",
      "notes": "Any hooks."
    }}
  ]
}}"""

def merge_into_codex(codex, new_entities, stories_by_title, date_key):
    """Merge extracted entities into the codex, building story_appearances."""

    def ensure_min_appearance(story_appearances, first_story_title, first_date):
        if story_appearances and isinstance(story_appearances, list) and len(story_appearances) > 0:
            return story_appearances
        if first_story_title:
            return [{"date": first_date or date_key, "title": first_story_title}]
        return []

    def find_story_date(story_title):
        """Find the date for a given story title."""
        for d, s in stories_by_title:
            if s.get("title", "").strip().lower() == story_title.strip().lower():
                return d
        return date_key

    def stories_for_entity(name):
        """Find all stories that mention this entity, skipping leading articles."""
        _SKIP = {'the', 'a', 'an'}
        _words = [w.strip('()[].,!?') for w in name.lower().split()]
        _sig   = [w for w in _words if w and w not in _SKIP]
        # Use a two-word phrase if possible (more specific), else one word
        if len(_sig) >= 2:
            _key = _sig[0] + ' ' + _sig[1]
        else:
            _key = _sig[0] if _sig else (_words[0] if _words else name.lower())
        matches = []
        seen = set()
        for d, s in stories_by_title:
            key = (d, s.get("title", ""))
            if key not in seen:
                text = (s.get("text", "") + " " + s.get("title", "")).lower()
                if _key in text:
                    matches.append({"date": d, "title": s.get("title", "")})
                    seen.add(key)
        return matches

    # ── Characters ───────────────────────────────────────────────────────
    existing_chars = {c["name"].lower(): c for c in codex.get("characters", [])}
    for c in new_entities.get("characters", []):
        name = c.get("name", "Unknown")
        name_low = name.lower()
        first_story_title = c.get("first_story", "")
        first_date = find_story_date(first_story_title)
        story_appearances = ensure_min_appearance(stories_for_entity(name), first_story_title, first_date)

        if name_low not in existing_chars:
            existing_chars[name_low] = {
                "name":              name,
                "tagline":           c.get("tagline", ""),
                "role":              c.get("role", "Unknown"),
                "status":            c.get("status", "Unknown"),
                "world":             c.get("world", "The Known World"),
                "bio":               c.get("bio", ""),
                "traits":            c.get("traits", []),
                "first_story":       first_story_title,
                "first_date":        first_date,
                "appearances":       len(story_appearances) or 1,
                "story_appearances": story_appearances,
            }
    codex["characters"] = list(existing_chars.values())

    # ── Places ───────────────────────────────────────────────────────────
    existing_places = {p["name"].lower(): p for p in codex.get("places", [])}
    for p in new_entities.get("places", []):
        name = p.get("name", "Unknown")
        name_low = name.lower()
        first_story_title = p.get("first_story", "")
        first_date = find_story_date(first_story_title)
        story_appearances = ensure_min_appearance(stories_for_entity(name), first_story_title, first_date)

        if name_low not in existing_places:
            existing_places[name_low] = {
                "name":              name,
                "tagline":           p.get("tagline", ""),
                "place_type":        p.get("place_type", ""),
                "world":             p.get("world", "The Known World"),
                "atmosphere":        p.get("atmosphere", ""),
                "description":       p.get("description", ""),
                "status":            p.get("status", "unknown"),
                "first_story":       first_story_title,
                "first_date":        first_date,
                "appearances":       len(story_appearances) or 1,
                "story_appearances": story_appearances,
            }
    codex["places"] = list(existing_places.values())

    # ── Events ───────────────────────────────────────────────────────────
    existing_events = {e["name"].lower(): e for e in codex.get("events", [])}
    for e in new_entities.get("events", []):
        name = e.get("name", "Unknown")
        name_low = name.lower()
        first_story_title = e.get("first_story", "")
        first_date = find_story_date(first_story_title)
        story_appearances = ensure_min_appearance(stories_for_entity(name), first_story_title, first_date)

        if name_low not in existing_events:
            existing_events[name_low] = {
                "name":              name,
                "tagline":           e.get("tagline", ""),
                "event_type":        e.get("event_type", ""),
                "participants":      e.get("participants", []),
                "outcome":           e.get("outcome", ""),
                "significance":      e.get("significance", ""),
                "first_story":       first_story_title,
                "first_date":        first_date,
                "appearances":       len(story_appearances) or 1,
                "story_appearances": story_appearances,
            }
    codex["events"] = list(existing_events.values())

    # ── Weapons ──────────────────────────────────────────────────────────
    existing_weapons = {w["name"].lower(): w for w in codex.get("weapons", [])}
    for w in new_entities.get("weapons", []):
        name = w.get("name", "Unknown")
        name_low = name.lower()
        first_story_title = w.get("first_story", "")
        first_date = find_story_date(first_story_title)
        story_appearances = ensure_min_appearance(stories_for_entity(name), first_story_title, first_date)

        if name_low not in existing_weapons:
            existing_weapons[name_low] = {
                "name":              name,
                "tagline":           w.get("tagline", ""),
                "weapon_type":       w.get("weapon_type", ""),
                "origin":            w.get("origin", ""),
                "powers":            w.get("powers", ""),
                "last_known_holder": w.get("last_known_holder", ""),
                "status":            w.get("status", "unknown"),
                "first_story":       first_story_title,
                "first_date":        first_date,
                "appearances":       len(story_appearances) or 1,
                "story_appearances": story_appearances,
            }
    codex["weapons"] = list(existing_weapons.values())

    # ── Artifacts ────────────────────────────────────────────────────────
    existing_artifacts = {a["name"].lower(): a for a in codex.get("artifacts", [])}
    for a in new_entities.get("artifacts", []):
        name = a.get("name", "Unknown")
        name_low = name.lower()
        first_story_title = a.get("first_story", "")
        first_date = find_story_date(first_story_title)
        story_appearances = ensure_min_appearance(stories_for_entity(name), first_story_title, first_date)

        if name_low not in existing_artifacts:
            existing_artifacts[name_low] = {
                "name":              name,
                "tagline":           a.get("tagline", ""),
                "artifact_type":     a.get("artifact_type", ""),
                "origin":            a.get("origin", ""),
                "powers":            a.get("powers", ""),
                "last_known_holder": a.get("last_known_holder", ""),
                "status":            a.get("status", "unknown"),
                "first_story":       first_story_title,
                "first_date":        first_date,
                "appearances":       len(story_appearances) or 1,
                "story_appearances": story_appearances,
            }
    codex["artifacts"] = list(existing_artifacts.values())
    # ── Factions ──────────────────────────────────────────────
    existing_factions = {x["name"].lower(): x for x in codex.get("factions", [])}
    for f in new_entities.get("factions", []):
        name = f.get("name", "Unknown")
        name_low = name.lower()
        first_story_title = f.get("first_story", "")
        first_date = find_story_date(first_story_title)
        story_appearances = ensure_min_appearance(stories_for_entity(name), first_story_title, first_date)
        if name_low not in existing_factions:
            existing_factions[name_low] = {
                "name":              name,
                "tagline":           f.get("tagline", ""),
                "alignment":          f.get("alignment", ""),
                "goals":              f.get("goals", ""),
                "leader":             f.get("leader", ""),
                "status":            f.get("status", "unknown"),
                "first_story":       first_story_title,
                "first_date":        first_date,
                "appearances":       len(story_appearances) or 1,
                "story_appearances": story_appearances,
            }
    codex["factions"] = list(existing_factions.values())

    # ── Lore ──────────────────────────────────────────────────
    existing_lore = {x["name"].lower(): x for x in codex.get("lore", [])}
    for lo in new_entities.get("lore", []):
        name = lo.get("name", "Unknown")
        name_low = name.lower()
        first_story_title = lo.get("first_story", "")
        first_date = find_story_date(first_story_title)
        story_appearances = ensure_min_appearance(stories_for_entity(name), first_story_title, first_date)
        if name_low not in existing_lore:
            existing_lore[name_low] = {
                "name":              name,
                "tagline":           lo.get("tagline", ""),
                "category":           lo.get("category", ""),
                "source":             lo.get("source", ""),
                "status":            lo.get("status", "unknown"),
                "first_story":       first_story_title,
                "first_date":        first_date,
                "appearances":       len(story_appearances) or 1,
                "story_appearances": story_appearances,
            }
    codex["lore"] = list(existing_lore.values())

    # ── Flora_fauna ───────────────────────────────────────────
    existing_flora_fauna = {x["name"].lower(): x for x in codex.get("flora_fauna", [])}
    for ff in new_entities.get("flora_fauna", []):
        name = ff.get("name", "Unknown")
        name_low = name.lower()
        first_story_title = ff.get("first_story", "")
        first_date = find_story_date(first_story_title)
        story_appearances = ensure_min_appearance(stories_for_entity(name), first_story_title, first_date)
        if name_low not in existing_flora_fauna:
            existing_flora_fauna[name_low] = {
                "name":              name,
                "tagline":           ff.get("tagline", ""),
                "type":               ff.get("type", ""),
                "rarity":             ff.get("rarity", ""),
                "habitat":            ff.get("habitat", ""),
                "status":            ff.get("status", "unknown"),
                "first_story":       first_story_title,
                "first_date":        first_date,
                "appearances":       len(story_appearances) or 1,
                "story_appearances": story_appearances,
            }
    codex["flora_fauna"] = list(existing_flora_fauna.values())

    # ── Magic ─────────────────────────────────────────────────
    existing_magic = {x["name"].lower(): x for x in codex.get("magic", [])}
    for mg in new_entities.get("magic", []):
        name = mg.get("name", "Unknown")
        name_low = name.lower()
        first_story_title = mg.get("first_story", "")
        first_date = find_story_date(first_story_title)
        story_appearances = ensure_min_appearance(stories_for_entity(name), first_story_title, first_date)
        if name_low not in existing_magic:
            existing_magic[name_low] = {
                "name":              name,
                "tagline":           mg.get("tagline", ""),
                "type":               mg.get("type", ""),
                "element":            mg.get("element", ""),
                "difficulty":         mg.get("difficulty", ""),
                "status":            mg.get("status", "unknown"),
                "first_story":       first_story_title,
                "first_date":        first_date,
                "appearances":       len(story_appearances) or 1,
                "story_appearances": story_appearances,
            }
    codex["magic"] = list(existing_magic.values())

    # ── Relics ────────────────────────────────────────────────
    existing_relics = {x["name"].lower(): x for x in codex.get("relics", [])}
    for rl in new_entities.get("relics", []):
        name = rl.get("name", "Unknown")
        name_low = name.lower()
        first_story_title = rl.get("first_story", "")
        first_date = find_story_date(first_story_title)
        story_appearances = ensure_min_appearance(stories_for_entity(name), first_story_title, first_date)
        if name_low not in existing_relics:
            existing_relics[name_low] = {
                "name":              name,
                "tagline":           rl.get("tagline", ""),
                "origin":             rl.get("origin", ""),
                "power":              rl.get("power", ""),
                "curse":              rl.get("curse", ""),
                "status":            rl.get("status", "unknown"),
                "first_story":       first_story_title,
                "first_date":        first_date,
                "appearances":       len(story_appearances) or 1,
                "story_appearances": story_appearances,
            }
    codex["relics"] = list(existing_relics.values())

    # ── Regions ───────────────────────────────────────────────
    existing_regions = {x["name"].lower(): x for x in codex.get("regions", [])}
    for rg in new_entities.get("regions", []):
        name = rg.get("name", "Unknown")
        name_low = name.lower()
        first_story_title = rg.get("first_story", "")
        first_date = find_story_date(first_story_title)
        story_appearances = ensure_min_appearance(stories_for_entity(name), first_story_title, first_date)
        if name_low not in existing_regions:
            existing_regions[name_low] = {
                "name":              name,
                "tagline":           rg.get("tagline", ""),
                "climate":            rg.get("climate", ""),
                "terrain":            rg.get("terrain", ""),
                "ruler":              rg.get("ruler", ""),
                "status":            rg.get("status", "unknown"),
                "first_story":       first_story_title,
                "first_date":        first_date,
                "appearances":       len(story_appearances) or 1,
                "story_appearances": story_appearances,
            }
    codex["regions"] = list(existing_regions.values())

    # ── Substances ────────────────────────────────────────────
    existing_substances = {x["name"].lower(): x for x in codex.get("substances", [])}
    for sub in new_entities.get("substances", []):
        name = sub.get("name", "Unknown")
        name_low = name.lower()
        first_story_title = sub.get("first_story", "")
        first_date = find_story_date(first_story_title)
        story_appearances = ensure_min_appearance(stories_for_entity(name), first_story_title, first_date)
        if name_low not in existing_substances:
            existing_substances[name_low] = {
                "name":              name,
                "tagline":           sub.get("tagline", ""),
                "type":               sub.get("type", ""),
                "rarity":             sub.get("rarity", ""),
                "properties":         sub.get("properties", ""),
                "use":                sub.get("use", ""),
                "status":            sub.get("status", "unknown"),
                "first_story":       first_story_title,
                "first_date":        first_date,
                "appearances":       len(story_appearances) or 1,
                "story_appearances": story_appearances,
            }
    codex["substances"] = list(existing_substances.values())

    return codex

def migrate_characters_json(codex):
    """Migrate any entries from characters.json that aren't already in codex."""
    if not os.path.exists(CHARACTERS_FILE):
        return codex

    with open(CHARACTERS_FILE, "r", encoding="utf-8") as f:
        chars_data = json.load(f)

    existing_names = {c["name"].lower() for c in codex.get("characters", [])}
    migrated = 0
    for c in chars_data.get("characters", []):
        if c.get("name", "").lower() not in existing_names:
            codex.setdefault("characters", []).append(c)
            existing_names.add(c["name"].lower())
            migrated += 1

    if migrated:
        print(f"\u2713 Migrated {migrated} characters from {CHARACTERS_FILE}")
    return codex

# ── Main ──────────────────────────────────────────────────────────────────
def main():
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("ERROR: ANTHROPIC_API_KEY environment variable not set.", file=sys.stderr)
        sys.exit(1)

    today    = datetime.now(timezone.utc)
    date_key = today.strftime("%Y-%m-%d")

    print("=" * 60)
    print("CODEX BACKFILL")
    print("=" * 60)

    # Load all stories
    all_stories = load_all_stories()
    if not all_stories:
        print("No stories found. Exiting.", file=sys.stderr)
        sys.exit(1)

    # Initialize empty codex
    codex = {
        "last_updated": date_key,
        "characters":   [],
        "places":       [],
        "events":       [],
        "weapons":      [],
        "artifacts":    [],
        "factions":     [],
        "lore":         [],
        "flora_fauna":  [],
        "magic":        [],
        "relics":       [],
        "regions":      [],
        "substances":   [],
    }

    # First: migrate existing characters.json data
    codex = migrate_characters_json(codex)

    client = anthropic.Anthropic(api_key=api_key)

    # Process stories in batches of 5 to stay within token limits
    BATCH_SIZE = 2
    batches = [all_stories[i:i+BATCH_SIZE] for i in range(0, len(all_stories), BATCH_SIZE)]
    print(f"\nProcessing {len(all_stories)} stories in {len(batches)} batches of up to {BATCH_SIZE}...")

    for batch_num, batch in enumerate(batches, 1):
        batch_titles = [s.get("title", "?") for _, s in batch]
        print(f"\n[Batch {batch_num}/{len(batches)}] Stories: {batch_titles}")

        prompt = build_extraction_prompt(batch, codex)

        try:
            resp = client.messages.create(
                model=MODEL,
                max_tokens=4096,
                messages=[{"role": "user", "content": prompt}]
            )
            raw = resp.content[0].text.strip()
            new_entities = parse_json_response(raw)

            n_chars  = len(new_entities.get("characters", []))
            n_places = len(new_entities.get("places", []))
            n_events = len(new_entities.get("events", []))
            n_weaps  = len(new_entities.get("weapons", []))
            n_arts   = len(new_entities.get("artifacts", []))
            print(f"  Extracted: {n_chars} chars, {n_places} places, "
                  f"{n_events} events, {n_weaps} weapons, {n_arts} artifacts")

            codex = merge_into_codex(codex, new_entities, all_stories, date_key)

        except (ValueError, json.JSONDecodeError) as e:
            print(f"  WARNING: Could not parse batch {batch_num}: {e}", file=sys.stderr)
            continue
        except Exception as e:
            print(f"  ERROR in batch {batch_num}: {e}", file=sys.stderr)
            continue

    # Final codex summary
    codex["last_updated"] = date_key
    print(f"\n{'='*60}")
    print(f"FINAL CODEX TOTALS:")
    print(f"  Characters: {len(codex['characters'])}")
    print(f"  Places:     {len(codex['places'])}")
    print(f"  Events:     {len(codex['events'])}")
    print(f"  Weapons:    {len(codex['weapons'])}")
    print(f"  Artifacts:  {len(codex['artifacts'])}")
    print(f"{'='*60}")

    # Save codex.json
    with open(CODEX_FILE, "w", encoding="utf-8") as f:
        json.dump(codex, f, ensure_ascii=True, indent=2)
    print(f"\n\u2713 Saved {CODEX_FILE}")

if __name__ == "__main__":
    main()
