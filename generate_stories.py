#!/usr/bin/env python3
"""
generate_stories.py
Calls the Claude API to generate 10 sword-and-sorcery stories,
saves them to stories.json (today's edition) and to archive/<date>.json.
Updates archive/index.json with the running list of available dates.
Maintains lore.json (world bible) and characters.json (UI character list).
Run daily via GitHub Actions.
"""

import os
import json
import sys
import re
from datetime import datetime, timezone
import anthropic

# ── Config ─────────────────────────────────────────────────────────────────
MODEL           = "claude-haiku-4-5-20251001"
NUM_STORIES     = 10
OUTPUT_FILE     = "stories.json"
ARCHIVE_DIR     = "archive"
ARCHIVE_IDX     = "archive/index.json"
LORE_FILE       = "lore.json"
CHARACTERS_FILE = "characters.json"

# Subgenres are generated dynamically by the AI for each story


# ── Lore helpers ─────────────────────────────────────────────────────────────
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


# ── Story generation prompt ────────────────────────────────────────────────
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

    return f"""You are a pulp fantasy writer in the tradition of Robert E. Howard, Clark Ashton Smith, and Fritz Leiber. Generate exactly 10 original short sword-and-sorcery stories. Each story should be vivid, action-packed, and around 120–160 words long.

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


# ── Lore extraction prompt ─────────────────────────────────────────────────
def build_lore_extraction_prompt(stories, existing_lore):
    existing_names = {c["name"].lower() for c in existing_lore.get("characters", [])}
    existing_places = {p["name"].lower() for p in existing_lore.get("places", [])}
    existing_deities = {d["name"].lower() for d in existing_lore.get("deities_and_entities", [])}
    existing_artifacts = {a["name"].lower() for a in existing_lore.get("artifacts", [])}

    stories_text = "\n\n".join(
        f"STORY {i+1}: {s['title']}\n{s['text']}"
        for i, s in enumerate(stories)
    )

    return f"""You are a lore archivist for a sword-and-sorcery story universe. Analyze the following stories and extract NEW lore elements — characters, places, deities/entities, and artifacts that appear in these stories but are NOT already in the existing lore lists.

ALREADY KNOWN (do NOT re-extract these):
- Characters: {', '.join(existing_names) if existing_names else 'none'}
- Places: {', '.join(existing_places) if existing_places else 'none'}
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
      "role": "Role (e.g. Thief, Warlord, Sorceress)",
      "world": "known_world",
      "status": "active / dead / cursed / unknown / etc",
      "bio": "2-3 sentence bio based strictly on what appears in the story.",
      "traits": ["trait1", "trait2", "trait3"],
      "known_locations": ["place names mentioned"],
      "affiliations": ["groups or individuals"],
      "notes": "Any story hooks or unresolved threads."
      "tagline": "Three punchy evocative words. (e.g. Cursed. Reckless. Hunted.)"
    }}
  ],
  "places": [
    {{
      "id": "snake_case_id",
      "name": "Place Name",
      "world": "known_world",
      "description": "Description based on the story.",
      "status": "active / ruins / unknown / etc",
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
      "world": "known_world",
      "location": "where it is or was last seen",
      "description": "What it does, based on the story.",
      "status": "active / destroyed / sealed / lost",
      "notes": "Any hooks."
    }}
  ]
}}"""


# ── Lore merging ──────────────────────────────────────────────────────────
def merge_lore(existing_lore, new_lore, date_key):
    """Merge newly extracted lore into the existing lore, skipping duplicates by name."""
    for category in ["characters", "places", "deities_and_entities", "artifacts"]:
        existing_names = {
            item["name"].lower()
            for item in existing_lore.get(category, [])
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


# ── Characters file update ────────────────────────────────────────────────
def update_characters_file(lore, date_key, stories=None):
    """Merge today's lore characters into characters.json, preserving history."""
    stories = stories or []

    # ── Load existing characters ──────────────────────────────────────────────────────
    existing_chars = {}
    if os.path.exists(CHARACTERS_FILE):
        try:
            with open(CHARACTERS_FILE, "r", encoding="utf-8") as f:
                for ch in json.load(f).get("characters", []):
                    existing_chars[ch["name"].lower()] = ch
        except (json.JSONDecodeError, IOError):
            pass

    # ── Map each character to the specific stories they appear in today ───
    def stories_for(name):
        first = name.split()[0].lower()
        return [
            {"date": date_key, "title": s.get("title", "")}
            for s in stories
            if first in (s.get("text", "") + " " + s.get("title", "")).lower()
        ]

    # ── Merge new characters from today's lore ────────────────────────────────────
    for c in lore.get("characters", []):
        name     = c.get("name", "Unknown")
        name_low = name.lower()
        world    = next(
            (w["name"] for w in lore.get("worlds", []) if w["id"] == c.get("world")),
            c.get("world", "The Known World")
        )
        today_appearances = stories_for(name)

        if name_low in existing_chars:
            ex = existing_chars[name_low]
            # Update mutable fields with latest lore data
            ex["role"]   = c.get("role",   ex.get("role",   "Unknown"))
            ex["status"] = c.get("status", ex.get("status", "Unknown"))
            ex["world"]  = world
            ex["bio"]    = c.get("bio",    ex.get("bio",    ""))
            ex["traits"] = c.get("traits", ex.get("traits", []))
            # Preserve tagline; fill if blank and model provided one
            if c.get("tagline") and not ex.get("tagline"):
                ex["tagline"] = c["tagline"]
            # Append today's appearances if not already recorded
            prior    = ex.get("story_appearances", [])
            new_ones = [a for a in today_appearances
                        if not any(p["date"] == a["date"] and p["title"] == a["title"]
                                   for p in prior)]
            if new_ones:
                ex["appearances"]       = ex.get("appearances", 1) + len(new_ones)
                ex["story_appearances"] = prior + new_ones
        else:
            first_title = today_appearances[0]["title"] if today_appearances else ""
            existing_chars[name_low] = {
                "name":             name,
                "tagline":          c.get("tagline", ""),
                "role":             c.get("role",    "Unknown"),
                "status":           c.get("status",  "Unknown"),
                "world":            world,
                "bio":              c.get("bio",     ""),
                "traits":           c.get("traits",  []),
                "first_story":      first_title,
                "first_date":       date_key,
                "appearances":      len(today_appearances) or 1,
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


# ── Archive helpers ──────────────────────────────────────────────────────────
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


# ── Main ────────────────────────────────────────────────────────────────────
def main():
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("ERROR: ANTHROPIC_API_KEY environment variable not set.", file=sys.stderr)
        sys.exit(1)

    today     = datetime.now(timezone.utc)
    today_str = today.strftime("%B %d, %Y")
    date_key  = today.strftime("%Y-%m-%d")

    print(f"Generating stories for {date_key}…")

    # ── Load existing lore ──────────────────────────────────────────────────
    lore = load_lore()
    print(f"✔ Loaded lore ({len(lore.get('characters', []))} characters, "
          f"{len(lore.get('places', []))} places)")

    client = anthropic.Anthropic(api_key=api_key)

    # ── CALL 1: Generate stories with lore context ─────────────────────────
    print("Calling Claude to generate stories…")
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

    # Attach sub-genre labels and first_story field
    stories = []
    for i, s in enumerate(stories_raw[:NUM_STORIES]):
        stories.append({
            "title":    s.get("title", "Untitled"),
            "text":     s.get("text",  ""),
            "subgenre": s.get("subgenre", "Sword & Sorcery")
        })

    print(f"✔ Generated {len(stories)} stories")

    # ── CALL 2: Extract new lore from generated stories ────────────────────
    print("Calling Claude to extract lore from new stories…")
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
        # Tag new characters with story titles from the stories list
        # (best effort — we match by checking if name appears in story text)
        for char in new_lore.get("characters", []):
            for s in stories:
                if char["name"].lower() in s["text"].lower() or char["name"].lower() in s["title"].lower():
                    char["first_story"] = s["title"]
                    break
        lore = merge_lore(lore, new_lore, date_key)
        new_char_count  = len(new_lore.get("characters", []))
        new_place_count = len(new_lore.get("places", []))
        print(f"✔ Extracted {new_char_count} new characters, {new_place_count} new places")
    except (ValueError, json.JSONDecodeError) as e:
        print(f"WARNING: Could not parse lore extraction JSON: {e}", file=sys.stderr)
        print("Continuing without updating lore.", file=sys.stderr)

    # ── Save today's stories.json ──────────────────────────────────────────
    output = {
        "date":         date_key,
        "generated_at": today.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "stories":      stories
    }

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=True, indent=2)
    print(f"✔ Saved {len(stories)} stories to {OUTPUT_FILE}")

    # ── Save to archive/<date>.json ────────────────────────────────────────
    ensure_archive_dir()
    archive_file = os.path.join(ARCHIVE_DIR, f"{date_key}.json")
    with open(archive_file, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=True, indent=2)
    print(f"✔ Archived to {archive_file}")

    # ── Update archive/index.json ──────────────────────────────────────────
    idx = load_archive_index()
    if date_key not in idx["dates"]:
        idx["dates"].insert(0, date_key)
        idx["dates"].sort(reverse=True)
    save_archive_index(idx)
    print(f"✔ Updated {ARCHIVE_IDX} ({len(idx['dates'])} dates total)")

    # ── Save lore.json ─────────────────────────────────────────────────────
    save_lore(lore, date_key)
    print(f"✔ Saved {LORE_FILE} ({len(lore.get('characters',[]))} characters total)")

    # ── Update characters.json ─────────────────────────────────────────────
    update_characters_file(lore, date_key, stories)


if __name__ == "__main__":
    main()
