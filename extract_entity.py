#!/usr/bin/env python3
"""extract_entity.py

Server-side (GitHub Actions) entity extraction from highlighted story text.

Given a date, story title, and the highlighted text fragment, this script:
  1. Loads the full story for context.
  2. Calls Haiku to classify the highlighted text into the correct codex category
     and produce a full entity record.
  3. Merges the result into codex.json, lore.json, and characters.json.

Designed to be invoked via workflow_dispatch through the audit-proxy /extract
endpoint so the API key stays on the server.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from typing import Any

import anthropic


CODEX_FILE = "codex.json"
STORIES_FILE = "stories.json"
ARCHIVE_DIR = "archive"

# All codex categories the model may classify into.
CODEX_CATEGORIES = [
    "characters", "places", "events", "rituals", "weapons", "artifacts",
    "factions", "lore", "flora_fauna", "magic", "relics", "regions",
    "substances", "polities", "hemispheres", "continents",
    "realms", "provinces", "districts",
]


def _load_json(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_day_payload(date_key: str) -> dict:
    """Load the story-day payload (archive or current stories.json)."""
    archive_path = os.path.join(ARCHIVE_DIR, f"{date_key}.json")
    if os.path.exists(archive_path):
        data = _load_json(archive_path)
        if isinstance(data, dict):
            return data

    if os.path.exists(STORIES_FILE):
        data = _load_json(STORIES_FILE)
        if isinstance(data, dict) and str(data.get("date", "")).strip() == date_key:
            return data

    raise FileNotFoundError(
        f"Could not find stories for {date_key}. "
        f"Expected {archive_path} or {STORIES_FILE} with date={date_key}."
    )


def find_story(day_payload: dict, title: str) -> dict:
    want = str(title or "").strip().casefold()
    stories = day_payload.get("stories")
    if not isinstance(stories, list):
        raise ValueError("Invalid day payload: missing stories list.")
    for s in stories:
        if not isinstance(s, dict):
            continue
        if str(s.get("title", "")).strip().casefold() == want:
            return s
    available = [str(s.get("title", "")).strip() for s in stories
                 if isinstance(s, dict) and s.get("title")]
    raise ValueError(
        f"Story not found for title={title!r}.\n"
        f"Available: {', '.join(available[:20])}"
    )


def build_extract_prompt(highlighted_text: str, story_title: str,
                         story_text: str, date_key: str) -> str:
    """Build the prompt that asks Haiku to classify a highlighted text fragment."""
    return f"""You are the world-bible keeper for a sword-and-sorcery serial called Daily Blade.

A developer has highlighted the following text from a story and wants it added to the codex.

HIGHLIGHTED TEXT:
\"\"\"{highlighted_text}\"\"\"

FULL STORY (for context):
Title: {story_title}
Date: {date_key}
---
{story_text}
---

Your task: Determine the BEST codex category for the highlighted text and produce a complete entity record.

AVAILABLE CATEGORIES:
- characters: Named people/beings (fields: name, tagline, role, status, world, bio, traits[], travel_scope, home_place, home_region, home_realm, aliases[])
- places: Named locations (fields: name, tagline, place_type, world, hemisphere, continent, realm, province, region, district, atmosphere, description, status, parent_place)
- events: Named events (fields: name, tagline, participants[], outcome)
- rituals: Named rituals (fields: name, tagline, type, description, cost)
- weapons: Named weapons (fields: name, tagline, type, description, status)
- artifacts: Named artifacts (fields: name, tagline, type, description, status)
- factions: Named groups/organizations (fields: name, tagline, type, status, description)
- lore: World-building concepts/legends (fields: name, tagline, description)
- flora_fauna: Creatures/plants (fields: name, tagline, type, rarity, habitat, status)
- magic: Named spells/magic systems (fields: name, tagline, type, description, status)
- relics: Ancient objects of power (fields: name, tagline, type, description, status)
- regions: Named geographic regions (fields: name, tagline, description, status)
- districts: Named intra-city wards/quarters/neighborhoods (fields: name, tagline, description, parent_place, province, region, function, status, notes)
- provinces: Named provinces (fields: name, tagline, description, realm, region, function, status, notes)
- realms: Named realms (fields: name, tagline, description, continent, capital, function, taxation, military, status, notes)
- continents: Continents (fields: name, tagline, description, hemispheres[], climate_zones[], function, status, notes)
- hemispheres: Hemispheres (fields: name, tagline, description, function, status, notes)
- substances: Named materials/substances (fields: name, tagline, type, description, status)
- polities: Kingdoms/empires/nations (fields: name, tagline, type, status, description, seat, realm, region)

Respond with ONLY valid JSON in this exact format:
{{
  "category": "<one of the categories above>",
  "entity": {{
    "name": "<proper name as it appears or should appear in the codex>",
    ... all relevant fields for that category ...
  }}
}}

Rules:
- Pick the single most appropriate category.
- The entity name should be the proper noun form (e.g. "The Drowned Hollows" not "drowned hollows").
- Fill in as many fields as the story context supports. Use "unknown" for fields you cannot determine.
- For characters, include aliases if the text uses a shortened form.
- Keep descriptions concise but informative (1-3 sentences).
- Do NOT wrap the JSON in markdown code fences.
"""


def _norm_name_key(text: str) -> str:
    return re.sub(r"\s+", " ", str(text or "").replace("’", "'").strip()).casefold()


def infer_explicit_character_from_story(highlighted_text: str, story_title: str,
                                        story_text: str, gs) -> dict | None:
    """Return a character entity when the highlighted text is an explicit named intro.

    This covers cases like:
    - "a young scholar named Brenn"
    - "Brenn, a young scholar"
    """
    highlighted = str(highlighted_text or "").strip()
    if not highlighted:
        return None

    if not hasattr(gs, "_extract_named_character_mentions"):
        return None

    mentions = gs._extract_named_character_mentions([
        {"title": story_title, "text": story_text}
    ])
    target = _norm_name_key(highlighted)
    if not target:
        return None

    for mention in mentions:
        name = str(mention.get("name") or "").strip()
        if _norm_name_key(name) != target:
            continue
        descriptor = str(mention.get("descriptor") or "character").strip() or "character"
        role = descriptor.title()
        alias_helper = getattr(gs, "_is_descriptor_placeholder_character_name", lambda _x: False)
        aliases = [] if alias_helper(descriptor) else ([descriptor] if descriptor.casefold() != name.casefold() else [])
        return {
            "category": "characters",
            "entity": {
                "name": name,
                "aliases": aliases,
                "tagline": "Named. Present. Emerging.",
                "role": role,
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
                "notes": "Auto-added from highlighted text plus story context.",
            },
        }
    return None


def parse_response(raw: str) -> dict:
    """Parse the JSON response from the model."""
    text = raw.strip()
    # Strip markdown fences if present.
    if text.startswith("```"):
        lines = text.split("\n")
        lines = [l for l in lines if not l.strip().startswith("```")]
        text = "\n".join(lines).strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        # Try to find JSON object in the text.
        start = text.find("{")
        end = text.rfind("}")
        if start >= 0 and end > start:
            try:
                return json.loads(text[start:end + 1])
            except json.JSONDecodeError:
                pass
    return {}


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Extract a single entity from highlighted story text"
    )
    parser.add_argument("--date", required=True, help="Story issue date (YYYY-MM-DD)")
    parser.add_argument("--title", required=True, help="Exact story title")
    parser.add_argument("--text", required=True, help="Highlighted text to categorize")
    parser.add_argument("--max-tokens", type=int, default=2048,
                        help="Max tokens for the extraction call")
    args = parser.parse_args()

    # Reuse generator logic for merge.
    import generate_stories as gs

    if hasattr(gs, "_maybe_load_dotenv"):
        gs._maybe_load_dotenv()

    date_key = str(args.date).strip()
    title = str(args.title).strip()
    highlighted = str(args.text).strip()

    if not highlighted:
        print("ERROR: --text is empty.", file=sys.stderr)
        return 1

    print(f"Extracting entity from highlighted text: {highlighted!r}")
    print(f"Story: {title} ({date_key})")

    # Load the full story for context.
    day = load_day_payload(date_key)
    story = find_story(day, title)
    story_text = str(story.get("text", "") or "")

    inferred = infer_explicit_character_from_story(highlighted, title, story_text, gs)
    if inferred is not None:
        category = inferred["category"]
        entity = inferred["entity"]
        raw = json.dumps(inferred, ensure_ascii=False)
        print(f"Detected explicit character from story context: {entity.get('name', '')}")
    else:
        api_key = os.environ.get("ANTHROPIC_API_KEY")
        if not api_key:
            print("ERROR: ANTHROPIC_API_KEY not set.", file=sys.stderr)
            return 2

        # Call Haiku.
        prompt = build_extract_prompt(highlighted, title, story_text, date_key)
        client = anthropic.Anthropic(api_key=api_key)
        model = getattr(gs, "MODEL", "claude-3-5-haiku-latest")

        print(f"Calling {model}...")
        resp = client.messages.create(
            model=model,
            max_tokens=int(args.max_tokens),
            messages=[{"role": "user", "content": prompt}],
        )

        raw = resp.content[0].text.strip() if resp and resp.content else ""
        if not raw:
            print("ERROR: Empty response from model.", file=sys.stderr)
            return 3

        result = parse_response(raw)
        category = str(result.get("category", "")).strip()
        entity = result.get("entity")

    if not category or category not in CODEX_CATEGORIES:
        print(f"ERROR: Invalid category: {category!r}", file=sys.stderr)
        print(f"Raw response:\n{raw[:2000]}", file=sys.stderr)
        return 3

    if not isinstance(entity, dict) or not entity.get("name"):
        print(f"ERROR: Invalid entity in response.", file=sys.stderr)
        print(f"Raw response:\n{raw[:2000]}", file=sys.stderr)
        return 3

    entity_name = str(entity["name"]).strip()
    print(f"Classified as: {category} -> {entity_name}")

    # Build a mini lore payload and merge using existing infrastructure.
    new_lore = {category: [entity]}

    # Tag with story appearance.
    entity["first_story"] = title
    entity["first_date"] = date_key
    entity["appearances"] = 1
    entity["story_appearances"] = [{"date": date_key, "title": title}]

    # Merge into lore.json.
    existing_lore = gs.load_lore()
    gs.merge_lore(existing_lore, new_lore, date_key)
    gs.save_lore(existing_lore, date_key)
    print(f"\u2713 Merged into lore.json")

    # Merge into codex.json.
    gs.update_codex_file(
        new_lore, date_key=date_key, stories=[story],
        assume_all_from_stories=True,
    )
    print(f"\u2713 Merged into codex.json")

    # Merge into characters.json if it's a character.
    if category == "characters" and hasattr(gs, "update_characters_file"):
        gs.update_characters_file(new_lore, date_key=date_key, stories=[story])
        print(f"\u2713 Merged into characters.json")

    # Verify it landed.
    try:
        codex = _load_json(CODEX_FILE) if os.path.exists(CODEX_FILE) else {}
        cat_list = codex.get(category, [])
        found = any(
            str(e.get("name", "")).strip().casefold() == entity_name.casefold()
            for e in cat_list if isinstance(e, dict)
        )
        if found:
            print(f"\u2713 Verified: {entity_name} exists in codex.json/{category}")
        else:
            print(f"WARNING: {entity_name} not found in codex after merge.", file=sys.stderr)
    except Exception as e:
        print(f"WARNING: Could not verify codex: {e}", file=sys.stderr)

    return 0


if __name__ == "__main__":
    sys.exit(main())
