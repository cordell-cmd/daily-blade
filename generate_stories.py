#!/usr/bin/env python3
"""
generate_stories.py
Calls the Claude API to generate 10 sword-and-sorcery stories,
saves them to stories.json (today's edition) and to archive/<date>.json.
Updates archive/index.json with the running list of available dates.
Run daily via GitHub Actions.
"""

import os
import json
import sys
from datetime import datetime, timezone
import anthropic

# ── Config ─────────────────────────────────────────────────────────────────
MODEL        = "claude-haiku-4-5-20251001"
NUM_STORIES  = 10
OUTPUT_FILE  = "stories.json"
ARCHIVE_DIR  = "archive"
ARCHIVE_IDX  = "archive/index.json"

SUBGENRES = [
    "Sword & Sorcery", "Dark Fantasy", "Lost World", "Barbarian Quest",
    "Ancient Curse", "Forbidden Tomb", "Witch Hunt", "Blood Oath",
    "Demon Pact", "War of Kings"
]

# ── Prompt ──────────────────────────────────────────────────────────────────
def build_prompt(today_str):
    return f"""You are a pulp fantasy writer in the tradition of Robert E. Howard, Clark Ashton Smith, and Fritz Leiber. Generate exactly 10 original short sword-and-sorcery stories. Each story should be vivid, action-packed, and around 120–160 words long.

Today's date is {today_str}. Use this as subtle creative inspiration if you like.

Respond with ONLY valid JSON — no prose before or after — matching this exact structure:
[
  {{ "title": "Story Title Here", "text": "Full story text here…" }},
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
- No two stories should share a protagonist or primary location"""


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
        json.dump(idx, f, ensure_ascii=False, indent=2)


# ── Main ────────────────────────────────────────────────────────────────────
def main():
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("ERROR: ANTHROPIC_API_KEY environment variable not set.", file=sys.stderr)
        sys.exit(1)

    today = datetime.now(timezone.utc)
    today_str = today.strftime("%B %d, %Y")
    date_key  = today.strftime("%Y-%m-%d")

    print(f"Generating stories for {date_key}…")

    client = anthropic.Anthropic(api_key=api_key)

    message = client.messages.create(
        model=MODEL,
        max_tokens=4096,
        messages=[{"role": "user", "content": build_prompt(today_str)}]
    )

    raw = message.content[0].text.strip()

    # Strip markdown code fences if present
    if "```" in raw:
        import re
        m = re.search(r"```(?:json)?\s*([\s\S]*?)```", raw)
        if m:
            raw = m.group(1).strip()

    # Find JSON array bounds
    start = raw.find("[")
    end   = raw.rfind("]")
    if start == -1 or end == -1:
        print("ERROR: Could not find JSON array in response.", file=sys.stderr)
        print("Raw response:", raw[:500], file=sys.stderr)
        sys.exit(1)

    stories_raw = json.loads(raw[start:end + 1])

    # Attach sub-genre labels
    stories = []
    for i, s in enumerate(stories_raw[:NUM_STORIES]):
        stories.append({
            "title":    s.get("title", "Untitled"),
            "text":     s.get("text",  ""),
            "subgenre": SUBGENRES[i % len(SUBGENRES)]
        })

    output = {
        "date":         date_key,
        "generated_at": today.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "stories":      stories
    }

    # ── Save today's stories.json ──────────────────────────────────────────
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    print(f"✔ Saved {len(stories)} stories to {OUTPUT_FILE}")

    # ── Save to archive/<date>.json ────────────────────────────────────────
    ensure_archive_dir()
    archive_file = os.path.join(ARCHIVE_DIR, f"{date_key}.json")
    with open(archive_file, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    print(f"✔ Archived to {archive_file}")

    # ── Update archive/index.json ──────────────────────────────────────────
    idx = load_archive_index()
    if date_key not in idx["dates"]:
        idx["dates"].insert(0, date_key)          # newest first
        idx["dates"].sort(reverse=True)           # keep sorted newest-first
    save_archive_index(idx)
    print(f"✔ Updated {ARCHIVE_IDX} ({len(idx['dates'])} dates total)")


if __name__ == "__main__":
    main()
