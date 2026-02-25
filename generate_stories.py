#!/usr/bin/env python3
"""
generate_stories.py
Calls the Claude API to generate 10 sword-and-sorcery stories
and saves them to stories.json. Run daily via GitHub Actions.
"""

import os
import json
import sys
from datetime import datetime, timezone
import anthropic

MODEL      = "claude-haiku-4-5-20251001"
NUM_STORIES = 10
OUTPUT_FILE = "stories.json"

SUBGENRES = [
    "Sword & Sorcery", "Dark Fantasy", "Lost World", "Barbarian Quest",
    "Ancient Curse", "Forbidden Tomb", "Witch Hunt", "Blood Oath",
    "Demon Pact", "War of Kings"
]

def build_prompt(today_str):
    return f"""You are a pulp fantasy writer in the tradition of Robert E. Howard, Clark Ashton Smith, and Fritz Leiber. Generate exactly 10 original short sword-and-sorcery stories. Each story should be vivid, action-packed, and around 120-160 words long.

Today's date is {today_str}. Use this as subtle creative inspiration if you like.

Respond with ONLY valid JSON - no prose before or after - matching this exact structure:
[
  {{ "title": "Story Title Here", "text": "Full story text here..." }},
  ...9 more entries...
]

Guidelines:
- Heroes and antiheroes with colorful names (barbarians, sell-swords, sorcerers, thieves)
- Vivid exotic settings: crumbling empires, cursed ruins, blasted steppes, sorcerous cities
- Stakes that feel epic: ancient evil, demonic pacts, dying gods, vengeful sorcery
- Each story must be complete with a beginning, conflict, and satisfying (or ironic) ending
- Vary protagonists, locations, and types of magic/conflict across all 10 stories
- Use dramatic, muscular prose - short punchy sentences mixed with lush description
- No two stories should share a protagonist or primary location"""


def main():
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("ERROR: ANTHROPIC_API_KEY environment variable not set.", file=sys.stderr)
        sys.exit(1)

    today = datetime.now(timezone.utc)
    today_str = today.strftime("%B %d, %Y")
    date_key  = today.strftime("%Y-%m-%d")

    print(f"Generating stories for {date_key}...")

    client = anthropic.Anthropic(api_key=api_key)

    message = client.messages.create(
        model=MODEL,
        max_tokens=4096,
        messages=[{"role": "user", "content": build_prompt(today_str)}]
    )

    raw = message.content[0].text.strip()

    if "```" in raw:
        import re
        m = re.search(r"```(?:json)?\s*([\s\S]*?)```", raw)
        if m:
            raw = m.group(1).strip()

    start = raw.find("[")
    end   = raw.rfind("]")
    if start == -1 or end == -1:
        print("ERROR: Could not find JSON array in response.", file=sys.stderr)
        sys.exit(1)

    stories_raw = json.loads(raw[start:end + 1])

    stories = []
    for i, s in enumerate(stories_raw[:NUM_STORIES]):
        stories.append({
            "title":   s.get("title", "Untitled"),
            "text":    s.get("text",  ""),
            "subgenre": SUBGENRES[i % len(SUBGENRES)]
        })

    output = {
        "date":         date_key,
        "generated_at": today.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "stories":      stories
    }

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"Saved {len(stories)} stories to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
