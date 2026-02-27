#!/usr/bin/env python3
"""audit_story.py

Server-side (GitHub Actions) story audit using Anthropic Haiku.

Given a date and exact story title, re-extract lore entities from that single story
and merge into codex.json using the same merge logic as the daily generator.

This is designed to be invoked via workflow_dispatch so the API key stays on the
server (GitHub Actions secrets), not in the browser.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any

import anthropic


STORIES_FILE = "stories.json"
ARCHIVE_DIR = "archive"
CODEX_FILE = "codex.json"


def _load_json(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_day_payload(date_key: str) -> dict:
    """Load the story-day payload (either today's stories.json or archive/<date>.json)."""
    archive_path = os.path.join(ARCHIVE_DIR, f"{date_key}.json")
    if os.path.exists(archive_path):
        data = _load_json(archive_path)
        if not isinstance(data, dict):
            raise ValueError(f"Invalid archive payload: {archive_path}")
        return data

    if os.path.exists(STORIES_FILE):
        data = _load_json(STORIES_FILE)
        if isinstance(data, dict) and str(data.get("date", "")).strip() == date_key:
            return data

    raise FileNotFoundError(
        f"Could not find stories for {date_key}. Expected {archive_path} or {STORIES_FILE} with date={date_key}."
    )


def find_story(day_payload: dict, title: str) -> dict:
    want = str(title or "").strip().casefold()
    if not want:
        raise ValueError("Title is required.")

    stories = day_payload.get("stories")
    if not isinstance(stories, list):
        raise ValueError("Invalid day payload: missing stories list.")

    for s in stories:
        if not isinstance(s, dict):
            continue
        t = str(s.get("title", "")).strip().casefold()
        if t == want:
            return s

    available = [str(s.get("title", "")).strip() for s in stories if isinstance(s, dict) and s.get("title")]
    sample = "\n".join(f"- {t}" for t in available[:40])
    raise ValueError(f"Story not found for title={title!r}. Available titles:\n{sample}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit a single story and merge extracted lore into codex.json")
    parser.add_argument("--date", required=True, help="Story issue date (YYYY-MM-DD)")
    parser.add_argument("--title", required=True, help="Exact story title")
    parser.add_argument("--max-tokens", type=int, default=4096, help="Max tokens for the audit extraction call")
    args = parser.parse_args()

    # Reuse generator logic for prompt + merge.
    import generate_stories as gs  # local import to keep module-level side effects minimal

    if hasattr(gs, "_maybe_load_dotenv"):
        gs._maybe_load_dotenv()  # type: ignore[attr-defined]

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("ERROR: ANTHROPIC_API_KEY environment variable not set.", file=sys.stderr)
        return 2

    date_key = str(args.date).strip()
    title = str(args.title).strip()

    day = load_day_payload(date_key)
    story = find_story(day, title)

    existing = _load_json(CODEX_FILE) if os.path.exists(CODEX_FILE) else {}
    prompt = gs.build_lore_extraction_prompt([story], existing)

    client = anthropic.Anthropic(api_key=api_key)
    resp = client.messages.create(
        model=getattr(gs, "MODEL", "claude-3-5-haiku-latest"),
        max_tokens=int(args.max_tokens),
        messages=[{"role": "user", "content": prompt}],
    )

    raw = resp.content[0].text.strip() if resp and resp.content else ""
    extracted = gs.parse_json_response(raw)
    lore = gs.normalize_extracted_lore(extracted)

    # Merge into codex.json (writes the file).
    gs.update_codex_file(lore, date_key=date_key, stories=[story])

    print(f"✓ Audit merged for {date_key} / {title}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
