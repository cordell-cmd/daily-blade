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


def _story_key(date_key: str, title: str) -> tuple[str, str]:
    return (str(date_key or "").strip(), str(title or "").strip())


def _iter_named_entities(payload: Any):
    """Yield (category, name) pairs from an extracted lore payload."""
    if not isinstance(payload, dict):
        return
    for cat, arr in payload.items():
        if not isinstance(arr, list):
            continue
        for obj in arr:
            if not isinstance(obj, dict):
                continue
            name = str(obj.get("name", "")).strip()
            if not name:
                continue
            yield str(cat), name


def _has_story_appearance(entry: Any, date_key: str, title: str) -> bool:
    if not isinstance(entry, dict):
        return False
    want_date, want_title = _story_key(date_key, title)
    sa = entry.get("story_appearances")
    if not isinstance(sa, list):
        return False
    for it in sa:
        if not isinstance(it, dict):
            continue
        d = str(it.get("date", "")).strip()
        t = str(it.get("title", "")).strip()
        if d == want_date and t == want_title:
            return True
    return False


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
    parser.add_argument("--max-tokens", type=int, default=8192, help="Max tokens for the audit extraction call")
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
    lore = gs.filter_lore_to_stories(lore, [story])

    # Merge into lore.json (same as daily generation and backfill).
    existing_lore = gs.load_lore()
    gs.merge_lore(existing_lore, lore, date_key)
    gs.save_lore(existing_lore, date_key)

    # Merge into codex.json (writes the file).
    gs.update_codex_file(lore, date_key=date_key, stories=[story], assume_all_from_stories=True)

    # Merge into characters.json.
    if hasattr(gs, "update_characters_file"):
        gs.update_characters_file(lore, date_key=date_key, stories=[story])

    # Tiny coverage sanity-check: if the model extracted entities but none got linked to
    # this audited story, fail fast (prevents silent no-op audits).
    try:
        updated = _load_json(CODEX_FILE) if os.path.exists(CODEX_FILE) else {}
    except Exception as e:
        print(f"ERROR: Could not reload {CODEX_FILE} for coverage check: {e}", file=sys.stderr)
        return 3

    extracted_named = list(_iter_named_entities(lore))
    extracted_count = 0
    linked_count = 0
    for cat, nm in extracted_named:
        arr = updated.get(cat)
        if not isinstance(arr, list):
            continue
        extracted_count += 1
        found = None
        want = nm.casefold()
        for it in arr:
            if not isinstance(it, dict):
                continue
            if str(it.get("name", "")).strip().casefold() == want:
                found = it
                break
        if found and _has_story_appearance(found, date_key, title):
            linked_count += 1

    if extracted_count > 0 and linked_count == 0:
        print(
            "ERROR: Audit extracted entities, but none were linked to the audited story. "
            "This likely indicates a story_appearances regression.",
            file=sys.stderr,
        )
        print(
            json.dumps(
                {
                    "date": date_key,
                    "title": title,
                    "extracted_entities": extracted_count,
                    "linked_entities": linked_count,
                },
                ensure_ascii=False,
            ),
            file=sys.stderr,
        )
        return 4

    print(f"✓ Audit merged for {date_key} / {title}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
