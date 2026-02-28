#!/usr/bin/env python3
"""
backfill_extraction.py
Re-run lore extraction for a specific date's stories using the batched approach.

Usage:
    python backfill_extraction.py --date 2026-02-28
    python backfill_extraction.py --date 2026-02-28 --batch-size 3 --max-tokens 8192

This will:
1. Load the stories from archive/<date>.json (or stories.json if it matches)
2. Run batched lore extraction using the same prompt/logic as generate_stories.py
3. Merge the extracted lore into lore.json and codex.json
"""

import os
import sys
import json
import argparse

def main() -> int:
    parser = argparse.ArgumentParser(
        description="Re-run batched lore extraction for a specific date's stories"
    )
    parser.add_argument("--date", required=True, help="Issue date (YYYY-MM-DD)")
    parser.add_argument("--batch-size", type=int, default=3, help="Stories per extraction batch")
    parser.add_argument("--max-tokens", type=int, default=8192, help="Max output tokens per batch")
    parser.add_argument("--dry-run", action="store_true", help="Print what would be extracted without saving")
    args = parser.parse_args()

    # Import the generator module
    import generate_stories as gs

    if hasattr(gs, "_maybe_load_dotenv"):
        gs._maybe_load_dotenv()

    import anthropic

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("ERROR: ANTHROPIC_API_KEY environment variable not set.", file=sys.stderr)
        return 2

    date_key = str(args.date).strip()

    # Override batch settings if specified
    gs.EXTRACTION_BATCH_SIZE = args.batch_size
    gs.EXTRACTION_MAX_TOKENS = args.max_tokens

    # Load stories for this date
    archive_file = os.path.join(gs.ARCHIVE_DIR, f"{date_key}.json")
    stories_file = gs.OUTPUT_FILE

    stories = None
    source = None

    if os.path.exists(archive_file):
        with open(archive_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        if data.get("date") == date_key:
            stories = data.get("stories", [])
            source = archive_file

    if not stories and os.path.exists(stories_file):
        with open(stories_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        if data.get("date") == date_key:
            stories = data.get("stories", [])
            source = stories_file

    if not stories:
        print(f"ERROR: No stories found for date {date_key}", file=sys.stderr)
        return 1

    print(f"Found {len(stories)} stories from {source}")
    for i, s in enumerate(stories, 1):
        print(f"  {i}. {s.get('title', '?')}")

    # Load current lore
    lore = gs.load_lore()
    print(f"\nCurrent lore: {len(lore.get('characters', []))} characters, "
          f"{len(lore.get('places', []))} places, "
          f"{len(lore.get('events', []))} events")

    # Run batched extraction
    print(f"\nRunning batched extraction (batch_size={args.batch_size}, max_tokens={args.max_tokens})...")
    client = anthropic.Anthropic(api_key=api_key)

    new_lore = gs._extract_lore_batched(client, stories, lore)
    new_lore = gs.filter_lore_to_stories(new_lore, stories)
    new_lore = gs.ensure_named_leaders_present(new_lore, stories)

    # Attach first_story to characters
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

    # Print summary
    print("\n── Extraction Summary ──")
    total = 0
    for cat in sorted(new_lore.keys()):
        arr = new_lore.get(cat, [])
        if isinstance(arr, list) and arr:
            names = [str(it.get("name", "?")) for it in arr if isinstance(it, dict)]
            print(f"  {cat}: {len(arr)} → {', '.join(names[:10])}")
            total += len(arr)
    print(f"  TOTAL: {total} entities extracted")

    # Check per-story coverage
    print("\n── Per-Story Coverage ──")
    all_names = []
    for cat, arr in new_lore.items():
        if not isinstance(arr, list):
            continue
        for it in arr:
            if isinstance(it, dict) and it.get("name"):
                all_names.append(str(it["name"]).strip())

    for s in stories:
        title = s.get("title", "?")
        text = (s.get("text", "") or "").lower()
        title_low = title.lower()
        hits = [n for n in all_names if n.lower() in text or n.lower() in title_low]
        status = "✓" if hits else "✗ ZERO"
        print(f"  {status} {title}: {len(hits)} entities")

    if args.dry_run:
        print("\n[DRY RUN] No files modified.")
        return 0

    # Merge into lore.json
    print("\nMerging into lore.json...")
    lore = gs.merge_lore(lore, new_lore, date_key)
    gs.save_lore(lore, date_key)
    print(f"✓ Saved lore.json ({len(lore.get('characters', []))} characters total)")

    # Update codex.json
    print("Updating codex.json...")
    gs.update_codex_file(lore, date_key, stories)
    print("✓ Updated codex.json")

    # Update characters.json
    print("Updating characters.json...")
    gs.update_characters_file(lore, date_key, stories)
    print("✓ Updated characters.json")

    print("\nDone! Re-run with --dry-run to preview without saving.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
