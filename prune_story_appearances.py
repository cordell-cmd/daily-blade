#!/usr/bin/env python3
"""Prune codex story_appearances to only stories that actually mention the entity.

Why this exists:
- story_appearances power the UI (story badges, inferred location, etc).
- overly-loose substring matching can create false links (e.g. "crow" matching "crown").

This script is safe/idempotent:
- It only REMOVES story_appearances that fail a strict mention check.
- It does not invent new appearances.

Usage:
  python3 prune_story_appearances.py
"""

from __future__ import annotations

import json
import os
import re
from typing import Any

import generate_stories as gs

CODEX_FILE = "codex.json"
ARCHIVE_DIR = "archive"
ARCHIVE_INDEX = os.path.join(ARCHIVE_DIR, "index.json")
STORIES_FILE = "stories.json"


def _load_json(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _story_key(date: str, title: str) -> tuple[str, str]:
    return (str(date or "").strip(), str(title or "").strip().casefold())


def _build_story_blob_index() -> dict[tuple[str, str], str]:
    idx: dict[tuple[str, str], str] = {}

    dates: list[str] = []
    if os.path.exists(ARCHIVE_INDEX):
        try:
            j = _load_json(ARCHIVE_INDEX)
            if isinstance(j, dict) and isinstance(j.get("dates"), list):
                dates = [str(d).strip() for d in j["dates"] if str(d).strip()]
        except Exception:
            dates = []

    # Load archives
    for d in dates:
        p = os.path.join(ARCHIVE_DIR, f"{d}.json")
        if not os.path.exists(p):
            continue
        try:
            day = _load_json(p)
        except Exception:
            continue
        stories = day.get("stories") if isinstance(day, dict) else None
        if not isinstance(stories, list):
            continue
        for s in stories:
            if not isinstance(s, dict):
                continue
            title = str(s.get("title", "")).strip()
            if not title:
                continue
            blob = title + "\n" + str(s.get("text", "") or "")
            idx[_story_key(d, title)] = blob

    # Load current stories.json as fallback
    if os.path.exists(STORIES_FILE):
        try:
            day = _load_json(STORIES_FILE)
        except Exception:
            day = None
        if isinstance(day, dict):
            d = str(day.get("date", "")).strip()
            stories = day.get("stories")
            if d and isinstance(stories, list):
                for s in stories:
                    if not isinstance(s, dict):
                        continue
                    title = str(s.get("title", "")).strip()
                    if not title:
                        continue
                    blob = title + "\n" + str(s.get("text", "") or "")
                    idx[_story_key(d, title)] = blob

    return idx


def _strip_trailing_parenthetical(name: str) -> str:
    return re.sub(r"\s*\([^)]*\)\s*$", "", str(name or "")).strip()


def _base_name(name: str) -> str:
    # mirror gs._norm_entity_key behavior lightly
    s = _strip_trailing_parenthetical(name).replace("’", "'")
    s = re.sub(r"\s+", " ", s).strip()
    return s.casefold()


def _item_is_mentioned(cat: str, item: dict, blob: str) -> bool:
    nm = str(item.get("name", "")).strip()
    if not nm:
        return False
    if cat == "characters":
        if gs.entity_name_mentioned_in_text(nm, blob):
            return True
        aliases = item.get("aliases")
        if isinstance(aliases, list):
            for a in aliases:
                if gs.entity_name_mentioned_in_text(str(a or "").strip(), blob):
                    return True
        return False
    return gs.entity_name_mentioned_in_text(nm, blob)


def main() -> int:
    if not os.path.exists(CODEX_FILE):
        raise SystemExit(f"Missing {CODEX_FILE}")

    codex = _load_json(CODEX_FILE)
    if not isinstance(codex, dict):
        raise SystemExit(f"Invalid {CODEX_FILE} format")

    story_blobs = _build_story_blob_index()

    # Remove relics that are actually characters (cross-category drift)
    char_bases = {
        _base_name(c.get("name", ""))
        for c in (codex.get("characters") or [])
        if isinstance(c, dict) and str(c.get("name", "")).strip()
    }
    relics = codex.get("relics")
    if isinstance(relics, list) and relics:
        kept = []
        dropped = 0
        for r in relics:
            if not isinstance(r, dict):
                continue
            if _base_name(r.get("name", "")) in char_bases:
                dropped += 1
                continue
            kept.append(r)
        if dropped:
            print(f"Dropped {dropped} relic(s) that match character names")
        codex["relics"] = kept

    total_removed = 0
    total_kept = 0

    for cat, arr in list(codex.items()):
        if not isinstance(arr, list):
            continue
        for item in arr:
            if not isinstance(item, dict):
                continue
            apps = item.get("story_appearances")
            if not isinstance(apps, list) or not apps:
                continue

            new_apps = []
            for a in apps:
                if not isinstance(a, dict):
                    continue
                d = str(a.get("date", "") or "").strip()
                t = str(a.get("title", "") or "").strip()
                if not d or not t:
                    continue
                blob = story_blobs.get(_story_key(d, t))
                if not blob:
                    continue
                if _item_is_mentioned(cat, item, blob):
                    new_apps.append({"date": d, "title": t})

            # If everything got pruned, try preserving first_story if it matches.
            if not new_apps:
                fd = str(item.get("first_date", "") or "").strip()
                ft = str(item.get("first_story", "") or "").strip()
                if fd and ft:
                    blob = story_blobs.get(_story_key(fd, ft))
                    if blob and _item_is_mentioned(cat, item, blob):
                        new_apps = [{"date": fd, "title": ft}]

            removed = len(apps) - len(new_apps)
            if removed > 0:
                total_removed += removed
            total_kept += len(new_apps)

            item["story_appearances"] = new_apps
            if new_apps:
                item["appearances"] = len(new_apps)
                if not str(item.get("first_story", "") or "").strip():
                    item["first_story"] = new_apps[0].get("title", "")
                if not str(item.get("first_date", "") or "").strip():
                    item["first_date"] = new_apps[0].get("date", "")

    with open(CODEX_FILE, "w", encoding="utf-8") as f:
        json.dump(codex, f, ensure_ascii=True, indent=2)

    print(f"Pruned story_appearances: removed={total_removed}, kept={total_kept}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
