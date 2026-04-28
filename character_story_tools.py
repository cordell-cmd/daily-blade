#!/usr/bin/env python3
"""Shared helpers for reading Daily Blade story text by issue/title."""

from __future__ import annotations

import json
import os
from typing import Any


DEFAULT_ARCHIVE_INDEX = "archive/index.json"
DEFAULT_ARCHIVE_DIR = "archive"
DEFAULT_STORIES_FILE = "stories.json"


def _load_json(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_story_catalog(
    archive_index_path: str = DEFAULT_ARCHIVE_INDEX,
    archive_dir: str = DEFAULT_ARCHIVE_DIR,
    stories_path: str = DEFAULT_STORIES_FILE,
) -> dict[tuple[str, str], dict[str, Any]]:
    catalog: dict[tuple[str, str], dict[str, Any]] = {}
    dates: list[str] = []

    if os.path.exists(archive_index_path):
        try:
            idx = _load_json(archive_index_path)
            raw = idx.get("dates") if isinstance(idx, dict) else []
            if isinstance(raw, list):
                dates = [str(x or "").strip() for x in raw if str(x or "").strip()]
        except Exception:
            dates = []

    for date_key in dates:
        path = os.path.join(archive_dir, f"{date_key}.json")
        if not os.path.exists(path):
            continue
        try:
            day = _load_json(path)
        except Exception:
            continue
        stories = day.get("stories") if isinstance(day, dict) else []
        if not isinstance(stories, list):
            continue
        for story in stories:
            if not isinstance(story, dict):
                continue
            title = str(story.get("title") or "").strip()
            if not title:
                continue
            catalog[(date_key, title.lower())] = story

    if os.path.exists(stories_path):
        try:
            today = _load_json(stories_path)
            date_key = str(today.get("date") or "").strip() if isinstance(today, dict) else ""
            stories = today.get("stories") if isinstance(today, dict) else []
            if date_key and isinstance(stories, list):
                for story in stories:
                    if not isinstance(story, dict):
                        continue
                    title = str(story.get("title") or "").strip()
                    if not title:
                        continue
                    catalog[(date_key, title.lower())] = story
        except Exception:
            pass

    return catalog


def story_for_appearance(
    catalog: dict[tuple[str, str], dict[str, Any]],
    date_key: str,
    title: str,
) -> dict[str, Any] | None:
    return catalog.get((str(date_key or "").strip(), str(title or "").strip().lower()))


def gather_story_texts_for_character(
    character: dict[str, Any],
    catalog: dict[tuple[str, str], dict[str, Any]],
    max_stories: int = 0,
) -> list[dict[str, str]]:
    apps = character.get("story_appearances") if isinstance(character, dict) else None
    if not isinstance(apps, list):
        apps = []

    out: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for app in apps:
        if not isinstance(app, dict):
            continue
        date_key = str(app.get("date") or "").strip()
        title = str(app.get("title") or "").strip()
        if not date_key or not title:
            continue
        key = (date_key, title.lower())
        if key in seen:
            continue
        seen.add(key)
        story = story_for_appearance(catalog, date_key, title)
        if not story:
            continue
        out.append({
            "date": date_key,
            "title": str(story.get("title") or title).strip(),
            "text": str(story.get("text") or "").strip(),
            "subgenre": str(story.get("subgenre") or "").strip(),
        })

    if max_stories > 0:
        out = out[-max_stories:]
    return out
