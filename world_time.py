#!/usr/bin/env python3
"""Utilities for Daily Blade world-time calculations.

Phase 1 provides a deterministic clock anchored to issue dates.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Iterable


DEFAULT_WORLD_DAYS_PER_ISSUE = 10
DEFAULT_WORLD_TIME_CONFIG = "world_time_config.json"
DEFAULT_ARCHIVE_INDEX = "archive/index.json"
DEFAULT_STORIES_FILE = "stories.json"


def _is_iso_date(value: str) -> bool:
    try:
        datetime.strptime(str(value), "%Y-%m-%d")
        return True
    except Exception:
        return False


def _load_json(path: str):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_world_days_per_issue(
    config_path: str = DEFAULT_WORLD_TIME_CONFIG,
    env_var: str = "WORLD_DAYS_PER_ISSUE",
    default: int = DEFAULT_WORLD_DAYS_PER_ISSUE,
) -> int:
    env_val = (os.environ.get(env_var) or "").strip()
    if env_val:
        try:
            parsed = int(env_val)
            if parsed >= 1:
                return parsed
        except Exception:
            pass

    if os.path.exists(config_path):
        try:
            cfg = _load_json(config_path)
            parsed = int(cfg.get("world_days_per_issue", default))
            if parsed >= 1:
                return parsed
        except Exception:
            pass

    return int(default)


def load_issue_dates(
    archive_index_path: str = DEFAULT_ARCHIVE_INDEX,
    stories_path: str = DEFAULT_STORIES_FILE,
) -> list[str]:
    dates: set[str] = set()

    if os.path.exists(archive_index_path):
        try:
            idx = _load_json(archive_index_path)
            for raw in (idx.get("dates") if isinstance(idx, dict) else []) or []:
                d = str(raw or "").strip()
                if _is_iso_date(d):
                    dates.add(d)
        except Exception:
            pass

    if os.path.exists(stories_path):
        try:
            stories = _load_json(stories_path)
            d = str((stories or {}).get("date") or "").strip()
            if _is_iso_date(d):
                dates.add(d)
        except Exception:
            pass

    if not dates:
        return []

    return sorted(dates)


def build_issue_index(issue_dates: Iterable[str]) -> dict[str, int]:
    ordered = [d for d in issue_dates if _is_iso_date(d)]
    return {d: i + 1 for i, d in enumerate(ordered)}


def issue_delta_to_years(issue_delta: int, world_days_per_issue: int) -> float:
    return (float(issue_delta) * float(world_days_per_issue)) / 365.0


def world_year_from_issue(issue_index: int, world_days_per_issue: int) -> float:
    return issue_delta_to_years(int(issue_index), int(world_days_per_issue))


@dataclass(frozen=True)
class WorldClock:
    issue_dates: list[str]
    world_days_per_issue: int

    @property
    def issue_index_by_date(self) -> dict[str, int]:
        return build_issue_index(self.issue_dates)

    @property
    def current_issue_index(self) -> int:
        return len(self.issue_dates)

    @property
    def first_issue_date(self) -> str | None:
        return self.issue_dates[0] if self.issue_dates else None

    @property
    def latest_issue_date(self) -> str | None:
        return self.issue_dates[-1] if self.issue_dates else None

    def years_between(self, start_issue_index: int, end_issue_index: int) -> float:
        return issue_delta_to_years(int(end_issue_index) - int(start_issue_index), self.world_days_per_issue)


def build_world_clock(
    archive_index_path: str = DEFAULT_ARCHIVE_INDEX,
    stories_path: str = DEFAULT_STORIES_FILE,
    config_path: str = DEFAULT_WORLD_TIME_CONFIG,
) -> WorldClock:
    dates = load_issue_dates(archive_index_path=archive_index_path, stories_path=stories_path)
    world_days_per_issue = load_world_days_per_issue(config_path=config_path)
    return WorldClock(issue_dates=dates, world_days_per_issue=world_days_per_issue)
