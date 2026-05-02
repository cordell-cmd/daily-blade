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
DEFAULT_CALENDAR_MONTH_NAMES = [
    "Frostwane",
    "Thawrise",
    "Rainmoot",
    "Bloomtide",
    "Highsun",
    "Goldwane",
    "Emberturn",
    "Harvestmere",
    "Redfall",
    "Mistmere",
    "Longnight",
    "Dawnbreak",
]
DEFAULT_CALENDAR_MONTH_LENGTHS = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
DEFAULT_CALENDAR_WEEKDAY_NAMES = [
    "Crown's Day",
    "Forge Day",
    "Tide Day",
    "Market Day",
    "Hearth Day",
    "Saints' Day",
    "Ash Day",
]
DEFAULT_CALENDAR_ERA = "AE"
DEFAULT_ANCHOR_WORLD_YEAR = 472
DEFAULT_ANCHOR_WORLD_MONTH = 1
DEFAULT_ANCHOR_WORLD_DAY = 1


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


@dataclass(frozen=True)
class WorldCalendarConfig:
    month_names: tuple[str, ...]
    month_lengths: tuple[int, ...]
    weekday_names: tuple[str, ...]
    era: str
    anchor_real_date: str | None
    anchor_world_year: int
    anchor_world_month: int
    anchor_world_day: int

    @property
    def days_per_year(self) -> int:
        return sum(self.month_lengths)


@dataclass(frozen=True)
class WorldDate:
    year: int
    month: int
    day: int
    weekday: int
    absolute_day: int

    def to_label(self, calendar: WorldCalendarConfig, include_weekday: bool = False) -> str:
        month_name = calendar.month_names[self.month - 1]
        core = f"{self.day} {month_name} {self.year} {calendar.era}".strip()
        if include_weekday:
            weekday_name = calendar.weekday_names[self.weekday]
            return f"{weekday_name}, {core}"
        return core


def _normalize_month_lengths(values: list[int] | tuple[int, ...] | None) -> tuple[int, ...]:
    raw = list(values or [])
    if len(raw) != 12:
        return tuple(DEFAULT_CALENDAR_MONTH_LENGTHS)
    normalized: list[int] = []
    for value in raw:
        try:
            parsed = int(value)
        except Exception:
            return tuple(DEFAULT_CALENDAR_MONTH_LENGTHS)
        if parsed <= 0:
            return tuple(DEFAULT_CALENDAR_MONTH_LENGTHS)
        normalized.append(parsed)
    return tuple(normalized)


def _normalize_name_list(values: list[str] | tuple[str, ...] | None, default: list[str]) -> tuple[str, ...]:
    raw = [str(value or "").strip() for value in (values or [])]
    if len(raw) != len(default) or any(not value for value in raw):
        return tuple(default)
    return tuple(raw)


def load_world_calendar_config(config_path: str = DEFAULT_WORLD_TIME_CONFIG) -> WorldCalendarConfig:
    cfg = _load_json(config_path) if os.path.exists(config_path) else {}
    calendar_cfg = cfg.get("calendar") if isinstance(cfg, dict) and isinstance(cfg.get("calendar"), dict) else {}

    anchor_cfg = calendar_cfg.get("anchor_world_date") if isinstance(calendar_cfg.get("anchor_world_date"), dict) else {}
    anchor_real_date = str(calendar_cfg.get("anchor_real_date") or "").strip() or None

    try:
        anchor_year = int(anchor_cfg.get("year", DEFAULT_ANCHOR_WORLD_YEAR))
    except Exception:
        anchor_year = DEFAULT_ANCHOR_WORLD_YEAR
    try:
        anchor_month = int(anchor_cfg.get("month", DEFAULT_ANCHOR_WORLD_MONTH))
    except Exception:
        anchor_month = DEFAULT_ANCHOR_WORLD_MONTH
    try:
        anchor_day = int(anchor_cfg.get("day", DEFAULT_ANCHOR_WORLD_DAY))
    except Exception:
        anchor_day = DEFAULT_ANCHOR_WORLD_DAY

    month_names = _normalize_name_list(calendar_cfg.get("month_names"), DEFAULT_CALENDAR_MONTH_NAMES)
    month_lengths = _normalize_month_lengths(calendar_cfg.get("month_lengths"))
    weekday_names = _normalize_name_list(calendar_cfg.get("weekday_names"), DEFAULT_CALENDAR_WEEKDAY_NAMES)
    era = str(calendar_cfg.get("era") or DEFAULT_CALENDAR_ERA).strip() or DEFAULT_CALENDAR_ERA

    if anchor_month < 1 or anchor_month > len(month_lengths):
        anchor_month = DEFAULT_ANCHOR_WORLD_MONTH
    month_length = month_lengths[anchor_month - 1]
    if anchor_day < 1 or anchor_day > month_length:
        anchor_day = min(DEFAULT_ANCHOR_WORLD_DAY, month_length)

    return WorldCalendarConfig(
        month_names=month_names,
        month_lengths=month_lengths,
        weekday_names=weekday_names,
        era=era,
        anchor_real_date=anchor_real_date,
        anchor_world_year=anchor_year,
        anchor_world_month=anchor_month,
        anchor_world_day=anchor_day,
    )


def _day_of_year(month: int, day: int, month_lengths: tuple[int, ...]) -> int:
    return sum(month_lengths[: month - 1]) + int(day)


def _world_date_from_ordinal(ordinal: int, calendar: WorldCalendarConfig) -> WorldDate:
    days_per_year = calendar.days_per_year
    if days_per_year <= 0:
        raise ValueError("World calendar must have at least one day per year")

    year_offset, day_offset = divmod(int(ordinal), days_per_year)
    year = calendar.anchor_world_year + year_offset
    running = day_offset
    month = 1
    for idx, month_length in enumerate(calendar.month_lengths, start=1):
        if running < month_length:
            month = idx
            day = running + 1
            break
        running -= month_length
    else:
        month = len(calendar.month_lengths)
        day = calendar.month_lengths[-1]

    weekday = int(ordinal) % len(calendar.weekday_names)
    return WorldDate(year=year, month=month, day=day, weekday=weekday, absolute_day=int(ordinal))


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
    calendar: WorldCalendarConfig

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

    def issue_index_for_date(self, date_key: str) -> int | None:
        return self.issue_index_by_date.get(str(date_key or "").strip())

    def world_date_for_issue_index(self, issue_index: int) -> WorldDate:
        anchor_day_of_year = _day_of_year(self.calendar.anchor_world_month, self.calendar.anchor_world_day, self.calendar.month_lengths)
        ordinal = (anchor_day_of_year - 1) + ((int(issue_index) - 1) * int(self.world_days_per_issue))
        return _world_date_from_ordinal(ordinal, self.calendar)

    def world_date_for_date(self, date_key: str) -> WorldDate | None:
        issue_index = self.issue_index_for_date(date_key)
        if issue_index is None:
            return None
        return self.world_date_for_issue_index(issue_index)

    def format_world_date(self, date_key: str, include_weekday: bool = False) -> str | None:
        world_date = self.world_date_for_date(date_key)
        if world_date is None:
            return None
        return world_date.to_label(self.calendar, include_weekday=include_weekday)


def build_world_clock(
    archive_index_path: str = DEFAULT_ARCHIVE_INDEX,
    stories_path: str = DEFAULT_STORIES_FILE,
    config_path: str = DEFAULT_WORLD_TIME_CONFIG,
) -> WorldClock:
    dates = load_issue_dates(archive_index_path=archive_index_path, stories_path=stories_path)
    world_days_per_issue = load_world_days_per_issue(config_path=config_path)
    calendar = load_world_calendar_config(config_path=config_path)
    return WorldClock(issue_dates=dates, world_days_per_issue=world_days_per_issue, calendar=calendar)
