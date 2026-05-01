#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import anthropic


ROOT = Path(__file__).resolve().parent
CODEX_FILE = ROOT / "codex.json"
WORLD_EVENTS_FILE = ROOT / "world-events.json"
ARCHIVE_INDEX_FILE = ROOT / "archive" / "index.json"
ARCHIVE_DIR = ROOT / "archive"
OUTPUT_FILE = ROOT / "chronicle.json"
CHRONICLE_ARCHIVE_DIR = ROOT / "chronicle-archive"
CHRONICLE_ARCHIVE_INDEX_FILE = CHRONICLE_ARCHIVE_DIR / "index.json"

MODEL = os.environ.get("ANTHROPIC_MODEL", "claude-haiku-4-5-20251001").strip() or "claude-haiku-4-5-20251001"
ISSUE_TIMEZONE = (os.environ.get("ISSUE_TIMEZONE") or "America/New_York").strip()
CADENCE_DAYS = int(os.environ.get("CHRONICLE_CADENCE_DAYS", "16") or 16)
MAX_WORLD_EVENTS = int(os.environ.get("CHRONICLE_MAX_WORLD_EVENTS", "8") or 8)
MAX_ENTITIES = int(os.environ.get("CHRONICLE_MAX_ENTITIES", "18") or 18)
MAX_STORIES = int(os.environ.get("CHRONICLE_MAX_STORIES", "12") or 12)
MAX_CONNECTIONS = int(os.environ.get("CHRONICLE_MAX_CONNECTIONS", "10") or 10)
MAX_WORLD_EVENTS_FULL_HISTORY = int(os.environ.get("CHRONICLE_FULL_HISTORY_MAX_WORLD_EVENTS", "10") or 10)
MAX_ENTITIES_FULL_HISTORY = int(os.environ.get("CHRONICLE_FULL_HISTORY_MAX_ENTITIES", "24") or 24)
MAX_STORIES_FULL_HISTORY = int(os.environ.get("CHRONICLE_FULL_HISTORY_MAX_STORIES", "16") or 16)
MAX_CONNECTIONS_FULL_HISTORY = int(os.environ.get("CHRONICLE_FULL_HISTORY_MAX_CONNECTIONS", "12") or 12)
MAX_TOKENS = int(os.environ.get("CHRONICLE_MAX_TOKENS", "2200") or 2200)
KEEP_ENTRIES = int(os.environ.get("CHRONICLE_KEEP_ENTRIES", "32") or 32)
ALLOW_FALLBACK = (os.environ.get("CHRONICLE_ALLOW_FALLBACK", "1").strip().lower() in {"1", "true", "yes", "y"})

ENTITY_CATEGORIES = [
    "characters",
    "places",
    "events",
    "factions",
    "polities",
    "deities_and_entities",
    "weapons",
    "flora_fauna",
    "districts",
    "regions",
    "realms",
    "provinces",
    "artifacts",
    "rituals",
    "magic",
    "relics",
    "substances",
]

CATEGORY_WEIGHTS = {
    "events": 12,
    "characters": 11,
    "places": 10,
    "factions": 8,
    "polities": 8,
    "deities_and_entities": 8,
    "realms": 6,
    "regions": 6,
    "provinces": 5,
    "weapons": 6,
    "artifacts": 5,
    "rituals": 5,
    "flora_fauna": 5,
    "districts": 4,
    "magic": 4,
    "relics": 4,
    "substances": 3,
}

TYPE_LABELS = {
    "characters": "Characters",
    "places": "Places",
    "events": "Events",
    "factions": "Factions",
    "polities": "Polities",
    "deities_and_entities": "Entities",
    "weapons": "Weapons",
    "flora_fauna": "Flora & Fauna",
    "districts": "Districts",
    "regions": "Regions",
    "realms": "Realms",
    "provinces": "Provinces",
    "artifacts": "Artifacts",
    "rituals": "Rituals",
    "magic": "Magic",
    "relics": "Relics",
    "substances": "Substances",
}

SCORING_WEIGHTS = [
    {
        "signal": "Recent appearances",
        "weight": "+12 each",
        "description": "Entities repeating across the current chronicle window matter most.",
    },
    {
        "signal": "Recent issue spread",
        "weight": "+6 each issue",
        "description": "Recurring across multiple issues outranks a one-day spike.",
    },
    {
        "signal": "Lifetime appearances",
        "weight": "+3 each (capped)",
        "description": "Long-running continuity raises baseline importance.",
    },
    {
        "signal": "Co-appearance network",
        "weight": "+2 degree, +1 edge weight",
        "description": "Entities that intersect many others become chronicle-worthy hubs.",
    },
    {
        "signal": "World-event overlap",
        "weight": "+5 each overlap",
        "description": "Direct ties to major arcs push entities upward.",
    },
    {
        "signal": "Category baseline",
        "weight": "3-12",
        "description": "Events, characters, and places get slightly higher starting weight because they anchor narrative continuity.",
    },
]


def _maybe_load_dotenv() -> None:
    try:
        from dotenv import load_dotenv  # type: ignore
    except Exception:
        return
    load_dotenv(override=False)


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _tz_now_date_key() -> str:
    try:
        tz = ZoneInfo(ISSUE_TIMEZONE)
    except Exception:
        tz = timezone.utc
    return datetime.now(tz).strftime("%Y-%m-%d")


def _load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def _write_json(path: Path, data: Any) -> None:
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=True, indent=2)


def _archive_stamp(value: str) -> str:
    raw = str(value or "").strip() or _now_iso()
    return re.sub(r"[^0-9A-Za-z-]+", "-", raw.replace(":", "-"))


def _archive_chronicle_entry(entry: dict[str, Any]) -> None:
    CHRONICLE_ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)

    stem = _archive_stamp(entry.get("generated_at") or entry.get("date") or _now_iso())
    archive_path = CHRONICLE_ARCHIVE_DIR / f"{stem}.json"
    suffix = 2
    while archive_path.exists():
        archive_path = CHRONICLE_ARCHIVE_DIR / f"{stem}-{suffix}.json"
        suffix += 1

    _write_json(archive_path, entry)

    index_payload = _load_json(CHRONICLE_ARCHIVE_INDEX_FILE, {"entries": []})
    index_entries = index_payload.get("entries") if isinstance(index_payload, dict) and isinstance(index_payload.get("entries"), list) else []
    index_entries = [row for row in index_entries if isinstance(row, dict) and str(row.get("path") or "").strip() != archive_path.name]
    index_entries.insert(
        0,
        {
            "generated_at": entry.get("generated_at"),
            "date": entry.get("date"),
            "issue_number": entry.get("issue_number"),
            "title": entry.get("title"),
            "model": entry.get("model"),
            "window_start": entry.get("window_start"),
            "window_end": entry.get("window_end"),
            "path": archive_path.name,
        },
    )
    index_entries.sort(key=lambda row: str(row.get("generated_at") or ""), reverse=True)
    _write_json(CHRONICLE_ARCHIVE_INDEX_FILE, {"entries": index_entries})


def _parse_date_key(value: str) -> datetime.date | None:
    s = str(value or "").strip()
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except Exception:
        return None


def _story_key(date_key: str, title: str) -> str:
    date_key = str(date_key or "").strip()
    title = re.sub(r"\s+", " ", str(title or "").strip()).lower()
    return f"{date_key}::{title}" if date_key and title else ""


def _normalize_name(value: str) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip()).lower()


def _concept_key(value: str) -> str:
    normalized = _normalize_name(value)
    normalized = re.sub(r"\s*\([^)]*\)", "", normalized)
    normalized = re.sub(r"^(?:the|a|an)\s+", "", normalized)
    normalized = re.sub(r"[^a-z0-9]+", " ", normalized)
    return re.sub(r"\s+", " ", normalized).strip()


def _display_name_rank(value: str) -> tuple[int, int, int]:
    raw = str(value or "").strip()
    lowered = raw.lower()
    return (
        1 if re.match(r"^(?:the|a|an)\s+", lowered) else 0,
        1 if "(" in raw and ")" in raw else 0,
        len(raw),
    )


def _cap_for_run(default_cap: int, full_history_cap: int, full_history: bool) -> int:
    return max(1, full_history_cap if full_history else default_cap)


def _truncate(text: str, limit: int = 420) -> str:
    raw = re.sub(r"\s+", " ", str(text or "").strip())
    if len(raw) <= limit:
        return raw
    return raw[: max(0, limit - 1)].rstrip() + "…"


def _multiline_truncate(text: str, limit: int = 1200) -> str:
    raw = str(text or "").strip()
    if len(raw) <= limit:
        return raw
    return raw[: max(0, limit - 1)].rstrip() + "…"


def _normalize_model_payload(text: str) -> str:
    payload = str(text or "").strip()
    if payload.startswith("```"):
        payload = re.sub(r"^```(?:json)?\s*", "", payload)
        payload = re.sub(r"\s*```$", "", payload)
    return payload.strip()


def _extract_json_object(payload: str) -> str:
    start = payload.find("{")
    end = payload.rfind("}")
    if start >= 0 and end > start:
        return payload[start : end + 1]
    return payload


def _parse_model_json(text: str) -> dict[str, Any]:
    payload = _normalize_model_payload(text)
    candidates = [payload]
    extracted = _extract_json_object(payload)
    if extracted != payload:
        candidates.append(extracted)

    last_error: Exception | None = None
    for candidate in candidates:
        try:
            parsed = json.loads(candidate)
        except Exception as exc:
            last_error = exc
            continue
        if isinstance(parsed, dict):
            return parsed
        raise ValueError("Chronicle model response was not a JSON object")

    if last_error is not None:
        raise last_error
    raise ValueError("Chronicle model response was empty")


def _issue_number(dates_desc: list[str], date_key: str) -> int | None:
    try:
        idx = dates_desc.index(date_key)
    except ValueError:
        return None
    return len(dates_desc) - idx


def _coerce_story_apps(obj: dict[str, Any]) -> list[dict[str, str]]:
    apps = obj.get("story_appearances") if isinstance(obj.get("story_appearances"), list) else []
    out: list[dict[str, str]] = []
    seen = set()
    for row in apps:
        if not isinstance(row, dict):
            continue
        date_key = str(row.get("date") or "").strip()
        title = str(row.get("title") or "").strip()
        key = _story_key(date_key, title)
        if not key or key in seen:
            continue
        seen.add(key)
        out.append({"date": date_key, "title": title})
    first_story = str(obj.get("first_story") or "").strip()
    first_date = str(obj.get("first_date") or "").strip()
    first_key = _story_key(first_date, first_story)
    if first_key and first_key not in seen:
        out.append({"date": first_date, "title": first_story})
    out.sort(key=lambda row: (row.get("date") or "", row.get("title") or ""))
    return out


def _unique_nonempty(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        item = str(value or "").strip()
        if not item or item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def _dedupe_entity_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: list[dict[str, Any]] = []
    concept_index: dict[str, dict[str, Any]] = {}
    for row in rows:
        name = str(row.get("name") or "").strip()
        key = _concept_key(name)
        if not key:
            continue
        existing = concept_index.get(key)
        if existing is None:
            copied = dict(row)
            concept_index[key] = copied
            deduped.append(copied)
            continue
        if _display_name_rank(name) < _display_name_rank(str(existing.get("name") or "")):
            existing["name"] = name
        existing_why = existing.get("why") if isinstance(existing.get("why"), list) else []
        row_why = row.get("why") if isinstance(row.get("why"), list) else []
        existing["why"] = _unique_nonempty([*existing_why, *[str(item).strip() for item in row_why if str(item).strip()]])
        for field in ("recent_appearances", "appearance_total", "network_degree", "connection_weight", "world_event_overlap", "score"):
            existing[field] = max(int(existing.get(field) or 0), int(row.get(field) or 0))
        if not str(existing.get("last_seen") or "") or str(row.get("last_seen") or "") > str(existing.get("last_seen") or ""):
            existing["last_seen"] = row.get("last_seen")
    return deduped


def _dedupe_story_entities(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: list[dict[str, Any]] = []
    seen: dict[str, dict[str, Any]] = {}
    for row in rows:
        name = str(row.get("name") or "").strip()
        key = _concept_key(name)
        if not key:
            continue
        existing = seen.get(key)
        if existing is None:
            copied = dict(row)
            seen[key] = copied
            deduped.append(copied)
            continue
        if _display_name_rank(name) < _display_name_rank(str(existing.get("name") or "")):
            existing["name"] = name
        existing["score"] = max(int(existing.get("score") or 0), int(row.get("score") or 0))
    return deduped


def _dedupe_connections(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for row in rows:
        source = row.get("source") if isinstance(row.get("source"), dict) else {}
        target = row.get("target") if isinstance(row.get("target"), dict) else {}
        pair = tuple(sorted((_concept_key(source.get("name") or ""), _concept_key(target.get("name") or ""))))
        if not pair[0] or not pair[1] or pair[0] == pair[1] or pair in seen:
            continue
        seen.add(pair)
        deduped.append(row)
    return deduped


def _entity_summary(category: str, row: dict[str, Any]) -> str:
    candidates = [
        row.get("tagline"),
        row.get("bio"),
        row.get("description"),
        row.get("outcome"),
        row.get("notes"),
        row.get("goals"),
        row.get("powers"),
        row.get("origin"),
        row.get("function"),
        row.get("status"),
    ]
    if category == "characters":
        role = str(row.get("role") or "").strip()
        bio = str(row.get("bio") or row.get("description") or "").strip()
        return _truncate(f"{role}. {bio}" if role and bio else (role or bio), 320)
    if category == "districts":
        parent = str(row.get("parent_place") or row.get("place") or "").strip()
        func = str(row.get("function") or row.get("description") or "").strip()
        return _truncate(f"{parent}. {func}" if parent and func else (parent or func), 320)
    for value in candidates:
        if str(value or "").strip():
            return _truncate(str(value or ""), 320)
    return ""


def _clean_history(payload: dict[str, Any]) -> dict[str, Any]:
    entries = payload.get("entries") if isinstance(payload.get("entries"), list) else []
    cleaned = []
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        date_key = str(entry.get("date") or "").strip()
        if not _parse_date_key(date_key):
            continue
        cleaned.append(entry)
    cleaned.sort(key=lambda row: str(row.get("date") or ""), reverse=True)
    return {
        "generated_at": payload.get("generated_at") or None,
        "cadence_days": int(payload.get("cadence_days") or CADENCE_DAYS),
        "entries": cleaned,
        "latest_entry": cleaned[0] if cleaned else None,
    }


def _select_window_dates(archive_dates_desc: list[str], end_date_key: str, last_date_key: str | None, cadence_days: int, full_history: bool = False) -> list[str]:
    end_date = _parse_date_key(end_date_key)
    if not end_date:
        return []
    if full_history:
        picked = []
        for value in reversed(archive_dates_desc):
            d = _parse_date_key(value)
            if d and d <= end_date:
                picked.append(value)
        return picked
    lower_bound = end_date - timedelta(days=max(0, cadence_days - 1))
    if last_date_key:
        last_date = _parse_date_key(last_date_key)
        if last_date:
            lower_bound = max(lower_bound, last_date + timedelta(days=1))
    picked = []
    for value in reversed(archive_dates_desc):
        d = _parse_date_key(value)
        if not d:
            continue
        if lower_bound <= d <= end_date:
            picked.append(value)
    return picked


def _load_window_stories(window_dates: list[str], archive_dates_desc: list[str]) -> tuple[list[dict[str, Any]], dict[str, int]]:
    stories: list[dict[str, Any]] = []
    issue_numbers: dict[str, int] = {}
    for date_key in window_dates:
        issue_no = _issue_number(archive_dates_desc, date_key)
        if issue_no is not None:
            issue_numbers[date_key] = issue_no
        payload = _load_json(ARCHIVE_DIR / f"{date_key}.json", {})
        rows = payload.get("stories") if isinstance(payload, dict) and isinstance(payload.get("stories"), list) else []
        for idx, story in enumerate(rows, start=1):
            if not isinstance(story, dict):
                continue
            title = str(story.get("title") or "").strip()
            text = str(story.get("text") or "").strip()
            key = _story_key(date_key, title)
            if not key:
                continue
            stories.append(
                {
                    "key": key,
                    "date": date_key,
                    "issue_number": issue_no,
                    "sequence": idx,
                    "title": title,
                    "subgenre": str(story.get("subgenre") or "").strip(),
                    "text": text,
                    "summary": _truncate(text, 520),
                }
            )
    stories.sort(key=lambda row: (row.get("date") or "", int(row.get("sequence") or 0)))
    return stories, issue_numbers


def _rank_world_events(payload: dict[str, Any], window_date_set: set[str], max_world_events: int) -> list[dict[str, Any]]:
    events = payload.get("events") if isinstance(payload.get("events"), list) else []
    stage_rank = {"seed": 1, "simmering": 2, "rising": 3, "crisis": 4, "climax": 5, "aftermath": 0}
    setting_rank = {"legacy": 0, "active_arc": 1, "setting_shift": 2, "flashpoint": 3}
    deduped: dict[str, dict[str, Any]] = {}
    for row in events:
        if not isinstance(row, dict):
            continue
        key = _concept_key(row.get("name") or "")
        if not key:
            continue
        apps = row.get("story_appearances") if isinstance(row.get("story_appearances"), list) else []
        recent_overlap = sum(1 for app in apps if isinstance(app, dict) and str(app.get("date") or "") in window_date_set)
        score = (
            int(row.get("intensity") or 0) * 20
            + int(row.get("recent_issues") or 0) * 8
            + min(40, int(row.get("appearance_total") or 0) // 4)
            + stage_rank.get(str(row.get("stage") or "").strip().lower(), 0) * 8
            + setting_rank.get(str(row.get("setting_state") or "").strip().lower(), 0) * 10
            + (15 if not bool(row.get("resolved")) else 0)
            + recent_overlap * 5
        )
        enriched = dict(row)
        enriched["_score"] = score
        enriched["_recent_overlap"] = recent_overlap
        prev = deduped.get(key)
        if prev is None or int(prev.get("_score") or 0) < score:
            deduped[key] = enriched
    ranked = sorted(deduped.values(), key=lambda row: (int(row.get("_score") or 0), str(row.get("name") or "")), reverse=True)
    return ranked[:max_world_events]


def _rank_entities(codex: dict[str, Any], stories: list[dict[str, Any]], world_events: list[dict[str, Any]], cadence_days: int, max_entities: int, max_connections: int) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    story_lookup = {row["key"]: row for row in stories}
    top_event_story_keys = {
        _story_key(app.get("date") or "", app.get("title") or "")
        for event in world_events
        for app in (event.get("story_appearances") if isinstance(event.get("story_appearances"), list) else [])
        if isinstance(app, dict)
    }
    top_event_name_keys = {_concept_key(row.get("name") or "") for row in world_events}

    entity_rows: list[dict[str, Any]] = []
    story_entities: dict[str, list[str]] = defaultdict(list)
    entity_index: dict[str, dict[str, Any]] = {}

    for category in ENTITY_CATEGORIES:
        rows = codex.get(category) if isinstance(codex.get(category), list) else []
        for obj in rows:
            if not isinstance(obj, dict):
                continue
            name = str(obj.get("name") or "").strip()
            if not name:
                continue
            entity_id = f"{category}:{_normalize_name(name)}"
            apps = _coerce_story_apps(obj)
            total_keys = [_story_key(app.get("date") or "", app.get("title") or "") for app in apps]
            total_keys = [key for key in total_keys if key]
            recent_keys = [key for key in total_keys if key in story_lookup]
            if not total_keys and not recent_keys:
                continue
            summary = _entity_summary(category, obj)
            entity = entity_index.get(entity_id)
            if entity is None:
                entity = {
                    "id": entity_id,
                    "name": name,
                    "type": category,
                    "label": TYPE_LABELS.get(category, category.replace("_", " ").title()),
                    "summary": summary,
                    "recent_appearances": 0,
                    "recent_issue_count": 0,
                    "appearance_total": 0,
                    "issue_span": 0,
                    "last_seen": "",
                    "story_keys": [],
                    "all_story_keys": [],
                    "score": 0,
                    "category_weight": CATEGORY_WEIGHTS.get(category, 3),
                    "network_degree": 0,
                    "connection_weight": 0,
                    "world_event_overlap": 0,
                    "linked_entities": [],
                    "why": [],
                }
                entity_rows.append(entity)
                entity_index[entity_id] = entity
            elif summary and len(summary) > len(str(entity.get("summary") or "")):
                entity["summary"] = summary

            entity["story_keys"] = _unique_nonempty([*(entity.get("story_keys") or []), *recent_keys])
            entity["all_story_keys"] = _unique_nonempty([*(entity.get("all_story_keys") or []), *total_keys])

    for entity in entity_rows:
        for key in entity.get("story_keys") or []:
            story_entities[key].append(entity["id"])

        recent_dates = sorted({story_lookup[key]["date"] for key in entity.get("story_keys") or [] if key in story_lookup})
        all_dates = sorted({key.split("::", 1)[0] for key in entity.get("all_story_keys") or [] if "::" in key})
        entity["recent_appearances"] = len(entity.get("story_keys") or [])
        entity["recent_issue_count"] = len(recent_dates)
        entity["appearance_total"] = len(entity.get("all_story_keys") or [])
        entity["issue_span"] = len(all_dates)
        entity["last_seen"] = recent_dates[-1] if recent_dates else (all_dates[-1] if all_dates else "")

    connections: dict[tuple[str, str], dict[str, Any]] = {}
    neighbors: dict[str, defaultdict[str, int]] = defaultdict(lambda: defaultdict(int))
    for story_key, entity_ids in story_entities.items():
        unique_ids = sorted(set(entity_ids))
        for idx, left in enumerate(unique_ids):
            for right in unique_ids[idx + 1 :]:
                pair = (left, right)
                conn = connections.setdefault(pair, {"weight": 0, "story_keys": []})
                conn["weight"] += 1
                conn["story_keys"].append(story_key)
                neighbors[left][right] += 1
                neighbors[right][left] += 1

    for entity in entity_rows:
        last_seen = _parse_date_key(entity.get("last_seen") or "")
        recency_bonus = 0
        if last_seen and stories:
            end_date = _parse_date_key(stories[-1].get("date") or "")
            if end_date:
                days_since = max(0, (end_date - last_seen).days)
                recency_bonus = max(0, cadence_days - days_since)
        overlap = len([key for key in entity.get("story_keys") or [] if key in top_event_story_keys])
        if entity["type"] == "events" and _concept_key(entity["name"]) in top_event_name_keys:
            overlap += 2
        entity["world_event_overlap"] = overlap
        entity["network_degree"] = len(neighbors.get(entity["id"], {}))
        entity["connection_weight"] = sum(neighbors.get(entity["id"], {}).values())
        entity["score"] = (
            int(entity["recent_appearances"]) * 12
            + int(entity["recent_issue_count"]) * 6
            + min(12, int(entity["appearance_total"])) * 3
            + int(entity["category_weight"])
            + min(24, int(entity["network_degree"]) * 2)
            + min(24, int(entity["connection_weight"]))
            + int(entity["world_event_overlap"]) * 5
            + recency_bonus
        )
        linked = sorted(neighbors.get(entity["id"], {}).items(), key=lambda row: (int(row[1]), row[0]), reverse=True)[:5]
        entity["linked_entities"] = [
            {
                "id": other_id,
                "name": entity_index[other_id]["name"],
                "type": entity_index[other_id]["type"],
                "weight": weight,
            }
            for other_id, weight in linked
            if other_id in entity_index
        ]
        why = []
        if entity["recent_appearances"]:
            why.append(f"{entity['recent_appearances']} recent tale appearance(s)")
        if entity["world_event_overlap"]:
            why.append(f"overlaps {entity['world_event_overlap']} major arc beat(s)")
        if entity["network_degree"]:
            why.append(f"connected to {entity['network_degree']} other ranked entities")
        if entity["appearance_total"] and entity["appearance_total"] != entity["recent_appearances"]:
            why.append(f"{entity['appearance_total']} total recorded appearance(s)")
        entity["why"] = why

    ranked_entities = sorted(entity_rows, key=lambda row: (int(row.get("score") or 0), str(row.get("name") or "")), reverse=True)
    top_entities = _dedupe_entity_rows(ranked_entities)[:max_entities]

    connection_rows = []
    for (left, right), conn in connections.items():
        if left not in entity_index or right not in entity_index:
            continue
        connection_rows.append(
            {
                "source": {"name": entity_index[left]["name"], "type": entity_index[left]["type"]},
                "target": {"name": entity_index[right]["name"], "type": entity_index[right]["type"]},
                "weight": int(conn["weight"]),
                "stories": [story_lookup[key]["title"] for key in conn["story_keys"][:4] if key in story_lookup],
            }
        )
    connection_rows.sort(key=lambda row: (int(row.get("weight") or 0), row["source"]["name"], row["target"]["name"]), reverse=True)
    return ranked_entities, top_entities, _dedupe_connections(connection_rows)[:max_connections]


def _rank_stories(stories: list[dict[str, Any]], entity_rows: list[dict[str, Any]], world_events: list[dict[str, Any]], max_stories: int) -> list[dict[str, Any]]:
    story_map = {row["key"]: dict(row) for row in stories}
    entity_by_story: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for entity in entity_rows:
        for key in entity.get("story_keys") or []:
            if key in story_map:
                entity_by_story[key].append(entity)

    top_event_keys = {
        _story_key(app.get("date") or "", app.get("title") or "")
        for event in world_events
        for app in (event.get("story_appearances") if isinstance(event.get("story_appearances"), list) else [])
        if isinstance(app, dict)
    }

    ranked = []
    for key, story in story_map.items():
        entities = sorted(entity_by_story.get(key, []), key=lambda row: int(row.get("score") or 0), reverse=True)
        deduped_entities = _dedupe_story_entities(
            [
                {"name": entity.get("name"), "type": entity.get("type"), "score": entity.get("score")}
                for entity in entities
            ]
        )
        score = sum(min(20, int(entity.get("score") or 0)) for entity in deduped_entities[:8])
        score += len({entity.get("type") for entity in deduped_entities}) * 4
        if key in top_event_keys:
            score += 18
        story["_score"] = score
        story["entities"] = deduped_entities[:8]
        ranked.append(story)
    ranked.sort(key=lambda row: (int(row.get("_score") or 0), str(row.get("date") or ""), str(row.get("title") or "")), reverse=True)
    return ranked[:max_stories]


def _build_dossier(window_dates: list[str], stories: list[dict[str, Any]], world_events: list[dict[str, Any]], ranked_entities: list[dict[str, Any]], top_entities: list[dict[str, Any]], connections: list[dict[str, Any]], chronicle_history: dict[str, Any], max_stories: int) -> dict[str, Any]:
    previous = chronicle_history.get("latest_entry") if isinstance(chronicle_history.get("latest_entry"), dict) else None
    selected_stories = _rank_stories(stories, ranked_entities, world_events, max_stories)
    top_by_type: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in ranked_entities:
        if len(top_by_type[row["type"]]) >= 4:
            continue
        top_by_type[row["type"]].append(row)

    return {
        "window": {
            "start_date": window_dates[0] if window_dates else None,
            "end_date": window_dates[-1] if window_dates else None,
            "issue_count": len(window_dates),
            "story_count": len(stories),
        },
        "previous_chronicle": {
            "date": previous.get("date") if previous else None,
            "title": previous.get("title") if previous else None,
            "dek": previous.get("dek") if previous else None,
            "chronicle_excerpt": _multiline_truncate(str(previous.get("chronicle") or ""), 1800) if previous else "",
            "ongoing_threads": (previous.get("ongoing_threads") or [])[:6] if previous else [],
        },
        "world_events": [
            {
                "name": row.get("name"),
                "tagline": row.get("tagline"),
                "event_type": row.get("event_type"),
                "stage": row.get("stage"),
                "setting_state": row.get("setting_state"),
                "intensity": row.get("intensity"),
                "recent_issues": row.get("recent_issues"),
                "appearance_total": row.get("appearance_total"),
                "last_seen": row.get("last_seen"),
                "summary": _multiline_truncate(str(row.get("arc_summary") or ""), 1800),
            }
            for row in world_events
        ],
        "selected_entities": [
            {
                "name": row.get("name"),
                "type": row.get("type"),
                "label": row.get("label"),
                "score": row.get("score"),
                "summary": row.get("summary"),
                "recent_appearances": row.get("recent_appearances"),
                "appearance_total": row.get("appearance_total"),
                "last_seen": row.get("last_seen"),
                "why": row.get("why"),
                "linked_entities": row.get("linked_entities"),
            }
            for row in top_entities
        ],
        "selected_stories": [
            {
                "date": row.get("date"),
                "issue_number": row.get("issue_number"),
                "title": row.get("title"),
                "subgenre": row.get("subgenre"),
                "summary": row.get("summary"),
                "text": row.get("text"),
                "entities": row.get("entities"),
            }
            for row in selected_stories
        ],
        "pipeline": {
            "scoring_weights": SCORING_WEIGHTS,
            "top_entities": [
                {
                    "name": row.get("name"),
                    "type": row.get("type"),
                    "label": row.get("label"),
                    "score": row.get("score"),
                    "recent_appearances": row.get("recent_appearances"),
                    "appearance_total": row.get("appearance_total"),
                    "network_degree": row.get("network_degree"),
                    "connection_weight": row.get("connection_weight"),
                    "world_event_overlap": row.get("world_event_overlap"),
                    "last_seen": row.get("last_seen"),
                    "why": row.get("why"),
                }
                for row in top_entities
            ],
            "top_entities_by_type": {
                TYPE_LABELS.get(category, category.replace("_", " ").title()): [
                    {
                        "name": row.get("name"),
                        "type": row.get("type"),
                        "score": row.get("score"),
                        "why": row.get("why"),
                    }
                    for row in rows
                ]
                for category, rows in sorted(top_by_type.items(), key=lambda item: TYPE_LABELS.get(item[0], item[0]))
            },
            "top_world_events": [
                {
                    "name": row.get("name"),
                    "stage": row.get("stage"),
                    "setting_state": row.get("setting_state"),
                    "intensity": row.get("intensity"),
                    "recent_issues": row.get("recent_issues"),
                    "score": row.get("_score"),
                }
                for row in world_events
            ],
            "top_connections": connections,
            "selected_stories": [
                {
                    "date": row.get("date"),
                    "title": row.get("title"),
                    "subgenre": row.get("subgenre"),
                    "score": row.get("_score"),
                    "entities": row.get("entities"),
                }
                for row in selected_stories
            ],
        },
    }


def _build_prompt(issue_date: str, issue_number: int | None, dossier: dict[str, Any]) -> str:
    issue_label = f"Issue {issue_number:03d}" if isinstance(issue_number, int) else "Latest Issue"
    return f"""You are the chronicler of Edhra, writing an ongoing narrative overview of the world behind The Daily Blade.

Your source material is already curated from canon. Treat the dossier below as authoritative.

DATE ANCHOR: {issue_date}
ISSUE LABEL: {issue_label}

DOSSIER JSON:
{json.dumps(dossier, ensure_ascii=False, indent=2)}

Task:
- Write a world chronicle that reads like a serious fantasy history-in-progress, not a wiki entry.
- Pull together the most important overlaps across characters, places, events, factions, and ongoing pressures.
- Emphasize cause/effect, escalation, and how one storyline affects another.
- Focus on what matters now in Edhra and Valdris.
- You may infer significance and connective tissue, but do NOT invent unsupported facts.

Style constraints:
- Multi-paragraph prose, readable and vivid, but not purple.
- Aim for roughly 700-1200 words for the main chronicle body.
- Keep names, places, and event titles consistent with the dossier.
- This is not a daily issue recap; it is a periodic world-state chronicle.

Return ONLY valid JSON matching exactly this shape:
{{
  "title": "Short evocative chronicle title",
  "dek": "One-sentence deck explaining the current world moment",
  "chronicle": "Multi-paragraph plain text with blank lines between paragraphs",
  "current_state": ["4-7 concise bullets"],
  "ongoing_threads": ["4-8 unresolved threads to watch"]
}}
"""


def _fallback_entry(issue_date: str, issue_number: int | None, dossier: dict[str, Any]) -> dict[str, Any]:
    top_events = dossier.get("world_events") if isinstance(dossier.get("world_events"), list) else []
    top_entities = dossier.get("selected_entities") if isinstance(dossier.get("selected_entities"), list) else []
    top_stories = dossier.get("selected_stories") if isinstance(dossier.get("selected_stories"), list) else []
    event_names = [str(row.get("name") or "").strip() for row in top_events[:3] if str(row.get("name") or "").strip()]
    figure_names = [str(row.get("name") or "").strip() for row in top_entities[:4] if str(row.get("name") or "").strip()]
    story_titles = [str(row.get("title") or "").strip() for row in top_stories[:4] if str(row.get("title") or "").strip()]
    issue_label = f"Issue {issue_number:03d}" if isinstance(issue_number, int) else issue_date
    title = f"Chronicle of {event_names[0]}" if event_names else f"Chronicle of {issue_label}"
    dek = "A stitched overview of the strongest pressures shaping Edhra and Valdris."
    paragraphs = []
    if event_names:
        paragraphs.append(
            f"By {issue_date}, the world of Edhra is being pulled most strongly by {', '.join(event_names[:-1]) + (', and ' + event_names[-1] if len(event_names) > 1 else event_names[0])}. These arcs are no longer isolated disturbances; they are the pressures around which the current age is organizing itself."
        )
    if figure_names:
        paragraphs.append(
            f"The figures and forces surfacing most often in this span include {', '.join(figure_names)}. Their repeated intersections suggest that the world is cohering around recurring centers of consequence rather than disconnected adventures."
        )
    if story_titles:
        paragraphs.append(
            f"Recent turning points cluster around tales such as {', '.join(story_titles)}. Taken together, they show a setting where personal decisions, factional maneuvers, and larger catastrophes are feeding one another instead of unfolding in separate lanes."
        )
    if not paragraphs:
        paragraphs.append("No chronicle-worthy material was available to summarize yet.")
    current_state = []
    for row in top_events[:4]:
        name = str(row.get("name") or "").strip()
        stage = str(row.get("stage") or "").strip()
        if name:
            current_state.append(f"{name} is currently at {stage or 'an active'} stage.")
    if not current_state:
        current_state.append("The archive has not yet accumulated enough linked material for a full chronicle state panel.")
    ongoing = []
    for row in top_events[:4]:
        text = str(row.get("summary") or "").strip()
        match = re.search(r"OPEN THREADS:\s*(.*)", text, flags=re.IGNORECASE | re.DOTALL)
        if not match:
            continue
        for line in match.group(1).splitlines():
            line = line.strip()
            if line.startswith("-"):
                ongoing.append(line.lstrip("- ").strip())
            if len(ongoing) >= 6:
                break
        if len(ongoing) >= 6:
            break
    if not ongoing:
        ongoing = ["A full Haiku chronicle synthesis has not yet been produced."]
    return {
        "title": title,
        "dek": dek,
        "chronicle": "\n\n".join(paragraphs),
        "current_state": current_state[:6],
        "ongoing_threads": ongoing[:8],
        "model": "fallback-v1",
    }


def _build_json_repair_prompt(invalid_response: str) -> str:
    return f"""You previously responded to a chronicle request with malformed JSON.

Rewrite the response below as valid JSON only.

Requirements:
- Return ONLY a JSON object.
- Do not use markdown fences.
- Preserve the prose and bullet text as faithfully as possible.
- The object must contain exactly these keys:
  - title: string
  - dek: string
  - chronicle: string
  - current_state: array of strings
  - ongoing_threads: array of strings

Malformed response:
{invalid_response}
"""


def _call_model_once(client: anthropic.Anthropic, prompt: str) -> str:
    msg = client.messages.create(
        model=MODEL,
        max_tokens=MAX_TOKENS,
        messages=[{"role": "user", "content": prompt}],
    )
    return str(msg.content[0].text or "").strip()


def _call_model(prompt: str, api_key: str) -> dict[str, Any]:
    client = anthropic.Anthropic(api_key=api_key)
    text = _call_model_once(client, prompt)
    try:
        return _parse_model_json(text)
    except Exception:
        repaired_text = _call_model_once(client, _build_json_repair_prompt(_normalize_model_payload(text)))
        payload = _parse_model_json(repaired_text)
        if not isinstance(payload, dict):
            raise ValueError("Chronicle model response was not a JSON object")
        return payload


def _build_empty_payload() -> dict[str, Any]:
    return {
        "generated_at": _now_iso(),
        "cadence_days": CADENCE_DAYS,
        "entries": [],
        "latest_entry": None,
    }


def main() -> int:
    _maybe_load_dotenv()

    parser = argparse.ArgumentParser(description="Build a periodic world chronicle for The Daily Blade.")
    parser.add_argument("--force", action="store_true", help="Force a chronicle build regardless of cadence.")
    args = parser.parse_args()

    force = bool(args.force or (os.environ.get("FORCE_CHRONICLE_REGEN") or "").strip().lower() in {"1", "true", "yes", "y"})

    codex = _load_json(CODEX_FILE, {})
    archive_idx = _load_json(ARCHIVE_INDEX_FILE, {"dates": []})
    chronicle_history = _clean_history(_load_json(OUTPUT_FILE, _build_empty_payload()))
    world_events_payload = _load_json(WORLD_EVENTS_FILE, {"events": []})

    archive_dates_desc = archive_idx.get("dates") if isinstance(archive_idx, dict) and isinstance(archive_idx.get("dates"), list) else []
    archive_dates_desc = [str(value).strip() for value in archive_dates_desc if _parse_date_key(str(value).strip())]
    if not archive_dates_desc:
        print(f"WARNING: No archive dates found in {ARCHIVE_INDEX_FILE}; chronicle not built.", file=sys.stderr)
        if not OUTPUT_FILE.exists():
            _write_json(OUTPUT_FILE, chronicle_history)
        return 0

    end_date_key = archive_dates_desc[0]
    latest_entry = chronicle_history.get("latest_entry") if isinstance(chronicle_history.get("latest_entry"), dict) else None
    latest_entry_date = str(latest_entry.get("date") or "").strip() if latest_entry else ""
    due = True
    if latest_entry_date and not force:
        prev = _parse_date_key(latest_entry_date)
        curr = _parse_date_key(end_date_key)
        if prev and curr:
            due = (curr - prev).days >= CADENCE_DAYS

    if not due and chronicle_history.get("entries"):
        print(f"Chronicle not due yet (latest: {latest_entry_date}, cadence: {CADENCE_DAYS} days).")
        return 0

    prior_entries = chronicle_history.get("entries") if isinstance(chronicle_history.get("entries"), list) else []
    last_completed_window_date = latest_entry_date or None
    earlier_entry_dates = []
    for row in prior_entries:
        if not isinstance(row, dict):
            continue
        row_date = str(row.get("date") or "").strip()
        if row_date and row_date != end_date_key:
            earlier_entry_dates.append(row_date)
    use_full_history_window = not earlier_entry_dates
    if latest_entry_date == end_date_key:
        last_completed_window_date = None
        for row in prior_entries:
            if not isinstance(row, dict):
                continue
            row_date = str(row.get("date") or "").strip()
            if row_date and row_date != end_date_key:
                last_completed_window_date = row_date
                break

    window_dates = _select_window_dates(
        archive_dates_desc,
        end_date_key,
        last_completed_window_date,
        CADENCE_DAYS,
        full_history=use_full_history_window,
    )
    max_world_events = _cap_for_run(MAX_WORLD_EVENTS, MAX_WORLD_EVENTS_FULL_HISTORY, use_full_history_window)
    max_entities = _cap_for_run(MAX_ENTITIES, MAX_ENTITIES_FULL_HISTORY, use_full_history_window)
    max_stories = _cap_for_run(MAX_STORIES, MAX_STORIES_FULL_HISTORY, use_full_history_window)
    max_connections = _cap_for_run(MAX_CONNECTIONS, MAX_CONNECTIONS_FULL_HISTORY, use_full_history_window)
    stories, issue_numbers = _load_window_stories(window_dates, archive_dates_desc)
    world_events = _rank_world_events(world_events_payload, {row.get("date") for row in stories if row.get("date")}, max_world_events)
    ranked_entities, top_entities, top_connections = _rank_entities(codex, stories, world_events, CADENCE_DAYS, max_entities, max_connections)
    dossier = _build_dossier(window_dates, stories, world_events, ranked_entities, top_entities, top_connections, chronicle_history, max_stories)

    issue_number = _issue_number(archive_dates_desc, end_date_key)
    api_key = (os.environ.get("ANTHROPIC_API_KEY") or "").strip()

    if api_key:
        try:
            chronicle_core = _call_model(_build_prompt(end_date_key, issue_number, dossier), api_key)
            model_name = MODEL
        except Exception as exc:
            if not ALLOW_FALLBACK:
                raise
            print(f"WARNING: Chronicle synthesis failed; using fallback summary: {exc}", file=sys.stderr)
            chronicle_core = _fallback_entry(end_date_key, issue_number, dossier)
            model_name = chronicle_core.get("model") or "fallback-v1"
    else:
        if not ALLOW_FALLBACK:
            raise SystemExit("ERROR: ANTHROPIC_API_KEY is required to build the chronicle.")
        chronicle_core = _fallback_entry(end_date_key, issue_number, dossier)
        model_name = chronicle_core.get("model") or "fallback-v1"

    entry = {
        "date": end_date_key,
        "issue_number": issue_number,
        "window_start": dossier["window"]["start_date"],
        "window_end": dossier["window"]["end_date"],
        "generated_at": _now_iso(),
        "model": model_name,
        "title": str(chronicle_core.get("title") or f"Chronicle of Issue {issue_number:03d}" if isinstance(issue_number, int) else f"Chronicle of {end_date_key}").strip(),
        "dek": str(chronicle_core.get("dek") or "A periodic chronicle of Edhra and Valdris.").strip(),
        "chronicle": str(chronicle_core.get("chronicle") or "").strip(),
        "current_state": [str(x).strip() for x in (chronicle_core.get("current_state") if isinstance(chronicle_core.get("current_state"), list) else []) if str(x).strip()],
        "ongoing_threads": [str(x).strip() for x in (chronicle_core.get("ongoing_threads") if isinstance(chronicle_core.get("ongoing_threads"), list) else []) if str(x).strip()],
        "pipeline": dossier["pipeline"],
        "input_stats": {
            "issue_count": dossier["window"]["issue_count"],
            "story_count": dossier["window"]["story_count"],
            "selected_world_events": len(dossier.get("world_events") or []),
            "selected_entities": len(dossier.get("selected_entities") or []),
            "selected_stories": len(dossier.get("selected_stories") or []),
        },
    }

    kept = [row for row in prior_entries if isinstance(row, dict) and str(row.get("date") or "").strip() != end_date_key]
    kept.insert(0, entry)
    kept = kept[: max(1, KEEP_ENTRIES)]

    payload = {
        "generated_at": _now_iso(),
        "cadence_days": CADENCE_DAYS,
        "latest_entry": kept[0],
        "entries": kept,
    }
    _write_json(OUTPUT_FILE, payload)
    _archive_chronicle_entry(entry)
    print(
        f"✓ Wrote {OUTPUT_FILE.name} "
        f"({entry['title']} · {entry['window_start']} → {entry['window_end']} · model={entry['model']})"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())