#!/usr/bin/env python3
"""Build lineage/union codex sidecar from character and temporal data."""

from __future__ import annotations

import argparse
import json
import os
import re
from datetime import datetime, timezone
from typing import Any


DEFAULT_CODEX_FILE = "codex.json"
DEFAULT_TEMPORAL_FILE = "character-temporal.json"
DEFAULT_OUTPUT_FILE = "lineages.json"

MARRIAGE_PATTERN = re.compile(r"\bmarried\b|\bmarriage\b|\bwedding\b|\bwed\b|\bwife\b|\bhusband\b|\bconsort\b", re.IGNORECASE)
ROMANCE_PATTERN = re.compile(r"\bfell in love\b|\bin love\b|\blover\b|\bbeloved\b", re.IGNORECASE)
PARTNER_STATUS_PATTERN = re.compile(r"\bpartner(?:ed)?\s+(?:to|with)\b", re.IGNORECASE)
FAMILY_RED_FLAG_PATTERN = re.compile(r"\bsister\b|\bdaughter\b|\bson\b|\bmother\b|\bfather\b|\bbrother\b|\bgrandmother\b|\bgrandfather\b|\bkin\b", re.IGNORECASE)


def _load_json(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _write_json(path: str, data: Any) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=True, indent=2)
        f.write("\n")


def _norm(value: str) -> str:
    return str(value or "").strip().lower()


def _pair_key(a: str, b: str) -> tuple[str, str]:
    aa = str(a or "").strip()
    bb = str(b or "").strip()
    return tuple(sorted((aa, bb), key=lambda item: item.lower()))


def _relationship_type(source_type: str, text: str, other_name: str) -> str | None:
    raw = str(text or "")
    if not raw or not other_name:
        return None
    other_re = re.compile(rf"(?<![\w']){re.escape(other_name.lower())}(?![\w'])")
    sentences = [seg.strip() for seg in re.split(r"(?<=[.!?])\s+", raw) if seg.strip()]
    direct_partner_re = re.compile(rf"\bpartner(?:ed)?\s+(?:to|with)\s+{re.escape(other_name.lower())}\b", re.IGNORECASE)

    if source_type.startswith("status") and direct_partner_re.search(raw.lower()):
        return "partnership"

    for sentence in sentences:
        lowered = sentence.lower()
        name_match = other_re.search(lowered)
        if not name_match:
            continue
        romance_patterns = (
            re.compile(rf"{re.escape(other_name.lower())}[^.]{0,80}\bfell in love\b", re.IGNORECASE),
            re.compile(rf"\bfell in love\b[^.]{0,80}{re.escape(other_name.lower())}", re.IGNORECASE),
            re.compile(rf"\bbeloved\s+{re.escape(other_name.lower())}\b", re.IGNORECASE),
            re.compile(rf"\blover\s+{re.escape(other_name.lower())}\b", re.IGNORECASE),
        )
        if any(pattern.search(lowered) for pattern in romance_patterns):
            return "romance"
        marriage_patterns = (
            re.compile(rf"\bmarried\s+(?:to\s+)?{re.escape(other_name.lower())}\b", re.IGNORECASE),
            re.compile(rf"\bwed\s+{re.escape(other_name.lower())}\b", re.IGNORECASE),
            re.compile(rf"\bwife\s+{re.escape(other_name.lower())}\b", re.IGNORECASE),
            re.compile(rf"\bhusband\s+{re.escape(other_name.lower())}\b", re.IGNORECASE),
            re.compile(rf"\bconsort\s+{re.escape(other_name.lower())}\b", re.IGNORECASE),
            re.compile(rf"{re.escape(other_name.lower())}[^.]{0,40}\bconsort\b", re.IGNORECASE),
        )
        if any(pattern.search(lowered) for pattern in marriage_patterns):
            return "marriage"
        if FAMILY_RED_FLAG_PATTERN.search(sentence):
            continue
    return None


def _build_name_patterns(characters: list[dict[str, Any]]) -> list[tuple[str, re.Pattern[str]]]:
    items: list[tuple[str, re.Pattern[str]]] = []
    seen: set[str] = set()
    for char in characters:
        if not isinstance(char, dict):
            continue
        name = str(char.get("name") or "").strip()
        if not name:
            continue
        key = _norm(name)
        if key in seen:
            continue
        seen.add(key)
        items.append((name, re.compile(rf"(?<![\w']){re.escape(name.lower())}(?![\w'])")))
    items.sort(key=lambda item: len(item[0]), reverse=True)
    return items


def _mentioned_character_names(text: str, *, self_name: str, patterns: list[tuple[str, re.Pattern[str]]]) -> list[str]:
    lowered = str(text or "").lower()
    out: list[str] = []
    self_key = _norm(self_name)
    for name, pattern in patterns:
        key = _norm(name)
        if not key or key == self_key:
            continue
        if pattern.search(lowered):
            out.append(name)
    return out


def _initial_status(partner_a: str, partner_b: str, temporal_by_name: dict[str, dict[str, Any]]) -> str:
    a_temporal = temporal_by_name.get(_norm(partner_a), {})
    b_temporal = temporal_by_name.get(_norm(partner_b), {})
    if a_temporal.get("alive", True) is False or b_temporal.get("alive", True) is False:
        return "historical"
    return "ongoing"


def _ensure_union(unions: dict[tuple[str, str], dict[str, Any]], partner_a: str, partner_b: str, temporal_by_name: dict[str, dict[str, Any]]) -> dict[str, Any]:
    key = _pair_key(partner_a, partner_b)
    if key not in unions:
        unions[key] = {
            "name": f"{key[0]} & {key[1]}",
            "partners": [key[0], key[1]],
            "union_type": "lineage",
            "status": _initial_status(key[0], key[1], temporal_by_name),
            "children": [],
            "story_appearances": [],
            "evidence": [],
            "summary": "",
        }
    return unions[key]


def _append_story_appearance(union: dict[str, Any], date_key: str, title: str) -> None:
    if not date_key or not title:
        return
    apps = union.setdefault("story_appearances", [])
    key = (_norm(date_key), _norm(title))
    if any((_norm(row.get("date") or ""), _norm(row.get("title") or "")) == key for row in apps if isinstance(row, dict)):
        return
    apps.append({"date": date_key, "title": title})


def _append_evidence(union: dict[str, Any], text: str) -> None:
    raw = str(text or "").strip()
    if not raw:
        return
    evidence = union.setdefault("evidence", [])
    if raw not in evidence:
        evidence.append(raw)


def build_lineage_payload(codex_path: str = DEFAULT_CODEX_FILE, temporal_path: str = DEFAULT_TEMPORAL_FILE) -> dict[str, Any]:
    codex = _load_json(codex_path)
    temporal_payload = _load_json(temporal_path) if os.path.exists(temporal_path) else {}
    characters = codex.get("characters") if isinstance(codex, dict) else []
    if not isinstance(characters, list):
        characters = []
    temporal_rows = temporal_payload.get("characters") if isinstance(temporal_payload, dict) else []
    if not isinstance(temporal_rows, list):
        temporal_rows = []

    temporal_by_name = {
        _norm(row.get("name") or ""): (row.get("temporal") if isinstance(row, dict) and isinstance(row.get("temporal"), dict) else {})
        for row in temporal_rows
        if isinstance(row, dict) and str(row.get("name") or "").strip()
    }
    patterns = _build_name_patterns(characters)
    unions: dict[tuple[str, str], dict[str, Any]] = {}

    for char in characters:
        if not isinstance(char, dict):
            continue
        name = str(char.get("name") or "").strip()
        if not name:
            continue
        sources: list[tuple[str, str, str, str]] = []
        sources.append(("status", str(char.get("status") or ""), str(char.get("first_date") or ""), str(char.get("first_story") or "")))
        sources.append(("bio", str(char.get("bio") or ""), str(char.get("first_date") or ""), str(char.get("first_story") or "")))
        history = char.get("status_history") if isinstance(char.get("status_history"), list) else []
        for ev in history:
            if not isinstance(ev, dict):
                continue
            sources.append(("status_history_note", str(ev.get("note") or ""), str(ev.get("date") or ""), str(ev.get("story_title") or "")))
            sources.append(("status_history_evidence", str(ev.get("evidence") or ""), str(ev.get("date") or ""), str(ev.get("story_title") or "")))

        for source_type, text, date_key, story_title in sources:
            others = _mentioned_character_names(text, self_name=name, patterns=patterns)
            for other in others:
                union_type = _relationship_type(source_type, text, other)
                if not union_type:
                    continue
                union = _ensure_union(unions, name, other, temporal_by_name)
                if union_type == "marriage":
                    union["union_type"] = "marriage"
                elif union_type == "romance" and union.get("union_type") != "marriage":
                    union["union_type"] = "romance"
                elif union.get("union_type") == "lineage":
                    union["union_type"] = "partnership"
                _append_story_appearance(union, date_key, story_title)
                _append_evidence(union, f"{source_type}: {text.strip()[:240]}")

    for row in temporal_rows:
        if not isinstance(row, dict):
            continue
        child_name = str(row.get("name") or "").strip()
        temporal = row.get("temporal") if isinstance(row.get("temporal"), dict) else {}
        lineage = temporal.get("lineage") if isinstance(temporal.get("lineage"), dict) else {}
        parents = [str(x or "").strip() for x in (lineage.get("parents") or []) if str(x or "").strip()]
        if len(parents) < 2:
            continue
        union = _ensure_union(unions, parents[0], parents[1], temporal_by_name)
        if child_name and child_name not in union.setdefault("children", []):
            union["children"].append(child_name)
        union["status"] = "ongoing" if temporal.get("alive", True) else union.get("status") or "historical"
        first_date = str(temporal.get("first_recorded_date") or "")
        _append_evidence(union, f"lineage: child {child_name}")
        if first_date:
            _append_story_appearance(union, first_date, child_name)

    rows: list[dict[str, Any]] = []
    for union in unions.values():
        apps = sorted(
            union.get("story_appearances") or [],
            key=lambda row: (str(row.get("date") or ""), str(row.get("title") or "")),
        )
        children = sorted(set(str(child or "").strip() for child in (union.get("children") or []) if str(child or "").strip()), key=str.lower)
        partners = union.get("partners") or []
        first_story = str(apps[0].get("title") or "") if apps else ""
        first_date = str(apps[0].get("date") or "") if apps else ""
        union_type = str(union.get("union_type") or "lineage")
        status = str(union.get("status") or "ongoing")
        summary = f"{partners[0]} and {partners[1]} are recorded in an ongoing {union_type}."
        if status != "ongoing":
            summary = f"{partners[0]} and {partners[1]} are recorded in a historical {union_type}."
        if children:
            summary += f" Their lineage currently records {len(children)} child{'ren' if len(children) != 1 else ''}."
        rows.append({
            "name": str(union.get("name") or "").strip(),
            "partners": partners,
            "union_type": union_type,
            "status": status,
            "children": children,
            "summary": summary,
            "appearances": len(apps),
            "first_story": first_story,
            "first_date": first_date,
            "story_appearances": apps,
            "evidence": list(union.get("evidence") or [])[:6],
        })

    rows.sort(key=lambda item: _norm(item.get("name") or ""))
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source_codex": codex_path,
        "source_temporal": temporal_path,
        "count": len(rows),
        "lineages": rows,
    }


def refresh_lineages(
    codex_path: str = DEFAULT_CODEX_FILE,
    temporal_path: str = DEFAULT_TEMPORAL_FILE,
    output_path: str = DEFAULT_OUTPUT_FILE,
) -> dict[str, Any]:
    payload = build_lineage_payload(codex_path=codex_path, temporal_path=temporal_path)
    _write_json(output_path, payload)
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description="Build lineage sidecar payload from codex and temporal data.")
    parser.add_argument("--codex", default=DEFAULT_CODEX_FILE)
    parser.add_argument("--temporal", default=DEFAULT_TEMPORAL_FILE)
    parser.add_argument("--output", default=DEFAULT_OUTPUT_FILE)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    payload = build_lineage_payload(codex_path=args.codex, temporal_path=args.temporal)
    if args.dry_run:
        print(f"Lineage rows: {payload.get('count', 0)}")
        return 0

    _write_json(args.output, payload)
    print(f"Wrote {args.output} with {payload.get('count', 0)} lineages")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())