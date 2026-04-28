#!/usr/bin/env python3
"""Build character temporal metadata from existing codex records.

Phase 1 output is a sidecar payload (`character-temporal.json`) consumed by the UI.
This avoids rewriting large canonical JSON files while still retrofitting all
existing characters with age and lifecycle context.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
from datetime import datetime, timezone
from typing import Any

try:
    import anthropic
except Exception:  # pragma: no cover - dependency may be absent in some contexts
    anthropic = None

from character_story_tools import gather_story_texts_for_character, load_story_catalog
from world_time import build_world_clock


DEFAULT_CODEX_FILE = "codex.json"
DEFAULT_OUTPUT_FILE = "character-temporal.json"
DEFAULT_MODEL = "claude-haiku-4-5-20251001"

MORTAL_PROFILE = "mortal_humanlike"
LONG_LIVED_PROFILE = "long_lived_supernatural"
AGELESS_PROFILE = "ageless_supernatural"

WORD_NUMBERS = {
    "one": 1,
    "two": 2,
    "three": 3,
    "four": 4,
    "five": 5,
    "six": 6,
    "seven": 7,
    "eight": 8,
    "nine": 9,
    "ten": 10,
    "eleven": 11,
    "twelve": 12,
    "thirteen": 13,
    "fourteen": 14,
    "fifteen": 15,
    "sixteen": 16,
    "seventeen": 17,
    "eighteen": 18,
    "nineteen": 19,
    "twenty": 20,
    "thirty": 30,
    "forty": 40,
    "fifty": 50,
    "sixty": 60,
    "seventy": 70,
    "eighty": 80,
    "ninety": 90,
}

AGELESS_MARKERS = (
    "deity",
    "god",
    "goddess",
    "demon",
    "daemon",
    "devil",
    "entity",
    "eldritch",
    "lich",
    "undead",
    "ghost",
    "specter",
    "wraith",
    "shade",
    "immortal",
    "eternal",
    "ageless",
    "neither living nor dead",
)

LONG_LIVED_MARKERS = (
    "spirit",
    "dragon",
    "wyvern",
    "guardian",
    "ancient being",
    "old magic",
    "predates kingdoms",
    "older than kingdoms",
    "for centuries",
    "for millennia",
    "thousand years",
)


def _load_json(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _write_json(path: str, data: Any) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=True, indent=2)
        f.write("\n")


def _hash_bucket(name: str) -> int:
    digest = hashlib.sha1(str(name or "").encode("utf-8"), usedforsecurity=False).hexdigest()
    return int(digest[:8], 16)


def _pick_in_range(name: str, low: int, high: int) -> int:
    if high <= low:
        return low
    span = high - low + 1
    return low + (_hash_bucket(name) % span)


def _parse_number_token(token: str) -> int | None:
    raw = str(token or "").strip().lower().replace("-", " ")
    if not raw:
        return None
    if raw.isdigit():
        return int(raw)
    parts = [p for p in raw.split() if p]
    total = 0
    for part in parts:
        if part not in WORD_NUMBERS:
            return None
        total += WORD_NUMBERS[part]
    return total or None


def _heuristic_role_age(name: str, role: str, status: str, aging_profile: str) -> int:
    text = f"{role} {status}".lower()

    if aging_profile == AGELESS_PROFILE:
        return _pick_in_range(name, 400, 4000)
    if aging_profile == LONG_LIVED_PROFILE:
        return _pick_in_range(name, 120, 900)

    if any(k in text for k in ("child", "boy", "girl", "orphan", "infant")):
        return _pick_in_range(name, 8, 15)
    if any(k in text for k in ("apprentice", "initiate", "pupil", "novice", "student", "young")):
        return _pick_in_range(name, 14, 22)
    if any(k in text for k in ("heir", "prince", "princess", "squire")):
        return _pick_in_range(name, 16, 30)
    if any(k in text for k in ("elder", "matriarch", "patriarch", "old", "ancient", "hermit")):
        return _pick_in_range(name, 50, 74)
    if any(k in text for k in ("captain", "commander", "general", "auditor", "merchant", "sorcerer", "mage", "broker", "priest")):
        return _pick_in_range(name, 27, 49)

    return _pick_in_range(name, 22, 42)


def _story_text_segments(name: str, aliases: list[str], source: str, text: str) -> list[str]:
    raw = str(text or "").strip()
    if not raw:
        return []
    if not source.startswith("story:"):
        return [raw]

    lowered_name = str(name or "").strip().lower()
    alias_terms = [str(alias or "").strip().lower() for alias in aliases if str(alias or "").strip()]
    name_tokens = [tok for tok in re.findall(r"[a-zA-Z']+", lowered_name) if len(tok) >= 4]
    alias_tokens = [tok for alias in alias_terms for tok in re.findall(r"[a-zA-Z']+", alias) if len(tok) >= 4]
    match_terms = sorted(set(([lowered_name] if lowered_name else []) + alias_terms + name_tokens + alias_tokens), key=len, reverse=True)
    if not match_terms:
        return [raw]

    segments = [seg.strip() for seg in re.split(r"(?<=[.!?])\s+", raw) if seg.strip()]
    relevant = [seg for seg in segments if any(term and term in seg.lower() for term in match_terms)]
    return relevant or []


def _infer_aging_profile(char: dict[str, Any], story_catalog: dict[tuple[str, str], dict[str, Any]]) -> tuple[str, list[str]]:
    name = str(char.get("name") or "").strip()
    aliases = [str(x or "").strip() for x in (char.get("aliases") or []) if str(x or "").strip()]
    fields = [
        ("role", str(char.get("role") or "")),
        ("tagline", str(char.get("tagline") or "")),
        ("bio", str(char.get("bio") or "")),
        ("notes", str(char.get("notes") or "")),
        ("traits", " ; ".join(str(x or "") for x in (char.get("traits") or []))),
        ("aliases", " ; ".join(str(x or "") for x in (char.get("aliases") or []))),
    ]
    story_rows = gather_story_texts_for_character(char, story_catalog, max_stories=6)
    for row in story_rows:
        fields.append((f"story:{row['date']}:{row['title']}", row.get("text") or ""))

    age_less_hits: list[str] = []
    long_lived_hits: list[str] = []
    for source, text in fields:
        for segment in _story_text_segments(name, aliases, source, text):
            lowered = str(segment or "").lower()
            for marker in AGELESS_MARKERS:
                if marker in lowered:
                    age_less_hits.append(f"{marker} [{source}]")
            for marker in LONG_LIVED_MARKERS:
                if marker in lowered:
                    long_lived_hits.append(f"{marker} [{source}]")

    if age_less_hits:
        return AGELESS_PROFILE, age_less_hits[:5]
    if long_lived_hits:
        return LONG_LIVED_PROFILE, long_lived_hits[:5]
    return MORTAL_PROFILE, []


def _age_votes_from_text(source: str, text: str, aging_profile: str) -> list[dict[str, Any]]:
    votes: list[dict[str, Any]] = []

    def add_vote(low: int, high: int, weight: float, reason: str) -> None:
        votes.append({
            "low": low,
            "high": high,
            "weight": weight,
            "reason": reason,
            "source": source,
        })

    raw = str(text or "").strip()
    if not raw:
        return votes
    lowered = raw.lower()

    for pattern in (
        r"\b(\d{1,3})\s*[- ]year[- ]old\b",
        r"\baged\s+(\d{1,3})\b",
        r"\bat\s+(\d{1,3})\s*,\b",
    ):
        for match in re.finditer(pattern, lowered):
            exact = int(match.group(1))
            add_vote(exact, exact, 12.0, f"explicit age mention: {match.group(0)}")

    phrase_bands = [
        ((6, 12, 10.0), [r"\binfant\b", r"\btoddler\b", r"\blittle boy\b", r"\blittle girl\b", r"\bsmall child\b"]),
        ((8, 15, 9.0), [r"\bchild\b", r"\bboy\b", r"\bgirl\b", r"\borphan\b"]),
        ((13, 19, 10.0), [r"\bteenager\b", r"\badolescent\b", r"\byouth\b", r"\byoungster\b"]),
        ((15, 24, 9.5), [r"\byoung\s+[a-z\-]+\b", r"\byoung man\b", r"\byoung woman\b", r"\byoung scribe\b"]),
        ((14, 24, 8.5), [r"\bapprentice\b", r"\binitiate\b", r"\bpupil\b", r"\bnovice\b", r"\bstudent\b"]),
        ((35, 55, 7.0), [r"\bmiddle-aged\b", r"\bmiddle aged\b"]),
        ((55, 82, 10.0), [r"\belderly\b", r"\bold woman\b", r"\bold man\b", r"\baged\b", r"\bgrandmother\b", r"\bgrandfather\b", r"\bmatriarch\b", r"\bpatriarch\b", r"\belder\b"]),
    ]
    for (low, high, weight), patterns in phrase_bands:
        for pattern in patterns:
            if re.search(pattern, lowered):
                add_vote(low, high, weight, f"age cue: {pattern}")

    if aging_profile != MORTAL_PROFILE:
        immortal_bands = [
            ((120, 450, 8.0), [r"\bfor centuries\b", r"\bcenturies old\b"]),
            ((400, 2400, 9.0), [r"\bancient\b", r"\bold magic\b", r"\bpredates kingdoms\b", r"\bolder than kingdoms\b"]),
            ((600, 5000, 10.0), [r"\bimmortal\b", r"\beternal\b", r"\bageless\b"]),
            ((1200, 8000, 10.5), [r"\bpre-human\b", r"\bolder than human kingdoms\b"]),
        ]
        for (low, high, weight), patterns in immortal_bands:
            for pattern in patterns:
                if re.search(pattern, lowered):
                    add_vote(low, high, weight, f"supernatural age cue: {pattern}")

        for match in re.finditer(r"\bfor\s+([a-z\-]+|\d{1,3})\s+centuries\b", lowered):
            centuries = _parse_number_token(match.group(1))
            if centuries is None:
                continue
            base = centuries * 100
            add_vote(base, base + 180, 11.0, f"centuries duration cue: {match.group(0)}")

        for match in re.finditer(r"\bfor\s+([a-z\-]+|\d{1,5})\s+thousand\s+years\b", lowered):
            thousands = _parse_number_token(match.group(1))
            if thousands is None:
                continue
            base = thousands * 1000
            add_vote(base, base + 250, 12.0, f"millennial duration cue: {match.group(0)}")

        for match in re.finditer(r"\b(\d{1,5})\s*[- ]year[- ]old\b", lowered):
            exact = int(match.group(1))
            if exact > 120:
                add_vote(exact, exact, 12.0, f"explicit supernatural age mention: {match.group(0)}")

    years_patterns = [
        r"\bfor\s+([a-z\-]+|\d{1,3})\s+years\b",
        r"\bspent\s+([a-z\-]+|\d{1,3})\s+years\b",
        r"\bafter\s+([a-z\-]+|\d{1,3})\s+years\b",
        r"\b([a-z\-]+|\d{1,3})\s+years\s+later\b",
    ]
    for pattern in years_patterns:
        for match in re.finditer(pattern, lowered):
            years = _parse_number_token(match.group(1))
            if years is None:
                continue
            # Someone with X years of remembered work/life history is unlikely
            # to be younger than early adolescence + X.
            if aging_profile == MORTAL_PROFILE:
                min_age = max(14, years + 12)
                max_age = min(90, years + 40)
            elif aging_profile == LONG_LIVED_PROFILE:
                min_age = max(24, years + 40)
                max_age = min(1500, years + 220)
            else:
                min_age = max(80, years + 100)
                max_age = min(8000, years + 900)
            add_vote(min_age, max_age, 5.5, f"duration cue: {match.group(0)}")

    return votes


def _estimate_age_from_story_evidence(char: dict[str, Any], story_catalog: dict[tuple[str, str], dict[str, Any]]) -> tuple[int, list[str], str]:
    name = str(char.get("name") or "").strip()
    role = str(char.get("role") or "")
    status = str(char.get("status") or "")
    aliases = [str(x or "").strip() for x in (char.get("aliases") or []) if str(x or "").strip()]
    aging_profile, profile_evidence = _infer_aging_profile(char, story_catalog)
    heuristic_age = _heuristic_role_age(name, role, status, aging_profile)

    story_rows = gather_story_texts_for_character(char, story_catalog, max_stories=0)
    text_sources: list[tuple[str, str]] = [
        ("role", role),
        ("tagline", str(char.get("tagline") or "")),
        ("bio", str(char.get("bio") or "")),
        ("notes", str(char.get("notes") or "")),
        ("aliases", " ; ".join(str(x or "") for x in (char.get("aliases") or []))),
    ]
    history = char.get("status_history") if isinstance(char.get("status_history"), list) else []
    for ev in history:
        if not isinstance(ev, dict):
            continue
        text_sources.append((f"status_history:{ev.get('story_title') or ''}", str(ev.get("note") or "")))
        text_sources.append((f"status_history_evidence:{ev.get('story_title') or ''}", str(ev.get("evidence") or "")))
    for row in story_rows:
        text_sources.append((f"story:{row['date']}:{row['title']}", row.get("text") or ""))

    votes: list[dict[str, Any]] = []
    for source, text in text_sources:
        for segment in _story_text_segments(name, aliases, source, text):
            votes.extend(_age_votes_from_text(source, segment, aging_profile))

    # Always include the role/status heuristic as a low-weight fallback.
    votes.append({
        "low": max(0, heuristic_age - 3),
        "high": heuristic_age + 3,
        "weight": 2.0,
        "reason": "role/status heuristic",
        "source": "heuristic",
    })

    total_weight = sum(float(v["weight"]) for v in votes)
    weighted_low = sum(float(v["low"]) * float(v["weight"]) for v in votes) / total_weight
    weighted_high = sum(float(v["high"]) * float(v["weight"]) for v in votes) / total_weight
    estimated = int(round((weighted_low + weighted_high) / 2.0))
    max_age = 90 if aging_profile == MORTAL_PROFILE else (2000 if aging_profile == LONG_LIVED_PROFILE else 12000)
    estimated = max(0, min(max_age, estimated))

    non_heuristic = [v for v in votes if v.get("source") != "heuristic"]
    confidence = "high" if len(non_heuristic) >= 3 else ("medium" if len(non_heuristic) >= 1 else "low")
    evidence = [f"{v['reason']} [{v['source']}]" for v in sorted(votes, key=lambda item: float(item['weight']), reverse=True)[:5]]
    if profile_evidence:
        evidence = profile_evidence[:2] + evidence
    return estimated, evidence[:5], confidence


def _life_stage(age_years: float, aging_profile: str) -> str:
    if aging_profile == AGELESS_PROFILE:
        return "ageless"
    if aging_profile == LONG_LIVED_PROFILE:
        if age_years < 30:
            return "young"
        if age_years < 150:
            return "prime"
        if age_years < 500:
            return "venerable"
        return "ancient"
    if age_years < 13:
        return "child"
    if age_years < 20:
        return "youth"
    if age_years < 45:
        return "adult"
    if age_years < 65:
        return "mature"
    return "elder"


def _infer_alive(status: str) -> bool:
    s = str(status or "").strip().lower()
    if not s:
        return True
    return not ("dead" in s or "deceased" in s)


def _extract_death_date(char: dict[str, Any]) -> str | None:
    history = char.get("status_history")
    if not isinstance(history, list):
        return None
    best = None
    for ev in history:
        if not isinstance(ev, dict):
            continue
        to_status = str(ev.get("to_status") or "").strip().lower()
        if "dead" not in to_status and "deceased" not in to_status:
            continue
        d = str(ev.get("date") or "").strip()
        if d and (best is None or d > best):
            best = d
    return best


def _first_recorded_date(char: dict[str, Any], latest_issue_date: str | None) -> str | None:
    first = str(char.get("first_date") or "").strip()
    if first:
        return first

    apps = char.get("story_appearances") if isinstance(char.get("story_appearances"), list) else []
    dates = sorted({
        str(a.get("date") or "").strip()
        for a in apps
        if isinstance(a, dict) and str(a.get("date") or "").strip()
    })
    if dates:
        return dates[0]

    return latest_issue_date


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _age_rate_for_profile(aging_profile: str) -> float:
    if aging_profile == LONG_LIVED_PROFILE:
        return 0.18
    if aging_profile == AGELESS_PROFILE:
        return 0.02
    return 1.0


def _max_age_for_profile(aging_profile: str) -> int:
    if aging_profile == LONG_LIVED_PROFILE:
        return 2000
    if aging_profile == AGELESS_PROFILE:
        return 12000
    return 90


def _health_defaults(name: str, current_age: float, aging_profile: str) -> tuple[float, float, float, str]:
    if aging_profile == MORTAL_PROFILE:
        frailty = _clamp((current_age - 35.0) / 55.0, 0.05, 0.95)
        baseline_vitality = round(_clamp(1.0 - frailty * 0.85, 0.05, 0.99), 3)
        resilience = _clamp(1.0 - frailty + ((_hash_bucket(name) % 7) - 3) * 0.02, 0.05, 0.95)
        condition_profile = "humanlike"
    elif aging_profile == LONG_LIVED_PROFILE:
        frailty = _clamp((current_age - 800.0) / 6000.0, 0.02, 0.18)
        baseline_vitality = round(_clamp(0.96 - frailty * 0.25, 0.78, 0.995), 3)
        resilience = _clamp(0.88 - frailty * 0.1 + ((_hash_bucket(name) % 7) - 3) * 0.015, 0.55, 0.99)
        condition_profile = "supernatural"
    else:
        frailty = 0.02
        baseline_vitality = 0.99
        resilience = _clamp(0.9 + ((_hash_bucket(name) % 7) - 3) * 0.01, 0.75, 0.99)
        condition_profile = "ageless_supernatural"
    return baseline_vitality, round(frailty, 3), round(resilience, 3), condition_profile


def _memory_horizon_years(current_age: float, aging_profile: str) -> float:
    if aging_profile == MORTAL_PROFILE:
        return round(min(current_age, 60.0), 1)
    if aging_profile == LONG_LIVED_PROFILE:
        return round(max(80.0, current_age * 0.75), 1)
    return round(max(120.0, current_age), 1)


def _finalize_temporal_fields(
    *,
    temporal: dict[str, Any],
    name: str,
    appearances: int,
    age_first: int,
    aging_profile: str,
    age_confidence: str,
    age_evidence: list[str],
    profile_evidence: list[str],
) -> dict[str, Any]:
    first_issue_index = int(temporal.get("first_recorded_issue_index") or temporal.get("current_issue_index") or 1)
    current_issue_index = int(temporal.get("current_issue_index") or first_issue_index)
    world_days_per_issue = int(temporal.get("world_days_per_issue") or 10)
    elapsed_issues = max(0, current_issue_index - first_issue_index)
    elapsed_years = ((elapsed_issues * float(world_days_per_issue)) / 365.0) * _age_rate_for_profile(aging_profile)
    current_age = max(0.0, float(age_first) + elapsed_years)
    baseline_vitality, frailty, resilience, condition_profile = _health_defaults(name, current_age, aging_profile)
    xp = round((max(current_age - 12.0, 0.0) * 0.12) + (appearances * 0.28), 2)
    wisdom = _clamp(round((0.22 + xp / 100.0), 3), 0.0, 1.0)

    health = temporal.setdefault("health", {})
    existing_active = list(health.get("active_conditions") or [])
    existing_chronic = list(health.get("chronic_conditions") or [])

    temporal["age_first_recorded_years"] = int(max(0, min(_max_age_for_profile(aging_profile), age_first)))
    temporal["birth_issue_index_est"] = int(round(first_issue_index - ((temporal["age_first_recorded_years"] * 365.0) / float(world_days_per_issue))))
    temporal["current_age_years"] = round(current_age, 2)
    temporal["life_stage"] = _life_stage(current_age, aging_profile)
    temporal["aging_profile"] = aging_profile
    temporal["aging_profile_evidence"] = profile_evidence[:5]
    temporal["age_confidence"] = age_confidence
    temporal["age_evidence"] = age_evidence[:5]
    health["baseline_vitality"] = baseline_vitality
    health["chronic_conditions"] = existing_chronic
    health["active_conditions"] = existing_active
    health["frailty"] = frailty
    health["resilience"] = resilience
    health["condition_profile"] = condition_profile

    wisdom_block = temporal.setdefault("wisdom", {})
    wisdom_block["experience_points"] = xp
    wisdom_block["wisdom_score"] = round(wisdom, 3)
    wisdom_block["temperament_shift"] = wisdom_block.get("temperament_shift") or "steady"
    wisdom_block["memory_horizon_years"] = _memory_horizon_years(current_age, aging_profile)
    temporal["temporal_inference"] = "story_estimated" if age_confidence in {"high", "medium"} else "heuristic_estimated"
    return temporal


def _load_existing_temporal_payload(path: str) -> dict[str, Any]:
    if not path or not os.path.exists(path):
        return {}
    try:
        data = _load_json(path)
    except Exception:
        return {}
    rows = data.get("characters") if isinstance(data, dict) else []
    if not isinstance(rows, list):
        return {}
    out: dict[str, Any] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        key = str(row.get("name") or "").strip().lower()
        if not key or key in out:
            continue
        temporal = row.get("temporal") if isinstance(row.get("temporal"), dict) else None
        if temporal:
            out[key] = temporal
    return out


def _build_haiku_age_prompt(char: dict[str, Any], temporal: dict[str, Any], story_catalog: dict[tuple[str, str], dict[str, Any]]) -> str:
    story_rows = gather_story_texts_for_character(char, story_catalog, max_stories=6)
    evidence = [
        {
            "date": str(row.get("date") or ""),
            "title": str(row.get("title") or ""),
            "text": str(row.get("text") or "")[:1800],
        }
        for row in story_rows
    ]
    payload = {
        "name": char.get("name"),
        "aliases": char.get("aliases"),
        "role": char.get("role"),
        "status": char.get("status"),
        "traits": char.get("traits"),
        "bio": char.get("bio"),
        "status_history": char.get("status_history"),
        "current_inferred_temporal": temporal,
        "story_evidence": evidence,
    }
    return f"""You are adjudicating the age and aging profile of a fantasy character.

Return ONLY valid JSON with this schema:
{{
  "age_first_recorded_years": 0,
  "aging_profile": "mortal_humanlike",
  "age_confidence": "low",
  "age_evidence": ["short reason"]
}}

Rules:
- Allowed aging_profile values: mortal_humanlike, long_lived_supernatural, ageless_supernatural.
- Use only supplied canon and story evidence.
- New mortal characters should usually remain within ordinary human ranges unless text strongly implies otherwise.
- Long-lived and ageless supernatural beings can remember centuries or millennia of change; do not collapse them into mortal age expectations.
- If the evidence only suggests an ancient supernatural being without an exact age, choose a plausible age band consistent with its role and history.
- Prefer conservative judgments over dramatic invention.

CHARACTER JSON:
{json.dumps(payload, ensure_ascii=False, indent=2)}
"""


def _apply_haiku_age_adjudication(
    rows: list[dict[str, Any]],
    characters: list[dict[str, Any]],
    story_catalog: dict[tuple[str, str], dict[str, Any]],
    *,
    age_mode: str,
    model: str,
    existing_output_path: str,
) -> list[dict[str, Any]]:
    existing_by_name = _load_existing_temporal_payload(existing_output_path)
    char_by_name = {
        str(char.get("name") or "").strip().lower(): char
        for char in characters
        if isinstance(char, dict) and str(char.get("name") or "").strip()
    }

    client = None
    resolved_mode = age_mode
    api_key = (os.environ.get("ANTHROPIC_API_KEY") or "").strip()
    if age_mode in {"auto", "haiku"}:
        if api_key and anthropic is not None:
            client = anthropic.Anthropic(api_key=api_key)
            resolved_mode = "haiku"
        elif age_mode == "haiku":
            raise SystemExit("ANTHROPIC_API_KEY is required for --age-mode haiku")
        else:
            resolved_mode = "deterministic"

    if resolved_mode != "haiku" or client is None:
        return rows

    for row in rows:
        if not isinstance(row, dict):
            continue
        name = str(row.get("name") or "").strip()
        key = name.lower()
        char = char_by_name.get(key)
        temporal = row.get("temporal") if isinstance(row.get("temporal"), dict) else None
        if not char or not temporal:
            continue

        is_new = key not in existing_by_name
        is_low_conf_supernatural = str(temporal.get("aging_profile") or MORTAL_PROFILE) != MORTAL_PROFILE and str(temporal.get("age_confidence") or "").lower() == "low"
        if not (is_new or is_low_conf_supernatural):
            continue

        try:
            prompt = _build_haiku_age_prompt(char, temporal, story_catalog)
            msg = client.messages.create(
                model=model,
                max_tokens=800,
                messages=[{"role": "user", "content": prompt}],
            )
            text = (msg.content[0].text or "").strip()
            data = json.loads(text)
            if not isinstance(data, dict):
                raise ValueError("age adjudication response was not an object")
            next_profile = str(data.get("aging_profile") or temporal.get("aging_profile") or MORTAL_PROFILE)
            if next_profile not in {MORTAL_PROFILE, LONG_LIVED_PROFILE, AGELESS_PROFILE}:
                next_profile = str(temporal.get("aging_profile") or MORTAL_PROFILE)
            next_age = int(round(float(data.get("age_first_recorded_years") or temporal.get("age_first_recorded_years") or 0)))
            next_age = max(0, min(_max_age_for_profile(next_profile), next_age))
            next_conf = str(data.get("age_confidence") or temporal.get("age_confidence") or "medium").lower()
            if next_conf not in {"low", "medium", "high", "explicit"}:
                next_conf = str(temporal.get("age_confidence") or "medium")
            next_evidence = [
                str(item or "").strip()
                for item in (data.get("age_evidence") or [])
                if str(item or "").strip()
            ] or list(temporal.get("age_evidence") or [])
            profile_evidence = list(temporal.get("aging_profile_evidence") or [])
            if is_new:
                next_evidence = [f"haiku adjudication for new character: {entry}" for entry in next_evidence[:3]] + list(temporal.get("age_evidence") or [])
            else:
                next_evidence = [f"haiku adjudication for low-confidence supernatural age: {entry}" for entry in next_evidence[:3]] + list(temporal.get("age_evidence") or [])
            _finalize_temporal_fields(
                temporal=temporal,
                name=name,
                appearances=int(char.get("appearances") or 0),
                age_first=next_age,
                aging_profile=next_profile,
                age_confidence=next_conf,
                age_evidence=next_evidence,
                profile_evidence=profile_evidence,
            )
            temporal["temporal_inference"] = "haiku_adjudicated"
        except Exception:
            continue

    return rows


def _build_temporal(
    char: dict[str, Any],
    issue_index_by_date: dict[str, int],
    current_issue_index: int,
    world_days_per_issue: int,
    story_catalog: dict[tuple[str, str], dict[str, Any]],
) -> dict[str, Any]:
    name = str(char.get("name") or "").strip()
    role = str(char.get("role") or "")
    status = str(char.get("status") or "")
    appearances = int(char.get("appearances") or 0)

    first_date = _first_recorded_date(char, None)
    first_issue_index = int(issue_index_by_date.get(str(first_date or ""), current_issue_index or 1))

    explicit_temporal = char.get("temporal") if isinstance(char.get("temporal"), dict) else {}
    aging_profile, profile_evidence = _infer_aging_profile(char, story_catalog)
    if "age_first_recorded_years" in explicit_temporal:
        try:
            age_first = int(round(float(explicit_temporal.get("age_first_recorded_years"))))
            age_evidence = ["explicit temporal age_first_recorded_years"]
            age_confidence = "explicit"
        except Exception:
            age_first, age_evidence, age_confidence = _estimate_age_from_story_evidence(char, story_catalog)
    else:
        age_first, age_evidence, age_confidence = _estimate_age_from_story_evidence(char, story_catalog)

    alive = _infer_alive(status)
    death_date = _extract_death_date(char)
    death_issue_index = int(issue_index_by_date.get(str(death_date or ""), 0)) if death_date else None

    temporal = {
        "first_recorded_date": first_date,
        "first_recorded_issue_index": first_issue_index,
        "age_first_recorded_years": age_first,
        "birth_issue_index_est": int(round(first_issue_index - ((age_first * 365.0) / float(world_days_per_issue)))),
        "current_issue_index": int(current_issue_index),
        "alive": bool(alive),
        "deceased_date": death_date,
        "deceased_issue_index": death_issue_index,
        "world_days_per_issue": int(world_days_per_issue),
        "health": {
            "chronic_conditions": [],
            "active_conditions": [],
        },
        "wisdom": {
            "temperament_shift": "steady",
        },
        "lineage": {
            "parents": [],
            "children": [],
            "ancestors": [],
        },
        "temporal_inference": "story_estimated" if age_confidence in {"high", "medium"} else "heuristic_estimated",
    }
    return _finalize_temporal_fields(
        temporal=temporal,
        name=name,
        appearances=appearances,
        age_first=age_first,
        aging_profile=aging_profile,
        age_confidence=age_confidence,
        age_evidence=age_evidence,
        profile_evidence=profile_evidence,
    )


def build_temporal_payload(
    codex_path: str,
    world_days_per_issue: int | None = None,
    *,
    age_mode: str = "auto",
    model: str = DEFAULT_MODEL,
    existing_output_path: str = DEFAULT_OUTPUT_FILE,
) -> dict[str, Any]:
    codex = _load_json(codex_path)
    characters = codex.get("characters") if isinstance(codex, dict) else []
    if not isinstance(characters, list):
        characters = []

    clock = build_world_clock()
    issue_dates = clock.issue_dates
    issue_index_by_date = clock.issue_index_by_date
    days_per_issue = int(world_days_per_issue or clock.world_days_per_issue)
    current_issue_index = clock.current_issue_index or max(1, len(issue_dates))
    story_catalog = load_story_catalog()

    rows = []
    for char in characters:
        if not isinstance(char, dict):
            continue
        name = str(char.get("name") or "").strip()
        if not name:
            continue
        temporal = _build_temporal(
            char=char,
            issue_index_by_date=issue_index_by_date,
            current_issue_index=current_issue_index,
            world_days_per_issue=days_per_issue,
            story_catalog=story_catalog,
        )
        rows.append({"name": name, "temporal": temporal})

    rows = _apply_haiku_age_adjudication(
        rows,
        characters,
        story_catalog,
        age_mode=age_mode,
        model=model,
        existing_output_path=existing_output_path,
    )

    rows.sort(key=lambda r: str(r.get("name") or "").lower())

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": codex_path,
        "world_days_per_issue": days_per_issue,
        "issue_start_date": issue_dates[0] if issue_dates else None,
        "issue_end_date": issue_dates[-1] if issue_dates else None,
        "current_issue_index": current_issue_index,
        "count": len(rows),
        "characters": rows,
    }


def refresh_character_temporal(
    codex_path: str = DEFAULT_CODEX_FILE,
    output_path: str = DEFAULT_OUTPUT_FILE,
    *,
    world_days_per_issue: int | None = None,
    age_mode: str = "auto",
    model: str = DEFAULT_MODEL,
) -> dict[str, Any]:
    payload = build_temporal_payload(
        codex_path,
        world_days_per_issue=world_days_per_issue,
        age_mode=age_mode,
        model=model,
        existing_output_path=output_path,
    )
    _write_json(output_path, payload)
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill character temporal metadata into a sidecar payload.")
    parser.add_argument("--codex", default=DEFAULT_CODEX_FILE, help="Path to codex.json (default: codex.json)")
    parser.add_argument("--output", default=DEFAULT_OUTPUT_FILE, help="Output JSON path (default: character-temporal.json)")
    parser.add_argument("--world-days-per-issue", type=int, default=0, help="Override world days advanced per issue")
    parser.add_argument("--age-mode", choices=["auto", "deterministic", "haiku"], default="auto", help="Age adjudication mode")
    parser.add_argument("--model", default=DEFAULT_MODEL, help="Anthropic model for age adjudication")
    parser.add_argument("--dry-run", action="store_true", help="Compute and print summary only")
    args = parser.parse_args()

    payload = build_temporal_payload(
        args.codex,
        world_days_per_issue=(args.world_days_per_issue or None),
        age_mode=args.age_mode,
        model=args.model,
        existing_output_path=args.output,
    )

    if args.dry_run:
        print(f"Temporal rows: {payload.get('count', 0)}")
        print(f"World days/issue: {payload.get('world_days_per_issue')}")
        print(f"Issue span: {payload.get('issue_start_date')} -> {payload.get('issue_end_date')}")
        return 0

    _write_json(args.output, payload)
    print(f"Wrote {args.output} with {payload.get('count', 0)} characters")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
