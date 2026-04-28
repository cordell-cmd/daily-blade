#!/usr/bin/env python3
"""Phase 2 lifecycle simulation for Daily Blade characters.

This updates character temporal state with condition transitions, mortality,
and birth/lineage events. It can use Claude Haiku when ANTHROPIC_API_KEY is
available, and falls back to a conservative deterministic mode otherwise.
"""

from __future__ import annotations

import argparse
import json
import os
import re
from copy import deepcopy
from datetime import datetime, timezone
from typing import Any

try:
    import anthropic
except Exception:  # pragma: no cover - dependency may be absent in some contexts
    anthropic = None

from character_story_tools import gather_story_texts_for_character, load_story_catalog
from world_time import build_world_clock


DEFAULT_MODEL = "claude-haiku-4-5-20251001"
DEFAULT_CODEX_FILE = "codex.json"
DEFAULT_TEMPORAL_FILE = "character-temporal.json"
DEFAULT_LOG_FILE = "character-lifecycle-log.json"

MORTAL_PROFILE = "mortal_humanlike"
LONG_LIVED_PROFILE = "long_lived_supernatural"
AGELESS_PROFILE = "ageless_supernatural"

SEVERE_DEATH_CUES = (
    r"\bdies\b",
    r"\bdied\b",
    r"\bis dead\b",
    r"\bwas killed\b",
    r"\bwas slain\b",
    r"\bwas executed\b",
    r"\bwas never seen again\b",
    r"\blies dying\b",
    r"\blay dying\b",
)

CONDITION_PATTERNS = {
    "fever": (r"\bfever\b", r"\bfeverish\b"),
    "wasting sickness": (r"\bwasting\b", r"\bcough\b", r"\bconsumption\b"),
    "shadowburn": (r"\bshadow[- ]?burn\b", r"\bshadow[- ]?scar\b"),
    "memory fracture": (r"\bmemory fracture\b", r"\bidentity begins dissolving\b", r"\bforgetting their own names\b"),
    "grave wound": (r"\bbleeding\b", r"\bwound\b", r"\bmaimed\b", r"\bcrippled\b"),
    "frail sight": (r"\bfailing sight\b", r"\bblind\b", r"\bclouded vision\b"),
}

BIRTH_CUES = (
    r"\bgave birth\b",
    r"\bnewborn\b",
    r"\binfant son\b",
    r"\binfant daughter\b",
    r"\btheir child\b",
    r"\bher child\b",
    r"\bhis child\b",
)


def _load_json(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _write_json(path: str, data: Any) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=True, indent=2)
        f.write("\n")


def _norm_name(value: str) -> str:
    return str(value or "").strip().lower()


def _life_stage(age_years: float) -> str:
    if age_years < 13:
        return "child"
    if age_years < 20:
        return "youth"
    if age_years < 45:
        return "adult"
    if age_years < 65:
        return "mature"
    return "elder"


def _aging_profile(temporal: dict[str, Any]) -> str:
    return str(temporal.get("aging_profile") or MORTAL_PROFILE)


def _condition_profile(temporal: dict[str, Any]) -> str:
    health = temporal.get("health") if isinstance(temporal.get("health"), dict) else {}
    return str(health.get("condition_profile") or "humanlike")


def _allows_story_births(temporal: dict[str, Any]) -> bool:
    profile = _aging_profile(temporal)
    age = float(temporal.get("current_age_years") or 0.0)
    if profile == MORTAL_PROFILE:
        return 16 <= age <= 70
    return True


def _recent_story_rows(char: dict[str, Any], catalog: dict[tuple[str, str], dict[str, Any]], issue_index_by_date: dict[str, int], current_issue_index: int, lookback_issues: int) -> list[dict[str, str]]:
    rows = gather_story_texts_for_character(char, catalog, max_stories=0)
    out = []
    for row in rows:
        idx = int(issue_index_by_date.get(str(row.get("date") or ""), 0))
        if idx and (current_issue_index - idx) <= lookback_issues:
            out.append(row)
    return out


def _extract_condition_signals(text: str) -> list[str]:
    lowered = str(text or "").lower()
    found: list[str] = []
    for label, patterns in CONDITION_PATTERNS.items():
        if any(re.search(pattern, lowered) for pattern in patterns):
            found.append(label)
    return found


def _has_named_death_cue(name: str, text: str) -> bool:
    raw = str(text or "")
    lowered = raw.lower()
    name_pat = re.escape(str(name or "").strip().lower())
    if not name_pat:
        return False
    sentences = [seg.strip().lower() for seg in re.split(r"(?<=[.!?])\s+", raw) if seg.strip()]
    for sent in sentences:
        if not re.search(name_pat, sent):
            continue
        if any(re.search(cue, sent) for cue in SEVERE_DEATH_CUES):
            return True
    return False


def _extract_birth_events(name: str, text: str) -> list[dict[str, str]]:
    lowered = str(text or "")
    if not any(re.search(pattern, lowered, flags=re.IGNORECASE) for pattern in BIRTH_CUES):
        return []

    # Try a few explicit naming constructions first.
    patterns = [
        rf"{re.escape(name)}[^.]*?gave birth to ([A-Z][a-zA-Z'\-]+)",
        rf"{re.escape(name)}[^.]*?child named ([A-Z][a-zA-Z'\-]+)",
        rf"([A-Z][a-zA-Z'\-]+), child of {re.escape(name)}",
    ]
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            return [{"child_name": match.group(1).strip(), "reason": "explicit birth cue in story"}]
    return []


def _deterministic_decision(char: dict[str, Any], temporal: dict[str, Any], recent_rows: list[dict[str, str]], current_date: str, current_issue_index: int) -> dict[str, Any]:
    name = str(char.get("name") or "").strip()
    active = set(str(x or "") for x in ((temporal.get("health") or {}).get("active_conditions") or []))
    chronic = set(str(x or "") for x in ((temporal.get("health") or {}).get("chronic_conditions") or []))
    alive = bool(temporal.get("alive", True))
    resilience = float((temporal.get("health") or {}).get("resilience") or 0.5)
    condition_profile = _condition_profile(temporal)

    decision = {
        "alive": alive,
        "deceased_date": temporal.get("deceased_date"),
        "health": {
            "active_conditions_add": [],
            "active_conditions_remove": [],
            "chronic_conditions_add": [],
            "frailty_delta": 0.0,
            "resilience_delta": 0.0,
        },
        "births": [],
        "notes": [],
        "mode": "deterministic",
    }

    all_text = "\n\n".join(str(row.get("text") or "") for row in recent_rows)
    signals = []
    for row in recent_rows:
        signals.extend(_extract_condition_signals(row.get("text") or ""))
    unique_signals = sorted(set(signals))
    for label in unique_signals:
        if label not in active:
            decision["health"]["active_conditions_add"].append(label)
            decision["notes"].append(f"new condition from recent story cues: {label}")

    if any(_has_named_death_cue(name, row.get("text") or "") for row in recent_rows):
        decision["alive"] = False
        decision["deceased_date"] = current_date
        decision["notes"].append("death triggered by explicit named death cue in recent story")

    severe_count = len([x for x in unique_signals if x in {"grave wound", "wasting sickness", "memory fracture"}])
    if severe_count:
        frailty_scale = 0.03 if condition_profile != "humanlike" else 0.06
        resilience_scale = 0.02 if condition_profile != "humanlike" else 0.04
        decision["health"]["frailty_delta"] += min(0.18, frailty_scale * severe_count)
        decision["health"]["resilience_delta"] -= min(0.12, resilience_scale * severe_count)
    elif unique_signals and resilience >= 0.6:
        decision["health"]["resilience_delta"] += 0.02

    if alive and _allows_story_births(temporal):
        births = _extract_birth_events(name, all_text)
        decision["births"].extend(births)

    return decision


def _build_haiku_prompt(char: dict[str, Any], temporal: dict[str, Any], recent_rows: list[dict[str, str]], current_date: str) -> str:
    evidence = [
        {
            "date": str(r.get("date") or ""),
            "title": str(r.get("title") or ""),
            "text": str(r.get("text") or "")[:1800],
        }
        for r in recent_rows
    ]
    payload = {
        "name": char.get("name"),
        "role": char.get("role"),
        "status": char.get("status"),
        "bio": char.get("bio"),
        "temporal": temporal,
        "recent_story_evidence": evidence,
        "current_date": current_date,
    }
    return f"""You are deciding character lifecycle transitions for an ongoing fantasy simulation.

Return ONLY valid JSON with this schema:
{{
  "alive": true,
  "deceased_date": null,
  "health": {{
    "active_conditions_add": ["condition"],
    "active_conditions_remove": ["condition"],
    "chronic_conditions_add": ["condition"],
    "frailty_delta": 0.0,
    "resilience_delta": 0.0
  }},
  "births": [
    {{"child_name": "Name", "other_parent": "unknown", "reason": "short reason"}}
  ],
  "notes": ["short note"]
}}

Rules:
- Use only facts supported by the supplied bio, current temporal state, and recent story evidence.
- Be conservative. Prefer no change when evidence is weak.
- Long-lived and ageless supernatural beings may remember centuries or millennia of change; do not infer mortal frailty or short memory unless the evidence explicitly supports it.
- Only mark death when the evidence strongly implies the named character has died or is dead.
- Only add births when the evidence strongly implies a new child has entered this character's lineage.
- Keep condition names short and reusable.
- Do not invent large family trees, political changes, or plot twists.

CHARACTER JSON:
{json.dumps(payload, ensure_ascii=False, indent=2)}
"""


def _haiku_decision(client, model: str, char: dict[str, Any], temporal: dict[str, Any], recent_rows: list[dict[str, str]], current_date: str) -> dict[str, Any]:
    prompt = _build_haiku_prompt(char, temporal, recent_rows, current_date)
    msg = client.messages.create(
        model=model,
        max_tokens=1000,
        messages=[{"role": "user", "content": prompt}],
    )
    text = (msg.content[0].text or "").strip()
    data = json.loads(text)
    if not isinstance(data, dict):
        raise ValueError("Haiku lifecycle response was not an object")
    data["mode"] = "haiku"
    return data


def _apply_decision(name_to_temporal: dict[str, dict[str, Any]], decision: dict[str, Any], char_key: str, display_name: str, current_date: str, current_issue_index: int, world_days_per_issue: int, log_rows: list[dict[str, Any]]) -> None:
    temporal = name_to_temporal[char_key]
    health = temporal.setdefault("health", {})
    active = list(health.get("active_conditions") or [])
    chronic = list(health.get("chronic_conditions") or [])
    condition_profile = _condition_profile(temporal)

    for label in decision.get("health", {}).get("active_conditions_add", []) or []:
        if label and label not in active:
            active.append(label)
            log_rows.append({"type": "condition_added", "name": display_name, "condition": label, "date": current_date})
    for label in decision.get("health", {}).get("active_conditions_remove", []) or []:
        if label in active:
            active.remove(label)
            log_rows.append({"type": "condition_removed", "name": display_name, "condition": label, "date": current_date})
    for label in decision.get("health", {}).get("chronic_conditions_add", []) or []:
        if label and label not in chronic:
            chronic.append(label)
            log_rows.append({"type": "chronic_condition_added", "name": display_name, "condition": label, "date": current_date})

    health["active_conditions"] = active
    health["chronic_conditions"] = chronic
    frailty_floor = 0.05 if condition_profile == "humanlike" else 0.02
    frailty_ceiling = 0.99 if condition_profile == "humanlike" else 0.45
    health["frailty"] = round(max(frailty_floor, min(frailty_ceiling, float(health.get("frailty") or 0.0) + float(decision.get("health", {}).get("frailty_delta") or 0.0))), 3)
    health["resilience"] = round(max(0.05, min(0.99, float(health.get("resilience") or 0.5) + float(decision.get("health", {}).get("resilience_delta") or 0.0))), 3)

    if bool(decision.get("alive", True)) is False and temporal.get("alive", True):
        temporal["alive"] = False
        temporal["deceased_date"] = decision.get("deceased_date") or current_date
        temporal["deceased_issue_index"] = current_issue_index
        log_rows.append({"type": "death", "name": display_name, "date": temporal["deceased_date"], "notes": decision.get("notes", [])})

    lineage = temporal.setdefault("lineage", {"parents": [], "children": [], "ancestors": []})
    for birth in decision.get("births", []) or []:
        if not isinstance(birth, dict):
            continue
        child_name = str(birth.get("child_name") or "").strip()
        if not child_name:
            continue
        if child_name not in lineage.setdefault("children", []):
            lineage["children"].append(child_name)
        other_parent = str(birth.get("other_parent") or "unknown").strip()
        if child_name not in name_to_temporal:
            parent_profile = _aging_profile(temporal)
            parent_condition_profile = _condition_profile(temporal)
            name_to_temporal[child_name] = {
                "first_recorded_date": current_date,
                "first_recorded_issue_index": current_issue_index,
                "age_first_recorded_years": 0,
                "birth_issue_index_est": current_issue_index,
                "current_issue_index": current_issue_index,
                "current_age_years": 0.0,
                "life_stage": "child",
                "aging_profile": parent_profile if parent_profile != AGELESS_PROFILE else LONG_LIVED_PROFILE,
                "aging_profile_evidence": [f"birth lineage via {display_name}"],
                "age_confidence": "lifecycle",
                "age_evidence": [f"birth event via {display_name}"],
                "alive": True,
                "deceased_date": None,
                "deceased_issue_index": None,
                "health": {
                    "baseline_vitality": 0.97 if parent_condition_profile != "humanlike" else 0.95,
                    "chronic_conditions": [],
                    "active_conditions": [],
                    "frailty": 0.05,
                    "resilience": 0.97 if parent_condition_profile != "humanlike" else 0.95,
                    "condition_profile": parent_condition_profile,
                },
                "wisdom": {
                    "experience_points": 0.0,
                    "wisdom_score": 0.05,
                    "temperament_shift": "newborn",
                },
                "lineage": {
                    "parents": [display_name] + ([other_parent] if other_parent and other_parent.lower() != "unknown" else []),
                    "children": [],
                    "ancestors": [display_name] + ([other_parent] if other_parent and other_parent.lower() != "unknown" else []),
                },
                "temporal_inference": "lifecycle_birth",
            }
        else:
            child_lineage = name_to_temporal[child_name].setdefault("lineage", {"parents": [], "children": [], "ancestors": []})
            if display_name not in child_lineage.setdefault("parents", []):
                child_lineage["parents"].append(display_name)
            if other_parent and other_parent.lower() != "unknown" and other_parent not in child_lineage["parents"]:
                child_lineage["parents"].append(other_parent)
        log_rows.append({"type": "birth", "name": display_name, "child_name": child_name, "date": current_date, "notes": birth.get("reason") or ""})


def simulate_lifecycle(
    codex_path: str,
    temporal_path: str,
    log_path: str,
    mode: str,
    lookback_issues: int,
    max_candidates: int,
    model: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    codex = _load_json(codex_path)
    temporal_payload = _load_json(temporal_path)
    characters = codex.get("characters") if isinstance(codex, dict) else []
    temporal_rows = temporal_payload.get("characters") if isinstance(temporal_payload, dict) else []
    if not isinstance(characters, list):
        characters = []
    if not isinstance(temporal_rows, list):
        temporal_rows = []

    char_by_name = {_norm_name(c.get("name") or ""): c for c in characters if isinstance(c, dict) and str(c.get("name") or "").strip()}
    temporal_by_name = {
        _norm_name(r.get("name") or ""): deepcopy(r.get("temporal") or {})
        for r in temporal_rows
        if isinstance(r, dict) and str(r.get("name") or "").strip()
    }

    clock = build_world_clock()
    issue_index_by_date = clock.issue_index_by_date
    current_issue_index = int(temporal_payload.get("current_issue_index") or clock.current_issue_index or 1)
    current_date = str(temporal_payload.get("issue_end_date") or clock.latest_issue_date or "")
    world_days_per_issue = int(temporal_payload.get("world_days_per_issue") or clock.world_days_per_issue)
    story_catalog = load_story_catalog()

    candidates: list[tuple[str, dict[str, Any], dict[str, Any], list[dict[str, str]]]] = []
    for key, char in char_by_name.items():
        temporal = temporal_by_name.get(key)
        if not temporal:
            continue
        recent_rows = _recent_story_rows(char, story_catalog, issue_index_by_date, current_issue_index, lookback_issues)
        age = float(temporal.get("current_age_years") or 0.0)
        health = temporal.get("health") if isinstance(temporal.get("health"), dict) else {}
        risky = recent_rows or (health.get("active_conditions") or []) or (health.get("chronic_conditions") or [])
        if risky:
            candidates.append((key, char, temporal, recent_rows))

    candidates.sort(key=lambda item: (
        -len(item[3]),
        -int(item[1].get("appearances") or 0),
        str(item[1].get("name") or ""),
    ))
    candidates = candidates[:max_candidates]

    client = None
    resolved_mode = mode
    if mode in {"auto", "haiku"}:
        api_key = (os.environ.get("ANTHROPIC_API_KEY") or "").strip()
        if api_key and anthropic is not None:
            client = anthropic.Anthropic(api_key=api_key)
            resolved_mode = "haiku"
        elif mode == "haiku":
            raise SystemExit("ANTHROPIC_API_KEY is required for --mode haiku")
        else:
            resolved_mode = "deterministic"

    log_rows: list[dict[str, Any]] = []
    for key, char, temporal, recent_rows in candidates:
        decision = None
        if resolved_mode == "haiku" and client is not None and recent_rows:
            try:
                decision = _haiku_decision(client, model, char, temporal, recent_rows, current_date)
            except Exception as err:
                decision = _deterministic_decision(char, temporal, recent_rows, current_date, current_issue_index)
                decision.setdefault("notes", []).append(f"haiku fallback: {err}")
        else:
            decision = _deterministic_decision(char, temporal, recent_rows, current_date, current_issue_index)
        _apply_decision(temporal_by_name, decision, key, str(char.get("name") or key), current_date, current_issue_index, world_days_per_issue, log_rows)

    updated_rows = [{"name": char.get("name"), "temporal": temporal_by_name[_norm_name(char.get("name") or "")]} for char in characters if _norm_name(char.get("name") or "") in temporal_by_name]
    # Append lifecycle-born children not yet in codex.
    codex_names = {_norm_name(c.get("name") or "") for c in characters if isinstance(c, dict)}
    extra_names = sorted(name for name in temporal_by_name.keys() if name and name not in codex_names)
    for key in extra_names:
        display_name = next((row.get("child_name") for row in reversed(log_rows) if row.get("type") == "birth" and _norm_name(row.get("child_name") or "") == key), key.title())
        updated_rows.append({"name": display_name, "temporal": temporal_by_name[key]})

    updated_rows.sort(key=lambda row: _norm_name(row.get("name") or ""))
    temporal_payload["generated_at"] = datetime.now(timezone.utc).isoformat()
    temporal_payload["count"] = len(updated_rows)
    temporal_payload["characters"] = updated_rows

    log_payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "mode": resolved_mode,
        "count": len(log_rows),
        "events": log_rows,
    }
    _write_json(temporal_path, temporal_payload)
    _write_json(log_path, log_payload)
    return temporal_payload, log_payload


def main() -> int:
    parser = argparse.ArgumentParser(description="Simulate Daily Blade character lifecycle transitions.")
    parser.add_argument("--codex", default=DEFAULT_CODEX_FILE)
    parser.add_argument("--temporal", default=DEFAULT_TEMPORAL_FILE)
    parser.add_argument("--log", default=DEFAULT_LOG_FILE)
    parser.add_argument("--mode", choices=["auto", "deterministic", "haiku"], default="auto")
    parser.add_argument("--lookback-issues", type=int, default=6)
    parser.add_argument("--max-candidates", type=int, default=120)
    parser.add_argument("--model", default=DEFAULT_MODEL)
    args = parser.parse_args()

    temporal_payload, log_payload = simulate_lifecycle(
        codex_path=args.codex,
        temporal_path=args.temporal,
        log_path=args.log,
        mode=args.mode,
        lookback_issues=args.lookback_issues,
        max_candidates=args.max_candidates,
        model=args.model,
    )
    print(f"Updated {args.temporal} ({temporal_payload.get('count', 0)} characters)")
    print(f"Wrote {args.log} ({log_payload.get('count', 0)} events, mode={log_payload.get('mode')})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
