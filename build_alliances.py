#!/usr/bin/env python3
"""Build alliance sidecar payload from codex and temporal data.

The payload is conservative by design: it records only alliances with explicit
cue phrases plus named participants found in the same sentence.
"""

from __future__ import annotations

import argparse
import json
import os
import re
from collections import Counter
from datetime import datetime, timezone
from typing import Any


DEFAULT_CODEX_FILE = "codex.json"
DEFAULT_TEMPORAL_FILE = "character-temporal.json"
DEFAULT_OUTPUT_FILE = "alliances.json"

ENTITY_CATEGORIES = ("characters", "factions", "polities")

ALLIANCE_CUES_STRONG: list[tuple[str, tuple[str, ...]]] = [
    (
        "business",
        (
            r"\bbusiness alliance\b",
            r"\btrade alliance\b",
            r"\btrade pact\b",
            r"\btrade agreement\b",
            r"\bcommercial alliance\b",
            r"\bjoint venture\b",
            r"\bconsortium\b",
            r"\btrade partner(?:s)?\b",
            r"\bmercantile pact\b",
        ),
    ),
    (
        "military",
        (
            r"\bmilitary alliance\b",
            r"\bdefen[cs]e pact\b",
            r"\bmutual defen[cs]e\b",
            r"\bwar pact\b",
            r"\bwar coalition\b",
            r"\bcampaign alliance\b",
        ),
    ),
    (
        "political",
        (
            r"\ballied with\b",
            r"\balliance with\b",
            r"\bcoalition with\b",
            r"\bpact with\b",
            r"\btreaty with\b",
            r"\baccord with\b",
            r"\bcompact with\b",
            r"\baligned with\b",
            r"\bpartner(?:ed)? with\b",
        ),
    ),
    (
        "family_adjacent",
        (
            r"\bmarriage pact\b",
            r"\bhouse alliance\b",
            r"\bsworn sibling\b",
            r"\bfoster[- ]kin\b",
            r"\bkinship pact\b",
        ),
    ),
]

ALLIANCE_CUES_CONTEXTUAL: list[tuple[str, tuple[str, ...]]] = [
    (
        "business",
        (
            r"\bbacked by\b",
            r"\bsupported by\b",
            r"\bunder charter with\b",
            r"\bjointly funded by\b",
            r"\bin partnership with\b",
            r"\bwith support from\b",
        ),
    ),
    (
        "military",
        (
            r"\bunder protection of\b",
            r"\bprotected by\b",
            r"\bjoint patrol(?:s)? with\b",
            r"\bcoordinat(?:e|es|ed|ing) with\b",
            r"\bcampaign with\b",
            r"\bmarches with\b",
        ),
    ),
    (
        "political",
        (
            r"\bworks with\b",
            r"\bwork with\b",
            r"\bcooperat(?:e|es|ed|ing) with\b",
            r"\bjoined(?:\s+the)?\s+coalition\b",
            r"\bpart of\s+[^.]{0,80}\bcoalition\b",
            r"\baligned against\b",
            r"\bwith aid from\b",
            r"\bwith backing from\b",
        ),
    ),
]

NON_ALLIANCE_POLITICAL_CONTEXT = re.compile(
    r"\bruled by\b|\bgoverned by\b|\bon behalf of\b|\bunder rule of\b",
    re.IGNORECASE,
)

HARD_DIPLOMATIC_CUE = re.compile(
    r"\balliance\b|\btreaty\b|\baccord\b|\bpacts?\b|\bcoalition\b|\bmutual defen[cs]e\b",
    re.IGNORECASE,
)

FAMILY_RED_FLAG = re.compile(
    r"\bsister\b|\bbrother\b|\bdaughter\b|\bson\b|\bmother\b|\bfather\b|\bgrandmother\b|\bgrandfather\b",
    re.IGNORECASE,
)


def _load_json(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _write_json(path: str, data: Any) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=True, indent=2)
        f.write("\n")


def _norm(value: str) -> str:
    return str(value or "").strip().lower()


def _type_label(type_name: str) -> str:
    t = str(type_name or "").strip().lower()
    if t == "polities":
        return "polity"
    if t.endswith("s"):
        return t[:-1]
    return t


def _entity_key(participant: dict[str, str]) -> str:
    return f"{_norm(participant.get('type') or '')}::{_norm(participant.get('name') or '')}"


def _alliance_pair_key(a: dict[str, str], b: dict[str, str]) -> tuple[str, str]:
    aa = _entity_key(a)
    bb = _entity_key(b)
    if aa <= bb:
        return aa, bb
    return bb, aa


def _classify_alliance_type(sentence: str) -> tuple[str, str] | None:
    text = str(sentence or "")
    if not text:
        return None

    lowered = text.lower()
    for alliance_type, patterns in ALLIANCE_CUES_STRONG:
        if any(re.search(pat, lowered, flags=re.IGNORECASE) for pat in patterns):
            if alliance_type == "political" and FAMILY_RED_FLAG.search(text):
                # Avoid turning direct family statements into alliances.
                continue
            return alliance_type, "strong"

    for alliance_type, patterns in ALLIANCE_CUES_CONTEXTUAL:
        if any(re.search(pat, lowered, flags=re.IGNORECASE) for pat in patterns):
            if alliance_type == "political" and FAMILY_RED_FLAG.search(text):
                continue
            if alliance_type == "political" and NON_ALLIANCE_POLITICAL_CONTEXT.search(text):
                continue
            return alliance_type, "contextual"

    if re.search(r"\balliance\b", lowered):
        return "political", "contextual"
    return None


def _build_entity_patterns(codex: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for cat in ENTITY_CATEGORIES:
        items = codex.get(cat) if isinstance(codex, dict) else []
        if not isinstance(items, list):
            continue
        for item in items:
            if not isinstance(item, dict):
                continue
            name = str(item.get("name") or "").strip()
            if not name:
                continue
            k = f"{cat}::{_norm(name)}"
            if k in seen:
                continue
            seen.add(k)
            rows.append(
                {
                    "type": cat,
                    "name": name,
                    "regex": re.compile(rf"(?<![\w']){re.escape(name.lower())}(?![\w'])"),
                }
            )
    rows.sort(key=lambda r: len(r["name"]), reverse=True)
    return rows


def _mentioned_entities(text: str, *, self_entity: dict[str, str], patterns: list[dict[str, Any]]) -> list[dict[str, str]]:
    lowered = str(text or "").lower()
    out: list[dict[str, str]] = []
    seen: set[str] = set()
    self_key = _entity_key(self_entity)
    for row in patterns:
        if not row["regex"].search(lowered):
            continue
        participant = {"type": row["type"], "name": row["name"]}
        k = _entity_key(participant)
        if not k or k == self_key or k in seen:
            continue
        seen.add(k)
        out.append(participant)
    return out


def _initial_status(a: dict[str, str], b: dict[str, str], temporal_by_name: dict[str, dict[str, Any]]) -> str:
    for participant in (a, b):
        if _norm(participant.get("type") or "") != "characters":
            continue
        temporal = temporal_by_name.get(_norm(participant.get("name") or ""), {})
        if temporal.get("alive", True) is False:
            return "historical"
    return "ongoing"


def _source_rows(entity: dict[str, Any]) -> list[tuple[str, str, str, str]]:
    rows: list[tuple[str, str, str, str]] = []
    first_date = str(entity.get("first_date") or "")
    first_story = str(entity.get("first_story") or "")
    for field in ("status", "bio", "description", "notes", "goals", "tagline"):
        txt = str(entity.get(field) or "").strip()
        if txt:
            rows.append((field, txt, first_date, first_story))

    for field in ("affiliations", "allies"):
        vals = entity.get(field) if isinstance(entity.get(field), list) else []
        clean = [str(v or "").strip() for v in vals if str(v or "").strip()]
        if clean:
            rows.append((field, "; ".join(clean), first_date, first_story))

    history = entity.get("status_history") if isinstance(entity.get("status_history"), list) else []
    for ev in history:
        if not isinstance(ev, dict):
            continue
        d = str(ev.get("date") or "").strip()
        t = str(ev.get("story_title") or "").strip()
        note = str(ev.get("note") or "").strip()
        evidence = str(ev.get("evidence") or "").strip()
        if note:
            rows.append(("status_history_note", note, d, t))
        if evidence:
            rows.append(("status_history_evidence", evidence, d, t))
    return rows


def _append_story_appearance(alliance: dict[str, Any], date_key: str, title: str) -> None:
    d = str(date_key or "").strip()
    t = str(title or "").strip()
    if not d or not t:
        return
    apps = alliance.setdefault("story_appearances", [])
    key = (_norm(d), _norm(t))
    for row in apps:
        if not isinstance(row, dict):
            continue
        if (_norm(row.get("date") or ""), _norm(row.get("title") or "")) == key:
            return
    apps.append({"date": d, "title": t})


def _append_evidence(alliance: dict[str, Any], text: str) -> None:
    raw = str(text or "").strip()
    if not raw:
        return
    evidence = alliance.setdefault("evidence", [])
    if raw not in evidence:
        evidence.append(raw)


def _alliance_confidence(row: dict[str, Any], participant_types: set[str], appearances: int) -> str:
    score = int(row.get("_signal_score") or 0)
    source_count = len(row.get("_source_types") or set())
    institutional = bool(participant_types & {"factions", "polities"})
    hard_cue = False
    if source_count >= 2:
        score += 1
    if appearances >= 2:
        score += 1
    if institutional:
        score += 1
        evidence_lines = [str(x or "") for x in (row.get("evidence") or [])]
        hard_cue = any(HARD_DIPLOMATIC_CUE.search(line) for line in evidence_lines)
        if hard_cue:
            score += 1

    if institutional and hard_cue and score >= 3:
        return "high"
    if score >= 4:
        return "high"
    if score >= 2:
        return "medium"
    return "low"


def build_alliance_payload(codex_path: str = DEFAULT_CODEX_FILE, temporal_path: str = DEFAULT_TEMPORAL_FILE) -> dict[str, Any]:
    codex = _load_json(codex_path)
    temporal_payload = _load_json(temporal_path) if os.path.exists(temporal_path) else {}

    temporal_rows = temporal_payload.get("characters") if isinstance(temporal_payload, dict) else []
    if not isinstance(temporal_rows, list):
        temporal_rows = []
    temporal_by_name = {
        _norm(row.get("name") or ""): (
            row.get("temporal") if isinstance(row, dict) and isinstance(row.get("temporal"), dict) else {}
        )
        for row in temporal_rows
        if isinstance(row, dict) and str(row.get("name") or "").strip()
    }

    patterns = _build_entity_patterns(codex if isinstance(codex, dict) else {})
    alliances: dict[tuple[str, str], dict[str, Any]] = {}

    for cat in ENTITY_CATEGORIES:
        entities = codex.get(cat) if isinstance(codex, dict) else []
        if not isinstance(entities, list):
            continue
        for entity in entities:
            if not isinstance(entity, dict):
                continue
            source_name = str(entity.get("name") or "").strip()
            if not source_name:
                continue
            self_entity = {"type": cat, "name": source_name}

            for source_type, text, date_key, story_title in _source_rows(entity):
                sentences = [seg.strip() for seg in re.split(r"(?<=[.!?])\s+", str(text or "")) if seg.strip()]
                for idx, sentence in enumerate(sentences):
                    signal = _classify_alliance_type(sentence)
                    if not signal:
                        continue
                    alliance_type, signal_strength = signal
                    others = _mentioned_entities(sentence, self_entity=self_entity, patterns=patterns)
                    # Contextual cues often put the institution name in adjacent text.
                    if not others and signal_strength == "contextual":
                        window = [sentence]
                        if idx > 0:
                            window.append(sentences[idx - 1])
                        if idx + 1 < len(sentences):
                            window.append(sentences[idx + 1])
                        others = _mentioned_entities(" ".join(window), self_entity=self_entity, patterns=patterns)
                    for other in others:
                        pair_key = _alliance_pair_key(self_entity, other)
                        if pair_key not in alliances:
                            participants = sorted([self_entity, other], key=lambda p: (_norm(p.get("type") or ""), _norm(p.get("name") or "")))
                            alliances[pair_key] = {
                                "participants": participants,
                                "alliance_type": alliance_type,
                                "status": _initial_status(participants[0], participants[1], temporal_by_name),
                                "story_appearances": [],
                                "evidence": [],
                                "summary": "",
                                "_signal_score": 0,
                                "_source_types": set(),
                            }
                        row = alliances[pair_key]
                        if alliance_type == "business":
                            row["alliance_type"] = "business"
                        elif alliance_type == "military" and row.get("alliance_type") not in {"business"}:
                            row["alliance_type"] = "military"
                        elif alliance_type == "family_adjacent" and row.get("alliance_type") not in {"business", "military"}:
                            row["alliance_type"] = "family_adjacent"
                        elif row.get("alliance_type") not in {"business", "military", "family_adjacent"}:
                            row["alliance_type"] = "political"

                        _append_story_appearance(row, date_key, story_title)
                        _append_evidence(row, f"{source_type}: {sentence[:220]}")
                        row["_signal_score"] = int(row.get("_signal_score") or 0) + (2 if signal_strength == "strong" else 1)
                        row.setdefault("_source_types", set()).add(source_type)

    rows: list[dict[str, Any]] = []
    for alliance in alliances.values():
        participants = alliance.get("participants") if isinstance(alliance.get("participants"), list) else []
        if len(participants) != 2:
            continue
        left = participants[0]
        right = participants[1]
        left_name = str(left.get("name") or "").strip()
        right_name = str(right.get("name") or "").strip()
        if not left_name or not right_name:
            continue

        apps = sorted(
            [a for a in (alliance.get("story_appearances") or []) if isinstance(a, dict)],
            key=lambda row: (str(row.get("date") or ""), str(row.get("title") or "")),
        )
        first_story = str(apps[0].get("title") or "") if apps else ""
        first_date = str(apps[0].get("date") or "") if apps else ""
        alliance_type = str(alliance.get("alliance_type") or "political")
        status = str(alliance.get("status") or "ongoing")
        status_phrase = "ongoing" if status == "ongoing" else "historical"
        summary = f"{left_name} and {right_name} are recorded in an {status_phrase} {alliance_type.replace('_', ' ')} alliance."

        evidence = list(alliance.get("evidence") or [])
        participant_types = {
            str(left.get("type") or "").strip(),
            str(right.get("type") or "").strip(),
        }
        confidence = _alliance_confidence(alliance, participant_types, len(apps))

        rows.append(
            {
                "name": f"{left_name} & {right_name}",
                "participants": [
                    {"type": str(left.get("type") or ""), "name": left_name},
                    {"type": str(right.get("type") or ""), "name": right_name},
                ],
                "alliance_type": alliance_type,
                "status": status,
                "confidence": confidence,
                "summary": summary,
                "appearances": len(apps),
                "first_story": first_story,
                "first_date": first_date,
                "story_appearances": apps,
                "evidence": evidence[:6],
            }
        )

    name_counts = Counter(str(item.get("name") or "") for item in rows)
    for item in rows:
        current_name = str(item.get("name") or "")
        if name_counts.get(current_name, 0) <= 1:
            continue
        participants = item.get("participants") if isinstance(item.get("participants"), list) else []
        if len(participants) != 2:
            continue
        left = participants[0] if isinstance(participants[0], dict) else {}
        right = participants[1] if isinstance(participants[1], dict) else {}
        left_name = str(left.get("name") or "").strip()
        right_name = str(right.get("name") or "").strip()
        left_type = str(left.get("type") or "").strip()
        right_type = str(right.get("type") or "").strip()
        if not left_name or not right_name:
            continue
        left_label = _type_label(left_type)
        right_label = _type_label(right_type)
        item["name"] = f"{left_name} ({left_label}) & {right_name} ({right_label})"

    rows.sort(key=lambda item: _norm(item.get("name") or ""))
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source_codex": codex_path,
        "source_temporal": temporal_path,
        "count": len(rows),
        "alliances": rows,
    }


def refresh_alliances(
    codex_path: str = DEFAULT_CODEX_FILE,
    temporal_path: str = DEFAULT_TEMPORAL_FILE,
    output_path: str = DEFAULT_OUTPUT_FILE,
) -> dict[str, Any]:
    payload = build_alliance_payload(codex_path=codex_path, temporal_path=temporal_path)
    _write_json(output_path, payload)
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description="Build alliance sidecar payload from codex and temporal data.")
    parser.add_argument("--codex", default=DEFAULT_CODEX_FILE)
    parser.add_argument("--temporal", default=DEFAULT_TEMPORAL_FILE)
    parser.add_argument("--output", default=DEFAULT_OUTPUT_FILE)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    payload = build_alliance_payload(codex_path=args.codex, temporal_path=args.temporal)
    if args.dry_run:
        print(f"Alliance rows: {payload.get('count', 0)}")
        return 0

    _write_json(args.output, payload)
    print(f"Wrote {args.output} with {payload.get('count', 0)} alliances")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
