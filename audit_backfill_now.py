#!/usr/bin/env python3
"""audit_backfill_now.py

No-LLM codex completeness audit.

Scans story text (stories.json + archive/<date>.json) for likely proper-noun phrases
(e.g., "Sunken Marches", "Siege of Blackthorn", "Ritual of Dismissal") and reports
candidates that are NOT present as a name in codex.json.

This is intentionally heuristic: it favors surfacing missing lore candidates quickly.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import unicodedata
from collections import Counter, defaultdict
from dataclasses import dataclass
from typing import Iterable


OUTPUT_FILE = "stories.json"
ARCHIVE_DIR = "archive"
ARCHIVE_IDX = os.path.join(ARCHIVE_DIR, "index.json")
CODEX_FILE = "codex.json"


CONNECTORS = {
    "of",
    "the",
    "and",
    "for",
    "in",
    "on",
    "at",
    "to",
    "from",
    "with",
    "without",
    "under",
    "over",
    "near",
    "by",
    "upon",
    "within",
    "between",
    "beyond",
}

SENTENCE_START_STOP = {
    "The",
    "A",
    "An",
    "And",
    "But",
    "Or",
    "If",
    "When",
    "As",
    "In",
    "On",
    "At",
    "To",
    "From",
    "For",
    "With",
    "Without",
    "After",
    "Before",
    "By",
    "Upon",
}

GENERIC_PHRASES = {
    "The Known World",
    "Known World",
}

LEADING_FILLER = {
    "Then",
    "When",
    "After",
    "Before",
    "While",
    "Once",
    "Soon",
    "Now",
}

PRONOUN_CONTRACTION_PREFIXES = {
    "i",
    "you",
    "he",
    "she",
    "it",
    "we",
    "they",
    "there",
    "here",
    "that",
    "this",
    "what",
    "who",
    "where",
    "when",
    "why",
    "how",
}

# Phrases that often indicate named events/rites when written in title case.
EVENT_HEADWORDS = {
    "Siege",
    "War",
    "Battle",
    "Treaty",
    "Pact",
    "Accord",
    "Rebellion",
    "Uprising",
    "Conclave",
    "Council",
    "Ritual",
    "Oath",
    "Duel",
    "Trial",
    "Sack",
    "Fall",
    "Burning",
    "Night",
    "Day",
    "Massacre",
    "Plague",
    "Curse",
    "Coronation",
    "Festival",
    "March",
    "Marches",
}


def _strip_control_chars(s: str) -> str:
    if not s:
        return ""
    return "".join(ch for ch in s if (ch == "\n" or ch == "\t" or ord(ch) >= 32) and ord(ch) != 127)


def _normalize_space(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def _strip_diacritics(s: str) -> str:
    if not s:
        return ""
    n = unicodedata.normalize("NFKD", s)
    return "".join(ch for ch in n if not unicodedata.combining(ch))


def _norm_key(s: str) -> str:
    s = _normalize_space(s)
    s = s.replace("’", "'")
    return s.casefold()


def load_json(path: str):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def iter_story_sources() -> Iterable[tuple[str, dict]]:
    """Yield (date_key, story_dict) for current + archives."""
    if os.path.exists(OUTPUT_FILE):
        data = load_json(OUTPUT_FILE)
        date_key = data.get("date", "unknown")
        for s in data.get("stories", []) or []:
            yield date_key, s

    if os.path.exists(ARCHIVE_IDX):
        idx = load_json(ARCHIVE_IDX)
        for date_key in idx.get("dates", []) or []:
            archive_path = os.path.join(ARCHIVE_DIR, f"{date_key}.json")
            if not os.path.exists(archive_path):
                continue
            data = load_json(archive_path)
            archive_date = data.get("date", date_key)
            for s in data.get("stories", []) or []:
                yield archive_date, s


def load_codex_names() -> set[str]:
    codex = load_json(CODEX_FILE) if os.path.exists(CODEX_FILE) else {}
    names: set[str] = set()

    def add_key(k: str):
        k = _norm_key(k)
        if k and len(k) >= 3:
            names.add(k)

    for _, v in codex.items():
        if not isinstance(v, list):
            continue
        for item in v:
            if not isinstance(item, dict):
                continue
            nm = (item.get("name") or "").strip()
            if nm:
                key = _norm_key(nm).replace("’", "'")
                add_key(key)

                # Mirror the UI's aliasing rules.
                if key.startswith("the "):
                    add_key(key[4:].strip())
                the_idx = key.find(" the ")
                if the_idx > 2:
                    add_key(key[:the_idx].strip())
                comma_idx = key.find(",")
                if comma_idx > 2:
                    add_key(key[:comma_idx].strip())
    return names


# Match multiword Title Case-ish phrases, allowing small connectors.
# Example: "Sunken Marches", "Ritual of Dismissal", "Vetch's Tower".
TITLE_PHRASE_RE = re.compile(
    r"\b(?:"
    r"[A-Z][A-Za-z0-9'’\-]+"
    r"(?:\s+(?:of|the|and|for|in|on|at|to|from|with|without|under|over|near|by|upon|within|between|beyond)\s+"
    r"[A-Z][A-Za-z0-9'’\-]+)?"
    r")(?:\s+"
    r"(?:[A-Z][A-Za-z0-9'’\-]+"
    r"(?:\s+(?:of|the|and|for|in|on|at|to|from|with|without|under|over|near|by|upon|within|between|beyond)\s+"
    r"[A-Z][A-Za-z0-9'’\-]+)?"
    r")){0,5}\b"
)


@dataclass(frozen=True)
class Occurrence:
    date: str
    title: str
    snippet: str


def extract_candidates(text: str) -> list[str]:
    text = _strip_control_chars(text or "")
    text = text.replace("\u00a0", " ")
    scan = _strip_diacritics(text)

    cands: list[str] = []
    for m in TITLE_PHRASE_RE.finditer(scan):
        phrase = _normalize_space(m.group(0)).replace("’", "'")
        if not phrase:
            continue

        if phrase in GENERIC_PHRASES:
            continue

        words = phrase.split(" ")

        # Drop leading temporal filler when it incorrectly gets captured.
        if len(words) >= 2 and words[0] in LEADING_FILLER:
            phrase = " ".join(words[1:])
            words = phrase.split(" ")

        # Strip leading article if it sneaks in (e.g., "the Blackthorn").
        if phrase.lower().startswith("the ") and len(words) >= 2:
            phrase = phrase[4:].strip()
            words = phrase.split(" ")

        # Normalize trailing possessive on the last token: "Morthaxes's" -> "Morthaxes".
        if words and words[-1].endswith("'s") and len(words[-1]) > 2:
            last = words[-1][:-2]
            if last.casefold() not in PRONOUN_CONTRACTION_PREFIXES:
                words[-1] = last
                phrase = " ".join(words)
        if len(words) == 1:
            # Single-word candidates tend to be noisy; keep only if possessive/hyphenated.
            if "'" not in phrase and "-" not in phrase:
                continue

            # Drop common English contractions (He'd, We'll, They're, etc.).
            m_contr = re.match(r"^([A-Za-z]+)'(d|ll|re|ve|m)$", phrase)
            if m_contr and m_contr.group(1).casefold() in PRONOUN_CONTRACTION_PREFIXES:
                continue

            # Normalize possessive tokens (Velgrim's -> Velgrim).
            if phrase.endswith("'s") and len(phrase) > 2:
                base = phrase[:-2]
                if base.casefold() in PRONOUN_CONTRACTION_PREFIXES:
                    continue
                phrase = base
                words = [phrase]

        # Drop obvious sentence-starter junk like "The".
        if words[0] in SENTENCE_START_STOP and len(words) == 1:
            continue

        # Avoid phrases that are only connectors.
        if all(w.casefold() in CONNECTORS for w in words):
            continue

        # Favor event-style phrases even if only two words.
        if len(words) == 2 and words[0] in SENTENCE_START_STOP:
            continue

        # Ignore very short phrases.
        if len(phrase) < 4:
            continue

        cands.append(phrase)

    # Extra pass: explicitly capture "X of Y" event/rite patterns.
    event_pat = re.compile(
        r"\b(?:" + "|".join(sorted(EVENT_HEADWORDS)) + r")\s+of\s+[A-Z][A-Za-z0-9'’\-]+(?:\s+[A-Z][A-Za-z0-9'’\-]+)?\b"
    )
    for m in event_pat.finditer(scan):
        cands.append(_normalize_space(m.group(0)).replace("’", "'"))

    # Also capture lowercase headwords ("siege of Blackthorn", "ritual of dismissal").
    # We normalize to Title Case so it lines up with how codex entries are typically named.
    eventish_pat = re.compile(
        r"\b(siege|ritual|treaty|war|battle|uprising|rebellion|accord|pact|curse|plague)\s+of\s+([A-Za-z][A-Za-z'’\-]+(?:\s+[A-Za-z][A-Za-z'’\-]+){0,3})\b",
        re.IGNORECASE,
    )
    for m in eventish_pat.finditer(scan):
        head = (m.group(1) or "").strip()
        tail = (m.group(2) or "").strip()
        if not head or not tail:
            continue
        tail_title = " ".join(w[:1].upper() + w[1:] for w in tail.replace("’", "'").split())
        cands.append(f"{head[:1].upper() + head[1:].lower()} of {tail_title}")

    return cands


def build_snippet(text: str, phrase: str, max_len: int = 120) -> str:
    text = _normalize_space(_strip_control_chars(text or ""))
    phrase = _normalize_space(phrase)
    if not text or not phrase:
        return ""
    idx = text.lower().find(phrase.lower())
    if idx < 0:
        return (text[:max_len] + ("…" if len(text) > max_len else ""))
    start = max(0, idx - 40)
    end = min(len(text), idx + len(phrase) + 60)
    snippet = text[start:end]
    if start > 0:
        snippet = "…" + snippet
    if end < len(text):
        snippet = snippet + "…"
    return snippet


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--top", type=int, default=100, help="Max missing candidates to print")
    ap.add_argument("--min-count", type=int, default=1, help="Only show candidates seen at least this many times")
    ap.add_argument("--json-out", default="", help="Optional path to write JSON report")
    args = ap.parse_args()

    codex_names = load_codex_names()

    counts: Counter[str] = Counter()
    occs: dict[str, list[Occurrence]] = defaultdict(list)

    story_count = 0
    for date_key, s in iter_story_sources():
        title = (s.get("title") or "").strip()
        text = (s.get("text") or "")
        story_count += 1

        # Audit story text only; titles are frequently non-entity phrases.
        for phrase in extract_candidates(text):
            key = _norm_key(phrase)
            # Skip if already in codex.
            if key in codex_names:
                continue
            counts[phrase] += 1
            if len(occs[phrase]) < 3:
                occs[phrase].append(
                    Occurrence(
                        date=str(date_key),
                        title=title or "(untitled)",
                        snippet=build_snippet(text, phrase),
                    )
                )

    missing = [(p, c) for p, c in counts.items() if c >= args.min_count]
    missing.sort(key=lambda t: (-t[1], t[0].casefold()))

    report = {
        "stories_scanned": story_count,
        "codex_names": len(codex_names),
        "missing_unique": len(missing),
        "missing": [
            {
                "phrase": phrase,
                "count": cnt,
                "examples": [o.__dict__ for o in occs.get(phrase, [])],
            }
            for phrase, cnt in missing[: args.top]
        ],
    }

    print(f"stories_scanned: {story_count}")
    print(f"codex_names:     {len(codex_names)}")
    print(f"missing_unique:  {len(missing)}")
    print("\nTop missing candidates:\n")
    for phrase, cnt in missing[: args.top]:
        print(f"- {phrase}  (x{cnt})")
        for o in occs.get(phrase, []):
            print(f"    {o.date} | {o.title}")
            if o.snippet:
                print(f"      {o.snippet}")

    if args.json_out:
        with open(args.json_out, "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        print(f"\nWrote JSON report: {args.json_out}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
