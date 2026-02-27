#!/usr/bin/env python3
"""Audit story→codex entity coverage.

Purpose:
- Identify stories where certain categories (especially places/events) have 0 hits.
- List name-like candidates that appear in story text but do not exist in codex.

This is a heuristic audit tool; it does not change data.

Usage:
  python audit_entity_coverage.py
  python audit_entity_coverage.py --max-missing 30
"""

from __future__ import annotations

import argparse
import json
import os
import re
from typing import Dict, List, Set, Tuple

ROOT = os.path.dirname(os.path.abspath(__file__))


def load_json(path: str):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def build_codex_name_index(codex: dict) -> Dict[str, Set[str]]:
    name_sets: Dict[str, Set[str]] = {}
    for cat, items in (codex or {}).items():
        if not isinstance(items, list):
            continue
        names = set()
        for it in items:
            if isinstance(it, dict) and it.get("name"):
                names.add(str(it["name"]).strip())
        if names:
            name_sets[str(cat)] = names
    return name_sets


_STOP_SINGLE = {
    "a",
    "an",
    "and",
    "are",
    "as",
    "at",
    "be",
    "been",
    "but",
    "by",
    "each",
    "for",
    "from",
    "had",
    "has",
    "have",
    "he",
    "her",
    "hers",
    "him",
    "his",
    "i",
    "if",
    "in",
    "into",
    "is",
    "it",
    "its",
    "like",
    "me",
    "my",
    "no",
    "not",
    "now",
    "of",
    "off",
    "on",
    "one",
    "or",
    "our",
    "out",
    "she",
    "so",
    "some",
    "soon",
    "than",
    "that",
    "the",
    "their",
    "then",
    "there",
    "these",
    "they",
    "this",
    "those",
    "three",
    "to",
    "too",
    "two",
    "under",
    "up",
    "upon",
    "was",
    "we",
    "were",
    "what",
    "when",
    "who",
    "why",
    "will",
    "with",
    "you",
    "your",
}
_BAD_SINGLE = {"anything", "everything", "nothing", "someone", "something", "yes"}

_CAND_RE = re.compile(
    r"\b[A-Z][\w’'\-]+(?:(?:(?:[ \t]+(?:of|the|and|in|on|at|to|for)[ \t]+)|[ \t]+)[A-Z][\w’'\-]+){1,4}\b"
    r"|\b[A-Z][\w’'\-]{2,}\b"
)


def extract_name_candidates(text: str, max_candidates: int = 200) -> List[str]:
    candidates: List[str] = []
    seen: Set[str] = set()
    for chunk in (text or "").splitlines():
        if not chunk.strip():
            continue
        for m in _CAND_RE.finditer(chunk):
            cand = (m.group(0) or "").strip()
            if not cand:
                continue
            cand_norm = " ".join(cand.split())
            key = cand_norm.lower()
            if key in seen:
                continue
            if " " not in cand_norm:
                if key in _STOP_SINGLE or key in _BAD_SINGLE:
                    continue
                if len(cand_norm) <= 2:
                    continue
            seen.add(key)
            candidates.append(cand_norm)
            if len(candidates) >= max_candidates:
                return candidates
    return candidates


def story_hits(text: str, name_sets: Dict[str, Set[str]]) -> Dict[str, List[str]]:
    tl = (text or "").lower()
    hits: Dict[str, List[str]] = {}
    for cat, names in name_sets.items():
        matched = [nm for nm in names if nm.lower() in tl]
        if matched:
            hits[cat] = matched
    return hits


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--max-missing", type=int, default=25)
    args = ap.parse_args()

    stories_path = os.path.join(ROOT, "stories.json")
    codex_path = os.path.join(ROOT, "codex.json")

    stories_json = load_json(stories_path)
    codex_json = load_json(codex_path)

    stories = stories_json.get("stories") or []
    name_sets = build_codex_name_index(codex_json)
    cats = sorted(name_sets)

    all_names_lower = {nm.lower() for names in name_sets.values() for nm in names}

    print(f"codex categories: {len(cats)}")
    print("counts:", {c: len(name_sets[c]) for c in cats})

    for i, s in enumerate(stories):
        title = (s.get("title") or "").strip()
        text = (s.get("text") or "")
        blob = title + "\n" + text
        hits = story_hits(blob, name_sets)
        counts = {k: len(v) for k, v in hits.items()}

        # Focus signal: stories that have some hits but zero places.
        if sum(counts.values()) > 0 and counts.get("places", 0) == 0:
            candidates = extract_name_candidates(blob)
            missing = [c for c in candidates if c.lower() not in all_names_lower]
            print("\n---")
            print(f"#{i}: {title}")
            print("hit-counts:", counts)
            print("missing-candidates:", missing[: max(0, int(args.max_missing))])

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
