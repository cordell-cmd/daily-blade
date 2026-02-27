#!/usr/bin/env python3
"""quick_backfill_codex_from_text.py

No-LLM, heuristic codex filler.

Why:
- Story entity badges depend on `codex.json` names.
- If extraction/backfill missed entities (esp. places), badges won't appear.
- This script creates minimal placeholder entries from story text so the UI
  can pick them up immediately, without waiting for the next LLM run.

Safety:
- Only adds entries when the name is not already in `codex.json` (case-insensitive).
- Skips story titles and obvious sentence-starter noise.
- Prefer adding "unknown" fields over hallucinating details.

Usage:
  python quick_backfill_codex_from_text.py
  python quick_backfill_codex_from_text.py --include-archives
  python quick_backfill_codex_from_text.py --dry-run
"""

from __future__ import annotations

import argparse
import json
import os
import re
from typing import Dict, Iterable, List, Optional, Set, Tuple

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

POLITY_KEYWORDS = {
    "Crown",
    "Throne",
    "Regency",
    "High Council",
    "Council",
    "Empire",
    "Kingdom",
    "Duchy",
    "Principality",
    "Protectorate",
    "Republic",
    "Theocracy",
}

FACTION_KEYWORDS = {
    "Guild",
    "Brotherhood",
    "Order",
    "Cult",
    "Company",
    "Legion",
    "Syndicate",
    "Cabal",
    "Circle",
    "House",
}

PLACE_HEADWORDS = {
    "Castle": "fortress",
    "Tower": "tower",
    "Keep": "fortress",
    "Fort": "fort",
    "Citadel": "fortress",
    "Temple": "temple",
    "Shrine": "shrine",
    "Abbey": "abbey",
    "Monastery": "monastery",
    "Port": "port",
    "Harbor": "harbor",
    "Harbour": "harbor",
    "Cove": "cove",
    "Bay": "bay",
    "March": "wilderness",
    "Marches": "wilderness",
    "Marsh": "wilderness",
    "Moor": "wilderness",
    "Woods": "wilderness",
    "Wood": "wilderness",
    "Forest": "wilderness",
    "Desert": "wilderness",
    "Wastes": "wilderness",
    "Deep": "wilderness",
    "Pass": "pass",
    "Vale": "valley",
    "Valley": "valley",
    "Bridge": "bridge",
    "Road": "road",
    "Gate": "gate",
    "Gates": "gate",
    "River": "river",
    "Lake": "lake",
    "Mount": "mountain",
    "Mountain": "mountain",
    "Isle": "isle",
    "Island": "isle",
    "Ruins": "ruins",
    "Tomb": "tomb",
    "Barrow": "barrow",
    "Cairn": "cairn",
    "Hall": "hall",
    "Market": "market",
    "Garden": "garden",
    "Gardens": "garden",
}


LANGUAGE_KEYWORDS = {
    "Tongue",
    "Language",
    "Script",
    "Cant",
    "Runes",
    "Glyphs",
}


KINSHIP_RE = re.compile(
    r"\b([A-Z][\w'’\-]+)'s\s+(brother|sister|son|daughter|father|mother|uncle|aunt|cousin)\b",
    re.IGNORECASE,
)


def load_json(path: str):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path: str, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=True, indent=2)


def norm_key(s: str) -> str:
    s = (s or "").strip().replace("’", "'")
    s = re.sub(r"\s+", " ", s)
    return s.casefold()


def iter_story_sources(include_archives: bool) -> Iterable[Tuple[str, dict]]:
    if os.path.exists(OUTPUT_FILE):
        data = load_json(OUTPUT_FILE)
        date_key = data.get("date") or "unknown"
        for s in data.get("stories", []) or []:
            yield date_key, s

    if not include_archives:
        return

    if os.path.exists(ARCHIVE_IDX):
        idx = load_json(ARCHIVE_IDX)
        for date_key in idx.get("dates", []) or []:
            archive_path = os.path.join(ARCHIVE_DIR, f"{date_key}.json")
            if not os.path.exists(archive_path):
                continue
            data = load_json(archive_path)
            archive_date = data.get("date") or date_key
            for s in data.get("stories", []) or []:
                yield archive_date, s


def load_codex() -> dict:
    if os.path.exists(CODEX_FILE):
        return load_json(CODEX_FILE)
    return {"last_updated": "unknown"}


def codex_name_keys(codex: dict) -> Set[str]:
    keys: Set[str] = set()

    def add(nm: str):
        k = norm_key(nm)
        if k and len(k) >= 3:
            keys.add(k)

    for _, v in (codex or {}).items():
        if not isinstance(v, list):
            continue
        for item in v:
            if not isinstance(item, dict):
                continue
            nm = (item.get("name") or "").strip()
            if not nm:
                continue
            k = norm_key(nm)
            add(k)
            if k.startswith("the "):
                add(k[4:].strip())
            the_idx = k.find(" the ")
            if the_idx > 2:
                add(k[:the_idx].strip())
            comma_idx = k.find(",")
            if comma_idx > 2:
                add(k[:comma_idx].strip())

    return keys


# Title-case phrase matcher (line-local, avoids spanning paragraphs).
# Based on the proven matcher in audit_backfill_now.py.
TITLE_PHRASE_RE = re.compile(
    r"\b(?:"
    r"[A-Z][\w'’\-]+"
    r"(?:\s+(?:of|the|and|for|in|on|at|to|from|with|without|under|over|near|by|upon|within|between|beyond)\s+"
    r"[A-Z][\w'’\-]+)?"
    r")(?:\s+"
    r"(?:[A-Z][\w'’\-]+"
    r"(?:\s+(?:of|the|and|for|in|on|at|to|from|with|without|under|over|near|by|upon|within|between|beyond)\s+"
    r"[A-Z][\w'’\-]+)?"
    r")){0,5}\b"
)


def extract_candidates_from_story(story: dict) -> List[str]:
    title = (story.get("title") or "").strip()
    text = (story.get("text") or "")
    candidates: List[str] = []
    seen: Set[str] = set()

    def add(cand: str):
        cand = (cand or "").strip()
        if not cand:
            return
        cand = re.sub(r"\s+", " ", cand)
        cand = cand.replace("—", "-").replace("–", "-").replace("’", "'")
        if title and cand.casefold() == title.casefold():
            return
        if cand in SENTENCE_START_STOP:
            return
        key = norm_key(cand)
        if not key or key in seen:
            return
        seen.add(key)
        candidates.append(cand)

    for line in text.splitlines():
        raw = line
        line = line.strip().replace("—", "-").replace("–", "-")
        if not line:
            continue

        # Title-case candidates (core path)
        for m in TITLE_PHRASE_RE.finditer(line):
            add(m.group(0) or "")

        lower = line.lower()

        # Events like "the war for the Sunken Wells" (note: 'war' can be lowercase)
        for m in re.finditer(
            r"\b(war|battle|siege|treaty|pact|accord|rebellion|uprising|massacre|plague|curse)\s+for\s+the\s+([A-Z][\w'’\-]+(?:\s+[A-Z][\w'’\-]+){0,6})\b",
            line,
        ):
            head = (m.group(1) or "").strip().capitalize()
            tail = (m.group(2) or "").strip()
            add(f"{head} for the {tail}")

        # Places like "ruins of Old Keth" / "gardens of the Underlords" (head can be lowercase)
        for m in re.finditer(r"\b(ruins|gardens)\s+of\s+(the\s+)?([A-Z][\w'’\-]+(?:\s+[A-Z][\w'’\-]+){0,6})\b", line, re.IGNORECASE):
            head = (m.group(1) or "").strip().capitalize()
            the = (m.group(2) or "")
            tail = (m.group(3) or "").strip()
            if the.strip():
                add(f"{head} of the {tail}")
            else:
                add(f"{head} of {tail}")

        # Factions like "the khan's armies" (often lowercase)
        if re.search(r"\bthe\s+khan's\s+armies\b", lower):
            add("The Khan's Armies")

        # Contextual kinship: line-leading "Name ... her brother" -> "Name's brother"
        # Keep this conservative to avoid junk like "Worse's brother".
        m = re.search(r"^\s*([A-Z][\w’\-]+)\b[^\n]{0,180}\bher\s+brother\b", line)
        if m:
            who = (m.group(1) or "").strip().replace("’", "'")
            if who.endswith("'s"):
                who = who[:-2]
            if who and who not in {"Worse", "Better", "Perhaps", "Then", "But", "And", "Or", "If", "When", "As"}:
                add(f"{who}'s brother")

    return candidates


def looks_like_character(name: str) -> bool:
    n = (name or "").strip()
    if not n or n.startswith("The "):
        return False

    if re.match(r"^[A-Z][\w’'\-]+\s+the\s+[A-Z][\w’'\-]+(?:\s+[A-Z][\w’'\-]+){0,2}$", n):
        return True

    if re.match(r"^(Master|Lady|Lord|Sir|Dame)\s+[A-Z][\w’'\-]+$", n):
        return True
    return False


def classify_candidate(name: str) -> Tuple[str, dict]:
    n = (name or "").strip()

    # Characters (kinship labels like "Yareth's brother")
    if re.search(r"\b[A-Z][\w'’\-]+\s*'s\s+(brother|sister|son|daughter|father|mother|uncle|aunt|cousin)\b", n):
        return "characters", {
            "name": n,
            "tagline": "",
            "role": "Unknown",
            "status": "unknown",
            "travel_scope": "unknown",
            "home_place": "",
            "home_region": "",
            "home_realm": "",
            "status_history": [],
            "world": "The Known World",
            "bio": "",
            "traits": [],
            "notes": "",
        }

    # Lore: languages / scripts
    if any(k in n for k in LANGUAGE_KEYWORDS):
        return "lore", {
            "name": n,
            "tagline": "",
            "category": "language",
            "source": "",
            "status": "rumored",
            "notes": "",
        }

    # Events
    head = n.split()[0] if n.split() else ""
    if head in EVENT_HEADWORDS or any(hw in n for hw in EVENT_HEADWORDS):
        event_type = head.lower() if head in EVENT_HEADWORDS else "event"
        return "events", {
            "name": n,
            "tagline": "",
            "event_type": event_type,
            "participants": [],
            "outcome": "",
            "significance": "",
        }

    # Polities
    if any(k in n for k in POLITY_KEYWORDS):
        polity_type = "council" if "Council" in n else "crown" if "Crown" in n else "polity"
        return "polities", {
            "name": n,
            "tagline": "",
            "polity_type": polity_type,
            "realm": "unknown",
            "region": "unknown",
            "seat": "unknown",
            "sovereigns": [],
            "claimants": [],
            "status": "unknown",
            "description": "",
            "notes": "",
        }

    # Factions
    if re.search(r"\b(khan's\s+armies|khan\s+armies)\b", n, re.IGNORECASE):
        return "factions", {
            "name": "The Khan's Armies",
            "tagline": "",
            "alignment": "",
            "goals": "",
            "leader": "",
            "status": "unknown",
            "notes": "",
        }

    # Factions (heuristic plural groups like "Underlords" / "Steppe Riders")
    if " " not in n and re.search(r"(lords|riders|armies|hosts|clans)$", n, re.IGNORECASE):
        return "factions", {
            "name": n,
            "tagline": "",
            "alignment": "",
            "goals": "",
            "leader": "",
            "status": "unknown",
            "notes": "",
        }

    if any(k in n for k in FACTION_KEYWORDS):
        return "factions", {
            "name": n,
            "tagline": "",
            "alignment": "",
            "goals": "",
            "leader": "",
            "status": "unknown",
            "notes": "",
        }

    # Characters
    if looks_like_character(n):
        return "characters", {
            "name": n,
            "tagline": "",
            "role": "Unknown",
            "status": "unknown",
            "travel_scope": "unknown",
            "home_place": "",
            "home_region": "",
            "home_realm": "",
            "status_history": [],
            "world": "The Known World",
            "bio": "",
            "traits": [],
            "notes": "",
        }

    # Places
    # Use headwords anywhere in phrase for type; default to unknown type.
    place_type = ""
    for hw, typ in PLACE_HEADWORDS.items():
        if re.search(rf"\b{re.escape(hw)}\b", n):
            place_type = typ
            break

    return "places", {
        "name": n,
        "tagline": "",
        "place_type": place_type,
        "world": "The Known World",
        "hemisphere": "unknown",
        "continent": "unknown",
        "subcontinent": "unknown",
        "realm": "unknown",
        "province": "unknown",
        "region": "unknown",
        "district": "unknown",
        "atmosphere": "",
        "description": "",
        "status": "unknown",
        "notes": "",
    }


def _is_high_signal_single_token(name: str) -> bool:
    """Single-token candidates are very noisy; keep only strong signals."""
    n = (name or "").strip()
    if not n or " " in n:
        return False
    low = n.casefold().replace("’", "'")

    # Drop possessive fragments (we want possessives only as part of a phrase, e.g. "Vetch's Tower").
    if low.endswith("'s"):
        return False

    # Drop common pronoun contractions that can appear capitalized at sentence start.
    if low in {"i'm", "i've", "i'll", "you've", "you'll", "you're", "we're", "we've", "they're", "they've", "it's", "that's", "there's"}:
        return False

    # Drop broken hyphen fragments.
    if n.startswith("-") or n.endswith("-"):
        return False

    # Keep fantasy-style names with punctuation/diacritics.
    if any(ch in n for ch in ["'", "’", "-"]):
        return True
    # Otherwise, skip (too many false positives like "Instead", "Tomorrow").
    return False


def _is_place_like_phrase(name: str) -> bool:
    n = (name or "").strip()
    if not n:
        return False
    for hw in PLACE_HEADWORDS.keys():
        if re.search(rf"\b{re.escape(hw)}\b", n):
            return True
    # Permit single-token, high-signal names (e.g., Xul'thyris, Thul-Kâr).
    if _is_high_signal_single_token(n):
        return True
    return False


def should_add_placeholder(cat: str, name: str) -> bool:
    """Gate placeholder creation to avoid polluting the codex."""
    n = (name or "").strip()
    if not n:
        return False

    if cat == "places":
        return _is_place_like_phrase(n)

    if cat == "events":
        # Events are already fairly constrained by classification.
        return True

    if cat == "polities":
        return True

    if cat == "factions":
        return True

    if cat == "characters":
        # Only allow obvious character formats; skip single-token names.
        if " " not in n:
            # Allow kinship labels which are multi-token but might include apostrophe.
            return False
        return looks_like_character(n) or ("'s " in n and any(r in n.lower() for r in ["brother","sister","son","daughter","father","mother","uncle","aunt","cousin"]))

    if cat == "lore":
        # Allow language/legend/history entries if they look like a proper noun.
        return True

    return False


def signature_key_for_name(name: str) -> str:
    skip = {"the", "a", "an", "of", "and", "to", "in", "on", "at"}
    words = [w.strip("()[]{}.,!?\"'“”‘’:-").lower() for w in (name or "").split()]
    sig = [w for w in words if w and w not in skip]
    if len(sig) >= 2:
        return sig[0] + " " + sig[1]
    if sig:
        return sig[0]
    return words[0] if words else ""


def appearances_for(name: str, stories_with_dates: List[Tuple[str, dict]]) -> List[dict]:
    key = signature_key_for_name(name)
    if not key:
        return []
    apps = []
    for date_key, s in stories_with_dates:
        blob = ((s.get("title") or "") + "\n" + (s.get("text") or "")).lower()
        if key in blob:
            apps.append({"date": date_key, "title": s.get("title") or ""})
    return apps


def ensure_category_list(codex: dict, cat: str) -> List[dict]:
    arr = codex.get(cat)
    if not isinstance(arr, list):
        arr = []
        codex[cat] = arr
    return arr


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--include-archives", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--max-add", type=int, default=40)
    ap.add_argument("--story-title", type=str, default="", help="Only scan stories whose title contains this text")
    ap.add_argument("--disable-place-gate", action="store_true", help="Scan even if the story already matches a known place")
    args = ap.parse_args()

    codex = load_codex()
    existing_keys = codex_name_keys(codex)

    existing_place_names = {
        norm_key((p.get("name") or ""))
        for p in (codex.get("places") or [])
        if isinstance(p, dict) and (p.get("name") or "").strip()
    }

    # Gather stories
    stories_with_dates = list(iter_story_sources(include_archives=bool(args.include_archives)))
    if args.story_title:
        want = args.story_title.casefold().strip()
        stories_with_dates = [
            (d, s)
            for d, s in stories_with_dates
            if want in ((s.get("title") or "").casefold())
        ]

    # For skipping story titles:
    all_titles = {norm_key((s.get("title") or "")) for _, s in stories_with_dates if (s.get("title") or "").strip()}

    additions: List[Tuple[str, dict]] = []

    for _, s in stories_with_dates:
        # Default behavior: only intervene for stories that currently have zero place matches.
        # Can be disabled for targeted fixes (e.g., missing events/lore/factions).
        blob = ((s.get("title") or "") + "\n" + (s.get("text") or "")).lower()
        if not args.disable_place_gate:
            has_place_hit = any((pn and pn in blob) for pn in existing_place_names)
            if has_place_hit:
                continue

        # Add kinship refs like "Yareth's brother" as lightweight character placeholders.
        for m in KINSHIP_RE.finditer(s.get("text") or ""):
            who = (m.group(1) or "").strip()
            rel = (m.group(2) or "").strip().lower()
            if not who or not rel:
                continue
            label = f"{who}'s {rel}"
            ck = norm_key(label)
            if ck and ck not in existing_keys:
                cat, entry = ("characters", {
                    "name": label,
                    "tagline": "",
                    "role": "Unknown",
                    "status": "unknown",
                    "travel_scope": "unknown",
                    "home_place": "",
                    "home_region": "",
                    "home_realm": "",
                    "status_history": [],
                    "world": "The Known World",
                    "bio": "",
                    "traits": [],
                    "notes": "",
                })
                apps = appearances_for(entry["name"], stories_with_dates)
                if apps:
                    entry["first_story"] = apps[0]["title"]
                    entry["first_date"] = apps[0]["date"]
                    entry["story_appearances"] = apps
                    entry["appearances"] = max(1, len(apps))
                else:
                    entry["first_story"] = ""
                    entry["first_date"] = (load_json(OUTPUT_FILE).get("date") if os.path.exists(OUTPUT_FILE) else "unknown")
                    entry["story_appearances"] = []
                    entry["appearances"] = 1
                additions.append((cat, entry))
                existing_keys.add(ck)
                if len(additions) >= int(args.max_add):
                    break
        if len(additions) >= int(args.max_add):
            break

        for cand in extract_candidates_from_story(s):
            ck = norm_key(cand)
            if not ck or ck in existing_keys:
                continue
            if ck in all_titles:
                continue

            # Skip pure connector phrases.
            if all(w.casefold() in CONNECTORS for w in cand.split() if w):
                continue

            cat, entry = classify_candidate(cand)

            if not should_add_placeholder(cat, entry.get("name") or ""):
                continue

            apps = appearances_for(entry["name"], stories_with_dates)
            if apps:
                entry["first_story"] = apps[0]["title"]
                entry["first_date"] = apps[0]["date"]
                entry["story_appearances"] = apps
                entry["appearances"] = max(1, len(apps))
            else:
                entry["first_story"] = ""
                entry["first_date"] = (load_json(OUTPUT_FILE).get("date") if os.path.exists(OUTPUT_FILE) else "unknown")
                entry["story_appearances"] = []
                entry["appearances"] = 1

            additions.append((cat, entry))
            existing_keys.add(ck)

            if len(additions) >= int(args.max_add):
                break
        if len(additions) >= int(args.max_add):
            break

    if not additions:
        print("No missing candidates to add.")
        return 0

    # Apply
    per_cat: Dict[str, int] = {}
    sample_by_cat: Dict[str, List[str]] = {}
    for cat, entry in additions:
        per_cat[cat] = per_cat.get(cat, 0) + 1
        sample_by_cat.setdefault(cat, [])
        if len(sample_by_cat[cat]) < 20:
            sample_by_cat[cat].append(entry.get("name") or "")
        ensure_category_list(codex, cat).append(entry)

    # Update last_updated based on current stories.json date if present.
    if os.path.exists(OUTPUT_FILE):
        try:
            codex["last_updated"] = load_json(OUTPUT_FILE).get("date") or codex.get("last_updated")
        except Exception:
            pass

    print("Planned additions:", per_cat)
    for cat in sorted(sample_by_cat):
        print(f"  {cat} sample:", "; ".join([n for n in sample_by_cat[cat] if n]))
    if args.dry_run:
        print("Dry run: not writing codex.json")
        return 0

    save_json(CODEX_FILE, codex)
    print(f"Wrote {CODEX_FILE} with {len(additions)} new placeholder entr(ies).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
