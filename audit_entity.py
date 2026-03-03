#!/usr/bin/env python3
"""audit_entity.py

Server-side (GitHub Actions) codex entity completeness audit using Anthropic Haiku.

Given an entity name and type (codex category), load the entity from codex.json,
gather all story texts from its story_appearances, and ask Haiku to compare the
codex card against the actual story evidence.

Results are written to audit-entity-result.json so the browser can fetch them.

Usage:
    python audit_entity.py --entity-name "Kaelen Dray" --entity-type "characters"
"""

from __future__ import annotations

import argparse
import datetime
import json
import os
import re
import sys
from glob import glob

import anthropic


CODEX_FILE = "codex.json"
STORIES_FILE = "stories.json"
ARCHIVE_DIR = "archive"
RESULT_FILE = "audit-entity-result.json"


def _load_json(path: str):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _save_json(path: str, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
        f.write("\n")


def _normalize_key(name: str) -> str:
    return str(name or "").strip().casefold()


def find_entity(codex: dict, entity_type: str, entity_name: str):
    """Find an entity in the codex by type and name."""
    arr = codex.get(entity_type, [])
    if not isinstance(arr, list):
        return None
    key = _normalize_key(entity_name)
    for entry in arr:
        if isinstance(entry, dict) and _normalize_key(entry.get("name", "")) == key:
            return entry
    return None


def get_story_appearances(entity: dict) -> list[dict]:
    """Get all story appearances from the entity."""
    apps: list[dict] = []
    sa = entity.get("story_appearances", [])
    if isinstance(sa, list):
        for item in sa:
            if isinstance(item, dict) and item.get("date") and item.get("title"):
                apps.append({
                    "date": str(item["date"]).strip(),
                    "title": str(item["title"]).strip(),
                })
    if not apps:
        if entity.get("first_date") and entity.get("first_story"):
            apps.append({
                "date": str(entity["first_date"]).strip(),
                "title": str(entity["first_story"]).strip(),
            })
    return apps


def _iter_all_stories() -> list[dict]:
    """Load all stories from archive/*.json (excluding index.json) plus stories.json."""
    out: list[dict] = []

    # archive payloads
    archive_paths = sorted(glob(os.path.join(ARCHIVE_DIR, "*.json")))
    for path in archive_paths:
        if os.path.basename(path) == "index.json":
            continue
        try:
            day = _load_json(path)
        except Exception:
            continue
        if not isinstance(day, dict):
            continue
        date_key = str(day.get("date") or os.path.splitext(os.path.basename(path))[0]).strip()
        stories = day.get("stories", [])
        if not isinstance(stories, list):
            continue
        for s in stories:
            if not isinstance(s, dict):
                continue
            out.append({
                "date": date_key,
                "title": str(s.get("title", "")).strip(),
                "text": str(s.get("text", "")),
            })

    # current stories.json (may contain an un-archived date during local dev)
    if os.path.exists(STORIES_FILE):
        try:
            day = _load_json(STORIES_FILE)
            if isinstance(day, dict) and isinstance(day.get("stories"), list):
                date_key = str(day.get("date") or "").strip()
                for s in day.get("stories", []):
                    if not isinstance(s, dict):
                        continue
                    out.append({
                        "date": date_key,
                        "title": str(s.get("title", "")).strip(),
                        "text": str(s.get("text", "")),
                    })
        except Exception:
            pass

    # De-dup by date+title
    seen = set()
    deduped: list[dict] = []
    for s in out:
        k = (str(s.get("date", "")).strip(), _normalize_key(s.get("title", "")))
        if k in seen:
            continue
        seen.add(k)
        deduped.append(s)
    return deduped


def _extract_aliases(entity: dict) -> list[str]:
    aliases: list[str] = []
    for key in ("aliases", "alias", "aka", "epithets", "titles"):
        val = entity.get(key)
        if isinstance(val, list):
            for it in val:
                s = str(it or "").strip()
                if s:
                    aliases.append(s)
        elif isinstance(val, str):
            s = val.strip()
            if s:
                aliases.append(s)
    # unique, stable order
    seen = set()
    out: list[str] = []
    for a in aliases:
        k = _normalize_key(a)
        if k in seen:
            continue
        seen.add(k)
        out.append(a)
    return out


def _build_token_uniqueness(codex: dict, entity_type: str) -> dict[str, int]:
    """Return a map of token(casefold) -> number of entities that claim it.

    We consider tokens from an entity's name and aliases.
    Used to decide whether a single-token alias is safe to auto-link.
    """
    counts: dict[str, int] = {}
    arr = codex.get(entity_type, [])
    if not isinstance(arr, list):
        return counts

    for entry in arr:
        if not isinstance(entry, dict):
            continue
        tokens: set[str] = set()

        name = str(entry.get("name", "")).strip()
        for t in name.split():
            if t:
                tokens.add(t.casefold())

        for a in _extract_aliases(entry):
            for t in str(a).strip().split():
                if t:
                    tokens.add(t.casefold())

        for t in tokens:
            counts[t] = counts.get(t, 0) + 1

    return counts


def _role_signals(entity: dict) -> set[str]:
    """Extract a small set of role keywords used for disambiguation."""
    signals: set[str] = set()

    role = str(entity.get("role", "") or "").strip().casefold()
    status = str(entity.get("status", "") or "").strip().casefold()

    # Split on non-letters to get words.
    for w in re.split(r"[^a-z]+", role):
        if w:
            signals.add(w)
    for w in re.split(r"[^a-z]+", status):
        if w:
            signals.add(w)

    # A few common lore roles that tend to appear in prose.
    # (We keep this list short to avoid accidental matches.)
    keep = {
        "demon",
        "warlord",
        "general",
        "commander",
        "captain",
        "merchant",
        "sorcerer",
        "sorceress",
        "conjuress",
        "collector",
        "brigand",
        "thief",
        "prince",
        "king",
        "queen",
        "lich",
        "dragon",
        "wyvern",
    }
    return {s for s in signals if s in keep}


def _negative_signals_for(entity_signals: set[str]) -> set[str]:
    """Signals that *contradict* an entity's role, used to avoid false links."""
    neg: set[str] = set()
    if "demon" in entity_signals:
        neg |= {"warlord", "general", "commander", "captain"}
    if "warlord" in entity_signals or "general" in entity_signals:
        neg |= {"demon", "collector"}
    return neg


def _context_window(text: str, start: int, end: int, radius: int = 80) -> str:
    lo = max(0, start - radius)
    hi = min(len(text), end + radius)
    return text[lo:hi]


def _discover_story_mentions(entity_name: str, entity: dict, all_stories: list[dict], token_counts: dict[str, int]):
    """Return (exact_matches, possible_matches).

    exact_matches: stories where full name or a safe multi-word alias appears in text.
    possible_matches: stories where only the first token appears (not safe to auto-link).
    """
    full = str(entity_name or "").strip()
    full_cf = full.casefold()
    first = (full.split() or [""])[0]
    first_re = re.compile(rf"\b{re.escape(first)}\b", re.IGNORECASE) if first else None

    aliases = _extract_aliases(entity)
    safe_aliases = [a for a in aliases if len(a.split()) >= 2]
    safe_aliases_cf = [a.casefold() for a in safe_aliases]

    # Single-token aliases can be ambiguous (especially across roles like demon vs warlord).
    # We only auto-link them when either:
    #  - the token is unique across the entity_type, AND context does not contradict the role, OR
    #  - the context contains a role signal (e.g. "warlord Varak") and does not contradict.
    single_aliases = [a.strip() for a in aliases if len(str(a).strip().split()) == 1 and str(a).strip()]
    single_aliases_re = [re.compile(rf"\b{re.escape(a)}\b", re.IGNORECASE) for a in single_aliases]

    pos_signals = _role_signals(entity)
    neg_signals = _negative_signals_for(pos_signals)

    exact: list[dict] = []
    possible: list[dict] = []

    for s in all_stories:
        text = str(s.get("text", ""))
        text_cf = text.casefold()
        if full and full_cf in text_cf:
            exact.append({**s, "match": "full_name"})
            continue
        hit_alias = None
        for a_cf, a in zip(safe_aliases_cf, safe_aliases):
            if a_cf and a_cf in text_cf:
                hit_alias = a
                break
        if hit_alias:
            exact.append({**s, "match": f"alias:{hit_alias}"})
            continue

        # Single-token alias match with disambiguation
        single_hit = None
        single_kind = None
        for a, rx in zip(single_aliases, single_aliases_re):
            for m in rx.finditer(text):
                ctx = _context_window(text, m.start(), m.end())
                ctx_cf = ctx.casefold()

                # If the nearby context contradicts this entity's role, don't auto-link.
                if any(re.search(rf"\b{re.escape(ns)}\b", ctx_cf) for ns in neg_signals):
                    single_kind = f"alias_token_conflict:{a}"
                    continue

                tok = a.casefold()
                unique = token_counts.get(tok, 0) == 1
                has_role_hint = any(re.search(rf"\b{re.escape(ps)}\b", ctx_cf) for ps in pos_signals) if pos_signals else False

                if unique or has_role_hint:
                    single_hit = a
                    single_kind = f"alias_token:{a}" + (":unique" if unique else ":role")
                    break
                # Otherwise keep as a possible mention, but not exact.
                single_kind = f"alias_token_weak:{a}"
            if single_hit:
                break

        if single_hit:
            exact.append({**s, "match": single_kind})
            continue
        elif single_kind and (single_kind.startswith("alias_token_weak") or single_kind.startswith("alias_token_conflict")):
            possible.append({**s, "match": single_kind})
            continue

        if first_re and first_re.search(text):
            possible.append({**s, "match": "first_token"})

    return exact, possible


def load_story_text(date_key: str, title: str):
    """Load a specific story's text from archive or stories.json."""
    want = _normalize_key(title)

    # Try archive first
    archive_path = os.path.join(ARCHIVE_DIR, f"{date_key}.json")
    if os.path.exists(archive_path):
        data = _load_json(archive_path)
        stories = data.get("stories", []) if isinstance(data, dict) else []
        for s in stories:
            if isinstance(s, dict) and _normalize_key(s.get("title", "")) == want:
                return s

    # Try stories.json
    if os.path.exists(STORIES_FILE):
        data = _load_json(STORIES_FILE)
        if isinstance(data, dict) and str(data.get("date", "")).strip() == date_key:
            stories = data.get("stories", [])
            for s in stories:
                if isinstance(s, dict) and _normalize_key(s.get("title", "")) == want:
                    return s

    return None


def build_audit_prompt(entity: dict, stories: list[dict]) -> str:
    """Build the completeness-check prompt."""
    entity_json = json.dumps(entity, indent=2, ensure_ascii=False)
    story_block = "\n\n".join(
        f'--- Story {i + 1}: "{s.get("title", "Untitled")}" ({s.get("date", "?")}) ---\n'
        f'{s.get("text", "(no text)")}'
        for i, s in enumerate(stories)
    )

    return f"""You are a lore auditor for a sword-and-sorcery serial fiction project called "The Daily Blade."

Below is a CODEX ENTRY (the current record for an entity) and ALL STORIES where this entity appears.

Your job: Compare the codex entry against every detail mentioned in the stories. Identify anything that is:
1. MISSING from the codex — facts, relationships, locations, events, traits, or status changes mentioned in stories but not in the card
2. INCORRECT — details in the codex that contradict what the stories say
3. INCOMPLETE — fields that exist but are vague/placeholder ("unknown") when the stories provide specifics
4. STALE — status, location, or relationship info that was true once but has changed in later stories

Return your analysis as JSON (no markdown fences) with this exact structure:
{{
  "findings": [
    {{
      "field": "bio",
      "issue": "missing",
      "detail": "Story 'X' reveals they were exiled from Pelimor, not mentioned in bio"
    }}
  ],
  "summary": "One-paragraph overall assessment of the card's completeness",
  "completeness_pct": 85
}}

If the card is perfect, return an empty findings array and completeness_pct of 100.

CODEX ENTRY:
{entity_json}

STORIES:
{story_block}"""


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Audit a codex entity for completeness against its story appearances"
    )
    parser.add_argument("--entity-name", required=True, help="Entity name")
    parser.add_argument("--entity-type", required=True, help="Entity type (codex category)")
    parser.add_argument("--max-tokens", type=int, default=2048)
    args = parser.parse_args()

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("ERROR: ANTHROPIC_API_KEY not set.", file=sys.stderr)
        return 2

    # Load codex and find entity
    if not os.path.exists(CODEX_FILE):
        print(f"ERROR: {CODEX_FILE} not found.", file=sys.stderr)
        return 1

    codex = _load_json(CODEX_FILE)
    entity = find_entity(codex, args.entity_type, args.entity_name)
    if not entity:
        print(
            f"ERROR: Entity '{args.entity_name}' of type '{args.entity_type}' "
            f"not found in codex.",
            file=sys.stderr,
        )
        return 1

    # Discover missing story appearances by scanning the archive for exact full-name mentions.
    # This is intentionally conservative: only exact full-name (or safe multi-word alias) matches
    # will be auto-linked into story_appearances.
    all_stories = _iter_all_stories()
    token_counts = _build_token_uniqueness(codex, args.entity_type)
    exact_matches, possible_matches = _discover_story_mentions(args.entity_name, entity, all_stories, token_counts)

    existing_keys = set()
    sa_existing = entity.get("story_appearances")
    if isinstance(sa_existing, list):
        for it in sa_existing:
            if isinstance(it, dict) and it.get("date") and it.get("title"):
                existing_keys.add((str(it["date"]).strip(), _normalize_key(it["title"])))

    added = []
    for s in exact_matches:
        date_key = str(s.get("date", "")).strip()
        title = str(s.get("title", "")).strip()
        if not date_key or not title:
            continue
        k = (date_key, _normalize_key(title))
        if k in existing_keys:
            continue
        added.append({"date": date_key, "title": title, "match": s.get("match")})
        existing_keys.add(k)

    codex_updated = False
    if added:
        sa = entity.get("story_appearances")
        if not isinstance(sa, list):
            sa = []
            entity["story_appearances"] = sa
        for it in added:
            sa.append({"date": it["date"], "title": it["title"]})

        # Keep 'appearances' consistent if present.
        if isinstance(entity.get("appearances"), int):
            entity["appearances"] = len([x for x in sa if isinstance(x, dict)])

        _save_json(CODEX_FILE, codex)
        codex_updated = True

    # Gather story appearances
    apps = get_story_appearances(entity)
    if not apps:
        print(f"ERROR: No story appearances found for '{args.entity_name}'.", file=sys.stderr)
        return 1

    # Load all story texts
    stories: list[dict] = []
    for app in apps:
        story = load_story_text(app["date"], app["title"])
        if story:
            stories.append({
                "date": app["date"],
                "title": story.get("title", app["title"]),
                "text": story.get("text", ""),
            })
        else:
            print(
                f"WARNING: Could not find story text for {app['date']}/{app['title']}",
                file=sys.stderr,
            )

    if not stories:
        print("ERROR: Could not load any story texts.", file=sys.stderr)
        return 1

    print(
        f"Auditing '{args.entity_name}' ({args.entity_type}) "
        f"against {len(stories)} stor{'y' if len(stories) == 1 else 'ies'}…"
    )

    # Call Anthropic Haiku
    prompt = build_audit_prompt(entity, stories)
    client = anthropic.Anthropic(api_key=api_key)
    # Use the same model as the daily generator unless overridden.
    import generate_stories as gs  # local import; keeps this script aligned with repo config

    model = (os.environ.get("ANTHROPIC_MODEL") or getattr(gs, "MODEL", "")).strip()
    if not model:
        model = "claude-haiku-4-5-20251001"

    resp = client.messages.create(
        model=model,
        max_tokens=int(args.max_tokens),
        messages=[{"role": "user", "content": prompt}],
    )

    raw = resp.content[0].text.strip() if resp and resp.content else ""

    # Parse JSON from response
    try:
        cleaned = raw
        if cleaned.startswith("```"):
            cleaned = cleaned.split("\n", 1)[-1] if "\n" in cleaned else cleaned[3:]
        if cleaned.endswith("```"):
            cleaned = cleaned[:-3]
        cleaned = cleaned.strip()
        result = json.loads(cleaned)
    except (json.JSONDecodeError, ValueError):
        result = {"findings": [], "summary": raw, "completeness_pct": None, "raw": True}

    # Attach metadata so the browser can verify it got the right result
    result["entity_name"] = args.entity_name
    result["entity_type"] = args.entity_type
    result["stories_checked"] = len(stories)
    result["story_appearances_added"] = added
    result["story_appearances_added_count"] = len(added)
    # Only include a small sample of possible (first-token-only) matches to avoid noise.
    result["possible_story_mentions_sample"] = [
        {"date": s.get("date"), "title": s.get("title"), "match": s.get("match")}
        for s in possible_matches[:10]
    ]
    result["codex_updated"] = bool(codex_updated)
    result["timestamp"] = datetime.datetime.now(datetime.timezone.utc).isoformat()

    # Write result
    _save_json(RESULT_FILE, result)
    print(f"Result written to {RESULT_FILE}")

    # Print summary
    findings = result.get("findings", [])
    pct = result.get("completeness_pct")
    print(f"Completeness: {pct}%" if pct is not None else "Completeness: unknown")
    print(f"Findings: {len(findings)}")
    for f in findings:
        print(f"  - [{f.get('issue', '?')}] {f.get('field', '?')}: {f.get('detail', '')}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
