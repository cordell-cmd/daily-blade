#!/usr/bin/env python3

import argparse
import copy
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


def _norm(s: str) -> str:
    return " ".join(str(s or "").strip().lower().split())


def _uniq_strings(items: List[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for x in items or []:
        xs = str(x).strip()
        if not xs:
            continue
        k = _norm(xs)
        if k in seen:
            continue
        seen.add(k)
        out.append(xs)
    return out


def _merge_story_appearances(a: Any, b: Any) -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = []
    seen = set()

    def add_one(item: Any) -> None:
        if not isinstance(item, dict):
            return
        date = str(item.get("date") or "").strip()
        title = str(item.get("title") or "").strip()
        if not date or not title:
            return
        key = f"{date}||{title}".lower()
        if key in seen:
            return
        seen.add(key)
        out.append({"date": date, "title": title})

    for it in (a or []):
        add_one(it)
    for it in (b or []):
        add_one(it)

    # stable sort: date then title
    def sort_key(x: Dict[str, str]) -> Tuple[str, str]:
        return (x.get("date", ""), x.get("title", "").lower())

    out.sort(key=sort_key)
    return out


def _pick_prefer_longer(a: str, b: str) -> str:
    a = str(a or "").strip()
    b = str(b or "").strip()
    if not a:
        return b
    if not b:
        return a
    return a if len(a) >= len(b) else b


def _pick_nonempty(a: Any, b: Any) -> Any:
    if a is None:
        return b
    if isinstance(a, str) and not a.strip():
        return b
    if isinstance(a, list) and len(a) == 0:
        return b
    if isinstance(a, dict) and len(a) == 0:
        return b
    return a


def _parse_date(s: str) -> Optional[datetime]:
    s = str(s or "").strip()
    if not s:
        return None
    for fmt in ("%Y-%m-%d", "%Y/%m/%d"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            pass
    return None


@dataclass
class MergeResult:
    source: str
    target: str
    aliases_added: List[str]
    story_appearances_added: int


def merge_character(source_obj: Dict[str, Any], target_obj: Dict[str, Any]) -> MergeResult:
    source_name = str(source_obj.get("name") or "").strip()
    target_name = str(target_obj.get("name") or "").strip()

    # Aliases
    target_aliases = _uniq_strings(list(target_obj.get("aliases") or []))
    source_aliases = _uniq_strings(list(source_obj.get("aliases") or []))
    merged_aliases = _uniq_strings(target_aliases + source_aliases + [source_name])
    aliases_added = [a for a in merged_aliases if _norm(a) not in {_norm(x) for x in target_aliases}]
    if merged_aliases:
        target_obj["aliases"] = merged_aliases

    # Story appearances
    before = list(target_obj.get("story_appearances") or [])
    merged_story_apps = _merge_story_appearances(target_obj.get("story_appearances"), source_obj.get("story_appearances"))
    target_obj["story_appearances"] = merged_story_apps
    story_appearances_added = max(0, len(merged_story_apps) - len(before))

    # Appearances count: keep consistent with story_appearances when present
    if isinstance(merged_story_apps, list) and merged_story_apps:
        target_obj["appearances"] = len(merged_story_apps)
    else:
        # fallback: sum if numbers
        try:
            target_obj["appearances"] = int(target_obj.get("appearances") or 0) + int(source_obj.get("appearances") or 0)
        except Exception:
            pass

    # First story/date: prefer earliest date if both exist
    td = _parse_date(target_obj.get("first_date"))
    sd = _parse_date(source_obj.get("first_date"))
    if td and sd:
        if sd < td:
            target_obj["first_date"] = source_obj.get("first_date")
            target_obj["first_story"] = source_obj.get("first_story")
    else:
        target_obj["first_date"] = _pick_nonempty(target_obj.get("first_date"), source_obj.get("first_date"))
        target_obj["first_story"] = _pick_nonempty(target_obj.get("first_story"), source_obj.get("first_story"))

    # Merge common scalar fields (prefer existing; if empty, take source).
    # Avoid introducing new keys with null/empty values.
    for k in [
        "tagline",
        "role",
        "status",
        "travel_scope",
        "home_place",
        "home_region",
        "home_realm",
        "world",
        "notes",
    ]:
        merged = _pick_nonempty(target_obj.get(k), source_obj.get(k))
        if merged is None:
            target_obj.pop(k, None)
            continue
        if isinstance(merged, str) and not merged.strip():
            target_obj.pop(k, None)
            continue
        target_obj[k] = merged

    # Bio: prefer longer, but keep target's if it's already longer
    target_obj["bio"] = _pick_prefer_longer(target_obj.get("bio", ""), source_obj.get("bio", ""))

    # Traits
    target_traits = [str(x) for x in (target_obj.get("traits") or [])]
    source_traits = [str(x) for x in (source_obj.get("traits") or [])]
    target_obj["traits"] = _uniq_strings(target_traits + source_traits)

    # Status history (if present)
    if (isinstance(target_obj.get("status_history"), list) or isinstance(source_obj.get("status_history"), list)):
        target_obj["status_history"] = list(target_obj.get("status_history") or []) + [
            x for x in (source_obj.get("status_history") or []) if x not in (target_obj.get("status_history") or [])
        ]

    # If aliases ended up empty, omit the field.
    if not target_obj.get("aliases"):
        target_obj.pop("aliases", None)

    return MergeResult(
        source=source_name,
        target=target_name,
        aliases_added=aliases_added,
        story_appearances_added=story_appearances_added,
    )


def _find_character_index(characters: List[Dict[str, Any]], name: str) -> Optional[int]:
    want = _norm(name)
    for i, obj in enumerate(characters):
        if _norm(obj.get("name")) == want:
            return i
    return None


def apply_merges(codex: Dict[str, Any], merges: List[Tuple[str, str]]) -> List[MergeResult]:
    characters = codex.get("characters")
    if not isinstance(characters, list):
        raise SystemExit("codex.json missing top-level 'characters' list")

    results: List[MergeResult] = []
    for source_name, target_name in merges:
        si = _find_character_index(characters, source_name)
        ti = _find_character_index(characters, target_name)
        if si is None:
            raise SystemExit(f"Could not find source character: {source_name}")
        if ti is None:
            raise SystemExit(f"Could not find target character: {target_name}")
        if si == ti:
            continue

        source_obj = characters[si]
        target_obj = characters[ti]
        res = merge_character(source_obj, target_obj)
        results.append(res)

        # Remove the source entry (adjust indices if needed)
        del characters[si]
        if si < ti:
            ti -= 1

    return results


def main() -> None:
    parser = argparse.ArgumentParser(description="Deduplicate/merge character entries in codex.json")
    parser.add_argument("--codex", default="codex.json", help="Path to codex.json")
    parser.add_argument("--apply", action="store_true", help="Write changes back to codex.json")
    args = parser.parse_args()

    codex_path = Path(args.codex)
    codex = json.loads(codex_path.read_text(encoding="utf-8"))
    original = copy.deepcopy(codex)

    # Conservative, explicit merges only.
    merges = [
        ("Kael", "Kael Bloodhorn"),
        ("Vex", "Vex the Curse-Bearer"),
    ]

    results = apply_merges(codex, merges)

    changed = codex != original
    print(f"Planned merges: {len(merges)}")
    for r in results:
        print(f"- {r.source} -> {r.target} (aliases +{len(r.aliases_added)}, story_appearances +{r.story_appearances_added})")

    if not changed:
        print("No changes.")
        return

    if args.apply:
        codex["last_updated"] = datetime.utcnow().strftime("%Y-%m-%d")
        codex_path.write_text(json.dumps(codex, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        print(f"Wrote updated codex to {codex_path}")
    else:
        print("Dry run only (pass --apply to write).")


if __name__ == "__main__":
    main()
